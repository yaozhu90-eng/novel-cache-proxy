
import http from "node:http";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD;
const OPENROUTER_BASE = "https://openrouter.ai/api/v1";
const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY;
const DIAG = process.env.CACHE_DIAGNOSTICS === "true";
const STATE_TABLE = "cherry_cache_breakpoints";
const MAX_BODY_BYTES = 12 * 1024 * 1024;
const DB_TIMEOUT_MS = 4000;

const sha256 = value => crypto.createHash("sha256").update(value).digest("hex");
const noMarkers = (key, value) =>
  key === "prompt_cache_breakpoint" ? undefined : value;

function sendJSON(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(data));
}

function isAuthorized(req) {
  if (!PROXY_PASSWORD) return false;
  const a = Buffer.from(req.headers.authorization || "");
  const b = Buffer.from(`Bearer ${PROXY_PASSWORD}`);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function makeSessionId(body) {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const firstUser = messages.findIndex(m => m?.role === "user");
  const initialMessages = firstUser >= 0
    ? messages.slice(0, firstUser + 1)
    : messages.slice(0, 1);
  return `cherry-${sha256(JSON.stringify({ initialMessages })).slice(0, 40)}`;
}

async function readJSONBody(req) {
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > MAX_BODY_BYTES) {
      const error = new Error("Request body too large");
      error.status = 413;
      throw error;
    }
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    const error = new Error("Invalid JSON body");
    error.status = 400;
    throw error;
  }
}

function userMessageFingerprint(message) {
  if (message?.role !== "user") return null;
  const text = typeof message.content === "string"
    ? message.content
    : JSON.stringify(message.content ?? null, noMarkers);
  return sha256(text);
}

// 固定全部纯文本用户消息的上游格式，避免断点移动改变旧前缀结构。
// 必须在 makeSessionId 和旧版 userMessageFingerprint 之后运行。
function normalizeUserTextContents(messages) {
  let count = 0;
  for (const message of messages) {
    if (message?.role === "user" &&
        typeof message.content === "string" && message.content.length > 0) {
      message.content = [{ type: "text", text: message.content }];
      count++;
    }
  }
  return count;
}

function clearBreakpoints(messages) {
  for (const message of messages) {
    if (!Array.isArray(message?.content)) continue;
    for (const part of message.content) {
      if (part && typeof part === "object") delete part.prompt_cache_breakpoint;
    }
  }
}

function markMessage(messages, index) {
  const message = messages[index];
  if (!message) return false;
  const marker = { mode: "explicit", ttl: "12h" };
  if (typeof message.content === "string" && message.content.length > 0) {
    message.content = [{
      type: "text", text: message.content,
      prompt_cache_breakpoint: marker
    }];
    return true;
  }
  if (Array.isArray(message.content)) {
    for (let j = message.content.length - 1; j >= 0; j--) {
      const part = message.content[j];
      if (part?.type === "text" && typeof part.text === "string" &&
          part.text.length > 0) {
        message.content[j] = { ...part, prompt_cache_breakpoint: marker };
        return true;
      }
    }
  }
  return false;
}

function configureCache(body, lastBreakpoint, originalFingerprints) {
  clearBreakpoints(body.messages);
  body.prompt_cache_options = { mode: "explicit", ttl: "12h" };
  const indexes = body.messages.flatMap((m, i) => m?.role === "user" ? [i] : []);
  const current = indexes.at(-1);
  let previous = null;
  let strategy = "previous-and-current";
  if (lastBreakpoint && Number.isInteger(lastBreakpoint.message_index)) {
    const i = lastBreakpoint.message_index;
    if (i < current && body.messages[i]?.role === "user" &&
        originalFingerprints[i] === lastBreakpoint.message_fingerprint) {
      previous = i;
    } else {
      for (let j = indexes.length - 2; j >= 0; j--) {
        const candidate = indexes[j];
        if (originalFingerprints[candidate] === lastBreakpoint.message_fingerprint) {
          previous = candidate;
          break;
        }
      }
    }
    if (previous !== null) strategy = "model-last-and-current";
  }
  if (previous === null) previous = indexes.at(-2) ?? null;
  const marked = [];
  for (const index of [previous, current]) {
    if (index !== null && index !== undefined && !marked.includes(index) &&
        markMessage(body.messages, index)) marked.push(index);
  }
  return { marked, strategy, previous, current };
}

function prefixSnapshotA
