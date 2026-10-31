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

function prefixSnapshotAt(body, index) {
  if (!Number.isInteger(index) || index < 0) return null;
  const message = body.messages[index];
  if (!Array.isArray(message?.content)) return null;
  let lastPart = -1;
  for (let j = message.content.length - 1; j >= 0; j--) {
    if (message.content[j]?.prompt_cache_breakpoint) {
      lastPart = j;
      break;
    }
  }
  if (lastPart < 0) return null;
  const prefix = [
    ...body.messages.slice(0, index),
    { ...message, content: message.content.slice(0, lastPart + 1) }
  ];
  return {
    hash: sha256(JSON.stringify(prefix, noMarkers)),
    message_hashes: prefix.map(message =>
      sha256(JSON.stringify(message, noMarkers)))
  };
}

function makePrefixDiagnostics(body) {
  const result = [];
  for (let i = 0; i < body.messages.length; i++) {
    const content = body.messages[i]?.content;
    if (!Array.isArray(content)) continue;
    for (let j = 0; j < content.length; j++) {
      if (!content[j]?.prompt_cache_breakpoint) continue;
      const prefix = [
        ...body.messages.slice(0, i),
        { ...body.messages[i], content: content.slice(0, j + 1) }
      ];
      result.push({
        message_index: i,
        part_index: j,
        prefix_sha256: sha256(JSON.stringify(prefix)),
        prefix_without_markers_sha256: sha256(JSON.stringify(prefix, noMarkers))
      });
    }
  }
  return result;
}

function requestContextHash(body) {
  const context = { ...body };
  for (const key of ["messages", "stream", "stream_options", "session_id",
                     "prompt_cache_options"]) delete context[key];
  return sha256(JSON.stringify(context));
}

function firstDifference(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return null;
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    if (a[i] !== b[i]) return i;
  }
  return null;
}

function modelKey(body) {
  return JSON.stringify([body.model || "", body.service_tier || "default"]);
}

async function dbRequest(path, options = {}) {
  if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
    throw new Error("Supabase environment variables missing");
  }
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...options,
    headers: {
      apikey: SUPABASE_SECRET_KEY,
      Authorization: `Bearer ${SUPABASE_SECRET_KEY}`,
      ...(options.headers || {})
    },
    signal: AbortSignal.timeout(DB_TIMEOUT_MS)
  });
  if (!response.ok) throw new Error(`Supabase HTTP ${response.status}`);
  if (response.status === 204) return null;
  const responseText = await response.text();
  return responseText.trim() ? JSON.parse(responseText) : null;
}

async function loadLastBreakpoint(sessionId, key) {
  const query = new URLSearchParams({
    select: "breakpoint_data", session_id: `eq.${sessionId}`,
    model_key: `eq.${key}`, limit: "1"
  });
  const rows = await dbRequest(`${STATE_TABLE}?${query}`);
  return Array.isArray(rows) ? rows[0]?.breakpoint_data ?? null : null;
}

async function saveLastBreakpoint(sessionId, key, data) {
  await dbRequest(`${STATE_TABLE}?on_conflict=session_id,model_key`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify({
      session_id: sessionId, model_key: key, breakpoint_data: data,
      updated_at: new Date().toISOString()
    })
  });
}

function makeSafeRequestLog(body, traceId) {
  const breakpoints = [];
  const messages = body.messages.map((message, index) => {
    const content = message?.content;
    if (Array.isArray(content)) {
      for (const part of content) {
        if (part?.prompt_cache_breakpoint) {
          breakpoints.push({
            message_index: index,
            mode: part.prompt_cache_breakpoint.mode,
            ttl: part.prompt_cache_breakpoint.ttl
          });
        }
      }
    }
    return {
      role: message?.role,
      content_type: typeof content === "string" ? "text" :
        Array.isArray(content) ? "parts" : "other"
    };
  });
  return {
    trace_id: traceId, endpoint: "/chat/completions", model: body.model,
    stream: body.stream === true, stream_options: body.stream_options,
    provider: body.provider, model_fallback_present: Object.hasOwn(body, "models"),
    prompt_cache_options: body.prompt_cache_options,
    session_id_present: typeof body.session_id === "string",
    prompt_cache_breakpoint_count: breakpoints.length,
    breakpoints, messages
  };
}

async function forwardRequest(req, res, path, body) {
  const traceId = crypto.randomUUID();
  const isChat = path === "/chat/completions";
  if (DIAG && isChat) {
    console.log("CACHE_DIAG_REQUEST", JSON.stringify(makeSafeRequestLog(body, traceId)));
    console.log("CACHE_DIAG_PREFIX", JSON.stringify({
      trace_id: traceId, session_id: body.session_id,
      model: body.model, service_tier: body.service_tier || "default",
      prefix_hash_algorithm: "sha256", breakpoints: makePrefixDiagnostics(body)
    }));
  }
  const upstream = await fetch(`${OPENROUTER_BASE}${path}`, {
    method: req.method,
    headers: {
      Authorization: `Bearer ${OPENROUTER_API_KEY}`,
      "Content-Type": "application/json"
    },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {})
  });
  const contentType = upstream.headers.get("content-type") || "application/json";
  res.writeHead(upstream.status, {
    "Content-Type": contentType,
    "Cache-Control": "no-store"
  });
  if (!upstream.body) {
    res.end();
    return { status: upstream.status, completed: true };
  }

  const isSSE = contentType.includes("text/event-stream");
  let buffer = "";
  let sawUsage = false;
  let diagnosticsStopped = false;
  let completed = false;
  const decoder = new TextDecoder();
  const MAX_DIAG_BUFFER = 128 * 1024;

  function recordUsage(data) {
    if (!data || typeof data !== "object" || !data.usage ||
        typeof data.usage !== "object") return;
    sawUsage = true;
    console.log("CACHE_DIAG_USAGE", JSON.stringify({
      trace_id: traceId, usage: data.usage
    }));
  }
  function processSSEEvent(eventText) {
    const payload = eventText.split("\n")
      .filter(line => line.startsWith("data:"))
      .map(line => line.slice(5).trimStart()).join("\n");
    if (!payload || payload === "[DONE]") return;
    try { recordUsage(JSON.parse(payload)); } catch { /* 非 JSON 事件 */ }
  }

  try {
    for await (const chunk of upstream.body) {
      if (res.destroyed) {
        completed = false;
        break;
      }
      res.write(chunk);
      if (!DIAG || !isChat || diagnosticsStopped) continue;
      buffer += decoder.decode(chunk, { stream: true });
      if (isSSE) {
        buffer = buffer.replace(/\r\n/g, "\n");
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) !== -1) {
          const eventText = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          processSSEEvent(eventText);
        }
        if (buffer.length > MAX_DIAG_BUFFER) buffer = "";
      } else if (buffer.length > MAX_DIAG_BUFFER) {
        diagnosticsStopped = true;
        buffer = "";
      }
    }
    completed = !res.destroyed;
    if (DIAG && isChat) {
      if (!isSSE && !diagnosticsStopped) {
        try { recordUsage(JSON.parse(buffer)); } catch { /* 非 JSON 响应 */ }
      }
      console.log("CACHE_DIAG_RESULT", JSON.stringify({
        trace_id: traceId, upstream_status: upstream.status,
        content_type: contentType, usage_found: sawUsage,
        stream_completed: completed
      }));
    }
  } finally {
    if (!res.destroyed) res.end();
  }
  return { status: upstream.status, completed };
}

const server = http.createServer(async (req, res) => {
  try {
    const path = new URL(req.url, "http://localhost").pathname;
    if (path === "/health" && req.method === "GET") {
      return sendJSON(res, 200, {
        status: "ok", service: "cherry-cache-proxy",
        requested_prompt_cache_ttl: "12h", cache_mode: "explicit",
        cache_strategy: "model-last-and-current",
        state_backend: SUPABASE_URL && SUPABASE_SECRET_KEY ? "supabase" : "unconfigured",
        provider_routes: {
          standard: ["openai"], floor: ["openai/flex"]
        },
        provider_fallbacks: false, diagnostics_enabled: DIAG,
        cache_prefix_normalization: "user-text-v1"
      });
    }
    if (!OPENROUTER_API_KEY || !PROXY_PASSWORD) {
      return sendJSON(res, 503, { error: "Server environment variables missing" });
    }
    if (!isAuthorized(req)) {
      return sendJSON(res, 401, { error: "Unauthorized" });
    }
    if (path === "/v1/models" && req.method === "GET") {
      return await forwardRequest(req, res, "/models");
    }
    if (path !== "/v1/chat/completions" || req.method !== "POST") {
      return sendJSON(res, 404, { error: "Not found" });
    }
    const body = await readJSONBody(req);
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return sendJSON(res, 400, { error: "Request body must be an object" });
    }
    if (!Array.isArray(body.messages)) {
      return sendJSON(res, 400, { error: "messages must be an array" });
    }

    // 根据客户端传入的模型 ID 固定路由。
    // 普通模型只用 Default；:floor 模型只用 Flex。
    const isFloor = typeof body.model === "string" &&
      body.model.endsWith(":floor");

    body.service_tier = isFloor ? "flex" : "default";

    body.provider = {
      only: isFloor ? ["openai/flex"] : ["openai"],
      allow_fallbacks: false
    };

    delete body.models;

    console.log("CACHE_ROUTE_PIN", JSON.stringify({
      model: body.model,
      service_tier: body.service_tier,
      provider_only: body.provider.only
    }));

    // 保持旧会话 ID 和 Supabase 用户消息指纹规则不变。
    body.session_id = makeSessionId(body);
    const originalFingerprints = body.messages.map(userMessageFingerprint);
    const normalizedCount = normalizeUserTextContents(body.messages);
    const key = modelKey(body);
    let lastBreakpoint = null;
    try {
      lastBreakpoint = await loadLastBreakpoint(body.session_id, key);
      console.log("CACHE_STATE_LOAD_OK", JSON.stringify({
        found: lastBreakpoint !== null, model: body.model,
        service_tier: body.service_tier || "default"
      }));
    } catch (error) {
      console.error("CACHE_STATE_LOAD_ERROR", error?.message || "unknown");
    }

    const currentUserIndex = body.messages.findLastIndex(m => m?.role === "user");
    const fingerprint = currentUserIndex < 0 ? null :
      originalFingerprints[currentUserIndex];
    const { marked, strategy, current, previous } =
      configureCache(body, lastBreakpoint, originalFingerprints);
    const previousSnapshot = prefixSnapshotAt(body, previous);
    const currentSnapshot = prefixSnapshotAt(body, current);
    const contextHash = requestContextHash(body);
    const savedHash = lastBreakpoint?.prefix_sha256 ?? null;
    const prefixMatch = savedHash && previousSnapshot
      ? savedHash === previousSnapshot.hash : null;

    if (DIAG) {
      console.log("CACHE_DIAG_BREAKPOINT", JSON.stringify({
        strategy, added: marked.length > 0, count: marked.length,
        message_indexes: marked,
        provider_only: body.provider.only, provider_fallbacks: false
      }));
      console.log("CACHE_PREFIX_COMPARE", JSON.stringify({
        session_id: body.session_id, model: body.model,
        previous_index: previous,
        previous_message_matches_saved: previous !== null &&
          originalFingerprints[previous] === lastBreakpoint?.message_fingerprint,
        previous_prefix_match: prefixMatch,
        saved_prefix_available: savedHash !== null,
        previous_prefix_sha256: previousSnapshot?.hash ?? null,
        saved_prefix_sha256: savedHash,
        first_different_message_index: firstDifference(
          lastBreakpoint?.prefix_message_hashes,
          previousSnapshot?.message_hashes
        ),
        other_request_fields_match: lastBreakpoint?.request_context_sha256
          ? lastBreakpoint.request_context_sha256 === contextHash : null,
        normalized_user_text_messages: normalizedCount
      }));
    }
    if (body.stream === true) {
      body.stream_options = { ...(body.stream_options || {}), include_usage: true };
    }
    const result = await forwardRequest(req, res, "/chat/completions", body);
    if (result.status >= 200 && result.status < 300 && result.completed &&
        current !== undefined && fingerprint) {
      try {
        await saveLastBreakpoint(body.session_id, key, {
          message_index: current,
          message_fingerprint: fingerprint,
          prefix_sha256: currentSnapshot?.hash ?? null,
          prefix_message_hashes: currentSnapshot?.message_hashes ?? null,
          request_context_sha256: contextHash
        });
        console.log("CACHE_STATE_SAVE_OK", JSON.stringify({
          model: body.model, service_tier: body.service_tier || "default",
          message_index: current
        }));
      } catch (error) {
        console.error("CACHE_STATE_SAVE_ERROR", error?.message || "unknown");
      }
    }
  } catch (error) {
    console.error("Proxy error:", error?.name || "Error");
    if (!res.headersSent) {
      return sendJSON(res, error.status || 502, {
        error: error.message || "Proxy error"
      });
    }
    res.destroy();
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Proxy listening on port ${PORT}`);
});
