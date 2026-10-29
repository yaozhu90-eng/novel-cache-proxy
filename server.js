
import http from "node:http";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD;
const OPENROUTER_BASE = "https://openrouter.ai/api/v1";
const MAX_BODY_BYTES = 12 * 1024 * 1024;
const DIAG = process.env.CACHE_DIAGNOSTICS === "true";

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
  if (typeof body.session_id === "string" && body.session_id.length > 0) {
    return body.session_id;
  }

  const messages = Array.isArray(body.messages) ? body.messages : [];
  const firstUser = messages.findIndex(m => m?.role === "user");
  const initialMessages = firstUser >= 0
    ? messages.slice(0, firstUser + 1)
    : messages.slice(0, 1);

  const identity = JSON.stringify({
    model: body.model || "",
    initialMessages
  });

  const hash = crypto
    .createHash("sha256")
    .update(identity)
    .digest("hex")
    .slice(0, 40);

  return `cherry-${hash}`;
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
    return JSON.parse(
      Buffer.concat(chunks).toString("utf8")
    );
  } catch {
    const error = new Error("Invalid JSON body");
    error.status = 400;
    throw error;
  }
}

function clearBreakpoints(messages) {
  for (const message of messages) {
    if (!Array.isArray(message?.content)) continue;

    for (const part of message.content) {
      if (part && typeof part === "object") {
        delete part.prompt_cache_breakpoint;
      }
    }
  }
}

function markMessage(messages, index) {
  const message = messages[index];
  if (!message) return false;

  const marker = {
    mode: "explicit",
    ttl: "12h"
  };

  if (
    typeof message.content === "string" &&
    message.content.length > 0
  ) {
    message.content = [{
      type: "text",
      text: message.content,
      prompt_cache_breakpoint: marker
    }];

    return true;
  }

  if (Array.isArray(message.content)) {
    for (let j = message.content.length - 1; j >= 0; j--) {
      const part = message.content[j];

      if (
        part?.type === "text" &&
        typeof part.text === "string" &&
        part.text.length > 0
      ) {
        message.content[j] = {
          ...part,
          prompt_cache_breakpoint: marker
        };
        return true;
      }
    }
  }

  return false;
}

function configureCache(body) {
  clearBreakpoints(body.messages);

  body.prompt_cache_options = {
    mode: "explicit",
    ttl: "12h"
  };

  const indexes = [];

  for (let i = 0; i < body.messages.length; i++) {
    if (body.messages[i]?.role === "user") {
      indexes.push(i);
    }
  }

  const marked = [];

  // 保留上一轮断点，同时标记当前轮断点。
  for (const index of indexes.slice(-2)) {
    if (markMessage(body.messages, index)) {
      marked.push(index);
    }
  }

  return marked;
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
      content_type:
        typeof content === "string"
          ? "text"
          : Array.isArray(content)
            ? "parts"
            : "other"
    };
  });

  return {
    trace_id: traceId,
    endpoint: "/chat/completions",
    model: body.model,
    stream: body.stream === true,
    stream_options: body.stream_options,
    provider: body.provider,
    model_fallback_present: Object.hasOwn(body, "models"),
    prompt_cache_options: body.prompt_cache_options,
    session_id_present: typeof body.session_id === "string",
    prompt_cache_breakpoint_count: breakpoints.length,
    breakpoints,
    messages
  };
}

async function forwardRequest(req, res, path, body) {
  const traceId = crypto.randomUUID();
  const isChat = path === "/chat/completions";

  if (DIAG && isChat) {
    console.log(
      "CACHE_DIAG_REQUEST",
      JSON.stringify(makeSafeRequestLog(body, traceId))
    );
  }

  const upstream = await fetch(`${OPENROUTER_BASE}${path}`, {
    method: req.method,
    headers: {
      Authorization: `Bearer ${OPENROUTER_API_KEY}`,
      "Content-Type": "application/json"
    },
    ...(body !== undefined
      ? { body: JSON.stringify(body) }
      : {})
  });

  const contentType =
    upstream.headers.get("content-type") || "application/json";

  res.writeHead(upstream.status, {
    "Content-Type": contentType,
    "Cache-Control": "no-store"
  });

  if (!upstream.body) {
    res.end();
    return;
  }

  const isSSE = contentType.includes("text/event-stream");

  let buffer = "";
  let sawUsage = false;
  let diagnosticsStopped = false;

  const decoder = new TextDecoder();
  const MAX_DIAG_BUFFER = 128 * 1024;

  function recordUsage(data) {
    if (
      !data ||
      typeof data !== "object" ||
      !data.usage ||
      typeof data.usage !== "object"
    ) {
      return;
    }

    sawUsage = true;

    console.log(
      "CACHE_DIAG_USAGE",
      JSON.stringify({
        trace_id: traceId,
        usage: data.usage
      })
    );
  }

  function processSSEEvent(eventText) {
    const payload = eventText
      .split("\n")
      .filter(line => line.startsWith("data:"))
      .map(line => line.slice(5).trimStart())
      .join("\n");

    if (!payload || payload === "[DONE]") return;

    try {
      recordUsage(JSON.parse(payload));
    } catch {
      // 忽略非 JSON 事件
    }
  }

  try {
    for await (const chunk of upstream.body) {
      if (res.destroyed) break;

      // 将上游响应原样转发给 Cherry Studio。
      res.write(chunk);

      if (!DIAG || !isChat || diagnosticsStopped) {
        continue;
      }

      buffer += decoder.decode(chunk, {
        stream: true
      });

      if (isSSE) {
        buffer = buffer.replace(/\r\n/g, "\n");

        let boundary;

        while ((boundary = buffer.indexOf("\n\n")) !== -1) {
          const eventText = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          processSSEEvent(eventText);
        }

        if (buffer.length > MAX_DIAG_BUFFER) {
          buffer = "";
        }
      } else if (buffer.length > MAX_DIAG_BUFFER) {
        diagnosticsStopped = true;
        buffer = "";
      }
    }

    if (DIAG && isChat) {
      if (!isSSE && !diagnosticsStopped) {
        try {
          recordUsage(JSON.parse(buffer));
        } catch {
          // 忽略非 JSON 响应
        }
      }

      console.log(
        "CACHE_DIAG_RESULT",
        JSON.stringify({
          trace_id: traceId,
          upstream_status: upstream.status,
          content_type: contentType,
          usage_found: sawUsage
        })
      );
    }
  } finally {
    if (!res.destroyed) {
      res.end();
    }
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const path = new URL(
      req.url,
      "http://localhost"
    ).pathname;

    if (path === "/health" && req.method === "GET") {
      return sendJSON(res, 200, {
        status: "ok",
        service: "cherry-cache-proxy",
        requested_prompt_cache_ttl: "12h",
        cache_mode: "explicit",
        cache_strategy: "previous-and-current",
        provider_only: ["openai", "openai/flex"],
        provider_fallbacks: false,
        diagnostics_enabled: DIAG
      });
    }

    if (!OPENROUTER_API_KEY || !PROXY_PASSWORD) {
      return sendJSON(res, 503, {
        error: "Server environment variables missing"
      });
    }

    if (!isAuthorized(req)) {
      return sendJSON(res, 401, {
        error: "Unauthorized"
      });
    }

    if (path === "/v1/models" && req.method === "GET") {
      return await forwardRequest(req, res, "/models");
    }

    if (
      path === "/v1/chat/completions" &&
      req.method === "POST"
    ) {
      const body = await readJSONBody(req);

      if (
        !body ||
        typeof body !== "object" ||
        Array.isArray(body)
      ) {
        return sendJSON(res, 400, {
          error: "Request body must be an object"
        });
      }

      if (!Array.isArray(body.messages)) {
        return sendJSON(res, 400, {
          error: "messages must be an array"
        });
      }

      // 只允许 OpenAI 官方普通端点和 OpenAI Flex。
      // 官方端点不可用时，不回退到第三方供应商。
      body.provider = {
        only: ["openai", "openai/flex"],
        allow_fallbacks: false
      };

      // 禁止模型级回退。
      delete body.models;

      // 保持同一对话的稳定 session_id。
      body.session_id = makeSessionId(body);

      // 保留上一轮 + 当前轮的显式缓存断点。
      const marked = configureCache(body);

      if (DIAG) {
        console.log(
          "CACHE_DIAG_BREAKPOINT",
          JSON.stringify({
            strategy: "previous-and-current",
            added: marked.length > 0,
            count: marked.length,
            message_indexes: marked,
            provider_only: ["openai", "openai/flex"],
            provider_fallbacks: false
          })
        );
      }

      if (body.stream === true) {
        body.stream_options = {
          ...(body.stream_options || {}),
          include_usage: true
        };
      }

      return await forwardRequest(
        req,
        res,
        "/chat/completions",
        body
      );
    }

    return sendJSON(res, 404, {
      error: "Not found"
    });
  } catch (error) {
    console.error(
      "Proxy error:",
      error?.name || "Error"
    );

    if (!res.headersSent) {
      return sendJSON(
        res,
        error.status || 502,
        {
          error: error.message || "Proxy error"
        }
      );
    }

    res.destroy();
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Proxy listening on port ${PORT}`);
});
