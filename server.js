import http from "node:http";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 3000);

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD;

const OPENROUTER_BASE = "https://openrouter.ai/api/v1";
const MAX_BODY_BYTES = 12 * 1024 * 1024;

// 在 Render 设置 CACHE_DIAGNOSTICS=true 才开启日志
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

  return (
    a.length === b.length &&
    crypto.timingSafeEqual(a, b)
  );
}

function makeSessionId(body) {
  if (
    typeof body.session_id === "string" &&
    body.session_id.length > 0
  ) {
    return body.session_id;
  }

  const messages = Array.isArray(body.messages)
    ? body.messages
    : [];

  const index = messages.findIndex(
    m => m && m.role === "user"
  );

  const initialMessages = index >= 0
    ? messages.slice(0, index + 1)
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

// 生成安全的诊断信息：不包含聊天正文或密钥
function makeSafeRequestLog(body, traceId) {
  return {
    trace_id: traceId,
    endpoint: "/chat/completions",
    model: body.model,
    stream: body.stream === true,
    stream_options: body.stream_options,
    prompt_cache_options: body.prompt_cache_options,
    prompt_cache_breakpoint_present:
      Object.hasOwn(body, "prompt_cache_breakpoint"),
    session_id_present:
      typeof body.session_id === "string",
    messages: body.messages.map(message => ({
      role: message?.role,
      content_type:
        typeof message?.content === "string"
          ? "text"
          : Array.isArray(message?.content)
            ? "parts"
            : "other"
    }))
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

  const upstream = await fetch(
    `${OPENROUTER_BASE}${path}`,
    {
      method: req.method,
      headers: {
        Authorization: `Bearer ${OPENROUTER_API_KEY}`,
        "Content-Type": "application/json"
      },
      ...(body !== undefined
        ? { body: JSON.stringify(body) }
        : {})
    }
  );

  const contentType =
    upstream.headers.get("content-type") ||
    "application/json";

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
      // 忽略不是 JSON 的事件
    }
  }

  try {
    for await (const chunk of upstream.body) {
      if (res.destroyed) break;

      // 保持原始响应内容，不影响 Cherry Studio
      res.write(chunk);

      if (!DIAG || !isChat || diagnosticsStopped) {
        continue;
      }

      buffer += decoder.decode(chunk, {
        stream: true
      });

      if (isSSE) {
        // 兼容 CRLF 和 LF 换行
        buffer = buffer.replace(/\r\n/g, "\n");

        let boundary;

        while (
          (boundary = buffer.indexOf("\n\n")) !== -1
        ) {
          const eventText = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);

          processSSEEvent(eventText);
        }

        if (buffer.length > MAX_DIAG_BUFFER) {
          // 避免无限积累数据
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

    if (
      path === "/v1/models" &&
      req.method === "GET"
    ) {
      return await forwardRequest(
        req,
        res,
        "/models"
      );
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

      // 保持同一聊天的稳定会话标识
      body.session_id = makeSessionId(body);

      // 申请 12 小时缓存
      // 是否真正生效仍需 OpenRouter 确认
      body.prompt_cache_options = {
        ttl: "12h"
      };

      // 让流式响应尽可能返回 usage
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
