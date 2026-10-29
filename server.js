
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

// 将最后一个可缓存的用户文本消息
// 转换为 OpenRouter 显式缓存断点
function addExplicitCacheBreakpoint(body) {
  const messages = body.messages;

  // 从后往前找到最后一条有文本的用户消息
  for (let i = messages.length - 1; i >= 0; i--) {
    const message = messages[i];

    if (!message || message.role !== "user") {
      continue;
    }

    // Cherry Studio 常见的字符串消息
    if (
      typeof message.content === "string" &&
      message.content.length > 0
    ) {
      message.content = [
        {
          type: "text",
          text: message.content,
          prompt_cache_breakpoint: {
            mode: "explicit"
          }
        }
      ];

      return {
        added: true,
        message_index: i
      };
    }

    // 已经是多内容块的消息
    if (Array.isArray(message.content)) {
      for (
        let j = message.content.length - 1;
        j >= 0;
        j--
      ) {
        const part = message.content[j];

        if (
          part &&
          part.type === "text" &&
          typeof part.text === "string" &&
          part.text.length > 0
        ) {
          message.content[j] = {
            ...part,
            prompt_cache_breakpoint: {
              mode: "explicit"
            }
          };

          return {
            added: true,
            message_index: i
          };
        }
      }
    }
  }

  return {
    added: false,
    message_index: null
  };
}

// 只记录安全的诊断信息，不记录聊天正文
function makeSafeRequestLog(body, traceId) {
  let breakpointCount = 0;

  const messages = body.messages.map(message => {
    const content = message?.content;

    if (Array.isArray(content)) {
      for (const part of content) {
        if (part?.prompt_cache_breakpoint) {
          breakpointCount++;
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
    prompt_cache_options: body.prompt_cache_options,
    prompt_cache_breakpoint_count: breakpointCount,
    session_id_present:
      typeof body.session_id === "string",
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
      // 忽略非 JSON 事件
    }
  }

  try {
    for await (const chunk of upstream.body) {
      if (res.destroyed) break;

      // 原样转发响应给 Cherry Studio
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

        while (
          (boundary = buffer.indexOf("\n\n")) !== -1
        ) {
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

      // 保持会话标识稳定
      body.session_id = makeSessionId(body);

      // 启用 OpenAI 12 小时显式缓存
      body.prompt_cache_options = {
        mode: "explicit",
        ttl: "12h"
      };

      // 添加显式缓存断点
      const breakpoint = addExplicitCacheBreakpoint(body);

      if (DIAG) {
        console.log(
          "CACHE_DIAG_BREAKPOINT",
          JSON.stringify({
            added: breakpoint.added,
            message_index: breakpoint.message_index
          })
        );
      }

      // 流式响应中返回 usage
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
