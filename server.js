
import http from "node:http";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD;

const OPENROUTER_BASE = "https://openrouter.ai/api/v1";
const MAX_BODY_BYTES = 12 * 1024 * 1024;

function sendJSON(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(data));
}

function isAuthorized(req) {
  const header = req.headers.authorization || "";
  const expected = `Bearer ${PROXY_PASSWORD}`;

  if (!PROXY_PASSWORD) return false;

  const a = Buffer.from(header);
  const b = Buffer.from(expected);

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

  const firstUser = messages.find(
    (message) => message && message.role === "user"
  );

  const firstUserIndex = messages.indexOf(firstUser);

  const initialMessages = firstUserIndex >= 0
    ? messages.slice(0, firstUserIndex + 1)
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

async function forwardRequest(req, res, path, body) {
  const upstreamHeaders = {
    Authorization: `Bearer ${OPENROUTER_API_KEY}`,
    "Content-Type": "application/json"
  };

  const options = {
    method: req.method,
    headers: upstreamHeaders
  };

  if (body !== undefined) {
    options.body = JSON.stringify(body);
  }

  const upstream = await fetch(
    `${OPENROUTER_BASE}${path}`,
    options
  );

  const responseHeaders = {
    "Content-Type":
      upstream.headers.get("content-type") ||
      "application/json",
    "Cache-Control": "no-store"
  };

  res.writeHead(upstream.status, responseHeaders);

  if (!upstream.body) {
    res.end();
    return;
  }

  for await (const chunk of upstream.body) {
    if (res.destroyed) break;
    res.write(chunk);
  }

  if (!res.destroyed) {
    res.end();
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
        requested_prompt_cache_ttl: "12h"
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

      // Keep the same conversation on a stable route.
      body.session_id = makeSessionId(body);

      // EXPERIMENTAL:
      // Request a 12-hour prompt cache lifetime.
      // OpenRouter may ignore this unsupported field.
      body.prompt_cache_options = {
        ttl: "12h"
      };

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
    console.error("Proxy error:", error.message);

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
