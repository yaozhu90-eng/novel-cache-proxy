
import http from "node:http";

const PORT = Number(process.env.PORT || 3000);
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD;

const MAX_BODY_BYTES = 12 * 1024 * 1024;

function sendJSON(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(data));
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/health" && req.method === "GET") {
    return sendJSON(res, 200, { status: "ok" });
  }

  if (!OPENROUTER_API_KEY || !PROXY_PASSWORD) {
    return sendJSON(res, 503, {
      error: "Server environment variables are missing"
    });
  }

  const password = req.headers.authorization || "";
  if (password !== `Bearer ${PROXY_PASSWORD}`) {
    return sendJSON(res, 401, {
      error: "Unauthorized"
    });
  }

  if (req.url === "/v1/models" && req.method === "GET") {
    try {
      const upstream = await fetch(
        "https://openrouter.ai/api/v1/models",
        {
          headers: {
            Authorization: `Bearer ${OPENROUTER_API_KEY}`
          }
        }
      );

      res.writeHead(upstream.status, {
        "Content-Type": "application/json"
      });
      return res.end(await upstream.text());
    } catch {
      return sendJSON(res, 502, {
        error: "OpenRouter connection failed"
      });
    }
  }

  if (
    req.url !== "/v1/chat/completions" ||
    req.method !== "POST"
  ) {
    return sendJSON(res, 404, {
      error: "Not found"
    });
  }

  let chunks = [];
  let size = 0;

  try {
    for await (const chunk of req) {
      size += chunk.length;

      if (size > MAX_BODY_BYTES) {
        return sendJSON(res, 413, {
          error: "Request too large"
        });
      }

      chunks.push(chunk);
    }

    const body = JSON.parse(Buffer.concat(chunks).toString());

    const upstream = await fetch(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${OPENROUTER_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      }
    );

    res.writeHead(upstream.status, {
      "Content-Type":
        upstream.headers.get("content-type") ||
        "application/json",
      "Cache-Control": "no-store"
    });

    if (!upstream.body) {
      return res.end();
    }

    for await (const chunk of upstream.body) {
      if (!res.write(chunk)) {
        await new Promise(resolve =>
          res.once("drain", resolve)
        );
      }
    }

    res.end();
  } catch (error) {
    if (!res.headersSent) {
      sendJSON(res, 502, {
        error: "Proxy request failed"
      });
    } else {
      res.destroy();
    }
  }
});

server.requestTimeout = 10 * 60 * 1000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Proxy listening on port ${PORT}`);
});
