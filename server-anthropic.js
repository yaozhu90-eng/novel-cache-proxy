/*
 * Claude 专用代理 (OpenRouter -> Anthropic)
 * 1. 强制 provider = anthropic（官方）
 * 2. cache_control ephemeral ttl=1h（滑动续期）
 * 3. 无数据库、无断点仓库
 * 4. 自动补 max_tokens
 * 5. 指纹日志：一眼看出前缀有没有被改
 */

import http from 'node:http';
import crypto from 'node:crypto';

const E = process.env;

function num(name, fb, min, max) {
  const raw = E[name];
  if (raw === undefined || raw === '') return fb;
  const v = Number(raw);
  if (!Number.isFinite(v)) return fb;
  return Math.min(max, Math.max(min, v));
}

const PORT = num('PORT', 3001, 1, 65535);
const API = E.OPENROUTER_API_KEY;
const PASSWORD = E.PROXY_PASSWORD;
const ORIGIN = (E.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1').replace(/\/+$/, '');

const PROVIDER = (E.PROVIDER_ONLY || 'anthropic').trim();
const MODEL_PREFIX = (E.MODEL_PREFIX || 'anthropic/').trim();
const CACHE_TTL = (E.CACHE_TTL || '1h').trim();
const CACHE_MARKERS = Math.floor(num('CACHE_MARKERS', 1, 0, 4));
const CACHE_MIN_CHARS = Math.floor(num('CACHE_MIN_CHARS', 4096, 0, 1000000));
const DEFAULT_MAX_TOKENS = Math.floor(num('DEFAULT_MAX_TOKENS', 8192, 1, 200000));

const KEEPALIVE_IDLE_MS = num('KEEPALIVE_IDLE_MS', 15000, 0, 120000);
const KEEPALIVE_TICK_MS = num('KEEPALIVE_TICK_MS', 5000, 1000, 60000);
const UPSTREAM_TIMEOUT_MS = num('UPSTREAM_TIMEOUT_MS', 30 * 60 * 1000, 1000, 2 * 60 * 60 * 1000);
const MAX_BODY_MB = num('MAX_BODY_MB', 32, 1, 512);
const MAX_BODY_BYTES = Math.floor(MAX_BODY_MB * 1024 * 1024);
const DIAG = E.CACHE_DIAGNOSTICS === 'true';
const VERSION = 'claude-1h-cache-v3';

/* ===== 指纹 ===== */

const hash = v => crypto.createHash('sha256').update(v).digest('hex').slice(0, 16);

let lastFp = null;

function fingerprint(body) {
  return {
    model: body.model,
    system: hash(JSON.stringify(body.messages.filter(m => m.role === 'system'))),
    tools: hash(JSON.stringify(body.tools ?? null)),
    head: hash(JSON.stringify((body.messages.find(m => m.role === 'user') || {}).content ?? ''))
  };
}

function diffFp(prev, next) {
  if (!prev) return null;
  return Object.keys(next).filter(k => prev[k] !== next[k]);
}

/* ===== 基础 ===== */

function json(res, status, value) {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(value));
}

function authorized(req) {
  if (!PASSWORD) return false;
  return (req.headers.authorization || '') === `Bearer ${PASSWORD}`;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let done = false;

    req.on('data', c => {
      if (done) return;
      size += c.length;

      if (size > MAX_BODY_BYTES) {
        done = true;
        chunks.length = 0;
        reject(Object.assign(new Error(`body exceeds ${MAX_BODY_MB} MiB`), { status: 413 }));
        return;
      }
      chunks.push(c);
    });

    req.on('end', () => {
      if (done) return;
      done = true;
      try {
        resolve({ body: JSON.parse(Buffer.concat(chunks, size).toString('utf8')), bytes: size });
      } catch {
        reject(Object.assign(new Error('Invalid JSON'), { status: 400 }));
      }
    });

    req.on('error', () => {
      if (done) return;
      done = true;
      reject(Object.assign(new Error('read failed'), { status: 400 }));
    });
  });
}

/* ===== 缓存标记 ===== */

function toBlocks(content) {
  if (typeof content === 'string') {
    if (!content) return [];
    return [{ type: 'text', text: content }];
  }
  if (!Array.isArray(content)) return [];

  return content.map(b => {
    if (!b || typeof b !== 'object') return b;
    if (!('cache_control' in b)) return b;
    const { cache_control, ...rest } = b;
    return rest;
  });
}

function markTail(message) {
  if (!message) return false;

  const blocks = toBlocks(message.content);
  if (!blocks.length) return false;

  for (let i = blocks.length - 1; i >= 0; i--) {
    const b = blocks[i];
    if (b && typeof b === 'object' && typeof b.type === 'string') {
      blocks[i] = { ...b, cache_control: { type: 'ephemeral', ttl: CACHE_TTL } };
      message.content = blocks;
      return true;
    }
  }
  return false;
}

function applyCache(messages) {
  if (CACHE_MARKERS <= 0) return [];

  let chars = 0;
  for (const m of messages) chars += JSON.stringify(m.content ?? '').length;
  if (chars < CACHE_MIN_CHARS) return [];

  const marked = [];

  for (let i = messages.length - 1; i >= Math.max(0, messages.length - 4); i--) {
    if (markTail(messages[i])) { marked.push(i); break; }
  }

  if (CACHE_MARKERS > 1) {
    const head = messages.findIndex(m => m.role === 'user');
    if (head >= 0 && !marked.includes(head) && markTail(messages[head])) marked.push(head);
  }

  return marked;
}

/* ===== 转发 ===== */

async function forward(req, res, path, body, trace) {
  const controller = new AbortController();
  let disconnected = false;
  let timedOut = false;

  res.on('close', () => {
    if (!res.writableEnded) { disconnected = true; controller.abort(); }
  });

  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, UPSTREAM_TIMEOUT_MS);
  timer.unref?.();

  let keepaliveTimer = null;
  let keepalivesSent = 0;
  let lastWrite = Date.now();
  let lineBuffer = '';
  let midEvent = false;
  let usage = null;
  let sawError = false;
  let upstreamStatus = null;

  function startKeepalive() {
    if (keepaliveTimer || KEEPALIVE_IDLE_MS <= 0) return;

    keepaliveTimer = setInterval(() => {
      if (res.destroyed || res.writableEnded || res.writableNeedDrain) return;
      if (midEvent || lineBuffer) return;
      if (Date.now() - lastWrite < KEEPALIVE_IDLE_MS) return;

      try {
        res.write(': keepalive\n\n');
        lastWrite = Date.now();
        keepalivesSent++;
      } catch {}
    }, KEEPALIVE_TICK_MS);

    keepaliveTimer.unref?.();
  }

  function scan(text) {
    lineBuffer += text;

    let i;
    while ((i = lineBuffer.indexOf('\n')) >= 0) {
      const line = lineBuffer.slice(0, i).replace(/\r$/, '');
      lineBuffer = lineBuffer.slice(i + 1);

      if (line === '') { midEvent = false; continue; }
      if (line.startsWith(':')) continue;
      if (!line.startsWith('data:')) continue;

      midEvent = true;
      const payload = line.slice(5).trim();
      if (!payload || payload === '[DONE]') continue;

      try {
        const obj = JSON.parse(payload);
        if (obj?.error) sawError = true;
        if (obj?.usage && typeof obj.usage === 'object') {
          usage = { ...(usage || {}), ...obj.usage };
        }
      } catch {}
    }
  }

  function result() {
    return { status: upstreamStatus, usage, disconnected, timedOut, keepalivesSent, sawError };
  }

  try {
    const response = await fetch(`${ORIGIN}${path}`, {
      method: req.method,
      headers: { Authorization: `Bearer ${API}`, 'Content-Type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: controller.signal
    });

    upstreamStatus = response.status;

    const contentType = response.headers.get('content-type') || 'application/json';
    const sse = contentType.includes('text/event-stream');

    res.writeHead(response.status, {
      'Content-Type': contentType,
      'Cache-Control': 'no-store',
      'X-Proxy-Trace-Id': trace,
      ...(sse ? { 'X-Accel-Buffering': 'no' } : {})
    });

    if (!response.body) return result();
    if (sse) startKeepalive();

    const decoder = new TextDecoder();

    for await (const chunk of response.body) {
      if (res.destroyed) { disconnected = true; controller.abort(); break; }

      if (!res.write(chunk)) {
        await new Promise(resolve => {
          const done = () => {
            res.off('drain', done);
            res.off('close', done);
            resolve();
          };
          res.once('drain', done);
          res.once('close', done);
        });
      }

      lastWrite = Date.now();
      if (body) scan(decoder.decode(chunk, { stream: true }));
    }

    if (body) scan(decoder.decode());

    return result();
  } catch (error) {
    if (!res.headersSent) {
      json(res, timedOut ? 504 : 502, { error: 'upstream failed', trace });
    } else if (!res.destroyed && !res.writableEnded) {
      res.destroy();
    }
    return { ...result(), error: error?.name || 'Error' };
  } finally {
    clearTimeout(timer);
    if (keepaliveTimer) clearInterval(keepaliveTimer);
    if (!res.destroyed && !res.writableEnded) res.end();
  }
}

/* ===== 服务 ===== */

const server = http.createServer(async (req, res) => {
  const trace = Math.random().toString(36).slice(2, 10);

  try {
    const path = new URL(req.url, 'http://x').pathname;

    if (path === '/health' && req.method === 'GET') {
      return json(res, 200, {
        status: 'ok',
        version: VERSION,
        provider_only: PROVIDER,
        model_prefix: MODEL_PREFIX,
        cache_ttl: CACHE_TTL,
        cache_markers: CACHE_MARKERS,
        cache_min_chars: CACHE_MIN_CHARS,
        default_max_tokens: DEFAULT_MAX_TOKENS,
        keepalive_idle_ms: KEEPALIVE_IDLE_MS,
        max_body_mb: MAX_BODY_MB
      });
    }

    if (!API || !PASSWORD) return json(res, 503, { error: 'Missing env', trace });
    if (!authorized(req)) return json(res, 401, { error: 'Unauthorized', trace });

    if (path === '/v1/models' && req.method === 'GET') {
      await forward(req, res, '/models', null, trace);
      return;
    }

    if (path !== '/v1/chat/completions' || req.method !== 'POST') {
      return json(res, 404, { error: 'Not found', trace });
    }

    const { body, bytes } = await readBody(req);

    if (!body || !Array.isArray(body.messages)) {
      throw Object.assign(new Error('messages must be an array'), { status: 400 });
    }
    if (typeof body.model !== 'string' || !body.model) {
      throw Object.assign(new Error('model is required'), { status: 400 });
    }
    if (!body.model.startsWith(MODEL_PREFIX)) {
      throw Object.assign(new Error(`only ${MODEL_PREFIX}* models allowed`), { status: 400 });
    }

    delete body.service_tier;
    delete body.prompt_cache_options;
    delete body.prompt_cache_key;
    delete body.models;

    body.provider = { only: [PROVIDER], allow_fallbacks: false };

    if (body.max_tokens == null && body.max_completion_tokens == null) {
      body.max_tokens = DEFAULT_MAX_TOKENS;
    }

    if (body.stream === true) {
      body.stream_options = { ...(body.stream_options || {}), include_usage: true };
    }

    const marked = applyCache(body.messages);

    const fp = fingerprint(body);
    const changed = diffFp(lastFp, fp);
    lastFp = fp;

    if (DIAG) {
      console.log('CLAUDE_REQ', JSON.stringify({
        trace, model: body.model, bytes,
        messages: body.messages.length,
        cache_marked: marked,
        cache_ttl: CACHE_TTL,
        max_tokens: body.max_tokens ?? null,
        fp,
        changed
      }));
    }

    const r = await forward(req, res, '/chat/completions', body, trace);

    const u = r.usage;
    const cached =
      u?.prompt_tokens_details?.cached_tokens ??
      u?.cache_read_input_tokens ??
      null;
    const written =
      u?.cache_write_tokens ??
      u?.cache_creation_input_tokens ??
      null;

    if (DIAG || r.error) {
      console.log('CLAUDE_RESULT', JSON.stringify({
        trace,
        status: r.status,
        disconnected: r.disconnected,
        timeout: r.timedOut,
        error: r.error || null,
        stream_error: r.sawError,
        keepalives_sent: r.keepalivesSent,
        prompt_tokens: u?.prompt_tokens ?? null,
        completion_tokens: u?.completion_tokens ?? null,
        cached_tokens: cached,
        cache_write_tokens: written,
        hit_rate: (u?.prompt_tokens && cached != null)
          ? Number((cached / u.prompt_tokens).toFixed(3))
          : null
      }));
    }
  } catch (error) {
    if (!res.headersSent) json(res, error.status || 502, { error: error.message, trace });
    else if (!res.destroyed && !res.writableEnded) res.destroy();
  }
});

server.requestTimeout = 5 * 60 * 1000;
server.headersTimeout = 60 * 1000;

server.listen(PORT, '0.0.0.0', () => {
  console.log('CLAUDE_PROXY_STARTED', JSON.stringify({
    version: VERSION,
    port: PORT,
    provider_only: PROVIDER,
    cache_ttl: CACHE_TTL,
    cache_markers: CACHE_MARKERS
  }));
});
