import http from 'node:http';
import crypto from 'node:crypto';

const E = process.env;
const PORT = Number(E.PORT || 3000);
const API = E.OPENROUTER_API_KEY;
const PASSWORD = E.PROXY_PASSWORD;
const SB = (E.SUPABASE_URL || '').replace(/\/+$/, '');
const SBKEY = E.SUPABASE_SECRET_KEY;
const DIAG = E.CACHE_DIAGNOSTICS === 'true';
const ORIGIN = E.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1';
const TABLE = 'cherry_cache_breakpoints';
const queues = new Map();
const hash = s => crypto.createHash('sha256').update(s).digest('hex');
const omit = (k, v) => k === 'prompt_cache_breakpoint' ? undefined : v;
const log = (name, obj) => console.log(name, JSON.stringify(obj));
const error = (message, status) => Object.assign(new Error(message), { status });

function json(res, status, value) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(value));
}
function authorized(req) {
  if (!PASSWORD) return false;
  const a = Buffer.from(req.headers.authorization || '');
  const b = Buffer.from('Bearer ' + PASSWORD);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
async function readBody(req) {
  const chunks = []; let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 64 * 1024 * 1024) throw error('Request body too large', 413);
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw error('Invalid JSON', 400); }
}
function makeSessionId(messages) {
  const i = messages.findIndex(m => m?.role === 'user');
  const initialMessages = messages.slice(0, i < 0 ? 1 : i + 1);
  return 'cherry-' + hash(JSON.stringify({ initialMessages })).slice(0, 40);
}
function fingerprint(m) {
  if (m?.role !== 'user') return null;
  return hash(typeof m.content === 'string' ? m.content : JSON.stringify(m.content ?? null, omit));
}
function normalize(messages) {
  let count = 0;
  for (const m of messages) if (m?.role === 'user' && typeof m.content === 'string' && m.content) {
    m.content = [{ type: 'text', text: m.content }]; count++;
  }
  return count;
}
function mark(messages, i) {
  const m = messages[i]; if (!m) return false;
  if (typeof m.content === 'string' && m.content) m.content = [{ type: 'text', text: m.content }];
  if (!Array.isArray(m.content)) return false;
  for (let j = m.content.length - 1; j >= 0; j--) {
    const p = m.content[j];
    if (p?.type === 'text' && typeof p.text === 'string' && p.text) {
      m.content[j] = { ...p, prompt_cache_breakpoint: { mode: 'explicit', ttl: '12h' } };
      return true;
    }
  }
  return false;
}
function choose(body, saved, fps) {
  for (const m of body.messages) if (Array.isArray(m?.content))
    for (const p of m.content) if (p && typeof p === 'object') delete p.prompt_cache_breakpoint;
  body.prompt_cache_options = { mode: 'explicit', ttl: '12h' };
  const users = body.messages.flatMap((m, i) => m?.role === 'user' ? [i] : []);
  const current = users.at(-1); let previous = null;
  if (saved?.message_fingerprint && Number.isInteger(current)) {
    const i = saved.message_index;
    if (Number.isInteger(i) && i < current && body.messages[i]?.role === 'user' &&
        fps[i] === saved.message_fingerprint) previous = i;
    else for (let j = users.length - 2; j >= 0; j--) {
      if (fps[users[j]] === saved.message_fingerprint) { previous = users[j]; break; }
    }
  }
  const fromState = previous !== null;
  if (previous === null) previous = users.at(-2) ?? null;
  const marked = [];
  for (const i of [previous, current]) if (Number.isInteger(i) && !marked.includes(i) && mark(body.messages, i)) marked.push(i);
  return { previous, current, marked, fromState };
}
function snapshot(body, i) {
  const m = body.messages[i]; if (!Array.isArray(m?.content)) return null;
  const j = m.content.findLastIndex(p => p?.prompt_cache_breakpoint);
  if (j < 0) return null;
  const prefix = [...body.messages.slice(0, i), { ...m, content: m.content.slice(0, j + 1) }];
  return { hash: hash(JSON.stringify(prefix, omit)), parts: prefix.map(x => hash(JSON.stringify(x, omit))) };
}
function prefixDiagnostics(body) {
  const rows = [];
  for (let i = 0; i < body.messages.length; i++) {
    const parts = body.messages[i]?.content;
    if (!Array.isArray(parts)) continue;
    for (let j = 0; j < parts.length; j++) if (parts[j]?.prompt_cache_breakpoint) {
      const prefix = [...body.messages.slice(0, i), { ...body.messages[i], content: parts.slice(0, j + 1) }];
      rows.push({ message_index: i, part_index: j,
        prefix_sha256: hash(JSON.stringify(prefix)),
        prefix_without_markers_sha256: hash(JSON.stringify(prefix, omit)) });
    }
  }
  return rows;
}
function contextHash(body) {
  const x = { ...body };
  for (const k of ['messages', 'session_id', 'stream', 'stream_options', 'prompt_cache_options']) delete x[k];
  return hash(JSON.stringify(x));
}
function firstDiff(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return null;
  for (let i = 0; i < Math.max(a.length, b.length); i++) if (a[i] !== b[i]) return i;
  return null;
}
function counts(usage) {
  const p = usage?.prompt_tokens_details || {};
  const cached = p.cached_tokens ?? usage?.cached_tokens;
  const written = p.cache_write_tokens ?? usage?.cache_write_tokens;
  return { cached: Number.isFinite(cached) ? cached : null,
    written: Number.isFinite(written) ? written : null };
}

async function db(path, options = {}) {
  if (!SB || !SBKEY) throw error('Supabase not configured', 503);
  const response = await fetch(SB + '/rest/v1/' + path, {
    ...options,
    headers: { apikey: SBKEY, Authorization: 'Bearer ' + SBKEY, ...options.headers },
    signal: AbortSignal.timeout(6000)
  });
  if (!response.ok) throw error('Supabase HTTP ' + response.status, 502);
  const s = await response.text(); return s.trim() ? JSON.parse(s) : null;
}
function criteria(session, key, more = {}) {
  return new URLSearchParams({ session_id: 'eq.' + session, model_key: 'eq.' + key, ...more });
}
async function loadRow(session, key) {
  const rows = await db(TABLE + '?' + criteria(session, key, { select: 'breakpoint_data,updated_at', limit: '1' }));
  return rows?.[0] || null;
}
async function saveCAS(session, key, value) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const old = await loadRow(session, key);
    const data = old?.breakpoint_data;
    if (Number.isInteger(data?.message_index) && data.message_index > value.message_index)
      return 'older_message_index';
    if (Number.isFinite(data?.request_started_ms) && data.request_started_ms > value.request_started_ms)
      return 'newer_request_exists';
    if (data?.message_index === value.message_index && data?.prefix_sha256 === value.prefix_sha256)
      return 'already_saved';
    const oldMs = Date.parse(old?.updated_at || '');
    const updated = new Date(Math.max(Date.now(), Number.isFinite(oldMs) ? oldMs + 1 : 0)).toISOString();
    const payload = { session_id: session, model_key: key, breakpoint_data: value, updated_at: updated };
    let rows;
    if (old) {
      rows = await db(TABLE + '?' + criteria(session, key, {
        updated_at: 'eq.' + old.updated_at, select: 'updated_at'
      }), {
        method: 'PATCH', headers: { 'Content-Type': 'application/json', Prefer: 'return=representation' },
        body: JSON.stringify(payload)
      });
    } else {
      rows = await db(TABLE + '?on_conflict=session_id,model_key', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Prefer: 'resolution=ignore-duplicates,return=representation' },
        body: JSON.stringify(payload)
      });
    }
    if (Array.isArray(rows) && rows.length === 1) return 'saved';
  }
  throw error('Supabase concurrent update retries exceeded', 503);
}
async function guardedSave(session, key, value) {
  const id = session + '\n' + key;
  const prior = queues.get(id) || Promise.resolve();
  const job = prior.catch(() => {}).then(() => saveCAS(session, key, value));
  queues.set(id, job);
  try { return await job; }
  finally { if (queues.get(id) === job) queues.delete(id); }
}
async function forward(req, res, path, body, trace) {
  if (body && DIAG) {
    log('CACHE_DIAG_REQUEST', {
      trace_id: trace, model: body.model, session_id: body.session_id,
      service_tier_requested: body.service_tier, provider: body.provider,
      prompt_cache_options: body.prompt_cache_options,
      breakpoints: prefixDiagnostics(body).map(({ message_index, part_index }) => ({ message_index, part_index }))
    });
    log('CACHE_DIAG_PREFIX', { trace_id: trace, session_id: body.session_id,
      model: body.model, service_tier: body.service_tier,
      prefix_hash_algorithm: 'sha256', breakpoints: prefixDiagnostics(body) });
  }
  const response = await fetch(ORIGIN + path, {
    method: req.method,
    headers: { Authorization: 'Bearer ' + API, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {})
  });
  const type = response.headers.get('content-type') || 'application/json';
  res.writeHead(response.status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  if (!response.body) { res.end(); return { status: response.status, complete: true, usage: null, actualTier: null }; }

  const sse = type.includes('text/event-stream');
  const decoder = new TextDecoder();
  let buffer = '', usage = null, actualTier = null, complete = false, tooLarge = false;
  function record(obj) {
    if (!obj || typeof obj !== 'object') return;
    if (typeof obj.service_tier === 'string') actualTier = obj.service_tier;
    if (obj.usage && typeof obj.usage === 'object') {
      usage = obj.usage;
      if (DIAG && body) log('CACHE_DIAG_USAGE', { trace_id: trace, usage });
    }
  }
  function event(text) {
    const payload = text.split('\n').filter(s => s.startsWith('data:')).map(s => s.slice(5).trimStart()).join('\n');
    if (payload && payload !== '[DONE]') try { record(JSON.parse(payload)); } catch {}
  }
  try {
    for await (const chunk of response.body) {
      if (res.destroyed) break;
      res.write(chunk);
      if (!body || tooLarge) continue;
      buffer += decoder.decode(chunk, { stream: true });
      if (sse) {
        buffer = buffer.replace(/\r\n/g, '\n');
        let n;
        while ((n = buffer.indexOf('\n\n')) !== -1) {
          event(buffer.slice(0, n)); buffer = buffer.slice(n + 2);
        }
        if (buffer.length > 131072) buffer = '';
      } else if (buffer.length > 4 * 1024 * 1024) { tooLarge = true; buffer = ''; }
    }
    complete = !res.destroyed;
    if (body && !tooLarge) {
      buffer += decoder.decode();
      if (sse) { if (buffer.trim()) event(buffer); }
      else try { record(JSON.parse(buffer)); } catch {}
    }
  } finally { if (!res.destroyed) res.end(); }
  if (body && DIAG) log('CACHE_DIAG_RESULT', { trace_id: trace, upstream_status: response.status,
    stream_completed: complete, usage_found: !!usage, actual_service_tier: actualTier, ...counts(usage) });
  return { status: response.status, complete, usage, actualTier };
}
http.createServer(async (req, res) => {
  try {
    const path = new URL(req.url, 'http://localhost').pathname;
    if (path === '/health' && req.method === 'GET') return json(res, 200, {
      status: 'ok', version: 'route-isolated-v4', requested_prompt_cache_ttl: '12h',
      cache_mode: 'explicit', cache_strategy: 'two-breakpoints',
      state_backend: SB && SBKEY ? 'supabase' : 'unconfigured',
      provider_routes: { standard: ['openai'], floor: ['openai/flex'] },
      provider_fallbacks: false, diagnostics_enabled: DIAG,
      cache_prefix_normalization: 'user-text-v1'
    });
    if (!API || !PASSWORD || !SB || !SBKEY) return json(res, 503, { error: 'Missing environment variables' });
    if (!authorized(req)) return json(res, 401, { error: 'Unauthorized' });
    if (path === '/v1/models' && req.method === 'GET') {
      await forward(req, res, '/models', null, crypto.randomUUID()); return;
    }
    if (path !== '/v1/chat/completions' || req.method !== 'POST') return json(res, 404, { error: 'Not found' });

    const body = await readBody(req);
    if (!body || typeof body !== 'object' || Array.isArray(body) || !Array.isArray(body.messages))
      throw error('messages must be an array', 400);
    if (typeof body.model !== 'string' || !body.model) throw error('model is required', 400);
    const trace = crypto.randomUUID(), started = Date.now();
    // 临时诊断：记录客户端标识，不修改请求，不打印聊天正文。
{
  const pickIdentifiers = obj => {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      return {};
    }

    const result = {};

    for (const [key, value] of Object.entries(obj)) {
      const looksLikeIdentifier =
        /(^|_)(id|uuid)$/i.test(key) ||
        /^(session|conversation|chat|thread|branch|parent|request|message)(Id|ID|Uuid)$/i.test(key);

      if (!looksLikeIdentifier) continue;

      if (
        value === null ||
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean'
      ) {
        result[key] = value;
      }
    }

    return result;
  };

  log('CLIENT_BRANCH_DIAG', {
    trace_id: trace,
    model: body.model,
    body_keys: Object.keys(body),
    client_ids: pickIdentifiers(body),
    metadata_keys:
      body.metadata &&
      typeof body.metadata === 'object' &&
      !Array.isArray(body.metadata)
        ? Object.keys(body.metadata)
        : [],
    metadata_ids: pickIdentifiers(body.metadata),
    client_session_header: req.headers['x-session-id'] ?? null
  });
}
    const floor = body.model.endsWith(':floor');
    body.service_tier = floor ? 'flex' : 'default';
    body.provider = { only: floor ? ['openai/flex'] : ['openai'], allow_fallbacks: false };
    delete body.models;
    const baseSession = makeSessionId(body.messages);
    body.session_id = floor ? baseSession + '-flex' : baseSession;
    const key = JSON.stringify([body.model, body.service_tier]);
    const fps = body.messages.map(fingerprint);
    const normalized = normalize(body.messages);
    log('CACHE_ROUTE_PIN', { trace_id: trace, model: body.model, service_tier: body.service_tier,
      provider_only: body.provider.only, session_id: body.session_id });

    let row = null;
    try { row = await loadRow(body.session_id, key);
      log('CACHE_STATE_LOAD_OK', { trace_id: trace, found: !!row, model: body.model, service_tier: body.service_tier });
    } catch (e) { log('CACHE_STATE_LOAD_ERROR', { trace_id: trace, error: e.message }); }
    const saved = row?.breakpoint_data || null;
    const { previous, current, marked, fromState } = choose(body, saved, fps);
    const before = snapshot(body, previous), after = snapshot(body, current);
    const ctx = contextHash(body);
    if (DIAG) {
      log('CACHE_DIAG_BREAKPOINT', { trace_id: trace, strategy: fromState ? 'model-route-last-and-current' : 'previous-and-current',
        message_indexes: marked, previous_index: previous, current_index: current,
        provider_only: body.provider.only, provider_fallbacks: false });
      log('CACHE_PREFIX_COMPARE', { trace_id: trace, session_id: body.session_id,
        model: body.model, service_tier: body.service_tier, previous_index: previous,
        previous_message_matches_saved: fromState,
        previous_prefix_match: saved?.prefix_sha256 && before ? saved.prefix_sha256 === before.hash : null,
        saved_prefix_available: !!saved?.prefix_sha256,
        first_different_message_index: firstDiff(saved?.prefix_message_hashes, before?.parts),
        other_request_fields_match: saved?.request_context_sha256 ? saved.request_context_sha256 === ctx : null,
        normalized_user_text_messages: normalized,
        saved_checkpoint_confidence: saved?.checkpoint_confidence || 'legacy_unknown' });
    }
    if (body.stream === true) body.stream_options = { ...(body.stream_options || {}), include_usage: true };
    const result = await forward(req, res, '/chat/completions', body, trace);
    const { cached, written } = counts(result.usage);
    const good = result.status >= 200 && result.status < 300 && result.complete;
    const routeMatches = !result.actualTier || result.actualTier === body.service_tier;
    if (!routeMatches) log('CACHE_ROUTE_MISMATCH', { trace_id: trace,
      requested: body.service_tier, reported: result.actualTier });
    if (!good || !routeMatches || !after || !marked.includes(current) || !fps[current] || !(written > 0)) {
      log('CACHE_STATE_SAVE_SKIPPED', { trace_id: trace, reason: !good ? 'unsuccessful' :
        !routeMatches ? 'route_mismatch' : written === null ? 'usage_missing' : written === 0 ? 'no_write' : 'uncacheable_current',
        cached_tokens: cached, cache_write_tokens: written });
      return;
    }
    try {
      const resultSave = await guardedSave(body.session_id, key, {
        message_index: current, message_fingerprint: fps[current],
        prefix_sha256: after.hash, prefix_message_hashes: after.parts,
        request_context_sha256: ctx, request_started_ms: started,
        last_write_observed_at: new Date().toISOString(),
        checkpoint_confidence: 'write_observed_position_inferred',
        cache_write_tokens: written, cache_read_tokens: cached
      });
      log(resultSave === 'saved' ? 'CACHE_STATE_SAVE_OK' : 'CACHE_STATE_SAVE_SKIPPED', {
        trace_id: trace, reason: resultSave, model: body.model,
        service_tier: body.service_tier, message_index: current,
        cache_write_tokens: written, checkpoint_confidence: 'write_observed_position_inferred' });
    } catch (e) { log('CACHE_STATE_SAVE_ERROR', { trace_id: trace, error: e.message }); }
  } catch (e) {
    console.error('Proxy error:', e.message);
    if (!res.headersSent) json(res, e.status || 502, { error: e.message });
    else res.destroy();
  }
}).listen(PORT, '0.0.0.0', () => console.log('Proxy listening on port ' + PORT));
