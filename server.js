import http from 'node:http';
import crypto from 'node:crypto';

/* =========================
 * Configuration
 * ========================= */

const E = process.env;

function envNumber(name, fallback, min, max) {
  const raw = E[name];
  const value = raw === undefined || raw === ''
    ? fallback
    : Number(raw);

  if (!Number.isFinite(value) || value < min || value > max) {
    throw new Error(`Invalid environment variable: ${name}`);
  }

  return value;
}

const PORT = envNumber('PORT', 3000, 1, 65535);
const API = E.OPENROUTER_API_KEY;
const PASSWORD = E.PROXY_PASSWORD;
const SB = (E.SUPABASE_URL || '').replace(/\/+$/, '');
const SBKEY = E.SUPABASE_SECRET_KEY;

const ORIGIN = (
  E.OPENROUTER_BASE_URL || 'https://openrouter.ai/api/v1'
).replace(/\/+$/, '');

const TABLE = 'cherry_cache_breakpoints';
const VERSION = 'route-isolated-v5-branches';

const DIAG = E.CACHE_DIAGNOSTICS === 'true';

const MAX_BODY_MB = envNumber('MAX_BODY_MB', 64, 1, 512);
const MAX_BODY_BYTES = Math.floor(MAX_BODY_MB * 1024 * 1024);

const UPSTREAM_TIMEOUT_MS = envNumber(
  'UPSTREAM_TIMEOUT_MS',
  30 * 60 * 1000,
  1000,
  2 * 60 * 60 * 1000
);

const DB_TIMEOUT_MS = envNumber(
  'DB_TIMEOUT_MS',
  10000,
  1000,
  60000
);

const MAX_CHECKPOINTS = Math.floor(envNumber(
  'MAX_CHECKPOINTS',
  128,
  4,
  512
));

// Local metadata retention, not upstream cache retention.
const CHECKPOINT_HISTORY_HOURS = envNumber(
  'CHECKPOINT_HISTORY_HOURS',
  48,
  12,
  720
);

const CACHE_TTL_MS = 30 * 60 * 1000;

const HISTORY_MS =
  CHECKPOINT_HISTORY_HOURS * 60 * 60 * 1000;

const MAX_EVENT_BYTES = 4 * 1024 * 1024;
const MAX_JSON_RESPONSE_BYTES = 8 * 1024 * 1024;

const queues = new Map();

/* =========================
 * Utilities
 * ========================= */

const hash = value =>
  crypto.createHash('sha256').update(value).digest('hex');

const omitMarker = (key, value) =>
  key === 'prompt_cache_breakpoint' ? undefined : value;

const log = (name, value) =>
  console.log(name, JSON.stringify(value));

function fail(message, status = 500, details = {}) {
  return Object.assign(new Error(message), {
    status,
    ...details
  });
}

function json(res, status, value, extraHeaders = {}) {
  if (res.destroyed || res.writableEnded) return;

  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    ...extraHeaders
  });

  res.end(JSON.stringify(value));
}

function authorized(req) {
  if (!PASSWORD) return false;

  const actual = Buffer.from(
    req.headers.authorization || ''
  );

  const expected = Buffer.from(`Bearer ${PASSWORD}`);

  return actual.length === expected.length &&
    crypto.timingSafeEqual(actual, expected);
}

function errorCode(error) {
  const value = error?.code ?? error?.type ?? null;

  return typeof value === 'string' ||
    typeof value === 'number'
    ? String(value).slice(0, 120)
    : null;
}

function serializeMessage(message) {
  return JSON.stringify(message, omitMarker);
}

function fingerprint(message) {
  if (message?.role !== 'user') return null;

  return hash(
    typeof message.content === 'string'
      ? message.content
      : JSON.stringify(message.content ?? null, omitMarker)
  );
}

/* =========================
 * Request body
 * ========================= */

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;

    function cleanup() {
      req.off('data', onData);
      req.off('end', onEnd);
      req.off('error', onError);
      req.off('aborted', onAborted);
    }

    function rejectOnce(error) {
      if (settled) return;

      settled = true;
      cleanup();

      // Drain the remaining request without retaining it.
      // Do not destroy the socket before sending a 413.
      req.on('error', () => {});
      req.resume();

      reject(error);
    }

    function onData(chunk) {
      size += chunk.length;

      if (size > MAX_BODY_BYTES) {
        rejectOnce(fail(
          `Request body exceeds ${MAX_BODY_MB} MiB`,
          413,
          {
            received_bytes: size,
            limit_bytes: MAX_BODY_BYTES
          }
        ));

        return;
      }

      chunks.push(chunk);
    }

    function onEnd() {
      if (settled) return;

      settled = true;
      cleanup();

      try {
        const text = Buffer.concat(chunks, size)
          .toString('utf8');

        resolve({
          body: JSON.parse(text),
          bytes: size
        });
      } catch {
        reject(fail('Invalid JSON', 400, {
          received_bytes: size
        }));
      }
    }

    function onError() {
      rejectOnce(fail('Request body read failed', 400));
    }

    function onAborted() {
      rejectOnce(fail('Client aborted request body', 400));
    }

    const declared = Number(req.headers['content-length']);

    if (
      Number.isFinite(declared) &&
      declared > MAX_BODY_BYTES
    ) {
      rejectOnce(fail(
        `Request body exceeds ${MAX_BODY_MB} MiB`,
        413,
        {
          declared_bytes: declared,
          limit_bytes: MAX_BODY_BYTES
        }
      ));

      return;
    }

    req.on('data', onData);
    req.on('end', onEnd);
    req.on('error', onError);
    req.on('aborted', onAborted);
  });
}

/* =========================
 * Session and normalization
 * ========================= */

function makeSessionId(messages) {
  // Preserve the original session-ID algorithm.
  const i = messages.findIndex(m => m?.role === 'user');

  const initialMessages =
    messages.slice(0, i < 0 ? 1 : i + 1);

  return 'cherry-' +
    hash(JSON.stringify(initialMessages)).slice(0, 40);
}

function normalize(messages) {
  let count = 0;

  for (const message of messages) {
    if (
      message?.role === 'user' &&
      typeof message.content === 'string' &&
      message.content
    ) {
      message.content = [{
        type: 'text',
        text: message.content
      }];

      count++;
    }
  }

  return count;
}

function removeMarkers(messages) {
  for (const message of messages) {
    if (!Array.isArray(message?.content)) continue;

    for (const part of message.content) {
      if (part && typeof part === 'object') {
        delete part.prompt_cache_breakpoint;
      }
    }
  }
}

function lastTextPart(message) {
  if (!Array.isArray(message?.content)) return -1;

  for (let i = message.content.length - 1; i >= 0; i--) {
    const part = message.content[i];

    if (
      part?.type === 'text' &&
      typeof part.text === 'string' &&
      part.text
    ) {
      return i;
    }
  }

  return -1;
}

function mark(messages, index) {
  if (!Number.isInteger(index)) return false;

  const message = messages[index];
  const partIndex = lastTextPart(message);

  if (partIndex < 0) return false;

  message.content[partIndex] = {
    ...message.content[partIndex],
    prompt_cache_breakpoint: {
      mode: 'explicit',
      ttl: '30m'
    }
  };

  return true;
}

/* =========================
 * Exact prefix indexing
 * ========================= */

function buildPrefixIndex(messages) {
  const points = new Map();
  const messageHashes = [];

  // Incremental hash of the same JSON array representation
  // used by the original snapshot function.
  const running = crypto.createHash('sha256');
  running.update('[');

  for (let i = 0; i < messages.length; i++) {
    if (i > 0) running.update(',');

    const message = messages[i];
    const fullSerialized = serializeMessage(message);

    messageHashes.push(hash(fullSerialized));

    const partIndex = lastTextPart(message);

    if (message.role === 'user' && partIndex >= 0) {
      const partialMessage = {
        ...message,
        content: message.content.slice(0, partIndex + 1)
      };

      const prefixHash = running.copy()
        .update(serializeMessage(partialMessage))
        .update(']')
        .digest('hex');

      points.set(i, {
        message_index: i,
        part_index: partIndex,
        prefix_sha256: prefixHash,
        message_fingerprint: fingerprint(message),
        last_message_sha256: hash(
          serializeMessage(partialMessage)
        )
      });
    }

    running.update(fullSerialized);
  }

  return { points, messageHashes };
}

function prefixMessageHashes(indexData, point) {
  return [
    ...indexData.messageHashes.slice(0, point.message_index),
    point.last_message_sha256
  ];
}

function firstDiff(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return null;

  const length = Math.max(a.length, b.length);

  for (let i = 0; i < length; i++) {
    if (a[i] !== b[i]) return i;
  }

  return null;
}

/* =========================
 * Configuration diagnostics
 * ========================= */

function requestConfig(body) {
  const excluded = new Set([
    'messages',
    'session_id',
    'stream',
    'stream_options',
    'prompt_cache_options'
  ]);

  const fieldHashes = {};

  for (const key of Object.keys(body).sort()) {
    if (excluded.has(key)) continue;

    const serialized = JSON.stringify(body[key]);

    if (serialized !== undefined) {
      fieldHashes[key] = hash(serialized);
    }
  }

  return {
    field_hashes: fieldHashes,
    sha256: hash(JSON.stringify(fieldHashes))
  };
}

function changedFields(before, after) {
  if (!before || typeof before !== 'object') return null;

  const keys = new Set([
    ...Object.keys(before),
    ...Object.keys(after)
  ]);

  return [...keys]
    .filter(key => before[key] !== after[key])
    .sort();
}

function clientDiagnostics(req, body, trace) {
  if (!DIAG) return;

  function pickIds(obj) {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      return {};
    }

    const result = {};

    for (const [key, value] of Object.entries(obj)) {
      const isId =
        /(^|_)(id|uuid)$/i.test(key) ||
        /^(session|conversation|chat|thread|branch|parent|request|message)(Id|ID|Uuid)$/i.test(key);

      if (
        isId &&
        (
          value === null ||
          ['string', 'number', 'boolean'].includes(typeof value)
        )
      ) {
        // Hash identifiers rather than logging their raw values.
        result[key] =
          value === null ? null : hash(String(value));
      }
    }

    return result;
  }

  log('CLIENT_BRANCH_DIAG', {
    trace_id: trace,
    model: body.model,
    reasoning: body.reasoning ?? null,
    reasoning_effort: body.reasoning_effort ?? null,
    include_reasoning: body.include_reasoning ?? null,
    max_tokens: body.max_tokens ?? null,
    max_completion_tokens: body.max_completion_tokens ?? null,
    body_keys: Object.keys(body),
    client_ids: pickIds(body),
    metadata_keys:
      body.metadata &&
      typeof body.metadata === 'object' &&
      !Array.isArray(body.metadata)
        ? Object.keys(body.metadata)
        : [],
    metadata_ids: pickIds(body.metadata),
    client_session_header:
      req.headers['x-session-id']
        ? hash(String(req.headers['x-session-id']))
        : null
  });
}

/* =========================
 * Supabase
 * ========================= */

async function db(path, options = {}) {
  if (!SB || !SBKEY) {
    throw fail('Supabase not configured', 503);
  }

  const response = await fetch(`${SB}/rest/v1/${path}`, {
    ...options,
    headers: {
      apikey: SBKEY,
      Authorization: `Bearer ${SBKEY}`,
      ...options.headers
    },
    signal: AbortSignal.timeout(DB_TIMEOUT_MS)
  });

  if (!response.ok) {
    throw fail(`Supabase HTTP ${response.status}`, 502);
  }

  const text = await response.text();
  return text.trim() ? JSON.parse(text) : null;
}

function criteria(session, key, more = {}) {
  return new URLSearchParams({
    session_id: `eq.${session}`,
    model_key: `eq.${key}`,
    ...more
  });
}

async function loadRow(session, key) {
  const rows = await db(
    `${TABLE}?${criteria(session, key, {
      select: 'breakpoint_data,updated_at',
      limit: '1'
    })}`
  );

  return rows?.[0] || null;
}

function checkpointTime(checkpoint) {
  if (Number.isFinite(checkpoint?.request_started_ms)) {
    return checkpoint.request_started_ms;
  }

  const parsed = Date.parse(
    checkpoint?.last_write_observed_at || ''
  );

  return Number.isFinite(parsed) ? parsed : 0;
}

function unpackCheckpoints(row) {
  const data = row?.breakpoint_data;

  if (!data || typeof data !== 'object') return [];

  const entries = Array.isArray(data.checkpoints)
    ? data.checkpoints
    : [];

  const result = entries.filter(cp =>
    cp &&
    Number.isInteger(cp.message_index) &&
    typeof cp.prefix_sha256 === 'string'
  );

  // Compatibility with the original single-checkpoint data.
  if (
    Number.isInteger(data.message_index) &&
    typeof data.prefix_sha256 === 'string'
  ) {
    const legacy = {
      ...data,
      request_started_ms:
        Number.isFinite(data.request_started_ms)
          ? data.request_started_ms
          : Date.parse(row.updated_at || '') || 0
    };

    delete legacy.checkpoints;

    const exists = result.some(cp =>
      cp.prefix_sha256 === legacy.prefix_sha256 &&
      cp.request_context_sha256 === legacy.request_context_sha256
    );

    if (!exists) result.push(legacy);
  }

  return result;
}

function mergeCheckpoints(oldRow, incoming) {
  const now = Date.now();
  const byIdentity = new Map();

  for (const cp of [
    ...unpackCheckpoints(oldRow),
    incoming
  ]) {
    const time = checkpointTime(cp);

    if (time && now - time > HISTORY_MS) continue;

    const identity = [
      cp.prefix_sha256,
      cp.request_context_sha256 || ''
    ].join('\n');

    const existing = byIdentity.get(identity);

    if (!existing || checkpointTime(cp) > checkpointTime(existing)) {
      byIdentity.set(identity, cp);
    }
  }

  return [...byIdentity.values()]
    .sort((a, b) => checkpointTime(b) - checkpointTime(a))
    .slice(0, MAX_CHECKPOINTS);
}

async function saveCAS(session, key, incoming) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const old = await loadRow(session, key);
    const checkpoints = mergeCheckpoints(old, incoming);

    if (!checkpoints.length) return 'no_checkpoint';

    const latest = checkpoints[0];

    const oldMs = Date.parse(old?.updated_at || '');

    const updated = new Date(Math.max(
      Date.now(),
      Number.isFinite(oldMs) ? oldMs + 1 : 0
    )).toISOString();

    // Keep legacy fields at the top level.
    // New code uses the full checkpoints array.
    const payload = {
      session_id: session,
      model_key: key,
      breakpoint_data: {
        ...latest,
        schema_version: 2,
        checkpoints
      },
      updated_at: updated
    };

    let rows;

    if (old) {
      rows = await db(
        `${TABLE}?${criteria(session, key, {
          updated_at: `eq.${old.updated_at}`,
          select: 'updated_at'
        })}`,
        {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json',
            Prefer: 'return=representation'
          },
          body: JSON.stringify(payload)
        }
      );
    } else {
      rows = await db(
        `${TABLE}?on_conflict=session_id,model_key`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Prefer:
              'resolution=ignore-duplicates,return=representation'
          },
          body: JSON.stringify(payload)
        }
      );
    }

    if (Array.isArray(rows) && rows.length === 1) {
      return 'saved';
    }

    // A concurrent request changed the row.
    // Reload and merge instead of discarding a branch.
  }

  throw fail('Supabase concurrent update retries exceeded', 503);
}

async function guardedSave(session, key, incoming) {
  const queueId = `${session}\n${key}`;
  const previous = queues.get(queueId) || Promise.resolve();

  const job = previous
    .catch(() => {})
    .then(() => saveCAS(session, key, incoming));

  queues.set(queueId, job);

  try {
    return await job;
  } finally {
    if (queues.get(queueId) === job) {
      queues.delete(queueId);
    }
  }
}

/* =========================
 * Checkpoint selection
 * ========================= */

function chooseCheckpoints(body, row, indexData, config) {
  const users = [];

  for (let i = 0; i < body.messages.length; i++) {
    if (body.messages[i].role === 'user') users.push(i);
  }

  const current = users.at(-1);
  const now = Date.now();
  const saved = unpackCheckpoints(row);

  const matches = saved.filter(cp => {
    if (!Number.isInteger(current)) return false;
    if (cp.message_index >= current) return false;

    const point = indexData.points.get(cp.message_index);
    if (!point || point.prefix_sha256 !== cp.prefix_sha256) {
      return false;
    }

    const time = checkpointTime(cp);

    // This is a local eligibility estimate, not a promise
    // that the upstream cache still exists.
    return time > 0 &&
      now - time >= 0 &&
      now - time < CACHE_TTL_MS;
  });

  // Prefer the longest matching prefix.
  // For equal positions, prefer the same request configuration.
  matches.sort((a, b) =>
    b.message_index - a.message_index ||
    Number(b.request_context_sha256 === config.sha256) -
      Number(a.request_context_sha256 === config.sha256) ||
    checkpointTime(b) - checkpointTime(a)
  );

  const selected = matches[0] || null;

  let previous = selected
    ? selected.message_index
    : users.at(-2) ?? null;

  const marked = [];

  for (const i of [previous, current]) {
    if (
      Number.isInteger(i) &&
      !marked.includes(i) &&
      mark(body.messages, i)
    ) {
      marked.push(i);
    }
  }

  return {
    previous,
    current,
    marked,
    selected,
    fromState: Boolean(selected),
    stored_count: saved.length,
    matching_count: matches.length
  };
}

/* =========================
 * Usage handling
 * ========================= */

function counts(usage) {
  const details =
    usage?.prompt_tokens_details ||
    usage?.input_tokens_details ||
    {};

  const cached =
    details.cached_tokens ??
    usage?.cached_tokens;

  const written =
    details.cache_write_tokens ??
    usage?.cache_write_tokens;

  return {
    cached: Number.isFinite(cached) ? cached : null,
    written: Number.isFinite(written) ? written : null
  };
}

function mergeUsage(previous, next) {
  const result = { ...(previous || {}), ...next };

  for (const key of [
    'prompt_tokens_details',
    'input_tokens_details',
    'completion_tokens_details',
    'output_tokens_details',
    'cost_details'
  ]) {
    if (previous?.[key] || next?.[key]) {
      result[key] = {
        ...(previous?.[key] || {}),
        ...(next?.[key] || {})
      };
    }
  }

  // Merge cumulative reports; do not sum them.
  return result;
}

function usageSummary(usage) {
  const { cached, written } = counts(usage);

  return {
    prompt_tokens:
      usage?.prompt_tokens ?? usage?.input_tokens ?? null,
    completion_tokens:
      usage?.completion_tokens ?? usage?.output_tokens ?? null,
    total_tokens: usage?.total_tokens ?? null,
    cached_tokens: cached,
    cache_write_tokens: written,
    reasoning_tokens:
      usage?.completion_tokens_details?.reasoning_tokens ??
      usage?.output_tokens_details?.reasoning_tokens ??
      null,
    cost: usage?.cost ?? null,
    cost_details: usage?.cost_details ?? null
  };
}

/* =========================
 * Forwarding and stream parsing
 * ========================= */

function waitDrain(res, signal) {
  return new Promise((resolve, reject) => {
    function cleanup() {
      res.off('drain', onDrain);
      res.off('close', onClose);
      res.off('error', onError);
      signal.removeEventListener('abort', onAbort);
    }

    function onDrain() {
      cleanup();
      resolve();
    }

    function onClose() {
      cleanup();
      reject(fail('Client disconnected', 499));
    }

    function onError() {
      cleanup();
      reject(fail('Client response write failed', 499));
    }

    function onAbort() {
      cleanup();
      reject(fail('Forwarding aborted', 499));
    }

    res.once('drain', onDrain);
    res.once('close', onClose);
    res.once('error', onError);
    signal.addEventListener('abort', onAbort, { once: true });

    if (signal.aborted) onAbort();
    else if (res.destroyed) onClose();
  });
}

async function forward(req, res, path, body, trace) {
  const controller = new AbortController();

  let disconnected = false;
  let timedOut = false;

  const onClientClose = () => {
    if (!res.writableEnded) {
      disconnected = true;
      controller.abort();
    }
  };

  res.on('close', onClientClose);

  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, UPSTREAM_TIMEOUT_MS);

  timer.unref?.();

  let usage = null;
  let usageEvents = 0;
  let actualTier = null;
  let tierMismatch = false;
  let generationId = null;
  let upstreamRequestId = null;
  let upstreamStatus = null;

  let sawDone = false;
  let sawFinish = false;
  let sawError = false;
  let parseIssue = false;
  let transportEnded = false;
  let transportError = false;

  const finishReasons = new Set();

  const result = () => ({
    status: upstreamStatus,
    complete:
      transportEnded &&
      !disconnected &&
      !transportError &&
      !sawError &&
      !parseIssue &&
      (sawDone || sawFinish),
    usage,
    actualTier,
    tierMismatch,
    generationId,
    upstreamRequestId
  });

  function record(obj) {
    if (!obj || typeof obj !== 'object') return;

    if (typeof obj.id === 'string') {
      generationId = obj.id;
    }

    if (typeof obj.service_tier === 'string') {
      actualTier = obj.service_tier;

      if (
        body &&
        actualTier !== body.service_tier
      ) {
        tierMismatch = true;
      }
    }

    if (obj.error) {
      sawError = true;

      log('UPSTREAM_STREAM_ERROR', {
        trace_id: trace,
        generation_id: generationId,
        error_code: errorCode(obj.error)
      });
    }

    if (Array.isArray(obj.choices)) {
      for (const choice of obj.choices) {
        if (choice?.error) sawError = true;

        if (choice?.finish_reason != null) {
          const reason = String(choice.finish_reason);
          finishReasons.add(reason);
          sawFinish = true;

          if (reason === 'error') sawError = true;
        }
      }
    }

    if (obj.usage && typeof obj.usage === 'object') {
      usageEvents++;
      usage = mergeUsage(usage, obj.usage);

      if (DIAG && body) {
        log('CACHE_DIAG_USAGE', {
          trace_id: trace,
          generation_id: generationId,
          event_number: usageEvents,
          usage: usageSummary(obj.usage)
        });
      }
    }
  }

  let eventData = [];
  let eventType = '';
  let eventBytes = 0;
  let dropEvent = false;

  function dispatchEvent() {
    if (eventType === 'error') sawError = true;

    if (!dropEvent && eventData.length) {
      const payload = eventData.join('\n');

      if (payload.trim() === '[DONE]') {
        sawDone = true;
      } else {
        try {
          record(JSON.parse(payload));
        } catch {
          parseIssue = true;
        }
      }
    }

    eventData = [];
    eventType = '';
    eventBytes = 0;
    dropEvent = false;
  }

  function consumeLine(line) {
    if (line.endsWith('\r')) line = line.slice(0, -1);

    if (line === '') {
      dispatchEvent();
      return;
    }

    if (line.startsWith(':')) return;

    if (line.startsWith('event:')) {
      eventType = line.slice(6).trim();
      return;
    }

    if (!line.startsWith('data:') || dropEvent) return;

    const value = line.slice(5).replace(/^ /, '');
    eventBytes += Buffer.byteLength(value);

    if (eventBytes > MAX_EVENT_BYTES) {
      parseIssue = true;
      dropEvent = true;
      eventData = [];
      return;
    }

    eventData.push(value);
  }

  try {
    const response = await fetch(`${ORIGIN}${path}`, {
      method: req.method,
      headers: {
        Authorization: `Bearer ${API}`,
        'Content-Type': 'application/json'
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: controller.signal
    });

    upstreamStatus = response.status;

    upstreamRequestId =
      response.headers.get('x-request-id') ||
      response.headers.get('request-id') ||
      null;

    const contentType =
      response.headers.get('content-type') ||
      'application/json';

    const sse = contentType.includes('text/event-stream');

    const headers = {
      'Content-Type': contentType,
      'Cache-Control': 'no-store',
      'X-Proxy-Trace-Id': trace
    };

    if (sse) headers['X-Accel-Buffering'] = 'no';

    res.writeHead(response.status, headers);

    if (!response.body) {
      transportEnded = true;
      return result();
    }

    const decoder = new TextDecoder();
    let textBuffer = '';
    let jsonBytes = 0;
    let jsonTooLarge = false;
    let droppingLongLine = false;

    function feedSSE(text) {
      textBuffer += text;

      let newline;

      while ((newline = textBuffer.indexOf('\n')) >= 0) {
        const line = textBuffer.slice(0, newline);
        textBuffer = textBuffer.slice(newline + 1);

        if (droppingLongLine) {
          droppingLongLine = false;
          continue;
        }

        consumeLine(line);
      }

      if (Buffer.byteLength(textBuffer) > MAX_EVENT_BYTES) {
        parseIssue = true;
        textBuffer = '';
        droppingLongLine = true;
      }
    }

    for await (const chunk of response.body) {
      if (res.destroyed) {
        disconnected = true;
        controller.abort();
        break;
      }

      // Forward unchanged bytes to the client.
      if (!res.write(chunk)) {
        await waitDrain(res, controller.signal);
      }

      if (!body) continue;

      const text = decoder.decode(chunk, { stream: true });

      if (sse) {
        feedSSE(text);
      } else {
        jsonBytes += chunk.length;

        if (jsonBytes > MAX_JSON_RESPONSE_BYTES) {
          jsonTooLarge = true;
          textBuffer = '';
        } else if (!jsonTooLarge) {
          textBuffer += text;
        }
      }
    }

    transportEnded = !disconnected && !controller.signal.aborted;

    if (body && transportEnded) {
      const tail = decoder.decode();

      if (sse) {
        feedSSE(tail);

        if (textBuffer && !droppingLongLine) {
          consumeLine(textBuffer);
        }

        if (eventData.length || eventType) {
          dispatchEvent();
        }
      } else if (!jsonTooLarge) {
        textBuffer += tail;

        try {
          record(JSON.parse(textBuffer));
        } catch {
          parseIssue = true;
        }
      } else {
        parseIssue = true;
      }
    }

    return result();
  } catch (error) {
    transportError = true;

    log('UPSTREAM_TRANSPORT_ERROR', {
      trace_id: trace,
      upstream_status: upstreamStatus,
      generation_id: generationId,
      timeout: timedOut,
      client_disconnected: disconnected,
      error_name: error?.name || 'Error'
    });

    if (!res.headersSent && !res.destroyed) {
      json(
        res,
        timedOut ? 504 : 502,
        {
          error: timedOut
            ? 'Upstream timeout'
            : 'Upstream connection failed',
          trace_id: trace
        }
      );
    } else if (!res.destroyed && !res.writableEnded) {
      // Do not make an interrupted stream look normally finished.
      res.destroy();
    }

    return result();
  } finally {
    clearTimeout(timer);
    res.off('close', onClientClose);

    if (!res.destroyed && !res.writableEnded) {
      res.end();
    }

    if (body) {
      const final = result();

      if (DIAG || !final.complete || tierMismatch) {
        log('CACHE_DIAG_RESULT', {
          trace_id: trace,
          generation_id: generationId,
          upstream_request_id: upstreamRequestId,
          upstream_status: upstreamStatus,
          stream_completed: final.complete,
          transport_ended: transportEnded,
          done_seen: sawDone,
          finish_seen: sawFinish,
          finish_reasons: [...finishReasons],
          stream_error: sawError,
          parse_issue: parseIssue,
          timeout: timedOut,
          client_disconnected: disconnected,
          usage_found: Boolean(usage),
          usage_events: usageEvents,
          actual_service_tier: actualTier,
          route_mismatch: tierMismatch,
          ...counts(usage)
        });
      }
    }
  }
}

/* =========================
 * Request handler
 * ========================= */

const server = http.createServer(async (req, res) => {
  const trace = crypto.randomUUID();

  try {
    const path = new URL(
      req.url,
      'http://localhost'
    ).pathname;

    if (path === '/health' && req.method === 'GET') {
      return json(res, 200, {
        status: 'ok',
        version: VERSION,
        requested_prompt_cache_ttl: '30m',
        cache_mode: 'explicit',
        cache_strategy: 'matching-prefix-and-current',
        checkpoint_storage: 'multi-checkpoint-json-v2',
        max_body_mb: MAX_BODY_MB,
        max_checkpoints_per_group: MAX_CHECKPOINTS,
        checkpoint_history_hours: CHECKPOINT_HISTORY_HOURS,
        state_backend: SB && SBKEY
          ? 'supabase'
          : 'unconfigured',
        provider_routes: {
          standard: ['openai'],
          floor: ['openai/flex']
        },
        provider_fallbacks: false,
        pro_merged: false,
        diagnostics_enabled: DIAG,
        cache_prefix_normalization: 'user-text-v1'
      });
    }

    if (!API || !PASSWORD || !SB || !SBKEY) {
      return json(res, 503, {
        error: 'Missing environment variables',
        trace_id: trace
      });
    }

    if (!authorized(req)) {
      return json(res, 401, {
        error: 'Unauthorized',
        trace_id: trace
      });
    }

    if (path === '/v1/models' && req.method === 'GET') {
      await forward(req, res, '/models', null, trace);
      return;
    }
    if (path === '/search' && req.method === 'POST') {
      const { body } = await readBody(req);
      const q = typeof body?.query === 'string' ? body.query.trim() : '';
      if (!q) return json(res, 400, { error: 'query is required', trace_id: trace });

      const tv = await fetch('https://api.tavily.com/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          api_key: E.TAVILY_KEY,
          query: q,
          search_depth: 'basic',
          max_results: 3,
          include_answer: true,
          include_raw_content: false
        })
      });

      if (!tv.ok) return json(res, 502, { error: 'tavily_error', status: tv.status, trace_id: trace });

      const d = await tv.json();
      const rs = (Array.isArray(d.results) ? d.results : []).slice(0, 3).map(x => ({
        title: String(x.title || '').slice(0, 120),
        url: String(x.url || ''),
        snippet: String(x.content || '').slice(0, 300)
      }));

      return json(res, 200, {
        answer: String(d.answer || '').slice(0, 800),
        results: rs,
        trace_id: trace
      });
    }

    if (path === '/extract' && req.method === 'POST') {
      const { body } = await readBody(req);
      const u = typeof body?.url === 'string' ? body.url.trim() : '';
      if (!/^https?:\/\//i.test(u)) {
        return json(res, 400, { error: 'url is required', trace_id: trace });
      }

      async function callExtract(depth) {
        return fetch('https://api.tavily.com/extract', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            api_key: E.TAVILY_KEY,
            urls: [u],
            extract_depth: depth,
            include_images: false
          })
        });
      }

      let tv = await callExtract('basic');
      if (!tv.ok) return json(res, 502, { error: 'tavily_error', status: tv.status, trace_id: trace });

      let d = await tv.json();
      let text = (d.results && d.results[0] && d.results[0].raw_content) || '';

      if (text.length < 500) {
        const tv2 = await callExtract('advanced');
        if (tv2.ok) {
          const d2 = await tv2.json();
          const t2 = (d2.results && d2.results[0] && d2.results[0].raw_content) || '';
          if (t2.length > text.length) text = t2;
        }
      }

      if (!text) return json(res, 404, { error: 'no_content', trace_id: trace });

      const maxChars = Math.min(Math.max(Number(body?.max_chars) || 12000, 1000), 60000);
      return json(res, 200, { url: u, chars: text.length, content: text.slice(0, maxChars), max_chars: maxChars, trace_id: trace });

    }
    if (
      path !== '/v1/chat/completions' ||
      req.method !== 'POST'
    ) {
      return json(res, 404, {
        error: 'Not found',
        trace_id: trace
      });
    }

    const started = Date.now();
    const { body, bytes } = await readBody(req);

    if (
      !body ||
      typeof body !== 'object' ||
      Array.isArray(body) ||
      !Array.isArray(body.messages)
    ) {
      throw fail('messages must be an array', 400);
    }

    if (
      !body.messages.every(message =>
        message &&
        typeof message === 'object' &&
        !Array.isArray(message) &&
        typeof message.role === 'string'
      )
    ) {
      throw fail('Invalid message object', 400);
    }

    if (
      typeof body.model !== 'string' ||
      !body.model
    ) {
      throw fail('model is required', 400);
    }

    if (DIAG) {
      log('REQUEST_BODY_SIZE', {
        trace_id: trace,
        bytes,
        mib: Number((bytes / 1024 / 1024).toFixed(3)),
        limit_bytes: MAX_BODY_BYTES
      });
    }

    // Observe client parameters before overriding route/session.
    clientDiagnostics(req, body, trace);

    const floor = body.model.endsWith(':floor');

    // Preserve the original strict routing behavior.
    body.service_tier = floor ? 'flex' : 'default';

    body.provider = {
      only: floor ? ['openai/flex'] : ['openai'],
      allow_fallbacks: false
    };

    delete body.models;

    // Compute before normalization, preserving original IDs.
    const baseSession = makeSessionId(body.messages);

    body.session_id = floor
      ? `${baseSession}-flex`
      : baseSession;

    // Pro/non-Pro and standard/Flex remain separate.
    const key = JSON.stringify([
      body.model,
      body.service_tier
    ]);

    const normalized = normalize(body.messages);

    removeMarkers(body.messages);

    body.prompt_cache_options = {
      mode: 'explicit',
      ttl: '30m'
    };

    if (body.stream === true) {
      body.stream_options = {
        ...(body.stream_options || {}),
        include_usage: true
      };
    }

    const config = requestConfig(body);
    const indexData = buildPrefixIndex(body.messages);

    log('CACHE_ROUTE_PIN', {
      trace_id: trace,
      model: body.model,
      service_tier: body.service_tier,
      provider_only: body.provider.only,
      session_id: body.session_id
    });

    let row = null;

    try {
      row = await loadRow(body.session_id, key);

      log('CACHE_STATE_LOAD_OK', {
        trace_id: trace,
        found: Boolean(row),
        model: body.model,
        service_tier: body.service_tier,
        checkpoint_count: unpackCheckpoints(row).length
      });
    } catch (error) {
      log('CACHE_STATE_LOAD_ERROR', {
        trace_id: trace,
        error: error.message
      });
    }

    const selection = chooseCheckpoints(
      body,
      row,
      indexData,
      config
    );

    const {
      previous,
      current,
      marked,
      selected,
      fromState
    } = selection;

    const before = indexData.points.get(previous) || null;
    const after = indexData.points.get(current) || null;

    if (DIAG) {
      log('CACHE_DIAG_BREAKPOINT', {
        trace_id: trace,
        strategy: fromState
          ? 'matching-prefix-and-current'
          : 'previous-and-current',
        message_indexes: marked,
        previous_index: previous,
        current_index: current ?? null,
        stored_checkpoints: selection.stored_count,
        matching_checkpoints: selection.matching_count,
        provider_only: body.provider.only,
        provider_fallbacks: false
      });

      log('CACHE_PREFIX_COMPARE', {
        trace_id: trace,
        session_id: body.session_id,
        model: body.model,
        service_tier: body.service_tier,
        previous_index: previous,
        previous_message_matches_saved: fromState,
        previous_prefix_match:
          selected && before
            ? selected.prefix_sha256 === before.prefix_sha256
            : null,
        saved_prefix_available:
          Boolean(selected?.prefix_sha256),
        first_different_message_index:
          selected && before
            ? firstDiff(
                selected.prefix_message_hashes,
                prefixMessageHashes(indexData, before)
              )
            : null,
        other_request_fields_match:
          selected?.request_context_sha256
            ? selected.request_context_sha256 === config.sha256
            : null,
        changed_request_fields: changedFields(
          selected?.request_config_field_hashes,
          config.field_hashes
        ),
        normalized_user_text_messages: normalized,
        saved_checkpoint_confidence:
          selected?.checkpoint_confidence || 'none'
      });

      log('CACHE_DIAG_REQUEST', {
        trace_id: trace,
        model: body.model,
        session_id: body.session_id,
        service_tier_requested: body.service_tier,
        provider: body.provider,
        reasoning: body.reasoning ?? null,
        reasoning_effort: body.reasoning_effort ?? null,
        include_reasoning: body.include_reasoning ?? null,
        prompt_cache_options: body.prompt_cache_options,
        breakpoints: marked.map(i => ({
          message_index: i,
          part_index: indexData.points.get(i)?.part_index ?? null
        }))
      });

      log('CACHE_DIAG_PREFIX', {
        trace_id: trace,
        session_id: body.session_id,
        model: body.model,
        service_tier: body.service_tier,
        prefix_hash_algorithm: 'sha256',
        breakpoints: marked.map(i => ({
          message_index: i,
          part_index: indexData.points.get(i)?.part_index ?? null,
          prefix_without_markers_sha256:
            indexData.points.get(i)?.prefix_sha256 ?? null
        }))
      });
    }

    const result = await forward(
      req,
      res,
      '/chat/completions',
      body,
      trace
    );

    const { cached, written } = counts(result.usage);

    const good =
      result.status >= 200 &&
      result.status < 300 &&
      result.complete;

    if (result.tierMismatch) {
      log('CACHE_ROUTE_MISMATCH', {
        trace_id: trace,
        requested: body.service_tier,
        reported: result.actualTier
      });
    }

    let skipReason = null;

    if (!good) {
      skipReason = 'unsuccessful_or_incomplete';
    } else if (result.tierMismatch) {
      skipReason = 'route_mismatch';
    } else if (!result.usage) {
      skipReason = 'usage_missing';
    } else if (written === null) {
      skipReason = 'write_usage_missing';
    } else if (!(written > 0)) {
      skipReason = 'no_write';
    } else if (
      !after ||
      !marked.includes(current)
    ) {
      skipReason = 'uncacheable_current';
    }

    if (skipReason) {
      log('CACHE_STATE_SAVE_SKIPPED', {
        trace_id: trace,
        generation_id: result.generationId,
        reason: skipReason,
        cached_tokens: cached,
        cache_write_tokens: written,
        requested_current_index: current ?? null
      });

      return;
    }

    // Positive write usage is evidence that this request wrote
    // cache, but the API does not identify each written boundary.
    // Preserve the original explicit "position inferred" label.
    const checkpoint = {
      message_index: current,
      part_index: after.part_index,
      message_fingerprint: after.message_fingerprint,
      prefix_sha256: after.prefix_sha256,
      request_context_sha256: config.sha256,
      request_config_field_hashes: config.field_hashes,
      request_started_ms: started,
      last_write_observed_at: new Date().toISOString(),
      checkpoint_confidence:
        'write_observed_position_inferred',
      cache_write_tokens: written,
      cache_read_tokens: cached,
      generation_id: result.generationId
    };

    try {
      const saved = await guardedSave(
        body.session_id,
        key,
        checkpoint
      );

      log(
        saved === 'saved'
          ? 'CACHE_STATE_SAVE_OK'
          : 'CACHE_STATE_SAVE_SKIPPED',
        {
          trace_id: trace,
          reason: saved,
          model: body.model,
          service_tier: body.service_tier,
          message_index: current,
          cache_write_tokens: written,
          checkpoint_confidence:
            'write_observed_position_inferred'
        }
      );
    } catch (error) {
      // The answer has already been forwarded.
      // Do not turn a metadata-save failure into another answer.
      log('CACHE_STATE_SAVE_ERROR', {
        trace_id: trace,
        error: error.message
      });
    }
  } catch (error) {
    log('PROXY_ERROR', {
      trace_id: trace,
      status: error.status || 502,
      error: error.message,
      received_bytes: error.received_bytes ?? null,
      declared_bytes: error.declared_bytes ?? null,
      limit_bytes: error.limit_bytes ?? null
    });

    if (!res.headersSent) {
      json(res, error.status || 502, {
        error: error.message,
        trace_id: trace,
        ...(error.status === 413
          ? { limit_bytes: MAX_BODY_BYTES }
          : {})
      });
    } else if (!res.destroyed && !res.writableEnded) {
      res.destroy();
    }
  }
});

// Request upload timeout, separate from upstream inference timeout.
server.requestTimeout = 5 * 60 * 1000;
server.headersTimeout = 60 * 1000;

server.listen(PORT, '0.0.0.0', () => {
  log('PROXY_STARTED', {
    version: VERSION,
    port: PORT,
    max_body_mb: MAX_BODY_MB,
    cache_ttl: '30m',
    max_checkpoints_per_group: MAX_CHECKPOINTS,
    pro_merged: false,
    diagnostics_enabled: DIAG
  });
});
