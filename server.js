/**
 * Coherent Spark Bulk Upload — Local Proxy Server
 *
 * Acts as a local proxy between the browser UI and the Coherent Spark REST API,
 * eliminating CORS issues and handling the full upload → compile → publish pipeline
 * with real-time Server-Sent Events (SSE) progress streaming.
 *
 * Endpoints:
 *   GET  /api/config          — Returns shared client/server config (max file size, etc.)
 *   POST /api/list-folders    — Fetches all Spark folders for a tenant
 *   POST /api/upload-stream   — Uploads a file and streams pipeline progress via SSE
 *
 * Usage:
 *   npm install
 *   node server.js
 *   Open http://localhost:3000
 *
 * Tunables (all optional; sensible defaults):
 *   PORT                          - HTTP port (default 3000)
 *   MAX_FILE_MB                   - Per-file size limit, megabytes (default 300)
 *   UPLOAD_TIMEOUT_MS             - Upload POST axios timeout (default 30 min)
 *   COMPILE_TIMEOUT_MS            - Total compile-polling budget (default 15 min)
 *   COMPILE_POLL_MS               - Interval between compile polls (default 3 s)
 *   COMPILE_MAX_TRANSIENT_ERRORS  - Consecutive poll failures before bailing (default 8)
 *   RETRY_MAX_ATTEMPTS            - Total attempts (incl. first try) for retryable calls (default 4)
 *   RETRY_BASE_MS                 - Base for exponential backoff (default 1000)
 */

'use strict';

const express  = require('express');
const multer   = require('multer');
const axios    = require('axios');
const FormData = require('form-data');
const fs       = require('fs');
const path     = require('path');
const { v4: uuidv4 } = require('uuid');

const app  = express();
const PORT = process.env.PORT || 3000;

// ─── Config ───────────────────────────────────────────────────────────────────

const CONFIG = {
  MAX_FILE_MB:                  num(process.env.MAX_FILE_MB,                  300),
  UPLOAD_TIMEOUT_MS:            num(process.env.UPLOAD_TIMEOUT_MS,            30 * 60_000),
  COMPILE_TIMEOUT_MS:           num(process.env.COMPILE_TIMEOUT_MS,           15 * 60_000),
  COMPILE_POLL_MS:              num(process.env.COMPILE_POLL_MS,              3_000),
  COMPILE_MAX_TRANSIENT_ERRORS: num(process.env.COMPILE_MAX_TRANSIENT_ERRORS, 8),
  RETRY_MAX_ATTEMPTS:           num(process.env.RETRY_MAX_ATTEMPTS,           4),
  RETRY_BASE_MS:                num(process.env.RETRY_BASE_MS,                1_000),
};

function num(v, d) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
}

// ─── Middleware ───────────────────────────────────────────────────────────────

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Multer — stores uploaded files in /tmp, up to MAX_FILE_MB each
const upload = multer({
  dest: '/tmp/spark-bulk-uploads/',
  limits: { fileSize: CONFIG.MAX_FILE_MB * 1024 * 1024 },
});

fs.mkdirSync('/tmp/spark-bulk-uploads', { recursive: true });

/**
 * Wrap upload.single('file') so a multer rejection (e.g. file too large) becomes
 * a graceful SSE error instead of an uncaught exception. We open the SSE response
 * here ourselves, emit one error event, then end. Client-side pre-flight makes
 * this rare but the degraded path must still be friendly.
 */
function uploadOrSseError(field) {
  const single = upload.single(field);
  return (req, res, next) => {
    single(req, res, (err) => {
      if (!err) return next();

      const isMulter = err instanceof multer.MulterError;
      const isTooLarge = isMulter && err.code === 'LIMIT_FILE_SIZE';

      // If the response hasn't started, emit a graceful SSE error.
      if (!res.headersSent) {
        res.setHeader('Content-Type',  'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Connection',    'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        res.flushHeaders();
        res.write(`data: ${JSON.stringify({
          type: 'error',
          stage: 'upload',
          code: isTooLarge ? 'file_too_large' : 'multer_error',
          message: isTooLarge
            ? `File exceeds the ${CONFIG.MAX_FILE_MB} MB per-file limit.`
            : `Upload pre-processing failed: ${err.message}`,
        })}\n\n`);
        res.end();
      } else {
        next(err);
      }
    });
  };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Normalise Spark base URL:
 *   spark.*  →  excel.*   (browser UI hostname → REST API hostname)
 * Also strips trailing slashes.
 */
function normaliseUrl(raw) {
  try {
    const trimmed = (raw || '').trim().replace(/\/+$/, '');
    const u = new URL(trimmed);
    if (u.hostname.startsWith('spark.')) {
      u.hostname = 'excel.' + u.hostname.slice('spark.'.length);
    }
    return u.toString().replace(/\/+$/, '');
  } catch {
    return (raw || '').replace(/\/+$/, '');
  }
}

/**
 * Build a link to the Spark UI's API Tester page for a freshly-published service.
 * This is the page non-technical users should land on after an upload — it
 * lets them try the service without writing any code.
 *
 *   excel.{env}.coherent.global/{tenant}                       ← internal API host
 *   spark.{env}.coherent.global/{tenant}/products/{f}/{s}/api-tester/testing  ← UI
 *
 * Returns null if the URL can't be parsed.
 */
function buildApiTesterUrl(baseUrl, folder, serviceName) {
  try {
    const u = new URL(baseUrl);
    if (u.hostname.startsWith('excel.')) {
      u.hostname = 'spark.' + u.hostname.slice('excel.'.length);
    }
    const tenant = extractTenant(baseUrl);
    if (!tenant) return null;
    const encF = encodeURIComponent(folder);
    const encS = encodeURIComponent(serviceName);
    return `${u.protocol}//${u.host}/${tenant}/products/${encF}/${encS}/api-tester/testing`;
  } catch {
    return null;
  }
}

/** Extract tenant slug from the URL path component (last segment before any sub-paths). */
function extractTenant(baseUrl) {
  try {
    const parts = new URL(baseUrl).pathname.replace(/^\//, '').split('/');
    return parts[0] || '';
  } catch {
    return '';
  }
}

/** Build the standard Spark request headers. */
function buildHeaders(baseUrl, token, apiKey) {
  const tenant  = extractTenant(baseUrl);
  const headers = {
    Accept:          'application/json',
    'x-tenant-name': tenant,
    'x-request-id':  uuidv4(),
    'x-spark-ua':    'spark-bulk-upload-ui/1.0',
  };
  if (token) {
    const t = (token || '').trim();
    // Bearer token — do NOT set x-synthetic-key alongside it
    headers.Authorization = t.toLowerCase().startsWith('bearer ') ? t : `Bearer ${t}`;
  } else if (apiKey) {
    const k = (apiKey || '').trim();
    // Spark API key auth: ONLY x-synthetic-key + x-tenant-name (per official docs).
    // Do NOT set an Authorization header — Spark will try to parse it as a Bearer JWT
    // and return INVALID_TOKEN even when the synthetic key itself is perfectly valid.
    headers['x-synthetic-key'] = k;
  }
  return headers;
}

/**
 * Spark "additional_details" comes back as a map of error-code → context, e.g.
 *   { ENGINE_1904_DATE_SYSTEM: ["1904DATESYSTEM"], DUPLICATE_INPUT_DEFINITION: ["MyRange"] }
 *
 * The Spark UI maps each code to a human sentence client-side. We do the same
 * here for codes we can confirm; everything else falls back to a humanised
 * "Title case: offending items" line so non-technical users at least see
 * something meaningful instead of raw JSON.
 *
 * Add to this table as new codes show up in the wild — keys are exact Spark codes.
 */
const SPARK_ERROR_MESSAGES = {
  ENGINE_1904_DATE_SYSTEM:
    "'Use 1904 date system' is incompatible with Spark. Disable it in Excel — " +
    "Windows: File → Options → Advanced → uncheck 'Use 1904 date system'. " +
    "Mac: Excel → Preferences → Calculation → uncheck '1904 date system'.",
  ENGINE_PRECISION_AS_DISPLAYED:
    "'Set precision as displayed' is incompatible with Spark. Disable it in Excel's Advanced Options.",
  ENGINE_LOTUS_COMPATIBILITY:
    "Excel's 'Lotus compatibility settings: Transition formula evaluation' is incompatible with Spark. " +
    "Disable it in Excel's Advanced Options.",
};

/**
 * Convert Spark's `additional_details` object into a readable, multi-line summary.
 * Returns '' if there's nothing useful to surface.
 */
function formatSparkErrorDetails(details) {
  if (!details || typeof details !== 'object') return '';
  const lines = [];
  for (const [code, value] of Object.entries(details)) {
    const friendly = SPARK_ERROR_MESSAGES[code];

    // Filter out values that are just the key echoed back (Spark does this for
    // some codes, e.g. value: ["1904DATESYSTEM"] for ENGINE_1904_DATE_SYSTEM).
    let items = '';
    if (Array.isArray(value)) {
      const slugged = code.replace(/_/g, '');
      items = value.filter(v => typeof v === 'string' && v.toUpperCase() !== slugged).join(', ');
    } else if (typeof value === 'string') {
      items = value;
    } else if (value && typeof value === 'object') {
      items = JSON.stringify(value).slice(0, 200);
    }

    if (friendly) {
      lines.push(items ? `${friendly} (${items})` : friendly);
    } else {
      const titleCase = code.toLowerCase().replace(/_/g, ' ').replace(/^./, c => c.toUpperCase());
      lines.push(items ? `${titleCase}: ${items}` : titleCase);
    }
  }
  return lines.join('\n');
}

/**
 * Format an Axios error into a human-readable string.
 *
 * Spark error bodies follow a common shape:
 *   { status: 'Error', error: { error_category, error_type, message, additional_details } }
 *
 * We surface error.message + decoded additional_details first (those are the
 * useful parts) and fall back to a longer raw-body slice only when the
 * structured fields aren't present.
 */
function formatAxiosError(err) {
  const code = err.response?.status;
  const body = err.response?.data;
  let msg = code ? `HTTP ${code}` : err.message;

  if (body && typeof body === 'object') {
    const e        = body.error || {};
    const headline = e.message || body.message || body.errorMessage;
    const category = e.error_category;
    const errType  = e.error_type;
    const details  = e.additional_details;

    // The decoded additional_details is the most useful thing for non-technical
    // users — it surfaces the actual reason in plain English when we can map it.
    const decoded = formatSparkErrorDetails(details);

    if (decoded) {
      // Lead with the decoded explanation; keep the headline + tag as a small suffix.
      msg += `\n${decoded}`;
      const suffixBits = [];
      if (headline && headline !== 'INVALID_ENGINE_CONFIGURATION') suffixBits.push(headline);
      const tag = [category, errType].filter(Boolean).join(' / ');
      if (tag) suffixBits.push(`(${tag})`);
      if (suffixBits.length) msg += `\n${suffixBits.join(' ')}`;
    } else if (headline) {
      msg += ` — ${headline}`;
      const tag = [category, errType].filter(Boolean).join(' / ');
      if (tag && tag !== headline) msg += ` (${tag})`;
      if (typeof details === 'string') msg += ` — ${details}`;
    } else {
      // No structured fields we recognise — fall back to a longer raw-body slice.
      msg += ` — ${JSON.stringify(body).slice(0, 800)}`;
    }
  } else if (body) {
    msg += ` — ${String(body).slice(0, 800)}`;
  }

  if (code === 400) msg += '\n💡 Open the file in the Spark console upload log for the full validation report.';
  if (code === 401) msg += '\n💡 For Bearer tokens: the token may have expired — refresh it from the Spark console.\n💡 For API keys: verify the key is correct and that the API key group has the Spark.FolderList.json (or Spark.AllEncompassingProxy.json) feature permission assigned.';
  if (code === 403) msg += '\n💡 Check that your key has write access to this folder/tenant.';
  if (code === 404) msg += '\n💡 Verify the base URL, tenant name, and that the folder exists.';
  if (code === 409) msg += '\n💡 A service with this name may already exist. Try a different name.';
  return msg;
}

// ─── Retry helper ─────────────────────────────────────────────────────────────

/** Network-level errors that are worth retrying. */
const RETRYABLE_NET_CODES = new Set([
  'ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN', 'EPIPE',
]);
/** HTTP statuses that are worth retrying. 401 is intentionally NOT here so it surfaces fast. */
const RETRYABLE_STATUSES = new Set([429, 502, 503, 504]);

function isRetryable(err) {
  const status = err?.response?.status;
  if (status && RETRYABLE_STATUSES.has(status)) return true;
  if (!err?.response && err?.code && RETRYABLE_NET_CODES.has(err.code)) return true;
  return false;
}

/**
 * Parse a Retry-After header. RFC 7231 allows seconds (delta) or HTTP-date.
 * Returns ms, or null if unparseable.
 */
function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(seconds * 1000, 60_000);
  const dateMs = Date.parse(value);
  if (!Number.isNaN(dateMs)) {
    const delta = dateMs - Date.now();
    return delta > 0 ? Math.min(delta, 60_000) : 0;
  }
  return null;
}

/**
 * Run `fn(attempt)` up to `maxAttempts` times with exponential backoff.
 *
 * @param {string}   label                 - human-readable label for logs
 * @param {Function} fn                    - async function that returns the success value
 * @param {object}   opts
 * @param {number}   [opts.maxAttempts]    - default CONFIG.RETRY_MAX_ATTEMPTS
 * @param {number}   [opts.baseMs]         - default CONFIG.RETRY_BASE_MS
 * @param {Function} [opts.onAttempt]      - (attempt, delayMs, err) → void; called before each retry sleep
 */
async function withRetry(label, fn, opts = {}) {
  const maxAttempts = opts.maxAttempts ?? CONFIG.RETRY_MAX_ATTEMPTS;
  const baseMs      = opts.baseMs      ?? CONFIG.RETRY_BASE_MS;

  let attempt = 0;
  while (true) {
    attempt++;
    try {
      return await fn(attempt);
    } catch (err) {
      const exhausted = attempt >= maxAttempts;
      if (exhausted || !isRetryable(err)) throw err;

      // Honour Retry-After for 429s; otherwise exp backoff capped at 10s.
      const status = err.response?.status;
      const retryAfter = status === 429 ? parseRetryAfter(err.response?.headers?.['retry-after']) : null;
      const backoff    = Math.min(baseMs * Math.pow(2, attempt - 1), 10_000);
      const delayMs    = retryAfter ?? backoff;

      console.warn(`   ↻ ${label} attempt ${attempt}/${maxAttempts} failed (${status ?? err.code ?? err.message}); retrying in ${delayMs}ms`);
      if (typeof opts.onAttempt === 'function') {
        try { opts.onAttempt(attempt, delayMs, err); } catch { /* swallow */ }
      }
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

// ─── Route: Config ────────────────────────────────────────────────────────────

app.get('/api/config', (_req, res) => {
  res.json({
    maxFileMb:        CONFIG.MAX_FILE_MB,
    uploadTimeoutMs:  CONFIG.UPLOAD_TIMEOUT_MS,
    compileTimeoutMs: CONFIG.COMPILE_TIMEOUT_MS,
  });
});

// ─── Route: List Folders ─────────────────────────────────────────────────────

/**
 * Coherent Spark URL structure differs between API versions:
 *
 *  v1 endpoints  → NO tenant in URL path; tenant passed via x-tenant-name header only
 *                  Server: https://excel.{env}.coherent.global
 *                  Path  : /api/v1/product/list
 *
 *  v3/v4 endpoints → tenant IS in the URL path (as well as x-tenant-name header)
 *                  Path  : /{tenant}/api/v3/...
 *
 * So if the user provides https://excel.uat.au.coherent.global/presales we must
 * strip the tenant segment for v1 calls, producing:
 *   https://excel.uat.au.coherent.global/api/v1/product/list   ← correct
 *   https://excel.uat.au.coherent.global/presales/api/v3/...   ← correct
 */
async function tryListFolders(base, headers) {
  const jsonHeaders = { ...headers, 'Content-Type': 'application/json' };

  // Derive the API root by stripping the last path segment (the tenant slug).
  // e.g. https://excel.uat.au.coherent.global/presales → https://excel.uat.au.coherent.global
  const apiRoot = base.replace(/\/[^/]+\/?$/, '');

  const attempts = [
    // v1 — no tenant in path (per official OpenAPI spec server definition)
    () => axios.post(`${apiRoot}/api/v1/product/list`, { pageSize: 500 }, { headers: jsonHeaders, timeout: 15_000 }),
    // v1 — with tenant in path (some deployments route this way)
    () => axios.post(`${base}/api/v1/product/list`,    { pageSize: 500 }, { headers: jsonHeaders, timeout: 15_000 }),
    // v3 variants — tenant in path
    () => axios.post(`${base}/api/v3/product/list`,    { pageSize: 500 }, { headers: jsonHeaders, timeout: 15_000 }),
    () => axios.post(`${base}/api/v3/folders`,          { pageSize: 500 }, { headers: jsonHeaders, timeout: 15_000 }),
    () => axios.post(`${base}/api/v3/folders`,          {},                { headers: jsonHeaders, timeout: 15_000 }),
    () => axios.get(`${base}/api/v3/folders`,                              { headers, timeout: 15_000 }),
  ];

  const urls = [
    `POST ${apiRoot}/api/v1/product/list  ← no tenant in path (v1 spec)`,
    `POST ${base}/api/v1/product/list     ← tenant in path`,
    `POST ${base}/api/v3/product/list`,
    `POST ${base}/api/v3/folders {pageSize:500}`,
    `POST ${base}/api/v3/folders {}`,
    `GET  ${base}/api/v3/folders`,
  ];

  let lastErr;
  for (let i = 0; i < attempts.length; i++) {
    try {
      const resp = await attempts[i]();
      // Parse the response — shape varies across endpoint versions
      const raw     = resp.data;
      const rawList = raw?.data
                   ?? raw?.response_data?.data
                   ?? raw?.items
                   ?? raw?.folders
                   ?? (Array.isArray(raw) ? raw : []);

      const folders = rawList
        .map(f => ({ id: f.id ?? f.folderId, name: f.name ?? f.folderName }))
        .filter(f => f.name)
        .sort((a, b) => a.name.localeCompare(b.name));

      console.log(`✅ Folders fetched via ${urls[i]} (${folders.length} found)`);
      return { folders, endpoint: urls[i] };
    } catch (err) {
      const code = err.response?.status;
      console.log(`   ↳ ${urls[i]} → ${code ?? err.message}`);
      // Stop immediately on auth errors; keep trying on 404/405 (wrong path or method)
      if (code === 401 || code === 403) throw err;
      lastErr = err;
    }
  }
  throw lastErr;
}

// ─── Route: Check Names (batch) ───────────────────────────────────────────────

/**
 * Batch existence check for service names. Used by the front-end before kicking
 * off a bulk upload — surfaces conflicts upfront in a single round so the user
 * can decide per file whether to add a version, rename, or skip.
 *
 * Request body:
 *   {
 *     baseUrl: string,
 *     token?: string,
 *     apiKey?: string,
 *     items:  [{ id, folder, serviceName }, ...]
 *   }
 *
 * Response:
 *   {
 *     success: true,
 *     results: [{ id, folder, serviceName, exists, version?, latestVersionId? }, ...],
 *     unsupported?: true   // present if the /exists endpoint isn't available on this tenant
 *   }
 *
 * Calls run with limited concurrency to avoid hammering Spark for big batches.
 */
app.post('/api/check-names', async (req, res) => {
  const { baseUrl, token, apiKey, items } = req.body || {};
  if (!baseUrl)            return res.status(400).json({ success: false, error: 'baseUrl is required' });
  if (!Array.isArray(items)) return res.status(400).json({ success: false, error: 'items must be an array' });

  const base    = normaliseUrl(baseUrl);
  const headers = buildHeaders(base, token, apiKey);

  const checkOne = async (item) => {
    const encF = encodeURIComponent(item.folder);
    const encS = encodeURIComponent(item.serviceName);
    const url  = `${base}/api/v3/folders/${encF}/services/${encS}/exists`;
    try {
      const r = await withRetry('check-exists',
        () => axios.get(url, { headers, timeout: 15_000 }),
        { maxAttempts: 2 });
      const rd = r.data?.response_data ?? {};
      return {
        id: item.id, folder: item.folder, serviceName: item.serviceName,
        exists:          !!rd.is_exists,
        version:         rd.version          ?? null,
        latestVersionId: rd.latest_version_id ?? null,
      };
    } catch (err) {
      const status = err.response?.status;
      // 401/403 are auth problems — let the caller handle those.
      if (status === 401 || status === 403) {
        const e = new Error(formatAxiosError(err));
        e.status = status;
        throw e;
      }
      // 404 from a misbehaving tenant or service-not-found → treat as "doesn't exist".
      // The actual upload will surface a real conflict via Spark's 409 if there is one.
      return {
        id: item.id, folder: item.folder, serviceName: item.serviceName,
        exists: false, version: null, latestVersionId: null,
        checkFailed: true, checkStatus: status ?? err.code ?? 'network',
      };
    }
  };

  // Limited concurrency — don't hammer Spark with 50 simultaneous requests.
  const CONCURRENCY = 6;
  const results = new Array(items.length);
  let nextIdx = 0;
  try {
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, items.length) }, async () => {
      while (true) {
        const i = nextIdx++;
        if (i >= items.length) return;
        results[i] = await checkOne(items[i]);
      }
    }));
    res.json({ success: true, results });
  } catch (err) {
    const status = err.status ?? 500;
    res.status(status).json({ success: false, error: err.message ?? String(err) });
  }
});

app.post('/api/list-folders', async (req, res) => {
  const { baseUrl, token, apiKey } = req.body || {};
  if (!baseUrl) {
    return res.status(400).json({ success: false, error: 'baseUrl is required' });
  }

  const normalised = normaliseUrl(baseUrl);
  const headers    = buildHeaders(normalised, token, apiKey);

  console.log(`\n🔍 Listing folders for: ${normalised}`);

  try {
    const { folders, endpoint } = await tryListFolders(normalised, headers);
    res.json({ success: true, folders, total: folders.length, endpoint });
  } catch (err) {
    const status  = err.response?.status ?? 500;
    const message = formatAxiosError(err);
    console.error('❌ List folders failed:', message);
    res.status(status).json({ success: false, error: message });
  }
});

// ─── Route: Upload Stream (SSE) ──────────────────────────────────────────────

app.post('/api/upload-stream', uploadOrSseError('file'), async (req, res) => {
  // Switch response to Server-Sent Events
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // disable nginx buffering if present
  res.flushHeaders();

  // Disable Nagle's algorithm so each SSE write is sent immediately as a TCP packet
  // rather than being buffered. This is critical for real-time progress updates.
  if (req.socket) req.socket.setNoDelay(true);

  const tempFilePath = req.file?.path;

  /** Write one SSE event as a JSON payload, then immediately flush the socket. */
  const send = (obj) => {
    if (!res.writableEnded && !res.destroyed) {
      res.write(`data: ${JSON.stringify(obj)}\n\n`);
      // If compression middleware is active, flush it; otherwise this is a no-op.
      if (typeof res.flush === 'function') res.flush();
    }
  };

  /** Clean up temp file on disk. */
  const cleanup = () => {
    if (!tempFilePath) return;
    fs.unlink(tempFilePath, (err) => {
      if (err && err.code !== 'ENOENT') {
        console.warn(`   cleanup: failed to unlink ${tempFilePath}: ${err.message}`);
      }
    });
  };

  /** Map an axios/Spark error to a code we can include in error SSE events. */
  const errorCode = (err) => {
    if (err?.response?.status) return err.response.status;
    if (err?.code) return err.code;
    return undefined;
  };

  try {
    const { baseUrl, token, apiKey, folder, serviceName } = req.body || {};
    const originalName  = req.file?.originalname ?? 'file.xlsx';

    if (!req.file) {
      send({ type: 'error', message: 'No file received by server.' });
      res.end(); return;
    }
    if (!folder)      { send({ type: 'error', message: 'folder is required.'      }); res.end(); return; }
    if (!serviceName) { send({ type: 'error', message: 'serviceName is required.' }); res.end(); return; }

    const base    = normaliseUrl(baseUrl);
    const headers = buildHeaders(base, token, apiKey);
    const encF    = encodeURIComponent(folder);
    const encS    = encodeURIComponent(serviceName);

    const fileSizeKb = req.file.size ? `${(req.file.size / 1024).toFixed(1)} KB` : 'size unknown';
    console.log(`\n📤 Upload request received`);
    console.log(`   File    : ${originalName} (${fileSizeKb})`);
    console.log(`   Folder  : ${folder}`);
    console.log(`   Service : ${serviceName}`);
    console.log(`   Auth    : ${headers.Authorization ? 'Bearer token' : headers['x-synthetic-key'] ? 'API key' : 'NONE'}`);

    // (Existence check happens upfront via POST /api/check-names — see public/index.html.
    // By the time we reach this point, the user has already decided per file whether
    // to add a version, rename, or skip. The serviceName here is the resolved one.)

    // ── Stage 1: Upload ──────────────────────────────────────────────────────
    send({ type: 'stage', stage: 'upload', status: 'active', message: 'Uploading file to Spark…' });

    const uploadUrl = `${base}/api/v3/folders/${encF}/services/${encS}/upload`;
    const nowIso    = new Date().toISOString();

    console.log(`   POST    : ${uploadUrl}`);

    const metadata = JSON.stringify({
      request_data: {
        version_difference:   'minor',
        effective_start_date: nowIso,
        effective_end_date:   '2099-12-31T00:00:00.000Z',
      },
    });

    // Throttle onUploadProgress events to ~5/sec so a 250 MB upload doesn't
    // flood the SSE channel with thousands of progress events.
    let lastProgressEmit = 0;
    const PROGRESS_THROTTLE_MS = 200;

    let uploadBody;
    try {
      const uploadResp = await withRetry('upload', (attempt) => {
        // FormData and the underlying read stream cannot be replayed across retries —
        // rebuild them inside the closure so each attempt gets a fresh stream.
        const formData = new FormData();
        formData.append('engineUploadRequestEntity', metadata);
        formData.append(
          'serviceFile',
          fs.createReadStream(tempFilePath),
          {
            filename:    originalName,
            contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          },
        );
        if (attempt > 1) {
          send({ type: 'stage', stage: 'upload', status: 'active',
                 message: `Retrying upload (attempt ${attempt}/${CONFIG.RETRY_MAX_ATTEMPTS})…` });
          lastProgressEmit = 0;
        }
        return axios.post(uploadUrl, formData, {
          headers:          { ...headers, ...formData.getHeaders() },
          timeout:          CONFIG.UPLOAD_TIMEOUT_MS,
          maxContentLength: Infinity,
          maxBodyLength:    Infinity,
          onUploadProgress: (e) => {
            const total = e.total || req.file.size || 0;
            if (!total) return;
            const now = Date.now();
            const isFinal = e.loaded >= total;
            if (!isFinal && now - lastProgressEmit < PROGRESS_THROTTLE_MS) return;
            lastProgressEmit = now;
            const pct = Math.round((e.loaded / total) * 100);
            send({
              type: 'stage', stage: 'upload', status: 'active',
              progress: pct,
              bytesLoaded: e.loaded,
              bytesTotal:  total,
              message: `Uploading… ${pct}%`,
            });
          },
        });
      });
      uploadBody = uploadResp.data;
      console.log(`   ✅ Upload HTTP ${uploadResp.status}`);
    } catch (err) {
      const code = errorCode(err);
      const body = err.response?.data;
      console.error(`   ❌ Upload failed — HTTP ${code ?? 'network error'}`);
      if (body) console.error(`   Response: ${JSON.stringify(body).slice(0, 500)}`);
      send({ type: 'error', stage: 'upload', message: formatAxiosError(err), code });
      res.end(); return;
    }

    // Log the FULL raw upload response so we can diagnose any field-name surprises
    console.log(`   Raw upload response: ${JSON.stringify(uploadBody).slice(0, 1000)}`);

    const rd          = uploadBody?.response_data ?? {};
    const rm          = uploadBody?.response_meta  ?? {};
    const jobId       = rd.nodegen_compilation_jobid;
    const origDocId   = rd.original_file_documentid;
    const engineDocId = rd.engine_file_documentid;

    console.log(`   response_data keys : ${Object.keys(rd).join(', ') || '(empty)'}`);
    console.log(`   response_meta keys : ${Object.keys(rm).join(', ') || '(empty)'}`);
    console.log(`   jobId       : ${jobId ?? 'MISSING'}`);
    console.log(`   origDocId   : ${origDocId ?? 'MISSING'}`);
    console.log(`   engineDocId : ${engineDocId ?? 'MISSING'}`);

    send({
      type:    'stage',
      stage:   'upload',
      status:  'done',
      progress: 100,
      message: 'File uploaded successfully',
      meta: {
        sheets:  rd.no_of_sheets,
        inputs:  rd.no_of_inputs,
        outputs: rd.no_of_outputs,
      },
    });

    if (!jobId) {
      console.warn('   ⚠️  No compilation job ID — cannot compile or publish.');
      send({ type: 'done', status: 'warning', message: 'No compilation job ID returned — check Spark console.' });
      res.end(); return;
    }

    // ── Stage 2: Poll Compilation ────────────────────────────────────────────
    send({ type: 'stage', stage: 'compile', status: 'active', progress: 0, message: 'Waiting for compilation…' });

    const compileUrl  = `${base}/api/v3/folders/${encF}/services/${encS}/getcompilationprogess/${jobId}`;
    console.log(`   Compile : GET ${compileUrl}`);
    const compileEnd            = Date.now() + CONFIG.COMPILE_TIMEOUT_MS;
    let   compileProgress       = 0;
    let   consecutiveTransients = 0;

    while (compileProgress < 100) {
      if (Date.now() > compileEnd) {
        send({ type: 'error', stage: 'compile', code: 'compile_timeout',
               message: `Compilation timed out after ${Math.round(CONFIG.COMPILE_TIMEOUT_MS / 60_000)} minutes.` });
        res.end(); return;
      }

      await new Promise(r => setTimeout(r, CONFIG.COMPILE_POLL_MS));

      let compileBody;
      try {
        const compileResp = await withRetry('compile-poll',
          () => axios.get(compileUrl, { headers, timeout: 30_000 }),
          { maxAttempts: 2 });
        compileBody = compileResp.data;
        consecutiveTransients = 0; // reset on any successful poll
      } catch (err) {
        // 401 mid-compile is terminal — surface so the client can halt the batch.
        if (err.response?.status === 401) {
          console.error(`   ❌ Compile poll auth failed — surfacing 401`);
          send({ type: 'error', stage: 'compile', code: 401,
                 message: `Authentication failed during compilation: ${formatAxiosError(err)}` });
          res.end(); return;
        }
        consecutiveTransients++;
        const remaining = CONFIG.COMPILE_MAX_TRANSIENT_ERRORS - consecutiveTransients;
        console.warn(`   ↻ Compile poll transient error (${consecutiveTransients}/${CONFIG.COMPILE_MAX_TRANSIENT_ERRORS}): ${err.message}`);
        if (consecutiveTransients > CONFIG.COMPILE_MAX_TRANSIENT_ERRORS) {
          send({ type: 'error', stage: 'compile', code: 'compile_unreachable',
                 message: `Compilation polling failed ${consecutiveTransients} consecutive times: ${formatAxiosError(err)}` });
          res.end(); return;
        }
        send({ type: 'stage', stage: 'compile', status: 'active', progress: compileProgress,
               message: `Compiling… (transient error, ${remaining} retries left)` });
        continue;
      }

      const crd        = compileBody?.response_data ?? {};
      compileProgress  = crd.progress ?? 0;
      const rawError   = crd.last_error_message;
      // Spark sometimes returns the string "null" (not JSON null) to mean "no error".
      // Treat "null", "undefined", empty string, and actual null/undefined as no error.
      const cError = (rawError && rawError !== 'null' && rawError !== 'undefined')
        ? rawError : null;

      console.log(`   ⚙️  Compile: ${compileProgress}%${cError ? ` — ERROR: ${cError}` : ''}`);
      send({ type: 'stage', stage: 'compile', status: 'active', progress: compileProgress, message: `Compiling: ${compileProgress}%` });
      // Heartbeat lets the client know the server is alive even when progress hasn't moved.
      send({ type: 'heartbeat', stage: 'compile', tsMs: Date.now() });

      if (cError && compileProgress < 100) {
        console.error(`   ❌ Compilation failed: ${cError}`);
        const errCode = crd.error_code ?? rm?.error_code ?? 'compile_failed';
        send({ type: 'error', stage: 'compile', code: errCode, message: `Compilation error: ${cError}` });
        res.end(); return;
      }
    }

    send({ type: 'stage', stage: 'compile', status: 'done', progress: 100, message: 'Compilation complete' });

    // ── Stage 3: Publish ─────────────────────────────────────────────────────
    if (!origDocId || !engineDocId) {
      send({ type: 'done', status: 'warning', message: 'Missing document IDs — service compiled but could not be published.' });
      res.end(); return;
    }

    send({ type: 'stage', stage: 'publish', status: 'active', message: 'Publishing service…' });

    const publishUrl = `${base}/api/v3/folders/${encF}/services/${encS}/publish`;
    const publishNow = new Date().toISOString();
    console.log(`   Publish : POST ${publishUrl}`);

    let publishBody;
    try {
      const publishResp = await withRetry('publish', (attempt) => {
        if (attempt > 1) {
          send({ type: 'stage', stage: 'publish', status: 'active',
                 message: `Retrying publish (attempt ${attempt}/${CONFIG.RETRY_MAX_ATTEMPTS})…` });
        }
        return axios.post(
          publishUrl,
          {
            request_data: {
              original_file_documentid: origDocId,
              engine_file_documentid:   engineDocId,
              draft_service_name:       serviceName,
              version_difference:       'minor',
              effective_start_date:     publishNow,
              effective_end_date:       '2099-12-31T00:00:00.000Z',
            },
          },
          { headers: { ...headers, 'Content-Type': 'application/json' }, timeout: 60_000 },
        );
      });
      publishBody = publishResp.data;
    } catch (err) {
      const code = errorCode(err);
      const body = err.response?.data;
      console.error(`   ❌ Publish failed — HTTP ${code ?? 'network error'}`);
      if (body) console.error(`   Response: ${JSON.stringify(body).slice(0, 500)}`);
      send({ type: 'error', stage: 'publish', message: formatAxiosError(err), code });
      res.end(); return;
    }

    const versionId    = publishBody?.response_data?.version_id;
    const executeUrl   = `${base}/api/v3/folders/${encF}/services/${encS}/execute`;
    const apiTesterUrl = buildApiTesterUrl(base, folder, serviceName);

    console.log(`   ✅ Published! version_id: ${versionId ?? '—'}`);
    console.log(`   🔗 Execute URL  : ${executeUrl}`);
    if (apiTesterUrl) console.log(`   🧪 API Tester   : ${apiTesterUrl}`);
    send({ type: 'stage', stage: 'publish', status: 'done', message: 'Published!' });
    send({ type: 'done', status: 'success', versionId, executeUrl, apiTesterUrl, folder, serviceName });

  } catch (err) {
    const code = errorCode(err);
    send({ type: 'error', code, message: err.message ?? 'Unexpected server error' });
  } finally {
    cleanup();
    if (!res.writableEnded) res.end();
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║   Coherent Spark Bulk Upload UI                  ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║   Open: http://localhost:${PORT}                    ║`);
  console.log('╚══════════════════════════════════════════════════╝');
  console.log(`   Max file size : ${CONFIG.MAX_FILE_MB} MB`);
  console.log(`   Compile cap   : ${Math.round(CONFIG.COMPILE_TIMEOUT_MS / 60_000)} min`);
  console.log(`   Retry         : up to ${CONFIG.RETRY_MAX_ATTEMPTS} attempts (base ${CONFIG.RETRY_BASE_MS}ms)`);
  console.log('');
});
