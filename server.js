/**
 * Coherent Spark Bulk Upload — Local Proxy Server
 *
 * Acts as a local proxy between the browser UI and the Coherent Spark REST API,
 * eliminating CORS issues and handling the full upload → compile → publish pipeline
 * with real-time Server-Sent Events (SSE) progress streaming.
 *
 * Endpoints:
 *   POST /api/list-folders    — Fetches all Spark folders for a tenant
 *   POST /api/upload-stream   — Uploads a file and streams pipeline progress via SSE
 *
 * Usage:
 *   npm install
 *   node server.js
 *   Open http://localhost:3000
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

// ─── Middleware ───────────────────────────────────────────────────────────────

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Multer — stores uploaded files in /tmp, up to 200 MB each
const upload = multer({
  dest: '/tmp/spark-bulk-uploads/',
  limits: { fileSize: 200 * 1024 * 1024 },
});

fs.mkdirSync('/tmp/spark-bulk-uploads', { recursive: true });

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

/** Format an Axios error into a human-readable string. */
function formatAxiosError(err) {
  const code = err.response?.status;
  const body = err.response?.data;
  let msg = code ? `HTTP ${code}` : err.message;
  if (body) {
    const detail = typeof body === 'object' ? JSON.stringify(body) : String(body);
    msg += ` — ${detail.slice(0, 400)}`;
  }
  if (code === 401) msg += '\n💡 For Bearer tokens: the token may have expired — refresh it from the Spark console.\n💡 For API keys: verify the key is correct and that the API key group has the Spark.FolderList.json (or Spark.AllEncompassingProxy.json) feature permission assigned.';
  if (code === 403) msg += '\n💡 Check that your key has write access to this folder/tenant.';
  if (code === 404) msg += '\n💡 Verify the base URL, tenant name, and that the folder exists.';
  if (code === 409) msg += '\n💡 A service with this name may already exist. Try a different name.';
  return msg;
}

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

app.post('/api/upload-stream', upload.single('file'), async (req, res) => {
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
    if (tempFilePath) fs.unlink(tempFilePath, () => {});
  };

  try {
    const { baseUrl, token, apiKey, folder, serviceName, updateVersion } = req.body || {};
    const originalName  = req.file?.originalname ?? 'file.xlsx';
    const isUpdate      = updateVersion === 'true';

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
    console.log(`   Mode    : ${isUpdate ? 'update existing version' : 'create new service'}`);
    console.log(`   Auth    : ${headers.Authorization ? 'Bearer token' : headers['x-synthetic-key'] ? 'API key' : 'NONE'}`);

    // ── Pre-flight: existence check (only when creating a new service) ────────
    if (!isUpdate) {
      send({ type: 'stage', stage: 'upload', status: 'active', message: 'Checking service name…' });
      try {
        const listUrl  = `${base}/api/v3/folders/${encF}/services`;
        const listResp = await axios.get(listUrl, {
          headers: { ...headers, 'Content-Type': 'application/json' },
          timeout: 15_000,
        });
        const services = listResp.data?.data ?? listResp.data?.items ?? listResp.data?.response_data?.data ?? [];
        const exists   = Array.isArray(services)
          && services.some(s => (s.name ?? s.serviceName ?? '').toLowerCase() === serviceName.toLowerCase());

        if (exists) {
          const errMsg = `A service named "${serviceName}" already exists in folder "${folder}". ` +
                         `Tick "Update?" to add a new version to the existing service instead.`;
          console.warn(`   ⚠️  Service exists — aborting (updateVersion=false)`);
          send({ type: 'error', stage: 'upload', message: errMsg });
          res.end(); cleanup(); return;
        }
        console.log(`   ✅ Name available — proceeding to create`);
      } catch (checkErr) {
        // If the existence check fails (e.g. 404 on the services list endpoint), log it
        // but proceed rather than blocking the upload — better a potential duplicate than a hard block.
        console.warn(`   ⚠️  Could not verify service existence (${checkErr.response?.status ?? checkErr.message}) — proceeding anyway`);
      }
    }

    // ── Stage 1: Upload ──────────────────────────────────────────────────────
    send({ type: 'stage', stage: 'upload', status: 'active', message: isUpdate ? 'Uploading new version to Spark…' : 'Uploading file to Spark…' });

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

    let uploadBody;
    try {
      const uploadResp = await axios.post(uploadUrl, formData, {
        headers:          { ...headers, ...formData.getHeaders() },
        timeout:          180_000,
        maxContentLength: Infinity,
        maxBodyLength:    Infinity,
      });
      uploadBody = uploadResp.data;
      console.log(`   ✅ Upload HTTP ${uploadResp.status}`);
    } catch (err) {
      const code = err.response?.status;
      const body = err.response?.data;
      console.error(`   ❌ Upload failed — HTTP ${code ?? 'network error'}`);
      if (body) console.error(`   Response: ${JSON.stringify(body).slice(0, 500)}`);
      send({ type: 'error', stage: 'upload', message: formatAxiosError(err), code });
      res.end(); cleanup(); return;
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
      res.end(); cleanup(); return;
    }

    // ── Stage 2: Poll Compilation ────────────────────────────────────────────
    send({ type: 'stage', stage: 'compile', status: 'active', progress: 0, message: 'Waiting for compilation…' });

    const compileUrl  = `${base}/api/v3/folders/${encF}/services/${encS}/getcompilationprogess/${jobId}`;
    console.log(`   Compile : GET ${compileUrl}`);
    const compileEnd  = Date.now() + 5 * 60_000; // 5-min timeout
    let   compileProgress = 0;

    while (compileProgress < 100) {
      if (Date.now() > compileEnd) {
        send({ type: 'error', stage: 'compile', message: 'Compilation timed out after 5 minutes.' });
        res.end(); cleanup(); return;
      }

      await new Promise(r => setTimeout(r, 3_000));

      let compileBody;
      try {
        const compileResp = await axios.get(compileUrl, { headers, timeout: 30_000 });
        compileBody       = compileResp.data;
      } catch (err) {
        // Transient error — keep trying
        send({ type: 'stage', stage: 'compile', status: 'active', progress: compileProgress, message: `Compiling… (retrying after error)` });
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

      if (cError && compileProgress < 100) {
        console.error(`   ❌ Compilation failed: ${cError}`);
        send({ type: 'error', stage: 'compile', message: `Compilation error: ${cError}` });
        res.end(); cleanup(); return;
      }
    }

    send({ type: 'stage', stage: 'compile', status: 'done', progress: 100, message: 'Compilation complete' });

    // ── Stage 3: Publish ─────────────────────────────────────────────────────
    if (!origDocId || !engineDocId) {
      send({ type: 'done', status: 'warning', message: 'Missing document IDs — service compiled but could not be published.' });
      res.end(); cleanup(); return;
    }

    send({ type: 'stage', stage: 'publish', status: 'active', message: 'Publishing service…' });

    const publishUrl = `${base}/api/v3/folders/${encF}/services/${encS}/publish`;
    const publishNow = new Date().toISOString();
    console.log(`   Publish : POST ${publishUrl}`);

    let publishBody;
    try {
      const publishResp = await axios.post(
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
      publishBody = publishResp.data;
    } catch (err) {
      const code = err.response?.status;
      const body = err.response?.data;
      console.error(`   ❌ Publish failed — HTTP ${code ?? 'network error'}`);
      if (body) console.error(`   Response: ${JSON.stringify(body).slice(0, 500)}`);
      send({ type: 'error', stage: 'publish', message: formatAxiosError(err), code });
      res.end(); cleanup(); return;
    }

    const versionId  = publishBody?.response_data?.version_id;
    const executeUrl = `${base}/api/v3/folders/${encF}/services/${encS}/execute`;

    console.log(`   ✅ Published! version_id: ${versionId ?? '—'}`);
    console.log(`   🔗 Execute URL: ${executeUrl}`);
    send({ type: 'stage', stage: 'publish', status: 'done', message: 'Published!' });
    send({ type: 'done', status: 'success', versionId, executeUrl, folder, serviceName });

  } catch (err) {
    send({ type: 'error', message: err.message ?? 'Unexpected server error' });
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
  console.log('');
});
