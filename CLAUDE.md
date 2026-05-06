# Coherent Spark Bulk Upload UI

A local Node.js web app that lets users drag-and-drop Excel files and bulk-upload them to Coherent Spark services via a browser UI. It acts as a local proxy to avoid CORS and streams real-time pipeline progress (upload → compile → publish) back to the browser via Server-Sent Events (SSE).

---

## Architecture

```
browser (public/index.html)  ←SSE→  server.js (Express proxy)  →HTTP→  Coherent Spark REST API
```

- **`server.js`** — Express server with four endpoints:
  - `GET  /api/config` — returns shared client/server config (`maxFileMb`, `uploadTimeoutMs`, `compileTimeoutMs`)
  - `POST /api/list-folders` — fetches all Spark folders for a tenant
  - `POST /api/check-names` — batch existence check (uses Spark's `GET /folders/{folder}/services/{service}/exists` per item; concurrency 6)
  - `POST /api/upload-stream` — runs the upload → compile → publish pipeline, streaming SSE progress events
- **`public/index.html`** — single-file SPA with a 3-step wizard: Connect → Configure → Upload
- **`test-connection.js`** — standalone CLI script to verify credentials before using the UI

## Running

```bash
npm install
node server.js            # starts on http://localhost:3000
PORT=3001 node server.js  # alternate port
```

### Tunables (env vars)

| Var | Default | Notes |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `MAX_FILE_MB` | `300` | Per-file size limit. Multer rejects above this with a graceful SSE error; the front-end pre-flights against `GET /api/config` so users see a friendly message before attempting the upload. |
| `UPLOAD_TIMEOUT_MS` | `1800000` (30 min) | Upload POST axios timeout. Big enough that a 250 MB upload over a slow link won't time out; users see live byte progress so it never looks frozen. |
| `COMPILE_TIMEOUT_MS` | `900000` (15 min) | Total compile-polling budget. |
| `COMPILE_POLL_MS` | `3000` | Interval between compile polls. |
| `COMPILE_MAX_TRANSIENT_ERRORS` | `8` | Consecutive poll-network failures before bailing (resets on any successful poll). |
| `RETRY_MAX_ATTEMPTS` | `4` | Total attempts (incl. first try) for retryable axios calls. |
| `RETRY_BASE_MS` | `1000` | Exponential-backoff base; capped at 10s. 429 honours `Retry-After` (capped at 60s). |

## Key Spark API facts

### URL structure
- v1 endpoints (`/api/v1/product/list`, `/api/v1/product/create`) use **no tenant in the URL path** — tenant goes only in the `x-tenant-name` header. Strip the tenant segment for these calls:
  ```
  https://excel.uat.au.coherent.global/presales  →  strip to  →  https://excel.uat.au.coherent.global
  ```
- v3/v4 endpoints keep the tenant in the path: `/{tenant}/api/v3/...`
- The `spark.*` hostname is the SPA frontend; the REST API lives at `excel.*`. `normaliseUrl()` remaps automatically.

### Auth
- **Bearer token**: `Authorization: Bearer <jwt>` header only
- **API key (synthetic key)**: `x-synthetic-key: <key>` header **only** — do NOT add an Authorization header alongside it; Spark will try to validate it as a JWT and return INVALID_TOKEN
- Both always need `x-tenant-name: <tenant>` header

### Upload → Compile → Publish pipeline
1. `POST /{tenant}/api/v3/folders/{folder}/services/{service}/upload` — multipart form with `engineUploadRequestEntity` (JSON metadata) + `serviceFile` (xlsx binary). Returns `nodegen_compilation_jobid`, `original_file_documentid`, `engine_file_documentid`.
2. `GET /{tenant}/api/v3/folders/{folder}/services/{service}/getcompilationprogess/{jobId}` — note the typo (`progess` not `progress`). Poll until `response_data.progress == 100`.
3. `POST /{tenant}/api/v3/folders/{folder}/services/{service}/publish` — body contains `original_file_documentid`, `engine_file_documentid`, `draft_service_name`, `version_difference`, date range.

### Known Spark quirks
- `last_error_message` is returned as the **string `"null"`** (not JSON null) when there is no error. Check `rawError && rawError !== 'null' && rawError !== 'undefined'` before treating it as an error.
- Compilation polling: Spark may return `progress: 100` at the same poll where an error is also set. Handle progress-first.

## SSE event format

Server writes `data: <JSON>\n\n`. Event shapes:

```js
{ type: 'stage', stage: 'upload'|'compile'|'publish'|'precheck',
                 status: 'active'|'done',
                 progress?: number,           // 0–100
                 bytesLoaded?: number,        // upload only
                 bytesTotal?: number,         // upload only
                 message: string }
{ type: 'heartbeat', stage: 'compile', tsMs: number }   // keeps client alive during long compiles
{ type: 'done',  status: 'success'|'warning', versionId?: string, executeUrl?: string, message?: string }
{ type: 'error', stage?: string, message: string,
                 code?: number | 'precheck_failed' | 'name_conflict' | 'file_too_large'
                              | 'compile_timeout' | 'compile_unreachable' | 'compile_failed' | string }
```

Notes for evolving the contract:
- Additions are backwards-compatible — `handleProgressEvent()` ignores unknown event types and unknown fields.
- `code === 401` triggers the front-end's auth-expired flow (halts new files, shows toast).

## Robustness features

- **Retry helper (`withRetry`)** wraps the upload POST, publish POST, compile-poll inner GET, and pre-flight GET. Retries on 429/502/503/504 and `ECONNRESET`/`ETIMEDOUT`/`ECONNABORTED`/`EAI_AGAIN`/`EPIPE`. **401 is intentionally terminal** so it surfaces fast for the auth-expired flow. `Retry-After` headers are honoured (capped at 60s).
- **Multipart retry caveat**: the upload `FormData` and `fs.createReadStream` are rebuilt **inside** each retry attempt because a stream cannot be replayed. The temp file on disk survives across retries; cleanup happens in the outer `finally`.
- **Bounded compile polling**: `COMPILE_TIMEOUT_MS` is the wall-clock cap; `COMPILE_MAX_TRANSIENT_ERRORS` (default 8) is a guard against polling spinning forever during sustained network blips. Successful polls reset the consecutive-error counter. Heartbeat events emit on every successful poll so the client knows the server is alive even when `progress` hasn't moved.
- **Fail-safe pre-flight check**: if the existence check itself fails, the upload is aborted with `code: 'precheck_failed'` (or `code: 401` if auth). This is a deliberate change from the old "log and proceed" behaviour, which could let credential glitches silently create duplicates.
- **Multer guard**: a too-large file produces a graceful SSE `error` event with `code: 'file_too_large'` rather than an uncaught middleware exception.

## Front-end state

```js
state = {
  baseUrl:  '',
  authType: 'token' | 'apikey',
  token:    '',
  apiKey:   '',
  folders:  [{ id, name }],
  files: [{
    id, file, serviceName, folder,
    conflict: null | {                       // populated by the conflict-resolution modal
      exists: true,
      version: string | null,
      latestVersionId: string | null,
      action: 'version' | 'rename' | 'skip',
      renamedTo?: string,
    },
    status: 'queued'|'active'|'success'|'warning'|'error'|'cancelled',
    stageState: { stage, progress, message, lastEventAt } | null,
    abortController: AbortController | null,
  }],
  config: { maxFileMb: number },             // populated from GET /api/config
  batch:  {
    running:     boolean,
    aborted:     boolean,                    // user pressed "Cancel remaining"
    authExpired: boolean,                    // a 401 was seen mid-batch
    paused:      boolean,                    // user pressed "Pause"; workers wait at top of loop
  },
}
```

## Concurrency & batching

- The browser runs N workers in parallel (slider 1–6, default 4, persisted in `localStorage.sparkUploadConcurrency`). One bad file CAN'T halt the batch — `runBatch()`'s per-file try/catch catches everything.
- "Cancel remaining" sets `state.batch.aborted` and aborts only **queued** files. In-flight files finish naturally.
- A 401 anywhere in the pipeline triggers `handleAuthExpired()`: workers stop picking new files, queued files are cancelled, and a sticky toast asks the user to re-Connect with a fresh token then click Retry failed.
- The live chip bar (Queued / In progress / Done / Failed) is recomputed by `recomputeBatchSummary()` whenever a file's status changes.
- The idle-timer sweep (5s interval) renders an "idle for Ns" sub-line on any active card whose last SSE event is more than 30s old.

## Feature: conflict-resolution modal

When the user clicks Upload, the front-end first hits `POST /api/check-names`
with all `(folder, serviceName)` pairs. The server uses Spark's per-service
`/exists` endpoint to determine which already exist. If any do, a modal opens
listing each conflict and offering three actions per row:

- **Add new version to existing service** (default) — upload proceeds with the same name; Spark auto-increments the version.
- **Rename and create as new service** — `entry.serviceName` is replaced with the user's new name; the upload creates a fresh service.
- **Skip this file** — `entry.status` is set to `cancelled`; the file is filtered out before `runBatch`.

A "Quick set all to: Add new version / Skip" bulk control speeds things up for big batches.

If `/api/check-names` fails (404 on a tenant that doesn't expose `/exists`, network error, etc.), the modal is skipped and uploads proceed; Spark itself will surface a real conflict via 409 from the upload endpoint, which the existing error path handles.

`/api/v3/folders/{folder}/services/{service}/exists` returns `{ is_exists, version, latest_version_id, ... }` directly — no list-and-filter required.

## Known edge cases to address

- [x] ~~Rate limiting (429): add retry-with-backoff~~ — done in `withRetry`, honours `Retry-After`.
- [x] ~~Large files: pre-validate on client and show friendly error before upload~~ — front-end checks against `GET /api/config`.
- [x] ~~Token expiry mid-batch: detect 401 and surface a "refresh your token" message~~ — `code: 401` triggers `handleAuthExpired()`.
- [x] ~~Concurrent uploads~~ — worker pool, slider 1–6.
- [ ] Service name conflicts on update (409): surface helpful error.
- [x] ~~Compilation errors: surface the actual `last_error_message` from Spark~~ — already wired; now also passes through `error_code`.
- [ ] Folder creation: currently users must pre-create folders in Spark; add a "New folder" option.
- [ ] Progress persistence: if the page is refreshed mid-upload, state is lost.
- [ ] Version bump type: currently hardcoded to `minor`; expose major/minor/patch selector.
