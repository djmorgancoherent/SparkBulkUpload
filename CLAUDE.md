# Coherent Spark Bulk Upload UI

A local Node.js web app that lets users drag-and-drop Excel files and bulk-upload them to Coherent Spark services via a browser UI. It acts as a local proxy to avoid CORS and streams real-time pipeline progress (upload → compile → publish) back to the browser via Server-Sent Events (SSE).

---

## Architecture

```
browser (public/index.html)  ←SSE→  server.js (Express proxy)  →HTTP→  Coherent Spark REST API
```

- **`server.js`** — Express server with two endpoints:
  - `POST /api/list-folders` — fetches all Spark folders for a tenant
  - `POST /api/upload-stream` — runs the upload → compile → publish pipeline, streaming SSE progress events
- **`public/index.html`** — single-file SPA with a 3-step wizard: Connect → Configure → Upload
- **`test-connection.js`** — standalone CLI script to verify credentials before using the UI

## Running

```bash
npm install
node server.js            # starts on http://localhost:3000
PORT=3001 node server.js  # alternate port
```

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
{ type: 'stage', stage: 'upload'|'compile'|'publish', status: 'active'|'done', progress?: number, message: string }
{ type: 'done',  status: 'success'|'warning', versionId?: string, executeUrl?: string, message?: string }
{ type: 'error', stage?: string, message: string, code?: number }
```

## Front-end state

```js
state = {
  baseUrl:  '',
  authType: 'token' | 'apiKey',
  token:    '',
  apiKey:   '',
  folders:  [{ id, name }],
  files: [{
    id, file, serviceName, folder,
    updateVersion: false,   // false = new service (pre-flight existence check); true = update existing
    status: 'queued'|'active'|'success'|'warning'|'error',
    stageState: null,
  }],
}
```

## Feature: updateVersion checkbox

Each file in Step 2 has an "Update?" checkbox (default: unchecked = New).

- **Unchecked (New)**: server calls `GET /api/v3/folders/{folder}/services` before uploading to check if a service with that name already exists. If it does, upload is aborted with a clear error message telling the user to tick the box.
- **Checked (Update)**: skips the existence check; Spark automatically increments the version.

## Known edge cases to address

- [ ] Rate limiting (429): add retry-with-backoff
- [ ] Large files (>50 MB): pre-validate on client and show friendly error before upload
- [ ] Token expiry mid-batch: detect 401 during upload/compile/publish and surface a "refresh your token" message
- [ ] Service name conflicts on update (409): surface helpful error
- [ ] Compilation errors: surface the actual `last_error_message` from Spark
- [ ] Folder creation: currently users must pre-create folders in Spark; add a "New folder" option
- [ ] Concurrent uploads: currently sequential; could parallelise with configurable concurrency
- [ ] Progress persistence: if the page is refreshed mid-upload, state is lost
- [ ] Version bump type: currently hardcoded to `minor`; expose major/minor/patch selector
