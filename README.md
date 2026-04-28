# Coherent Spark Bulk Upload UI

A local web app for bulk-uploading Excel files to [Coherent Spark](https://coherent.global) services. Drag and drop a batch of `.xlsx` files in your browser, pick a target folder, and watch the **upload → compile → publish** pipeline progress in real time.

The Node server runs on your machine and acts as a local proxy to the Spark REST API — no CORS issues, and your tenant credentials never leave your laptop.

## Features

- **Drag-and-drop bulk upload** — queue many files at once, each with its own service name and target folder.
- **Real-time progress** — each file's upload, compile, and publish stages stream over Server-Sent Events.
- **Pre-flight name conflict check** — by default the server checks whether a service of that name already exists before uploading. Tick the *Update* box to publish a new version of an existing service instead.
- **Two auth modes** — Bearer JWT or Spark synthetic API key.
- **No setup** — single Express server, single HTML file, no build step.

## Requirements

- Node.js ≥ 18
- A Coherent Spark tenant URL and credentials (Bearer token or synthetic key)

## Quick start

```bash
npm install
node server.js                # http://localhost:3000
PORT=3001 node server.js      # alternate port
```

Then open the URL printed in the console and follow the 3-step wizard:

1. **Connect** — paste your Spark base URL and token/API key.
2. **Configure** — drop in your `.xlsx` files, set service names, pick folders, tick *Update* for existing services.
3. **Upload** — watch each file move through upload → compile → publish.

### Verifying credentials before using the UI

A standalone CLI script is included to sanity-check your credentials:

```bash
node test-connection.js --url "https://excel.uat.au.coherent.global/<tenant>" --api-key "<your-key>"
node test-connection.js --url "https://spark.uat.au.coherent.global/<tenant>"  --token   "eyJ..."
```

## Architecture

```
browser (public/index.html)  ←SSE→  server.js (Express proxy)  →HTTP→  Coherent Spark REST API
```

| File | Role |
|---|---|
| `server.js` | Express proxy. Exposes `POST /api/list-folders` and `POST /api/upload-stream`. Handles auth, multipart upload, compilation polling, and publish. |
| `public/index.html` | Single-file SPA — the 3-step wizard. |
| `test-connection.js` | Standalone credential check (no server needed). |

### Spark URL conventions

- The browser-facing host (`spark.*.coherent.global`) is automatically remapped to the API host (`excel.*.coherent.global`).
- The tenant slug is extracted from the URL path and sent as the `x-tenant-name` header on every request.
- v1 endpoints (`/api/v1/product/list`, `/api/v1/product/create`) take **no tenant in the URL path** — the path is stripped for those calls. v3 and v4 endpoints keep the tenant in the path.

### SSE event format

The server writes `data: <JSON>\n\n` events to the browser:

```js
{ type: 'stage', stage: 'upload'|'compile'|'publish', status: 'active'|'done', progress?: number, message: string }
{ type: 'done',  status: 'success'|'warning', versionId?: string, executeUrl?: string, message?: string }
{ type: 'error', stage?: string, message: string, code?: number }
```

## Security notes

- Credentials are sent from the browser to your local server on every request and forwarded to Spark — they are **not persisted** anywhere on disk.
- Uploaded files are temporarily stored in `/tmp/spark-bulk-uploads/` and removed after each request.
- The server binds to `localhost` only; do not expose it to the public internet.

## Known limitations

See [CLAUDE.md](CLAUDE.md) for the in-repo notes on Spark API quirks and the current edge-case backlog (rate-limiting, large files, mid-batch token expiry, version-bump selector, parallel uploads, etc.).

## License

Internal tooling — no license specified.
