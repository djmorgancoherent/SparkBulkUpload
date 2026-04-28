#!/usr/bin/env node
/**
 * Coherent Spark — Connection Test
 * Run this to verify your credentials work before starting the UI server.
 *
 * Usage:
 *   node test-connection.js --url "https://excel.uat.au.coherent.global/presales" --api-key "your-key"
 *   node test-connection.js --url "https://spark.uat.au.coherent.global/presales"  --token "eyJ..."
 */

'use strict';

const https = require('https');
const { URL } = require('url');

// ── Parse CLI args ────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const get  = (flag) => { const i = args.indexOf(flag); return i !== -1 ? args[i + 1] : null; };

const rawUrl = get('--url');
const token  = get('--token');
const apiKey = get('--api-key');

if (!rawUrl || (!token && !apiKey)) {
  console.error('Usage:');
  console.error('  node test-connection.js --url "https://excel.uat.au.coherent.global/tenant" --api-key "your-key"');
  console.error('  node test-connection.js --url "https://spark.uat.au.coherent.global/tenant"  --token  "eyJ..."');
  process.exit(1);
}

// ── URL normalisation ─────────────────────────────────────────────────────────
function normalise(raw) {
  const trimmed = raw.trim().replace(/\/+$/, '');
  const u = new URL(trimmed);
  if (u.hostname.startsWith('spark.')) {
    console.log(`ℹ️  Remapping spark.* → excel.*`);
    u.hostname = 'excel.' + u.hostname.slice('spark.'.length);
  }
  return u.toString().replace(/\/+$/, '');
}

function extractTenant(base) {
  return new URL(base).pathname.replace(/^\//, '').split('/')[0] || '';
}

// ── Build headers ─────────────────────────────────────────────────────────────
const base   = normalise(rawUrl);
const tenant = extractTenant(base);

const headers = {
  'Accept':          'application/json',
  'Content-Type':    'application/json',
  'x-tenant-name':   tenant,
  'x-request-id':    `test-${Date.now()}`,
  'x-spark-ua':      'spark-bulk-upload-test/1.0',
};

if (token) {
  const t = token.trim();
  headers['Authorization'] = t.startsWith('Bearer ') ? t : `Bearer ${t}`;
} else {
  headers['x-synthetic-key'] = apiKey.trim();
}

// ── Print what we're sending ──────────────────────────────────────────────────
console.log('\n──────────────────────────────────────────────');
console.log('  Coherent Spark — Connection Test');
console.log('──────────────────────────────────────────────');
console.log(`  Base URL : ${base}`);
console.log(`  Tenant   : ${tenant}`);
if (token)  console.log(`  Auth     : Bearer token (${token.length} chars)`);
if (apiKey) console.log(`  Auth     : x-synthetic-key (${apiKey.length} chars, starts: ${apiKey.slice(0,6)}...)`);
console.log('');

// ── Make the request ──────────────────────────────────────────────────────────
function post(path, body) {
  return new Promise((resolve, reject) => {
    const url  = new URL(base + path);
    const data = JSON.stringify(body);

    const req = https.request({
      hostname: url.hostname,
      port:     443,
      path:     url.pathname + url.search,
      method:   'POST',
      headers:  { ...headers, 'Content-Length': Buffer.byteLength(data) },
    }, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(raw); } catch { parsed = raw; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// ── Run tests ─────────────────────────────────────────────────────────────────
async function run() {
  const candidates = [
    { method: 'POST', path: '/api/v1/product/list', body: { pageSize: 5 } },
    { method: 'POST', path: '/api/v3/product/list', body: { pageSize: 5 } },
    { method: 'POST', path: '/api/v3/folders',      body: { pageSize: 5 } },
  ];

  let anySuccess = false;

  for (const c of candidates) {
    process.stdout.write(`  Testing POST ${base}${c.path} ... `);
    try {
      const r = await post(c.path, c.body);
      if (r.status >= 200 && r.status < 300) {
        console.log(`✅ ${r.status} OK`);
        const list = r.body?.data ?? r.body?.items ?? r.body?.folders ?? [];
        console.log(`     Folders found: ${Array.isArray(list) ? list.length : '?'}`);
        if (Array.isArray(list) && list.length > 0) {
          console.log('     Sample folders:');
          list.slice(0, 5).forEach(f => console.log(`       • ${f.name}`));
        }
        anySuccess = true;
        break;
      } else {
        console.log(`❌ ${r.status}`);
        const msg = typeof r.body === 'object' ? JSON.stringify(r.body) : r.body;
        console.log(`     ${String(msg).slice(0, 200)}`);
        if (r.status === 401) {
          console.log('');
          console.log('  ⚠️  401 INVALID_TOKEN — common causes:');
          console.log('     • API key pasted incorrectly (extra spaces or partial copy)');
          console.log('     • Key generated in a different environment (e.g. production key used on UAT)');
          console.log('     • API key group lacks Spark.FolderList.json / Spark.AllEncompassingProxy.json permission');
          console.log('     • Key has been revoked — check Spark → Options → API Keys');
        }
      }
    } catch (err) {
      console.log(`❌ ${err.message}`);
    }
  }

  console.log('');
  if (anySuccess) {
    console.log('✅ Connection successful — you can now run: node server.js');
  } else {
    console.log('❌ All endpoints failed. Check the errors above.');
    console.log('');
    console.log('Next steps:');
    console.log('  1. Confirm the URL and tenant name are correct');
    console.log('  2. Verify the API key in Spark → Options → API Keys');
    console.log('  3. Ensure the key\'s group has Spark.FolderList.json permission');
    console.log('     (Spark → Options → Features permissions)');
  }
  console.log('──────────────────────────────────────────────\n');
}

run().catch(err => { console.error(err); process.exit(1); });
