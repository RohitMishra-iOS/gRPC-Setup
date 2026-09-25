/**
 * server.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Runs TWO services in a single Node.js process:
 *
 *  1. gRPC server      — HTTP/2, internal only (PORT or 50051)
 *  2. gRPC-Web proxy   — HTTP/1.1, public-facing (PROXY_PORT or 8080)
 *
 * Why single process?
 *   Render (and most PaaS platforms) expose exactly ONE port per service.
 *   The gRPC-Web proxy is that public port. The gRPC server runs internally
 *   on a second port that is never exposed to the internet.
 *
 * Platform clients:
 *   React / React Native (web) → PROXY_PORT  (gRPC-Web over HTTP/1.1)
 *   Android / iOS / Node       → PORT        (native gRPC over HTTP/2)
 *                                 (or also via proxy if direct access unavailable)
 */

'use strict';
require('dotenv').config();

const path        = require('path');
const http        = require('http');
const http2       = require('http2');
const grpc        = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const db          = require('./db');

// ── Config ────────────────────────────────────────────────────────────────────

// On Render, PORT is injected automatically. We use it as the gRPC-Web proxy
// port (the public-facing one). The gRPC server runs on GRPC_PORT internally.
const PROXY_PORT     = parseInt(process.env.PORT        || process.env.PROXY_PORT || '8080', 10);
const GRPC_PORT      = parseInt(process.env.GRPC_PORT   || '50051', 10);
const GRPC_HOST      = process.env.GRPC_HOST             || '0.0.0.0';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS      || '*';

const PROTO_PATH = path.join(__dirname, 'status.proto');

// ── google.protobuf.Struct ⇄ plain JS object conversion ────────────────────────
// grpc-js/proto-loader have no built-in wrapper for Struct/Value/ListValue
// (only google.protobuf.Any gets one), so requests/responses arrive and must
// be sent as the raw { fields: { key: { kind, <kind>_value } } } shape.

function valueToPlain(value) {
  if (!value) return null;
  switch (value.kind) {
    case 'numberValue': return value.numberValue;
    case 'stringValue': return value.stringValue;
    case 'boolValue':   return value.boolValue;
    case 'structValue': return structToPlain(value.structValue);
    case 'listValue':   return (value.listValue.values || []).map(valueToPlain);
    default:             return null; // nullValue, or unset
  }
}

function structToPlain(struct) {
  const out = {};
  const fields = (struct && struct.fields) || {};
  for (const key of Object.keys(fields)) {
    out[key] = valueToPlain(fields[key]);
  }
  return out;
}

function plainToValue(v) {
  if (v === null || v === undefined)  return { nullValue: 0 };
  if (typeof v === 'number')          return { numberValue: v };
  if (typeof v === 'string')          return { stringValue: v };
  if (typeof v === 'boolean')         return { boolValue: v };
  if (Array.isArray(v))               return { listValue: { values: v.map(plainToValue) } };
  if (typeof v === 'object')          return { structValue: plainToStruct(v) };
  return { nullValue: 0 };
}

function plainToStruct(obj) {
  const fields = {};
  for (const key of Object.keys(obj || {})) {
    fields[key] = plainToValue(obj[key]);
  }
  return { fields };
}

// ── 1. gRPC Server ────────────────────────────────────────────────────────────

function startGrpcServer() {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [__dirname],
  });

  const statusProto = grpc.loadPackageDefinition(packageDefinition).status;

  async function checkStatus(call, callback) {
    const body = structToPlain(call.request.message);
    console.log(`[gRPC] CheckStatus ← ${JSON.stringify(body)}`);

    const errors = validateEventsBatch(body);
    if (errors.length > 0) {
      callback(null, {
        code: 400,
        status: 'ERROR',
        message: plainToStruct({ ...body, status_code: 400, error: 'Validation failed', details: errors }),
      });
      return;
    }

    try {
      await db.saveKeyValues(body, body.batch_id);
    } catch (err) {
      console.error('[gRPC] DB save error:', err.message);
      callback(null, {
        code: 500,
        status: 'ERROR',
        message: plainToStruct({ ...body, status_code: 500, error: 'Database save failed', details: [err.message] }),
      });
      return;
    }

    console.log(`[gRPC] ✓ batch_id=${body.batch_id || 'n/a'} events=${body.events.length}`);

    callback(null, {
      code: 200,
      status: 'SUCCESS',
      message: plainToStruct({ ...body, status_code: 200 }),
    });
  }

  const server = new grpc.Server({
    'grpc.keepalive_time_ms':              10000,
    'grpc.keepalive_timeout_ms':           5000,
    'grpc.keepalive_permit_without_calls': 1,
    'grpc.http2.max_pings_without_data':   0,
    'grpc.http2.min_time_between_pings_ms': 10000,
  });

  server.addService(statusProto.StatusService.service, { CheckStatus: checkStatus });

  return new Promise((resolve, reject) => {
    server.bindAsync(
      `${GRPC_HOST}:${GRPC_PORT}`,
      grpc.ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) return reject(err);
        console.log(`✅  gRPC server        → internal port ${port}`);
        resolve(server);
      }
    );
  });
}

// ── 2. gRPC-Web Proxy ─────────────────────────────────────────────────────────

let h2session = null;

function getH2Session() {
  if (h2session && !h2session.destroyed && !h2session.closed) return h2session;

  h2session = http2.connect(`http://localhost:${GRPC_PORT}`);
  h2session.on('error', (err) => {
    console.error('[H2 Session] error:', err.message);
    h2session = null;
  });
  h2session.on('close', () => {
    h2session = null;
  });

  return h2session;
}

function setCorsHeaders(req, res) {
  const origin = req.headers['origin'] || '';
  const allow  =
    ALLOWED_ORIGINS === '*'
      ? '*'
      : ALLOWED_ORIGINS.split(',').map(o => o.trim()).includes(origin)
      ? origin
      : '';

  res.setHeader('Access-Control-Allow-Origin',   allow || '*');
  res.setHeader('Access-Control-Allow-Methods',  'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers',
    'content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization');
  res.setHeader('Access-Control-Expose-Headers',
    'grpc-status, grpc-message, trailer, te');
}

// ── Events batch ingest ───────────────────────────────────────────────────────
// Accepts an analytics-style event batch as JSON:
//   { events: [{ timestamp_ms, session_id, name, params }, ...], instance_id, ... }
// Validation: every event must have a numeric timestamp_ms and a non-blank name.

function validateEventsBatch(body) {
  if (!body || typeof body !== 'object') {
    return ['Body must be a JSON object'];
  }
  if (!Array.isArray(body.events)) {
    return ['"events" must be an array'];
  }

  const errors = [];
  body.events.forEach((event, i) => {
    if (typeof event.timestamp_ms !== 'number' || !Number.isFinite(event.timestamp_ms)) {
      errors.push(`events[${i}].timestamp_ms must be a number`);
    }
    if (typeof event.name !== 'string' || event.name.trim() === '') {
      errors.push(`events[${i}].name must not be blank`);
    }
  });

  return errors;
}

// Parses + validates a raw JSON string and echoes the same JSON back,
// merged with a status_code field (and error/details on failure).
async function processEventsBatch(rawJson) {
  let body;
  try {
    body = JSON.parse(rawJson || '{}');
  } catch (err) {
    return { statusCode: 400, payload: { status_code: 400, error: 'Invalid JSON', details: [err.message] } };
  }

  const errors = validateEventsBatch(body);
  if (errors.length > 0) {
    return { statusCode: 400, payload: { ...body, status_code: 400, error: 'Validation failed', details: errors } };
  }

  try {
    await db.saveKeyValues(body, body.batch_id);
  } catch (err) {
    return { statusCode: 500, payload: { ...body, status_code: 500, error: 'Database save failed', details: [err.message] } };
  }

  return { statusCode: 200, payload: { ...body, status_code: 200 } };
}

function handleEventsRequest(req, res) {
  const chunks = [];
  req.on('data', (chunk) => chunks.push(chunk));
  req.on('error', (err) => {
    console.error('[Events] request read error:', err.message);
    if (!res.headersSent) { res.writeHead(500); res.end(); }
  });

  req.on('end', async () => {
    const { statusCode, payload } = await processEventsBatch(Buffer.concat(chunks).toString('utf8'));

    if (statusCode === 200) {
      console.log(`[Events] ✓ batch_id=${payload.batch_id || 'n/a'} events=${payload.events.length}`);
    }

    res.writeHead(statusCode, { 'content-type': 'application/json' });
    res.end(JSON.stringify(payload));
  });
}

function handleProxyRequest(req, res) {
  setCorsHeaders(req, res);

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // ── Health check ─────────────────────────────────────────────────────────
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    return;
  }

  if (req.method !== 'POST') {
    res.writeHead(405);
    res.end('Method Not Allowed');
    return;
  }

  // ── Analytics events batch ──────────────────────────────────────────────
  if (req.url === '/events') {
    handleEventsRequest(req, res);
    return;
  }

  console.log(`[Proxy] ${req.method} ${req.url}`);

  const chunks = [];
  req.on('data', (chunk) => chunks.push(chunk));
  req.on('error', (err) => {
    console.error('[Proxy] request read error:', err.message);
    if (!res.headersSent) { res.writeHead(500); res.end(); }
  });

  req.on('end', () => {
    const bodyBuf = Buffer.concat(chunks);

    if (bodyBuf.length < 5) {
      res.writeHead(400);
      res.end('Bad Request: too short for gRPC-Web frame');
      return;
    }

    // Strip the 5-byte gRPC-Web frame header → raw protobuf
    const grpcBody  = bodyBuf.slice(5);

    // Re-wrap with gRPC length-prefix for HTTP/2
    const grpcFrame = Buffer.allocUnsafe(5 + grpcBody.length);
    grpcFrame[0]    = 0x00;
    grpcFrame.writeUInt32BE(grpcBody.length, 1);
    grpcBody.copy(grpcFrame, 5);

    let session;
    try {
      session = getH2Session();
    } catch (err) {
      console.error('[Proxy] H2 session error:', err.message);
      if (!res.headersSent) { res.writeHead(502); res.end('gRPC server unreachable'); }
      return;
    }

    const h2req = session.request({
      ':method':      'POST',
      ':path':        req.url,
      ':scheme':      'http',
      ':authority':   `localhost:${GRPC_PORT}`,
      'content-type': 'application/grpc',
      'te':           'trailers',
    });

    h2req.on('error', (err) => {
      console.error('[Proxy] H2 request error:', err.message);
      if (!res.headersSent) { res.writeHead(502); res.end(err.message); }
    });

    const respChunks = [];
    let grpcStatus   = '0';
    let grpcMessage  = '';

    h2req.on('response', (headers) => {
      if (headers['grpc-status'] !== undefined) {
        grpcStatus  = headers['grpc-status'];
        grpcMessage = headers['grpc-message'] || '';
      }
    });

    h2req.on('data',     (chunk)    => respChunks.push(chunk));
    h2req.on('trailers', (trailers) => {
      if (trailers['grpc-status'] !== undefined) {
        grpcStatus  = trailers['grpc-status'];
        grpcMessage = trailers['grpc-message'] || '';
      }
    });

    h2req.on('end', () => {
      const grpcResp = Buffer.concat(respChunks);

      // Encode gRPC-Web trailers frame (flag = 0x80)
      const trailerStr   = `grpc-status:${grpcStatus}\r\ngrpc-message:${grpcMessage}\r\n`;
      const trailerBytes = Buffer.from(trailerStr, 'utf8');
      const trailerFrame = Buffer.allocUnsafe(5 + trailerBytes.length);
      trailerFrame[0]    = 0x80;
      trailerFrame.writeUInt32BE(trailerBytes.length, 1);
      trailerBytes.copy(trailerFrame, 5);

      res.writeHead(200, {
        'content-type':          'application/grpc-web+proto',
        'x-content-type-options': 'nosniff',
      });
      res.end(Buffer.concat([grpcResp, trailerFrame]));

      console.log(`[Proxy] ✓ grpc-status: ${grpcStatus}`);
    });

    h2req.end(grpcFrame);
  });
}

function startProxy() {
  return new Promise((resolve, reject) => {
    const server = http.createServer(handleProxyRequest);

    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        reject(new Error(`Port ${PROXY_PORT} already in use. Set PORT or PROXY_PORT in .env`));
      } else {
        reject(err);
      }
    });

    server.listen(PROXY_PORT, () => {
      console.log(`✅  gRPC-Web proxy     → public  port ${PROXY_PORT}`);
      resolve(server);
    });
  });
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function main() {
  try {
    await db.connect();
    await startGrpcServer();
    await startProxy();

    console.log('');
    console.log('──────────────────────────────────────────');
    console.log('  React / React Native  →  :' + PROXY_PORT + '  (gRPC-Web)');
    console.log('  Android / iOS / Node  →  :' + GRPC_PORT  + '  (native gRPC)');
    console.log('──────────────────────────────────────────');
  } catch (err) {
    console.error('❌  Startup failed:', err.message);
    process.exit(1);
  }
}

main();
