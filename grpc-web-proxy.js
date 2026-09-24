/**
 * gRPC-Web Proxy
 * ─────────────────────────────────────────────────────────────────────────────
 * Manually bridges gRPC-Web (HTTP/1.1 from browsers) → native gRPC (HTTP/2).
 *
 * Why not http-proxy-middleware?
 *   Standard HTTP proxies forward HTTP/1.1 → HTTP/1.1. The gRPC server speaks
 *   raw HTTP/2 frames. Sending HTTP/1.1 to it causes "Parse Error: Expected HTTP/"
 *   So we use Node's built-in `http2` module to speak directly to the server.
 *
 * Client compatibility:
 *  ✅  React (web)       → proxy port 8080  (gRPC-Web over HTTP/1.1)
 *  ✅  React Native      → proxy port 8080  (gRPC-Web over HTTP/1.1)
 *  ✅  Android           → port 50051 direct (native gRPC / HTTP/2)
 *  ✅  iOS               → port 50051 direct (native gRPC / HTTP/2)
 *  ✅  Node.js           → port 50051 direct (native gRPC / HTTP/2)
 */

'use strict';
require('dotenv').config();

const http   = require('http');
const http2  = require('http2');

const GRPC_PORT  = parseInt(process.env.GRPC_PORT  || '50051', 10);
const PROXY_PORT = parseInt(process.env.PROXY_PORT || '8080',  10);
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS || '*';

// ── Reusable HTTP/2 client session to the gRPC server ────────────────────────

let h2session = null;

function getH2Session() {
  if (h2session && !h2session.destroyed && !h2session.closed) return h2session;

  h2session = http2.connect(`http://localhost:${GRPC_PORT}`);

  h2session.on('error', (err) => {
    console.error('[H2 Session] error:', err.message);
    h2session = null;
  });
  h2session.on('close', () => {
    console.warn('[H2 Session] closed — will reconnect on next request');
    h2session = null;
  });

  return h2session;
}

// ── CORS headers helper ───────────────────────────────────────────────────────

function setCorsHeaders(req, res) {
  const origin = req.headers['origin'] || '';
  const allow  = ALLOWED_ORIGINS === '*' ? '*' : (
    ALLOWED_ORIGINS.split(',').includes(origin) ? origin : ''
  );

  res.setHeader('Access-Control-Allow-Origin',   allow || '*');
  res.setHeader('Access-Control-Allow-Methods',  'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers',
    'content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization');
  res.setHeader('Access-Control-Expose-Headers',
    'grpc-status, grpc-message, trailer, te');
}

// ── Main request handler ──────────────────────────────────────────────────────

function handleRequest(req, res) {
  setCorsHeaders(req, res);

  // Preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  if (req.method !== 'POST') {
    res.writeHead(405);
    res.end('Method Not Allowed');
    return;
  }

  console.log(`[Proxy] ${req.method} ${req.url}`);

  // Collect the full request body from the browser
  const chunks = [];
  req.on('data', (chunk) => chunks.push(chunk));
  req.on('error', (err) => {
    console.error('[Proxy] request read error:', err.message);
    res.writeHead(500);
    res.end();
  });

  req.on('end', () => {
    const bodyBuf = Buffer.concat(chunks);

    // gRPC-Web frames are prefixed with 5 bytes: [flags(1)][length(4)]
    // Strip the 5-byte header to get raw protobuf bytes for gRPC
    if (bodyBuf.length < 5) {
      res.writeHead(400);
      res.end('Bad Request: too short for gRPC-Web frame');
      return;
    }

    const grpcBody = bodyBuf.slice(5); // raw protobuf message

    // Build the 5-byte gRPC length-prefix for HTTP/2
    const grpcFrame = Buffer.allocUnsafe(5 + grpcBody.length);
    grpcFrame[0] = 0x00;                              // no compression
    grpcFrame.writeUInt32BE(grpcBody.length, 1);      // message length
    grpcBody.copy(grpcFrame, 5);

    let session;
    try {
      session = getH2Session();
    } catch (err) {
      console.error('[Proxy] could not get H2 session:', err.message);
      res.writeHead(502);
      res.end('Bad Gateway: gRPC server unreachable');
      return;
    }

    // Forward as a native gRPC call over HTTP/2
    const h2req = session.request({
      ':method':       'POST',
      ':path':         req.url,
      ':scheme':       'http',
      ':authority':    `localhost:${GRPC_PORT}`,
      'content-type':  'application/grpc',
      'te':            'trailers',
    });

    h2req.on('error', (err) => {
      console.error('[Proxy] H2 request error:', err.message);
      if (!res.headersSent) {
        res.writeHead(502);
        res.end('Bad Gateway: ' + err.message);
      }
    });

    // Collect the gRPC response
    const respChunks = [];
    let grpcStatus  = '0';
    let grpcMessage = '';

    h2req.on('response', (headers) => {
      // gRPC status can come in response headers (unary) or trailers
      if (headers['grpc-status'] !== undefined) {
        grpcStatus  = headers['grpc-status'];
        grpcMessage = headers['grpc-message'] || '';
      }
    });

    h2req.on('data', (chunk) => respChunks.push(chunk));

    h2req.on('trailers', (trailers) => {
      if (trailers['grpc-status'] !== undefined) {
        grpcStatus  = trailers['grpc-status'];
        grpcMessage = trailers['grpc-message'] || '';
      }
    });

    h2req.on('end', () => {
      const grpcResp = Buffer.concat(respChunks);

      // gRPC response is also length-prefixed — pass it straight through
      // as a gRPC-Web data frame (same 5-byte prefix format)
      const dataFrame = grpcResp.length > 0 ? grpcResp : Buffer.alloc(0);

      // Encode gRPC-Web trailers frame: flag=0x80, then "grpc-status:X\r\n..."
      const trailerStr = `grpc-status:${grpcStatus}\r\ngrpc-message:${grpcMessage}\r\n`;
      const trailerBytes = Buffer.from(trailerStr, 'utf8');
      const trailerFrame = Buffer.allocUnsafe(5 + trailerBytes.length);
      trailerFrame[0] = 0x80;                              // trailer frame flag
      trailerFrame.writeUInt32BE(trailerBytes.length, 1);
      trailerBytes.copy(trailerFrame, 5);

      const fullResponse = Buffer.concat([dataFrame, trailerFrame]);

      res.writeHead(200, {
        'content-type':   'application/grpc-web+proto',
        'x-content-type-options': 'nosniff',
      });
      res.end(fullResponse);

      console.log(`[Proxy] → grpc-status: ${grpcStatus}${grpcMessage ? ' (' + grpcMessage + ')' : ''}`);
    });

    // Send the gRPC frame to the server
    h2req.end(grpcFrame);
  });
}

// ── HTTP/1.1 server (accepts gRPC-Web from browsers) ─────────────────────────

const server = http.createServer(handleRequest);

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`❌  Port ${PROXY_PORT} is already in use. Set PROXY_PORT in .env`);
  } else {
    console.error('❌  Proxy server error:', err.message);
  }
  process.exit(1);
});

server.listen(PROXY_PORT, () => {
  console.log(`✅  gRPC-Web proxy listening on port ${PROXY_PORT}`);
  console.log(`    → Forwarding to gRPC server at localhost:${GRPC_PORT} (HTTP/2)`);
});
