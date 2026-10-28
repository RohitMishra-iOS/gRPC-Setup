/**
 * grpc-proxy.cjs
 *
 * A lightweight HTTP server that acts as a gRPC-web bridge.
 * Browsers can't speak plain gRPC, so this proxy:
 *   1. Receives an HTTP POST from the React app (gRPC-web format)
 *   2. Translates it into a real gRPC call to your gRPC server (port 50051)
 *   3. Returns the response back to the browser
 *
 * Run with:  node grpc-proxy.cjs
 * Listens on: http://localhost:8080
 */

const http = require('http');
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const GRPC_SERVER = 'http://localhost:8080';   // your gRPC server
const PROXY_PORT  = 8080;               // Vite proxies /grpc → here

// ── Load proto ────────────────────────────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, 'src', 'grpc', 'status.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const statusProto = grpc.loadPackageDefinition(packageDef).status;
const grpcClient  = new statusProto.StatusService(
  GRPC_SERVER,
  grpc.credentials.createInsecure(),
);

// ── gRPC-web helpers ──────────────────────────────────────────────────────────

/** Read the raw body of an HTTP request as a Buffer. */
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end',  () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

/**
 * gRPC-web frames a message as:
 *   [1-byte flags][4-byte length][body...]
 * Parse the body out.
 */
function decodeGrpcWebFrame(buf) {
  // byte 0 = flags (0 = data frame), bytes 1-4 = big-endian length
  const length = buf.readUInt32BE(1);
  return buf.slice(5, 5 + length);
}

/**
 * Encode a Buffer into a gRPC-web data frame.
 */
function encodeGrpcWebFrame(buf) {
  const frame = Buffer.alloc(5 + buf.length);
  frame[0] = 0;                    // data frame flag
  frame.writeUInt32BE(buf.length, 1);
  buf.copy(frame, 5);
  return frame;
}

/**
 * Encode the trailing metadata frame (required by gRPC-web spec).
 * Format: flags=0x80, length, "grpc-status:0\r\n"
 */
function encodeTrailers(grpcStatus = 0) {
  const trailerStr = `grpc-status:${grpcStatus}\r\n`;
  const trailerBuf = Buffer.from(trailerStr, 'utf8');
  const frame = Buffer.alloc(5 + trailerBuf.length);
  frame[0] = 0x80;                 // trailers flag
  frame.writeUInt32BE(trailerBuf.length, 1);
  trailerBuf.copy(frame, 5);
  return frame;
}

// ── Deserialize StatusRequest from protobuf binary ────────────────────────────
function decodeStatusRequest(buf) {
  // Field 1 (string message): tag=0x0a, varint length, utf8 bytes
  let offset = 0;
  const result = {};
  while (offset < buf.length) {
    const tag    = buf[offset++];
    const field  = tag >> 3;
    const wire   = tag & 0x07;
    if (wire === 2) {                // length-delimited
      const len  = buf[offset++];   // single-byte varint (safe for short strings)
      const data = buf.slice(offset, offset + len);
      offset += len;
      if (field === 1) result.message = data.toString('utf8');
    } else {
      break;                         // unexpected wire type, stop
    }
  }
  return result;
}

// ── HTTP server ───────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  // CORS — allow the Vite dev server origin
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type,x-grpc-web,x-user-agent');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method !== 'POST') {
    res.writeHead(405);
    res.end('Method Not Allowed');
    return;
  }

  // Only handle CheckStatus
  if (!req.url.endsWith('/status.StatusService/CheckStatus')) {
    res.writeHead(404);
    res.end('Not Found');
    return;
  }

  try {
    const rawBody    = await readBody(req);
    const msgBuf     = decodeGrpcWebFrame(rawBody);
    const { message } = decodeStatusRequest(msgBuf);

    console.log(`[proxy] → CheckStatus("${message}")`);

    grpcClient.CheckStatus({ message: message || '' }, (err, response) => {
      if (err) {
        console.error('[proxy] gRPC error:', err.message);
        res.writeHead(200, {
          'content-type': 'application/grpc-web+proto',
          'x-grpc-status': String(err.code),
        });
        res.end(encodeTrailers(err.code || 2));
        return;
      }

      console.log('[proxy] ← response:', response);

      // Serialize StatusResponse: field 1=int32 code, 2=string status, 3=string message
      const codeVal    = response.code   || 0;
      const statusStr  = Buffer.from(response.status  || '', 'utf8');
      const messageStr = Buffer.from(response.message || '', 'utf8');

      // Build protobuf binary manually
      const parts = [];

      if (codeVal !== 0) {
        // field 1, wire type 0 (varint)
        parts.push(Buffer.from([0x08]));
        // encode varint
        const varint = [];
        let v = codeVal;
        while (v > 127) { varint.push((v & 0x7f) | 0x80); v >>= 7; }
        varint.push(v);
        parts.push(Buffer.from(varint));
      }
      if (statusStr.length > 0) {
        // field 2, wire type 2 (length-delimited)
        parts.push(Buffer.from([0x12, statusStr.length]));
        parts.push(statusStr);
      }
      if (messageStr.length > 0) {
        // field 3, wire type 2 (length-delimited)
        parts.push(Buffer.from([0x1a, messageStr.length]));
        parts.push(messageStr);
      }

      const responseBuf = Buffer.concat(parts);
      const dataFrame   = encodeGrpcWebFrame(responseBuf);
      const trailers    = encodeTrailers(0);

      res.writeHead(200, {
        'content-type':  'application/grpc-web+proto',
        'x-grpc-status': '0',
      });
      res.write(dataFrame);
      res.end(trailers);
    });

  } catch (e) {
    console.error('[proxy] internal error:', e);
    res.writeHead(500);
    res.end('Internal Server Error');
  }
});

server.listen(PROXY_PORT, () => {
  console.log(`gRPC-web proxy listening on http://localhost:${PROXY_PORT}`);
  console.log(`Forwarding to gRPC server at ${GRPC_SERVER}`);
});
