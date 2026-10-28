/**
 * status_grpc_web_pb.js
 * gRPC-web client stub for StatusService.
 * Uses plain XHR so it works in any browser without extra dependencies.
 */

import { StatusRequest, StatusResponse } from './status_pb.js';

// ── gRPC-web frame helpers ────────────────────────────────────────────────────

function encodeGrpcWebFrame(bodyBytes) {
  const frame = new Uint8Array(5 + bodyBytes.length);
  frame[0] = 0x00;                              // data frame flag
  new DataView(frame.buffer).setUint32(1, bodyBytes.length, false); // big-endian length
  frame.set(bodyBytes, 5);
  return frame;
}

function decodeGrpcWebFrame(buf) {
  // buf is Uint8Array: [flags(1)][length(4)][body...]
  if (buf.length < 5) return new Uint8Array(0);
  const length = new DataView(buf.buffer, buf.byteOffset).getUint32(1, false);
  return buf.slice(5, 5 + length);
}

// ── XHR-based unary gRPC-web call ─────────────────────────────────────────────

function grpcWebCall(hostname, path, requestBytes, callback) {
  const url    = `${hostname}${path}`;
  const frame  = encodeGrpcWebFrame(requestBytes);

  const xhr = new XMLHttpRequest();
  xhr.open('POST', url, true);
  xhr.responseType = 'arraybuffer';
  xhr.setRequestHeader('content-type',  'application/grpc-web+proto');
  xhr.setRequestHeader('x-grpc-web',    '1');
  xhr.setRequestHeader('x-user-agent',  'grpc-web-javascript/0.1');

  xhr.onload = function () {
    if (xhr.status !== 200) {
      callback({ code: xhr.status, message: `HTTP ${xhr.status}` }, null);
      return;
    }

    const responseBytes = new Uint8Array(xhr.response);

    // Scan through all frames — find the data frame (flag=0x00)
    let offset = 0;
    let msgBuf = null;
    let grpcStatus = 0;

    while (offset < responseBytes.length) {
      if (offset + 5 > responseBytes.length) break;
      const flag   = responseBytes[offset];
      const length = new DataView(responseBytes.buffer, responseBytes.byteOffset + offset).getUint32(1, false);
      const body   = responseBytes.slice(offset + 5, offset + 5 + length);
      offset      += 5 + length;

      if (flag === 0x00) {
        // data frame
        msgBuf = body;
      } else if (flag === 0x80) {
        // trailers frame — check grpc-status
        const trailers = new TextDecoder().decode(body);
        const match    = trailers.match(/grpc-status:(\d+)/);
        if (match) grpcStatus = parseInt(match[1], 10);
      }
    }

    if (grpcStatus !== 0) {
      callback({ code: grpcStatus, message: `gRPC error status ${grpcStatus}` }, null);
      return;
    }

    if (!msgBuf) {
      callback({ code: -1, message: 'No data frame in response' }, null);
      return;
    }

    try {
      const response = StatusResponse.deserializeBinary(msgBuf);
      callback(null, response);
    } catch (e) {
      callback({ code: -1, message: `Deserialize error: ${e.message}` }, null);
    }
  };

  xhr.onerror = function () {
    callback({ code: -1, message: 'Network error' }, null);
  };

  xhr.send(frame);
}

// ── StatusServiceClient ───────────────────────────────────────────────────────

export class StatusServiceClient {
  /**
   * @param {string} hostname  e.g. "http://localhost:8080"
   */
  constructor(hostname) {
    this.hostname = hostname.replace(/\/+$/, '');
  }

  /**
   * @param {StatusRequest} request
   * @param {Object} metadata  (unused, kept for API compatibility)
   * @param {function(error, StatusResponse)} callback
   */
  checkStatus(request, metadata, callback) {
    const requestBytes = request.serializeBinary();
    // When hostname is empty (same-origin dev proxy), prefix with /grpc
    // so Vite can match and forward the request to Render.
    const basePath = this.hostname === ''
      ? '/grpc/status.StatusService/CheckStatus'
      : '/status.StatusService/CheckStatus';
    grpcWebCall(
      this.hostname,
      basePath,
      requestBytes,
      callback,
    );
  }
}
