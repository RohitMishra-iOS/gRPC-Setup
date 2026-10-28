/**
 * statusService.js
 * Promise-based wrapper around StatusServiceClient.
 *
 * In development, Vite proxies /grpc/* → https://servergrpc.onrender.com/*
 * so the browser makes a same-origin request (no CORS issues).
 * VITE_GRPC_PROXY_URL is intentionally left empty in .env for dev.
 *
 * In production, set VITE_GRPC_PROXY_URL to your deployed server URL.
 */

import { StatusServiceClient } from './status_grpc_web_pb.js';
import { StatusRequest } from './status_pb.js';

// Empty string = same origin via /grpc Vite proxy in dev.
// Set VITE_GRPC_PROXY_URL=https://servergrpc.onrender.com for production builds.
export const PROXY_URL = import.meta.env.VITE_GRPC_PROXY_URL ?? '';

/**
 * Calls StatusService/CheckStatus.
 * @param {object} payload  — JSON payload to send (encoded as google.protobuf.Struct)
 * @param {string} [url]    — override proxy URL (optional, uses PROXY_URL by default)
 * @returns {Promise<{ code: number, status: string, message: object }>}
 */
export function checkStatus(payload, url = PROXY_URL) {
  return new Promise((resolve, reject) => {
    const client  = new StatusServiceClient(url);
    const request = new StatusRequest();
    request.setMessage(payload);

    client.checkStatus(request, {}, (err, response) => {
      if (err) {
        console.error('[gRPC] CheckStatus error:', err);
        reject(err);
      } else {
        const result = {
          code:    response.getCode(),
          status:  response.getStatus(),
          message: response.getMessage(),
        };
        console.log('[gRPC] CheckStatus response:', result);
        resolve(result);
      }
    });
  });
}
