# React gRPC-Web Demo

A React + Vite app that calls a gRPC service deployed on Render using gRPC-Web over HTTP/1.1.

## Overview

```
Browser (React)
  └── XHR → localhost:5173/grpc/...   (same-origin, no CORS)
               │
               └── Vite proxy middleware (Node https.request)
                     │
                     └── https://servergrpc.onrender.com/status.StatusService/CheckStatus
                                   │
                                   └── gRPC server (internal port 50051)
```

Browsers cannot speak native gRPC (HTTP/2 + trailers). The solution here is:

- The React app sends **gRPC-Web** requests (HTTP/1.1 + binary framing) to the same origin (`/grpc/...`)
- A custom Vite plugin middleware forwards those requests server-side to Render
- No CORS issues because the browser only ever talks to `localhost:5173`

---

## Project Structure

```
├── src/
│   ├── App.jsx                        # Root component
│   ├── components/
│   │   ├── GrpcDemo.jsx               # UI — input form + response display
│   │   └── GrpcDemo.css
│   └── grpc/
│       ├── status.proto               # Service definition (reference only)
│       ├── status_pb.js               # Protobuf encode/decode (hand-written, no codegen)
│       ├── status_grpc_web_pb.js      # gRPC-Web XHR client stub
│       └── statusService.js           # Promise wrapper around the client
├── vite.config.js                     # Vite config + custom gRPC proxy plugin
├── .env                               # VITE_GRPC_PROXY_URL (empty = use Vite proxy)
└── package.json
```

---

## Getting Started

### Prerequisites

- Node.js 18+
- npm

### Install

```bash
npm install
```

### Run in development

```bash
npm run dev
```

The app starts at `http://localhost:5173`. The Vite proxy middleware automatically forwards all `/grpc/*` requests to `https://servergrpc.onrender.com`.

### Build for production

```bash
npm run build
```

For production, set `VITE_GRPC_PROXY_URL` in your `.env` to point directly at the server (see [Environment Variables](#environment-variables) below).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VITE_GRPC_PROXY_URL` | `""` (empty) | gRPC-Web server URL. Empty = use Vite proxy at `/grpc`. Set to `https://servergrpc.onrender.com` for production builds. |

`.env` (dev):
```env
# Empty = Vite proxy handles forwarding. No CORS issues in dev.
VITE_GRPC_PROXY_URL=
```

`.env.production`:
```env
# Production: browser talks directly to Render.
# Render returns CORS headers so this works in deployed builds.
VITE_GRPC_PROXY_URL=https://servergrpc.onrender.com
```

---

## Proto Definition

Located at `src/grpc/status.proto`:

```proto
syntax = "proto3";
package status;

service StatusService {
  rpc CheckStatus (StatusRequest) returns (StatusResponse);
}

message StatusRequest {
  string message = 1;
}

message StatusResponse {
  int32  code    = 1;
  string status  = 2;
  string message = 3;
}
```

---

## How the gRPC-Web Client Works

The client is hand-written without relying on `protoc` codegen, split across three files:

### `status_pb.js`
Encodes/decodes `StatusRequest` and `StatusResponse` using raw protobuf binary format (varints + length-delimited fields). No dependency on `google-protobuf`.

### `status_grpc_web_pb.js`
Wraps each call in a 5-byte gRPC-Web frame (`flag[1] + length[4] + body`), sends it via `XMLHttpRequest`, then parses the response frames to extract the data frame and trailer frame (which carries `grpc-status`).

### `statusService.js`
Thin Promise wrapper. Reads `VITE_GRPC_PROXY_URL` from the environment. When empty, the client targets `/grpc/...` (same-origin Vite proxy). When set, it targets that URL directly.

---

## Why a Custom Vite Proxy Plugin

Vite's built-in `server.proxy` uses the `http-proxy` package, which returns `502 Bad Gateway` when the upstream response uses `Transfer-Encoding: chunked` over HTTPS — exactly what Render/Cloudflare returns.

The custom plugin in `vite.config.js` uses Node's native `https.request` to pipe the request and response directly, bypassing `http-proxy`:

```js
// vite.config.js (simplified)
function grpcProxyPlugin() {
  return {
    name: 'grpc-proxy',
    configureServer(server) {
      server.middlewares.use('/grpc', (req, res) => {
        const proxyReq = https.request({
          hostname: 'servergrpc.onrender.com',
          port: 443,
          path: req.url,
          method: req.method,
          headers: { ...req.headers, host: 'servergrpc.onrender.com' },
        }, (proxyRes) => {
          res.writeHead(proxyRes.statusCode, proxyRes.headers)
          proxyRes.pipe(res)
        })
        req.pipe(proxyReq)
      })
    },
  }
}
```

---

## Backend Server

The gRPC server is deployed at `https://servergrpc.onrender.com` (source in `server/`).

It runs two services in one process:

| Service | Protocol | Port | Used by |
|---|---|---|---|
| gRPC server | HTTP/2 (native gRPC) | 50051 (internal) | Proxy, iOS, Android |
| gRPC-Web proxy | HTTP/1.1 | 443 (public) | React (browser) |

See `server/README.md` for backend setup and deployment details.

---

## Using the Same Backend from iOS

iOS can use **native gRPC over HTTP/2** — no proxy needed.

See the previous iOS setup guide for Swift + SwiftUI using `grpc-swift`. The key difference:

| | React (this app) | iOS |
|---|---|---|
| Protocol | gRPC-Web (HTTP/1.1) | Native gRPC (HTTP/2) |
| Proxy needed | Yes (Vite middleware in dev) | No |
| Server URL | `https://servergrpc.onrender.com` | `servergrpc.onrender.com:443` |
| Code generation | Hand-written JS | `protoc` generates Swift |
