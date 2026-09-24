# gRPC Status Server

A lightweight gRPC server exposing a `StatusService` with a single `CheckStatus` RPC.  
Works with **React (web)**, **React Native**, **Android**, and **iOS**.

---

## Folder Structure

```
server/
├── server.js            # gRPC server (port 50051)
├── grpc-web-proxy.js    # HTTP/1.1 → gRPC proxy for browsers (port 8080)
├── status.proto         # Protobuf service definition
├── .env                 # Environment variables (not committed)
├── .env.example         # Template for .env
├── .gitignore
└── package.json
```

---

## Quick Start

```bash
cd server
npm install

# Start gRPC server only
npm start

# Start gRPC server + web proxy together
npm run start:all
```

---

## Environment Variables

| Variable          | Default     | Description                                          |
|-------------------|-------------|------------------------------------------------------|
| `GRPC_HOST`       | `0.0.0.0`   | Host the gRPC server binds to                        |
| `GRPC_PORT`       | `50051`      | Port for native gRPC clients (Android, iOS, Node)    |
| `PROXY_PORT`      | `8080`       | Port for the gRPC-Web proxy (React / browser)        |
| `ALLOWED_ORIGINS` | `*`          | Comma-separated CORS origins (`*` = allow all)       |

Copy `.env.example` → `.env` and adjust as needed.

---

## API Reference

### Proto Definition

```protobuf
syntax = "proto3";
package status;

service StatusService {
  rpc CheckStatus (StatusRequest) returns (StatusResponse);
}

message StatusRequest {
  string message = 1;    // Any string payload
}

message StatusResponse {
  int32  code    = 1;    // HTTP-style status code (200 = success)
  string status  = 2;    // "SUCCESS" on success
  string message = 3;    // Echo: "OK - received: <your message>"
}
```

### CheckStatus

| Field    | Type     | Description                     |
|----------|----------|---------------------------------|
| Request  | `string message` | Arbitrary message string |
| Response | `int32 code` | `200` on success             |
|          | `string status` | `"SUCCESS"`                 |
|          | `string message` | `"OK - received: <msg>"`   |

---

## Platform Integration Guide

### React (Web)

Uses **grpc-web** over the proxy on port **8080**.

```bash
npm install grpc-web google-protobuf
```

Generate JS stubs from the proto (requires `protoc` + `protoc-gen-grpc-web`):

```bash
protoc -I=. status.proto \
  --js_out=import_style=commonjs:./src/grpc \
  --grpc-web_out=import_style=commonjs,mode=grpcwebtext:./src/grpc
```

Call the service:

```js
import { StatusServiceClient } from './grpc/status_grpc_web_pb';
import { StatusRequest } from './grpc/status_pb';

const client = new StatusServiceClient('http://localhost:8080');

const request = new StatusRequest();
request.setMessage('Hello from React');

client.checkStatus(request, {}, (err, response) => {
  if (err) console.error(err);
  else console.log(response.getCode(), response.getStatus(), response.getMessage());
});
```

---

### React Native

Same as React web — use **grpc-web** through the proxy.  
Point the client URL to your machine's LAN IP instead of `localhost`:

```js
const client = new StatusServiceClient('http://192.168.x.x:8080');
```

---

### Android (Java / Kotlin)

Connects **directly** to the gRPC server on port **50051** using `grpc-java`.

`build.gradle`:
```gradle
implementation 'io.grpc:grpc-okhttp:1.63.0'
implementation 'io.grpc:grpc-protobuf-lite:1.63.0'
implementation 'io.grpc:grpc-stub:1.63.0'
```

`AndroidManifest.xml` (for cleartext in dev):
```xml
<uses-permission android:name="android.permission.INTERNET" />
<application android:usesCleartextTraffic="true" ...>
```

Kotlin usage:
```kotlin
val channel = ManagedChannelBuilder
    .forAddress("10.0.2.2", 50051) // 10.0.2.2 = host machine from emulator
    .usePlaintext()
    .build()

val stub = StatusServiceGrpc.newBlockingStub(channel)
val request = StatusRequest.newBuilder().setMessage("Hello from Android").build()
val response = stub.checkStatus(request)

Log.d("gRPC", "${response.code} ${response.status} ${response.message}")
channel.shutdown()
```

> **Note:** Use `10.0.2.2` for Android Emulator, or the machine's LAN IP for a physical device.

---

### iOS (Swift)

Connects **directly** to port **50051** using **grpc-swift**.

`Package.swift` dependencies:
```swift
.package(url: "https://github.com/grpc/grpc-swift.git", from: "1.21.0"),
```

Generate Swift stubs:
```bash
protoc status.proto \
  --swift_out=. \
  --grpc-swift_out=.
```

Usage:
```swift
import GRPC
import NIOCore
import NIOPosix

let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
defer { try? group.syncShutdownGracefully() }

let channel = try GRPCChannelPool.with(
    target: .host("localhost", port: 50051),
    transportSecurity: .plaintext,
    eventLoopGroup: group
)
defer { try? channel.close().wait() }

let client = Status_StatusServiceNIOClient(channel: channel)

var request = Status_StatusRequest()
request.message = "Hello from iOS"

let response = try client.checkStatus(request).response.wait()
print(response.code, response.status, response.message)
```

> **Note:** For physical iOS devices use your machine's LAN IP instead of `localhost`.

---

## Running in Development

```bash
# Terminal 1 — gRPC server
node server.js

# Terminal 2 — gRPC-Web proxy (needed only for React/browser clients)
node grpc-web-proxy.js
```

Or both together:
```bash
npm run start:all
```

With auto-reload on file changes:
```bash
npm run dev
```

---

## Port Summary

| Port   | Protocol       | Used by                          |
|--------|----------------|----------------------------------|
| `50051`| gRPC (HTTP/2)  | Android, iOS, Node.js            |
| `8080` | HTTP/1.1 (CORS)| React (web), React Native (web)  |
