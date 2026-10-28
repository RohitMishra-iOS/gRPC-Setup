> **2.0:** the DM iOS SDK (Analytics, Crashlytics, Remote Config, Performance, Messaging) talks to this server over gRPC-Web — see **README.md → Connecting the iOS SDK**. This file documents the original 1.x `CheckStatus` integration, which is unchanged.

# iOS Integration — StatusService / CheckStatus

This documents what changed on the server and exactly what the iOS app
needs to send now. **Read the "Breaking change" section first** — calls
built the old way will fail until the app is updated.

## Breaking change

`StatusRequest.message` and `StatusResponse.message` used to be a plain
`string` field, so the app could put a JSON string directly in it. That
field is now `google.protobuf.Struct` — a real, typed JSON value at the
protobuf level, not a string containing JSON text.

**Before (no longer works):**
```
StatusRequest {
  message: "{\"batch_id\":\"...\",\"events\":[...]}"   // string
}
```

**Now required:**
```
StatusRequest {
  message: Struct { ... }   // actual protobuf Struct, not a string
}
```

If the app still sends a string, the server will reject the call at
the protocol level (the bytes won't decode as a valid `Struct`) — it
won't reach the validation logic below at all.

## Endpoint

| | |
|---|---|
| Production | `https://servergrpc.onrender.com` |
| Local dev  | `http://localhost:8080` |
| Method     | `POST /status.StatusService/CheckStatus` |

**Transport:** the server only exposes **gRPC-Web** (HTTP/1.1 with
5-byte-framed messages), not plain native gRPC over HTTP/2 — Render
exposes a single public port, which is the gRPC-Web proxy. Whatever
client library the app uses must speak gRPC-Web, not raw HTTP/2 gRPC.

Each request/response body is one frame:
```
[flags: 1 byte][length: 4 bytes, big-endian][protobuf message bytes]
```
`flags = 0x00` for the data frame. Required headers:
```
content-type: application/grpc-web+proto
x-grpc-web: 1
```

## Proto contract

```proto
syntax = "proto3";

package status;

import "google/protobuf/struct.proto";

service StatusService {
  rpc CheckStatus (StatusRequest) returns (StatusResponse);
}

message StatusRequest {
  google.protobuf.Struct message = 1;
}

message StatusResponse {
  int32 code = 1;
  string status = 2;
  google.protobuf.Struct message = 3;
}
```

`google.protobuf.Struct` is a standard well-known protobuf type
representing arbitrary JSON (object/array/string/number/bool/null).
SwiftProtobuf has it built in as `Google_Protobuf_Struct` /
`Google_Protobuf_Value` — no need to hand-roll it.

## Request payload shape

`message` must be a JSON **object** shaped like an event batch:

```json
{
  "batch_id": "93EA4A28-C197-49A2-96BB-000824B4745F",
  "instance_id": "A0222173-B7DB-4317-912D-5A4B19437023",
  "screen_name": "HomePage",
  "device": { "manufacturer": "Apple", "model": "arm64", "...": "..." },
  "app": { "bundle_id": "in.rohit.ProductCardAnimation", "...": "..." },
  "user_properties": {},
  "events": [
    {
      "session_id": "DEBB65A7-6D91-4783-95DC-38A89145F547",
      "name": "Increase_Quantity",
      "timestamp_ms": 1790254238625,
      "params": { "quantity": 3 }
    }
  ]
}
```

**Validation rules** (server-side, per event in `events[]`):
- `timestamp_ms` — required, must be a number
- `name` — required, must be a non-blank string

Every other field is passed through untouched — the server doesn't
care about `batch_id`, `device`, `app`, etc., only `events[]`.

## Response shape

The server echoes the same JSON object back, merged with a
`status_code` field (and `error`/`details` on failure). This is
returned in `StatusResponse.message` (a `Struct`), alongside
`StatusResponse.code` and `StatusResponse.status`.

**Success (`code: 200`, `status: "SUCCESS"`):**
```json
{
  "batch_id": "93EA4A28-...",
  "events": [ ... ],
  "...": "... (everything you sent, echoed back)",
  "status_code": 200
}
```

**Validation failure (`code: 400`, `status: "ERROR"`):**
```json
{
  "events": [ { "timestamp_ms": 123, "name": "" } ],
  "status_code": 400,
  "error": "Validation failed",
  "details": ["events[0].name must not be blank"]
}
```

**Malformed request** — if the bytes don't decode as a valid `Struct`
at all, the call fails at the gRPC transport layer (a `grpc-status`
error in the trailers) before ever reaching this JSON response shape.
Handle that as a normal gRPC error, not a 400.

## Swift example (SwiftProtobuf)

Building the request payload:

```swift
import SwiftProtobuf

func makeCheckStatusRequest() -> Status_StatusRequest {
    var event = Google_Protobuf_Struct()
    event.fields["session_id"]   = .stringValue("DEBB65A7-...")
    event.fields["name"]         = .stringValue("Increase_Quantity")
    event.fields["timestamp_ms"] = .numberValue(Date().timeIntervalSince1970 * 1000)

    var params = Google_Protobuf_Struct()
    params.fields["quantity"] = .numberValue(3)
    event.fields["params"] = .structValue(params)

    var batch = Google_Protobuf_Struct()
    batch.fields["batch_id"] = .stringValue(UUID().uuidString)
    batch.fields["events"]   = .listValue(.init(values: [.structValue(event)]))

    var request = Status_StatusRequest()
    request.message = batch
    return request
}
```

Reading the response:

```swift
let response: Status_StatusResponse = /* result of the call */
if response.code == 200 {
    // response.message.fields["status_code"] == 200
} else {
    let error   = response.message.fields["error"]?.stringValue ?? "unknown"
    let details = response.message.fields["details"]?.listValue.values
        .compactMap { $0.stringValue } ?? []
}
```

(Exact generated type/method names depend on which gRPC-Web-capable
Swift library the app uses — grpc-swift, grpc-swift-web, Connect-Swift,
etc. — but the `Google_Protobuf_Struct` construction above is the same
regardless of transport.)

## Checklist for the iOS change

- [ ] Regenerate/update local proto stubs from the `status.proto` above (adds the `google/protobuf/struct.proto` import)
- [ ] Replace `message = jsonString` with a `Google_Protobuf_Struct` built from the event batch
- [ ] Confirm the transport is gRPC-Web (HTTP/1.1, framed), not HTTP/2 native gRPC
- [ ] Update response parsing: `message` is now a `Struct`, not a string — no more `JSON.parse`/`JSONSerialization` needed, read fields directly
- [ ] Handle the new `status_code` / `error` / `details` fields on validation failures
