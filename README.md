# DM SDK Backend (gRPC Status Server 2.0)

The Rust backend for the DM iOS SDK, a self-hosted replacement for Firebase's backend. One process
serves:

| Service | RPCs | Stored in |
|---|---|---|
| `status.StatusService` (Analytics, unchanged from 1.x) | `CheckStatus` | `event_data` |
| `dm.sdk.v1.CrashlyticsService` | `UploadCrashReports` | `crash_reports`, `crash_issues` |
| `dm.sdk.v1.RemoteConfigService` | `FetchConfig` | `rc_templates` |
| `dm.sdk.v1.PerformanceService` | `UploadPerformance` | `perf_records` |
| `dm.sdk.v1.MessagingService` | `RegisterDevice`, `UpdateTopicSubscription`, `UnregisterDevice` | `devices` |
| dSYM upload `POST /v1/symbols` | multipart (Scripts/dm_upload_symbols.sh) | `symbol_files` + GridFS `dsyms` |
| Admin API `/admin/...` | Remote Config publishing, push sending, crash issues, stability, performance | — |

Everything is reachable over **gRPC-Web** on the public port (what the iOS SDK uses) and over
**native gRPC** on the internal port. `dm_sdk.proto` is the contract; it is the same file as in
the iOS package (`Sources/AnalyticsSDKGRPCTransport/Protos/dm_sdk.proto`), so keep the two copies
identical.

---

## Folder Structure

```
server/
├── src/
│   ├── main.rs              # Config + starts native gRPC and gRPC-Web in one process
│   ├── config.rs            # Environment → Config
│   ├── db.rs                # MongoDB: collections, indexes, TTLs (created on startup)
│   ├── grpc.rs              # StatusService / CheckStatus (analytics)
│   ├── events.rs            # Analytics batch validation (/events + CheckStatus)
│   ├── proxy.rs             # gRPC-Web routing, /events, /health, /v1/symbols, /admin, CORS
│   ├── tests.rs             # Request-path tests (no MongoDB needed)
│   └── dm/
│       ├── mod.rs           # Shared state, API-key auth, request metadata
│       ├── services.rs      # Native gRPC adapters for the dm.sdk.v1 services
│       ├── crashlytics.rs   # Ingest, issue grouping, regression + velocity alerts
│       ├── symbols.rs       # dSYM storage (GridFS) + symbolication (addr2line)
│       ├── remote_config.rs # Templates, evaluation, experiments, caching, throttling
│       ├── conditions.rs    # Remote Config condition language
│       ├── performance.rs   # Performance ingest
│       ├── messaging.rs     # Device registration + topics
│       ├── apns.rs          # APNs HTTP/2 sender (.p8 token auth)
│       ├── admin.rs         # Admin HTTP API
│       └── alerts.rs        # Crash alerts → log + webhook
├── mongo/setup.js           # Creates the database from mongosh (optional — the server does it too)
├── status.proto             # Analytics contract (1.x)
├── dm_sdk.proto             # DM SDK 2.0 contract
├── build.rs                 # Generates Rust code from both protos
├── Cargo.toml / Cargo.lock
├── Dockerfile / render.yaml
└── .env.example
```

---

## Quick Start

Requires Rust (https://rustup.rs) and `protoc` (`brew install protobuf`).

```bash
cd server
cp .env.example .env      # set MONGODB_URI, DM_API_KEYS, DM_ADMIN_KEY (and APNS_* for pushes)
cargo run --release
cargo test                # 23 tests; no database needed
```

## Creating the MongoDB database

The database is created automatically: on every start the server creates any missing
collections and declares all indexes and TTLs (idempotent). The first start against an empty
Atlas cluster therefore creates the whole schema.

To create it ahead of the first deploy (and to attach `$jsonSchema` validators in *warn* mode,
which document the schema inside the database without ever rejecting writes), run:

```bash
MONGODB_DB_NAME=grpc_status mongosh "mongodb+srv://<user>:<password>@cluster0.xxxxx.mongodb.net/" mongo/setup.js
# optional: SEED_SAMPLE_TEMPLATE=1 DM_APP_ID=dmart-ready-ios … to publish a sample Remote Config v1
```

The database user needs `readWrite` on the database. Changing a TTL later (`DM_RETENTION_DAYS`)
also needs `dbAdmin`; without it the server logs a warning and keeps the old TTL.

### Schema

| Collection | `_id` | Key fields | Indexes |
|---|---|---|---|
| `event_data` | ObjectId | `events[]`, `batch_id`, `instance_id`, `installation_id`, `app_id`, `app.version_name`, `received_at` | `batch_id`, `app_id+received_at`, `installation_id`, `events.name+received_at` |
| `crash_reports` | report id | `app_id`, `installation_id`, `kind`, `is_fatal`, `timestamp`, `launch_id`, `app_version`, `issue_id`, `custom_keys`, `logs`, `breadcrumbs`, `binary_images`, `return_addresses` (hex), `symbolicated_frames`, `metric_kit` | `app_id+issue_id+timestamp`, `app_id+app_version+timestamp`, `installation_id+timestamp`, `binary_images.uuid+symbolicated`, TTL |
| `crash_issues` | issue id | `title`, `subtitle`, `signature`, `status` (open/closed/muted/regressed), `count`, `first_seen`, `last_seen`, `versions` | `app_id+last_seen`, `app_id+count` |
| `perf_records` | record id | `kind`, `name`, `start_time`, `duration_ms`, `counters`, `attributes`, `network.*`, `rendering.*`, `app_version` | `app_id+kind+name+start_time`, `app_id+app_version+kind`, TTL |
| `devices` | registration token | `app_id`, `installation_id`, `user_id`, `apns_token`, `apns_environment`, `topics[]`, `active` | **unique** `app_id+installation_id`, `app_id+topics+active`, `app_id+user_id`, `apns_token` |
| `rc_templates` | ObjectId | `app_id`, `version`, `template`, `published_by`, `created_at` | **unique** `app_id+version` |
| `symbol_files` | Mach-O UUID | `name`, `arch`, `text_vmaddr`, `gridfs_id`, `bundle_id`, `app_version` | `bundle_id+app_version` |
| `push_messages` | message id | `target`, `sent`, `failed`, `failure_reasons`, `payload` | `app_id+created_at`, TTL |
| `dsyms.files` / `dsyms.chunks` | GridFS | DWARF binaries | GridFS defaults |

TTL = `received_at` expires after `DM_RETENTION_DAYS` (default 90). Analytics (`event_data`)
keeps its 1.x behaviour of no expiry unless `DM_ANALYTICS_RETENTION_DAYS` is set.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GRPC_HOST` / `GRPC_PORT` | `0.0.0.0` / `50051` | Native gRPC (internal) |
| `PORT` / `PROXY_PORT` | `8080` | gRPC-Web + HTTP (public); Render sets `PORT` |
| `ALLOWED_ORIGINS` | `*` | CORS origins |
| `MONGODB_URI` | — | **Required** |
| `MONGODB_DB_NAME` | `grpc_status` | Database name |
| `DM_API_KEYS` | — | `key` or `app_id:key`, comma-separated. Required as `x-api-key` on dm.sdk.v1 and `/v1/symbols`. **Unset = no auth (dev only).** `CheckStatus` stays open, as in 1.x |
| `DM_ADMIN_KEY` | — | Enables `/admin` (send as `x-admin-key`) |
| `DM_RETENTION_DAYS` | `90` | TTL for crash/perf/push documents (0 = forever) |
| `DM_ANALYTICS_RETENTION_DAYS` | `0` | TTL for `event_data` (0 = forever) |
| `DM_RC_FETCHES_PER_HOUR` / `DM_RC_REALTIME_CHECKS_PER_HOUR` | `30` / `120` | Remote Config throttle per installation |
| `DM_ALERT_WEBHOOK_URL` | — | Slack-compatible webhook for crash alerts |
| `DM_VELOCITY_ALERT_THRESHOLD` | `25` | Crashes of one issue per hour that trigger an alert |
| `DM_SYMBOLS_MAX_MB` | `512` | Max dSYM upload |
| `DM_DSYM_CACHE_DIR` | `$TMPDIR/dm-dsym-cache` | Extracted DWARF cache |
| `APNS_KEY_P8` or `APNS_KEY_PATH`, `APNS_KEY_ID`, `APNS_TEAM_ID` | — | APNs token auth (push sending) |
| `APNS_TOPIC` | — | apns-topic fallback (normally the device's bundle id) |

---

## Connecting the iOS SDK

```swift
DMSDK.configure(
    options: DMOptions(appId: "dmart-ready-ios"),
    backend: DMGrpcBackend(baseURL: URL(string: "https://<your-service>.onrender.com")!,
                           apiKey: "<a key from DM_API_KEYS>"),
    analytics: DMAnalyticsConfig(endpointUrl: url, transport: DMGrpcEventTransport(baseURL: url))
)
```

dSYM upload (Xcode Run Script): `DM_SYMBOLS_UPLOAD_URL=https://<your-service>.onrender.com/v1/symbols`,
`DM_API_KEY=<key>`, `DM_APP_ID=dmart-ready-ios`.

### Status codes the SDK relies on

| Situation | gRPC status | SDK behaviour |
|---|---|---|
| Success | `OK` | delete local copy |
| MongoDB unreachable | `UNAVAILABLE` (14) | keep + retry with backoff |
| Missing/invalid API key | `UNAUTHENTICATED` (16) | drop |
| Malformed request, invalid topic/token | `INVALID_ARGUMENT` (3) | drop |
| Too many Remote Config fetches | `RESOURCE_EXHAUSTED` (8) + `retry-after-seconds` trailer | back off for that long |
| Unknown registration token on topic change | `NOT_FOUND` (5) | drop (re-registration fixes it) |

Uploads are idempotent (`_id` = the client's report/record id), so a retry after a lost response
never double-counts.

---

## Crashlytics

* **Grouping:** reports are grouped into issues by error type + the top two *in-app* frames
  (main executable or any image you uploaded a dSYM for); exception reasons are left out because
  they contain addresses and indices. MetricKit diagnostics are grouped by diagnostic type,
  signal and the attributed thread's top frame.
* **Symbolication:** pure Rust (addr2line + object), no Xcode needed on the server. Frames get
  function, file and line when the image's dSYM has been uploaded. Reports that arrive *before*
  their dSYM are re-symbolicated automatically when it's uploaded. Swift function names are
  stored as they appear in the DWARF data (mangled `$s…` names when no demangled name exists).
* **Alerts** (log + `DM_ALERT_WEBHOOK_URL`): new fatal issue, regression (a crash on an issue you
  marked `closed` reopens it as `regressed`), and velocity (≥ threshold crashes in an hour, at
  most once an hour per issue).
* **Release monitoring:** `GET /admin/stability/:app_id?days=7` gives crash-free sessions % and
  crash-free users % per app version: Analytics `session_start` events are the denominator, and
  fatal `signal`/`exception` reports are the numerator.

## Remote Config

Templates are JSON, versioned, and immutable. The newest version is served. Example:

```json
{
  "description": "Diwali 2026",
  "conditions": [
    {"name": "india_v2", "expression": {"all": [{"country": {"in": ["IN"]}}, {"app_version": {"gte": "2.0.0"}}]}},
    {"name": "gold", "expression": {"user_property": {"name": "membership_tier", "eq": "gold"}}},
    {"name": "beta_10", "expression": {"percent": {"seed": "beta", "lt": 10}}},
    {"name": "sale_window", "expression": {"time": {"after": "2026-11-01T00:00:00+05:30", "before": "2026-11-05T00:00:00+05:30"}}}
  ],
  "parameters": {
    "show_diwali_banner":   {"default_value": false, "conditional_values": [{"condition": "sale_window", "value": true}]},
    "express_slot_minutes": {"default_value": 30, "conditional_values": [{"condition": "gold", "value": 15}]},
    "new_checkout":         {"conditional_values": [{"condition": "beta_10", "value": true}]}
  },
  "experiments": [{
    "experiment_id": "checkout_button", "condition": "india_v2", "traffic_percent": 50,
    "variants": [{"variant_id": "control", "weight": 50, "values": {}},
                 {"variant_id": "orange", "weight": 50, "values": {"checkout_color": "orange"}}]
  }]
}
```

* **Condition priority** is the order of the `conditions` array: the first matching condition
  wins, as in Firebase.
* A parameter without `default_value` is omitted when no condition matches, so the app's
  in-app default applies.
* Condition fields: `app_version`, `app_build`, `os_version`, `device_model`, `platform`,
  `country`, `language`, `locale`, `app_id`, `bundle_id`, `user_id`, `installation_id`,
  `user_property`, `custom_signal`, `percent`, `time`.
* Operators: `eq ne in not_in gt gte lt lte contains prefix exists`. Version strings compare
  numerically (`2.10.0 > 2.9.1`).
* Experiments assign variants by a stable hash of the installation, so each device stays in its
  variant. The SDK turns them into `dm_exp_<id>` user properties on Analytics events.
* Real-time: `PUT …?notify=true` sends a silent `rc_update` push to the app's devices; listening
  apps fetch at once. Otherwise they pick up changes on their next poll.

## Admin API

All routes need `x-admin-key: $DM_ADMIN_KEY`.

```bash
H="x-admin-key: $DM_ADMIN_KEY"; B=https://<your-service>.onrender.com

# Remote Config
curl -X PUT -H "$H" -H 'content-type: application/json' --data @template.json "$B/admin/remote-config/dmart-ready-ios?notify=true"
curl -H "$H" "$B/admin/remote-config/dmart-ready-ios"
curl -H "$H" "$B/admin/remote-config/dmart-ready-ios/versions"
curl -X POST -H "$H" "$B/admin/remote-config/dmart-ready-ios/rollback/3"
curl -X POST -H "$H" -H 'content-type: application/json' "$B/admin/remote-config/dmart-ready-ios/preview" \
     -d '{"context": {"country": "IN", "app_version": "2.1.0", "user_properties": {"membership_tier": "gold"}}}'

# Push (target: registration_token | installation_id | user_id | topic | {"all": true})
curl -X POST -H "$H" -H 'content-type: application/json' "$B/admin/messaging/send" -d '{
  "app_id": "dmart-ready-ios", "target": {"topic": "offers_gold"},
  "notification": {"title": "Diwali sale", "body": "Up to 40% off", "image_url": "https://…/banner.jpg"},
  "campaign_id": "diwali-2026", "campaign_name": "Diwali offers", "data": {"deeplink": "dmart://offers"}
}'
curl -X POST -H "$H" -H 'content-type: application/json' "$B/admin/messaging/send" -d '{"app_id": "dmart-ready-ios", "target": {"all": true}, "dry_run": true}'
curl -H "$H" "$B/admin/messaging/dmart-ready-ios/devices"

# Crashlytics / release monitoring / performance
curl -H "$H" "$B/admin/crashlytics/dmart-ready-ios/issues?fatal=true&sort=count"
curl -H "$H" "$B/admin/crashlytics/issues/<issue_id>"
curl -X PATCH -H "$H" -H 'content-type: application/json' "$B/admin/crashlytics/issues/<issue_id>" -d '{"status": "closed"}'
curl -H "$H" "$B/admin/stability/dmart-ready-ios?days=7"
curl -H "$H" "$B/admin/performance/dmart-ready-ios?days=7"
```

Push payloads carry a `dm` block (`message_id`, `campaign_id`, `campaign_name`, `label`,
`image_url`). That's what the SDK reads for `notification_open` analytics and for rich images.
Tokens Apple rejects (410 / `BadDeviceToken` / `Unregistered`) are deactivated automatically.

---

# Analytics service (1.x reference)

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
cargo run
```

With auto-reload on file changes (`cargo install cargo-watch`):
```bash
cargo watch -x run
```

Docker (same image Render builds):
```bash
docker build -t grpc-status-server .
docker run --env-file .env -p 8080:8080 grpc-status-server
```

---

## Port Summary

| Port   | Protocol       | Used by                          |
|--------|----------------|----------------------------------|
| `50051`| gRPC (HTTP/2)  | Android, iOS                     |
| `8080` | HTTP/1.1 (CORS)| React (web), React Native (web)  |
