# API keys, end to end: every client endpoint must reject a request with no x-api-key or a wrong
# one (401 / gRPC UNAUTHENTICATED) and accept the right one. Also checks that the server refuses
# to start without DM_API_KEYS. Run against a server started with tests/e2e.env:
#   python3 tests/e2e_api_keys.py [path/to/grpc-status-server]
import json, os, subprocess, sys, uuid

import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
GOOD = "test-key"
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:300]}"))


def varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out


def field(num, payload):  # length-delimited protobuf field
    return varint(num << 3 | 2) + varint(len(payload)) + payload


def grpc_frame(message):
    return b"\x00" + len(message).to_bytes(4, "big") + message


def grpc_status(r):
    if "grpc-status" in r.headers:
        return r.headers["grpc-status"]
    # Trailer frame (0x80) inside the body.
    body = r.content
    i = 0
    while i + 5 <= len(body):
        flag, n = body[i], int.from_bytes(body[i + 1:i + 5], "big")
        chunk = body[i + 5:i + 5 + n]
        if flag & 0x80:
            for line in chunk.decode(errors="replace").split("\r\n"):
                if line.lower().startswith("grpc-status:"):
                    return line.split(":", 1)[1].strip()
        i += 5 + n
    return "0"


INST = f"inst-keys-{uuid.uuid4().hex[:8]}"
CTX = {"appId": APP, "installationId": INST, "platform": "ios"}
context_pb = field(1, APP.encode()) + field(2, INST.encode())

# (name, method, path, json body or raw bytes, extra headers, kind) — kind "http" expects 401,
# "grpc" expects grpc-status 16.
cases = [
    ("analytics /events (JSON)", "POST", "/events", {"app_id": APP, "batch_id": str(uuid.uuid4()), "installation_id": INST, "events": [{"name": "key_test", "timestamp_ms": 0}]}, {}, "http"),
    ("analytics CheckStatus (gRPC-Web, iOS)", "POST", "/status.StatusService/CheckStatus", grpc_frame(b""), {"content-type": "application/grpc-web+proto", "x-dm-app-id": APP}, "grpc"),
    ("Remote Config FetchConfig (gRPC-Web)", "POST", "/dm.sdk.v1.RemoteConfigService/FetchConfig", grpc_frame(field(1, context_pb)), {"content-type": "application/grpc-web+proto"}, "grpc"),
    ("Remote Config FetchConfig (JSON)", "POST", "/v1/rpc/dm.sdk.v1.RemoteConfigService/FetchConfig", {"context": CTX}, {}, "http"),
    ("Crashlytics upload", "POST", "/v1/rpc/dm.sdk.v1.CrashlyticsService/UploadCrashReports", {"context": CTX, "reports": []}, {}, "http"),
    ("Performance upload", "POST", "/v1/rpc/dm.sdk.v1.PerformanceService/UploadPerformance", {"context": CTX, "records": []}, {}, "http"),
    ("Messaging register", "POST", "/v1/rpc/dm.sdk.v1.MessagingService/RegisterDevice", {"context": CTX, "apnsToken": "ab" * 32}, {}, "http"),
    ("Installations register", "POST", "/v1/installations", {"app_id": APP, "platform": "ios"}, {}, "http"),
    ("Auth config", "GET", "/v1/auth/config", None, {}, "http"),
    ("Auth sign-up", "POST", "/v1/accounts/signUp", {"returnSecureToken": True}, {}, "http"),
    ("Auth token refresh", "POST", "/v1/token", {"grant_type": "refresh_token", "refresh_token": "x"}, {}, "http"),
    ("App Check challenge", "POST", "/v1/appcheck/challenge", {"app_id": APP}, {}, "http"),
    ("App Check exchange", "POST", "/v1/appcheck/exchange", {"app_id": APP, "provider": "debug", "debug_token": "NOT-A-REGISTERED-TOKEN-0000"}, {}, "http"),
    ("Storage list", "GET", "/v1/storage/b/default/o?prefix=", None, {}, "http"),
    ("Database query", "POST", "/v1/db/query", {"parent": "", "collection": "c"}, {}, "http"),
    ("Functions call", "POST", "/v1/functions/addMessage", {"data": {}}, {}, "http"),
    ("Web Push key", "GET", "/v1/messaging/webpush-key", None, {}, "http"),
    ("dSYM / mapping upload", "POST", "/v1/symbols", None, {}, "http"),
]


def send(method, path, body, headers):
    headers = {"x-dm-app-id": APP, **headers}
    if isinstance(body, bytes):
        return requests.request(method, B + path, data=body, headers=headers, timeout=30)
    if path == "/v1/symbols":
        return requests.post(B + path, files={"app_id": (None, APP)}, headers=headers, timeout=30)
    return requests.request(method, B + path, json=body, headers=headers, timeout=30)


for name, method, path, body, extra, kind in cases:
    for label, key in [("no key", None), ("wrong key", "test")]:
        headers = dict(extra)
        if key:
            headers["x-api-key"] = key
        r = send(method, path, body, headers)
        ok = r.status_code == 401 if kind == "http" else grpc_status(r) == "16"
        check(f"{name}: {label} → rejected", ok, f"HTTP {r.status_code} grpc={grpc_status(r) if kind == 'grpc' else '-'} {r.text[:150]}")
    r = send(method, path, body, {**extra, "x-api-key": GOOD})
    # With the right key the request may still fail for its own reasons (e.g. a fake refresh
    # token), just never because of the key.
    key_error = "API_KEY" in r.text or "API key" in r.text or "x-api-key" in r.text
    ok = (r.status_code != 401 or not key_error) if kind == "http" else grpc_status(r) != "16"
    check(f"{name}: right key → not rejected for the key", ok, f"HTTP {r.status_code} {r.text[:150]}")

# A key registered for another app is refused too.
r = send("POST", "/v1/rpc/dm.sdk.v1.RemoteConfigService/FetchConfig", {"context": {**CTX, "appId": "dmart-ready"}}, {"x-api-key": GOOD, "x-dm-app-id": "dmart-ready"})
check("key of another app → rejected", r.status_code == 401, r.text[:200])

# The server must refuse to start without DM_API_KEYS (unless DM_ALLOW_UNAUTHENTICATED=true).
binary = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "target", "debug", "grpc-status-server")
if os.path.exists(binary):
    env = {k: v for k, v in os.environ.items() if k not in ("DM_API_KEYS", "DM_ALLOW_UNAUTHENTICATED")}
    env.update({"PORT": "18099", "GRPC_PORT": "18098", "MONGODB_URI": "mongodb://127.0.0.1:27017/?directConnection=true", "DM_TOKEN_SECRET": "x" * 40})
    try:
        # Run outside the server folder so its .env (which sets DM_API_KEYS) isn't loaded.
        p = subprocess.run([os.path.abspath(binary)], env=env, capture_output=True, text=True, timeout=30, cwd="/")
        out = p.stdout + p.stderr
        check("server refuses to start without DM_API_KEYS", p.returncode != 0 and "DM_API_KEYS is not set" in out, out[-400:])
    except subprocess.TimeoutExpired:
        check("server refuses to start without DM_API_KEYS", False, "server kept running")
else:
    print(f"SKIP startup check (no binary at {binary})")

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
