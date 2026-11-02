# Input validation, end to end. Run against a server started with tests/e2e.env:
#   python3 tests/e2e_validation.py
# Every check sends a malformed request and expects the documented rejection (or, for analytics
# and crash/perf uploads, that only the bad part is dropped). Repeatable: ids are unique per run.
import gzip, json, time, uuid

import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
RUN = uuid.uuid4().hex[:8]
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:400]}"))


def post(path, body=None, headers=None, **kw):
    return requests.post(B + path, json=body, headers={**H, **(headers or {})}, **kw)


def violations(r):
    try:
        details = r.json()["error"].get("details") or []
        return [v["field"] for d in details for v in d.get("fieldViolations", [])]
    except Exception:
        return []


def rpc(method, body):
    return post(f"/v1/rpc/dm.sdk.v1.{method}", body)


now = int(time.time() * 1000)
CTX = {"appId": APP, "installationId": f"inst-{RUN}", "platform": "android", "appVersion": "1.0"}

# ── Analytics /events ────────────────────────────────────────────────────────
good = {"name": "add_to_cart", "timestamp_ms": now, "session_id": "1", "params": {"item_id": "SKU_1", "value": 9.5}}
bad_name = {"name": "1-bad", "timestamp_ms": now, "params": {}}
old = {"name": "view_item", "timestamp_ms": now - 90 * 86400 * 1000, "params": {}}
bad_param = {"name": "search", "timestamp_ms": now, "params": {"search_term": "milk", "nested": {"x": 1}, "bad-name": 1}}
batch = {"batch_id": f"b-{RUN}-1", "app_id": APP, "installation_id": f"inst-{RUN}", "events": [good, bad_name, old, bad_param],
         "user_properties": {"plan": "gold", "this_name_is_far_too_long_for_a_property": "x"}, "consent": {"analytics_storage": "maybe"}}
r = requests.post(B + "/events", json=batch, headers=H)
body = r.json()
check("events: bad events dropped, good ones accepted", r.status_code == 200 and body["accepted"] and [e["index"] for e in body["rejected_events"]] == [1, 2], body)
check("events: invalid params / user properties / consent reported as warnings",
      any("params.nested" in w for w in body["warnings"]) and any("bad-name" in w for w in body["warnings"]) and any("user_properties" in w for w in body["warnings"]) and any("consent" in w for w in body["warnings"]), body.get("warnings"))
r = requests.post(B + "/events", json={"events": "nope"}, headers=H)
check("events: non-array events → 400", r.status_code == 400 and r.json()["accepted"] is False, r.text)
r = requests.post(B + "/events", json={"events": [good] * 501}, headers=H)
check("events: more than 500 events → 400", r.status_code == 400, r.text)
r = requests.post(B + "/events", json={"events": [good], "debug": "yes"}, headers=H)
check("events: debug must be boolean", r.status_code == 400, r.text)
r = requests.post(B + "/events", json={"app_id": APP, "events": [good]}, headers={"x-api-key": "wrong-key"})
check("events: a wrong API key → 401", r.status_code == 401, r.text)
r = requests.post(B + "/events", data=gzip.compress(json.dumps({"batch_id": f"b-{RUN}-gz", "app_id": APP, "events": [good]}).encode()),
                  headers={**H, "content-type": "application/json", "content-encoding": "gzip"})
check("events: gzip bodies (iOS HTTP transport) are accepted", r.status_code == 200 and r.json()["accepted"], r.text)

# ── Installations / App Check ────────────────────────────────────────────────
r = post("/v1/installations", {"app_id": APP, "platform": "symbian"})
check("installations: unknown platform → 400 with field details", r.status_code == 400 and "platform" in violations(r), r.text)
r = post("/v1/installations", {"app_id": APP, "fid": "not-a-fid"})
check("installations: malformed fid → 400", r.status_code == 400 and "fid" in violations(r), r.text)
r = requests.post(B + "/v1/installations/bad!/auth-token", headers={"authorization": "FIS_v2 x"})
check("installations: malformed fid in path → 400", r.status_code == 400, r.text)
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "magic", "token": "x" * 20})
check("appcheck: unknown provider → 400", r.status_code == 400 and "provider" in violations(r), r.text)
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "playintegrity", "token": "t" * 40, "package_name": "nodots"})
check("appcheck: invalid Android package name → 400", r.status_code == 400 and "package_name" in violations(r), r.text)

# ── Auth ─────────────────────────────────────────────────────────────────────
r = post("/v1/accounts/signUp", {"email": "not-an-email", "password": "secret123"})
check("auth: invalid email → INVALID_EMAIL", r.status_code == 400 and r.json()["error"]["reason"] == "INVALID_EMAIL", r.text)
r = post("/v1/accounts/signUp", {"email": f"v-{RUN}@example.com", "password": "123"})
check("auth: short password → WEAK_PASSWORD", r.status_code == 400 and r.json()["error"]["reason"] == "WEAK_PASSWORD", r.text)
r = post("/v1/accounts/signUp", {})
id_token = r.json()["idToken"]
r = post("/v1/accounts/update", {"idToken": id_token, "displayName": "x" * 300})
check("auth: displayName over 256 characters → 400", r.status_code == 400 and "displayName" in violations(r), r.text)
r = post("/v1/accounts/update", {"idToken": id_token, "photoUrl": "javascript:alert(1)"})
check("auth: photoUrl must be http(s)", r.status_code == 400 and "photoUrl" in violations(r), r.text)
r = post("/v1/accounts/update", {"idToken": id_token, "deleteAttribute": ["EMAIL"]})
check("auth: unknown deleteAttribute → 400", r.status_code == 400, r.text)
r = post("/v1/accounts/sendOobCode", {"requestType": "EMAIL_SIGNIN", "email": f"v-{RUN}@example.com", "continueUrl": "https://evil.example.net/phish"})
check("auth: continueUrl outside the authorized domains → 400", r.status_code == 400 and "continueUrl" in violations(r), r.text)
r = post("/v1/accounts/sendOobCode", {"requestType": "MAGIC", "email": f"v-{RUN}@example.com"})
check("auth: unknown requestType → 400", r.status_code == 400 and "requestType" in violations(r), r.text)
r = post("/v1/accounts/signInWithIdp", {"providerId": "facebook.com", "idToken": "x"})
check("auth: unsupported providerId → 400", r.status_code == 400 and "providerId" in violations(r), r.text)
r = requests.post(B + "/admin/auth/custom-token", json={"uid": "u1", "claims": {"sub": "admin"}}, headers=ADMIN)
check("admin: reserved custom claim → 400 with violations", r.status_code == 400 and any(v["field"] == "claims.sub" for v in r.json().get("violations", [])), r.text)
r = requests.post(B + "/admin/auth/custom-token", json={"uid": "u1", "claims": {"blob": "x" * 2000}}, headers=ADMIN)
check("admin: custom claims over 1000 bytes → 400", r.status_code == 400, r.text)
r = requests.post(B + "/admin/auth/users", json={"email": f"admin-{RUN}@example.com", "password": "abc"}, headers=ADMIN)
check("admin: create user with a weak password → 400", r.status_code == 400 and any(v["field"] == "password" for v in r.json().get("violations", [])), r.text)
r = requests.post(B + "/admin/auth/users", json={"email": "bad email"}, headers=ADMIN)
check("admin: create user with an invalid email → 400", r.status_code == 400, r.text)

# ── Storage ──────────────────────────────────────────────────────────────────
bearer = {"authorization": f"Bearer {id_token}"}
r = requests.post(B + "/v1/storage/b/default/o?name=users/x/a%0Ab.txt", data=b"x", headers={**H, **bearer, "content-type": "text/plain"})
check("storage: object name with a newline → 400", r.status_code == 400, r.text)
r = requests.post(B + "/v1/storage/b/Bad_Bucket!/o?name=a.txt", data=b"x", headers={**H, **bearer})
check("storage: invalid bucket name → 400", r.status_code == 400, r.text)
boundary = "dmv"
meta = json.dumps({"name": "users/x/a.txt", "contentType": "not a mime", "metadata": {"k": 5}})
multipart = f"--{boundary}\r\nContent-Type: application/json\r\n\r\n{meta}\r\n--{boundary}\r\nContent-Type: text/plain\r\n\r\nhello\r\n--{boundary}--\r\n".encode()
r = requests.post(B + "/v1/storage/b/default/o", data=multipart, headers={**H, **bearer, "content-type": f"multipart/related; boundary={boundary}"})
check("storage: invalid contentType and non-string custom metadata → 400", r.status_code == 400 and {"contentType", "metadata.k"} <= set(violations(r)), r.text)
r = requests.get(B + "/v1/storage/b/default/o?prefix=users/&maxResults=0", headers={**H, **bearer})
check("storage: maxResults 0 → 400", r.status_code == 400 and "maxResults" in violations(r), r.text)

# ── Functions ────────────────────────────────────────────────────────────────
r = post("/v1/functions/1bad", {"data": {}})
check("functions: invalid callable name → 400", r.status_code == 400 and "name" in violations(r), r.text)
r = requests.post(B + "/admin/functions", json={"name": f"fn{RUN}", "url": "ftp://x"}, headers=ADMIN)
check("admin: function url must be http(s)", r.status_code == 400, r.text)
r = requests.post(B + "/admin/functions", json={"name": f"fn{RUN}", "url": "https://fn.example.com", "timeout_seconds": 9999}, headers=ADMIN)
check("admin: function timeout over 540 s → 400", r.status_code == 400, r.text)

# ── Database ─────────────────────────────────────────────────────────────────
deep = {}
cursor = deep
for _ in range(25):
    cursor["x"] = {}
    cursor = cursor["x"]
r = post("/v1/db/commit", {"writes": [{"op": "set", "path": f"counters/deep-{RUN}", "data": deep}]}, headers=bearer)
check("database: values nested more than 20 levels → 400", r.status_code == 400, r.text)
r = post("/v1/db/query", {"collection": "posts", "limit": 0})
check("database: limit 0 → 400", r.status_code == 400 and "limit" in violations(r), r.text)
r = post("/v1/db/query", {"collection": "posts", "limitToLast": 5})
check("database: limitToLast without orderBy → 400", r.status_code == 400, r.text)
r = post("/v1/db/query", {"collection": "posts", "orderBy": [{"field": "a", "direction": "sideways"}]})
check("database: unknown orderBy direction → 400", r.status_code == 400, r.text)
r = post("/v1/db/aggregate", {"query": {"collection": "posts"}, "aggregations": [{"alias": f"a{i}", "op": "count"} for i in range(6)]})
check("database: more than 5 aggregations → 400", r.status_code == 400 and "aggregations" in violations(r), r.text)
r = post("/v1/db/aggregate", {"query": {"collection": "posts"}, "aggregations": [{"alias": "s", "op": "sum"}]})
check("database: sum without a field → 400", r.status_code == 400, r.text)
r = post("/v1/db/commit", {"writes": [{"op": "set", "path": "counters/bad\u0001id", "data": {"v": 1}}]}, headers=bearer)
check("database: control character in a document id → 400", r.status_code == 400, r.text)

# ── dm.sdk.v1 RPCs ───────────────────────────────────────────────────────────
r = rpc("RemoteConfigService/FetchConfig", {"context": {**CTX, "platform": "symbian"}})
check("rpc: unknown context.platform → 400", r.status_code == 400, r.text)
r = rpc("RemoteConfigService/FetchConfig", {"context": CTX, "customSignals": {f"k{i}": "v" for i in range(101)}})
check("remote config: more than 100 custom signals → 400", r.status_code == 400, r.text)
r = rpc("RemoteConfigService/FetchConfig", {"context": CTX, "countryCode": "INDIA"})
check("remote config: malformed country code → 400", r.status_code == 400, r.text)
good_report = {"id": f"rep-{RUN}-ok", "kind": "CRASH_KIND_NON_FATAL_ERROR", "timestampMs": str(now), "exceptionName": f"ValidationProbe{RUN}", "frames": [{"symbol": "a.b", "line": "3"}]}
bad_report = {"id": "has space", "kind": "CRASH_KIND_NON_FATAL_ERROR", "timestampMs": str(now)}
r = rpc("CrashlyticsService/UploadCrashReports", {"context": CTX, "reports": [good_report, bad_report]})
issues = requests.get(B + f"/admin/crashlytics/{APP}/issues?fatal=false", headers=ADMIN).json()["issues"]
check("crashlytics: invalid report dropped, valid one stored", r.ok and any(f"ValidationProbe{RUN}" in json.dumps(i) for i in issues), r.text)
r = rpc("PerformanceService/UploadPerformance", {"context": CTX, "records": [
    {"id": f"perf-{RUN}-1", "kind": "PERF_KIND_TRACE", "name": f"ok_trace_{RUN}", "startTimeMs": str(now), "durationUs": "1000"},
    {"id": f"perf-{RUN}-2", "kind": "PERF_KIND_TRACE", "name": "_reserved", "startTimeMs": str(now), "durationUs": "1000"},
    {"id": f"perf-{RUN}-3", "kind": "PERF_KIND_TRACE", "name": "negative", "startTimeMs": str(now), "durationUs": "-5"}]})
stats = json.dumps(requests.get(B + f"/admin/performance/{APP}", headers=ADMIN).json())
check("performance: invalid records dropped, valid one stored", r.ok and f"ok_trace_{RUN}" in stats and '"_reserved"' not in stats, r.text)
r = rpc("MessagingService/RegisterDevice", {"context": CTX, "pushTransport": "webpush", "webPush": {"endpoint": "https://push.example.com/x", "p256dh": "AAAA", "auth": "AAAA"}})
check("messaging: Web Push keys of the wrong length → 400", r.status_code == 400, r.text)
r = rpc("MessagingService/UpdateTopicSubscription", {"context": CTX, "registrationToken": "garbage", "topic": "news", "subscribe": True})
check("messaging: malformed registration token → 400", r.status_code == 400, r.text)

# ── Admin: messaging send / Remote Config ────────────────────────────────────
r = requests.post(B + "/admin/messaging/send", json={"app_id": APP, "target": {"topic": "bad topic!"}, "notification": {"title": "x"}, "dry_run": True}, headers=ADMIN)
check("admin send: invalid topic → 400 with violations", r.status_code == 400 and any(v["field"] == "target.topic" for v in r.json().get("violations", [])), r.text)
r = requests.post(B + "/admin/messaging/send", json={"app_id": APP, "target": {"all": True}, "data": {"aps": "x"}, "ttl_seconds": 99999999, "dry_run": True}, headers=ADMIN)
fields = {v["field"] for v in r.json().get("violations", [])} if r.status_code == 400 else set()
check("admin send: reserved data key and TTL over 28 days → 400", {"data.aps", "ttl_seconds"} <= fields, r.text)
r = requests.post(B + "/admin/messaging/send", json={"app_id": APP, "target": {"all": True}, "notification": {"title": "", "body": ""}, "dry_run": True}, headers=ADMIN)
check("admin send: notification without title or body → 400", r.status_code == 400, r.text)
r = requests.put(B + f"/admin/remote-config/{APP}", json={"parameters": {"show-banner": {"default_value": "true"}}}, headers=ADMIN)
check("admin remote config: parameter keys follow Firebase rules", r.status_code == 400 and "show-banner" in r.text, r.text)

passed = sum(ok for _, ok in results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
