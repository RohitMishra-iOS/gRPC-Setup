# Run against a server started with tests/e2e.env (see README "Testing"): python3 tests/e2e_platform.py
"""End-to-end test of the platform services against a running server + MongoDB."""
import base64, hashlib, hmac, json, os, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests, websocket

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
results = []

def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {detail}"))

def post(path, body=None, headers=None, **kw):
    return requests.post(B + path, json=body, headers={**H, **(headers or {})}, **kw)

# ── Function server (verifies the signature, echoes context) ──
class Fn(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"]))
        sig = self.headers.get("x-dm-signature", "")
        t = sig.split(",")[0][2:]
        expected = "v1=" + hmac.new(b"fn-secret", f"{t}.".encode() + body, hashlib.sha256).hexdigest()
        req = json.loads(body)
        if self.path == "/stream":
            self.send_response(200); self.send_header("content-type", "text/event-stream"); self.end_headers()
            for i in range(3):
                self.wfile.write(f"data: {json.dumps({'message': i})}\n\n".encode()); self.wfile.flush()
            self.wfile.write(b'data: {"result": "done"}\n\n')
            return
        if req["data"].get("fail"):
            out = {"error": {"status": "INVALID_ARGUMENT", "message": "bad input"}}
        else:
            out = {"result": {"echo": req["data"], "uid": (req["context"]["auth"] or {}).get("uid"), "sig_ok": sig.endswith(expected), "appcheck": req["context"]["app"]["appcheck"]}}
        payload = json.dumps(out).encode()
        self.send_response(200); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(payload))); self.end_headers(); self.wfile.write(payload)

fn_server = HTTPServer(("127.0.0.1", 8099), Fn)
threading.Thread(target=fn_server.serve_forever, daemon=True).start()

# ── Installations ──
r = post("/v1/installations", {"app_id": APP, "platform": "ios", "sdk_version": "3.0.0"})
inst = r.json()
check("installations: register", r.ok and len(inst["fid"]) == 22 and inst["fid"][0] in "cdef", r.text)
r = requests.post(f"{B}/v1/installations/{inst['fid']}/auth-token", headers={"authorization": "FIS_v2 " + inst["refresh_token"]})
check("installations: refresh auth token", r.ok and r.json()["token"], r.text)
r = requests.post(f"{B}/v1/installations/{inst['fid']}/auth-token", headers={"authorization": "FIS_v2 wrong"})
check("installations: wrong refresh token rejected", r.status_code == 401, r.text)
r = post("/v1/installations", {"app_id": APP}, headers={"x-api-key": "bad"})
check("installations: bad API key rejected", r.status_code == 401, r.text)
fis = inst["auth_token"]["token"]

# ── App Check ──
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "debug", "token": "DEBUG-TOKEN-1234567890"})
check("appcheck: debug exchange", r.ok and r.json()["ttl_seconds"] >= 1800, r.text)
appcheck = r.json()["token"]
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "debug", "token": "nope"})
check("appcheck: unknown debug token rejected", r.status_code == 403, r.text)
r = requests.post(f"{B}/admin/appcheck/debug-tokens", json={"name": "Rohit's iPhone"}, headers=ADMIN)
console_token = r.json()["token"]
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "debug", "token": console_token, "limited_use": True})
limited = r.json().get("token")
check("appcheck: console-registered debug token + limited use", r.ok and limited, r.text)
r1 = requests.post(f"{B}/v1/appcheck/verify", json={"token": limited}).json()
r2 = requests.post(f"{B}/v1/appcheck/verify", json={"token": limited}).json()
check("appcheck: limited-use token consumed once", r1["already_consumed"] is False and r2["already_consumed"] is True, (r1, r2))
r = post("/v1/appcheck/exchange", {"app_id": APP, "provider": "devicecheck", "token": "x"})
check("appcheck: devicecheck without key → clear 501", r.status_code == 501 and "DeviceCheck" in r.text, r.text)
r = requests.post(f"{B}/admin/appcheck/{APP}/token", json={}, headers=ADMIN)
check("appcheck: admin mints custom-provider token", r.ok and requests.post(f"{B}/v1/appcheck/verify", json={"token": r.json()["token"]}).json()["provider"] == "custom", r.text)

# ── Auth ──
r = post("/v1/accounts/signUp", {})
anon = r.json()
check("auth: anonymous sign-up", r.ok and anon["isAnonymous"] and anon["idToken"], r.text)
r = post("/v1/accounts/signUp", {"email": "rohit@example.com", "password": "secret123"}, headers={"authorization": "Bearer " + anon["idToken"]})
check("auth: link email/password to anonymous user (same uid)", r.ok and r.json()["localId"] == anon["localId"] and not r.json()["isAnonymous"], r.text)
r = post("/v1/accounts/signUp", {"email": "rohit@example.com", "password": "secret123"})
check("auth: duplicate email → EMAIL_EXISTS", r.status_code == 409 and "EMAIL_EXISTS" in r.text, r.text)
r = post("/v1/accounts/signUp", {"email": "bad", "password": "secret123"})
check("auth: invalid email", r.status_code == 400 and "INVALID_EMAIL" in r.text, r.text)
r = post("/v1/accounts/signUp", {"email": "x@y.com", "password": "123"})
check("auth: weak password", r.status_code == 400 and "WEAK_PASSWORD" in r.text, r.text)
r = post("/v1/accounts/signInWithPassword", {"email": "ROHIT@example.com", "password": "secret123"})
user = r.json()
check("auth: sign in with password (case-insensitive email)", r.ok and user["localId"] == anon["localId"] and user["registered"], r.text)
r = post("/v1/accounts/signInWithPassword", {"email": "rohit@example.com", "password": "wrong"})
check("auth: wrong password", r.status_code == 401 and "INVALID_LOGIN_CREDENTIALS" in r.text, r.text)
r = post("/v1/accounts/lookup", {"idToken": user["idToken"]})
check("auth: lookup", r.ok and r.json()["users"][0]["email"] == "rohit@example.com", r.text)
r = post("/v1/accounts/update", {"idToken": user["idToken"], "displayName": "Rohit"})
check("auth: update profile", r.ok and r.json()["displayName"] == "Rohit", r.text)
r = requests.post(f"{B}/v1/token", data={"grant_type": "refresh_token", "refresh_token": user["refreshToken"]}, headers=H)
check("auth: refresh token (form-encoded)", r.ok and r.json()["id_token"] and r.json()["user_id"] == user["localId"], r.text)
id_token = r.json()["id_token"]
claims = json.loads(base64.urlsafe_b64decode(id_token.split(".")[1] + "=="))
check("auth: ID token claims Firebase-shaped", claims["sub"] == user["localId"] and claims["firebase"]["sign_in_provider"] == "password" and claims["name"] == "Rohit", claims)
r = requests.post(f"{B}/admin/auth/custom-token", json={"uid": "backend-user-1", "claims": {"role": "admin"}}, headers=ADMIN)
r = post("/v1/accounts/signInWithCustomToken", {"token": r.json()["token"]})
custom = r.json()
cclaims = json.loads(base64.urlsafe_b64decode(custom["idToken"].split(".")[1] + "=="))
check("auth: custom token sign-in carries custom claims", r.ok and custom["localId"] == "backend-user-1" and cclaims.get("role") == "admin", r.text)
r = requests.post(f"{B}/admin/auth/email-link", json={"type": "PASSWORD_RESET", "email": "rohit@example.com"}, headers=ADMIN)
link = r.json()["link"]
code = urllib.parse.parse_qs(urllib.parse.urlparse(link).query)["oobCode"][0]
r = post("/v1/accounts/resetPassword", {"oobCode": code})
check("auth: reset code verifies", r.ok and r.json()["email"] == "rohit@example.com", r.text)
r = post("/v1/accounts/resetPassword", {"oobCode": code, "newPassword": "newsecret1"})
check("auth: password reset", r.ok, r.text)
r = requests.post(f"{B}/v1/token", json={"grant_type": "refresh_token", "refresh_token": user["refreshToken"]}, headers=H)
check("auth: reset revokes old refresh tokens", r.status_code == 401, r.text)
r = post("/v1/accounts/signInWithPassword", {"email": "rohit@example.com", "password": "newsecret1"})
user = r.json()
check("auth: sign in with new password", r.ok, r.text)
r = post("/v1/accounts/sendOobCode", {"requestType": "EMAIL_SIGNIN", "email": "linkuser@example.com"})
check("auth: email sign-in link requested", r.ok, r.text)
r = requests.post(f"{B}/admin/auth/email-link", json={"type": "EMAIL_SIGNIN", "email": "linkuser@example.com"}, headers=ADMIN)
code = urllib.parse.parse_qs(urllib.parse.urlparse(r.json()["link"]).query)["oobCode"][0]
r = post("/v1/accounts/signInWithEmailLink", {"email": "linkuser@example.com", "oobCode": code})
check("auth: sign in with email link creates verified user", r.ok and r.json()["emailVerified"], r.text)
r = requests.patch(f"{B}/admin/auth/users/backend-user-1", json={"disabled": True}, headers=ADMIN)
r = post("/v1/accounts/lookup", {"idToken": custom["idToken"]})
check("auth: disabled user's token rejected", r.status_code == 403 and "USER_DISABLED" in r.text, r.text)
r = requests.get(f"{B}/admin/auth/users?q=rohit", headers=ADMIN)
check("auth: admin search users", r.ok and r.json()["users"][0]["email"] == "rohit@example.com", r.text)
r = post("/v1/accounts/signInWithIdp", {"providerId": "google.com", "idToken": "x"})
check("auth: Google sign-in unconfigured → OPERATION_NOT_ALLOWED", r.status_code == 403, r.text)
bearer = {"authorization": "Bearer " + user["idToken"]}
uid = user["localId"]

# ── Storage ──
r = requests.post(f"{B}/v1/storage/b/default/o?name=users/{uid}/hello.txt", data=b"hello world", headers={**H, **bearer, "content-type": "text/plain"})
meta = r.json()
check("storage: simple upload", r.ok and meta["size"] == "11" and meta["md5Hash"] == base64.b64encode(hashlib.md5(b"hello world").digest()).decode(), r.text)
enc = urllib.parse.quote(f"users/{uid}/hello.txt", safe="")
r = requests.get(f"{B}/v1/storage/b/default/o/{enc}?alt=media", headers={**H, **bearer})
check("storage: download", r.content == b"hello world", r.text)
r = requests.get(f"{B}/v1/storage/b/default/o/{enc}?alt=media", headers={**H, **bearer, "range": "bytes=6-"})
check("storage: range download", r.status_code == 206 and r.content == b"world", r.status_code)
r = requests.get(f"{B}/v1/storage/b/default/o/{enc}?alt=media", headers=H)
check("storage: default rules deny signed-out read", r.status_code == 403, r.text)
r = requests.get(f"{B}/v1/storage/b/default/o/{enc}?alt=media&token={meta['downloadTokens']}")
check("storage: download token bypasses rules", r.content == b"hello world", r.text)
body = b'--xyz\r\nContent-Type: application/json\r\n\r\n{"name":"users/' + uid.encode() + b'/m.json","metadata":{"kind":"test"}}\r\n--xyz\r\nContent-Type: application/json\r\n\r\n{"a":1}\r\n--xyz--\r\n'
r = requests.post(f"{B}/v1/storage/b/default/o", data=body, headers={**H, **bearer, "content-type": "multipart/related; boundary=xyz"})
check("storage: multipart upload with custom metadata", r.ok and r.json()["metadata"]["kind"] == "test", r.text)
big = os.urandom(256 * 1024 * 2 + 1000)
r = requests.post(f"{B}/v1/storage/b/default/o?name=users/{uid}/big.bin", json={"contentType": "application/octet-stream"}, headers={**H, **bearer, "x-goog-upload-protocol": "resumable", "x-goog-upload-command": "start", "x-goog-upload-header-content-length": str(len(big))})
url = r.headers.get("x-goog-upload-url")
check("storage: resumable start", r.ok and url, r.text)
r = requests.put(B + url, data=big[:262144], headers={**H, **bearer, "x-goog-upload-command": "upload", "x-goog-upload-offset": "0"})
r = requests.put(B + url, data=b"", headers={**H, "x-goog-upload-command": "query"})
check("storage: resumable query reports progress", r.headers.get("x-goog-upload-size-received") == "262144", dict(r.headers))
r = requests.put(B + url, data=big[262144:], headers={**H, **bearer, "x-goog-upload-command": "upload, finalize", "x-goog-upload-offset": "262144"})
check("storage: resumable finalize", r.ok and r.headers.get("x-goog-upload-status") == "final" and r.json()["size"] == str(len(big)), r.text)
r = requests.get(f"{B}/v1/storage/b/default/o/{urllib.parse.quote(f'users/{uid}/big.bin', safe='')}?alt=media", headers={**H, **bearer})
check("storage: resumable content intact", r.content == big, len(r.content))
r = requests.get(f"{B}/v1/storage/b/default/o?prefix=users/{uid}/&delimiter=/", headers={**H, **bearer})
check("storage: list", r.ok and len(r.json()["items"]) == 3, r.text)
r = requests.put(f"{B}/admin/rules/storage", json={"rules": [{"match": "users/{uid}/{rest=**}", "read": "auth.uid == uid", "create": "auth.uid == uid && request.resource.size < 1000", "update": "auth.uid == uid && request.resource.size < 1000", "delete": "auth.uid == uid"}]}, headers=ADMIN)
time.sleep(5.5)
r = requests.post(f"{B}/v1/storage/b/default/o?name=users/{uid}/too-big.bin", data=b"x" * 2000, headers={**H, **bearer})
check("storage: custom rule size limit enforced", r.status_code == 403, r.text)
r = requests.post(f"{B}/v1/storage/b/default/o?name=users/someone-else/x.txt", data=b"x", headers={**H, **bearer})
check("storage: custom rule owner-only enforced", r.status_code == 403, r.text)
r = requests.delete(f"{B}/v1/storage/b/default/o/{enc}", headers={**H, **bearer})
check("storage: delete", r.status_code == 204, r.text)

# ── Functions ──
requests.post(f"{B}/admin/functions", json={"name": "echo", "url": "http://127.0.0.1:8099/echo", "require_auth": True}, headers=ADMIN)
requests.post(f"{B}/admin/functions", json={"name": "stream", "url": "http://127.0.0.1:8099/stream"}, headers=ADMIN)
r = post("/v1/functions/echo", {"data": {"x": 1}}, headers={**bearer, "x-dm-appcheck": appcheck})
out = r.json().get("result", {})
check("functions: call with auth + App Check context + signature", r.ok and out["echo"] == {"x": 1} and out["uid"] == uid and out["sig_ok"] and out["appcheck"]["provider"] == "debug", r.text)
r = post("/v1/functions/echo", {"data": {"x": 1}})
check("functions: require_auth enforced", r.status_code == 401, r.text)
r = post("/v1/functions/echo", {"data": {"fail": True}}, headers=bearer)
check("functions: function error mapped to HTTP 400", r.status_code == 400 and r.json()["error"]["status"] == "INVALID_ARGUMENT", r.text)
r = post("/v1/functions/stream", {"data": {}}, headers={"accept": "text/event-stream"}, stream=True)
chunks = [l for l in r.iter_lines() if l]
check("functions: streaming callable passes SSE through", r.headers["content-type"].startswith("text/event-stream") and len(chunks) == 4, chunks)
r = post("/v1/functions/missing", {"data": {}})
check("functions: unknown function 404", r.status_code == 404, r.text)

# ── Database ──
requests.put(f"{B}/admin/rules/database", json={"rules": [
    {"match": "users/{uid}/{rest=**}", "read": "auth.uid == uid", "write": "auth.uid == uid"},
    {"match": "posts/{id}", "read": "true", "create": "auth != null && request.resource.data.author == auth.uid", "update": "auth.uid == resource.data.author", "delete": "auth.uid == resource.data.author"},
    {"match": "counters/{id}", "read": "true", "write": "auth != null"},
]}, headers=ADMIN)
time.sleep(5.5)

# listener first, so it sees the writes
ws = websocket.create_connection(B.replace("http", "ws") + "/v1/db/listen", timeout=10)
ws.send(json.dumps({"type": "auth", "apiKey": "test-key", "appId": APP}))
check("db/ws: authenticated (signed out)", json.loads(ws.recv())["type"] == "authenticated")
ws.send(json.dumps({"type": "listen", "targetId": 1, "query": {"collection": "posts", "orderBy": [{"field": "score", "direction": "desc"}], "limit": 2}}))
first = json.loads(ws.recv())
check("db/ws: initial empty snapshot", first["type"] == "snapshot" and first["documents"] == [], first)

def write(writes, headers=bearer, tx=None):
    body = {"writes": writes}
    if tx: body["transaction"] = tx
    return post("/v1/db/commit", body, headers=headers)

r = write([
    {"op": "set", "path": "posts/p1", "data": {"author": uid, "title": "One", "score": 10, "tags": ["a"], "at": {"__op": "serverTimestamp"}}},
    {"op": "set", "path": "posts/p2", "data": {"author": uid, "title": "Two", "score": 30}},
    {"op": "set", "path": "posts/p3", "data": {"author": uid, "title": "Three", "score": 20, "loc": {"__type": "geopoint", "latitude": 19.0, "longitude": 72.8}}},
])
check("db: batched set (atomic, 3 docs)", r.ok and len(r.json()["results"]) == 3, r.text)
snap = json.loads(ws.recv())
check("db/ws: live snapshot after batch (top 2 by score)", [d["id"] for d in snap["documents"]] == ["p2", "p3"] and all(c["type"] == "added" for c in snap["changes"]), snap)
r = write([{"op": "set", "path": "posts/p4", "data": {"author": "someone-else", "title": "Spoof"}}])
check("db: rules reject spoofed author", r.status_code == 403, r.text)
r = write([{"op": "set", "path": "posts/p9", "data": {"author": uid}}, {"op": "set", "path": "posts/p10", "data": {"author": "x"}}])
r2 = requests.get(f"{B}/v1/db/documents/posts/p9", headers=H)
check("db: batch is all-or-nothing", r.status_code == 403 and r2.json()["exists"] is False, (r.text, r2.text))
r = write([{"op": "update", "path": "posts/p1", "data": {"score": {"__op": "increment", "by": 25}, "tags": {"__op": "arrayUnion", "values": ["b", "a"]}, "meta.views": 1}}])
doc = requests.get(f"{B}/v1/db/documents/posts/p1", headers=H).json()["document"]
check("db: update with increment / arrayUnion / dotted path", doc["data"]["score"] == 35 and doc["data"]["tags"] == ["a", "b"] and doc["data"]["meta"]["views"] == 1 and doc["data"]["at"]["__type"] == "timestamp", doc)
snap = json.loads(ws.recv())
check("db/ws: re-ordered snapshot after update", [d["id"] for d in snap["documents"]] == ["p1", "p2"] and {c["type"] for c in snap["changes"]} >= {"added", "removed"}, snap)
r = post("/v1/db/query", {"collection": "posts", "where": {"and": [{"field": "score", "op": ">=", "value": 15}, {"field": "author", "op": "==", "value": uid}]}, "orderBy": [{"field": "score"}]})
check("db: compound query", [d["id"] for d in r.json()["documents"]] == ["p3", "p2", "p1"], r.text)
r = post("/v1/db/query", {"collection": "posts", "orderBy": [{"field": "score"}], "startAfter": [20], "limit": 1})
check("db: cursor startAfter", [d["id"] for d in r.json()["documents"]] == ["p2"], r.text)
r = post("/v1/db/query", {"collection": "posts", "where": {"or": [{"field": "title", "op": "==", "value": "One"}, {"field": "tags", "op": "array-contains", "value": "zzz"}, {"field": "score", "op": "in", "value": [30]}]}})
check("db: OR + in + array-contains", sorted(d["id"] for d in r.json()["documents"]) == ["p1", "p2"], r.text)
r = post("/v1/db/aggregate", {"query": {"collection": "posts"}, "aggregations": [{"alias": "n", "op": "count"}, {"alias": "total", "op": "sum", "field": "score"}, {"alias": "avg", "op": "avg", "field": "score"}]})
check("db: aggregation count/sum/avg", r.json()["result"] == {"n": 3, "total": 85.0, "avg": 85 / 3}, r.text)
write([{"op": "set", "path": f"users/{uid}/notes/n1", "data": {"text": "mine"}}])
r = requests.get(f"{B}/v1/db/documents/users/{uid}/notes/n1", headers={**H, **bearer})
r2 = requests.get(f"{B}/v1/db/documents/users/{uid}/notes/n1", headers=H)
other = post("/v1/accounts/signUp", {}).json()["idToken"]
r3 = requests.get(f"{B}/v1/db/documents/users/{uid}/notes/n1", headers={**H, "authorization": "Bearer " + other})
check("db: owner-only subcollection rule", r.ok and r2.status_code == 403 and r3.status_code == 403, (r.text, r2.text, r3.text))
r = post("/v1/db/query", {"collectionGroup": "notes"}, headers=bearer)
check("db: collection-group query obeys rules (denied for others' docs)", r.ok and len(r.json()["documents"]) == 1, r.text)

# transactions: conflict → ABORTED, retry succeeds
write([{"op": "set", "path": "counters/c1", "data": {"n": 0}}])
tx = post("/v1/db/beginTransaction", {}, headers=bearer).json()["transaction"]
n = requests.get(f"{B}/v1/db/documents/counters/c1?transaction={tx}", headers={**H, **bearer}).json()["document"]["data"]["n"]
write([{"op": "set", "path": "counters/c1", "data": {"n": 100}}])  # concurrent writer
r = write([{"op": "set", "path": "counters/c1", "data": {"n": n + 1}}], tx=tx)
check("db: transaction aborted on concurrent change", r.status_code == 409 and "ABORTED" in r.text, r.text)
tx = post("/v1/db/beginTransaction", {}, headers=bearer).json()["transaction"]
n = requests.get(f"{B}/v1/db/documents/counters/c1?transaction={tx}", headers={**H, **bearer}).json()["document"]["data"]["n"]
r = write([{"op": "set", "path": "counters/c1", "data": {"n": n + 1}}], tx=tx)
final = requests.get(f"{B}/v1/db/documents/counters/c1", headers=H).json()["document"]["data"]["n"]
check("db: transaction retry commits", r.ok and final == 101, (r.text, final))
r = write([{"op": "set", "path": "posts/p1", "data": {"x": 1}, "precondition": {"exists": False}}])
check("db: precondition exists=false", r.status_code == 409, r.text)

ws.send(json.dumps({"type": "listen", "targetId": 2, "document": "posts/p2"}))
snap = json.loads(ws.recv())
check("db/ws: document listener", snap["targetId"] == 2 and snap["exists"] and snap["document"]["data"]["title"] == "Two", snap)
write([{"op": "delete", "path": "posts/p2"}])
msgs = [json.loads(ws.recv()), json.loads(ws.recv())]
doc_msg = next(m for m in msgs if m["targetId"] == 2)
q_msg = next(m for m in msgs if m["targetId"] == 1)
check("db/ws: delete reaches doc + query listeners", doc_msg["exists"] is False and "removed" in {c["type"] for c in q_msg["changes"]}, msgs)
ws.send(json.dumps({"type": "listen", "targetId": 3, "document": f"users/{uid}/notes/n1"}))
err = json.loads(ws.recv())
check("db/ws: rules apply to listeners", err["type"] == "error" and err["code"] == "PERMISSION_DENIED", err)
ws.close()

# admin browse
r = requests.get(f"{B}/admin/db/collections", headers=ADMIN)
check("admin: db collections", {c["path"] for c in r.json()["collections"]} >= {"posts", "counters"}, r.text)
r = requests.get(f"{B}/admin/platform/overview", headers=ADMIN)
check("admin: platform overview", r.ok and r.json()["users"] >= 3 and r.json()["db_documents"] >= 4, r.text)
r = requests.post(f"{B}/admin/rules/database/test", json={"path": "posts/x", "op": "create", "auth": {"uid": "u1"}, "request": {"data": {"author": "u2"}}}, headers=ADMIN)
check("admin: rules simulator", r.ok and r.json()["allowed"] is False, r.text)

# ── JSON RPC for the original five ──
r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.RemoteConfigService/FetchConfig", json={"context": {"appId": APP, "installationId": inst["fid"], "platform": "android"}}, headers=H)
check("rpc: FetchConfig as proto3 JSON", r.ok and str(r.json().get("state", "FETCH_STATE_NO_TEMPLATE")).startswith("FETCH_STATE_"), r.text)
r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.MessagingService/RegisterDevice", json={"context": {"appId": APP, "installationId": inst["fid"]}}, headers=H)
check("rpc: validation error → HTTP 400", r.status_code == 400, r.text)

passed = sum(1 for _, ok in results if ok)
print(f"\n{passed}/{len(results)} passed")
