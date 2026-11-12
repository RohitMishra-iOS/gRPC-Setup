# Uninstall detection and the installs report (dm-specs MSG-020…MSG-022, ANA-016), end to end.
# Starts fake APNs and FCM servers and its own server (database dm_e2e_uninstall) pointed at them
# with the throw-away keys in tests/fixtures/push-test-keys. Needs MongoDB on 27017 and a built
# server:   cargo build && python3 tests/e2e_uninstall.py [path/to/grpc-status-server]
import json, os, subprocess, sys, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymongo
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
BINARY = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "target", "debug", "grpc-status-server")
APNS_PORT, FCM_PORT, PORT, GRPC_PORT = 18771, 18772, 18093, 18092
B = f"http://127.0.0.1:{PORT}"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
DBNAME = "dm_e2e_uninstall"
MONGO = "mongodb://127.0.0.1:27017/?directConnection=true"
RUN = uuid.uuid4().hex[:6]
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:600]}"))


# ── Fake push services ───────────────────────────────────────────────────────
apns_calls, fcm_calls = [], []
NOW_MS = int(time.time() * 1000)
GONE_AT_MS = NOW_MS - 3_600_000          # Apple: the token stopped working an hour ago


class Apns(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_POST(self):
        token = self.path.rsplit("/", 1)[-1]
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        apns_calls.append({"token": token, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
        if token.startswith("dead"):
            status, reply = 410, {"reason": "Unregistered", "timestamp": GONE_AT_MS}
        elif token.startswith("badd"):
            status, reply = 400, {"reason": "BadDeviceToken"}
        else:
            status, reply = 200, None
        self.send_response(status)
        data = json.dumps(reply).encode() if reply else b""
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Fcm(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        if self.path == "/token":
            status, reply = 200, {"access_token": "fake-token", "expires_in": 3600, "token_type": "Bearer"}
        else:
            message = json.loads(body)["message"]
            fcm_calls.append(message)
            if message["token"].startswith("gone"):
                status, reply = 404, {"error": {"code": 404, "message": "Requested entity was not found.", "status": "NOT_FOUND",
                                                "details": [{"@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": "UNREGISTERED"}]}}
            else:
                status, reply = 200, {"name": "projects/p/messages/1"}
        data = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


for port, handler in [(APNS_PORT, Apns), (FCM_PORT, Fcm)]:
    threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", port), handler).serve_forever, daemon=True).start()

# ── Server pointed at them ───────────────────────────────────────────────────
db = pymongo.MongoClient(MONGO)[DBNAME]
db.client.drop_database(DBNAME)
keys = os.path.join(HERE, "fixtures", "push-test-keys")
service_account = {"type": "service_account", "project_id": "dm-test", "client_email": "push@dm-test.iam.example",
                   "private_key": open(os.path.join(keys, "fcm-test.pem")).read()}
env = {k: v for k, v in os.environ.items() if not k.startswith(("DM_", "APNS_", "MONGODB_"))}
env.update({
    "PORT": str(PORT), "GRPC_PORT": str(GRPC_PORT), "MONGODB_URI": MONGO, "MONGODB_DB_NAME": DBNAME,
    "DM_API_KEYS": f"{APP}:test-key", "DM_ADMIN_KEY": "test-admin", "DM_TOKEN_SECRET": "e2e-secret-0123456789abcdef",
    "DM_ANALYTICS_TIMEZONE": "Asia/Kolkata",
    "APNS_KEY_PATH": os.path.join(keys, "apns-test.p8"), "APNS_KEY_ID": "TESTKEY123", "APNS_TEAM_ID": "TESTTEAM12",
    "APNS_BASE_URL": f"http://127.0.0.1:{APNS_PORT}",
    "DM_FCM_SERVICE_ACCOUNT": json.dumps(service_account),
    "DM_FCM_BASE_URL": f"http://127.0.0.1:{FCM_PORT}", "DM_FCM_TOKEN_URL": f"http://127.0.0.1:{FCM_PORT}/token",
    "DM_UNINSTALL_PROBE_HOURS": "24", "DM_UNINSTALL_PROBE_TICK_SECONDS": "3600",
})
server = subprocess.Popen([os.path.abspath(BINARY)], env=env, cwd="/", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
try:
    for _ in range(60):
        try:
            if requests.get(f"{B}/health", timeout=1).ok:
                break
        except requests.RequestException:
            pass
        time.sleep(0.5)

    def ctx(inst, platform):
        return {"appId": APP, "installationId": inst, "platform": platform, "bundleId": "com.dmart.ready", "appVersion": "5.13.0",
                "appBuild": "513", "osVersion": "18.1" if platform == "ios" else "14", "deviceModel": "iPhone16,2" if platform == "ios" else "Pixel 8",
                "locale": "en_IN", "timezone": "Asia/Kolkata", "sdkVersion": "test"}

    def register(inst, platform, **token):
        body = {"context": ctx(inst, platform), "notificationAuthorization": "authorized", **token}
        r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.MessagingService/RegisterDevice", json=body, headers=H, timeout=30)
        assert r.ok, r.text
        return r.json()["registrationToken"]

    def first_open(inst, platform, source, at_us=None, previous=None, device_key=None):
        at_us = at_us or int(time.time() * 1e6) - 7_200_000_000  # installed two hours ago
        params = [{"key": "first_open_time", "value": {"intValue": str(at_us // 1000)}}]
        if previous:  # iOS SDK: the install before this one on the device, from the Keychain
            params.append({"key": "previous_installation_id", "value": {"stringValue": previous}})
        if device_key:  # Android SDK: hash that survives a reinstall (ANDROID_ID)
            params.append({"key": "device_install_key", "value": {"stringValue": device_key}})
        body = {"context": ctx(inst, platform), "batchId": f"fo-{inst}", "userPseudoId": inst,
                "appInfo": {"id": "com.dmart.ready", "version": "5.13.0", "installSource": source},
                "events": [{"name": "first_open", "timestampMicros": str(at_us), "params": params}]}
        r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.AnalyticsService/UploadEvents", json=body, headers=H, timeout=30)
        assert r.ok, r.text

    hexid = lambda prefix: (prefix + uuid.uuid4().hex + uuid.uuid4().hex)[:64]
    I = {k: f"{k}-{RUN}" for k in ["ios_keep", "ios_gone", "and_keep", "and_gone", "ios_rotated", "ios_badcfg", "ios_campaign"]}
    tokens = {
        "ios_keep": register(I["ios_keep"], "ios", apnsToken=hexid("aaaa"), apnsEnvironment="production"),
        "ios_gone": register(I["ios_gone"], "ios", apnsToken=hexid("dead"), apnsEnvironment="production"),
        "ios_rotated": register(I["ios_rotated"], "ios", apnsToken=hexid("dead"), apnsEnvironment="production"),
        "ios_badcfg": register(I["ios_badcfg"], "ios", apnsToken=hexid("badd"), apnsEnvironment="sandbox"),
        "and_keep": register(I["and_keep"], "android", pushTransport="fcm", fcmToken=f"keep:{RUN}:" + "x" * 140),
        "and_gone": register(I["and_gone"], "android", pushTransport="fcm", fcmToken=f"gone:{RUN}:" + "x" * 140),
    }
    first_open(I["ios_keep"], "ios", "com.apple.AppStore")
    first_open(I["ios_gone"], "ios", "com.apple.TestFlight")
    first_open(I["ios_badcfg"], "ios", "xcode")
    first_open(I["and_keep"], "android", "com.android.vending")
    first_open(I["and_gone"], "android", "android_studio")
    # ios_rotated was used *after* Apple says its token stopped working → not an uninstall.
    first_open(I["ios_rotated"], "ios", "com.apple.AppStore", at_us=int(time.time() * 1e6) - 60_000_000)

    # ── The silent check ──────────────────────────────────────────────────────
    r = requests.post(f"{B}/admin/messaging/uninstall-check", params={"app_id": APP, "force": "true"}, headers=ADMIN, timeout=60)
    run = r.json()
    check("[MSG-021] a check round runs over the app's iOS and Android devices", r.ok and run.get("checked") == 6, run)
    check("[MSG-020] two uninstalls found (iOS 410 Unregistered, Android UNREGISTERED)", run.get("uninstalls") == 2, run)

    probe = next((c for c in apns_calls if c["token"] == db.devices.find_one({"_id": tokens["ios_keep"]})["apns_token"]), None)
    check("[MSG-021] APNs check is a silent background push", probe and probe["headers"].get("apns-push-type") == "background"
          and probe["headers"].get("apns-priority") == "5" and probe["body"].get("aps") == {"content-available": 1}
          and probe["body"]["dm"].get("label") == "dm_uninstall_probe", probe)
    fprobe = next((m for m in fcm_calls if m["token"].startswith(f"keep:{RUN}")), None)
    check("[MSG-021] FCM check is a data-only, normal-priority message", fprobe and "notification" not in fprobe
          and fprobe["android"]["priority"] == "NORMAL" and json.loads(fprobe["data"]["dm"])["label"] == "dm_uninstall_probe", fprobe)

    removes = {r["user_pseudo_id"]: r for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_")
               for r in db[c].find({"event_name": "app_remove"})}
    p = lambda row, k: next((e["value"]["string_value"] for e in row["event_params"] if e["key"] == k), None)
    check("[MSG-020] app_remove logged for the removed iOS and Android installs only", set(removes) == {I["ios_gone"], I["and_gone"]}, sorted(removes))
    ios_rm, and_rm = removes.get(I["ios_gone"], {}), removes.get(I["and_gone"], {})
    check("[MSG-020] iOS app_remove is dated when Apple says the token stopped working", ios_rm and abs(ios_rm["event_timestamp"] - GONE_AT_MS * 1000) < 1000, ios_rm.get("event_timestamp"))
    check("[MSG-020] app_remove is a full GA4 row (platform, app_info, detected_by, origin auto)",
          ios_rm.get("platform") == "IOS" and and_rm.get("platform") == "ANDROID" and p(ios_rm, "detected_by") == "apns" and p(and_rm, "detected_by") == "fcm"
          and p(ios_rm, "DM_event_origin") == "auto" and ios_rm["app_info"]["install_source"] == "com.apple.TestFlight" and ios_rm["device"]["operating_system"] == "iOS", (ios_rm, and_rm))
    dev = {k: db.devices.find_one({"_id": t}) for k, t in tokens.items()}
    check("[MSG-020] removed installs' devices are deactivated as uninstalled", dev["ios_gone"]["active"] is False and dev["ios_gone"]["invalid_reason"] == "uninstalled"
          and dev["and_gone"]["invalid_reason"] == "uninstalled", (dev["ios_gone"], dev["and_gone"]))
    check("[MSG-020] a token that died while the install kept being used is not an uninstall", I["ios_rotated"] not in removes and dev["ios_rotated"]["active"] is False, dev["ios_rotated"])
    check("[MSG-020] BadDeviceToken (configuration) is not an uninstall and the device stays active", I["ios_badcfg"] not in removes and dev["ios_badcfg"]["active"] is True, dev["ios_badcfg"])
    check("[MSG-021] installed devices stay active and are marked checked", dev["ios_keep"]["active"] and "probed_at" in dev["ios_keep"] and dev["and_keep"]["active"] and "probed_at" in dev["and_keep"])

    again = requests.post(f"{B}/admin/messaging/uninstall-check", params={"app_id": APP}, headers=ADMIN, timeout=60).json()
    check("[MSG-021] devices checked within the interval are not checked again", again.get("checked") == 0, again)

    # ── A regular campaign also detects an uninstall, once ───────────────────
    tokens["ios_campaign"] = register(I["ios_campaign"], "ios", apnsToken=hexid("dead"), apnsEnvironment="production")
    first_open(I["ios_campaign"], "ios", "com.apple.AppStore")
    sent = requests.post(f"{B}/admin/messaging/send", headers=ADMIN, timeout=60,
                         json={"app_id": APP, "target": {"installation_id": I["ios_campaign"]}, "notification": {"title": "Sale", "body": "50% off"}}).json()
    check("[MSG-020] a regular send reports the uninstall", sent.get("uninstalls") == 1, sent)
    count = lambda inst: sum(db[c].count_documents({"event_name": "app_remove", "user_pseudo_id": inst}) for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_"))
    check("[MSG-020] … and logs app_remove for it", count(I["ios_campaign"]) == 1)
    db.devices.update_one({"_id": tokens["ios_campaign"]}, {"$set": {"active": True}})
    requests.post(f"{B}/admin/messaging/send", headers=ADMIN, timeout=60, json={"app_id": APP, "target": {"installation_id": I["ios_campaign"]}, "notification": {"title": "Again"}})
    check("[MSG-020] app_remove is logged once per install", count(I["ios_campaign"]) == 1, count(I["ios_campaign"]))

    # ── Older SDKs reporting the check as a received message ────────────────
    now_us = int(time.time() * 1e6)
    body = {"context": ctx(I["ios_keep"], "ios"), "batchId": f"nr-{RUN}", "userPseudoId": I["ios_keep"], "events": [
        {"name": "notification_receive", "timestampMicros": str(now_us), "params": [{"key": "label", "value": {"stringValue": "dm_uninstall_probe"}}]},
        {"name": "notification_receive", "timestampMicros": str(now_us + 1), "params": [{"key": "label", "value": {"stringValue": "promo"}}]}]}
    requests.post(f"{B}/v1/rpc/dm.sdk.v1.AnalyticsService/UploadEvents", json=body, headers=H, timeout=30)
    labels = [p(r, "label") for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_")
              for r in db[c].find({"event_name": "notification_receive", "user_pseudo_id": I["ios_keep"]})]
    check("[MSG-021] the check is not stored as notification_receive (older SDKs)", labels == ["promo"], labels)

    # ── Delete and reinstall on the same iPhone: iOS hands back the same APNs token ──
    same_token = hexid("cafe")
    I["ios_first"], I["ios_again"] = f"ios_first-{RUN}", f"ios_again-{RUN}"
    first_open(I["ios_first"], "ios", "com.apple.AppStore")
    tokens["ios_first"] = register(I["ios_first"], "ios", apnsToken=same_token, apnsEnvironment="sandbox")
    register(I["ios_first"], "ios", apnsToken=same_token, apnsEnvironment="sandbox")  # relaunch: same install
    check("[MSG-020] relaunching the same install is not an uninstall", count(I["ios_first"]) == 0)
    first_open(I["ios_again"], "ios", "xcode", at_us=int(time.time() * 1e6))
    before_reinstall = int(time.time() * 1e6)
    tokens["ios_again"] = register(I["ios_again"], "ios", apnsToken=same_token, apnsEnvironment="sandbox")
    rows = [r for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_") for r in db[c].find({"event_name": "app_remove", "user_pseudo_id": I["ios_first"]})]
    check("[MSG-020] a reinstall with the same token logs app_remove for the earlier install",
          len(rows) == 1 and p(rows[0], "detected_by") == "reinstall" and rows[0]["event_timestamp"] <= before_reinstall + 1_000_000, rows)
    old, new = db.devices.find_one({"_id": tokens["ios_first"]}), db.devices.find_one({"_id": tokens["ios_again"]})
    check("[MSG-020] the earlier install's device is uninstalled, the new one active", old["active"] is False and old["invalid_reason"] == "uninstalled" and new["active"] is True, (old, new))

    # ── Delete and reinstall without push (no token): the Keychain names the removed install ──
    I["kc_first"], I["kc_again"], I["kc_other"] = f"kc_first-{RUN}", f"kc_again-{RUN}", f"kc_other-{RUN}"
    first_open(I["kc_first"], "ios", "adhoc")
    reinstalled_at = int(time.time() * 1e6)
    first_open(I["kc_again"], "ios", "adhoc", at_us=reinstalled_at, previous=I["kc_first"])
    first_open(I["kc_again"], "ios", "adhoc", at_us=reinstalled_at, previous=I["kc_first"])  # retried upload
    first_open(I["kc_other"], "ios", "adhoc", at_us=reinstalled_at, previous=f"never-seen-{RUN}")
    time.sleep(1.5)
    rows = [r for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_") for r in db[c].find({"event_name": "app_remove", "user_pseudo_id": I["kc_first"]})]
    check("[ANA-017] a first_open naming the previous install logs its app_remove once, even without push",
          len(rows) == 1 and p(rows[0], "detected_by") == "reinstall" and rows[0]["event_timestamp"] == reinstalled_at - 1_000_000, rows)
    check("[ANA-017] an unknown previous install logs nothing", count(f"never-seen-{RUN}") == 0)

    # ── Android delete and reinstall: the device install key names the removed install ──
    I["dk_first"], I["dk_again"] = f"dk_first-{RUN}", f"dk_again-{RUN}"
    key = uuid.uuid4().hex
    first_open(I["dk_first"], "android", "com.sec.android.app.samsungapps", device_key=key)
    time.sleep(1)
    check("[ANA-017] the first install with a device key is not an uninstall", count(I["dk_first"]) == 0)
    reinstalled_at = int(time.time() * 1e6)
    first_open(I["dk_again"], "android", "com.sec.android.app.samsungapps", at_us=reinstalled_at, device_key=key)
    time.sleep(1.5)
    rows = [r for c in db.list_collection_names() if c.startswith(f"analytics_{APP}.events_") for r in db[c].find({"event_name": "app_remove", "user_pseudo_id": I["dk_first"]})]
    check("[ANA-017] Android: the same device key from a new install logs app_remove for the earlier one",
          len(rows) == 1 and p(rows[0], "detected_by") == "reinstall" and rows[0]["event_timestamp"] == reinstalled_at - 1_000_000 and rows[0]["platform"] == "ANDROID", rows)
    check("[ANA-017] the key now belongs to the new install", db.dm_device_install_keys.find_one({"_id": f"{APP}:{key}"})["installation_id"] == I["dk_again"])

    # ── Installs vs uninstalls report ────────────────────────────────────────
    today = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 19800))
    yesterday = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 19800 - 86400))
    rep = requests.get(f"{B}/admin/analytics/{APP}/installs", params={"start": yesterday, "end": today, "tz": "Asia/Kolkata"}, headers=ADMIN, timeout=60).json()
    check("[MSG-022] totals: 14 installs, 6 uninstalls, net 8", rep.get("totals") == {"installs": 14, "uninstalls": 6, "net": 8}, rep.get("totals"))
    check("[MSG-022] daily buckets add up", sum(b["installs"] for b in rep["series"]["buckets"]) == 14 and sum(b["uninstalls"] for b in rep["series"]["buckets"]) == 6, rep["series"])
    plat = {x["platform"]: x for x in rep["platforms"]}
    check("[MSG-022] by platform", plat.get("IOS", {}).get("installs") == 10 and plat.get("IOS", {}).get("uninstalls") == 4
          and plat.get("ANDROID", {}).get("installs") == 4 and plat.get("ANDROID", {}).get("uninstalls") == 2, rep["platforms"])
    src = {(x["label"], x["platform"]): x for x in rep["sources"]}
    check("[ANA-016] by install source with readable names", src.get(("App Store", "IOS"), {}).get("installs") == 4 and src.get(("TestFlight", "IOS"), {}).get("uninstalls") == 1
          and src.get(("Xcode (development build)", "IOS"), {}).get("installs") == 2 and src.get(("Google Play", "ANDROID"), {}).get("installs") == 1
          and src.get(("Android Studio / adb", "ANDROID"), {}).get("uninstalls") == 1, rep["sources"])
    d = rep.get("detection", {})
    check("[MSG-022] detection status: methods, credentials, devices with push", d.get("detected_by") == {"apns": 2, "fcm": 1, "reinstall": 3} and d.get("apns_configured") and d.get("fcm_configured")
          and d["push_devices"]["apns"]["devices"] >= 1 and d.get("last_check_at"), d)
    ios_only = requests.get(f"{B}/admin/analytics/{APP}/installs", params={"start": yesterday, "end": today, "platform": "IOS"}, headers=ADMIN, timeout=60).json()
    check("[MSG-022] platform filter", ios_only["totals"] == {"installs": 10, "uninstalls": 4, "net": 6}, ios_only["totals"])
    st = requests.get(f"{B}/admin/messaging/uninstall-check", headers=ADMIN, timeout=30).json()
    check("[MSG-021] status endpoint", st.get("enabled") and st.get("interval_hours") == 24 and st.get("total_uninstalls") == 2 and st.get("last_run_at"), st)
    check("[MSG-021] admin key required", requests.post(f"{B}/admin/messaging/uninstall-check", timeout=30).status_code == 401)
finally:
    server.terminate()
    try:
        out = server.communicate(timeout=10)[0]
    except subprocess.TimeoutExpired:
        server.kill()
        out = ""
    if not all(results):
        print("--- server log (tail) ---\n" + out[-3000:])
    db.client.drop_database(DBNAME)

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
