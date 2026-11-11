# firebase_* → DM_* parameter names (dm-specs ANA-015), end to end: uploads from SDK versions that
# still send firebase_* are stored with DM_* names; rows stored before the rename are migrated;
# app code cannot use the DM_ prefix.
# Run against a server started with tests/e2e.env:
#   python3 tests/e2e_dm_keys.py
import os, time, uuid
from datetime import datetime, timedelta, timezone

import pymongo
import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
DB = pymongo.MongoClient(os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017/?directConnection=true"))[os.environ.get("MONGODB_DB_NAME", "dm_e2e")]
IST = timezone(timedelta(hours=5, minutes=30))
RUN = uuid.uuid4().hex[:8]
OLD = ["firebase_screen", "firebase_screen_class", "firebase_screen_id", "firebase_previous_screen", "firebase_previous_class", "firebase_previous_id", "firebase_event_origin"]
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:500]}"))


def p(key, **value):
    return {"key": key, "value": value}


def keys(row):
    return [e["key"] for e in row["event_params"]]


t0 = int(time.time() * 1_000_000)
table = f"analytics_{APP}.events_" + datetime.fromtimestamp(t0 / 1e6, IST).strftime("%Y%m%d")

# 1. An upload from an SDK that still uses the firebase_* names (iOS ≤ 2.1.0, Android/Web before the rename).
ctx = {"appId": APP, "installationId": f"old-{RUN}", "platform": "ios", "appVersion": "2.0.0"}
r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.AnalyticsService/UploadEvents", headers=H, timeout=30, json={
    "context": ctx, "batchId": f"old-{RUN}", "userPseudoId": f"old-{RUN}",
    "events": [
        {"name": "screen_view", "timestampMicros": str(t0), "params": [
            p("firebase_screen", stringValue="Cart"), p("firebase_screen_class", stringValue="CartVC"), p("firebase_screen_id", intValue="11"),
            p("firebase_previous_screen", stringValue="Home"), p("firebase_previous_class", stringValue="HomeVC"), p("firebase_previous_id", intValue="10"),
            p("firebase_event_origin", stringValue="auto"), p("ga_session_id", intValue="1762850400")]},
        {"name": "add_to_cart", "timestampMicros": str(t0 + 1), "params": [p("DM_custom", intValue="1"), p("dm_custom", intValue="1"), p("qty", intValue="2"), p("DM_screen", stringValue="Cart")]},
    ],
})
check("[ANA-015] upload accepted", r.ok, r.text)
time.sleep(0.3)
sv = DB[table].find_one({"user_pseudo_id": f"old-{RUN}", "event_name": "screen_view"})
check("[ANA-015] firebase_* names from older SDKs are stored as DM_*", sv and set(keys(sv)) >= {"DM_screen", "DM_screen_class", "DM_screen_id", "DM_previous_screen", "DM_previous_class", "DM_previous_id", "DM_event_origin"}
      and not any(k.startswith("firebase_") for k in keys(sv)), sv and keys(sv))
check("[ANA-015] app_info.DM_app_id", sv and sv["app_info"].get("DM_app_id") == APP and "firebase_app_id" not in sv["app_info"], sv and sv.get("app_info"))
cart = DB[table].find_one({"user_pseudo_id": f"old-{RUN}", "event_name": "add_to_cart"})
check("[ANA-015] DM_ is reserved for the SDK: a custom DM_custom is dropped, DM_screen kept", cart and "DM_custom" not in keys(cart) and "dm_custom" not in keys(cart)
      and "qty" in keys(cart) and "DM_screen" in keys(cart), cart and keys(cart))

# 2. Screen reports read the new names.
sc = requests.get(f"{B}/admin/analytics/{APP}/screens?days=1", headers=ADMIN, timeout=30).json()
check("[ANA-015] screens report groups by DM_screen", any(s.get("screen_name") == "Cart" for s in sc.get("screens", [])), sc)

# 3. Rows stored before the rename (written straight to the table, as the old server did).
legacy_id = f"legacy-{RUN}"
DB[table].insert_one({
    "event_name": "screen_view", "event_timestamp": t0 + 5, "user_pseudo_id": legacy_id,
    "event_params": [{"key": k, "value": {"string_value": "Old", "int_value": None, "float_value": None, "double_value": None}} for k in OLD]
                    + [{"key": "qty", "value": {"string_value": None, "int_value": 1, "float_value": None, "double_value": None}}],
    "app_info": {"id": "com.dmart.ready", "version": "1.0", "install_store": None, "firebase_app_id": APP, "install_source": None},
})
m = requests.post(f"{B}/admin/analytics/migrate-dm-keys", headers=ADMIN, timeout=120)
check("[ANA-015] migration endpoint runs", m.ok and m.json().get("rows", 0) >= 1, m.text)
old_row = DB[table].find_one({"user_pseudo_id": legacy_id})
check("[ANA-015] stored firebase_* keys are renamed in place, values and order kept",
      keys(old_row) == [k.replace("firebase_", "DM_") for k in OLD] + ["qty"] and all(e["value"]["string_value"] == "Old" for e in old_row["event_params"][:7]), keys(old_row))
check("[ANA-015] stored app_info.firebase_app_id renamed", old_row["app_info"].get("DM_app_id") == APP and "firebase_app_id" not in old_row["app_info"], old_row["app_info"])
again = requests.post(f"{B}/admin/analytics/migrate-dm-keys", headers=ADMIN, timeout=120).json()
check("[ANA-015] running it again changes nothing", again.get("rows") == 0, again)
check("[ANA-015] no firebase_ keys left in the events tables", sum(
    DB[c].count_documents({"$or": [{"event_params.key": {"$regex": "^firebase_"}}, {"app_info.firebase_app_id": {"$exists": True}}]})
    for c in DB.list_collection_names() if c.startswith(f"analytics_{APP}.events_")) == 0)
check("[ANA-015] start-up migration recorded once", DB["dm_migrations"].find_one({"_id": "rename_firebase_keys_v1"}) is not None)
DB[table].delete_one({"user_pseudo_id": legacy_id})  # a partial row; other suites export every column
check("[ANA-015] admin key required", requests.post(f"{B}/admin/analytics/migrate-dm-keys", timeout=30).status_code == 401)

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
