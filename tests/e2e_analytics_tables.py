# GA4 table layout, end to end (dm-specs ANA-010, TBL-001…TBL-014). Run against a server started
# with tests/e2e.env plus:
#   DM_ANALYTICS_TIMEZONE=Asia/Kolkata DM_ANALYTICS_CURRENCY=INR DM_USD_RATES=INR:0.012,EUR:1.08 DM_TRUST_GEO_HEADERS=true
#   python3 tests/e2e_analytics_tables.py
import json, os, struct, time, uuid
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
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:400]}"))


def upload(body, headers=None):
    return requests.post(f"{B}/v1/rpc/dm.sdk.v1.AnalyticsService/UploadEvents", json=body, headers=headers or H, timeout=30)


def now_us():
    return int(time.time() * 1_000_000)


def p(key, **value):
    return {"key": key, "value": value}


def table_for(ts_us):
    return f"analytics_{APP}.events_" + datetime.fromtimestamp(ts_us / 1e6, IST).strftime("%Y%m%d")


def rows(ts_us, **filter):
    return list(DB[table_for(ts_us)].find(filter).sort("batch_event_index", 1))


def param(row, key):
    return next((e["value"] for e in row["event_params"] if e["key"] == key), None)


inst = f"inst-{RUN}"
pseudo = f"pseudo-{RUN}"
ctx = {"appId": APP, "installationId": inst, "platform": "ios", "appVersion": "2.1.0", "bundleId": "com.dmart.ready", "osVersion": "18.1", "deviceModel": "iPhone16,2"}
t0 = now_us()
batch = f"b-{RUN}"
body = {
    "context": ctx,
    "batchId": batch,
    "bundleSequenceId": "7",
    "uploadTimestampMicros": str(t0 - 1_000_000),
    "userId": f"user-{RUN}",
    "userPseudoId": pseudo,
    "userFirstTouchTimestampMicros": str(t0 - 3_600_000_000),
    "userProperties": [{"key": "city", "value": {"stringValue": "Ahmedabad"}, "setTimestampMicros": str(t0 - 10)}],
    "device": {"category": "mobile", "mobileBrandName": "Apple", "mobileModelName": "iPhone", "mobileOsHardwareModel": "iPhone16,2",
               "operatingSystem": "iOS", "operatingSystemVersion": "18.1", "vendorId": "IDFV-1", "language": "en-in",
               "isLimitedAdTracking": "Yes", "timeZoneOffsetSeconds": "19800"},
    "appInfo": {"id": "com.dmart.ready", "version": "2.1.0", "installStore": "App Store"},
    "privacyInfo": {"analyticsStorage": "Yes", "adsStorage": "No", "usesTransientToken": "No"},
    "events": [
        {"name": "session_start", "timestampMicros": str(t0), "params": [p("ga_session_id", intValue="1762670000"), p("ga_session_number", intValue="1"), p("DM_event_origin", stringValue="auto")]},
        {"name": "campaign_details", "timestampMicros": str(t0 + 1), "params": [p("source", stringValue="google"), p("medium", stringValue="cpc"), p("campaign", stringValue="diwali")]},
        {"name": "add_to_cart", "timestampMicros": str(t0 + 2), "previousTimestampMicros": str(t0 - 50),
         "params": [p("price", doubleValue=10.5), p("qty", intValue="2"), p("name", stringValue="x"), p("flag", intValue="1"), p("DM_screen", stringValue="HomePage"), p("ga_custom", intValue="1")],
         "items": [{"params": [p("item_id", stringValue="A"), p("price", intValue="100"), p("quantity", intValue="1")]}]},
        {"name": "purchase", "timestampMicros": str(t0 + 3),
         "params": [p("value", intValue="300"), p("currency", stringValue="INR"), p("tax", intValue="10"), p("transaction_id", stringValue=f"T-{RUN}")],
         "items": [{"params": [p("item_id", stringValue="A"), p("price", intValue="100"), p("quantity", intValue="3"), p("shelf", stringValue="top")]}]},
        {"name": "purchase", "timestampMicros": str(t0 + 4), "params": [p("value", intValue="50"), p("currency", stringValue="INR")]},
        {"name": "ad_impression", "timestampMicros": str(t0 + 5), "params": [p("value", doubleValue=0.5), p("currency", stringValue="USD"), p("ad_format", stringValue="banner")]},
        {"name": "user_engagement", "timestampMicros": str(t0 + 6), "params": [p("engagement_time_msec", intValue="1500")]},
        {"name": "bad name!", "timestampMicros": str(t0 + 7)},
    ],
}
r = upload(body, {**H, "cf-ipcountry": "IN", "cf-ipcity": "Ahmedabad", "cf-region": "Gujarat"})
answer = r.json() if r.ok else {}
check("[ANA-010] UploadEvents answers accepted with stored count, rejected event and warnings",
      r.ok and answer.get("accepted") is True and answer.get("storedEvents") == 7
      and [e["index"] for e in answer.get("rejectedEvents", [])] == [7]
      and any("ga_custom" in w for w in answer.get("warnings", [])), r.text)

table = table_for(t0)
check("[TBL-001] rows are in analytics_<app>.events_YYYYMMDD (property time zone)", DB[table].count_documents({"user_pseudo_id": pseudo}) == 7, table)
idx = {i["name"] for i in DB[table].list_indexes()}
check("[TBL-001] the daily table has its indexes", {"event_name_time", "pseudo_time", "user_id", "time"} <= idx, idx)

all_rows = rows(t0, user_pseudo_id=pseudo)
by = {r["event_name"]: r for r in all_rows}
COLUMNS = ["event_date", "event_timestamp", "event_name", "event_params", "event_previous_timestamp", "event_value_in_usd",
           "event_bundle_sequence_id", "event_server_timestamp_offset", "user_id", "user_pseudo_id", "privacy_info", "user_properties",
           "user_first_touch_timestamp", "user_ltv", "device", "geo", "app_info", "traffic_source", "stream_id", "platform",
           "event_dimensions", "ecommerce", "items", "collected_traffic_source", "is_active_user", "batch_event_index", "batch_page_id",
           "batch_ordering_id", "session_traffic_source_last_click", "publisher"]
check("[TBL-002] every row has every GA4 column, in export order", all([k for k in row if k != "_id"] == COLUMNS for row in all_rows),
      [k for k in all_rows[0] if k != "_id"] if all_rows else "no rows")
check("[TBL-002] records have all sub-fields", all(set(row["device"]) >= {"category", "mobile_marketing_name", "time_zone_offset_seconds", "web_info"}
      and set(row["geo"]) == {"continent", "sub_continent", "country", "region", "city", "metro"}
      and set(row["app_info"]) == {"id", "version", "install_store", "DM_app_id", "install_source"} for row in all_rows))
cart = by.get("add_to_cart", {})
check("[TBL-003] typed values: exactly one field set",
      param(cart, "price") == {"string_value": None, "int_value": None, "float_value": None, "double_value": 10.5}
      and param(cart, "qty")["int_value"] == 2 and param(cart, "name")["string_value"] == "x" and param(cart, "flag")["int_value"] == 1, cart.get("event_params"))
check("[ANA-001] reserved-prefix custom parameter dropped, automatic kept", param(cart, "ga_custom") is None and param(cart, "DM_screen")["string_value"] == "HomePage")
check("[TBL-002] event_date / platform / stream_id", cart.get("event_date") == table[-8:] and cart.get("platform") == "IOS" and cart.get("stream_id") == f"{APP}:ios", cart.get("event_date"))
check("[TBL-005] user fields: id, pseudo id, first touch, property with set time, privacy",
      cart.get("user_id") == f"user-{RUN}" and cart.get("user_pseudo_id") == pseudo and cart.get("user_first_touch_timestamp") == t0 - 3_600_000_000
      and cart["user_properties"][0]["value"]["set_timestamp_micros"] == t0 - 10 and cart["privacy_info"]["ads_storage"] == "No", cart.get("user_properties"))
check("[TBL-006] device record and Apple marketing name", cart["device"]["mobile_marketing_name"] == "iPhone 15 Pro Max"
      and cart["device"]["operating_system_version"] == "iOS 18.1" and cart["device"]["time_zone_offset_seconds"] == 19800, cart.get("device"))
check("[TBL-006] geo from trusted CDN headers", cart["geo"]["country"] == "India" and cart["geo"]["continent"] == "Asia" and cart["geo"]["region"] == "Gujarat", cart.get("geo"))
check("[TBL-006] app_info", cart["app_info"]["DM_app_id"] == APP and cart["app_info"]["install_store"] == "App Store", cart.get("app_info"))
camp = by.get("campaign_details", {})
check("[TBL-007] collected_traffic_source from campaign params", (camp.get("collected_traffic_source") or {}).get("manual_source") == "google", camp.get("collected_traffic_source"))
check("[TBL-007] first-touch traffic_source and last-click channel group",
      cart["traffic_source"] == {"name": "diwali", "medium": "cpc", "source": "google"}
      and cart["session_traffic_source_last_click"]["cross_channel_campaign"]["default_channel_group"] == "Paid Search"
      and all_rows[0]["traffic_source"]["source"] == "(direct)", (cart.get("traffic_source"), all_rows[0].get("traffic_source")))
purchases = [r for r in all_rows if r["event_name"] == "purchase"]
first = purchases[0] if purchases else {}
check("[TBL-008] ecommerce record and USD conversion",
      first.get("ecommerce", {}).get("purchase_revenue") == 300 and first["ecommerce"]["purchase_revenue_in_usd"] == 3.6
      and first["ecommerce"]["tax_value"] == 10 and first["ecommerce"]["total_item_quantity"] == 3 and first["ecommerce"]["unique_items"] == 1
      and first["event_value_in_usd"] == 3.6, first.get("ecommerce"))
check("[TBL-008] items mapped with revenue and item_params", first.get("items", [{}])[0].get("item_revenue") == 300
      and first["items"][0]["item_params"][0]["key"] == "shelf" and cart["items"][0]["quantity"] == 1, first.get("items"))
check("[TBL-005] user_ltv accumulates purchases in the property currency",
      [r["user_ltv"]["revenue"] for r in purchases] == [300, 350] and purchases[1]["user_ltv"]["currency"] == "INR", [r.get("user_ltv") for r in purchases])
check("[TBL-008] non-ecommerce events have ecommerce = null", by.get("session_start", {}).get("ecommerce") is None)
ad = by.get("ad_impression", {})
check("[TBL-009] publisher for ad_impression", (ad.get("publisher") or {}).get("ad_revenue_in_usd") == 0.5 and cart.get("publisher") is None, ad.get("publisher"))
check("[TBL-010] bundle sequence, batch index, previous timestamp, server offset",
      all(r["event_bundle_sequence_id"] == 7 for r in all_rows) and [r["batch_event_index"] for r in all_rows] == list(range(7))
      and cart["event_previous_timestamp"] == t0 - 50 and cart["event_server_timestamp_offset"] >= 1_000_000, [r["batch_event_index"] for r in all_rows])
check("[TBL-010] is_active_user", by["user_engagement"]["is_active_user"] is True and cart["is_active_user"] is False)

# Idempotency
again = upload(body)
check("[ANA-010] the same batch_id twice: duplicate, nothing more stored", again.ok and again.json().get("duplicate") is True and DB[table].count_documents({"user_pseudo_id": pseudo}) == 7, again.text)

# User tables
users = DB[f"analytics_{APP}.users_{table[-8:]}"].find_one({"_id": f"user-{RUN}"})
pseudo_row = DB[f"analytics_{APP}.pseudonymous_users_{table[-8:]}"].find_one({"_id": pseudo})
check("[TBL-011] users_ and pseudonymous_users_ snapshots",
      users is not None and pseudo_row is not None and users["user_ltv"]["sessions"] >= 1 and users["user_ltv"]["purchases"] == 2
      and pseudo_row["device"]["unified_screen_name"] == "HomePage" and users["occurrence_date"] == table[-8:]
      and users["user_properties"][0]["value"]["user_property_name"] == "city", users)

# Cookieless
cookieless = {**body, "batchId": f"c-{RUN}", "privacyInfo": {"analyticsStorage": "No"}, "events": [{"name": "page_view", "timestampMicros": str(now_us())}]}
r = upload(cookieless)
row = DB[table].find_one({"_id": f"c-{RUN}:0"})
check("[TBL-005] analytics_storage = No: no ids, no properties", r.ok and row is not None and row["user_pseudo_id"] is None and row["user_id"] is None and row["user_properties"] == [], row)

# Debug
r = upload({**body, "batchId": f"d-{RUN}", "debug": True, "events": [{"name": "debug_probe", "timestampMicros": str(now_us())}]})
row = DB[table].find_one({"_id": f"d-{RUN}:0"})
check("[ANA-009, TBL-010] debug uploads carry debug_mode = 1", row is not None and param(row, "debug_mode")["int_value"] == 1, row and row.get("event_params"))

# Web
web_ctx = {"appId": APP, "installationId": f"web-{RUN}", "platform": "web"}
r = upload({"context": web_ctx, "batchId": f"w-{RUN}", "pageId": "123", "device": {"category": "desktop", "browser": "Chrome", "browserVersion": "130", "hostname": "shop.dmart.in"},
            "events": [{"name": "page_view", "timestampMicros": str(now_us()), "params": [p("page_location", stringValue="https://shop.dmart.in/cart")]}]})
row = DB[table].find_one({"_id": f"w-{RUN}:0"})
check("[TBL-006] web rows: WEB platform, no app_info, hostname, web_info", row is not None and row["platform"] == "WEB" and row["app_info"] is None
      and row["event_dimensions"]["hostname"] == "shop.dmart.in" and row["device"]["web_info"]["browser"] == "Chrome" and row["batch_page_id"] == 123, row)

# Validation at batch level
r = upload({"context": ctx, "events": [{"name": "e", "timestampMicros": str(now_us())}] * 501})
check("[ANA-010, ANA-002] 501 events → 400 INVALID_ARGUMENT", r.status_code == 400, r.text[:200])
r = upload({"context": ctx, "batchId": "has space", "events": []})
check("[ANA-010] malformed batch_id → 400", r.status_code == 400, r.text[:200])
r = upload({"context": ctx, "privacyInfo": {"analyticsStorage": "granted"}, "events": []})
check("[ANA-010] privacy values must be Yes / No → 400", r.status_code == 400, r.text[:200])
r = upload(body, {"x-dm-app-id": APP, "x-api-key": "wrong"})
check("[KEY-001] UploadEvents with a wrong key → 401", r.status_code == 401, r.text[:200])

# Binary gRPC-Web: the oneof survives protobuf encoding (protoc-encoded request).
def varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out

def ld(num, payload):
    return varint(num << 3 | 2) + varint(len(payload)) + payload

def vi(num, value):
    return varint(num << 3) + varint(value)

ts = now_us()
context_pb = ld(1, APP.encode()) + ld(2, f"bin-{RUN}".encode()) + ld(4, b"ios")
value_int = vi(2, 2)                                      # AnalyticsValue.int_value = 2
value_double = varint(4 << 3 | 1) + struct.pack("<d", 2.5)  # AnalyticsValue.double_value = 2.5
event_pb = ld(1, b"add_to_cart") + vi(2, ts) + ld(4, ld(1, b"qty") + ld(2, value_int)) + ld(4, ld(1, b"price") + ld(2, value_double))
request_pb = ld(1, context_pb) + ld(2, f"g-{RUN}".encode()) + ld(14, event_pb)
frame = b"\x00" + len(request_pb).to_bytes(4, "big") + request_pb
r = requests.post(f"{B}/dm.sdk.v1.AnalyticsService/UploadEvents", data=frame, headers={**H, "content-type": "application/grpc-web+proto"}, timeout=30)
row = DB[table_for(ts)].find_one({"_id": f"g-{RUN}:0"})
check("[ANA-010, WIRE-001] gRPC-Web binary upload keeps int vs double", row is not None and param(row, "qty")["int_value"] == 2 and param(row, "price")["double_value"] == 2.5, (r.headers.get("grpc-status"), row))

# Old /events format → same tables (TBL-004, TBL-013)
old = {"app_id": APP, "batch_id": f"o-{RUN}", "installation_id": f"old-{RUN}", "instance_id": f"old-{RUN}", "screen_name": "Home",
       "app": {"version_name": "1.9", "bundle_id": "com.dmart.ready"}, "device": {"os_version": "17.5", "manufacturer": "Apple", "model": "iPhone15,3", "timezone": "Asia/Kolkata"},
       "events": [{"name": "screen_view", "timestamp_ms": int(time.time() * 1000), "session_id": "abc", "params": {"screen_name": "Cart", "screen_class": "CartVC", "previous_screen_name": "Home"}},
                  {"name": "add_to_cart", "timestamp_ms": int(time.time() * 1000), "session_id": "abc", "params": {"price": 10.5, "qty": 2, "items": [{"item_id": "A", "price": 5}]}}]}
r = requests.post(B + "/events", json=old, headers=H, timeout=30)
olds = list(DB[table].find({"user_pseudo_id": f"old-{RUN}"}).sort("batch_event_index", 1))
sv = olds[0] if olds else {}
check("[TBL-013] old /events format still answers accepted and lands in the tables", r.ok and r.json().get("accepted") is True and len(olds) == 2, r.text[:200])
check("[TBL-004] old parameter names become GA4 names", param(sv, "DM_screen") == {"string_value": "Cart", "int_value": None, "float_value": None, "double_value": None}
      and param(sv, "screen_name") is None and param(sv, "DM_previous_screen")["string_value"] == "Home" and param(sv, "ga_session_id")["int_value"] is not None
      and param(sv, "DM_event_origin")["string_value"] == "auto", sv.get("event_params"))
check("[TBL-003] old JSON numbers: whole → int_value, other → double_value", olds and param(olds[1], "qty")["int_value"] == 2 and param(olds[1], "price")["double_value"] == 10.5 and olds[1]["items"][0]["item_id"] == "A")

# Migration of old event_data batches (TBL-013)
legacy_id = f"m-{RUN}"
DB.event_data.insert_one({"app_id": APP, "batch_id": legacy_id, "installation_id": f"mig-{RUN}", "received_at": datetime.now(timezone.utc),
                          "events": [{"name": "legacy_event", "timestamp_ms": int(time.time() * 1000), "params": {"n": 1}}]})
DB.analytics_batches.insert_one({"_id": legacy_id, "app_id": APP})  # the old server had claimed it
m1 = requests.post(f"{B}/admin/analytics/{APP}/migrate-legacy?days=1", headers=ADMIN, timeout=60).json()
count1 = DB[table].count_documents({"_id": f"{legacy_id}:0"})
m2 = requests.post(f"{B}/admin/analytics/{APP}/migrate-legacy?days=1", headers=ADMIN, timeout=60).json()
count2 = DB[table].count_documents({"_id": f"{legacy_id}:0"})
check("[TBL-013] migrate-legacy converts old batches once", count1 == 1 and count2 == 1 and m2.get("events_stored") == 0, (m1, m2))

# Admin: tables, rows, export, schema (TBL-012)
tables = requests.get(f"{B}/admin/analytics/{APP}/tables", headers=ADMIN, timeout=30).json()
names = [t["table"] for t in tables.get("tables", [])]
check("[TBL-012] tables listing", table.split(".", 1)[1] in names and any(n.startswith("users_") for n in names) and tables.get("timezone") == "Asia/Kolkata", tables)
rws = requests.get(f"{B}/admin/analytics/{APP}/tables/{table.split('.', 1)[1]}?limit=3", headers=ADMIN, timeout=30).json()
check("[TBL-012] newest rows", len(rws.get("rows", [])) == 3, rws)
exp = requests.get(f"{B}/admin/analytics/{APP}/tables/{table.split('.', 1)[1]}/export", headers=ADMIN, timeout=60)
lines = [json.loads(l) for l in exp.text.splitlines() if l.strip()]
check("[TBL-012] NDJSON export: one object per line, no _id, all columns", exp.ok and lines and all("_id" not in l and list(l) == COLUMNS for l in lines), exp.text[:300])
bad = requests.get(f"{B}/admin/analytics/{APP}/tables/events_x", headers=ADMIN, timeout=30)
check("[TBL-012] invalid table name → 400", bad.status_code == 400, bad.text)
schema = requests.get(f"{B}/admin/analytics/schema/events", headers=ADMIN, timeout=30).json()
check("[TBL-012] BigQuery schema names every column", [f["name"] for f in schema] == COLUMNS, [f.get("name") for f in schema])

# Dashboards over the tables (TBL-014)
ov = requests.get(f"{B}/admin/analytics/{APP}/overview?days=1&tz=Asia/Kolkata", headers=ADMIN, timeout=30).json()
check("[TBL-014] overview counts the table rows", ov["totals"]["events"] >= 10 and ov["totals"]["sessions"] >= 1 and any(e["name"] == "add_to_cart" for e in ov["top_events"]), ov.get("totals"))
st = requests.get(f"{B}/admin/analytics/{APP}/stream?installation_id={inst}", headers=ADMIN, timeout=30).json()
check("[TBL-014] DebugView stream by installation (via the pseudo id)", any(e["name"] == "add_to_cart" and e.get("screen_name") == "HomePage" for e in st.get("events", [])), st)
ed = requests.get(f"{B}/admin/analytics/{APP}/events/add_to_cart?days=1", headers=ADMIN, timeout=30).json()
check("[TBL-014] event detail with parameter breakdown", ed["totals"]["count"] >= 2 and any(pr["key"] == "qty" for pr in ed["params"]), ed.get("params"))
sc = requests.get(f"{B}/admin/analytics/{APP}/screens?days=1", headers=ADMIN, timeout=30).json()
check("[TBL-014] screens grouped by DM_screen", any(s["screen_name"] == "Cart" for s in sc.get("screens", [])), sc)
ud = requests.get(f"{B}/admin/analytics/{APP}/users/{inst}", headers=ADMIN, timeout=30).json()
check("[TBL-014] user detail: profile and timeline", ud.get("latest_batch", {}).get("user_id") == f"user-{RUN}" and len(ud.get("timeline", [])) >= 7, {k: ud.get(k) for k in ("latest_batch",)})

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
