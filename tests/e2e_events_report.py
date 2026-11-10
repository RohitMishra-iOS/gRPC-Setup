# The GA4 "Events: Event name" report (dm-specs EVR-001…EVR-004), end to end through the real
# upload path. Uses three past days, so data uploaded "now" by other suites is outside the range.
# Run against a server started with tests/e2e.env:
#   python3 tests/e2e_events_report.py
import time, uuid
from datetime import datetime, timedelta, timezone

import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
IST = timezone(timedelta(hours=5, minutes=30))
RUN = uuid.uuid4().hex[:8]
EA, EB = f"evr_a_{RUN}", f"evr_b_{RUN}"
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:600]}"))


def p(key, **value):
    return {"key": key, "value": value}


today = datetime.now(IST).date()
D = {n: today - timedelta(days=n) for n in (1, 2, 3)}       # three past days in IST


def at(day, minute=0):
    noon = datetime(day.year, day.month, day.day, 12, 0, tzinfo=IST) + timedelta(minutes=minute)
    return str(int(noon.timestamp() * 1_000_000))


def upload(user, platform, events):
    ctx = {"appId": APP, "installationId": f"{user}-{RUN}", "platform": platform, "appVersion": "2.1.0"}
    body = {"context": ctx, "batchId": f"{user}-{RUN}-{uuid.uuid4().hex[:6]}", "userPseudoId": f"{user}-{RUN}", "events": events}
    r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.AnalyticsService/UploadEvents", json=body, headers=H, timeout=30)
    assert r.ok, r.text


def ev(name, day, minute, *params):
    return {"name": name, "timestampMicros": at(day, minute), "params": list(params)}


def report(**q):
    q = {"start": D[3].isoformat(), "end": D[1].isoformat(), "tz": "Asia/Kolkata", **q}
    return requests.get(f"{B}/admin/analytics/{APP}/events-report", params=q, headers=ADMIN, timeout=120)


def revenue(rep, name):
    return next((x["revenue"] for x in rep.get("rows", []) if x["name"] == name), 0)


# Purchases and refunds use standard names, so other data in the range is subtracted out.
before = report().json()

# User A (iOS, active through engagement time): 3× A on day-3, 2× A + a purchase on day-1.
upload("ua", "ios", [ev(EA, D[3], i) for i in range(3)] + [ev("user_engagement", D[3], 5, p("engagement_time_msec", intValue="1000"))])
upload("ua", "ios", [ev(EA, D[1], i) for i in range(2)] + [ev("purchase", D[1], 9, p("value", intValue="1000"), p("currency", stringValue="INR"), p("transaction_id", stringValue=f"T-{RUN}"))])
# User B (Android, never engaged): 1× A and 4× B on day-2.
upload("ub", "android", [ev(EA, D[2], 0)] + [ev(EB, D[2], i + 1) for i in range(4)])
# User C (web, engaged session): 2× B on day-1, one with session_engaged = 1; a refund.
upload("uc", "web", [ev(EB, D[1], 0, p("session_engaged", intValue="1")), ev(EB, D[1], 1),
                     ev("refund", D[1], 2, p("value", intValue="200"), p("currency", stringValue="INR"), p("transaction_id", stringValue=f"T-{RUN}"))])
time.sleep(0.5)


r = report(series=f"{EA},{EB}")
rep = r.json() if r.ok else {}
rows = {row["name"]: row for row in rep.get("rows", [])}
a, b = rows.get(EA, {}), rows.get(EB, {})
check("[EVR-001] report answers for a date range", r.ok and rep["start"] == D[3].isoformat() and rep["end"] == D[1].isoformat() and rep["tz"] == "Asia/Kolkata", r.text)
check("[EVR-001] event count and total users per event", a.get("count") == 6 and a.get("users") == 2 and b.get("count") == 6 and b.get("users") == 2, (a, b))
check("[EVR-001] active users: engagement time / engaged session / first open", a.get("active_users") == 1 and b.get("active_users") == 1, (a, b))
check("[EVR-001] rows are sorted by event count", [x["count"] for x in rep.get("rows", [])] == sorted([x["count"] for x in rep.get("rows", [])], reverse=True))
check("[EVR-001] totals: count is the sum of rows, users are distinct users", rep.get("totals", {}).get("count") == sum(x["count"] for x in rep.get("rows", []))
      and rep["totals"]["users"] >= 3 and rep["totals"]["active_users"] >= 2 and rep["totals"]["users"] < sum(x["users"] for x in rep["rows"]), rep.get("totals"))

# Revenue: purchases + ad revenue − refunds, in the property currency (INR with DM_USD_RATES, else USD).
cur = rep.get("currency")
scale = 1.0 if cur == "INR" else 0.012
d_pur, d_ref = revenue(rep, "purchase") - revenue(before, "purchase"), revenue(rep, "refund") - revenue(before, "refund")
check(f"[EVR-002] purchase revenue in {cur}", abs(d_pur - 1000 * scale) < 0.05, (cur, d_pur))
check(f"[EVR-002] refunds subtract from total revenue", abs(d_ref + 200 * scale) < 0.05, (cur, d_ref))
check("[EVR-002] total revenue grows by purchase − refund", abs(rep["totals"]["revenue"] - before["totals"]["revenue"] - 800 * scale) < 0.05, (rep["totals"], before["totals"]))
check("[EVR-002] total revenue = sum of rows", abs(rep["totals"]["revenue"] - sum(x["revenue"] for x in rep["rows"])) < 0.05, rep.get("totals"))

# Over time.
buckets = rep.get("series", {}).get("buckets", [])
by_day = {x["date"]: x for x in buckets}
check("[EVR-003] one zero-filled bucket per day", [x["date"] for x in buckets] == [D[3].isoformat(), D[2].isoformat(), D[1].isoformat()], [x["date"] for x in buckets])
check("[EVR-003] plotted events per day", [by_day.get(D[n].isoformat(), {}).get("values", {}).get(EA) for n in (3, 2, 1)] == [3, 1, 2]
      and [by_day.get(D[n].isoformat(), {}).get("values", {}).get(EB) for n in (3, 2, 1)] == [0, 4, 2], buckets)
check("[EVR-003] Total line = every event in the bucket", sum(x["total"] for x in buckets) == rep["totals"]["count"], (buckets, rep.get("totals")))
check("[EVR-003] series names come back in the order asked", rep["series"]["names"] == [EA, EB])
default = report().json()
check("[EVR-003] without series the top 5 events are plotted", default["series"]["names"] == [x["name"] for x in default["rows"][:5]], default["series"]["names"])

week = report(granularity="week", series=EA).json()
sunday = lambda d: d - timedelta(days=(d.weekday() + 1) % 7)
check("[EVR-003] weeks start on Sunday", [x["date"] for x in week["series"]["buckets"]] == sorted({sunday(D[n]).isoformat() for n in (1, 2, 3)})
      and sum(x["values"][EA] for x in week["series"]["buckets"]) == 6, week["series"]["buckets"])
month = report(granularity="month", series=EA).json()
check("[EVR-003] months start on the 1st", all(x["date"].endswith("-01") for x in month["series"]["buckets"]) and sum(x["values"][EA] for x in month["series"]["buckets"]) == 6, month["series"]["buckets"])

# Filters and ranges.
android = report(platform="ANDROID", series=EA).json()
arows = {row["name"]: row for row in android["rows"]}
check("[EVR-004] platform filter", arows.get(EA, {}).get("count") == 1 and arows.get(EB, {}).get("count") == 4 and android["platform"] == "ANDROID", (arows.get(EA), arows.get(EB)))
one = report(start=D[2].isoformat(), end=D[2].isoformat()).json()
orows = {row["name"]: row for row in one["rows"]}
check("[EVR-004] a one-day range only counts that day", orows.get(EA, {}).get("count") == 1 and orows.get(EB, {}).get("count") == 4, one["rows"][:5])
utc = report(tz="UTC").json()
check("[EVR-004] another time zone answers in that zone", utc.get("tz") == "UTC" and len(utc["series"]["buckets"]) == 3)

for name, q in [("end before start", {"start": D[1].isoformat(), "end": D[3].isoformat()}), ("bad date", {"start": "2026-02-30"}),
                ("unknown zone", {"tz": "Mars/Base"}), ("bad granularity", {"granularity": "hour"}), ("bad platform", {"platform": "TV"}),
                ("more than 400 days", {"start": "2024-01-01"}), ("more than 10 series", {"series": ",".join(f"e{i}" for i in range(11))})]:
    r = report(**q)
    check(f"[EVR-004] 400 for {name}", r.status_code == 400 and "error" in r.json(), r.text)
check("[EVR-004] admin key required", requests.get(f"{B}/admin/analytics/{APP}/events-report", timeout=30).status_code == 401)

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
