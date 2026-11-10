# Crash symbolication with file and line, end to end (dm-specs CRA-005): a real arm64 dSYM
# (tests/fixtures, see its README) is uploaded and crash reports resolve to
# `ViewController.addToCart(quantity: Int)` at `ViewController.c:10`.
# Run against a server started with tests/e2e.env:
#   python3 tests/e2e_symbols.py
import os, time, uuid

import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
RUN = uuid.uuid4().hex[:8]
DSYM = os.path.join(os.path.dirname(__file__), "fixtures", "ProductCardAnimation.app.dSYM.zip")
DSYM_UUID = "4C4C443B-5555-3144-A1D3-448C2E620BD3"
TEXT_VMADDR = 0x100000000
LOAD = 0x104A00000          # where the app was loaded on the device (ASLR slide)
CRASH_PC = 0x100004018      # line 10, the null write in add_to_cart
CALL_RETURN = 0x100004048   # return address after the call on line 14
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:500]}"))


def crash(n):
    return {
        "id": f"sym-{RUN}-{n}", "kind": "CRASH_KIND_SIGNAL", "isFatal": True, "launchId": f"launch-{RUN}-{n}",
        "timestampMs": str(int(time.time() * 1000)), "appVersion": "2.1", "appBuild": "7", "osVersion": "18.1", "deviceModel": "iPhone16,2",
        "signalNumber": 11, "signalName": "SIGSEGV", "signalCode": 1, "faultAddress": "0",
        "returnAddresses": [str(LOAD + CRASH_PC - TEXT_VMADDR), str(LOAD + CALL_RETURN - TEXT_VMADDR)],
        "binaryImages": [{"name": "ProductCardAnimation", "uuid": DSYM_UUID, "loadAddress": str(LOAD), "size": str(0x10000), "isMainExecutable": True}],
    }


def upload_crash(n):
    ctx = {"appId": APP, "installationId": f"inst-sym-{RUN}", "platform": "ios", "appVersion": "2.1"}
    r = requests.post(f"{B}/v1/rpc/dm.sdk.v1.CrashlyticsService/UploadCrashReports", json={"context": ctx, "reports": [crash(n)]}, headers=H, timeout=30)
    assert r.ok, r.text
    return requests.get(f"{B}/admin/crashlytics/reports/sym-{RUN}-{n}", headers=ADMIN, timeout=30).json()["report"]


# 1. A crash that arrives before its dSYM: addresses only.
before = upload_crash(1)
check("[CRA-005] without the dSYM the report is not symbolicated", before.get("symbolicated") is False and not before.get("crash_location"), before.get("symbolicated_frames"))

# 2. Upload the dSYM: the stored report and its issue update by themselves.
with open(DSYM, "rb") as f:
    r = requests.post(f"{B}/v1/symbols", headers=H, files={"dsym": ("ProductCardAnimation.app.dSYM.zip", f, "application/zip")},
                      data={"app_id": APP, "uuids": DSYM_UUID, "bundle_id": "com.dmart.ready", "app_version": "2.1", "app_build": "7"}, timeout=60)
check("[CRA-005] dSYM upload accepted", r.ok, r.text)
time.sleep(1)
after = requests.get(f"{B}/admin/crashlytics/reports/sym-{RUN}-1", headers=ADMIN, timeout=30).json()["report"]
frames = after.get("symbolicated_frames", [])
top = frames[0] if frames else {}
check("[CRA-005] the crashing frame has file and line", (top.get("file") or "").endswith("/ViewController.c") and top.get("line") == 10, top)
check("[CRA-005] Swift function names are demangled (class + method)", top.get("symbol") == "ViewController.addToCart(quantity: Int)", top.get("symbol"))
check("[CRA-005] the caller resolves to its call line", len(frames) > 1 and frames[1].get("symbol") == "main" and frames[1].get("line") == 14, frames[1:2])
check("[CRA-005] report crash_location", after.get("crash_location") == "ViewController.c:10 · ViewController.addToCart(quantity: Int)", after.get("crash_location"))
issue = requests.get(f"{B}/admin/crashlytics/issues/{after['issue_id']}", headers=ADMIN, timeout=30).json()["issue"]
check("[CRA-005] the issue gets the location after a late dSYM", issue.get("crash_location") == after.get("crash_location"), issue.get("crash_location"))

# 3. A crash that arrives after the dSYM is symbolicated right away.
direct = upload_crash(2)
check("[CRA-005] a new crash is symbolicated on arrival", direct.get("symbolicated") is True and (direct.get("crash_location") or "").startswith("ViewController.c:10"), direct.get("crash_location"))

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
