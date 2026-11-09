# Contract checks for requirements that had no test when the specs were written (dm-specs).
# Run against a server started with tests/e2e.env:
#   python3 tests/e2e_contract.py [path/to/grpc-status-server]
import json, os, subprocess, sys, time, uuid

import requests

B = "http://127.0.0.1:8090"
APP = "dmart-ready-ios"
H = {"x-api-key": "test-key", "x-dm-app-id": APP}
ADMIN = {"x-admin-key": "test-admin"}
RUN = uuid.uuid4().hex[:8]
results = []


def check(name, cond, detail=""):
    results.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  → {str(detail)[:300]}"))


def rpc(method, body):
    return requests.post(f"{B}/v1/rpc/dm.sdk.v1.{method}", json=body, headers=H, timeout=30)


now = int(time.time() * 1000)
version = f"9.9.{RUN}"
inst = f"inst-contract-{RUN}"
ctx = {"appId": APP, "installationId": inst, "platform": "ios", "appVersion": version}

# ── WIRE-005 / ANA-004: the same batch twice is stored once ───────────────────
batch = {"app_id": APP, "batch_id": str(uuid.uuid4()), "installation_id": inst, "instance_id": inst,
         "app": {"version_name": version}, "events": [{"name": "session_start", "timestamp_ms": now, "session_id": "s1"}]}
first = requests.post(B + "/events", json=batch, headers=H, timeout=30).json()
second = requests.post(B + "/events", json=batch, headers=H, timeout=30).json()
check("[WIRE-005, ANA-004] the same batch_id twice is stored once", first.get("duplicate") is False and second.get("duplicate") is True, (first, second))

# ── CRA-007: a crash on a closed issue reopens it as regressed ───────────────
def report(n):
    return {"id": f"rep-{RUN}-{n}", "kind": "CRASH_KIND_EXCEPTION", "isFatal": True, "launchId": f"launch-{RUN}-{n}",
            "timestampMs": str(now), "exceptionName": f"ContractProbe{RUN}", "exceptionReason": f"reason {n}",
            "frames": [{"symbol": "ContractApp.checkout", "line": "42"}]}

r = rpc("CrashlyticsService/UploadCrashReports", {"context": ctx, "reports": [report(1)]})
issues = requests.get(f"{B}/admin/crashlytics/{APP}/issues?fatal=true", headers=ADMIN, timeout=30).json()["issues"]
issue = next((i for i in issues if f"ContractProbe{RUN}" in json.dumps(i)), None)
check("[CRA-004] fatal report creates an issue", r.ok and issue is not None, r.text)
if issue:
    closed = requests.patch(f"{B}/admin/crashlytics/issues/{issue['_id']}", json={"status": "closed"}, headers=ADMIN, timeout=30)
    rpc("CrashlyticsService/UploadCrashReports", {"context": ctx, "reports": [report(2)]})
    after = requests.get(f"{B}/admin/crashlytics/issues/{issue['_id']}", headers=ADMIN, timeout=30).json()
    status = after.get("issue", {}).get("status")
    check("[CRA-007] a crash on a closed issue reopens it as regressed", closed.ok and status == "regressed", (closed.text, status))

# ── CRA-008: crash-free statistics per version ───────────────────────────────
stability = requests.get(f"{B}/admin/stability/{APP}?days=1", headers=ADMIN, timeout=30).json()
row = next((v for v in stability.get("versions", []) if v.get("app_version") == version), None)
check("[CRA-008] crash-free sessions and users are reported per version",
      row is not None and row.get("sessions", 0) >= 1 and row.get("crashed_sessions", 0) >= 1
      and row.get("crash_free_sessions_pct") is not None and row["crash_free_sessions_pct"] < 100, row)

# ── ERR-007: invalid configuration stops the server at startup ───────────────
binary = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "target", "debug", "grpc-status-server")
if os.path.exists(binary):
    env = {k: v for k, v in os.environ.items() if not k.startswith("DM_")}
    env.update({"PORT": "18097", "GRPC_PORT": "18096", "MONGODB_URI": "mongodb://127.0.0.1:27017/?directConnection=true",
                "DM_API_KEYS": "dmart-ready-ios:" + "k" * 32, "DM_TOKEN_SECRET": "short"})
    try:
        # Outside the server folder so its .env is not loaded.
        p = subprocess.run([os.path.abspath(binary)], env=env, capture_output=True, text=True, timeout=30, cwd="/")
        out = p.stdout + p.stderr
        check("[ERR-007] a DM_TOKEN_SECRET under 16 characters stops the server", p.returncode != 0 and "DM_TOKEN_SECRET" in out, out[-400:])
    except subprocess.TimeoutExpired:
        check("[ERR-007] a DM_TOKEN_SECRET under 16 characters stops the server", False, "server kept running")
else:
    print(f"SKIP startup check (no binary at {binary})")

passed = sum(results)
print(f"\n{passed}/{len(results)} passed")
raise SystemExit(0 if passed == len(results) else 1)
