# Remote Config key subscriptions (subscribed_keys), end to end. Run against a server started with
# tests/e2e.env:  python3 tests/e2e_rc_subscriptions.py
import requests, threading, time, json, uuid
B="http://127.0.0.1:8090"; A={"x-admin-key":"test-admin"}; H={"x-api-key":"test-key"}
APP="dmart-ready-ios"; INST="inst-sub-"+uuid.uuid4().hex[:6]
def publish(params):
    r=requests.put(f"{B}/admin/remote-config/{APP}",json={"parameters":{k:{"default_value":v} for k,v in params.items()},"conditions":[],"experiments":[]},headers=A); return r.json()["version"]
def fetch(last, keys=None, realtime=True):
    body={"context":{"appId":APP,"installationId":INST,"platform":"ios"},"lastTemplateVersion":str(last),"isRealtimeCheck":realtime}
    if keys is not None: body["subscribedKeys"]=keys
    return requests.post(f"{B}/v1/rpc/dm.sdk.v1.RemoteConfigService/FetchConfig",json=body,headers=H,timeout=30)
results=[]
def check(name, ok, detail=""):
    results.append(ok); print(("PASS " if ok else "FAIL ")+name+("" if ok else f" → {detail}"))
v1=publish({"splash_iOS":"splash_v1","home_title":"Diwali"})
full=fetch(0, realtime=False).json()
check("[RC-020] full fetch returns every key", set(full["entries"])=={"splash_iOS","home_title"} and not full.get("partial"), full)
# 1) publish a change to another key while subscribed to splash_iOS → device is NOT woken
out={}
t=threading.Thread(target=lambda: out.update(r=fetch(v1,["splash_iOS"]), t=time.time())); start=time.time(); t.start(); time.sleep(1)
publish({"splash_iOS":"splash_v1","home_title":"Diwali Dhamaka"}); t.join()
r=out["r"].json(); held=out["t"]-start
check("[RC-020] other key changed → held until timeout, NO_CHANGE", r["state"]=="FETCH_STATE_NO_CHANGE" and held>=3, (r, held))
# 2) change splash_iOS → device woken quickly with only that key
out={}
t=threading.Thread(target=lambda: out.update(r=fetch(v1,["splash_iOS"]), t=time.time())); start=time.time(); t.start(); time.sleep(1)
v3=publish({"splash_iOS":"splash_v2","home_title":"Diwali Dhamaka"}); t.join()
r=out["r"].json(); held=out["t"]-start
check("[RC-020, RC-021] subscribed key changed → answered at once with only that key", r["state"]=="FETCH_STATE_UPDATED" and r.get("partial") is True and r["entries"]=={"splash_iOS":"splash_v2"} and held<4, (r, held))
check("[RC-021] template version is the new one", int(r["templateVersion"])==v3, r)
# 3) without subscription: any publish wakes it with all keys (classic)
out={}
t=threading.Thread(target=lambda: out.update(r=fetch(v3))); t.start(); time.sleep(1)
publish({"splash_iOS":"splash_v2","home_title":"Changed again"}); t.join()
r=out["r"].json()
check("[RC-010] no subscription → every publish, all keys", r["state"]=="FETCH_STATE_UPDATED" and set(r["entries"])=={"splash_iOS","home_title"} and not r.get("partial"), r)
# 4) subscribed key removed from template → partial update without it
out={}
t=threading.Thread(target=lambda: out.update(r=fetch(v3,["splash_iOS"]))); t.start(); time.sleep(1)
publish({"home_title":"Changed again"}); t.join()
r=out["r"].json()
check("[RC-021] subscribed key deleted → partial update with the key missing", r["state"]=="FETCH_STATE_UPDATED" and r.get("partial") is True and r.get("entries",{})=={}, r)
# 5) validation
r=fetch(v3,["bad-key","ok_key"])
check("[RC-023] invalid subscribed key → 400", r.status_code==400 and "subscribed_keys[0]" in r.text, r.text)
r=fetch(v3,["splash_iOS"], realtime=False)
check("[RC-023] subscribed_keys on a normal fetch → 400", r.status_code==400 and "subscribed_keys" in r.text, r.text)
r=fetch(v3,[f"k{i}" for i in range(501)])
check("[RC-023] more than 500 keys → 400", r.status_code==400, r.text[:200])
print(f"\n{sum(results)}/{len(results)} passed")
raise SystemExit(0 if all(results) else 1)
