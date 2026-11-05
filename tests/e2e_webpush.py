"""Web Push interop: server encrypts + VAPID-signs; http_ece (reference impl) decrypts."""
# spec: MSG-001, MSG-003 (Web Push registration and RFC 8291 encryption, decrypted by the reference implementation)
import base64, json, threading, requests, http_ece, jwt
from http.server import BaseHTTPRequestHandler, HTTPServer
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
import os, sys

B = "http://127.0.0.1:8090"; H = {"x-api-key": "test-key", "x-dm-app-id": "dmart-ready-ios"}
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
ua_key = ec.generate_private_key(ec.SECP256R1())
ua_pub = ua_key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
auth = os.urandom(16)
received = {}

class Push(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"]))
        received.update(headers=dict(self.headers), body=body)
        self.send_response(201); self.end_headers()

srv = HTTPServer(("127.0.0.1", 8098), Push); threading.Thread(target=srv.serve_forever, daemon=True).start()
vapid_public = requests.get(B + "/v1/messaging/webpush-key", headers=H).json()["publicKey"]
r = requests.post(B + "/v1/rpc/dm.sdk.v1.MessagingService/RegisterDevice", headers=H, json={
    "context": {"appId": "dmart-ready-ios", "installationId": "web-install-1", "platform": "web"},
    "pushTransport": "webpush", "webPush": {"endpoint": "http://127.0.0.1:8098/push/abc", "p256dh": b64u(ua_pub), "auth": b64u(auth)}, "topics": ["news"]})
print("register:", r.status_code, r.json())
r = requests.post(B + "/admin/messaging/send", headers={"x-admin-key": "test-admin"}, json={
    "app_id": "dmart-ready-ios", "target": {"topic": "news"}, "notification": {"title": "Diwali sale", "body": "40% off"}, "data": {"deeplink": "dmart://offers"}})
print("send:", r.json())
plain = http_ece.decrypt(received["body"], private_key=ua_key, dh=None, auth_secret=auth, version="aes128gcm")
msg = json.loads(plain)
print("decrypted:", msg)
auth_header = received["headers"]["authorization"]
token = auth_header.split("t=")[1].split(",")[0]
key_b = base64.urlsafe_b64decode(vapid_public + "==")
pub = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), key_b)
claims = jwt.decode(token, pub, algorithms=["ES256"], audience="http://127.0.0.1")
ok = (msg["notification"]["title"] == "Diwali sale" and msg["data"]["deeplink"] == "dmart://offers" and msg["data"]["dm"]["message_id"].startswith("m-")
      and received["headers"]["content-encoding"] == "aes128gcm" and claims["sub"].startswith("mailto:") and r.json()["sent"] == 1)
print("WEB PUSH INTEROP:", "PASS" if ok else "FAIL")
