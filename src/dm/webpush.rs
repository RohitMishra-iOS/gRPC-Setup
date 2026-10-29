//! Web Push (RFC 8030) with VAPID (RFC 8292) and aes128gcm payload encryption (RFC 8291 / 8188).
//! No third party in between: the browser vendor's push service (FCM for Chrome, Mozilla autopush,
//! Apple Web Push) is addressed directly through the subscription's endpoint.
//!
//! Configure DM_WEBPUSH_VAPID_PRIVATE_KEY (PKCS#8 PEM; `\n` escapes allowed) and
//! DM_WEBPUSH_SUBJECT ("mailto:you@example.com" or https URL). Generate a key pair with
//! POST /admin/messaging/webpush/keys. The public key (base64url) is what the JS SDK passes as
//! applicationServerKey; it is served at GET /v1/messaging/webpush-key.

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_128_GCM};
use ring::agreement::{agree_ephemeral, EphemeralPrivateKey, UnparsedPublicKey, ECDH_P256};
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use serde_json::{json, Value as Json};

pub struct WebPushClient {
    key_pair: EcdsaKeyPair,
    encoding_key: jsonwebtoken::EncodingKey,
    public_key: String,
    subject: String,
    http: reqwest::Client,
}

#[derive(Debug)]
pub struct WebPushError {
    pub status: u16,
    pub reason: String,
}

impl WebPushError {
    /// 404/410 from the push service: the subscription is gone.
    pub fn is_invalid_token(&self) -> bool {
        self.status == 404 || self.status == 410
    }
}

pub struct Subscription<'a> {
    pub endpoint: &'a str,
    pub p256dh: &'a str,
    pub auth: &'a str,
}

fn pem_to_der(pem: &str) -> Result<Vec<u8>, String> {
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    STANDARD.decode(body.trim()).map_err(|e| format!("VAPID key PEM: {e}"))
}

/// New VAPID key pair: (PKCS#8 PEM private key, base64url public key).
pub fn generate_keys() -> (String, String) {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).expect("P-256 key generation");
    let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).expect("generated key parses");
    let b64 = STANDARD.encode(pkcs8.as_ref());
    let lines: Vec<String> = b64.as_bytes().chunks(64).map(|c| String::from_utf8_lossy(c).to_string()).collect();
    let pem = format!("-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n", lines.join("\n"));
    (pem, URL_SAFE_NO_PAD.encode(pair.public_key().as_ref()))
}

impl WebPushClient {
    pub fn from_env() -> Option<Result<Self, String>> {
        let pem = crate::env_nonempty("DM_WEBPUSH_VAPID_PRIVATE_KEY")?.replace("\\n", "\n");
        let subject = crate::env_nonempty("DM_WEBPUSH_SUBJECT").unwrap_or_else(|| "mailto:admin@example.com".into());
        Some(Self::new(&pem, &subject))
    }

    pub fn new(pem: &str, subject: &str) -> Result<Self, String> {
        let der = pem_to_der(pem)?;
        let key_pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &der, &SystemRandom::new()).map_err(|e| format!("VAPID key: {e}"))?;
        let public_key = URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref());
        let encoding_key = jsonwebtoken::EncodingKey::from_ec_der(&der);
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(15)).build().map_err(|e| e.to_string())?;
        Ok(Self { key_pair, encoding_key, public_key, subject: subject.to_string(), http })
    }

    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    fn vapid(&self, endpoint: &str) -> Result<String, WebPushError> {
        let url = reqwest::Url::parse(endpoint).map_err(|e| WebPushError { status: 0, reason: format!("bad endpoint: {e}") })?;
        let audience = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
        let exp = chrono::Utc::now().timestamp() + 12 * 3600;
        let jwt = jsonwebtoken::encode(&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256), &json!({ "aud": audience, "exp": exp, "sub": self.subject }), &self.encoding_key)
            .map_err(|e| WebPushError { status: 0, reason: format!("VAPID JWT: {e}") })?;
        let _ = &self.key_pair;
        Ok(format!("vapid t={jwt}, k={}", self.public_key))
    }

    pub async fn send(&self, subscription: &Subscription<'_>, payload: &Json, ttl_seconds: u64, urgent: bool, topic: Option<&str>) -> Result<(), WebPushError> {
        let body = encrypt(subscription.p256dh, subscription.auth, payload.to_string().as_bytes()).map_err(|reason| WebPushError { status: 0, reason })?;
        let mut request = self
            .http
            .post(subscription.endpoint)
            .header("authorization", self.vapid(subscription.endpoint)?)
            .header("content-encoding", "aes128gcm")
            .header("content-type", "application/octet-stream")
            .header("ttl", ttl_seconds.to_string())
            .header("urgency", if urgent { "high" } else { "normal" })
            .body(body);
        if let Some(topic) = topic {
            // Topic: ≤ 32 URL-safe base64 chars.
            let clean: String = topic.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').take(32).collect();
            request = request.header("topic", clean);
        }
        let response = request.send().await.map_err(|e| WebPushError { status: 0, reason: e.to_string() })?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        Err(WebPushError { status, reason: format!("HTTP {status}: {}", text.chars().take(200).collect::<String>()) })
    }
}

struct Len(usize);
impl KeyType for Len {
    fn len(&self) -> usize {
        self.0
    }
}

fn hkdf(salt: &[u8], ikm: &[u8], info: &[u8], len: usize) -> Vec<u8> {
    let prk = Salt::new(HKDF_SHA256, salt).extract(ikm);
    let info = [info];
    let okm = prk.expand(&info, Len(len)).expect("hkdf length");
    let mut out = vec![0u8; len];
    okm.fill(&mut out).expect("hkdf fill");
    out
}

/// RFC 8291 aes128gcm encryption for one record (payload ≤ 3993 bytes).
pub fn encrypt(p256dh: &str, auth: &str, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let ua_public = URL_SAFE_NO_PAD.decode(p256dh.trim_end_matches('=')).map_err(|_| "p256dh is not base64url")?;
    let auth_secret = URL_SAFE_NO_PAD.decode(auth.trim_end_matches('=')).map_err(|_| "auth is not base64url")?;
    if ua_public.len() != 65 || auth_secret.len() != 16 {
        return Err("subscription keys have the wrong length".into());
    }
    if plaintext.len() > 3993 {
        return Err("payload too large for Web Push (max ~4 KB)".into());
    }
    let rng = SystemRandom::new();
    let as_private = EphemeralPrivateKey::generate(&ECDH_P256, &rng).map_err(|_| "ECDH key generation failed")?;
    let as_public = as_private.compute_public_key().map_err(|_| "ECDH public key failed")?.as_ref().to_vec();
    let shared = agree_ephemeral(as_private, &UnparsedPublicKey::new(&ECDH_P256, &ua_public), |secret| secret.to_vec()).map_err(|_| "invalid subscription public key")?;
    encrypt_with(&shared, &ua_public, &as_public, &auth_secret, plaintext, &rng)
}

fn encrypt_with(shared: &[u8], ua_public: &[u8], as_public: &[u8], auth_secret: &[u8], plaintext: &[u8], rng: &dyn SecureRandom) -> Result<Vec<u8>, String> {
    // IKM = HKDF(auth_secret, ecdh_secret, "WebPush: info" || 0x00 || ua_public || as_public, 32)
    let mut key_info = b"WebPush: info\0".to_vec();
    key_info.extend_from_slice(ua_public);
    key_info.extend_from_slice(as_public);
    let ikm = hkdf(auth_secret, shared, &key_info, 32);
    let mut salt = [0u8; 16];
    rng.fill(&mut salt).map_err(|_| "rng")?;
    let cek = hkdf(&salt, &ikm, b"Content-Encoding: aes128gcm\0", 16);
    let nonce = hkdf(&salt, &ikm, b"Content-Encoding: nonce\0", 12);
    let key = LessSafeKey::new(UnboundKey::new(&AES_128_GCM, &cek).map_err(|_| "aes key")?);
    let mut record = plaintext.to_vec();
    record.push(0x02); // last-record delimiter
    key.seal_in_place_append_tag(Nonce::try_assume_unique_for_key(&nonce).map_err(|_| "nonce")?, Aad::empty(), &mut record).map_err(|_| "seal")?;
    // Header: salt(16) | rs(4) | idlen(1) | keyid(as_public)
    let mut out = salt.to_vec();
    out.extend_from_slice(&4096u32.to_be_bytes());
    out.push(as_public.len() as u8);
    out.extend_from_slice(as_public);
    out.extend_from_slice(&record);
    Ok(out)
}

/// Receiver side (tests only): decrypts with the UA's ECDH secret.
#[cfg(test)]
fn decrypt_with(shared: &[u8], ua_public: &[u8], auth_secret: &[u8], body: &[u8]) -> Vec<u8> {
    let salt = &body[..16];
    let idlen = body[20] as usize;
    let as_public = &body[21..21 + idlen];
    let mut key_info = b"WebPush: info\0".to_vec();
    key_info.extend_from_slice(ua_public);
    key_info.extend_from_slice(as_public);
    let ikm = hkdf(auth_secret, shared, &key_info, 32);
    let cek = hkdf(salt, &ikm, b"Content-Encoding: aes128gcm\0", 16);
    let nonce = hkdf(salt, &ikm, b"Content-Encoding: nonce\0", 12);
    let key = LessSafeKey::new(UnboundKey::new(&AES_128_GCM, &cek).unwrap());
    let mut record = body[21 + idlen..].to_vec();
    let plain = key.open_in_place(Nonce::try_assume_unique_for_key(&nonce).unwrap(), Aad::empty(), &mut record).unwrap();
    let mut plain = plain.to_vec();
    assert_eq!(plain.pop(), Some(0x02));
    plain
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc8291_round_trip() {
        let rng = SystemRandom::new();
        // UA (browser) key pair and the AS (server) ephemeral pair, agreed from both sides.
        let ua_private = EphemeralPrivateKey::generate(&ECDH_P256, &rng).unwrap();
        let ua_public = ua_private.compute_public_key().unwrap().as_ref().to_vec();
        let as_private = EphemeralPrivateKey::generate(&ECDH_P256, &rng).unwrap();
        let as_public = as_private.compute_public_key().unwrap().as_ref().to_vec();
        let shared_as = agree_ephemeral(as_private, &UnparsedPublicKey::new(&ECDH_P256, &ua_public), |s| s.to_vec()).unwrap();
        let shared_ua = agree_ephemeral(ua_private, &UnparsedPublicKey::new(&ECDH_P256, &as_public), |s| s.to_vec()).unwrap();
        assert_eq!(shared_as, shared_ua);
        let mut auth = [0u8; 16];
        rng.fill(&mut auth).unwrap();
        let body = encrypt_with(&shared_as, &ua_public, &as_public, &auth, b"{\"hello\":\"web\"}", &rng).unwrap();
        assert_eq!(&body[21..86], &as_public[..]);
        assert_eq!(decrypt_with(&shared_ua, &ua_public, &auth, &body), b"{\"hello\":\"web\"}");
    }

    #[test]
    fn vapid_keys_and_header() {
        let (pem, public) = generate_keys();
        let client = WebPushClient::new(&pem, "mailto:test@example.com").unwrap();
        assert_eq!(client.public_key(), public);
        assert_eq!(URL_SAFE_NO_PAD.decode(&public).unwrap().len(), 65);
        let header = client.vapid("https://fcm.googleapis.com/fcm/send/abc").unwrap();
        assert!(header.starts_with("vapid t=") && header.ends_with(&format!(", k={public}")));
        let jwt = header.trim_start_matches("vapid t=").split(',').next().unwrap();
        let claims: Json = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(jwt.split('.').nth(1).unwrap()).unwrap()).unwrap();
        assert_eq!(claims["aud"], "https://fcm.googleapis.com");
    }
}
