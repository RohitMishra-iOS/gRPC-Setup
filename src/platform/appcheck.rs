//! App Check — proves a request comes from your genuine app on a genuine device. A provider
//! produces an attestation, the server verifies it and returns a short-lived App Check token,
//! which the SDKs send as `x-dm-appcheck` on every call.
//!
//! POST /v1/appcheck/exchange
//!   {"app_id": "...", "provider": "debug",          "token": "<debug token>"}
//!   {"app_id": "...", "provider": "devicecheck",    "token": "<DCDevice token, base64>"}
//!   {"app_id": "...", "provider": "playintegrity",  "token": "<integrity token>", "package_name": "..."}
//!   {"app_id": "...", "provider": "appattest",      "key_id": "...", "attestation": "<base64 CBOR>", "challenge": "..."}
//!   {"app_id": "...", "provider": "appattest",      "key_id": "...", "assertion": "<base64 CBOR>", "challenge": "..."}
//!   optional "limited_use": true  → single-use token (replay protection)
//!   → {"token": "...", "ttl_seconds": 3600}
//! POST /v1/appcheck/challenge   {"app_id"} → {"challenge", "ttl_seconds"}   (App Attest)
//! POST /v1/appcheck/verify      {"token", "consume"?} → claims   (for your own backends)
//!
//! Admin (dm/admin.rs → platform_admin.rs): mint tokens for the custom provider, manage debug tokens.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use base64::Engine;
use mongodb::bson::{doc, Binary, DateTime};
use serde::Deserialize;
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};

use super::tokens::{self, AppCheckClaims};
use super::{ok, parse_json, ApiError, ApiResult};
use crate::dm::AppState;

pub const DEBUG_TOKENS: &str = "appcheck_debug_tokens";
pub const APP_ATTEST_KEYS: &str = "appcheck_app_attest_keys";
pub const CHALLENGES: &str = "appcheck_challenges";
pub const USED_TOKENS: &str = "appcheck_used_tokens";

#[derive(Deserialize, Default)]
#[serde(default)]
struct ExchangeBody {
    #[serde(alias = "appId")]
    app_id: String,
    provider: String,
    token: String,
    #[serde(alias = "packageName")]
    package_name: String,
    #[serde(alias = "keyId")]
    key_id: String,
    attestation: String,
    assertion: String,
    challenge: String,
    #[serde(alias = "limitedUse")]
    limited_use: bool,
}

fn b64(text: &str) -> Result<Vec<u8>, ApiError> {
    let cleaned: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(&cleaned)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(cleaned.trim_end_matches('=')))
        .map_err(|_| ApiError::bad_request("INVALID_ARGUMENT", "value is not base64"))
}

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(axum::http::StatusCode::FORBIDDEN, "ATTESTATION_INVALID", message)
}

pub fn issue(state: &AppState, app_id: &str, provider: &str, limited_use: bool, ttl: Option<u64>) -> Json {
    let ttl = ttl.unwrap_or(state.config.platform.appcheck_ttl_seconds) as i64;
    let now = tokens::now();
    let claims = AppCheckClaims {
        typ: "appcheck".into(),
        sub: app_id.into(),
        provider: provider.into(),
        iat: now,
        exp: if limited_use { now + 300 } else { now + ttl },
        jti: uuid::Uuid::new_v4().to_string(),
        limited_use,
    };
    json!({ "token": state.tokens.sign(&claims), "ttl_seconds": claims.exp - now })
}

/// Provider-specific required fields and sizes.
fn validate_exchange(app_id: &str, b: &ExchangeBody) -> crate::validate::V {
    use crate::validate::{limits, V};
    let mut v = V::new();
    v.id("app_id", app_id, limits::ID);
    if v.one_of("provider", &b.provider, &["debug", "devicecheck", "playintegrity", "appattest"]) {
        match b.provider.as_str() {
            "debug" => {
                v.len("token", &b.token, 8, 256);
            }
            "devicecheck" | "playintegrity" => {
                v.len("token", &b.token, 16, 64 * 1024);
            }
            _ => {
                v.len("key_id", &b.key_id, 16, 128);
                v.len("challenge", &b.challenge, 16, 256);
                if b.attestation.is_empty() == b.assertion.is_empty() {
                    v.fail("attestation", "send either attestation (first time) or assertion (afterwards)");
                }
                v.check(b.attestation.len() <= 64 * 1024 && b.assertion.len() <= 16 * 1024, "attestation", "is too large");
            }
        }
    }
    if b.provider == "playintegrity" {
        // Java package name: segments of letters/digits/underscores, at least two.
        let parts: Vec<&str> = b.package_name.split('.').collect();
        v.check(parts.len() >= 2 && b.package_name.len() <= 255 && parts.iter().all(|p| p.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')), "package_name", "must be an Android package name like com.dmart.app");
    }
    v
}

pub async fn exchange(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let body: ExchangeBody = parse_json(&body)?;
    let app_id = if body.app_id.is_empty() {
        headers.get("x-dm-app-id").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
    } else {
        body.app_id.clone()
    };
    validate_exchange(&app_id, &body).into_api()?;
    if state.config.auth_enabled() {
        let key = headers.get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.key_allows(key, &app_id) {
            return Err(ApiError::unauthenticated("API_KEY_INVALID", "missing or invalid x-api-key for this app"));
        }
    }
    match body.provider.as_str() {
        "debug" => verify_debug(&state, &app_id, &body.token).await?,
        "devicecheck" => verify_devicecheck(&state, &body.token).await?,
        "playintegrity" => verify_play_integrity(&state, &body.token, &body.package_name).await?,
        "appattest" => verify_app_attest(&state, &app_id, &body).await?,
        "" => return Err(ApiError::bad_request("INVALID_ARGUMENT", "provider is required")),
        other => return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("unknown provider '{other}'"))),
    }
    ok(issue(&state, &app_id, &body.provider, body.limited_use, None))
}

/// Verifies an App Check token. Limited-use tokens are consumed only when `consume` is set.
pub async fn verify_token(state: &AppState, token: &str) -> Result<AppCheckClaims, String> {
    let claims: AppCheckClaims = state.tokens.verify(token, "appcheck")?;
    Ok(claims)
}

#[derive(Deserialize)]
struct VerifyBody {
    token: String,
    #[serde(default)]
    consume: bool,
}

pub async fn verify(State(state): State<AppState>, body: Bytes) -> ApiResult {
    let body: VerifyBody = parse_json(&body)?;
    let claims = verify_token(&state, &body.token).await.map_err(|e| ApiError::unauthenticated("APP_CHECK_INVALID", e))?;
    let mut already_consumed = false;
    if body.consume || claims.limited_use {
        let record = doc! { "_id": &claims.jti, "expires_at": DateTime::from_millis(claims.exp * 1000) };
        match state.db.coll(USED_TOKENS).insert_one(record).await {
            Ok(_) => {}
            Err(err) if crate::db::is_duplicate_key(&err) => already_consumed = true,
            Err(err) => return Err(ApiError::db(err)),
        }
    }
    ok(json!({ "app_id": claims.sub, "provider": claims.provider, "expires_at": claims.exp, "limited_use": claims.limited_use, "already_consumed": already_consumed }))
}

// ── Debug provider ───────────────────────────────────────────────────────────

async fn verify_debug(state: &AppState, app_id: &str, token: &str) -> ApiResult<()> {
    if token.is_empty() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "token (the debug token) is required"));
    }
    if state.config.platform.appcheck_debug_tokens.iter().any(|t| crate::config::constant_time_eq(t.as_bytes(), token.as_bytes())) {
        return Ok(());
    }
    let found = state
        .db
        .coll(DEBUG_TOKENS)
        .find_one(doc! { "_id": token, "$or": [{ "app_id": app_id }, { "app_id": null }] })
        .await
        .map_err(ApiError::db)?;
    match found {
        Some(_) => Ok(()),
        None => Err(invalid("unknown debug token — add it in the console (App Check → Debug tokens) or DM_APPCHECK_DEBUG_TOKENS")),
    }
}

// ── DeviceCheck (Apple) ──────────────────────────────────────────────────────

async fn verify_devicecheck(state: &AppState, device_token: &str) -> ApiResult<()> {
    let p = &state.config.platform;
    let (Some(pem), Some(key_id), Some(team_id)) = (&p.devicecheck_key_pem, &p.devicecheck_key_id, &p.devicecheck_team_id) else {
        return Err(ApiError::new(axum::http::StatusCode::NOT_IMPLEMENTED, "PROVIDER_NOT_CONFIGURED", "DeviceCheck needs DM_DEVICECHECK_KEY_P8/KEY_ID/TEAM_ID (or the APNs key with DeviceCheck enabled)"));
    };
    if device_token.is_empty() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "token (DCDevice token) is required"));
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some(key_id.clone());
    let key = jsonwebtoken::EncodingKey::from_ec_pem(pem.as_bytes()).map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "PROVIDER_NOT_CONFIGURED", format!("DeviceCheck key: {e}")))?;
    let jwt = jsonwebtoken::encode(&header, &json!({ "iss": team_id, "iat": tokens::now() }), &key)
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", e.to_string()))?;
    let host = if p.devicecheck_development { "api.development.devicecheck.apple.com" } else { "api.devicecheck.apple.com" };
    let response = state
        .http
        .post(format!("https://{host}/v1/validate_device_token"))
        .bearer_auth(jwt)
        .json(&json!({ "device_token": device_token, "transaction_id": uuid::Uuid::new_v4().to_string(), "timestamp": chrono::Utc::now().timestamp_millis() }))
        .send()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("DeviceCheck: {e}")))?;
    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        Err(invalid(format!("DeviceCheck rejected the token ({status}): {text}")))
    }
}

// ── Play Integrity (Android) ─────────────────────────────────────────────────

async fn google_access_token(state: &AppState, service_account: &str, scope: &str) -> ApiResult<String> {
    let sa: Json = serde_json::from_str(service_account)
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "PROVIDER_NOT_CONFIGURED", format!("service account JSON: {e}")))?;
    let email = sa["client_email"].as_str().unwrap_or_default();
    let pem = sa["private_key"].as_str().unwrap_or_default();
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "PROVIDER_NOT_CONFIGURED", format!("service account key: {e}")))?;
    let now = tokens::now();
    let assertion = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &json!({ "iss": email, "scope": scope, "aud": "https://oauth2.googleapis.com/token", "iat": now, "exp": now + 3600 }),
        &key,
    )
    .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", e.to_string()))?;
    let response: Json = state
        .http
        .post("https://oauth2.googleapis.com/token")
        .form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", assertion.as_str())])
        .send()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("Google OAuth: {e}")))?
        .json()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("Google OAuth: {e}")))?;
    response["access_token"].as_str().map(str::to_string).ok_or_else(|| {
        ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("Google OAuth: {response}"))
    })
}

/// Checks a decoded Play Integrity verdict (separated for tests).
pub fn check_integrity_verdict(payload: &Json, package_name: &str, now_ms: i64) -> Result<(), String> {
    let request = &payload["requestDetails"];
    if request["requestPackageName"].as_str() != Some(package_name) {
        return Err(format!("token was issued to {}, not {package_name}", request["requestPackageName"]));
    }
    let issued: i64 = request["timestampMillis"].as_str().and_then(|s| s.parse().ok()).or_else(|| request["timestampMillis"].as_i64()).unwrap_or(0);
    if (now_ms - issued).abs() > 10 * 60 * 1000 {
        return Err("integrity token is older than 10 minutes".into());
    }
    if payload["appIntegrity"]["appRecognitionVerdict"].as_str() != Some("PLAY_RECOGNIZED") {
        return Err(format!("app not recognized by Play ({})", payload["appIntegrity"]["appRecognitionVerdict"]));
    }
    let device = payload["deviceIntegrity"]["deviceRecognitionVerdict"].as_array().cloned().unwrap_or_default();
    if !device.iter().any(|v| v == "MEETS_DEVICE_INTEGRITY" || v == "MEETS_STRONG_INTEGRITY") {
        return Err("device does not meet device integrity".into());
    }
    Ok(())
}

async fn verify_play_integrity(state: &AppState, integrity_token: &str, package_name: &str) -> ApiResult<()> {
    let Some(sa) = &state.config.platform.play_integrity_service_account else {
        return Err(ApiError::new(axum::http::StatusCode::NOT_IMPLEMENTED, "PROVIDER_NOT_CONFIGURED", "Play Integrity needs DM_PLAY_INTEGRITY_SERVICE_ACCOUNT"));
    };
    if integrity_token.is_empty() || package_name.is_empty() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "token and package_name are required"));
    }
    let access = google_access_token(state, sa, "https://www.googleapis.com/auth/playintegrity").await?;
    let response: Json = state
        .http
        .post(format!("https://playintegrity.googleapis.com/v1/{package_name}:decodeIntegrityToken"))
        .bearer_auth(access)
        .json(&json!({ "integrity_token": integrity_token }))
        .send()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("Play Integrity: {e}")))?
        .json()
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("Play Integrity: {e}")))?;
    let payload = &response["tokenPayloadExternal"];
    if payload.is_null() {
        return Err(invalid(format!("Play Integrity: {response}")));
    }
    check_integrity_verdict(payload, package_name, chrono::Utc::now().timestamp_millis()).map_err(invalid)
}

// ── App Attest (Apple) ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ChallengeBody {
    #[serde(default, alias = "appId")]
    app_id: String,
}

/// One-time challenge for App Attest (valid 5 minutes).
pub async fn challenge(State(state): State<AppState>, body: Bytes) -> ApiResult {
    let body: ChallengeBody = parse_json(&body)?;
    let challenge = tokens::random_token();
    state
        .db
        .coll(CHALLENGES)
        .insert_one(doc! { "_id": &challenge, "app_id": &body.app_id, "expires_at": DateTime::from_millis(chrono::Utc::now().timestamp_millis() + 300_000) })
        .await
        .map_err(ApiError::db)?;
    ok(json!({ "challenge": challenge, "ttl_seconds": 300 }))
}

async fn take_challenge(state: &AppState, challenge: &str) -> ApiResult<()> {
    let removed = state
        .db
        .coll(CHALLENGES)
        .find_one_and_delete(doc! { "_id": challenge, "expires_at": { "$gt": DateTime::now() } })
        .await
        .map_err(ApiError::db)?;
    removed.map(|_| ()).ok_or_else(|| invalid("unknown or expired challenge — request a new one from /v1/appcheck/challenge"))
}

/// The app's App Attest identity: "<TEAMID>.<bundle id>" per app id, from DM_APP_ATTEST_APPS
/// ("app_id=TEAMID.bundle,..."); production unless DM_APP_ATTEST_DEVELOPMENT=true.
fn app_attest_identity(app_id: &str) -> Option<String> {
    crate::env_nonempty("DM_APP_ATTEST_APPS")?
        .split(',')
        .filter_map(|e| e.trim().split_once('='))
        .find(|(id, _)| *id == app_id)
        .map(|(_, ident)| ident.to_string())
}

async fn verify_app_attest(state: &AppState, app_id: &str, body: &ExchangeBody) -> ApiResult<()> {
    let identity = app_attest_identity(app_id).ok_or_else(|| {
        ApiError::new(axum::http::StatusCode::NOT_IMPLEMENTED, "PROVIDER_NOT_CONFIGURED", format!("add {app_id}=<TEAMID>.<bundle id> to DM_APP_ATTEST_APPS"))
    })?;
    if body.key_id.is_empty() || body.challenge.is_empty() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "key_id and challenge are required"));
    }
    take_challenge(state, &body.challenge).await?;
    let keys = state.db.coll(APP_ATTEST_KEYS);
    let development = crate::env_nonempty("DM_APP_ATTEST_DEVELOPMENT").is_some_and(|v| v == "true" || v == "1");

    if !body.attestation.is_empty() {
        let root = crate::env_nonempty("DM_APP_ATTEST_ROOT_CA").map(|r| r.replace("\\n", "\n")).ok_or_else(|| {
            ApiError::new(axum::http::StatusCode::NOT_IMPLEMENTED, "PROVIDER_NOT_CONFIGURED", "set DM_APP_ATTEST_ROOT_CA to Apple_App_Attestation_Root_CA.pem (apple.com/certificateauthority)")
        })?;
        let verified = app_attest::verify_attestation(&b64(&body.attestation)?, &body.challenge, &b64(&body.key_id)?, &identity, development, &root)
            .map_err(invalid)?;
        keys.update_one(
            doc! { "_id": &body.key_id },
            doc! { "$set": { "app_id": app_id, "public_key": Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: verified.public_key }, "counter": 0_i64, "created_at": DateTime::now() } },
        )
        .upsert(true)
        .await
        .map_err(ApiError::db)?;
        return Ok(());
    }
    if !body.assertion.is_empty() {
        let record = keys
            .find_one(doc! { "_id": &body.key_id, "app_id": app_id })
            .await
            .map_err(ApiError::db)?
            .ok_or_else(|| invalid("unknown App Attest key — attest it first"))?;
        let public_key = record.get_binary_generic("public_key").map_err(|_| invalid("stored key is corrupt"))?.clone();
        let counter = record.get_i64("counter").unwrap_or(0);
        let new_counter = app_attest::verify_assertion(&b64(&body.assertion)?, body.challenge.as_bytes(), &public_key, &identity, counter).map_err(invalid)?;
        keys.update_one(doc! { "_id": &body.key_id, "counter": counter }, doc! { "$set": { "counter": new_counter as i64, "last_used": DateTime::now() } })
            .await
            .map_err(ApiError::db)?;
        return Ok(());
    }
    Err(ApiError::bad_request("INVALID_ARGUMENT", "attestation or assertion is required"))
}

pub mod app_attest {
    //! App Attest verification, following Apple's "Validating apps that connect to your server".
    use super::*;
    use x509_parser::prelude::*;

    pub struct Verified {
        /// Uncompressed P-256 public key (65 bytes).
        pub public_key: Vec<u8>,
    }

    fn cbor(bytes: &[u8]) -> Result<ciborium::Value, String> {
        ciborium::de::from_reader(bytes).map_err(|e| format!("CBOR: {e}"))
    }

    fn map_get<'a>(value: &'a ciborium::Value, key: &str) -> Option<&'a ciborium::Value> {
        value.as_map()?.iter().find(|(k, _)| k.as_text() == Some(key)).map(|(_, v)| v)
    }

    pub struct AuthData<'a> {
        pub rp_id_hash: &'a [u8],
        pub counter: u32,
        pub aaguid: Option<&'a [u8]>,
        pub credential_id: Option<&'a [u8]>,
    }

    pub fn parse_auth_data(data: &[u8]) -> Result<AuthData<'_>, String> {
        if data.len() < 37 {
            return Err("authenticator data too short".into());
        }
        let counter = u32::from_be_bytes([data[33], data[34], data[35], data[36]]);
        let (aaguid, credential_id) = if data[32] & 0x40 != 0 && data.len() >= 55 {
            let len = u16::from_be_bytes([data[53], data[54]]) as usize;
            if data.len() < 55 + len {
                return Err("credential id truncated".into());
            }
            (Some(&data[37..53]), Some(&data[55..55 + len]))
        } else {
            (None, None)
        };
        Ok(AuthData { rp_id_hash: &data[0..32], counter, aaguid, credential_id })
    }

    pub fn verify_attestation(object: &[u8], challenge: &str, key_id: &[u8], identity: &str, development: bool, root_pem: &str) -> Result<Verified, String> {
        let attestation = cbor(object)?;
        if map_get(&attestation, "fmt").and_then(|v| v.as_text()) != Some("apple-appattest") {
            return Err("not an apple-appattest attestation".into());
        }
        let statement = map_get(&attestation, "attStmt").ok_or("attStmt missing")?;
        let x5c = map_get(statement, "x5c").and_then(|v| v.as_array()).ok_or("x5c missing")?;
        let certs: Vec<&[u8]> = x5c.iter().filter_map(|c| c.as_bytes().map(Vec::as_slice)).collect();
        if certs.len() < 2 {
            return Err("x5c needs credential + intermediate certificates".into());
        }
        let auth_data = map_get(&attestation, "authData").and_then(|v| v.as_bytes()).ok_or("authData missing")?;

        // 1. Chain: credCert ← intermediate ← Apple App Attestation Root CA.
        let (_, root_pem) = parse_x509_pem(root_pem.as_bytes()).map_err(|e| format!("root CA PEM: {e}"))?;
        let root = root_pem.parse_x509().map_err(|e| format!("root CA: {e}"))?;
        let (_, cred) = X509Certificate::from_der(certs[0]).map_err(|e| format!("credential cert: {e}"))?;
        let (_, intermediate) = X509Certificate::from_der(certs[1]).map_err(|e| format!("intermediate cert: {e}"))?;
        intermediate.verify_signature(Some(root.public_key())).map_err(|_| "intermediate not signed by Apple's App Attestation root")?;
        cred.verify_signature(Some(intermediate.public_key())).map_err(|_| "credential certificate not signed by the intermediate")?;
        let now = ASN1Time::now();
        if !cred.validity().is_valid_at(now) || !intermediate.validity().is_valid_at(now) {
            return Err("certificate expired or not yet valid".into());
        }

        // 2–4. nonce = SHA256(authData || SHA256(challenge)) must equal the credCert extension.
        let client_hash = Sha256::digest(challenge.as_bytes());
        let mut nonce_input = auth_data.to_vec();
        nonce_input.extend_from_slice(&client_hash);
        let nonce = Sha256::digest(&nonce_input);
        let oid = x509_parser::der_parser::oid!(1.2.840 .113635 .100 .8 .2);
        let ext = cred.extensions().iter().find(|e| e.oid == oid).ok_or("nonce extension missing")?;
        // The extension value is SEQUENCE { [1] EXPLICIT OCTET STRING nonce }; the nonce is the
        // last 32 bytes.
        let value = ext.value;
        if value.len() < 32 || value[value.len() - 32..] != nonce[..] {
            return Err("nonce mismatch (challenge or authData tampered)".into());
        }

        // 5. key id = SHA256(public key).
        let public_key = cred.public_key().subject_public_key.data.to_vec();
        if Sha256::digest(&public_key)[..] != *key_id {
            return Err("key id does not match the attested public key".into());
        }

        // 6–9. authenticator data.
        let parsed = parse_auth_data(auth_data)?;
        if parsed.rp_id_hash != Sha256::digest(identity.as_bytes()).as_slice() {
            return Err(format!("attestation is for another app (expected {identity})"));
        }
        if parsed.counter != 0 {
            return Err("counter must be 0 in an attestation".into());
        }
        let expected_aaguid: &[u8] = if development { b"appattestdevelop" } else { b"appattest\0\0\0\0\0\0\0" };
        if parsed.aaguid != Some(expected_aaguid) {
            return Err(if development { "not a development attestation" } else { "not a production attestation (set DM_APP_ATTEST_DEVELOPMENT=true for debug builds)" }.into());
        }
        if parsed.credential_id != Some(key_id) {
            return Err("credential id does not match the key id".into());
        }
        Ok(Verified { public_key })
    }

    /// Returns the new counter.
    pub fn verify_assertion(object: &[u8], client_data: &[u8], public_key: &[u8], identity: &str, previous_counter: i64) -> Result<u32, String> {
        let assertion = cbor(object)?;
        let signature = map_get(&assertion, "signature").and_then(|v| v.as_bytes()).ok_or("signature missing")?;
        let auth_data = map_get(&assertion, "authenticatorData").and_then(|v| v.as_bytes()).ok_or("authenticatorData missing")?;
        let mut message = auth_data.clone();
        message.extend_from_slice(&Sha256::digest(client_data));
        // nonce = SHA256(authenticatorData || SHA256(clientData)); the signature is ECDSA-P256-SHA256
        // over the nonce, i.e. a SHA256 of it again.
        let nonce = Sha256::digest(&message);
        let key = ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_ASN1, public_key);
        key.verify(&nonce, signature).map_err(|_| "assertion signature is invalid")?;
        let parsed = parse_auth_data(auth_data)?;
        if parsed.rp_id_hash != Sha256::digest(identity.as_bytes()).as_slice() {
            return Err("assertion is for another app".into());
        }
        if i64::from(parsed.counter) <= previous_counter {
            return Err("assertion counter did not increase (replay)".into());
        }
        Ok(parsed.counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn play_integrity_verdicts() {
        let now = 1_800_000_000_000;
        let good = json!({
            "requestDetails": { "requestPackageName": "com.dmart.ready", "timestampMillis": now.to_string() },
            "appIntegrity": { "appRecognitionVerdict": "PLAY_RECOGNIZED" },
            "deviceIntegrity": { "deviceRecognitionVerdict": ["MEETS_DEVICE_INTEGRITY"] },
        });
        assert!(check_integrity_verdict(&good, "com.dmart.ready", now).is_ok());
        assert!(check_integrity_verdict(&good, "com.other", now).is_err());
        assert!(check_integrity_verdict(&good, "com.dmart.ready", now + 11 * 60 * 1000).is_err());
        let mut sideloaded = good.clone();
        sideloaded["appIntegrity"]["appRecognitionVerdict"] = json!("UNRECOGNIZED_VERSION");
        assert!(check_integrity_verdict(&sideloaded, "com.dmart.ready", now).is_err());
        let mut emulator = good.clone();
        emulator["deviceIntegrity"]["deviceRecognitionVerdict"] = json!([]);
        assert!(check_integrity_verdict(&emulator, "com.dmart.ready", now).is_err());
    }

    #[test]
    fn app_attest_assertion_signature_and_counter() {
        use ring::rand::SystemRandom;
        use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_ASN1_SIGNING};
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng).unwrap();
        let identity = "TEAM123.com.dmart.ready";
        let mut auth_data = Sha256::digest(identity.as_bytes()).to_vec();
        auth_data.push(0x01);
        auth_data.extend_from_slice(&7u32.to_be_bytes());
        let client_data = b"challenge-123";
        let mut message = auth_data.clone();
        message.extend_from_slice(&Sha256::digest(client_data));
        let nonce = Sha256::digest(&message);
        let signature = pair.sign(&rng, &nonce).unwrap();
        let assertion = ciborium::Value::Map(vec![
            (ciborium::Value::Text("signature".into()), ciborium::Value::Bytes(signature.as_ref().to_vec())),
            (ciborium::Value::Text("authenticatorData".into()), ciborium::Value::Bytes(auth_data)),
        ]);
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&assertion, &mut encoded).unwrap();
        let public_key = pair.public_key().as_ref();
        assert_eq!(app_attest::verify_assertion(&encoded, client_data, public_key, identity, 6).unwrap(), 7);
        assert!(app_attest::verify_assertion(&encoded, client_data, public_key, identity, 7).is_err(), "replay");
        assert!(app_attest::verify_assertion(&encoded, b"other", public_key, identity, 0).is_err(), "wrong client data");
        assert!(app_attest::verify_assertion(&encoded, client_data, public_key, "TEAM.other", 0).is_err(), "wrong app");
    }
}
