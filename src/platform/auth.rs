//! Authentication (Firebase Auth / Identity Toolkit equivalent).
//!
//! Every sign-in returns a 1-hour ID token (HS256 JWT, Firebase-shaped claims) plus a long-lived
//! refresh token; clients refresh ~5 minutes before expiry via POST /v1/token. Request and response
//! field names follow the Identity Toolkit REST API (camelCase), so existing knowledge transfers.
//!
//! POST /v1/accounts/signUp                {email?, password?, displayName?}      (none = anonymous)
//! POST /v1/accounts/signInWithPassword    {email, password}
//! POST /v1/accounts/signInWithCustomToken {token}                                (minted by your backend)
//! POST /v1/accounts/signInWithIdp         {providerId: "google.com"|"apple.com", idToken, nonce?}
//! POST /v1/accounts/signInWithEmailLink   {email, oobCode}
//! POST /v1/accounts/lookup                {idToken}
//! POST /v1/accounts/update                {idToken, displayName?, photoUrl?, email?, password?,
//!                                          deleteAttribute?: ["DISPLAY_NAME","PHOTO_URL"],
//!                                          deleteProvider?: [...]}   or {oobCode} to verify an email
//! POST /v1/accounts/delete                {idToken}
//! POST /v1/accounts/sendOobCode           {requestType: "PASSWORD_RESET"|"VERIFY_EMAIL"|"EMAIL_SIGNIN", email?, idToken?, continueUrl?}
//! POST /v1/accounts/resetPassword         {oobCode, newPassword?}    (without newPassword: checks the code)
//! POST /v1/token                          {grant_type: "refresh_token", refresh_token}
//! GET  /v1/auth/config                    which sign-in methods are enabled
//!
//! An Authorization: Bearer <ID token> on signInWithIdp / signInWithPassword-free signUp links the
//! new credential to the signed-in (e.g. anonymous) user instead of creating a new account.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use mongodb::bson::{self, doc, Bson, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json};

use super::tokens::{self, CustomTokenClaims, IdClaims, ID_TOKEN_TTL};
use super::{bearer, caller, ok, parse_json, ApiError, ApiResult};
use crate::dm::AppState;

pub const USERS: &str = "auth_users";
pub const REFRESH_TOKENS: &str = "auth_refresh_tokens";
pub const OOB_CODES: &str = "auth_oob_codes";

fn err(reason: &str) -> ApiError {
    let status = match reason {
        "EMAIL_NOT_FOUND" | "INVALID_PASSWORD" | "INVALID_LOGIN_CREDENTIALS" | "INVALID_ID_TOKEN" | "TOKEN_EXPIRED"
        | "INVALID_REFRESH_TOKEN" | "USER_NOT_FOUND" | "CREDENTIAL_TOO_OLD_LOGIN_AGAIN" | "INVALID_CUSTOM_TOKEN"
        | "INVALID_IDP_RESPONSE" => StatusCode::UNAUTHORIZED,
        "USER_DISABLED" | "OPERATION_NOT_ALLOWED" | "ADMIN_ONLY_OPERATION" => StatusCode::FORBIDDEN,
        "EMAIL_EXISTS" | "CREDENTIAL_ALREADY_IN_USE" | "FEDERATED_USER_ID_ALREADY_LINKED" => StatusCode::CONFLICT,
        "TOO_MANY_ATTEMPTS_TRY_LATER" => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::BAD_REQUEST,
    };
    ApiError::new(status, reason, "")
}

fn normalize_email(email: &str) -> Result<String, ApiError> {
    let email = email.trim().to_lowercase();
    if crate::validate::is_email(&email) {
        Ok(email)
    } else {
        Err(err("INVALID_EMAIL"))
    }
}

/// The password policy (DM_AUTH_PASSWORD_MIN_LENGTH / DM_AUTH_PASSWORD_REQUIRE). Every unmet
/// requirement is listed so the app can show them all at once.
pub fn check_password(state: &AppState, password: &str) -> Result<(), ApiError> {
    let p = &state.config.platform;
    let mut missing = Vec::new();
    if password.chars().count() < p.password_min_length {
        missing.push(format!("at least {} characters", p.password_min_length));
    }
    if password.len() > crate::validate::limits::PASSWORD_MAX {
        return Err(ApiError::bad_request("WEAK_PASSWORD", "Password is too long"));
    }
    for class in &p.password_require {
        let (ok, label) = match class.as_str() {
            "upper" => (password.chars().any(char::is_uppercase), "an uppercase letter"),
            "lower" => (password.chars().any(char::is_lowercase), "a lowercase letter"),
            "digit" => (password.chars().any(|c| c.is_ascii_digit()), "a digit"),
            "symbol" => (password.chars().any(|c| !c.is_alphanumeric() && !c.is_whitespace()), "a symbol"),
            _ => (true, ""),
        };
        if !ok {
            missing.push(label.to_string());
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ApiError::bad_request("WEAK_PASSWORD", format!("Password must contain {}", missing.join(", ")))
            .with_details(json!([{ "@type": "dm.PasswordPolicy", "requirements": missing }])))
    }
}

/// Custom claims: a JSON object of at most 1000 bytes without the reserved JWT/Firebase names.
pub fn validate_custom_claims(v: &mut crate::validate::V, field: &str, claims: &Map<String, Json>) {
    const RESERVED: &[&str] = &["acr", "amr", "at_hash", "aud", "auth_time", "azp", "cnf", "c_hash", "exp", "firebase", "iat", "iss", "jti", "nbf", "nonce", "sub", "typ", "user_id", "app_id", "uid"];
    let size = serde_json::to_string(claims).map(|s| s.len()).unwrap_or(usize::MAX);
    v.check(size <= crate::validate::limits::CUSTOM_CLAIMS_BYTES, field, format!("must be at most {} bytes of JSON (got {size})", crate::validate::limits::CUSTOM_CLAIMS_BYTES));
    for key in claims.keys() {
        v.check(!RESERVED.contains(&key.as_str()), format!("{field}.{key}"), "is a reserved claim name");
    }
    v.json_depth(field, &Json::Object(claims.clone()), 10);
}

/// Field rules for every accounts/* request (sizes, formats, allowed values).
fn validate_body(state: &AppState, action: &str, b: &Body) -> crate::validate::V {
    use crate::validate::{limits, V};
    let mut v = V::new();
    v.check(b.email.len() <= limits::EMAIL, "email", format!("must be at most {} characters", limits::EMAIL));
    v.check(b.password.len() <= limits::PASSWORD_MAX && b.new_password.len() <= limits::PASSWORD_MAX, "password", "is too long");
    if let Some(name) = &b.display_name {
        v.text("displayName", name, limits::DISPLAY_NAME);
    }
    if let Some(url) = b.photo_url.as_deref().filter(|u| !u.is_empty()) {
        v.url("photoUrl", url, false);
    }
    for (field, value) in [("token", &b.token), ("idToken", &b.id_token), ("accessToken", &b.access_token)] {
        v.check(value.len() <= limits::ID_TOKEN, field, "is too long");
    }
    v.text("nonce", &b.nonce, 256);
    v.opt_id("oobCode", &b.oob_code, limits::ID);
    if action == "signInWithIdp" {
        v.one_of("providerId", &b.provider_id, &["google.com", "apple.com"]);
    }
    if action == "sendOobCode" {
        v.one_of("requestType", &b.request_type, &["PASSWORD_RESET", "VERIFY_EMAIL", "EMAIL_SIGNIN"]);
    }
    if !b.continue_url.is_empty() {
        // Like Firebase's authorized domains: email links may only lead back to your own sites.
        if v.url("continueUrl", &b.continue_url, false) {
            let host = crate::validate::url_host(&b.continue_url).unwrap_or_default();
            let allowed = &state.config.platform.authorized_domains;
            v.check(
                allowed.iter().any(|d| host == *d || host.ends_with(&format!(".{d}"))),
                "continueUrl",
                format!("host '{host}' is not an authorized domain (DM_AUTH_AUTHORIZED_DOMAINS)"),
            );
        }
    }
    for attribute in &b.delete_attribute {
        v.one_of("deleteAttribute", attribute, &["DISPLAY_NAME", "PHOTO_URL"]);
    }
    v.max_count("deleteProvider", b.delete_provider.len(), 10);
    for provider in &b.delete_provider {
        v.len("deleteProvider", provider, 1, 64);
    }
    v
}

fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default().hash_password(password.as_bytes(), &salt).expect("argon2 hashing").to_string()
}

fn password_matches(hash: &str, password: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|parsed| Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
}

fn new_uid() -> String {
    // 28 chars, like Firebase uids.
    tokens::random_token().chars().filter(|c| c.is_ascii_alphanumeric()).take(28).collect()
}

// ── Token issuing ────────────────────────────────────────────────────────────

fn id_claims(state: &AppState, user: &Document, provider: &str) -> IdClaims {
    let now = tokens::now();
    let uid = user.get_str("_id").unwrap_or_default().to_string();
    let custom: Map<String, Json> = user
        .get_document("custom_claims")
        .ok()
        .and_then(|d| serde_json::to_value(d).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    let mut identities = Map::new();
    if let Ok(providers) = user.get_array("providers") {
        for p in providers.iter().filter_map(Bson::as_document) {
            let id = p.get_str("provider_id").unwrap_or_default();
            let raw = p.get_str("raw_id").unwrap_or_default();
            identities.entry(id.to_string()).or_insert_with(|| json!([])).as_array_mut().unwrap().push(json!(raw));
        }
    }
    IdClaims {
        typ: "id".into(),
        iss: format!("dm-auth/{}", state.config.platform.project_id),
        aud: state.config.platform.project_id.clone(),
        sub: uid.clone(),
        user_id: uid,
        auth_time: user.get_datetime("last_sign_in_at").map(|d| d.timestamp_millis() / 1000).unwrap_or(now),
        iat: now,
        exp: now + ID_TOKEN_TTL,
        email: user.get_str("email").ok().map(str::to_string),
        email_verified: user.get_bool("email_verified").unwrap_or(false),
        name: user.get_str("display_name").ok().map(str::to_string),
        picture: user.get_str("photo_url").ok().map(str::to_string),
        firebase: json!({ "sign_in_provider": provider, "identities": identities }),
        custom,
    }
}

async fn issue_tokens(state: &AppState, user: &Document, provider: &str) -> ApiResult<Json> {
    let uid = user.get_str("_id").unwrap_or_default();
    let refresh = tokens::random_token();
    state
        .db
        .coll(REFRESH_TOKENS)
        .insert_one(doc! { "_id": tokens::hash(&refresh), "uid": uid, "provider": provider, "created_at": DateTime::now(), "last_used": DateTime::now() })
        .await
        .map_err(ApiError::db)?;
    let id_token = state.tokens.sign(&id_claims(state, user, provider));
    Ok(json!({
        "kind": "dm#AuthResponse",
        "idToken": id_token,
        "refreshToken": refresh,
        "expiresIn": ID_TOKEN_TTL.to_string(),
        "localId": uid,
        "email": user.get_str("email").ok(),
        "displayName": user.get_str("display_name").ok(),
        "photoUrl": user.get_str("photo_url").ok(),
        "emailVerified": user.get_bool("email_verified").unwrap_or(false),
        "isAnonymous": user.get_bool("anonymous").unwrap_or(false),
        "providerId": provider,
    }))
}

/// Firebase-style user record for lookup / admin.
pub fn user_json(user: &Document) -> Json {
    let date_ms = |key: &str| user.get_datetime(key).ok().map(|d| d.timestamp_millis().to_string());
    let providers: Vec<Json> = user
        .get_array("providers")
        .map(|list| {
            list.iter()
                .filter_map(Bson::as_document)
                .map(|p| json!({ "providerId": p.get_str("provider_id").ok(), "rawId": p.get_str("raw_id").ok(), "email": p.get_str("email").ok(), "displayName": p.get_str("display_name").ok(), "photoUrl": p.get_str("photo_url").ok() }))
                .collect()
        })
        .unwrap_or_default();
    json!({
        "localId": user.get_str("_id").ok(),
        "email": user.get_str("email").ok(),
        "emailVerified": user.get_bool("email_verified").unwrap_or(false),
        "displayName": user.get_str("display_name").ok(),
        "photoUrl": user.get_str("photo_url").ok(),
        "disabled": user.get_bool("disabled").unwrap_or(false),
        "isAnonymous": user.get_bool("anonymous").unwrap_or(false),
        "passwordUpdatedAt": date_ms("password_updated_at"),
        "providerUserInfo": providers,
        "customAttributes": user.get_document("custom_claims").ok().map(|d| serde_json::to_value(d).unwrap_or(Json::Null).to_string()),
        "createdAt": date_ms("created_at"),
        "lastLoginAt": date_ms("last_sign_in_at"),
        "lastRefreshAt": user.get_datetime("last_refresh_at").ok().map(|d| d.try_to_rfc3339_string().unwrap_or_default()),
        "validSince": user.get_i64("tokens_valid_after").ok().map(|v| v.to_string()),
    })
}

// ── Verification (used by every product) ─────────────────────────────────────

/// Verifies an ID token: signature, expiry, user still exists / enabled, not revoked.
pub async fn verify_id_token(state: &AppState, token: &str) -> ApiResult<IdClaims> {
    let claims: IdClaims = state.tokens.verify(token, "id").map_err(|e| {
        if e.contains("ExpiredSignature") { err("TOKEN_EXPIRED") } else { ApiError::unauthenticated("INVALID_ID_TOKEN", e) }
    })?;
    // Revocation check is cached for a minute per uid to keep hot paths cheap.
    let cache = REVOCATION.get_or_init(Default::default);
    let cached = cache.lock().unwrap().get(&claims.sub).filter(|(at, _, _)| at.elapsed() < Duration::from_secs(60)).map(|(_, v, d)| (*v, *d));
    let (valid_after, disabled) = match cached {
        Some(v) => v,
        None => {
            let user = state.db.coll(USERS).find_one(doc! { "_id": &claims.sub }).await.map_err(ApiError::db)?.ok_or_else(|| err("USER_NOT_FOUND"))?;
            let v = (user.get_i64("tokens_valid_after").unwrap_or(0), user.get_bool("disabled").unwrap_or(false));
            cache.lock().unwrap().insert(claims.sub.clone(), (Instant::now(), v.0, v.1));
            v
        }
    };
    if disabled {
        return Err(err("USER_DISABLED"));
    }
    if claims.iat < valid_after {
        return Err(ApiError::unauthenticated("TOKEN_REVOKED", "the user's tokens were revoked; sign in again"));
    }
    Ok(claims)
}

static REVOCATION: OnceLock<Mutex<HashMap<String, (Instant, i64, bool)>>> = OnceLock::new();

/// Drops the cached revocation state (after disable / revoke / delete).
pub fn forget_cached(uid: &str) {
    if let Some(cache) = REVOCATION.get() {
        cache.lock().unwrap().remove(uid);
    }
}

async fn current_user(state: &AppState, id_token: &str) -> ApiResult<(IdClaims, Document)> {
    if id_token.is_empty() {
        return Err(err("MISSING_ID_TOKEN"));
    }
    let claims = verify_id_token(state, id_token).await?;
    let user = state.db.coll(USERS).find_one(doc! { "_id": &claims.sub }).await.map_err(ApiError::db)?.ok_or_else(|| err("USER_NOT_FOUND"))?;
    Ok((claims, user))
}

async fn reload(state: &AppState, uid: &str) -> ApiResult<Document> {
    state.db.coll(USERS).find_one(doc! { "_id": uid }).await.map_err(ApiError::db)?.ok_or_else(|| err("USER_NOT_FOUND"))
}

async fn touch_sign_in(state: &AppState, uid: &str) -> ApiResult<Document> {
    state
        .db
        .coll(USERS)
        .update_one(doc! { "_id": uid }, doc! { "$set": { "last_sign_in_at": DateTime::now(), "last_refresh_at": DateTime::now(), "failed_attempts": 0 } })
        .await
        .map_err(ApiError::db)?;
    reload(state, uid).await
}

// ── Routes ───────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Body {
    email: String,
    password: String,
    display_name: Option<String>,
    photo_url: Option<String>,
    token: String,
    id_token: String,
    provider_id: String,
    nonce: String,
    access_token: String,
    oob_code: String,
    new_password: String,
    request_type: String,
    continue_url: String,
    delete_attribute: Vec<String>,
    delete_provider: Vec<String>,
}

pub async fn accounts(State(state): State<AppState>, Path(action): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let caller = caller(&state, &headers, None, "auth").await?;
    let body: Body = parse_json(&body)?;
    validate_body(&state, &action, &body).into_api()?;
    match action.as_str() {
        "signUp" => sign_up(&state, &headers, body).await,
        "signInWithPassword" => sign_in_with_password(&state, body).await,
        "signInWithCustomToken" => sign_in_with_custom_token(&state, body).await,
        "signInWithIdp" => sign_in_with_idp(&state, caller.user.as_ref(), body).await,
        "signInWithEmailLink" => sign_in_with_email_link(&state, body).await,
        "lookup" => {
            let token = if body.id_token.is_empty() { bearer(&headers).unwrap_or_default().to_string() } else { body.id_token };
            let (_, user) = current_user(&state, &token).await?;
            ok(json!({ "kind": "dm#GetAccountInfoResponse", "users": [user_json(&user)] }))
        }
        "update" => update(&state, body).await,
        "delete" => {
            let token = if body.id_token.is_empty() { bearer(&headers).unwrap_or_default().to_string() } else { body.id_token };
            let (claims, _) = current_user(&state, &token).await?;
            delete_user(&state, &claims.sub).await?;
            ok(json!({ "kind": "dm#DeleteAccountResponse" }))
        }
        "sendOobCode" => send_oob_code(&state, body).await,
        "resetPassword" => reset_password(&state, body).await,
        other => Err(ApiError::not_found(format!("unknown accounts action '{other}'"))),
    }
}

pub async fn public_config(State(state): State<AppState>, headers: HeaderMap) -> ApiResult {
    super::require_api_key(&state, &headers, None)?;
    let p = &state.config.platform;
    ok(json!({
        "anonymous": p.auth_allow_anonymous,
        "emailPassword": p.auth_allow_signup,
        "customToken": true,
        "google": !p.auth_google_client_ids.is_empty(),
        "apple": !p.auth_apple_audiences.is_empty(),
        "emailLink": true,
    }))
}

async fn sign_up(state: &AppState, headers: &HeaderMap, body: Body) -> ApiResult {
    let p = &state.config.platform;
    let users = state.db.coll(USERS);
    // Signed-in anonymous user + email/password = link (upgrade the anonymous account).
    if let (Some(token), false) = (bearer(headers), body.email.is_empty()) {
        let (claims, _) = current_user(state, token).await?;
        let email = normalize_email(&body.email)?;
        check_password(state, &body.password)?;
        if users.find_one(doc! { "email": &email, "_id": { "$ne": &claims.sub } }).await.map_err(ApiError::db)?.is_some() {
            return Err(err("EMAIL_EXISTS"));
        }
        users
            .update_one(
                doc! { "_id": &claims.sub },
                doc! {
                    "$set": { "email": &email, "password_hash": hash_password(&body.password), "password_updated_at": DateTime::now(), "anonymous": false },
                    "$push": { "providers": { "provider_id": "password", "raw_id": &email, "email": &email } },
                },
            )
            .await
            .map_err(ApiError::db)?;
        let user = touch_sign_in(state, &claims.sub).await?;
        return ok(issue_tokens(state, &user, "password").await?);
    }

    let now = DateTime::now();
    let uid = new_uid();
    let mut user = doc! { "_id": &uid, "created_at": now, "last_sign_in_at": now, "last_refresh_at": now, "disabled": false, "email_verified": false, "providers": [], "custom_claims": {}, "tokens_valid_after": 0_i64 };
    let provider = if body.email.is_empty() && body.password.is_empty() {
        if !p.auth_allow_anonymous {
            return Err(err("OPERATION_NOT_ALLOWED"));
        }
        user.insert("anonymous", true);
        "anonymous"
    } else {
        if !p.auth_allow_signup {
            return Err(err("OPERATION_NOT_ALLOWED"));
        }
        let email = normalize_email(&body.email)?;
        check_password(state, &body.password)?;
        user.insert("anonymous", false);
        user.insert("email", &email);
        user.insert("password_hash", hash_password(&body.password));
        user.insert("password_updated_at", now);
        user.insert("providers", vec![Bson::Document(doc! { "provider_id": "password", "raw_id": &email, "email": &email })]);
        "password"
    };
    if let Some(name) = body.display_name.filter(|n| !n.is_empty()) {
        user.insert("display_name", name);
    }
    match users.insert_one(user.clone()).await {
        Ok(_) => {}
        Err(e) if crate::db::is_duplicate_key(&e) => return Err(err("EMAIL_EXISTS")),
        Err(e) => return Err(ApiError::db(e)),
    }
    ok(issue_tokens(state, &user, provider).await?)
}

async fn sign_in_with_password(state: &AppState, body: Body) -> ApiResult {
    let email = normalize_email(&body.email)?;
    let users = state.db.coll(USERS);
    let Some(user) = users.find_one(doc! { "email": &email }).await.map_err(ApiError::db)? else {
        // Same work as a real check, so response time doesn't reveal which emails exist.
        static DUMMY: OnceLock<String> = OnceLock::new();
        let _ = password_matches(DUMMY.get_or_init(|| hash_password("timing-equaliser")), &body.password);
        return Err(err("INVALID_LOGIN_CREDENTIALS"));
    };
    let uid = user.get_str("_id").unwrap_or_default().to_string();
    if let Ok(until) = user.get_datetime("locked_until") {
        if until.timestamp_millis() > chrono::Utc::now().timestamp_millis() {
            return Err(err("TOO_MANY_ATTEMPTS_TRY_LATER"));
        }
    }
    let hash = user.get_str("password_hash").unwrap_or("");
    if hash.is_empty() || !password_matches(hash, &body.password) {
        let failures = user.get_i32("failed_attempts").unwrap_or(0) + 1;
        let mut set = doc! { "failed_attempts": failures };
        if failures >= 10 {
            set.insert("locked_until", DateTime::from_millis(chrono::Utc::now().timestamp_millis() + 15 * 60_000));
            set.insert("failed_attempts", 0);
        }
        let _ = users.update_one(doc! { "_id": &uid }, doc! { "$set": set }).await;
        return Err(err("INVALID_LOGIN_CREDENTIALS"));
    }
    if user.get_bool("disabled").unwrap_or(false) {
        return Err(err("USER_DISABLED"));
    }
    let user = touch_sign_in(state, &uid).await?;
    let mut response = issue_tokens(state, &user, "password").await?;
    response["registered"] = json!(true);
    ok(response)
}

async fn sign_in_with_custom_token(state: &AppState, body: Body) -> ApiResult {
    let claims: CustomTokenClaims = state.tokens.verify(&body.token, "custom").map_err(|e| ApiError::unauthenticated("INVALID_CUSTOM_TOKEN", e))?;
    if claims.uid.is_empty() || claims.uid.len() > 128 {
        return Err(err("INVALID_CUSTOM_TOKEN"));
    }
    let users = state.db.coll(USERS);
    let now = DateTime::now();
    let claims_doc = bson::to_document(&claims.claims).unwrap_or_default();
    users
        .update_one(
            doc! { "_id": &claims.uid },
            doc! {
                "$setOnInsert": { "created_at": now, "disabled": false, "email_verified": false, "providers": [], "anonymous": false, "tokens_valid_after": 0_i64, "custom_claims": {} },
            },
        )
        .upsert(true)
        .await
        .map_err(ApiError::db)?;
    if !claims.claims.is_empty() {
        users.update_one(doc! { "_id": &claims.uid }, doc! { "$set": { "custom_claims": claims_doc } }).await.map_err(ApiError::db)?;
    }
    let user = touch_sign_in(state, &claims.uid).await?;
    if user.get_bool("disabled").unwrap_or(false) {
        return Err(err("USER_DISABLED"));
    }
    ok(issue_tokens(state, &user, "custom").await?)
}

/// Mints a custom token (used by the admin API / your backend through it).
pub fn mint_custom_token(state: &AppState, uid: &str, claims: Map<String, Json>) -> String {
    let now = tokens::now();
    state.tokens.sign(&CustomTokenClaims { typ: "custom".into(), uid: uid.into(), iat: now, exp: now + 3600, claims })
}

// ── Federated (Google / Apple) ───────────────────────────────────────────────

async fn jwks(state: &AppState, url: &str) -> ApiResult<jsonwebtoken::jwk::JwkSet> {
    static CACHE: OnceLock<Mutex<HashMap<String, (Instant, jsonwebtoken::jwk::JwkSet)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some((at, set)) = cache.lock().unwrap().get(url) {
        if at.elapsed() < Duration::from_secs(3600) {
            return Ok(set.clone());
        }
    }
    let set: jsonwebtoken::jwk::JwkSet = state
        .http
        .get(url)
        .send()
        .await
        .map_err(|e| ApiError::new(StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("{url}: {e}")))?
        .json()
        .await
        .map_err(|e| ApiError::new(StatusCode::BAD_GATEWAY, "UNAVAILABLE", format!("{url}: {e}")))?;
    cache.lock().unwrap().insert(url.to_string(), (Instant::now(), set.clone()));
    Ok(set)
}

pub struct IdpProfile {
    pub provider_id: String,
    pub raw_id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub name: Option<String>,
    pub picture: Option<String>,
}

async fn verify_idp_token(state: &AppState, provider: &str, id_token: &str, nonce: &str) -> ApiResult<IdpProfile> {
    let p = &state.config.platform;
    let (jwks_url, issuers, audiences): (&str, &[&str], &Vec<String>) = match provider {
        "google.com" => ("https://www.googleapis.com/oauth2/v3/certs", &["https://accounts.google.com", "accounts.google.com"], &p.auth_google_client_ids),
        "apple.com" => ("https://appleid.apple.com/auth/keys", &["https://appleid.apple.com"], &p.auth_apple_audiences),
        other => return Err(ApiError::bad_request("INVALID_PROVIDER_ID", format!("unsupported provider '{other}' (google.com, apple.com)"))),
    };
    if audiences.is_empty() {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "OPERATION_NOT_ALLOWED", format!("{provider} sign-in is not configured (DM_AUTH_{}_…)", if provider == "google.com" { "GOOGLE_CLIENT_IDS" } else { "APPLE_AUDIENCES" })));
    }
    let header = jsonwebtoken::decode_header(id_token).map_err(|_| err("INVALID_IDP_RESPONSE"))?;
    let set = jwks(state, jwks_url).await?;
    let jwk = header.kid.as_deref().and_then(|kid| set.find(kid)).ok_or_else(|| err("INVALID_IDP_RESPONSE"))?;
    let key = jsonwebtoken::DecodingKey::from_jwk(jwk).map_err(|_| err("INVALID_IDP_RESPONSE"))?;
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    validation.set_audience(audiences);
    validation.set_issuer(issuers);
    let data = jsonwebtoken::decode::<Json>(id_token, &key, &validation).map_err(|e| ApiError::unauthenticated("INVALID_IDP_RESPONSE", e.to_string()))?;
    let c = data.claims;
    if !nonce.is_empty() {
        // Apple puts SHA256(raw nonce) in the token; Google echoes the nonce as given.
        let hashed = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(nonce.as_bytes()));
        let token_nonce = c["nonce"].as_str().unwrap_or_default();
        if token_nonce != nonce && token_nonce != hashed {
            return Err(ApiError::unauthenticated("INVALID_IDP_RESPONSE", "nonce mismatch"));
        }
    }
    let verified = c["email_verified"].as_bool().or_else(|| c["email_verified"].as_str().map(|s| s == "true")).unwrap_or(false);
    Ok(IdpProfile {
        provider_id: provider.into(),
        raw_id: c["sub"].as_str().unwrap_or_default().into(),
        email: c["email"].as_str().map(|e| e.to_lowercase()),
        email_verified: verified,
        name: c["name"].as_str().map(str::to_string),
        picture: c["picture"].as_str().map(str::to_string),
    })
}

async fn sign_in_with_idp(state: &AppState, signed_in: Option<&IdClaims>, body: Body) -> ApiResult {
    let token = if body.id_token.is_empty() { body.access_token.clone() } else { body.id_token.clone() };
    if token.is_empty() {
        return Err(ApiError::bad_request("INVALID_IDP_RESPONSE", "idToken is required"));
    }
    let profile = verify_idp_token(state, &body.provider_id, &token, &body.nonce).await?;
    sign_in_with_profile(state, signed_in, profile).await
}

/// Shared by signInWithIdp and tests.
pub async fn sign_in_with_profile(state: &AppState, signed_in: Option<&IdClaims>, profile: IdpProfile) -> ApiResult {
    let users = state.db.coll(USERS);
    let identity = doc! { "provider_id": &profile.provider_id, "raw_id": &profile.raw_id, "email": profile.email.as_deref(), "display_name": profile.name.as_deref(), "photo_url": profile.picture.as_deref() };
    let existing = users
        .find_one(doc! { "providers": { "$elemMatch": { "provider_id": &profile.provider_id, "raw_id": &profile.raw_id } } })
        .await
        .map_err(ApiError::db)?;

    let uid = match (existing, signed_in) {
        (Some(user), Some(current)) if user.get_str("_id").unwrap_or_default() != current.sub => return Err(err("FEDERATED_USER_ID_ALREADY_LINKED")),
        (Some(user), _) => user.get_str("_id").unwrap_or_default().to_string(),
        (None, Some(current)) => {
            // Link to the signed-in user (e.g. upgrade an anonymous account).
            let mut set = doc! { "anonymous": false };
            if let Some(email) = &profile.email {
                set.insert("email", email);
                set.insert("email_verified", profile.email_verified);
            }
            users
                .update_one(doc! { "_id": &current.sub }, doc! { "$set": set, "$push": { "providers": identity.clone() } })
                .await
                .map_err(|e| if crate::db::is_duplicate_key(&e) { err("EMAIL_EXISTS") } else { ApiError::db(e) })?;
            current.sub.clone()
        }
        (None, None) => {
            // Same verified email as an existing account → attach the provider to it.
            let by_email = match (&profile.email, profile.email_verified) {
                (Some(email), true) => users.find_one(doc! { "email": email }).await.map_err(ApiError::db)?,
                _ => None,
            };
            match by_email {
                Some(user) => {
                    let uid = user.get_str("_id").unwrap_or_default().to_string();
                    users.update_one(doc! { "_id": &uid }, doc! { "$push": { "providers": identity.clone() }, "$set": { "email_verified": true } }).await.map_err(ApiError::db)?;
                    uid
                }
                None => {
                    let uid = new_uid();
                    let now = DateTime::now();
                    let mut user = doc! { "_id": &uid, "created_at": now, "disabled": false, "anonymous": false, "email_verified": profile.email_verified, "providers": [identity.clone()], "custom_claims": {}, "tokens_valid_after": 0_i64 };
                    if let Some(email) = &profile.email {
                        user.insert("email", email);
                    }
                    if let Some(name) = &profile.name {
                        user.insert("display_name", name);
                    }
                    if let Some(picture) = &profile.picture {
                        user.insert("photo_url", picture);
                    }
                    users.insert_one(user).await.map_err(|e| if crate::db::is_duplicate_key(&e) { err("EMAIL_EXISTS") } else { ApiError::db(e) })?;
                    uid
                }
            }
        }
    };
    let user = touch_sign_in(state, &uid).await?;
    if user.get_bool("disabled").unwrap_or(false) {
        return Err(err("USER_DISABLED"));
    }
    let mut response = issue_tokens(state, &user, &profile.provider_id).await?;
    response["federatedId"] = json!(profile.raw_id);
    ok(response)
}

// ── Account management ───────────────────────────────────────────────────────

async fn update(state: &AppState, body: Body) -> ApiResult {
    let users = state.db.coll(USERS);
    // Email verification through an emailed code.
    if body.id_token.is_empty() && !body.oob_code.is_empty() {
        let code = take_oob(state, &body.oob_code, "VERIFY_EMAIL").await?;
        let uid = code.get_str("uid").unwrap_or_default();
        users.update_one(doc! { "_id": uid }, doc! { "$set": { "email_verified": true } }).await.map_err(ApiError::db)?;
        return ok(json!({ "email": code.get_str("email").ok(), "emailVerified": true, "localId": uid }));
    }
    let (claims, user) = current_user(state, &body.id_token).await?;
    let mut set = Document::new();
    let mut unset = Document::new();
    let mut reissue = false;
    if let Some(name) = body.display_name {
        set.insert("display_name", name);
    }
    if let Some(url) = body.photo_url {
        set.insert("photo_url", url);
    }
    for attribute in &body.delete_attribute {
        match attribute.as_str() {
            "DISPLAY_NAME" => unset.insert("display_name", ""),
            "PHOTO_URL" => unset.insert("photo_url", ""),
            other => return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("cannot delete attribute {other}"))),
        };
    }
    let recent = claims.auth_time > tokens::now() - 5 * 60;
    if !body.email.is_empty() {
        if !recent {
            return Err(err("CREDENTIAL_TOO_OLD_LOGIN_AGAIN"));
        }
        let email = normalize_email(&body.email)?;
        set.insert("email", &email);
        set.insert("email_verified", false);
        reissue = true;
    }
    if !body.password.is_empty() {
        if !recent && !user.get_bool("anonymous").unwrap_or(false) {
            return Err(err("CREDENTIAL_TOO_OLD_LOGIN_AGAIN"));
        }
        check_password(state, &body.password)?;
        set.insert("password_hash", hash_password(&body.password));
        set.insert("password_updated_at", DateTime::now());
        set.insert("anonymous", false);
        reissue = true;
    }
    let mut update = doc! {};
    if !set.is_empty() {
        update.insert("$set", set);
    }
    if !unset.is_empty() {
        update.insert("$unset", unset);
    }
    if !body.delete_provider.is_empty() {
        update.insert("$pull", doc! { "providers": { "provider_id": { "$in": &body.delete_provider } } });
    }
    if !update.is_empty() {
        users.update_one(doc! { "_id": &claims.sub }, update).await.map_err(|e| if crate::db::is_duplicate_key(&e) { err("EMAIL_EXISTS") } else { ApiError::db(e) })?;
    }
    let user = reload(state, &claims.sub).await?;
    let mut response = user_json(&user);
    if reissue {
        // Email/password changes revoke other sessions, like Firebase.
        if !body.password.is_empty() {
            let _ = state.db.coll(REFRESH_TOKENS).delete_many(doc! { "uid": &claims.sub }).await;
        }
        let tokens = issue_tokens(state, &user, claims.provider()).await?;
        response["idToken"] = tokens["idToken"].clone();
        response["refreshToken"] = tokens["refreshToken"].clone();
        response["expiresIn"] = tokens["expiresIn"].clone();
    }
    ok(response)
}

pub async fn delete_user(state: &AppState, uid: &str) -> ApiResult<()> {
    state.db.coll(USERS).delete_one(doc! { "_id": uid }).await.map_err(ApiError::db)?;
    let _ = state.db.coll(REFRESH_TOKENS).delete_many(doc! { "uid": uid }).await;
    let _ = state.db.coll(OOB_CODES).delete_many(doc! { "uid": uid }).await;
    forget_cached(uid);
    Ok(())
}

// ── Email action codes ───────────────────────────────────────────────────────

pub async fn create_oob(state: &AppState, kind: &str, uid: Option<&str>, email: &str, continue_url: &str) -> ApiResult<(String, String)> {
    let code = tokens::random_token();
    let ttl_ms = if kind == "PASSWORD_RESET" { 3_600_000 } else { 3 * 86_400_000 };
    state
        .db
        .coll(OOB_CODES)
        .insert_one(doc! { "_id": &code, "type": kind, "uid": uid, "email": email, "continue_url": continue_url, "expires_at": DateTime::from_millis(chrono::Utc::now().timestamp_millis() + ttl_ms) })
        .await
        .map_err(ApiError::db)?;
    let mode = match kind {
        "PASSWORD_RESET" => "resetPassword",
        "VERIFY_EMAIL" => "verifyEmail",
        _ => "signIn",
    };
    let base = state.config.platform.auth_action_url.clone().unwrap_or_else(|| "dm-auth://action".into());
    let sep = if base.contains('?') { '&' } else { '?' };
    let link = format!(
        "{base}{sep}mode={mode}&oobCode={code}{}",
        if continue_url.is_empty() { String::new() } else { format!("&continueUrl={}", percent_encoding::utf8_percent_encode(continue_url, percent_encoding::NON_ALPHANUMERIC)) }
    );
    Ok((code, link))
}

async fn take_oob(state: &AppState, code: &str, kind: &str) -> ApiResult<Document> {
    state
        .db
        .coll(OOB_CODES)
        .find_one_and_delete(doc! { "_id": code, "type": kind, "expires_at": { "$gt": DateTime::now() } })
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| ApiError::bad_request("INVALID_OOB_CODE", "the action code is invalid, expired or already used"))
}

async fn deliver_email(state: &AppState, kind: &str, email: &str, link: &str) {
    match &state.config.platform.auth_email_webhook {
        Some(url) => {
            if let Err(e) = state.http.post(url).json(&json!({ "type": kind, "email": email, "link": link })).send().await {
                eprintln!("[Auth] email webhook failed: {e}");
            }
        }
        None => println!("[Auth] {kind} link for {email} (set DM_AUTH_EMAIL_WEBHOOK to send it): {link}"),
    }
}

async fn send_oob_code(state: &AppState, body: Body) -> ApiResult {
    let users = state.db.coll(USERS);
    match body.request_type.as_str() {
        "PASSWORD_RESET" => {
            let email = normalize_email(&body.email)?;
            // Respond the same whether or not the account exists (no enumeration).
            if let Some(user) = users.find_one(doc! { "email": &email }).await.map_err(ApiError::db)? {
                let (_, link) = create_oob(state, "PASSWORD_RESET", user.get_str("_id").ok(), &email, &body.continue_url).await?;
                deliver_email(state, "PASSWORD_RESET", &email, &link).await;
            }
            ok(json!({ "kind": "dm#GetOobConfirmationCodeResponse", "email": email }))
        }
        "VERIFY_EMAIL" => {
            let (claims, user) = current_user(state, &body.id_token).await?;
            let email = user.get_str("email").map_err(|_| ApiError::bad_request("MISSING_EMAIL", "the user has no email"))?.to_string();
            let (_, link) = create_oob(state, "VERIFY_EMAIL", Some(&claims.sub), &email, &body.continue_url).await?;
            deliver_email(state, "VERIFY_EMAIL", &email, &link).await;
            ok(json!({ "kind": "dm#GetOobConfirmationCodeResponse", "email": email }))
        }
        "EMAIL_SIGNIN" => {
            let email = normalize_email(&body.email)?;
            let (_, link) = create_oob(state, "EMAIL_SIGNIN", None, &email, &body.continue_url).await?;
            deliver_email(state, "EMAIL_SIGNIN", &email, &link).await;
            ok(json!({ "kind": "dm#GetOobConfirmationCodeResponse", "email": email }))
        }
        other => Err(ApiError::bad_request("INVALID_REQ_TYPE", format!("unsupported requestType '{other}'"))),
    }
}

async fn reset_password(state: &AppState, body: Body) -> ApiResult {
    if body.new_password.is_empty() {
        // Check the code without using it.
        let code = state
            .db
            .coll(OOB_CODES)
            .find_one(doc! { "_id": &body.oob_code, "type": "PASSWORD_RESET", "expires_at": { "$gt": DateTime::now() } })
            .await
            .map_err(ApiError::db)?
            .ok_or_else(|| ApiError::bad_request("INVALID_OOB_CODE", "the action code is invalid, expired or already used"))?;
        return ok(json!({ "email": code.get_str("email").ok(), "requestType": "PASSWORD_RESET" }));
    }
    check_password(state, &body.new_password)?;
    let code = take_oob(state, &body.oob_code, "PASSWORD_RESET").await?;
    let uid = code.get_str("uid").unwrap_or_default();
    state
        .db
        .coll(USERS)
        .update_one(doc! { "_id": uid }, doc! { "$set": { "password_hash": hash_password(&body.new_password), "password_updated_at": DateTime::now(), "tokens_valid_after": tokens::now(), "failed_attempts": 0 }, "$unset": { "locked_until": "" } })
        .await
        .map_err(ApiError::db)?;
    let _ = state.db.coll(REFRESH_TOKENS).delete_many(doc! { "uid": uid }).await;
    forget_cached(uid);
    ok(json!({ "email": code.get_str("email").ok(), "requestType": "PASSWORD_RESET" }))
}

async fn sign_in_with_email_link(state: &AppState, body: Body) -> ApiResult {
    let email = normalize_email(&body.email)?;
    let code = take_oob(state, &body.oob_code, "EMAIL_SIGNIN").await?;
    if code.get_str("email").unwrap_or_default() != email {
        return Err(ApiError::bad_request("INVALID_EMAIL", "the link was sent to a different email"));
    }
    let users = state.db.coll(USERS);
    let uid = match users.find_one(doc! { "email": &email }).await.map_err(ApiError::db)? {
        Some(user) => {
            let uid = user.get_str("_id").unwrap_or_default().to_string();
            users.update_one(doc! { "_id": &uid }, doc! { "$set": { "email_verified": true } }).await.map_err(ApiError::db)?;
            uid
        }
        None => {
            let uid = new_uid();
            let now = DateTime::now();
            users
                .insert_one(doc! { "_id": &uid, "email": &email, "email_verified": true, "created_at": now, "disabled": false, "anonymous": false, "providers": [{ "provider_id": "emailLink", "raw_id": &email, "email": &email }], "custom_claims": {}, "tokens_valid_after": 0_i64 })
                .await
                .map_err(ApiError::db)?;
            uid
        }
    };
    let user = touch_sign_in(state, &uid).await?;
    ok(issue_tokens(state, &user, "emailLink").await?)
}

// ── Refresh ──────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
struct RefreshBody {
    grant_type: String,
    refresh_token: String,
}

pub async fn refresh(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    caller(&state, &headers, None, "auth").await?;
    // JSON or application/x-www-form-urlencoded, like securetoken.googleapis.com.
    let body: RefreshBody = if body.first() == Some(&b'{') {
        parse_json(&body)?
    } else {
        let mut parsed = RefreshBody::default();
        for (k, v) in url_form(&body) {
            match k.as_str() {
                "grant_type" => parsed.grant_type = v,
                "refresh_token" => parsed.refresh_token = v,
                _ => {}
            }
        }
        parsed
    };
    if body.grant_type != "refresh_token" {
        return Err(ApiError::bad_request("INVALID_GRANT_TYPE", "grant_type must be refresh_token"));
    }
    let tokens_coll = state.db.coll(REFRESH_TOKENS);
    let record = tokens_coll
        .find_one_and_update(doc! { "_id": tokens::hash(&body.refresh_token) }, doc! { "$set": { "last_used": DateTime::now() } })
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| err("INVALID_REFRESH_TOKEN"))?;
    let uid = record.get_str("uid").unwrap_or_default().to_string();
    let provider = record.get_str("provider").unwrap_or("password").to_string();
    let user = state.db.coll(USERS).find_one(doc! { "_id": &uid }).await.map_err(ApiError::db)?.ok_or_else(|| err("USER_NOT_FOUND"))?;
    if user.get_bool("disabled").unwrap_or(false) {
        return Err(err("USER_DISABLED"));
    }
    let _ = state.db.coll(USERS).update_one(doc! { "_id": &uid }, doc! { "$set": { "last_refresh_at": DateTime::now() } }).await;
    let id_token = state.tokens.sign(&id_claims(&state, &user, &provider));
    ok(json!({
        "access_token": id_token, "id_token": id_token, "expires_in": ID_TOKEN_TTL.to_string(), "token_type": "Bearer",
        "refresh_token": body.refresh_token, "user_id": uid, "project_id": state.config.platform.project_id,
    }))
}

fn url_form(body: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(body)
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| {
            let decode = |s: &str| percent_encoding::percent_decode_str(&s.replace('+', " ")).decode_utf8_lossy().to_string();
            (decode(k), decode(v))
        })
        .collect()
}
