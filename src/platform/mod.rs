//! Platform services — the Firebase-style products beyond the original five, as JSON over HTTPS
//! (plus one WebSocket for live database queries). Every route lives under /v1:
//!
//!   Installations   POST   /v1/installations                       register (or re-register) an install
//!                   POST   /v1/installations/:fid/auth-token       new auth token (FIS_v2 <refresh token>)
//!                   DELETE /v1/installations/:fid                  delete (GDPR)
//!   App Check       POST   /v1/appcheck/exchange                   attestation → App Check token
//!                   POST   /v1/appcheck/verify                     for your own backends
//!   Auth            POST   /v1/accounts/{signUp,signInWithPassword,signInWithCustomToken,signInWithIdp,
//!                                        lookup,update,delete,sendOobCode,resetPassword}
//!                   POST   /v1/token                               refresh_token → new ID token
//!   Storage         see storage.rs   (/v1/storage/b/:bucket/o…)
//!   Functions       POST   /v1/functions/:name                     callable functions
//!   Database        see database.rs  (/v1/db/…, WebSocket /v1/db/listen)
//!   JSON RPC        POST   /v1/rpc/:service/:method                every dm.sdk.v1 RPC as proto3 JSON
//!
//! Request headers (all optional unless the server is configured to require them):
//!   x-api-key            API key (DM_API_KEYS), checked against x-dm-app-id
//!   x-dm-app-id          the calling app
//!   x-dm-appcheck        App Check token (required for products in DM_APPCHECK_ENFORCE)
//!   authorization        "Bearer <ID token>" for the signed-in user
//!   x-dm-installation-auth   installation auth token (FIS_v2), used by Functions / Messaging

pub mod admin;
pub mod appcheck;
pub mod auth;
pub mod database;
pub mod functions;
pub mod installations;
pub mod rpc_json;
pub mod rules;
pub mod storage;
pub mod tokens;

use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post};
use axum::{Json as AxumJson, Router};
use serde_json::{json, Value as Json};

use crate::dm::AppState;
use tokens::{AppCheckClaims, IdClaims};

pub fn router(state: AppState) -> Router<AppState> {
    let storage_limit = state.config.platform.storage_max_bytes + 1024 * 1024;
    Router::new()
        .route("/installations", post(installations::register))
        .route("/installations/:fid/auth-token", post(installations::generate_auth_token))
        .route("/installations/:fid", delete(installations::delete))
        .route("/appcheck/exchange", post(appcheck::exchange))
        .route("/appcheck/verify", post(appcheck::verify))
        .route("/appcheck/challenge", post(appcheck::challenge))
        .route("/accounts/:action", post(auth::accounts))
        .route("/token", post(auth::refresh))
        .route("/auth/config", get(auth::public_config))
        .route("/functions/:name", post(functions::call))
        .route("/rpc/:service/:method", post(rpc_json::call))
        .route("/messaging/webpush-key", get(webpush_key))
        .merge(storage::router().layer(DefaultBodyLimit::max(storage_limit)))
        .merge(database::router())
        .fallback(any(|| async { ApiError::new(StatusCode::NOT_FOUND, "NOT_FOUND", "no such endpoint") }))
}

/// Collections and indexes for the platform services (called from Db::ensure_schema).
pub async fn ensure_schema(db: &crate::db::Db) -> Result<(), crate::BoxError> {
    use crate::db::index;
    use mongodb::bson::doc;
    index(&db.coll(installations::COLLECTION), doc! { "app_id": 1, "last_seen": -1 }, "app_last_seen", false).await?;
    ttl_on(db, appcheck::CHALLENGES, "expires_at").await;
    ttl_on(db, appcheck::USED_TOKENS, "expires_at").await;
    // Unique only among users that have an email (anonymous / custom / phone users don't).
    let options = mongodb::options::IndexOptions::builder()
        .name("email_unique".to_string())
        .unique(true)
        .partial_filter_expression(doc! { "email": { "$type": "string" } })
        .build();
    db.coll(auth::USERS).create_index(mongodb::IndexModel::builder().keys(doc! { "email": 1 }).options(options).build()).await?;
    index(&db.coll(auth::USERS), doc! { "providers.provider_id": 1, "providers.raw_id": 1 }, "provider_identity", false).await?;
    index(&db.coll(auth::USERS), doc! { "created_at": -1 }, "created", false).await?;
    index(&db.coll(auth::REFRESH_TOKENS), doc! { "uid": 1 }, "uid", false).await?;
    ttl_on(db, auth::OOB_CODES, "expires_at").await;
    storage::ensure_schema(db).await?;
    database::ensure_schema(db).await?;
    Ok(())
}

/// TTL index that deletes documents when `field` (a date) passes.
async fn ttl_on(db: &crate::db::Db, collection: &str, field: &str) {
    use mongodb::bson::doc;
    let options = mongodb::options::IndexOptions::builder().name(format!("ttl_{field}")).expire_after(std::time::Duration::from_secs(0)).build();
    let model = mongodb::IndexModel::builder().keys(doc! { field: 1 }).options(options).build();
    if let Err(err) = db.coll(collection).create_index(model).await {
        eprintln!("⚠️  TTL index on {collection}.{field}: {err}");
    }
}

/// The VAPID public key browsers subscribe with (PushManager.subscribe applicationServerKey).
async fn webpush_key(axum::extract::State(state): axum::extract::State<AppState>, headers: HeaderMap) -> ApiResult {
    require_api_key(&state, &headers, None)?;
    match &state.webpush {
        Some(wp) => ok(json!({ "publicKey": wp.public_key() })),
        None => Err(ApiError::new(StatusCode::NOT_IMPLEMENTED, "WEBPUSH_NOT_CONFIGURED", "set DM_WEBPUSH_VAPID_PRIVATE_KEY (generate with POST /admin/messaging/webpush/keys)")),
    }
}

// ── Errors (Google API error format) ─────────────────────────────────────────

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    /// Machine-readable reason, e.g. "EMAIL_EXISTS", "PERMISSION_DENIED".
    pub reason: String,
    pub message: String,
    /// Structured details, e.g. BadRequest field violations from crate::validate.
    pub details: Option<Json>,
}

impl ApiError {
    pub fn new(status: StatusCode, reason: &str, message: impl Into<String>) -> Self {
        Self { status, reason: reason.to_string(), message: message.into(), details: None }
    }
    pub fn with_details(mut self, details: Json) -> Self {
        self.details = Some(details);
        self
    }
    pub fn bad_request(reason: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, reason, message)
    }
    pub fn unauthenticated(reason: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, reason, message)
    }
    pub fn denied(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "PERMISSION_DENIED", message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }
    pub fn db(err: impl std::fmt::Display) -> Self {
        eprintln!("[platform] database error: {err}");
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE", "storage temporarily unavailable")
    }
    fn status_name(&self) -> &'static str {
        match self.status {
            StatusCode::BAD_REQUEST => "INVALID_ARGUMENT",
            StatusCode::UNAUTHORIZED => "UNAUTHENTICATED",
            StatusCode::FORBIDDEN => "PERMISSION_DENIED",
            StatusCode::NOT_FOUND => "NOT_FOUND",
            StatusCode::CONFLICT => "ALREADY_EXISTS",
            StatusCode::PRECONDITION_FAILED => "FAILED_PRECONDITION",
            StatusCode::PAYLOAD_TOO_LARGE => "RESOURCE_EXHAUSTED",
            StatusCode::TOO_MANY_REQUESTS => "RESOURCE_EXHAUSTED",
            StatusCode::SERVICE_UNAVAILABLE => "UNAVAILABLE",
            StatusCode::GATEWAY_TIMEOUT => "DEADLINE_EXCEEDED",
            _ => "INTERNAL",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": {
            "code": self.status.as_u16(),
            "status": self.status_name(),
            "message": if self.message.is_empty() { self.reason.clone() } else { format!("{}: {}", self.reason, self.message) },
            "reason": self.reason,
        }});
        if let Some(details) = self.details {
            body["error"]["details"] = details;
        }
        (self.status, AxumJson(body)).into_response()
    }
}

pub type ApiResult<T = Response> = Result<T, ApiError>;

pub fn ok(value: Json) -> ApiResult {
    Ok(AxumJson(value).into_response())
}

// ── Request context ──────────────────────────────────────────────────────────

/// Who is calling: app (API key), App Check verdict and signed-in user, from the headers.
#[derive(Debug, Clone, Default)]
pub struct Caller {
    pub app_id: String,
    pub appcheck: Option<AppCheckClaims>,
    pub user: Option<IdClaims>,
    pub installation_id: Option<String>,
}

impl Caller {
    pub fn uid(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.sub.as_str())
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty())
}

pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    header(headers, "authorization").and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
}

/// API key check for endpoints that don't resolve a full caller: the key (x-api-key) must be valid
/// for `app_id` (from the request body, else the x-dm-app-id header).
pub fn require_api_key(state: &AppState, headers: &HeaderMap, app_id: Option<&str>) -> ApiResult<()> {
    if !state.config.auth_enabled() {
        return Ok(());
    }
    let app_id = app_id.filter(|a| !a.is_empty()).or_else(|| header(headers, "x-dm-app-id")).unwrap_or("");
    match header(headers, "x-api-key") {
        Some(key) if state.config.key_allows(key, app_id) => Ok(()),
        Some(_) => Err(ApiError::unauthenticated("API_KEY_INVALID", "API key not valid for this app (check x-dm-app-id)")),
        None => Err(ApiError::unauthenticated("API_KEY_MISSING", "x-api-key header is required")),
    }
}

/// Resolves the caller for `product` ("auth", "storage", "functions", "database", "rpc"):
/// checks the API key against the app id, App Check (when enforced for the product) and the
/// optional ID token. An invalid ID token is an error; a missing one just means "signed out".
pub async fn caller(state: &AppState, headers: &HeaderMap, query_key: Option<&str>, product: &str) -> ApiResult<Caller> {
    let platform = &state.config.platform;
    let app_id = header(headers, "x-dm-app-id").unwrap_or("").to_string();
    if state.config.auth_enabled() {
        let key = header(headers, "x-api-key").or(query_key);
        match key {
            Some(key) if state.config.key_allows(key, &app_id) => {}
            Some(_) => return Err(ApiError::unauthenticated("API_KEY_INVALID", "API key not valid for this app (check x-dm-app-id)")),
            None => return Err(ApiError::unauthenticated("API_KEY_MISSING", "x-api-key header is required")),
        }
    }

    let appcheck = match header(headers, "x-dm-appcheck") {
        Some(token) => match appcheck::verify_token(state, token).await {
            Ok(claims) if app_id.is_empty() || claims.sub == app_id => Some(claims),
            Ok(_) => return Err(ApiError::unauthenticated("APP_CHECK_INVALID", "App Check token was issued to another app")),
            Err(e) if platform.enforces(product) => return Err(ApiError::unauthenticated("APP_CHECK_INVALID", e)),
            Err(_) => None,
        },
        None if platform.enforces(product) => {
            return Err(ApiError::unauthenticated("APP_CHECK_MISSING", format!("App Check is enforced for {product}")))
        }
        None => None,
    };

    let user = match bearer(headers) {
        Some(token) => Some(auth::verify_id_token(state, token).await?),
        None => None,
    };

    let installation_id = header(headers, "x-dm-installation-auth").and_then(|t| {
        state
            .tokens
            .verify::<tokens::FisClaims>(t.strip_prefix("FIS_v2 ").unwrap_or(t), "fis")
            .ok()
            .map(|c| c.sub)
    });

    Ok(Caller { app_id, appcheck, user, installation_id })
}

/// The JSON body, with a readable error when it isn't JSON.
pub fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    let body: &[u8] = if body.is_empty() { b"{}" } else { body };
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request("INVALID_ARGUMENT", format!("invalid JSON body: {e}")))
}
