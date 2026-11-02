//! Admin API for the platform services (mounted under /admin with the same x-admin-key check).
//! Used by the console and by your own backend (the "Admin SDK" role).
//!
//! GET    /admin/platform/overview
//! Auth   GET  /admin/auth/users?q=&limit=&after=           POST /admin/auth/users
//!        GET|PATCH|DELETE /admin/auth/users/:uid          POST /admin/auth/users/:uid/revoke
//!        POST /admin/auth/custom-token {uid, claims?}      POST /admin/auth/email-link {type, email}
//!        POST /admin/auth/verify-id-token {idToken}
//! App Check  GET|POST /admin/appcheck/debug-tokens        DELETE /admin/appcheck/debug-tokens/:token
//!            POST /admin/appcheck/:app_id/token {ttl_seconds?}   (custom provider: your backend mints)
//! Installations  GET /admin/installations?app_id=&limit=
//! Storage    GET /admin/storage/buckets                   GET /admin/storage/b/:bucket?prefix=
//!            GET|DELETE /admin/storage/b/:bucket/o/:object
//! Database   GET /admin/db/collections?parent=            GET /admin/db/docs?collection=&limit=&after=
//!            GET|PUT|DELETE /admin/db/doc/*path
//! Rules      GET|PUT /admin/rules/:kind (storage|database)   POST /admin/rules/:kind/test
//! Functions  GET|POST /admin/functions    PUT|DELETE /admin/functions/:name    GET /admin/functions/:name/calls

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json as AxumJson, Router};
use futures_util::TryStreamExt;
use mongodb::bson::{self, doc, Bson, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json};

use super::rules::{self, Op};
use super::{auth, database, functions, installations, storage, tokens, ApiError, Caller};
use crate::dm::AppState;

type AdminResult = Result<Response, Response>;

fn fail(status: StatusCode, message: impl Into<String>) -> Response {
    (status, AxumJson(json!({ "error": message.into() }))).into_response()
}
fn from_api(e: ApiError) -> Response {
    fail(e.status, if e.message.is_empty() { e.reason } else { format!("{}: {}", e.reason, e.message) })
}
fn db_fail(e: impl std::fmt::Display) -> Response {
    eprintln!("[Admin] database error: {e}");
    fail(StatusCode::SERVICE_UNAVAILABLE, "database error")
}
fn done(value: Json) -> AdminResult {
    Ok(AxumJson(value).into_response())
}
/// Validation failures in the admin error shape: {"error": "<summary>", "violations": [{field, description}]}.
fn checked(v: crate::validate::V) -> Result<(), Response> {
    if v.is_ok() {
        return Ok(());
    }
    let violations: Vec<Json> = v.violations.iter().map(|x| json!({ "field": x.field, "description": x.description })).collect();
    Err((StatusCode::BAD_REQUEST, AxumJson(json!({ "error": v.summary(), "violations": violations }))).into_response())
}

/// Admin-side user fields: the same rules the client-facing auth API applies.
fn validate_user(state: &AppState, b: &UserBody) -> crate::validate::V {
    use crate::validate::{limits, V};
    let mut v = V::new();
    if let Some(uid) = b.uid.as_deref().filter(|u| !u.is_empty()) {
        v.len("uid", uid, 1, limits::UID);
        v.check(!uid.contains('/'), "uid", "must not contain '/'");
    }
    if let Some(email) = b.email.as_deref().filter(|e| !e.is_empty()) {
        v.email("email", &email.trim().to_lowercase());
    }
    if let Some(password) = b.password.as_deref() {
        if let Err(e) = auth::check_password(state, password) {
            v.fail("password", e.message);
        }
    }
    if let Some(name) = &b.display_name {
        v.text("displayName", name, limits::DISPLAY_NAME);
    }
    if let Some(url) = b.photo_url.as_deref().filter(|u| !u.is_empty()) {
        v.url("photoUrl", url, false);
    }
    if let Some(claims) = &b.custom_claims {
        auth::validate_custom_claims(&mut v, "customClaims", claims);
    }
    v
}

fn body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Response> {
    let bytes: &[u8] = if bytes.is_empty() { b"{}" } else { bytes };
    serde_json::from_slice(bytes).map_err(|e| fail(StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")))
}

fn admin_caller() -> Caller {
    Caller { app_id: "__admin__".into(), ..Default::default() }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/platform/overview", get(overview))
        .route("/auth/users", get(list_users).post(create_user))
        .route("/auth/users/:uid", get(get_user).patch(update_user).delete(delete_user))
        .route("/auth/users/:uid/revoke", post(revoke))
        .route("/auth/custom-token", post(custom_token))
        .route("/auth/email-link", post(email_link))
        .route("/auth/verify-id-token", post(verify_id_token))
        .route("/appcheck/debug-tokens", get(list_debug_tokens).post(add_debug_token))
        .route("/appcheck/debug-tokens/:token", axum::routing::delete(remove_debug_token))
        .route("/appcheck/:app_id/token", post(mint_appcheck))
        .route("/installations", get(list_installations))
        .route("/storage/buckets", get(buckets))
        .route("/storage/b/:bucket", get(browse))
        .route("/storage/b/:bucket/o/:object", get(object_info).delete(delete_object))
        .route("/db/collections", get(collections))
        .route("/db/docs", get(documents))
        .route("/db/doc/*path", get(get_doc).put(put_doc).delete(delete_doc))
        .route("/rules/:kind", get(get_rules).put(put_rules))
        .route("/rules/:kind/test", post(test_rules))
        .route("/functions", get(list_functions).post(upsert_function))
        .route("/functions/:name", axum::routing::put(upsert_named_function).delete(delete_function))
        .route("/functions/:name/calls", get(function_calls))
}

async fn count(state: &AppState, collection: &str, filter: Document) -> i64 {
    state.db.coll(collection).count_documents(filter).await.unwrap_or(0) as i64
}

async fn overview(State(state): State<AppState>) -> AdminResult {
    let storage_bytes: i64 = {
        let mut cursor = state.db.coll(storage::OBJECTS).aggregate(vec![doc! { "$group": { "_id": Bson::Null, "bytes": { "$sum": "$size" } } }]).await.map_err(db_fail)?;
        cursor.try_next().await.ok().flatten().and_then(|d| d.get_i64("bytes").ok().or_else(|| d.get_i32("bytes").ok().map(i64::from))).unwrap_or(0)
    };
    let day_ago = DateTime::from_millis(chrono::Utc::now().timestamp_millis() - 86_400_000);
    let p = &state.config.platform;
    done(json!({
        "installations": count(&state, installations::COLLECTION, doc! {}).await,
        "installations_active_24h": count(&state, installations::COLLECTION, doc! { "last_seen": { "$gte": day_ago } }).await,
        "users": count(&state, auth::USERS, doc! {}).await,
        "users_active_24h": count(&state, auth::USERS, doc! { "last_refresh_at": { "$gte": day_ago } }).await,
        "anonymous_users": count(&state, auth::USERS, doc! { "anonymous": true }).await,
        "storage_objects": count(&state, storage::OBJECTS, doc! {}).await,
        "storage_bytes": storage_bytes,
        "db_documents": count(&state, database::DOCUMENTS, doc! {}).await,
        "functions": count(&state, functions::FUNCTIONS, doc! {}).await,
        "function_calls_24h": count(&state, functions::CALLS, doc! { "at": { "$gte": day_ago } }).await,
        "config": {
            "project_id": p.project_id,
            "token_secret_configured": !p.token_secret_is_random,
            "appcheck_enforced": p.appcheck_enforce,
            "devicecheck": p.devicecheck_key_pem.is_some() && p.devicecheck_key_id.is_some(),
            "play_integrity": p.play_integrity_service_account.is_some(),
            "app_attest": crate::env_nonempty("DM_APP_ATTEST_ROOT_CA").is_some() && crate::env_nonempty("DM_APP_ATTEST_APPS").is_some(),
            "auth_anonymous": p.auth_allow_anonymous,
            "auth_signup": p.auth_allow_signup,
            "auth_google": !p.auth_google_client_ids.is_empty(),
            "auth_apple": !p.auth_apple_audiences.is_empty(),
            "auth_email_webhook": p.auth_email_webhook.is_some(),
            "functions_secret": p.functions_secret.is_some(),
            "storage_max_mb": p.storage_max_bytes / 1024 / 1024,
        },
    }))
}

// ── Auth users ───────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
struct UserQuery {
    q: String,
    limit: Option<i64>,
    after: Option<String>,
}

async fn list_users(State(state): State<AppState>, Query(query): Query<UserQuery>) -> AdminResult {
    let mut filter = doc! {};
    let q = query.q.trim();
    if !q.is_empty() {
        let escaped: String = q.chars().map(|c| if "\\^$.|?*+()[]{}".contains(c) { format!("\\{c}") } else { c.to_string() }).collect();
        filter.insert("$or", vec![doc! { "_id": q }, doc! { "email": { "$regex": escaped.clone(), "$options": "i" } }, doc! { "display_name": { "$regex": escaped, "$options": "i" } }]);
    }
    if let Some(after) = &query.after {
        filter.insert("created_at", doc! { "$lt": DateTime::from_millis(after.parse().unwrap_or(i64::MAX)) });
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let users: Vec<Document> = state.db.coll(auth::USERS).find(filter).sort(doc! { "created_at": -1 }).limit(limit).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    let next = (users.len() as i64 == limit).then(|| users.last().and_then(|u| u.get_datetime("created_at").ok()).map(|d| d.timestamp_millis().to_string())).flatten();
    done(json!({ "users": users.iter().map(auth::user_json).collect::<Vec<_>>(), "next": next, "total": count(&state, auth::USERS, doc! {}).await }))
}

async fn get_user(State(state): State<AppState>, Path(uid): Path<String>) -> AdminResult {
    let user = state.db.coll(auth::USERS).find_one(doc! { "_id": &uid }).await.map_err(db_fail)?.ok_or_else(|| fail(StatusCode::NOT_FOUND, "no such user"))?;
    let sessions = state.db.coll(auth::REFRESH_TOKENS).count_documents(doc! { "uid": &uid }).await.unwrap_or(0);
    done(json!({ "user": auth::user_json(&user), "active_sessions": sessions }))
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct UserBody {
    uid: Option<String>,
    email: Option<String>,
    password: Option<String>,
    display_name: Option<String>,
    photo_url: Option<String>,
    email_verified: Option<bool>,
    disabled: Option<bool>,
    custom_claims: Option<Map<String, Json>>,
}

fn hash(password: &str) -> String {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    argon2::Argon2::default().hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng)).expect("argon2").to_string()
}

async fn create_user(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: UserBody = body(&payload)?;
    checked(validate_user(&state, &b))?;
    let uid = b.uid.clone().filter(|u| !u.is_empty()).unwrap_or_else(|| tokens::random_token().chars().filter(|c| c.is_ascii_alphanumeric()).take(28).collect());
    let now = DateTime::now();
    let mut user = doc! { "_id": &uid, "created_at": now, "disabled": b.disabled.unwrap_or(false), "email_verified": b.email_verified.unwrap_or(false), "anonymous": false, "providers": [], "custom_claims": bson::to_document(&b.custom_claims.clone().unwrap_or_default()).unwrap_or_default(), "tokens_valid_after": 0_i64 };
    if let Some(email) = b.email.as_deref().map(|e| e.trim().to_lowercase()).filter(|e| !e.is_empty()) {
        user.insert("email", &email);
        user.insert("providers", vec![Bson::Document(doc! { "provider_id": "password", "raw_id": &email, "email": &email })]);
    }
    if let Some(password) = b.password.as_deref() {
        user.insert("password_hash", hash(password));
        user.insert("password_updated_at", now);
    }
    if let Some(name) = b.display_name {
        user.insert("display_name", name);
    }
    if let Some(url) = b.photo_url {
        user.insert("photo_url", url);
    }
    state.db.coll(auth::USERS).insert_one(user.clone()).await.map_err(|e| if crate::db::is_duplicate_key(&e) { fail(StatusCode::CONFLICT, "a user with this uid or email already exists") } else { db_fail(e) })?;
    done(json!({ "user": auth::user_json(&user) }))
}

async fn update_user(State(state): State<AppState>, Path(uid): Path<String>, payload: Bytes) -> AdminResult {
    let b: UserBody = body(&payload)?;
    checked(validate_user(&state, &b))?;
    let mut set = doc! {};
    if let Some(v) = b.disabled {
        set.insert("disabled", v);
    }
    if let Some(v) = b.email_verified {
        set.insert("email_verified", v);
    }
    if let Some(v) = b.display_name {
        set.insert("display_name", v);
    }
    if let Some(v) = b.photo_url {
        set.insert("photo_url", v);
    }
    if let Some(v) = b.email {
        set.insert("email", v.trim().to_lowercase());
    }
    if let Some(v) = b.password {
        set.insert("password_hash", hash(&v));
        set.insert("password_updated_at", DateTime::now());
    }
    if let Some(claims) = b.custom_claims {
        set.insert("custom_claims", bson::to_document(&claims).unwrap_or_default());
    }
    if set.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "nothing to update"));
    }
    let updated = state
        .db
        .coll(auth::USERS)
        .find_one_and_update(doc! { "_id": &uid }, doc! { "$set": set })
        .return_document(mongodb::options::ReturnDocument::After)
        .await
        .map_err(|e| if crate::db::is_duplicate_key(&e) { fail(StatusCode::CONFLICT, "email already in use") } else { db_fail(e) })?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "no such user"))?;
    if b.disabled == Some(true) {
        let _ = state.db.coll(auth::REFRESH_TOKENS).delete_many(doc! { "uid": &uid }).await;
    }
    auth::forget_cached(&uid);
    done(json!({ "user": auth::user_json(&updated) }))
}

async fn delete_user(State(state): State<AppState>, Path(uid): Path<String>) -> AdminResult {
    auth::delete_user(&state, &uid).await.map_err(from_api)?;
    done(json!({ "deleted": uid }))
}

async fn revoke(State(state): State<AppState>, Path(uid): Path<String>) -> AdminResult {
    state.db.coll(auth::USERS).update_one(doc! { "_id": &uid }, doc! { "$set": { "tokens_valid_after": tokens::now() } }).await.map_err(db_fail)?;
    let removed = state.db.coll(auth::REFRESH_TOKENS).delete_many(doc! { "uid": &uid }).await.map_err(db_fail)?.deleted_count;
    auth::forget_cached(&uid);
    done(json!({ "revoked_sessions": removed }))
}

async fn custom_token(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let uid = b["uid"].as_str().filter(|u| !u.is_empty() && u.len() <= 128).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "uid (1–128 chars) is required"))?;
    let claims = b["claims"].as_object().cloned().unwrap_or_default();
    let mut v = crate::validate::V::new();
    v.len("uid", uid, 1, crate::validate::limits::UID);
    if !b["claims"].is_null() && !b["claims"].is_object() {
        v.fail("claims", "must be an object");
    }
    auth::validate_custom_claims(&mut v, "claims", &claims);
    checked(v)?;
    done(json!({ "token": auth::mint_custom_token(&state, uid, claims), "expires_in": 3600 }))
}

async fn email_link(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let kind = b["type"].as_str().unwrap_or("PASSWORD_RESET");
    let email = b["email"].as_str().unwrap_or_default().trim().to_lowercase();
    let mut v = crate::validate::V::new();
    v.one_of("type", kind, &["PASSWORD_RESET", "VERIFY_EMAIL", "EMAIL_SIGNIN"]);
    v.email("email", &email);
    if let Some(url) = b["continueUrl"].as_str().filter(|u| !u.is_empty()) {
        v.url("continueUrl", url, false);
    }
    checked(v)?;
    let user = state.db.coll(auth::USERS).find_one(doc! { "email": &email }).await.map_err(db_fail)?;
    if kind != "EMAIL_SIGNIN" && user.is_none() {
        return Err(fail(StatusCode::NOT_FOUND, "no user with that email"));
    }
    let uid = user.as_ref().and_then(|u| u.get_str("_id").ok());
    let (_, link) = auth::create_oob(&state, kind, uid, &email, b["continueUrl"].as_str().unwrap_or("")).await.map_err(from_api)?;
    done(json!({ "link": link }))
}

async fn verify_id_token(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let claims = auth::verify_id_token(&state, b["idToken"].as_str().unwrap_or_default()).await.map_err(from_api)?;
    done(serde_json::to_value(claims).unwrap_or_default())
}

// ── App Check ────────────────────────────────────────────────────────────────

async fn list_debug_tokens(State(state): State<AppState>) -> AdminResult {
    let tokens: Vec<Document> = state.db.coll(super::appcheck::DEBUG_TOKENS).find(doc! {}).sort(doc! { "created_at": -1 }).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    done(json!({ "tokens": tokens.iter().map(|t| json!({ "token": t.get_str("_id").ok(), "name": t.get_str("name").ok(), "app_id": t.get_str("app_id").ok(), "created_at": t.get_datetime("created_at").ok().and_then(|d| d.try_to_rfc3339_string().ok()) })).collect::<Vec<_>>(), "env_tokens": state.config.platform.appcheck_debug_tokens.len() }))
}

async fn add_debug_token(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let mut v = crate::validate::V::new();
    if let Some(t) = b["token"].as_str().filter(|t| !t.is_empty()) {
        v.check(t.len() >= 16 && t.len() <= 256 && crate::validate::is_safe_id(t), "token", "must be 16-256 characters of A-Z a-z 0-9 . _ : - (leave empty to generate one)");
    }
    if let Some(name) = b["name"].as_str() {
        v.text("name", name, 100);
    }
    if let Some(app) = b["app_id"].as_str().filter(|a| !a.is_empty()) {
        v.id("app_id", app, crate::validate::limits::ID);
    }
    checked(v)?;
    let token = b["token"].as_str().map(str::to_string).filter(|t| t.len() >= 16).unwrap_or_else(|| uuid::Uuid::new_v4().to_string().to_uppercase());
    state
        .db
        .coll(super::appcheck::DEBUG_TOKENS)
        .insert_one(doc! { "_id": &token, "name": b["name"].as_str().unwrap_or("debug device"), "app_id": b["app_id"].as_str(), "created_at": DateTime::now() })
        .await
        .map_err(|e| if crate::db::is_duplicate_key(&e) { fail(StatusCode::CONFLICT, "token already registered") } else { db_fail(e) })?;
    done(json!({ "token": token }))
}

async fn remove_debug_token(State(state): State<AppState>, Path(token): Path<String>) -> AdminResult {
    state.db.coll(super::appcheck::DEBUG_TOKENS).delete_one(doc! { "_id": &token }).await.map_err(db_fail)?;
    done(json!({ "deleted": token }))
}

async fn mint_appcheck(State(state): State<AppState>, Path(app_id): Path<String>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let mut v = crate::validate::V::new();
    v.id("app_id", &app_id, crate::validate::limits::ID);
    if let Some(t) = b.get("ttl_seconds").filter(|t| !t.is_null()) {
        v.check(t.as_u64().is_some_and(|t| (1800..=7 * 86400).contains(&t)), "ttl_seconds", "must be between 1800 and 604800");
    }
    checked(v)?;
    let ttl = b["ttl_seconds"].as_u64().map(|t| t.clamp(1800, 7 * 86400));
    done(super::appcheck::issue(&state, &app_id, "custom", b["limited_use"].as_bool().unwrap_or(false), ttl))
}

// ── Installations ────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
struct InstallQuery {
    app_id: Option<String>,
    limit: Option<i64>,
}

async fn list_installations(State(state): State<AppState>, Query(query): Query<InstallQuery>) -> AdminResult {
    let filter = match &query.app_id {
        Some(app) => doc! { "app_id": app },
        None => doc! {},
    };
    let docs: Vec<Document> = state.db.coll(installations::COLLECTION).find(filter.clone()).projection(doc! { "refresh_hash": 0 }).sort(doc! { "last_seen": -1 }).limit(query.limit.unwrap_or(100).clamp(1, 1000)).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    done(json!({ "installations": docs.into_iter().map(|d| bson::Bson::Document(d).into_relaxed_extjson()).collect::<Vec<_>>(), "total": count(&state, installations::COLLECTION, filter).await }))
}

// ── Storage ──────────────────────────────────────────────────────────────────

async fn buckets(State(state): State<AppState>) -> AdminResult {
    let mut cursor = state.db.coll(storage::OBJECTS).aggregate(vec![doc! { "$group": { "_id": "$bucket", "objects": { "$sum": 1 }, "bytes": { "$sum": "$size" } } }, doc! { "$sort": { "_id": 1 } }]).await.map_err(db_fail)?;
    let mut out = Vec::new();
    while let Some(d) = cursor.try_next().await.map_err(db_fail)? {
        out.push(json!({ "name": d.get_str("_id").ok(), "objects": d.get_i32("objects").map(i64::from).or_else(|_| d.get_i64("objects")).unwrap_or(0), "bytes": d.get_i64("bytes").or_else(|_| d.get_i32("bytes").map(i64::from)).unwrap_or(0) }));
    }
    done(json!({ "buckets": out }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct BrowseQuery {
    prefix: String,
    after: Option<String>,
}

async fn browse(State(state): State<AppState>, Path(bucket): Path<String>, Query(query): Query<BrowseQuery>) -> AdminResult {
    let (prefixes, items, next) = storage::list_objects(&state, &bucket, &query.prefix, "/", 200, query.after.as_deref()).await.map_err(from_api)?;
    done(json!({
        "prefixes": prefixes,
        "items": items.iter().map(|m| json!({ "name": m.get_str("name").ok(), "size": m.get_i64("size").unwrap_or(0), "contentType": m.get_str("content_type").ok(), "updated": m.get_datetime("updated_at").ok().and_then(|d| d.try_to_rfc3339_string().ok()), "owner": m.get_str("owner_uid").ok() })).collect::<Vec<_>>(),
        "next": next,
    }))
}

async fn object_info(State(state): State<AppState>, Path((bucket, object)): Path<(String, String)>) -> AdminResult {
    let meta = state.db.coll(storage::OBJECTS).find_one(doc! { "bucket": &bucket, "name": &object }).await.map_err(db_fail)?.ok_or_else(|| fail(StatusCode::NOT_FOUND, "no such object"))?;
    let token = meta.get_array("download_tokens").ok().and_then(|a| a.first()).and_then(Bson::as_str).unwrap_or_default().to_string();
    let encoded = percent_encoding::utf8_percent_encode(&object, percent_encoding::NON_ALPHANUMERIC).to_string();
    done(json!({ "object": bson::Bson::Document(meta).into_relaxed_extjson(), "download_path": format!("/v1/storage/b/{bucket}/o/{encoded}?alt=media&token={token}") }))
}

async fn delete_object(State(state): State<AppState>, Path((bucket, object)): Path<(String, String)>) -> AdminResult {
    let meta = state.db.coll(storage::OBJECTS).find_one(doc! { "bucket": &bucket, "name": &object }).await.map_err(db_fail)?.ok_or_else(|| fail(StatusCode::NOT_FOUND, "no such object"))?;
    storage::delete_stored(&state, &meta).await.map_err(from_api)?;
    done(json!({ "deleted": object }))
}

// ── Database ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
struct CollectionsQuery {
    parent: String,
}

/// Collections directly under `parent` ("" = root, or a document path).
async fn collections(State(state): State<AppState>, Query(query): Query<CollectionsQuery>) -> AdminResult {
    let parent = query.parent.trim_matches('/');
    let filter = if parent.is_empty() {
        doc! { "collection": { "$regex": "^[^/]+$" } }
    } else {
        let escaped: String = parent.chars().map(|c| if "\\^$.|?*+()[]{}".contains(c) { format!("\\{c}") } else { c.to_string() }).collect();
        doc! { "collection": { "$regex": format!("^{escaped}/[^/]+$") } }
    };
    let mut cursor = state.db.coll(database::DOCUMENTS).aggregate(vec![doc! { "$match": filter }, doc! { "$group": { "_id": "$collection", "documents": { "$sum": 1 } } }, doc! { "$sort": { "_id": 1 } }]).await.map_err(db_fail)?;
    let mut out = Vec::new();
    while let Some(d) = cursor.try_next().await.map_err(db_fail)? {
        out.push(json!({ "path": d.get_str("_id").ok(), "documents": d.get_i32("documents").map(i64::from).or_else(|_| d.get_i64("documents")).unwrap_or(0) }));
    }
    done(json!({ "collections": out }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct DocsQuery {
    collection: String,
    limit: Option<i64>,
    after: Option<String>,
}

async fn documents(State(state): State<AppState>, Query(query): Query<DocsQuery>) -> AdminResult {
    let mut filter = doc! { "collection": query.collection.trim_matches('/') };
    if let Some(after) = &query.after {
        filter.insert("_id", doc! { "$gt": after });
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let docs: Vec<Document> = state.db.coll(database::DOCUMENTS).find(filter).sort(doc! { "_id": 1 }).limit(limit).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    let next = (docs.len() as i64 == limit).then(|| docs.last().and_then(|d| d.get_str("_id").ok()).map(str::to_string)).flatten();
    done(json!({ "documents": docs.iter().map(database::document_json).collect::<Vec<_>>(), "next": next }))
}

async fn get_doc(State(state): State<AppState>, Path(path): Path<String>) -> AdminResult {
    let stored = database::read_document(&state, &admin_caller(), &path).await.map_err(from_api)?;
    match stored {
        Some(d) => done(json!({ "exists": true, "document": database::document_json(&d) })),
        None => done(json!({ "exists": false })),
    }
}

async fn put_doc(State(state): State<AppState>, Path(path): Path<String>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let data = b["data"].as_object().cloned().ok_or_else(|| fail(StatusCode::BAD_REQUEST, "body must be {\"data\": {...}}"))?;
    let write = database::Write { op: "set".into(), path: path.clone(), data: Some(data), merge: b["merge"].as_bool().unwrap_or(false), merge_fields: None, precondition: None };
    let (_, stored) = database::commit(&state, &admin_caller(), vec![write], None).await.map_err(from_api)?;
    done(json!({ "document": stored.into_iter().next().flatten().map(|d| database::document_json(&d)) }))
}

async fn delete_doc(State(state): State<AppState>, Path(path): Path<String>) -> AdminResult {
    let write = database::Write { op: "delete".into(), path: path.clone(), data: None, merge: false, merge_fields: None, precondition: None };
    database::commit(&state, &admin_caller(), vec![write], None).await.map_err(from_api)?;
    done(json!({ "deleted": path }))
}

// ── Rules ────────────────────────────────────────────────────────────────────

fn rules_kind(kind: &str) -> Result<&str, Response> {
    match kind {
        "storage" | "database" => Ok(kind),
        _ => Err(fail(StatusCode::NOT_FOUND, "rules kind must be storage or database")),
    }
}

async fn get_rules(State(state): State<AppState>, Path(kind): Path<String>) -> AdminResult {
    let kind = rules_kind(&kind)?;
    let stored = state.db.coll("security_rules").find_one(doc! { "_id": kind }).await.map_err(db_fail)?;
    let rules = storage::load_rules(&state, kind).await;
    done(json!({ "kind": kind, "rules": rules, "is_default": stored.is_none(), "updated_at": stored.as_ref().and_then(|d| d.get_datetime("updated_at").ok()).and_then(|d| d.try_to_rfc3339_string().ok()), "updated_by": stored.as_ref().and_then(|d| d.get_str("updated_by").ok()) }))
}

async fn put_rules(State(state): State<AppState>, Path(kind): Path<String>, payload: Bytes) -> AdminResult {
    let kind = rules_kind(&kind)?;
    let b: Json = body(&payload)?;
    let parsed: Vec<rules::Rule> = serde_json::from_value(b["rules"].clone()).map_err(|e| fail(StatusCode::BAD_REQUEST, format!("rules: {e}")))?;
    rules::validate(&parsed).map_err(|e| fail(StatusCode::BAD_REQUEST, e))?;
    let as_bson = bson::to_bson(&serde_json::to_value(&parsed).unwrap_or_default()).unwrap_or(Bson::Null);
    state
        .db
        .coll("security_rules")
        .update_one(doc! { "_id": kind }, doc! { "$set": { "rules": as_bson, "updated_at": DateTime::now(), "updated_by": b["by"].as_str().unwrap_or("admin-api") } })
        .upsert(true)
        .await
        .map_err(db_fail)?;
    done(json!({ "kind": kind, "rules": parsed, "note": "takes effect within 5 seconds" }))
}

/// Simulates a request against the saved (or supplied) rules.
async fn test_rules(State(state): State<AppState>, Path(kind): Path<String>, payload: Bytes) -> AdminResult {
    let kind = rules_kind(&kind)?;
    let b: Json = body(&payload)?;
    let rules: Vec<rules::Rule> = match b.get("rules") {
        Some(r) if r.is_array() => serde_json::from_value(r.clone()).map_err(|e| fail(StatusCode::BAD_REQUEST, format!("rules: {e}")))?,
        _ => storage::load_rules(&state, kind).await,
    };
    rules::validate(&rules).map_err(|e| fail(StatusCode::BAD_REQUEST, e))?;
    let op = match b["op"].as_str().unwrap_or("get") {
        "get" => Op::Get,
        "list" => Op::List,
        "create" => Op::Create,
        "update" => Op::Update,
        "delete" => Op::Delete,
        other => return Err(fail(StatusCode::BAD_REQUEST, format!("unknown op {other}"))),
    };
    let path = b["path"].as_str().unwrap_or_default();
    let auth = match b["auth"].as_object() {
        Some(a) => json!({ "uid": a.get("uid"), "token": a.get("token").cloned().unwrap_or(json!({})) }),
        None => Json::Null,
    };
    let context = json!({
        "auth": auth,
        "request": { "time": chrono::Utc::now().timestamp_millis(), "method": op.name(), "resource": b.get("request").cloned().unwrap_or(Json::Null), "app": { "id": "test", "appcheck": true } },
        "resource": b.get("resource").cloned().unwrap_or(Json::Null),
    });
    match rules::check(&rules, path, op, &context) {
        Ok(()) => done(json!({ "allowed": true })),
        Err(reason) => done(json!({ "allowed": false, "reason": reason })),
    }
}

// ── Functions ────────────────────────────────────────────────────────────────

async fn list_functions(State(state): State<AppState>) -> AdminResult {
    let docs: Vec<Document> = state.db.coll(functions::FUNCTIONS).find(doc! {}).sort(doc! { "_id": 1 }).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    let day_ago = DateTime::from_millis(chrono::Utc::now().timestamp_millis() - 86_400_000);
    let mut out = Vec::new();
    for d in docs {
        let name = d.get_str("_id").unwrap_or_default().to_string();
        let calls = count(&state, functions::CALLS, doc! { "function": &name, "at": { "$gte": day_ago } }).await;
        let errors = count(&state, functions::CALLS, doc! { "function": &name, "at": { "$gte": day_ago }, "status": { "$nin": ["OK", "STREAM"] } }).await;
        out.push(json!({
            "name": name, "url": d.get_str("url").ok(), "enabled": d.get_bool("enabled").unwrap_or(true),
            "require_auth": d.get_bool("require_auth").unwrap_or(false), "require_appcheck": d.get_bool("require_appcheck").unwrap_or(false),
            "timeout_seconds": d.get_i64("timeout_seconds").unwrap_or(60), "region": d.get_str("region").ok(), "description": d.get_str("description").ok(),
            "has_secret": d.get_str("secret").is_ok(), "calls_24h": calls, "errors_24h": errors,
            "updated_at": d.get_datetime("updated_at").ok().and_then(|x| x.try_to_rfc3339_string().ok()),
        }));
    }
    done(json!({ "functions": out, "default_secret_configured": state.config.platform.functions_secret.is_some() }))
}

async fn upsert_function(State(state): State<AppState>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    let name = b["name"].as_str().unwrap_or_default().to_string();
    save_function(&state, &name, &b).await
}

async fn upsert_named_function(State(state): State<AppState>, Path(name): Path<String>, payload: Bytes) -> AdminResult {
    let b: Json = body(&payload)?;
    save_function(&state, &name, &b).await
}

async fn save_function(state: &AppState, name: &str, b: &Json) -> AdminResult {
    if !functions::is_function_name(name) {
        return Err(fail(StatusCode::BAD_REQUEST, "name: 1–63 letters, digits, _ or -, starting with a letter"));
    }
    let url = b["url"].as_str().unwrap_or_default();
    let mut v = crate::validate::V::new();
    v.url("url", url, false);
    if let Some(t) = b.get("timeout_seconds").filter(|t| !t.is_null()) {
        v.check(t.as_i64().is_some_and(|t| (1..=540).contains(&t)), "timeout_seconds", "must be between 1 and 540");
    }
    for key in ["enabled", "require_auth", "require_appcheck"] {
        v.check(b.get(key).map_or(true, |x| x.is_null() || x.is_boolean()), key, "must be a boolean");
    }
    for (key, max) in [("region", 64), ("description", 500)] {
        if let Some(text) = b[key].as_str() {
            v.text(key, text, max);
        }
    }
    if let Some(secret) = b["secret"].as_str().filter(|s| !s.is_empty()) {
        v.len("secret", secret, 16, 256);
    }
    checked(v)?;
    let mut set = doc! {
        "url": url, "enabled": b["enabled"].as_bool().unwrap_or(true), "require_auth": b["require_auth"].as_bool().unwrap_or(false),
        "require_appcheck": b["require_appcheck"].as_bool().unwrap_or(false), "timeout_seconds": b["timeout_seconds"].as_i64().unwrap_or(60).clamp(1, 540),
        "updated_at": DateTime::now(),
    };
    for key in ["region", "description"] {
        if let Some(v) = b[key].as_str() {
            set.insert(key, v);
        }
    }
    let mut update = doc! { "$set": set, "$setOnInsert": { "created_at": DateTime::now() } };
    match &b["secret"] {
        Json::String(s) if !s.is_empty() => {
            update.get_document_mut("$set").unwrap().insert("secret", s);
        }
        Json::Null if b.get("secret").is_some() => {
            update.insert("$unset", doc! { "secret": "" });
        }
        _ => {}
    }
    state.db.coll(functions::FUNCTIONS).update_one(doc! { "_id": name }, update).upsert(true).await.map_err(db_fail)?;
    done(json!({ "name": name, "saved": true }))
}

async fn delete_function(State(state): State<AppState>, Path(name): Path<String>) -> AdminResult {
    state.db.coll(functions::FUNCTIONS).delete_one(doc! { "_id": &name }).await.map_err(db_fail)?;
    done(json!({ "deleted": name }))
}

async fn function_calls(State(state): State<AppState>, Path(name): Path<String>) -> AdminResult {
    let docs: Vec<Document> = state.db.coll(functions::CALLS).find(doc! { "function": &name }).sort(doc! { "at": -1 }).limit(100).await.map_err(db_fail)?.try_collect().await.map_err(db_fail)?;
    done(json!({ "calls": docs.into_iter().map(|d| bson::Bson::Document(d).into_relaxed_extjson()).collect::<Vec<_>>() }))
}
