//! Installations (Firebase Installations equivalent): a per-install identity (FID) every other
//! product can rely on, with a long-lived refresh token and short-lived auth tokens.
//!
//! POST   /v1/installations                {fid?, app_id, platform?, sdk_version?}
//!        → {fid, refresh_token, auth_token: {token, expires_in}}
//!        The client may propose the FID it generated offline; the server keeps it if it is
//!        well-formed and not owned by another app, otherwise it returns a canonical one.
//! POST   /v1/installations/:fid/auth-token   Authorization: FIS_v2 <refresh_token>
//! DELETE /v1/installations/:fid              Authorization: FIS_v2 <refresh_token>
//!        Deletes the install (and its push registration), as Firebase does for GDPR deletion.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use mongodb::bson::{doc, DateTime};
use serde::Deserialize;
use serde_json::json;

use super::tokens::{self, FisClaims, FIS_TOKEN_TTL};
use super::{ok, parse_json, ApiError, ApiResult};
use crate::dm::AppState;

pub const COLLECTION: &str = "installations";

#[derive(Deserialize)]
struct RegisterBody {
    #[serde(default)]
    fid: Option<String>,
    #[serde(default, alias = "appId")]
    app_id: String,
    #[serde(default)]
    platform: String,
    #[serde(default, alias = "sdkVersion")]
    sdk_version: String,
}

fn auth_token(state: &AppState, fid: &str, app_id: &str) -> serde_json::Value {
    let now = tokens::now();
    let token = state.tokens.sign(&FisClaims { typ: "fis".into(), sub: fid.into(), app_id: app_id.into(), iat: now, exp: now + FIS_TOKEN_TTL });
    json!({ "token": token, "expires_in": FIS_TOKEN_TTL })
}

pub async fn register(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let body: RegisterBody = parse_json(&body)?;
    let app_id = if body.app_id.is_empty() {
        headers.get("x-dm-app-id").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
    } else {
        body.app_id
    };
    if app_id.is_empty() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "app_id is required"));
    }
    if state.config.auth_enabled() {
        let key = headers.get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.key_allows(key, &app_id) {
            return Err(ApiError::unauthenticated("API_KEY_INVALID", "missing or invalid x-api-key for this app"));
        }
    }

    let installations = state.db.coll(COLLECTION);
    let mut fid = body.fid.filter(|f| tokens::valid_fid(f)).unwrap_or_else(tokens::new_fid);
    if let Some(existing) = installations.find_one(doc! { "_id": &fid }).await.map_err(ApiError::db)? {
        if existing.get_str("app_id").unwrap_or("") != app_id {
            fid = tokens::new_fid(); // taken by another app: issue a canonical one instead
        }
    }
    let refresh = tokens::random_token();
    let now = DateTime::now();
    installations
        .update_one(
            doc! { "_id": &fid },
            doc! {
                "$set": { "app_id": &app_id, "platform": &body.platform, "sdk_version": &body.sdk_version,
                          "refresh_hash": tokens::hash(&refresh), "last_seen": now },
                "$setOnInsert": { "created_at": now },
            },
        )
        .upsert(true)
        .await
        .map_err(ApiError::db)?;

    ok(json!({ "fid": fid, "refresh_token": refresh, "auth_token": auth_token(&state, &fid, &app_id) }))
}

/// Checks `Authorization: FIS_v2 <refresh_token>` against the stored hash; returns the app id.
async fn check_refresh(state: &AppState, fid: &str, headers: &HeaderMap) -> ApiResult<String> {
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("FIS_v2 "))
        .ok_or_else(|| ApiError::unauthenticated("INVALID_REFRESH_TOKEN", "Authorization: FIS_v2 <refresh_token> required"))?;
    let record = state
        .db
        .coll(COLLECTION)
        .find_one(doc! { "_id": fid })
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| ApiError::not_found("installation not found (register again)"))?;
    let stored = record.get_str("refresh_hash").unwrap_or("");
    if !crate::config::constant_time_eq(stored.as_bytes(), tokens::hash(presented).as_bytes()) {
        return Err(ApiError::unauthenticated("INVALID_REFRESH_TOKEN", "refresh token does not match this installation"));
    }
    Ok(record.get_str("app_id").unwrap_or("").to_string())
}

pub async fn generate_auth_token(State(state): State<AppState>, Path(fid): Path<String>, headers: HeaderMap) -> ApiResult {
    let app_id = check_refresh(&state, &fid, &headers).await?;
    let _ = state.db.coll(COLLECTION).update_one(doc! { "_id": &fid }, doc! { "$set": { "last_seen": DateTime::now() } }).await;
    ok(auth_token(&state, &fid, &app_id))
}

pub async fn delete(State(state): State<AppState>, Path(fid): Path<String>, headers: HeaderMap) -> ApiResult {
    let app_id = check_refresh(&state, &fid, &headers).await?;
    state.db.coll(COLLECTION).delete_one(doc! { "_id": &fid }).await.map_err(ApiError::db)?;
    // Same identity is used for push: drop its registration too.
    let _ = state.db.devices.delete_many(doc! { "app_id": &app_id, "installation_id": &fid }).await;
    let _ = state.db.analytics_users.delete_many(doc! { "app_id": &app_id, "installation_id": &fid }).await;
    ok(json!({}))
}
