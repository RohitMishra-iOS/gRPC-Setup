//! Callable functions (Cloud Functions `onCall` equivalent).
//!
//! Your function code runs wherever you like (Render, Cloud Run, Lambda, a route in another
//! service). You register it once (console or admin API) as {name → url}. Apps call
//!
//!   POST /v1/functions/:name    {"data": …}    (Authorization: Bearer <ID token> optional)
//!
//! The server verifies the API key, App Check (if enforced for "functions") and the ID token, then
//! POSTs to your URL:
//!
//!   {"data": …, "context": {"auth": {"uid", "token": {…claims}} | null,
//!                           "app": {"app_id", "appcheck": {"provider"} | null},
//!                           "instance_id": "<FID>" | null, "function": "name"}}
//!   X-DM-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>     (DM_FUNCTIONS_SECRET / per function)
//!
//! Your endpoint answers {"result": …} or {"error": {"status": "INVALID_ARGUMENT", "message", "details"}}
//! and that is returned to the app as-is. If it answers `text/event-stream`, the stream is passed
//! through (streaming callables). Per-function options: require_auth, require_appcheck,
//! timeout_seconds, region (informational).

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, Mac};
use mongodb::bson::{doc, DateTime};
use serde_json::{json, Value as Json};
use sha2::Sha256;

use super::{caller, ApiError, ApiResult};
use crate::dm::AppState;

pub const FUNCTIONS: &str = "functions";
pub const CALLS: &str = "function_calls";

pub fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("t={timestamp},v1={}", hex::encode(mac.finalize().into_bytes()))
}

pub async fn call(State(state): State<AppState>, Path(name): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let mut v = crate::validate::V::new();
    v.check(is_function_name(&name), "name", "must be 1-63 letters, digits, _ or -, starting with a letter");
    v.check(body.len() <= crate::validate::limits::CALLABLE_BODY_BYTES, "body", "is larger than 10 MB");
    v.into_api()?;
    let caller = caller(&state, &headers, None, "functions").await?;
    let function = state
        .db
        .coll(FUNCTIONS)
        .find_one(doc! { "_id": &name, "enabled": { "$ne": false } })
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| ApiError::not_found(format!("function '{name}' is not registered")))?;
    if function.get_bool("require_auth").unwrap_or(false) && caller.user.is_none() {
        return Err(ApiError::unauthenticated("UNAUTHENTICATED", "this function requires a signed-in user"));
    }
    if function.get_bool("require_appcheck").unwrap_or(false) && caller.appcheck.is_none() {
        return Err(ApiError::unauthenticated("APP_CHECK_MISSING", "this function requires App Check"));
    }
    let request: Json = super::parse_json(&body)?;
    if !request.is_object() || request.get("data").is_none() {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", "body must be {\"data\": …}"));
    }
    let mut v = crate::validate::V::new();
    v.json_depth("data", &request["data"], crate::validate::limits::JSON_DEPTH);
    v.into_api()?;
    let url = function.get_str("url").map_err(|_| ApiError::db("function has no url"))?.to_string();
    let timeout = function.get_i64("timeout_seconds").or_else(|_| function.get_i32("timeout_seconds").map(i64::from)).unwrap_or(60).clamp(1, 540);

    let payload = json!({
        "data": request["data"],
        "context": {
            "auth": caller.user.as_ref().map(|u| json!({ "uid": u.sub, "token": u })),
            "app": { "app_id": caller.app_id, "appcheck": caller.appcheck.as_ref().map(|a| json!({ "provider": a.provider, "app_id": a.sub })) },
            "instance_id": caller.installation_id,
            "function": name,
        },
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let secret = function.get_str("secret").ok().map(str::to_string).or_else(|| state.config.platform.functions_secret.clone());
    let started = std::time::Instant::now();
    let mut outgoing = state
        .http
        .post(&url)
        .timeout(std::time::Duration::from_secs(timeout as u64))
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-dm-function", &name);
    if let Some(secret) = &secret {
        outgoing = outgoing.header("x-dm-signature", sign(secret, chrono::Utc::now().timestamp(), &bytes));
    }
    if let Some(accept) = headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) {
        outgoing = outgoing.header(header::ACCEPT, accept);
    }
    let response = outgoing.body(bytes).send().await;
    let elapsed = started.elapsed().as_millis() as i64;

    let log = |status: &str, http: u16| {
        let calls = state.db.coll(CALLS);
        let record = doc! { "function": &name, "status": status, "http_status": http as i32, "duration_ms": elapsed, "uid": caller.uid(), "app_id": &caller.app_id, "at": DateTime::now() };
        tokio::spawn(async move {
            let _ = calls.insert_one(record).await;
        });
    };

    let response = match response {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            log("DEADLINE_EXCEEDED", 504);
            return Err(ApiError::new(StatusCode::GATEWAY_TIMEOUT, "DEADLINE_EXCEEDED", format!("function '{name}' timed out after {timeout}s")));
        }
        Err(e) => {
            log("UNAVAILABLE", 503);
            return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE", format!("function '{name}' is unreachable: {e}")));
        }
    };
    let status = response.status();
    let is_stream = response.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|c| c.starts_with("text/event-stream"));
    if is_stream {
        log("STREAM", status.as_u16());
        let stream = response.bytes_stream();
        return Ok(Response::builder()
            .status(status.as_u16())
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .unwrap());
    }
    let text = response.bytes().await.unwrap_or_default();
    let parsed: Json = serde_json::from_slice(&text).unwrap_or_else(|_| json!({ "error": { "status": "INTERNAL", "message": "function returned a non-JSON response" } }));
    let outcome = if parsed.get("error").is_some() { parsed["error"]["status"].as_str().unwrap_or("INTERNAL").to_string() } else { "OK".to_string() };
    log(&outcome, status.as_u16());
    if parsed.get("result").is_none() && parsed.get("error").is_none() {
        return Ok((StatusCode::OK, axum::Json(json!({ "result": parsed }))).into_response());
    }
    let http = if parsed.get("error").is_some() && status.is_success() { error_status(&outcome) } else { StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK) };
    Ok((http, axum::Json(parsed)).into_response())
}

fn error_status(status: &str) -> StatusCode {
    match status {
        "INVALID_ARGUMENT" | "FAILED_PRECONDITION" | "OUT_OF_RANGE" => StatusCode::BAD_REQUEST,
        "UNAUTHENTICATED" => StatusCode::UNAUTHORIZED,
        "PERMISSION_DENIED" => StatusCode::FORBIDDEN,
        "NOT_FOUND" => StatusCode::NOT_FOUND,
        "ALREADY_EXISTS" | "ABORTED" => StatusCode::CONFLICT,
        "RESOURCE_EXHAUSTED" => StatusCode::TOO_MANY_REQUESTS,
        "UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
        "DEADLINE_EXCEEDED" => StatusCode::GATEWAY_TIMEOUT,
        "UNIMPLEMENTED" => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn signature_is_stable() {
        let a = super::sign("s3cret", 1700000000, b"{\"data\":1}");
        assert!(a.starts_with("t=1700000000,v1="));
        assert_eq!(a, super::sign("s3cret", 1700000000, b"{\"data\":1}"));
        assert_ne!(a, super::sign("other", 1700000000, b"{\"data\":1}"));
    }
}

/// Callable names: a letter, then letters, digits, _ or - (1-63 characters).
pub fn is_function_name(name: &str) -> bool {
    name.len() <= crate::validate::limits::FUNCTION_NAME
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
