//! POST /v1/rpc/:service/:method — every dm.sdk.v1 RPC (Analytics, Crashlytics, Remote Config, Performance,
//! Messaging) as proto3 JSON, served by the same handlers as gRPC / gRPC-Web. Field names may be
//! lowerCamelCase or the original snake_case; responses use lowerCamelCase, int64 as strings
//! (standard proto3 JSON mapping). Errors come back as HTTP status + {"error": {...}}.

use std::future::Future;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{de::DeserializeOwned, Serialize};
use tonic::{Code, Status};

use super::ApiError;
use crate::dm::{self, AppState, RequestMeta};

async fn call_json<Req, Res, F, Fut>(body: &[u8], handler: F) -> Result<Vec<u8>, Status>
where
    Req: DeserializeOwned,
    Res: Serialize,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Result<Res, Status>>,
{
    let body: &[u8] = if body.is_empty() { b"{}" } else { body };
    let request: Req = serde_json::from_slice(body).map_err(|e| Status::invalid_argument(format!("invalid JSON request: {e}")))?;
    let response = handler(request).await?;
    serde_json::to_vec(&response).map_err(|e| Status::internal(e.to_string()))
}

pub async fn dispatch_json(state: &AppState, meta: &RequestMeta, path: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
    use dm::{analytics, crashlytics, messaging, performance, remote_config};
    match path {
        "dm.sdk.v1.AnalyticsService/UploadEvents" => call_json(body, |r| analytics::upload_events(state, meta, r)).await,
        "dm.sdk.v1.CrashlyticsService/UploadCrashReports" => call_json(body, |r| crashlytics::upload_crash_reports(state, meta, r)).await,
        "dm.sdk.v1.RemoteConfigService/FetchConfig" => call_json(body, |r| remote_config::fetch_config(state, meta, r)).await,
        "dm.sdk.v1.PerformanceService/UploadPerformance" => call_json(body, |r| performance::upload_performance(state, meta, r)).await,
        "dm.sdk.v1.MessagingService/RegisterDevice" => call_json(body, |r| messaging::register_device(state, meta, r)).await,
        "dm.sdk.v1.MessagingService/UpdateTopicSubscription" => call_json(body, |r| messaging::update_topic_subscription(state, meta, r)).await,
        "dm.sdk.v1.MessagingService/UnregisterDevice" => call_json(body, |r| messaging::unregister_device(state, meta, r)).await,
        _ => Err(Status::unimplemented(format!("unknown method {path}"))),
    }
}

fn http_status(code: Code) -> StatusCode {
    match code {
        Code::InvalidArgument | Code::OutOfRange => StatusCode::BAD_REQUEST,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound | Code::Unimplemented => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::Aborted => StatusCode::CONFLICT,
        Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub async fn call_route(state: &AppState, headers: &HeaderMap, service: &str, method: &str, body: &[u8]) -> Response {
    let meta = RequestMeta::from_headers(headers);
    match dispatch_json(state, &meta, &format!("{service}/{method}"), body).await {
        Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json).into_response(),
        Err(status) => {
            let mut response = ApiError::new(http_status(status.code()), &format!("{:?}", status.code()).to_uppercase(), status.message().to_string()).into_response();
            if let Some(retry) = status.metadata().get("retry-after-seconds").and_then(|v| v.to_str().ok()) {
                if let Ok(value) = retry.parse() {
                    response.headers_mut().insert("retry-after", value);
                }
            }
            response
        }
    }
}

pub async fn call(State(state): State<AppState>, Path((service, method)): Path<(String, String)>, headers: HeaderMap, body: Bytes) -> Response {
    call_route(&state, &headers, &service, &method, &body).await
}
