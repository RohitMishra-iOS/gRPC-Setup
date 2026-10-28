//! Public HTTP/1.1 server: gRPC-Web, /events, /health, /v1/symbols, /admin
//! ─────────────────────────────────────────────────────────────────────────────
//! gRPC-Web requests are decoded and dispatched straight to the service handlers in-process —
//! no HTTP/2 hop to the internal gRPC port is needed.
//!
//!   POST /status.StatusService/CheckStatus                    analytics batches (1.x, unchanged)
//!   POST /dm.sdk.v1.CrashlyticsService/UploadCrashReports
//!   POST /dm.sdk.v1.RemoteConfigService/FetchConfig
//!   POST /dm.sdk.v1.PerformanceService/UploadPerformance
//!   POST /dm.sdk.v1.MessagingService/{RegisterDevice,UpdateTopicSubscription,UnregisterDevice}
//!   POST /events             analytics batch as plain JSON
//!   POST /v1/symbols         dSYM upload (multipart, Scripts/dm_upload_symbols.sh)
//!   *    /admin/...          admin API (see dm/admin.rs)
//!   GET  /health
//!
//! Client compatibility:
//!  ✅  React (web) / React Native / iOS DM SDK → this port  (gRPC-Web over HTTP/1.1)
//!  ✅  Android / iOS native gRPC               → GRPC_PORT  (HTTP/2)

use std::future::Future;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use prost::Message;
use serde_json::json;
use tonic::Status;

use crate::dm::{self, AppState, RequestMeta};
use crate::pb::StatusRequest;
use crate::{events, grpc};

const MAX_BODY_BYTES: usize = crate::MAX_MESSAGE_BYTES;

pub fn router(state: AppState) -> Router {
    let symbols_limit = state.config.symbols_max_bytes;
    Router::new()
        .route("/v1/symbols", post(dm::symbols::upload).layer(DefaultBodyLimit::max(symbols_limit)))
        .nest("/admin", dm::admin::router(state.clone()))
        .fallback(handle_request)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(axum::middleware::from_fn_with_state(state.clone(), cors))
        .with_state(state)
}

async fn cors(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let request_headers = request.headers().clone();
    if request.method() == Method::OPTIONS {
        let mut response = StatusCode::OK.into_response();
        set_cors_headers(&state.config.allowed_origins, &request_headers, response.headers_mut());
        return response;
    }
    let mut response = next.run(request).await;
    set_cors_headers(&state.config.allowed_origins, &request_headers, response.headers_mut());
    response
}

async fn handle_request(State(state): State<AppState>, method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let url = uri.path_and_query().map_or("/", |p| p.as_str());

    // ── Health check ─────────────────────────────────────────────────────────
    if method == Method::GET && (url == "/health" || url == "/") {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        return json_response(StatusCode::OK, json!({ "status": "ok", "timestamp": now }));
    }

    if method != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed").into_response();
    }

    // ── Analytics events batch ──────────────────────────────────────────────
    if url == "/events" {
        let (code, payload) = events::process_events_json(&state.db, &String::from_utf8_lossy(&body)).await;
        if code == 200 {
            println!("[Events] ✓ batch_id={} events={}", events::batch_id_label(&payload), events::events_len(&payload));
        }
        let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return json_response(status, payload);
    }

    println!("[Proxy] {method} {url}");
    let meta = RequestMeta::from_headers(&headers);
    handle_grpc_web(&state, &meta, uri.path(), &body).await
}

fn json_response(status: StatusCode, payload: serde_json::Value) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], payload.to_string()).into_response()
}

// ── gRPC-Web ──────────────────────────────────────────────────────────────────

/// Decodes the request message, runs the handler, encodes the response message.
async fn call<Req, Res, F, Fut>(message: &[u8], handler: F) -> Result<Vec<u8>, Status>
where
    Req: Message + Default,
    Res: Message,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Result<Res, Status>>,
{
    let request = Req::decode(message).map_err(|err| Status::invalid_argument(format!("could not decode request: {err}")))?;
    handler(request).await.map(|response| response.encode_to_vec())
}

pub async fn dispatch(state: &AppState, meta: &RequestMeta, path: &str, message: &[u8]) -> Result<Vec<u8>, Status> {
    use dm::{crashlytics, messaging, performance, remote_config};
    match path {
        "/dm.sdk.v1.CrashlyticsService/UploadCrashReports" => {
            call(message, |r| crashlytics::upload_crash_reports(state, meta, r)).await
        }
        "/dm.sdk.v1.RemoteConfigService/FetchConfig" => call(message, |r| remote_config::fetch_config(state, meta, r)).await,
        "/dm.sdk.v1.PerformanceService/UploadPerformance" => {
            call(message, |r| performance::upload_performance(state, meta, r)).await
        }
        "/dm.sdk.v1.MessagingService/RegisterDevice" => call(message, |r| messaging::register_device(state, meta, r)).await,
        "/dm.sdk.v1.MessagingService/UpdateTopicSubscription" => {
            call(message, |r| messaging::update_topic_subscription(state, meta, r)).await
        }
        "/dm.sdk.v1.MessagingService/UnregisterDevice" => {
            call(message, |r| messaging::unregister_device(state, meta, r)).await
        }
        _ => Err(Status::unimplemented(format!("The server does not implement the method {path}"))),
    }
}

async fn handle_grpc_web(state: &AppState, meta: &RequestMeta, path: &str, body: &[u8]) -> Response {
    // gRPC-Web frames are prefixed with 5 bytes: [flags(1)][length(4)]
    if body.len() < 5 {
        return (StatusCode::BAD_REQUEST, "Bad Request: too short for gRPC-Web frame").into_response();
    }
    let declared = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let message = &body[5..(5 + declared).min(body.len())];

    let outcome: Result<Vec<u8>, Status> = if body[0] & 0x01 != 0 {
        Err(Status::unimplemented("compressed gRPC-Web messages are not supported"))
    } else if path == "/status.StatusService/CheckStatus" {
        // 1.x behaviour kept exactly: decode failures are INTERNAL, app-level errors live in the
        // response's own code field.
        match StatusRequest::decode(message) {
            Ok(request) => Ok(grpc::check_status(&state.db, request).await.encode_to_vec()),
            Err(err) => Err(Status::internal(format!("Failed to parse request: {err}"))),
        }
    } else {
        dispatch(state, meta, path, message).await
    };

    let (data, trailers) = match outcome {
        Ok(bytes) => (frame(0x00, &bytes), "grpc-status:0\r\ngrpc-message:\r\n".to_string()),
        Err(status) => {
            let mut trailers = format!(
                "grpc-status:{}\r\ngrpc-message:{}\r\n",
                status.code() as i32,
                encode_grpc_message(status.message())
            );
            if let Some(retry) = status.metadata().get("retry-after-seconds").and_then(|v| v.to_str().ok()) {
                trailers.push_str(&format!("retry-after-seconds:{retry}\r\n"));
            }
            (Vec::new(), trailers)
        }
    };

    let mut out = data;
    out.extend_from_slice(&frame(0x80, trailers.as_bytes()));
    println!("[Proxy] ✓ {}", trailers.lines().next().unwrap_or_default());

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/grpc-web+proto"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        out,
    )
        .into_response()
}

fn frame(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(5 + payload.len());
    f.push(flag);
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
}

/// grpc-message is percent-encoded per the gRPC spec (the iOS client decodes it).
const GRPC_MESSAGE: &AsciiSet = &CONTROLS.add(b'%').add(b'\r').add(b'\n');

fn encode_grpc_message(message: &str) -> String {
    utf8_percent_encode(message, GRPC_MESSAGE).to_string()
}

// ── CORS ──────────────────────────────────────────────────────────────────────

fn set_cors_headers(allowed_origins: &str, req: &HeaderMap, res: &mut HeaderMap) {
    let origin = req.get(header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
    // Echo the origin when it is in the list; everything else gets '*' (same as the Node server).
    let listed = allowed_origins != "*" && allowed_origins.split(',').map(str::trim).any(|o| o == origin);
    let allow = if listed && !origin.is_empty() {
        HeaderValue::from_str(origin).unwrap_or(HeaderValue::from_static("*"))
    } else {
        HeaderValue::from_static("*")
    };

    res.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, allow);
    res.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, PUT, PATCH, OPTIONS"));
    res.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(
            "content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization, x-api-key, x-admin-key, \
             x-dm-app-id, x-dm-installation-id, x-dm-sdk-version",
        ),
    );
    res.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("grpc-status, grpc-message, retry-after-seconds, trailer, te"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let f = frame(0x80, b"grpc-status:0\r\n");
        assert_eq!(f[0], 0x80);
        assert_eq!(u32::from_be_bytes([f[1], f[2], f[3], f[4]]) as usize, f.len() - 5);
    }

    #[test]
    fn grpc_message_is_percent_encoded() {
        assert_eq!(encode_grpc_message("bad\r\nkey 100%"), "bad%0D%0Akey 100%25");
    }
}
