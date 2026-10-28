//! Public HTTP/1.1 server: gRPC-Web, /events, /health
//! ─────────────────────────────────────────────────────────────────────────────
//! gRPC-Web requests are decoded and dispatched straight to the StatusService
//! handler in-process — no HTTP/2 hop to the internal gRPC port is needed.
//!
//! Client compatibility:
//!  ✅  React (web)       → this port  (gRPC-Web over HTTP/1.1)
//!  ✅  React Native      → this port  (gRPC-Web over HTTP/1.1)
//!  ✅  Android / iOS     → GRPC_PORT  (native gRPC / HTTP/2)

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use prost::Message;
use serde_json::json;

use crate::db::Db;
use crate::pb::StatusRequest;
use crate::{events, grpc};

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub allowed_origins: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .fallback(handle_request)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn handle_request(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut res = route(&state, &method, &uri, body).await;
    set_cors_headers(&state.allowed_origins, &headers, res.headers_mut());
    res
}

async fn route(state: &AppState, method: &Method, uri: &Uri, body: Bytes) -> Response {
    if method == Method::OPTIONS {
        return StatusCode::OK.into_response();
    }

    let url = uri.path_and_query().map_or("/", |p| p.as_str());

    // ── Health check ─────────────────────────────────────────────────────────
    if method == Method::GET && url == "/health" {
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
    handle_grpc_web(&state.db, uri.path(), &body).await
}

fn json_response(status: StatusCode, payload: serde_json::Value) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], payload.to_string()).into_response()
}

// ── gRPC-Web ──────────────────────────────────────────────────────────────────

async fn handle_grpc_web(db: &Db, path: &str, body: &[u8]) -> Response {
    // gRPC-Web frames are prefixed with 5 bytes: [flags(1)][length(4)]
    if body.len() < 5 {
        return (StatusCode::BAD_REQUEST, "Bad Request: too short for gRPC-Web frame").into_response();
    }
    let message = &body[5..];

    let (grpc_status, grpc_message, data) = match path {
        "/status.StatusService/CheckStatus" => match StatusRequest::decode(message) {
            Ok(request) => {
                let response = grpc::check_status(db, request).await;
                (0, String::new(), frame(0x00, &response.encode_to_vec()))
            }
            Err(err) => (13, format!("Failed to parse request: {err}"), Vec::new()),
        },
        _ => (12, format!("The server does not implement the method {path}"), Vec::new()),
    };

    // gRPC-Web trailers frame: flag=0x80, then "grpc-status:X\r\n..."
    let trailers = format!("grpc-status:{grpc_status}\r\ngrpc-message:{grpc_message}\r\n");
    let mut out = data;
    out.extend_from_slice(&frame(0x80, trailers.as_bytes()));

    println!(
        "[Proxy] ✓ grpc-status: {grpc_status}{}",
        if grpc_message.is_empty() { String::new() } else { format!(" ({grpc_message})") }
    );

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
    res.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("POST, OPTIONS"));
    res.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization"),
    );
    res.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("grpc-status, grpc-message, trailer, te"),
    );
}
