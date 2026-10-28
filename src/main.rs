//! main.rs
//! ─────────────────────────────────────────────────────────────────────────────
//! Runs TWO services in a single process:
//!
//!  1. gRPC server      — HTTP/2, internal only (GRPC_PORT or 50051)
//!  2. gRPC-Web server  — HTTP/1.1, public-facing (PORT / PROXY_PORT or 8080)
//!
//! Why single process?
//!   Render (and most PaaS platforms) expose exactly ONE port per service.
//!   The gRPC-Web server is that public port. The gRPC server runs internally
//!   on a second port that is never exposed to the internet.
//!
//! Platform clients:
//!   React / React Native (web) → PROXY_PORT  (gRPC-Web over HTTP/1.1)
//!   Android / iOS              → GRPC_PORT   (native gRPC over HTTP/2)

mod config;
mod db;
mod dm;
mod events;
mod grpc;
mod proxy;
#[cfg(test)]
mod tests;

pub mod pb {
    tonic::include_proto!("status");
}

/// dm.sdk.v1 — Crashlytics / Remote Config / Performance / Messaging (dm_sdk.proto).
pub mod pb_dm {
    tonic::include_proto!("dm.sdk.v1");
}

use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::config::Config;
use crate::db::Db;
use crate::dm::services::DmSvc;
use crate::dm::AppState;
use crate::pb::status_service_server::StatusServiceServer;
use crate::pb_dm::crashlytics_service_server::CrashlyticsServiceServer;
use crate::pb_dm::messaging_service_server::MessagingServiceServer;
use crate::pb_dm::performance_service_server::PerformanceServiceServer;
use crate::pb_dm::remote_config_service_server::RemoteConfigServiceServer;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Crash/performance batches can be large (stack traces, MetricKit JSON).
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

fn log_config(config: &Config) {
    if config.auth_enabled() {
        println!("✅  API keys            → {} configured (x-api-key required on dm.sdk.v1)", config.api_keys.len());
    } else {
        println!("⚠️  DM_API_KEYS unset   → dm.sdk.v1 services accept unauthenticated calls (dev only)");
    }
    println!(
        "{}  Admin API           → {}",
        if config.admin_key.is_some() { "✅" } else { "⚠️ " },
        if config.admin_key.is_some() { "enabled at /admin" } else { "disabled (set DM_ADMIN_KEY)" }
    );
    println!(
        "{}  APNs                → {}",
        if config.apns.is_some() { "✅" } else { "⚠️ " },
        if config.apns.is_some() { "configured" } else { "not configured (push sending disabled)" }
    );
}

/// Like JS `process.env.X || default`: unset and empty both count as missing.
pub fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_port(keys: &[&str], default: u16) -> Result<u16, BoxError> {
    match keys.iter().find_map(|k| env_nonempty(k)) {
        Some(v) => Ok(v.parse().map_err(|_| format!("invalid port: {v}"))?),
        None => Ok(default),
    }
}

async fn bind(addr: &str, port: u16, env_hint: &str) -> Result<TcpListener, BoxError> {
    TcpListener::bind((addr, port)).await.map_err(|err| -> BoxError {
        if err.kind() == std::io::ErrorKind::AddrInUse {
            format!("Port {port} already in use. Set {env_hint} in .env").into()
        } else {
            err.into()
        }
    })
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

async fn run() -> Result<(), BoxError> {
    // On Render, PORT is injected automatically. We use it as the gRPC-Web
    // port (the public-facing one). The gRPC server runs on GRPC_PORT internally.
    let proxy_port = env_port(&["PORT", "PROXY_PORT"], 8080)?;
    let grpc_port = env_port(&["GRPC_PORT"], 50051)?;
    let grpc_host = env_nonempty("GRPC_HOST").unwrap_or_else(|| "0.0.0.0".into());
    let config = Config::from_env();

    let db = Db::connect(config.retention_days, config.analytics_retention_days).await?;
    log_config(&config);
    let state = AppState::new(db.clone(), config)?;

    // ── 1. gRPC Server ───────────────────────────────────────────────────────
    let grpc_listener = bind(&grpc_host, grpc_port, "GRPC_PORT").await?;
    println!("✅  gRPC server        → internal port {grpc_port}");
    let grpc_server = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(5)))
        .add_service(StatusServiceServer::new(grpc::StatusSvc { db: db.clone() }))
        .add_service(CrashlyticsServiceServer::new(DmSvc(state.clone())).max_decoding_message_size(MAX_MESSAGE_BYTES))
        .add_service(RemoteConfigServiceServer::new(DmSvc(state.clone())))
        .add_service(PerformanceServiceServer::new(DmSvc(state.clone())).max_decoding_message_size(MAX_MESSAGE_BYTES))
        .add_service(MessagingServiceServer::new(DmSvc(state.clone())))
        .serve_with_incoming(TcpListenerStream::new(grpc_listener));

    // ── 2. gRPC-Web server ───────────────────────────────────────────────────
    let proxy_listener = bind("0.0.0.0", proxy_port, "PORT or PROXY_PORT").await?;
    println!("✅  gRPC-Web proxy     → public  port {proxy_port}");
    let proxy_server = axum::serve(proxy_listener, proxy::router(state));

    println!();
    println!("──────────────────────────────────────────");
    println!("  React / React Native  →  :{proxy_port}  (gRPC-Web)");
    println!("  Android / iOS         →  :{grpc_port}  (native gRPC)");
    println!("──────────────────────────────────────────");

    tokio::select! {
        r = grpc_server => r?,
        r = proxy_server => r?,
        _ = shutdown_signal() => println!("Shutting down"),
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(err) = run().await {
        eprintln!("❌  Startup failed: {err}");
        std::process::exit(1);
    }
}
