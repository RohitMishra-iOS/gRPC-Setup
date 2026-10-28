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

mod db;
mod events;
mod grpc;
mod proxy;

pub mod pb {
    tonic::include_proto!("status");
}

use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::db::Db;
use crate::pb::status_service_server::StatusServiceServer;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

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
    let allowed_origins = env_nonempty("ALLOWED_ORIGINS").unwrap_or_else(|| "*".into());

    let db = Db::connect().await?;

    // ── 1. gRPC Server ───────────────────────────────────────────────────────
    let grpc_listener = bind(&grpc_host, grpc_port, "GRPC_PORT").await?;
    println!("✅  gRPC server        → internal port {grpc_port}");
    let grpc_server = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(5)))
        .add_service(StatusServiceServer::new(grpc::StatusSvc { db: db.clone() }))
        .serve_with_incoming(TcpListenerStream::new(grpc_listener));

    // ── 2. gRPC-Web server ───────────────────────────────────────────────────
    let proxy_listener = bind("0.0.0.0", proxy_port, "PORT or PROXY_PORT").await?;
    println!("✅  gRPC-Web proxy     → public  port {proxy_port}");
    let proxy_server = axum::serve(proxy_listener, proxy::router(proxy::AppState { db, allowed_origins }));

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
