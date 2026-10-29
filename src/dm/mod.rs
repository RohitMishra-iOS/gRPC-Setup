//! dm.sdk.v1 — backend for the DM SDK's Crashlytics, Remote Config, Performance and Messaging
//! modules (see dm_sdk.proto). Every RPC is a plain async function taking the shared `AppState`,
//! the request's `RequestMeta` (headers) and the decoded message, so the same code serves native
//! gRPC (services.rs) and gRPC-Web (proxy.rs).

pub mod admin;
pub mod admin_analytics;
pub mod alerts;
pub mod apns;
pub mod conditions;
pub mod crashlytics;
pub mod messaging;
pub mod performance;
pub mod remote_config;
pub mod services;
pub mod symbols;

use std::ops::Deref;
use std::sync::Arc;

use axum::http::HeaderMap;
use mongodb::bson::{doc, Document};
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::config::Config;
use crate::db::Db;
use crate::pb_dm::ClientContext;

/// Everything the handlers share. Cheap to clone.
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

pub struct Inner {
    pub db: Db,
    pub config: Config,
    pub http: reqwest::Client,
    pub rc: remote_config::RcState,
    pub apns: Option<apns::ApnsClient>,
}

impl Deref for AppState {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl AppState {
    pub fn new(db: Db, config: Config) -> Result<Self, crate::BoxError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let apns = match &config.apns {
            Some(apns_config) => Some(apns::ApnsClient::new(apns_config.clone())?),
            None => None,
        };
        Ok(Self(Arc::new(Inner { db, config, http, rc: remote_config::RcState::default(), apns })))
    }
}

/// The headers the iOS SDK sends with every call (DMGrpcWebClient).
#[derive(Clone, Debug, Default)]
pub struct RequestMeta {
    pub api_key: Option<String>,
    pub app_id: Option<String>,
    pub installation_id: Option<String>,
}

impl RequestMeta {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
        Self {
            api_key: get("x-api-key"),
            app_id: get("x-dm-app-id"),
            installation_id: get("x-dm-installation-id"),
        }
    }

    pub fn from_metadata(metadata: &MetadataMap) -> Self {
        let get = |name: &str| metadata.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
        Self {
            api_key: get("x-api-key"),
            app_id: get("x-dm-app-id"),
            installation_id: get("x-dm-installation-id"),
        }
    }
}

/// Validates the API key for the app the request claims to be. With no DM_API_KEYS configured,
/// everything is allowed (development).
pub fn authorize(config: &Config, meta: &RequestMeta, app_id: &str) -> Result<(), Status> {
    if !config.auth_enabled() {
        return Ok(());
    }
    match meta.api_key.as_deref() {
        Some(key) if config.key_allows(key, app_id) => Ok(()),
        Some(_) => Err(Status::unauthenticated("invalid API key for this app")),
        None => Err(Status::unauthenticated("missing x-api-key header")),
    }
}

/// The request's context, required on every dm.sdk.v1 call. The app id falls back to the
/// x-dm-app-id header for clients that leave the context field empty.
pub fn require_context(context: Option<ClientContext>, meta: &RequestMeta) -> Result<ClientContext, Status> {
    let mut context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
    if context.app_id.is_empty() {
        context.app_id = meta.app_id.clone().unwrap_or_default();
    }
    if context.installation_id.is_empty() {
        context.installation_id = meta.installation_id.clone().unwrap_or_default();
    }
    if context.installation_id.is_empty() {
        return Err(Status::invalid_argument("context.installation_id is required"));
    }
    Ok(context)
}

/// Context → embedded document (the device/app snapshot stored with every record).
pub fn context_doc(context: &ClientContext) -> Document {
    doc! {
        "app_id": &context.app_id,
        "installation_id": &context.installation_id,
        "user_id": non_empty(&context.user_id),
        "platform": &context.platform,
        "sdk_version": &context.sdk_version,
        "bundle_id": &context.bundle_id,
        "app_version": &context.app_version,
        "app_build": &context.app_build,
        "os_version": &context.os_version,
        "device_model": &context.device_model,
        "locale": &context.locale,
        "timezone": &context.timezone,
        "network_type": &context.network_type,
    }
}

/// Empty proto3 strings become BSON null, so queries like {user_id: null} work.
pub fn non_empty(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}

pub fn unavailable(err: impl std::fmt::Display) -> Status {
    eprintln!("[dm] database unavailable: {err}");
    Status::unavailable("storage temporarily unavailable")
}
