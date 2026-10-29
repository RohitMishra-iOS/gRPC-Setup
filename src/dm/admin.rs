//! Admin HTTP API (JSON) — the "Firebase console" for this backend. Every route requires
//! `x-admin-key: <DM_ADMIN_KEY>` (or `Authorization: Bearer <DM_ADMIN_KEY>`); without
//! DM_ADMIN_KEY set, the admin API is disabled.
//!
//!   Remote Config
//!     GET   /admin/remote-config/:app_id                     current template + version
//!     GET   /admin/remote-config/:app_id/versions            version history
//!     PUT   /admin/remote-config/:app_id[?notify=true]       publish a new version (body = template);
//!                                                            notify=true sends a silent rc_update push
//!     POST  /admin/remote-config/:app_id/rollback/:version   republish an old version as the newest
//!     POST  /admin/remote-config/:app_id/preview             body {"context": {...}} → what that device gets
//!   Messaging
//!     POST  /admin/messaging/send                            send a push (see SendRequest)
//!     GET   /admin/messaging/:app_id/devices                 registration counts
//!   Crashlytics
//!     GET   /admin/crashlytics/:app_id/issues[?status=&fatal=&version=&limit=]
//!     GET   /admin/crashlytics/issues/:issue_id              issue + recent reports + affected users
//!     PATCH /admin/crashlytics/issues/:issue_id              {"status": "open"|"closed"|"muted"}
//!   Release monitoring / performance
//!     GET   /admin/stability/:app_id[?days=7]                crash-free sessions & users per app version
//!     GET   /admin/performance/:app_id[?days=7]              app start, network, screens, traces

use std::collections::HashMap;

use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json as AxumJson, Router};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::bson::{self, doc, Bson, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json};

use super::admin_analytics as analytics;
use super::apns::PushRequest;
use super::conditions::EvalContext;
use super::remote_config::{self, Template};
use super::AppState;
use crate::config::constant_time_eq;

pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/remote-config/:app_id", get(get_template).put(put_template))
        .route("/remote-config/:app_id/versions", get(list_versions))
        .route("/remote-config/:app_id/rollback/:version", post(rollback))
        .route("/remote-config/:app_id/preview", post(preview))
        .route("/messaging/send", post(send))
        .route("/messaging/:app_id/devices", get(device_stats))
        .route("/crashlytics/:app_id/issues", get(list_issues))
        .route("/crashlytics/issues/:issue_id", get(get_issue).patch(update_issue))
        .route("/stability/:app_id", get(stability))
        .route("/performance/:app_id", get(performance))
        // Console views (admin_analytics.rs)
        .route("/apps", get(analytics::apps))
        .route("/analytics/:app_id/overview", get(analytics::overview))
        .route("/analytics/:app_id/realtime", get(analytics::realtime))
        .route("/analytics/:app_id/events", get(analytics::events))
        .route("/analytics/:app_id/events/:name", get(analytics::event_detail))
        .route("/analytics/:app_id/screens", get(analytics::screens))
        .route("/analytics/:app_id/stream", get(analytics::stream))
        .route("/analytics/:app_id/users/:installation_id", get(analytics::user_detail))
        .route("/crashlytics/:app_id/trend", get(analytics::crashlytics_trend))
        .route("/crashlytics/issues/:issue_id/trend", get(analytics::issue_trend))
        .route("/crashlytics/reports/:report_id", get(analytics::report_detail))
        .route("/messaging/:app_id/history", get(analytics::push_history))
        .layer(axum::middleware::from_fn_with_state(state, require_admin))
}

async fn require_admin(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let Some(expected) = state.config.admin_key.as_deref() else {
        return error(StatusCode::FORBIDDEN, "admin API disabled — set DM_ADMIN_KEY");
    };
    let headers = request.headers();
    let provided = headers
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")));
    if !provided.is_some_and(|p| constant_time_eq(p.as_bytes(), expected.as_bytes())) {
        return error(StatusCode::UNAUTHORIZED, "missing or invalid admin key");
    }
    next.run(request).await
}

type ApiResult = Result<Response, Response>;

pub(super) fn error(status: StatusCode, message: &str) -> Response {
    (status, AxumJson(json!({ "error": message }))).into_response()
}

pub(super) fn db_error(err: impl std::fmt::Display) -> Response {
    eprintln!("[Admin] database error: {err}");
    error(StatusCode::SERVICE_UNAVAILABLE, "database error")
}

pub(super) fn ok(value: Json) -> ApiResult {
    Ok((StatusCode::OK, AxumJson(value)).into_response())
}

/// BSON → relaxed JSON (dates as ISO strings, ints as numbers).
pub(super) fn to_json(document: Document) -> Json {
    Bson::Document(document).into_relaxed_extjson()
}

// ── Remote Config ────────────────────────────────────────────────────────────

async fn get_template(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    match remote_config::latest_template(&state, &app_id).await.map_err(db_error)? {
        Some(stored) => ok(json!({ "app_id": app_id, "version": stored.version, "template": stored.template })),
        None => Err(error(StatusCode::NOT_FOUND, "no template published for this app")),
    }
}

async fn list_versions(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let cursor = state
        .db
        .rc_templates
        .find(doc! { "app_id": &app_id })
        .sort(doc! { "version": -1 })
        .projection(doc! { "_id": 0, "version": 1, "description": 1, "published_by": 1, "created_at": 1 })
        .limit(100)
        .await
        .map_err(db_error)?;
    let versions: Vec<Json> = cursor.map_ok(to_json).try_collect().await.map_err(db_error)?;
    ok(json!({ "app_id": app_id, "versions": versions }))
}

#[derive(Deserialize)]
struct PublishQuery {
    #[serde(default)]
    notify: bool,
}

async fn put_template(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    Query(query): Query<PublishQuery>,
    body: axum::body::Bytes,
) -> ApiResult {
    let template: Template = serde_json::from_slice(&body)
        .map_err(|e| error(StatusCode::BAD_REQUEST, &format!("invalid template JSON: {e}")))?;
    template.validate().map_err(|e| error(StatusCode::BAD_REQUEST, &e))?;
    let version = remote_config::publish(&state, &app_id, &template, "admin-api")
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    println!("[RemoteConfig] published {app_id} v{version}");
    let notified = if query.notify { Some(spawn_config_update_push(&state, &app_id)) } else { None };
    ok(json!({ "app_id": app_id, "version": version, "realtime_push": notified }))
}

async fn rollback(State(state): State<AppState>, Path((app_id, version)): Path<(String, i64)>) -> ApiResult {
    let old = remote_config::template_version(&state, &app_id, version)
        .await
        .map_err(db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "no such version"))?;
    let new_version = remote_config::publish(&state, &app_id, &old.template, &format!("rollback-to-v{version}"))
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    ok(json!({ "app_id": app_id, "version": new_version, "rolled_back_to": version }))
}

#[derive(Deserialize, Default)]
struct PreviewContext {
    #[serde(default)]
    installation_id: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    app_version: String,
    #[serde(default)]
    app_build: String,
    #[serde(default)]
    os_version: String,
    #[serde(default)]
    device_model: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    country: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    locale: String,
    #[serde(default)]
    bundle_id: String,
    #[serde(default)]
    user_properties: HashMap<String, String>,
    #[serde(default)]
    custom_signals: HashMap<String, String>,
}

#[derive(Deserialize)]
struct PreviewRequest {
    #[serde(default)]
    context: PreviewContext,
    /// Preview a draft instead of the published template.
    template: Option<Template>,
}

async fn preview(State(state): State<AppState>, Path(app_id): Path<String>, AxumJson(request): AxumJson<PreviewRequest>) -> ApiResult {
    let (template, version) = match request.template {
        Some(draft) => {
            draft.validate().map_err(|e| error(StatusCode::BAD_REQUEST, &e))?;
            (draft, None)
        }
        None => {
            let stored = remote_config::latest_template(&state, &app_id)
                .await
                .map_err(db_error)?
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "no template published for this app"))?;
            (stored.template, Some(stored.version))
        }
    };
    let c = request.context;
    let ctx = EvalContext {
        app_id: app_id.clone(),
        installation_id: if c.installation_id.is_empty() { "preview".into() } else { c.installation_id },
        user_id: c.user_id,
        bundle_id: c.bundle_id,
        platform: if c.platform.is_empty() { "ios".into() } else { c.platform },
        app_version: c.app_version,
        app_build: c.app_build,
        os_version: c.os_version,
        device_model: c.device_model,
        locale: c.locale,
        language: c.language,
        country: c.country,
        user_properties: c.user_properties,
        custom_signals: c.custom_signals,
        now_ms: None,
    };
    let (entries, experiments) = template.evaluate(&ctx);
    let experiments: Vec<Json> = experiments.into_iter().map(|(e, v)| json!({ "experiment_id": e, "variant_id": v })).collect();
    ok(json!({ "version": version, "entries": entries, "experiments": experiments }))
}

// ── Messaging ────────────────────────────────────────────────────────────────

/// Exactly one target.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
enum Target {
    RegistrationToken(String),
    InstallationId(String),
    UserId(String),
    Topic(String),
    /// {"all": true} — every active device of the app.
    All(bool),
}

#[derive(Deserialize, Debug, Clone, Default)]
struct Notification {
    title: Option<String>,
    subtitle: Option<String>,
    body: Option<String>,
    #[serde(default = "default_sound")]
    sound: Option<String>,
    badge: Option<i64>,
    image_url: Option<String>,
    category: Option<String>,
    thread_id: Option<String>,
}

fn default_sound() -> Option<String> {
    Some("default".into())
}

#[derive(Deserialize, Debug, Clone)]
struct SendRequest {
    app_id: String,
    target: Target,
    /// Omit for a silent (background) push.
    notification: Option<Notification>,
    /// Custom keys merged into the payload's top level.
    #[serde(default)]
    data: Map<String, Json>,
    campaign_id: Option<String>,
    campaign_name: Option<String>,
    label: Option<String>,
    collapse_id: Option<String>,
    #[serde(default = "default_ttl")]
    ttl_seconds: u64,
    /// Count recipients without sending.
    #[serde(default)]
    dry_run: bool,
}

fn default_ttl() -> u64 {
    24 * 3600
}

async fn send(State(state): State<AppState>, AxumJson(request): AxumJson<SendRequest>) -> ApiResult {
    if matches!(request.target, Target::All(false)) {
        return Err(error(StatusCode::BAD_REQUEST, "target {\"all\": false} matches nobody — use {\"all\": true} to send to every device"));
    }
    if state.apns.is_none() && !request.dry_run {
        return Err(error(StatusCode::FAILED_DEPENDENCY, "APNs not configured — set APNS_KEY_P8/APNS_KEY_ID/APNS_TEAM_ID"));
    }
    let summary = deliver(&state, request, None).await.map_err(db_error)?;
    ok(summary)
}

/// Silent `{"dm": {"rc_update": "1"}}` push to every active device of the app, in the background.
fn spawn_config_update_push(state: &AppState, app_id: &str) -> &'static str {
    if state.apns.is_none() {
        return "skipped: APNs not configured";
    }
    let request = SendRequest {
        app_id: app_id.to_string(),
        target: Target::All(true),
        notification: None,
        data: Map::new(),
        campaign_id: None,
        campaign_name: None,
        label: Some("remote_config_update".into()),
        collapse_id: Some("dm-rc-update".into()),
        ttl_seconds: 3600,
        dry_run: false,
    };
    let state = state.clone();
    tokio::spawn(async move {
        match deliver(&state, request, Some(json!({ "rc_update": "1" }))).await {
            Ok(summary) => println!("[RemoteConfig] realtime push: {summary}"),
            Err(err) => eprintln!("[RemoteConfig] realtime push failed: {err}"),
        }
    });
    "started"
}

async fn deliver(state: &AppState, request: SendRequest, extra_dm: Option<Json>) -> Result<Json, mongodb::error::Error> {
    let mut filter = doc! { "app_id": &request.app_id, "active": true };
    match &request.target {
        Target::RegistrationToken(token) => filter.insert("_id", token),
        Target::InstallationId(id) => filter.insert("installation_id", id),
        Target::UserId(id) => filter.insert("user_id", id),
        Target::Topic(topic) => filter.insert("topics", topic),
        Target::All(_) => None,
    };

    let message_id = format!("m-{}", uuid::Uuid::new_v4().simple());
    let silent = request.notification.is_none();
    let payload = build_payload(&request, &message_id, extra_dm);

    if request.dry_run {
        let recipients = state.db.devices.count_documents(filter).await?;
        return Ok(json!({ "dry_run": true, "recipients": recipients, "payload": payload }));
    }
    let Some(apns) = state.apns.as_ref() else {
        return Ok(json!({ "error": "APNs not configured" }));
    };

    let cursor = state
        .db
        .devices
        .find(filter)
        .projection(doc! { "_id": 1, "apns_token": 1, "apns_environment": 1, "bundle_id": 1 })
        .await?;

    let payload_ref = &payload;
    let collapse_id = request.collapse_id.as_deref();
    let ttl_seconds = request.ttl_seconds;
    let results: Vec<(String, Result<(), super::apns::ApnsError>)> = cursor
        .filter_map(|device| async move { device.ok() })
        .map(|device| async move {
            let id = device.get_str("_id").unwrap_or_default().to_string();
            let token = device.get_str("apns_token").unwrap_or_default();
            let environment = device.get_str("apns_environment").unwrap_or("production");
            let topic = device
                .get_str("bundle_id")
                .ok()
                .filter(|t| !t.is_empty())
                .or(apns.default_topic())
                .unwrap_or_default();
            let result = apns
                .send(PushRequest {
                    device_token: token,
                    environment,
                    topic,
                    payload: payload_ref,
                    silent,
                    collapse_id,
                    ttl_seconds,
                })
                .await;
            (id, result)
        })
        .buffer_unordered(64)
        .collect()
        .await;

    let mut sent = 0;
    let mut failures: HashMap<String, u64> = HashMap::new();
    let mut invalid: Vec<String> = Vec::new();
    for (id, result) in &results {
        match result {
            Ok(()) => sent += 1,
            Err(err) => {
                *failures.entry(err.reason.clone()).or_default() += 1;
                if err.is_invalid_token() {
                    invalid.push(id.clone());
                }
            }
        }
    }
    if !invalid.is_empty() {
        state
            .db
            .devices
            .update_many(
                doc! { "_id": { "$in": &invalid } },
                doc! { "$set": { "active": false, "invalid_reason": "apns_rejected", "updated_at": DateTime::now() } },
            )
            .await?;
    }

    let failed: u64 = failures.values().sum();
    let target = format!("{:?}", request.target);
    let _ = state
        .db
        .push_messages
        .insert_one(doc! {
            "_id": &message_id,
            "app_id": &request.app_id,
            "target": &target,
            "silent": silent,
            "campaign_id": request.campaign_id.clone(),
            "campaign_name": request.campaign_name.clone(),
            "label": request.label.clone(),
            "payload": bson::to_bson(&payload).unwrap_or(Bson::Null),
            "recipients": results.len() as i64,
            "sent": sent as i64,
            "failed": failed as i64,
            "failure_reasons": bson::to_bson(&failures).unwrap_or(Bson::Null),
            "deactivated_devices": invalid.len() as i64,
            "created_at": DateTime::now(),
            "received_at": DateTime::now(),
        })
        .await;

    Ok(json!({
        "message_id": message_id,
        "recipients": results.len(),
        "sent": sent,
        "failed": failed,
        "failure_reasons": failures,
        "deactivated_devices": invalid.len(),
    }))
}

/// APNs JSON. The `dm` block is what DMMessaging/DMMessagingExtension read on the device.
fn build_payload(request: &SendRequest, message_id: &str, extra_dm: Option<Json>) -> Json {
    let mut aps = Map::new();
    match &request.notification {
        Some(n) => {
            let mut alert = Map::new();
            if let Some(v) = &n.title { alert.insert("title".into(), json!(v)); }
            if let Some(v) = &n.subtitle { alert.insert("subtitle".into(), json!(v)); }
            if let Some(v) = &n.body { alert.insert("body".into(), json!(v)); }
            aps.insert("alert".into(), Json::Object(alert));
            if let Some(v) = &n.sound { aps.insert("sound".into(), json!(v)); }
            if let Some(v) = n.badge { aps.insert("badge".into(), json!(v)); }
            if let Some(v) = &n.category { aps.insert("category".into(), json!(v)); }
            if let Some(v) = &n.thread_id { aps.insert("thread-id".into(), json!(v)); }
            if n.image_url.is_some() { aps.insert("mutable-content".into(), json!(1)); }
        }
        None => {
            aps.insert("content-available".into(), json!(1));
        }
    }

    let mut dm = Map::new();
    dm.insert("message_id".into(), json!(message_id));
    if let Some(v) = &request.campaign_id { dm.insert("campaign_id".into(), json!(v)); }
    if let Some(v) = &request.campaign_name { dm.insert("campaign_name".into(), json!(v)); }
    if let Some(v) = &request.label { dm.insert("label".into(), json!(v)); }
    if let Some(url) = request.notification.as_ref().and_then(|n| n.image_url.clone()) {
        dm.insert("image_url".into(), json!(url));
    }
    if let Some(Json::Object(extra)) = extra_dm {
        dm.extend(extra);
    }

    let mut payload = request.data.clone();
    payload.insert("aps".into(), Json::Object(aps));
    payload.insert("dm".into(), Json::Object(dm));
    Json::Object(payload)
}

async fn device_stats(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let pipeline = vec![
        doc! { "$match": { "app_id": &app_id } },
        doc! { "$group": {
            "_id": { "active": "$active", "environment": "$apns_environment", "authorization": "$notification_authorization" },
            "devices": { "$sum": 1 },
        }},
        doc! { "$sort": { "devices": -1 } },
    ];
    let rows: Vec<Json> = state.db.devices.aggregate(pipeline).await.map_err(db_error)?.map_ok(to_json).try_collect().await.map_err(db_error)?;
    let topics = vec![
        doc! { "$match": { "app_id": &app_id, "active": true } },
        doc! { "$unwind": "$topics" },
        doc! { "$group": { "_id": "$topics", "devices": { "$sum": 1 } } },
        doc! { "$sort": { "devices": -1 } },
        doc! { "$limit": 100 },
    ];
    let topics: Vec<Json> = state.db.devices.aggregate(topics).await.map_err(db_error)?.map_ok(to_json).try_collect().await.map_err(db_error)?;
    ok(json!({ "app_id": app_id, "breakdown": rows, "topics": topics }))
}

// ── Crashlytics ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct IssueQuery {
    status: Option<String>,
    fatal: Option<bool>,
    version: Option<String>,
    limit: Option<i64>,
    /// "last_seen" (default) or "count"
    sort: Option<String>,
}

async fn list_issues(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<IssueQuery>) -> ApiResult {
    let mut filter = doc! { "app_id": &app_id };
    if let Some(status) = &query.status { filter.insert("status", status); }
    if let Some(fatal) = query.fatal { filter.insert("is_fatal", fatal); }
    if let Some(version) = &query.version { filter.insert("versions", version); }
    let sort = if query.sort.as_deref() == Some("count") { doc! { "count": -1 } } else { doc! { "last_seen": -1 } };
    let issues: Vec<Json> = state
        .db
        .crash_issues
        .find(filter)
        .sort(sort)
        .limit(query.limit.unwrap_or(50).clamp(1, 500))
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    ok(json!({ "app_id": app_id, "issues": issues }))
}

async fn get_issue(State(state): State<AppState>, Path(issue_id): Path<String>) -> ApiResult {
    let issue = state
        .db
        .crash_issues
        .find_one(doc! { "_id": &issue_id })
        .await
        .map_err(db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "no such issue"))?;
    let reports: Vec<Json> = state
        .db
        .crash_reports
        .find(doc! { "issue_id": &issue_id })
        .sort(doc! { "timestamp": -1 })
        .limit(20)
        .projection(doc! { "uploaded_by": 0, "metric_kit": 0, "metric_kit_raw": 0, "return_addresses": 0 })
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    let affected = state
        .db
        .crash_reports
        .aggregate(vec![
            doc! { "$match": { "issue_id": &issue_id } },
            doc! { "$group": { "_id": "$installation_id" } },
            doc! { "$count": "installations" },
        ])
        .await
        .map_err(db_error)?
        .try_next()
        .await
        .map_err(db_error)?
        .and_then(|d| d.get_i32("installations").ok())
        .unwrap_or(0);
    ok(json!({ "issue": to_json(issue), "affected_installations": affected, "recent_reports": reports }))
}

#[derive(Deserialize)]
struct IssueUpdate {
    status: String,
}

async fn update_issue(State(state): State<AppState>, Path(issue_id): Path<String>, AxumJson(update): AxumJson<IssueUpdate>) -> ApiResult {
    if !["open", "closed", "muted"].contains(&update.status.as_str()) {
        return Err(error(StatusCode::BAD_REQUEST, "status must be open, closed or muted"));
    }
    let result = state
        .db
        .crash_issues
        .update_one(
            doc! { "_id": &issue_id },
            doc! { "$set": { "status": &update.status, "status_changed_at": DateTime::now() } },
        )
        .await
        .map_err(db_error)?;
    if result.matched_count == 0 {
        return Err(error(StatusCode::NOT_FOUND, "no such issue"));
    }
    ok(json!({ "issue_id": issue_id, "status": update.status }))
}

// ── Release monitoring ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DaysQuery {
    days: Option<i64>,
}

pub(super) fn since(days: Option<i64>) -> DateTime {
    let days = days.unwrap_or(7).clamp(1, 365);
    DateTime::from_millis(DateTime::now().timestamp_millis() - days * 86_400_000)
}

/// Crash-free sessions = 1 − (distinct crashed launches / session_start events);
/// crash-free users = 1 − (installations with a fatal crash / installations with a session),
/// per app version, over the window.
async fn stability(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<DaysQuery>) -> ApiResult {
    let from = since(query.days);
    let sessions_pipeline = vec![
        doc! { "$match": { "app_id": &app_id, "received_at": { "$gte": from }, "events.name": "session_start" } },
        doc! { "$unwind": "$events" },
        doc! { "$match": { "events.name": "session_start" } },
        doc! { "$group": { "_id": { "v": "$app.version_name", "i": "$installation_id" }, "sessions": { "$sum": 1 } } },
        doc! { "$group": { "_id": "$_id.v", "sessions": { "$sum": "$sessions" }, "users": { "$sum": 1 } } },
    ];
    let crashes_pipeline = vec![
        doc! { "$match": { "app_id": &app_id, "is_fatal": true, "kind": { "$in": ["signal", "exception"] }, "timestamp": { "$gte": from } } },
        doc! { "$group": { "_id": { "v": "$app_version", "i": "$installation_id" }, "launches": { "$addToSet": "$launch_id" }, "crashes": { "$sum": 1 } } },
        doc! { "$group": { "_id": "$_id.v", "crashed_sessions": { "$sum": { "$size": "$launches" } }, "crashed_users": { "$sum": 1 }, "crashes": { "$sum": "$crashes" } } },
    ];

    let sessions: Vec<Document> = state.db.event_data.aggregate(sessions_pipeline).await.map_err(db_error)?.try_collect().await.map_err(db_error)?;
    let crashes: Vec<Document> = state.db.crash_reports.aggregate(crashes_pipeline).await.map_err(db_error)?.try_collect().await.map_err(db_error)?;

    let number = |d: &Document, k: &str| -> f64 {
        match d.get(k) {
            Some(Bson::Int32(v)) => f64::from(*v),
            Some(Bson::Int64(v)) => *v as f64,
            Some(Bson::Double(v)) => *v,
            _ => 0.0,
        }
    };
    let version_of = |d: &Document| d.get_str("_id").unwrap_or("unknown").to_string();

    let mut rows: HashMap<String, Map<String, Json>> = HashMap::new();
    for s in &sessions {
        let row = rows.entry(version_of(s)).or_default();
        row.insert("sessions".into(), json!(number(s, "sessions")));
        row.insert("users".into(), json!(number(s, "users")));
    }
    for c in &crashes {
        let row = rows.entry(version_of(c)).or_default();
        row.insert("crashed_sessions".into(), json!(number(c, "crashed_sessions")));
        row.insert("crashed_users".into(), json!(number(c, "crashed_users")));
        row.insert("crashes".into(), json!(number(c, "crashes")));
    }
    let mut versions: Vec<Json> = rows
        .into_iter()
        .map(|(version, mut row)| {
            let get = |r: &Map<String, Json>, k: &str| r.get(k).and_then(Json::as_f64).unwrap_or(0.0);
            let rate = |crashed: f64, total: f64| if total > 0.0 { json!(((1.0 - crashed / total).max(0.0) * 10_000.0).round() / 100.0) } else { Json::Null };
            let crash_free_sessions = rate(get(&row, "crashed_sessions"), get(&row, "sessions"));
            let crash_free_users = rate(get(&row, "crashed_users"), get(&row, "users"));
            row.insert("app_version".into(), json!(version));
            row.insert("crash_free_sessions_pct".into(), crash_free_sessions);
            row.insert("crash_free_users_pct".into(), crash_free_users);
            Json::Object(row)
        })
        .collect();
    versions.sort_by(|a, b| {
        let key = |v: &Json| v["app_version"].as_str().unwrap_or("").split('.').map(|p| p.parse::<u64>().unwrap_or(0)).collect::<Vec<_>>();
        key(b).cmp(&key(a))
    });
    ok(json!({ "app_id": app_id, "since": from.try_to_rfc3339_string().unwrap_or_default(), "versions": versions }))
}

/// Count, average and p50/p90 (MongoDB 7.0+ `$percentile`; on older servers percentiles are
/// omitted rather than failing).
async fn performance(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<DaysQuery>) -> ApiResult {
    let from = since(query.days);
    let base = |kind: &str| doc! { "$match": { "app_id": &app_id, "kind": kind, "start_time": { "$gte": from } } };

    let app_start = summarize(&state, vec![base("app_start")], doc! { "app_version": "$app_version" }, Document::new(), 20).await?;
    let network = summarize(
        &state,
        vec![base("network_request")],
        doc! { "url": "$network.url", "method": "$network.method" },
        doc! {
            "success_rate": { "$avg": { "$cond": ["$network.success", 1, 0] } },
            "avg_response_bytes": { "$avg": "$network.response_bytes" },
        },
        50,
    )
    .await?;
    let screens = summarize(
        &state,
        vec![base("screen_rendering")],
        doc! { "screen": "$name" },
        doc! {
            "avg_slow_rate": { "$avg": "$rendering.slow_rate" },
            "avg_frozen_rate": { "$avg": "$rendering.frozen_rate" },
            "frozen_frames": { "$sum": "$counters.frozen_frames" },
        },
        50,
    )
    .await?;
    let traces = summarize(&state, vec![base("trace")], doc! { "name": "$name" }, Document::new(), 50).await?;

    ok(json!({
        "app_id": app_id,
        "since": from.try_to_rfc3339_string().unwrap_or_default(),
        "app_start": app_start,
        "network": network,
        "screens": screens,
        "traces": traces,
    }))
}

async fn summarize(state: &AppState, stages: Vec<Document>, group_id: Document, extra: Document, limit: i64) -> Result<Vec<Json>, Response> {
    let build = |with_percentiles: bool| {
        let mut group = doc! {
            "_id": group_id.clone(),
            "count": { "$sum": 1 },
            "avg_ms": { "$avg": "$duration_ms" },
        };
        if with_percentiles {
            group.insert("percentiles_ms", doc! { "$percentile": { "input": "$duration_ms", "p": [0.5, 0.9], "method": "approximate" } });
        }
        group.extend(extra.clone());
        let mut pipeline = stages.clone();
        pipeline.push(doc! { "$group": group });
        pipeline.push(doc! { "$sort": { "count": -1 } });
        pipeline.push(doc! { "$limit": limit });
        pipeline
    };
    let run = |pipeline: Vec<Document>| async move {
        let cursor = state.db.perf_records.aggregate(pipeline).await?;
        cursor.map_ok(to_json).try_collect::<Vec<Json>>().await
    };
    match run(build(true)).await {
        Ok(rows) => Ok(rows),
        Err(_) => run(build(false)).await.map_err(db_error),
    }
}
