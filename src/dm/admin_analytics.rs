//! Admin API — analytics + detail views used by the Next.js console (admin-console/).
//! Same auth as admin.rs (x-admin-key). Analytics read the GA4 daily tables
//! `analytics_<app>.events_YYYYMMDD` (dm-specs TBL-014) for the requested range and answer in the
//! same JSON shapes as before, so the console pages did not change.
//!
//!   GET /admin/apps                                          app ids seen in any collection
//!   GET /admin/analytics/:app_id/overview?days=&tz=          users/sessions/events per day, top events, versions, devices
//!   GET /admin/analytics/:app_id/realtime                    users + events in the last 30 minutes
//!   GET /admin/analytics/:app_id/events?days=                every event name with counts and users
//!   GET /admin/analytics/:app_id/events/:name?days=&tz=      one event: per day, parameter values, recent occurrences
//!   GET /admin/analytics/:app_id/events-report?start=&end=&tz=&granularity=&series=&platform=   GA4 Events report (dm/events_report.rs)
//!   GET /admin/analytics/:app_id/screens?days=               screen_view by firebase_screen
//!   GET /admin/analytics/:app_id/stream?limit=&installation_id=&event=   newest events (DebugView)
//!   GET /admin/analytics/:app_id/users/:installation_id      one user's profile, device and event timeline
//!   (tables / export / schema / migrate-legacy: dm/analytics/export.rs)
//!   GET /admin/crashlytics/:app_id/trend?days=&tz=           crashes / non-fatals / affected users per day
//!   GET /admin/crashlytics/issues/:issue_id/trend?days=&tz=  one issue per day
//!   GET /admin/crashlytics/reports/:report_id                one full report
//!   GET /admin/messaging/:app_id/history                     sent push campaigns

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Value as Json};

use super::admin::{db_error, error, ok, since, to_json};
use super::analytics::tables;
use super::AppState;

type ApiResult = Result<Response, Response>;

#[derive(Deserialize)]
pub struct RangeQuery {
    days: Option<i64>,
    tz: Option<String>,
}

/// IANA zone for day bucketing (validated loosely; MongoDB rejects unknown zones).
fn tz(query: &RangeQuery) -> Result<String, Response> {
    let zone = query.tz.clone().unwrap_or_else(|| "UTC".into());
    if zone.len() > 64 || !zone.chars().all(|c| c.is_ascii_alphanumeric() || "/_+-".contains(c)) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid tz"));
    }
    Ok(zone)
}

/// The typed value of event parameter `key` (whichever of the four fields is set).
pub fn param(key: &str) -> Bson {
    Bson::Document(doc! { "$let": {
        "vars": { "p": { "$first": { "$filter": { "input": { "$ifNull": ["$event_params", []] }, "as": "p", "cond": { "$eq": ["$$p.key", key] } } } } },
        "in": { "$ifNull": ["$$p.value.string_value", "$$p.value.int_value", "$$p.value.double_value", "$$p.value.float_value"] },
    }})
}

/// `event_params` as a plain `{key: value}` object (the console's params view).
fn params_object() -> Bson {
    Bson::Document(doc! { "$arrayToObject": { "$map": {
        "input": { "$ifNull": ["$event_params", []] },
        "as": "p",
        "in": { "k": "$$p.key", "v": { "$ifNull": ["$$p.value.string_value", "$$p.value.int_value", "$$p.value.double_value", "$$p.value.float_value"] } },
    }}})
}

fn uid() -> Bson {
    Bson::Document(doc! { "$ifNull": ["$user_pseudo_id", "$user_id"] })
}

/// event_timestamp (µs) → epoch ms.
fn ms() -> Bson {
    Bson::Document(doc! { "$toLong": { "$divide": ["$event_timestamp", 1000] } })
}

fn model() -> Bson {
    Bson::Document(doc! { "$ifNull": ["$device.mobile_marketing_name", "$device.mobile_os_hardware_model", "$device.mobile_model_name", "$device.category"] })
}

/// Runs `stages` over every event row since `from` (plus `extra` match conditions), adding `uid`,
/// `ms` and (with a zone) `day`.
async fn rows(state: &AppState, app_id: &str, from: DateTime, zone: Option<&str>, extra: Document, stages: Vec<Document>) -> Result<Vec<Document>, Response> {
    let from_ms = from.timestamp_millis();
    let names = tables::event_tables_since(app_id, from_ms, state.config.analytics.timezone);
    let mut first = doc! { "event_timestamp": { "$gte": from_ms * 1000 } };
    first.extend(extra);
    let mut fields = doc! { "uid": uid(), "ms": ms() };
    if let Some(zone) = zone {
        fields.insert("day", doc! { "$dateToString": { "format": "%Y-%m-%d", "date": { "$toDate": ms() }, "timezone": zone } });
    }
    let mut pipeline = vec![doc! { "$addFields": fields }];
    pipeline.extend(stages);
    tables::aggregate_union(&state.db, &names, first, pipeline).await.map_err(db_error)
}

fn list(docs: Vec<Document>) -> Vec<Json> {
    docs.into_iter().map(to_json).collect()
}

// ── Apps ─────────────────────────────────────────────────────────────────────

pub async fn apps(State(state): State<AppState>) -> ApiResult {
    let mut ids = std::collections::BTreeSet::new();
    for coll in [&state.db.crash_issues, &state.db.devices, &state.db.rc_templates, &state.db.analytics_users] {
        if let Ok(values) = coll.distinct("app_id", doc! {}).await {
            ids.extend(values.into_iter().filter_map(|v| v.as_str().map(str::to_string)));
        }
    }
    let recent = DateTime::from_millis(DateTime::now().timestamp_millis() - 30 * 86_400_000);
    if let Ok(values) = state.db.event_data.distinct("app_id", doc! { "received_at": { "$gte": recent } }).await {
        ids.extend(values.into_iter().filter_map(|v| v.as_str().map(str::to_string)));
    }
    ids.remove("");
    ok(json!({ "apps": ids }))
}

// ── Analytics ────────────────────────────────────────────────────────────────

fn totals_group() -> Document {
    doc! {
        "events": { "$sum": 1 },
        "users": { "$addToSet": "$uid" },
        "sessions": { "$sum": { "$cond": [{ "$eq": ["$event_name", "session_start"] }, 1, 0] } },
        "new_users": { "$sum": { "$cond": [{ "$in": ["$event_name", ["first_open", "first_visit"]] }, 1, 0] } },
        "engagement_ms": { "$sum": { "$cond": [{ "$eq": ["$event_name", "user_engagement"] }, { "$ifNull": [param("engagement_time_msec"), 0] }, 0] } },
    }
}

pub async fn overview(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let mut daily = doc! { "_id": "$day" };
    daily.extend(totals_group());
    let mut totals = doc! { "_id": Bson::Null };
    totals.extend(totals_group());
    let facet = doc! { "$facet": {
        "daily": [
            { "$group": daily },
            { "$project": { "_id": 0, "day": "$_id", "events": 1, "sessions": 1, "new_users": 1, "engagement_ms": 1, "users": { "$size": "$users" } } },
            { "$sort": { "day": 1 } },
        ],
        "totals": [
            { "$group": totals },
            { "$project": { "_id": 0, "events": 1, "sessions": 1, "new_users": 1, "engagement_ms": 1, "users": { "$size": "$users" } } },
        ],
        "top_events": [
            { "$group": { "_id": "$event_name", "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "name": "$_id", "count": 1, "users": { "$size": "$users" } } },
            { "$sort": { "count": -1 } },
            { "$limit": 10 },
        ],
        "versions": [
            { "$group": { "_id": "$app_info.version", "users": { "$addToSet": "$uid" }, "events": { "$sum": 1 } } },
            { "$project": { "_id": 0, "version": "$_id", "events": 1, "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
        "devices": [
            { "$group": { "_id": model(), "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "model": "$_id", "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
        "os_versions": [
            { "$group": { "_id": "$device.operating_system_version", "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "os_version": "$_id", "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
    }};
    let facet = rows(&state, &app_id, from, Some(&zone), doc! {}, vec![facet]).await?.pop().map(to_json).unwrap_or(json!({}));
    ok(json!({
        "app_id": app_id,
        "since": from.try_to_rfc3339_string().unwrap_or_default(),
        "tz": zone,
        "totals": facet["totals"].get(0).cloned().unwrap_or(json!({ "events": 0, "users": 0, "sessions": 0, "new_users": 0, "engagement_ms": 0 })),
        "daily": facet.get("daily").cloned().unwrap_or(json!([])),
        "top_events": facet.get("top_events").cloned().unwrap_or(json!([])),
        "versions": facet.get("versions").cloned().unwrap_or(json!([])),
        "devices": facet.get("devices").cloned().unwrap_or(json!([])),
        "os_versions": facet.get("os_versions").cloned().unwrap_or(json!([])),
    }))
}

pub async fn realtime(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let from = DateTime::from_millis(DateTime::now().timestamp_millis() - 30 * 60_000);
    let facet = doc! { "$facet": {
        "totals": [
            { "$group": { "_id": Bson::Null, "events": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "events": 1, "users": { "$size": "$users" } } },
        ],
        "per_minute": [
            { "$group": {
                "_id": { "$dateToString": { "format": "%H:%M", "date": { "$toDate": "$ms" } } },
                "events": { "$sum": 1 }, "users": { "$addToSet": "$uid" },
            }},
            { "$project": { "_id": 0, "minute": "$_id", "events": 1, "users": { "$size": "$users" } } },
            { "$sort": { "minute": 1 } },
        ],
        "top_events": [
            { "$group": { "_id": "$event_name", "count": { "$sum": 1 } } },
            { "$project": { "_id": 0, "name": "$_id", "count": 1 } },
            { "$sort": { "count": -1 } },
            { "$limit": 8 },
        ],
    }};
    let facet = rows(&state, &app_id, from, None, doc! {}, vec![facet]).await?.pop().map(to_json).unwrap_or(json!({}));
    ok(json!({
        "app_id": app_id,
        "totals": facet["totals"].get(0).cloned().unwrap_or(json!({ "events": 0, "users": 0 })),
        "per_minute": facet.get("per_minute").cloned().unwrap_or(json!([])),
        "top_events": facet.get("top_events").cloned().unwrap_or(json!([])),
    }))
}

pub async fn events(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let from = since(query.days);
    let stages = vec![
        doc! { "$group": { "_id": "$event_name", "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" }, "last_seen_ms": { "$max": "$ms" } } },
        doc! { "$project": { "_id": 0, "name": "$_id", "count": 1, "last_seen_ms": 1, "users": { "$size": "$users" } } },
        doc! { "$sort": { "count": -1 } },
        doc! { "$limit": 500 },
    ];
    let events = list(rows(&state, &app_id, from, None, doc! {}, stages).await?);
    ok(json!({ "app_id": app_id, "since": from.try_to_rfc3339_string().unwrap_or_default(), "events": events }))
}

/// One event row in the console's StreamEvent shape.
fn stream_projection() -> Document {
    doc! {
        "_id": 0,
        "name": "$event_name",
        "params": params_object(),
        "timestamp_ms": "$ms",
        "session_id": param("ga_session_id"),
        "installation_id": "$uid",
        "user_id": 1,
        "screen_name": param("firebase_screen"),
        "app_version": "$app_info.version",
        "device_model": model(),
        "os_version": "$device.operating_system_version",
        "platform": 1,
        "user_properties": { "$arrayToObject": { "$map": {
            "input": { "$ifNull": ["$user_properties", []] },
            "as": "p",
            "in": { "k": "$$p.key", "v": { "$ifNull": ["$$p.value.string_value", "$$p.value.int_value", "$$p.value.double_value", "$$p.value.float_value"] } },
        }}},
        "debug": { "$eq": [param("debug_mode"), 1] },
    }
}

pub async fn event_detail(State(state): State<AppState>, Path((app_id, name)): Path<(String, String)>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let facet = doc! { "$facet": {
        "daily": [
            { "$group": { "_id": "$day", "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "day": "$_id", "count": 1, "users": { "$size": "$users" } } },
            { "$sort": { "day": 1 } },
        ],
        "totals": [
            { "$group": { "_id": Bson::Null, "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "count": 1, "users": { "$size": "$users" } } },
        ],
        "params": [
            { "$unwind": "$event_params" },
            { "$group": {
                "_id": {
                    "k": "$event_params.key",
                    "v": { "$convert": {
                        "input": { "$ifNull": ["$event_params.value.string_value", "$event_params.value.int_value", "$event_params.value.double_value", "$event_params.value.float_value"] },
                        "to": "string", "onError": "(object)", "onNull": "null",
                    }},
                },
                "count": { "$sum": 1 },
            }},
            { "$sort": { "count": -1 } },
            { "$group": { "_id": "$_id.k", "total": { "$sum": "$count" }, "distinct_values": { "$sum": 1 }, "values": { "$push": { "value": "$_id.v", "count": "$count" } } } },
            { "$project": { "_id": 0, "key": "$_id", "total": 1, "distinct_values": 1, "values": { "$slice": ["$values", 10] } } },
            { "$sort": { "total": -1 } },
            { "$limit": 30 },
        ],
        "recent": [
            { "$sort": { "event_timestamp": -1 } },
            { "$limit": 25 },
            { "$project": stream_projection() },
        ],
    }};
    let facet = rows(&state, &app_id, from, Some(&zone), doc! { "event_name": &name }, vec![facet]).await?.pop().map(to_json).unwrap_or(json!({}));
    ok(json!({
        "app_id": app_id,
        "name": name,
        "since": from.try_to_rfc3339_string().unwrap_or_default(),
        "totals": facet["totals"].get(0).cloned().unwrap_or(json!({ "count": 0, "users": 0 })),
        "daily": facet.get("daily").cloned().unwrap_or(json!([])),
        "params": facet.get("params").cloned().unwrap_or(json!([])),
        "recent": facet.get("recent").cloned().unwrap_or(json!([])),
    }))
}

pub async fn screens(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let from = since(query.days);
    let stages = vec![
        doc! { "$group": { "_id": { "name": param("firebase_screen"), "class": param("firebase_screen_class") }, "views": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
        doc! { "$project": { "_id": 0, "screen_name": "$_id.name", "screen_class": "$_id.class", "views": 1, "users": { "$size": "$users" } } },
        doc! { "$sort": { "views": -1 } },
        doc! { "$limit": 100 },
    ];
    let screens = list(rows(&state, &app_id, from, None, doc! { "event_name": "screen_view" }, stages).await?);
    ok(json!({ "app_id": app_id, "screens": screens }))
}

#[derive(Deserialize)]
pub struct StreamQuery {
    limit: Option<i64>,
    installation_id: Option<String>,
    event: Option<String>,
}

/// The pseudo ids that belong to a DM installation id (iOS keeps a separate analytics instance id).
async fn pseudo_ids(state: &AppState, app_id: &str, installation_id: &str) -> Result<Vec<Bson>, Response> {
    let mut ids = vec![Bson::String(installation_id.to_string())];
    let profile = state.db.analytics_users.find_one(doc! { "app_id": app_id, "installation_id": installation_id }).await.map_err(db_error)?;
    if let Some(pseudo) = profile.as_ref().and_then(|p| p.get_str("user_pseudo_id").ok()) {
        ids.push(Bson::String(pseudo.to_string()));
    }
    Ok(ids)
}

pub async fn stream(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<StreamQuery>) -> ApiResult {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let mut extra = doc! {};
    if let Some(id) = query.installation_id.as_deref().filter(|s| !s.is_empty()) {
        extra.insert("user_pseudo_id", doc! { "$in": pseudo_ids(&state, &app_id, id).await? });
    }
    if let Some(name) = query.event.as_deref().filter(|s| !s.is_empty()) {
        extra.insert("event_name", name);
    }
    let from = DateTime::from_millis(DateTime::now().timestamp_millis() - 2 * 86_400_000);
    let stages = vec![doc! { "$sort": { "event_timestamp": -1 } }, doc! { "$limit": limit }, doc! { "$project": stream_projection() }];
    let events = list(rows(&state, &app_id, from, None, extra, stages).await?);
    ok(json!({ "app_id": app_id, "events": events }))
}

pub async fn user_detail(State(state): State<AppState>, Path((app_id, installation_id)): Path<(String, String)>) -> ApiResult {
    let profile = state.db.analytics_users.find_one(doc! { "app_id": &app_id, "installation_id": &installation_id }).await.map_err(db_error)?;
    let ids = pseudo_ids(&state, &app_id, &installation_id).await?;
    let from = DateTime::from_millis(DateTime::now().timestamp_millis() - 30 * 86_400_000);
    let stages = vec![doc! { "$sort": { "event_timestamp": -1 } }, doc! { "$limit": 300 }, doc! { "$project": stream_projection() }];
    let timeline = list(rows(&state, &app_id, from, None, doc! { "user_pseudo_id": { "$in": ids } }, stages).await?);
    let crashes: Vec<Json> = state
        .db
        .crash_reports
        .find(doc! { "app_id": &app_id, "installation_id": &installation_id })
        .sort(doc! { "timestamp": -1 })
        .limit(20)
        .projection(doc! { "_id": 1, "kind": 1, "is_fatal": 1, "timestamp": 1, "issue_id": 1, "exception_name": 1, "signal.name": 1, "app_version": 1 })
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    let device = state.db.devices.find_one(doc! { "app_id": &app_id, "installation_id": &installation_id }).await.map_err(db_error)?;
    if profile.is_none() && timeline.is_empty() && crashes.is_empty() && device.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "no data for this installation"));
    }
    // The profile in the shape the console's user page reads (`latest_batch`).
    let latest = profile.as_ref().map(|p| {
        let context = p.get_document("context").cloned().unwrap_or_default();
        let text = |k: &str| context.get_str(k).unwrap_or("").to_string();
        let d = p.get_document("device").cloned().unwrap_or_default();
        let dtext = |k: &str| d.get_str(k).ok().map(str::to_string);
        json!({
            "user_id": p.get_str("user_id").ok(),
            "user_pseudo_id": p.get_str("user_pseudo_id").ok(),
            "user_properties": p.get_document("user_properties").ok().cloned().map(to_json).unwrap_or(json!({})),
            "received_at": p.get_datetime("last_seen").ok().and_then(|t| t.try_to_rfc3339_string().ok()),
            "app": { "version_name": text("app_version"), "version_code": text("app_build"), "bundle_id": text("bundle_id") },
            "device": {
                "model": dtext("mobile_marketing_name").or_else(|| dtext("mobile_os_hardware_model")).unwrap_or_else(|| text("device_model")),
                "os_version": dtext("operating_system_version").unwrap_or_else(|| text("os_version")),
                "locale": text("locale"),
                "timezone": text("timezone"),
                "network_type": text("network_type"),
            },
            "platform": text("platform"),
            "ltv": {
                "revenue": p.get("ltv_revenue").cloned().map(|b| b.into_relaxed_extjson()),
                "currency": state.config.analytics.currency,
                "sessions": p.get("sessions").cloned().map(|b| b.into_relaxed_extjson()),
                "purchases": p.get("purchases").cloned().map(|b| b.into_relaxed_extjson()),
            },
            "first_source": p.get_document("first_source").ok().cloned().map(to_json),
        })
    });
    ok(json!({
        "app_id": app_id,
        "installation_id": installation_id,
        "first_seen": profile.as_ref().and_then(|d| d.get_datetime("first_seen").ok().and_then(|t| t.try_to_rfc3339_string().ok())),
        "latest_batch": latest,
        "push_registration": device.map(to_json),
        "timeline": timeline,
        "crashes": crashes,
    }))
}

/// session_start rows per (app version, user) since `from`, for crash-free statistics (admin.rs).
pub async fn sessions_by_version(state: &AppState, app_id: &str, from: DateTime) -> Result<Vec<Document>, Response> {
    let stages = vec![
        doc! { "$group": { "_id": { "v": "$app_info.version", "i": "$uid" }, "sessions": { "$sum": 1 } } },
        doc! { "$group": { "_id": "$_id.v", "sessions": { "$sum": "$sessions" }, "users": { "$sum": 1 } } },
    ];
    rows(state, app_id, from, None, doc! { "event_name": "session_start" }, stages).await
}

// ── Crashlytics ──────────────────────────────────────────────────────────────

async fn crash_trend(state: &AppState, mut filter: Document, query: &RangeQuery) -> Result<Json, Response> {
    let zone = tz(query)?;
    let from = since(query.days);
    filter.insert("timestamp", doc! { "$gte": from });
    let pipeline = vec![
        doc! { "$match": filter },
        doc! { "$group": {
            "_id": { "$dateToString": { "format": "%Y-%m-%d", "date": "$timestamp", "timezone": &zone } },
            "fatal": { "$sum": { "$cond": ["$is_fatal", 1, 0] } },
            "non_fatal": { "$sum": { "$cond": ["$is_fatal", 0, 1] } },
            "users": { "$addToSet": "$installation_id" },
        }},
        doc! { "$project": { "_id": 0, "day": "$_id", "fatal": 1, "non_fatal": 1, "users": { "$size": "$users" } } },
        doc! { "$sort": { "day": 1 } },
    ];
    let daily: Vec<Json> = state
        .db
        .crash_reports
        .aggregate(pipeline)
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    Ok(json!({ "since": from.try_to_rfc3339_string().unwrap_or_default(), "tz": zone, "daily": daily }))
}

pub async fn crashlytics_trend(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let trend = crash_trend(&state, doc! { "app_id": &app_id }, &query).await?;
    ok(trend)
}

pub async fn issue_trend(State(state): State<AppState>, Path(issue_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let trend = crash_trend(&state, doc! { "issue_id": &issue_id }, &query).await?;
    ok(trend)
}

pub async fn report_detail(State(state): State<AppState>, Path(report_id): Path<String>) -> ApiResult {
    let report = state
        .db
        .crash_reports
        .find_one(doc! { "_id": &report_id })
        .await
        .map_err(db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "no such report"))?;
    ok(json!({ "report": to_json(report) }))
}

// ── Messaging ────────────────────────────────────────────────────────────────

pub async fn push_history(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let messages: Vec<Json> = state
        .db
        .push_messages
        .find(doc! { "app_id": &app_id })
        .sort(doc! { "created_at": -1 })
        .limit(100)
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    ok(json!({ "app_id": app_id, "messages": messages }))
}
