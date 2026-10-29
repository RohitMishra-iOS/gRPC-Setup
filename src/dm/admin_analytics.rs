//! Admin API — analytics + detail views used by the Next.js console (admin-console/).
//! Same auth as admin.rs (x-admin-key). All analytics read `event_data` (one document per
//! uploaded batch, `events[]` inside), filtered by `received_at` (indexed) and then by each
//! event's own `timestamp_ms`.
//!
//!   GET /admin/apps                                          app ids seen in any collection
//!   GET /admin/analytics/:app_id/overview?days=&tz=          users/sessions/events per day, top events, versions, devices
//!   GET /admin/analytics/:app_id/realtime                    users + events in the last 30 minutes
//!   GET /admin/analytics/:app_id/events?days=                every event name with counts and users
//!   GET /admin/analytics/:app_id/events/:name?days=&tz=      one event: per day, parameter values, recent occurrences
//!   GET /admin/analytics/:app_id/screens?days=               screen_view by screen
//!   GET /admin/analytics/:app_id/stream?limit=&installation_id=&event=   newest events (DebugView)
//!   GET /admin/analytics/:app_id/users/:installation_id      one user's properties, device and event timeline
//!   GET /admin/crashlytics/:app_id/trend?days=&tz=           crashes / non-fatals / affected users per day
//!   GET /admin/crashlytics/issues/:issue_id/trend?days=&tz=  one issue per day
//!   GET /admin/crashlytics/reports/:report_id                one full report
//!   GET /admin/messaging/:app_id/history                     sent push campaigns
//!
//! Analytics batches from SDKs before 2.0 carry no `app_id`; they are included for every app.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Value as Json};

use super::admin::{db_error, error, ok, since, to_json};
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

fn app_filter(app_id: &str) -> Document {
    doc! { "$or": [ { "app_id": app_id }, { "app_id": { "$exists": false } }, { "app_id": "" } ] }
}

/// Batch-level match + unwind + per-event time filter + computed `uid` / `day` fields.
fn event_stages(app_id: &str, from: DateTime, zone: &str, event_name: Option<&str>) -> Vec<Document> {
    let mut first = app_filter(app_id);
    // received_at can lag the event time (offline queues), so look back one extra day.
    first.insert("received_at", doc! { "$gte": DateTime::from_millis(from.timestamp_millis() - 86_400_000) });
    if let Some(name) = event_name {
        first.insert("events.name", name);
    }
    let mut after = doc! { "events.timestamp_ms": { "$gte": from.timestamp_millis() } };
    if let Some(name) = event_name {
        after.insert("events.name", name);
    }
    vec![
        doc! { "$match": first },
        doc! { "$unwind": "$events" },
        doc! { "$match": after },
        doc! { "$addFields": {
            "uid": { "$ifNull": ["$installation_id", "$instance_id"] },
            "day": { "$dateToString": { "format": "%Y-%m-%d", "date": { "$toDate": "$events.timestamp_ms" }, "timezone": zone } },
        }},
    ]
}

async fn aggregate(state: &AppState, pipeline: Vec<Document>) -> Result<Vec<Document>, Response> {
    state.db.event_data.aggregate(pipeline).await.map_err(db_error)?.try_collect().await.map_err(db_error)
}

fn rows(docs: Vec<Document>) -> Vec<Json> {
    docs.into_iter().map(to_json).collect()
}

// ── Apps ─────────────────────────────────────────────────────────────────────

pub async fn apps(State(state): State<AppState>) -> ApiResult {
    let mut ids = std::collections::BTreeSet::new();
    for coll in [&state.db.crash_issues, &state.db.devices, &state.db.rc_templates] {
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

pub async fn overview(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let mut pipeline = event_stages(&app_id, from, &zone, None);
    pipeline.push(doc! { "$facet": {
        "daily": [
            { "$group": {
                "_id": "$day",
                "events": { "$sum": 1 },
                "users": { "$addToSet": "$uid" },
                "sessions": { "$sum": { "$cond": [{ "$eq": ["$events.name", "session_start"] }, 1, 0] } },
                "new_users": { "$sum": { "$cond": [{ "$eq": ["$events.name", "first_open"] }, 1, 0] } },
                "engagement_ms": { "$sum": { "$cond": [{ "$eq": ["$events.name", "user_engagement"] }, { "$ifNull": ["$events.params.engagement_time_msec", 0] }, 0] } },
            }},
            { "$project": { "_id": 0, "day": "$_id", "events": 1, "sessions": 1, "new_users": 1, "engagement_ms": 1, "users": { "$size": "$users" } } },
            { "$sort": { "day": 1 } },
        ],
        "totals": [
            { "$group": {
                "_id": null,
                "events": { "$sum": 1 },
                "users": { "$addToSet": "$uid" },
                "sessions": { "$sum": { "$cond": [{ "$eq": ["$events.name", "session_start"] }, 1, 0] } },
                "new_users": { "$sum": { "$cond": [{ "$eq": ["$events.name", "first_open"] }, 1, 0] } },
                "engagement_ms": { "$sum": { "$cond": [{ "$eq": ["$events.name", "user_engagement"] }, { "$ifNull": ["$events.params.engagement_time_msec", 0] }, 0] } },
            }},
            { "$project": { "_id": 0, "events": 1, "sessions": 1, "new_users": 1, "engagement_ms": 1, "users": { "$size": "$users" } } },
        ],
        "top_events": [
            { "$group": { "_id": "$events.name", "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "name": "$_id", "count": 1, "users": { "$size": "$users" } } },
            { "$sort": { "count": -1 } },
            { "$limit": 10 },
        ],
        "versions": [
            { "$group": { "_id": "$app.version_name", "users": { "$addToSet": "$uid" }, "events": { "$sum": 1 } } },
            { "$project": { "_id": 0, "version": "$_id", "events": 1, "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
        "devices": [
            { "$group": { "_id": "$device.model", "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "model": "$_id", "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
        "os_versions": [
            { "$group": { "_id": "$device.os_version", "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "os_version": "$_id", "users": { "$size": "$users" } } },
            { "$sort": { "users": -1 } },
            { "$limit": 10 },
        ],
    }});
    let mut result = aggregate(&state, pipeline).await?;
    let facet = result.pop().map(to_json).unwrap_or(json!({}));
    let totals = facet["totals"].get(0).cloned().unwrap_or(json!({ "events": 0, "users": 0, "sessions": 0, "new_users": 0, "engagement_ms": 0 }));
    ok(json!({
        "app_id": app_id,
        "since": from.try_to_rfc3339_string().unwrap_or_default(),
        "tz": zone,
        "totals": totals,
        "daily": facet["daily"],
        "top_events": facet["top_events"],
        "versions": facet["versions"],
        "devices": facet["devices"],
        "os_versions": facet["os_versions"],
    }))
}

pub async fn realtime(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let from = DateTime::from_millis(DateTime::now().timestamp_millis() - 30 * 60_000);
    let mut pipeline = event_stages(&app_id, from, "UTC", None);
    pipeline[0] = doc! { "$match": { "$and": [app_filter(&app_id), { "received_at": { "$gte": from } }] } };
    pipeline.push(doc! { "$facet": {
        "totals": [
            { "$group": { "_id": null, "events": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "events": 1, "users": { "$size": "$users" } } },
        ],
        "per_minute": [
            { "$group": {
                "_id": { "$dateToString": { "format": "%H:%M", "date": { "$toDate": "$events.timestamp_ms" } } },
                "events": { "$sum": 1 }, "users": { "$addToSet": "$uid" },
            }},
            { "$project": { "_id": 0, "minute": "$_id", "events": 1, "users": { "$size": "$users" } } },
            { "$sort": { "minute": 1 } },
        ],
        "top_events": [
            { "$group": { "_id": "$events.name", "count": { "$sum": 1 } } },
            { "$project": { "_id": 0, "name": "$_id", "count": 1 } },
            { "$sort": { "count": -1 } },
            { "$limit": 8 },
        ],
    }});
    let facet = aggregate(&state, pipeline).await?.pop().map(to_json).unwrap_or(json!({}));
    ok(json!({
        "app_id": app_id,
        "totals": facet["totals"].get(0).cloned().unwrap_or(json!({ "events": 0, "users": 0 })),
        "per_minute": facet["per_minute"],
        "top_events": facet["top_events"],
    }))
}

pub async fn events(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let mut pipeline = event_stages(&app_id, from, &zone, None);
    pipeline.push(doc! { "$group": {
        "_id": "$events.name",
        "count": { "$sum": 1 },
        "users": { "$addToSet": "$uid" },
        "last_seen_ms": { "$max": "$events.timestamp_ms" },
    }});
    pipeline.push(doc! { "$project": { "_id": 0, "name": "$_id", "count": 1, "last_seen_ms": 1, "users": { "$size": "$users" } } });
    pipeline.push(doc! { "$sort": { "count": -1 } });
    pipeline.push(doc! { "$limit": 500 });
    let events = rows(aggregate(&state, pipeline).await?);
    ok(json!({ "app_id": app_id, "since": from.try_to_rfc3339_string().unwrap_or_default(), "events": events }))
}

pub async fn event_detail(State(state): State<AppState>, Path((app_id, name)): Path<(String, String)>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let mut pipeline = event_stages(&app_id, from, &zone, Some(&name));
    pipeline.push(doc! { "$facet": {
        "daily": [
            { "$group": { "_id": "$day", "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "day": "$_id", "count": 1, "users": { "$size": "$users" } } },
            { "$sort": { "day": 1 } },
        ],
        "totals": [
            { "$group": { "_id": null, "count": { "$sum": 1 }, "users": { "$addToSet": "$uid" } } },
            { "$project": { "_id": 0, "count": 1, "users": { "$size": "$users" } } },
        ],
        "params": [
            { "$project": { "p": { "$objectToArray": { "$ifNull": ["$events.params", {}] } } } },
            { "$unwind": "$p" },
            { "$group": {
                "_id": { "k": "$p.k", "v": { "$convert": { "input": "$p.v", "to": "string", "onError": "(object)", "onNull": "null" } } },
                "count": { "$sum": 1 },
            }},
            { "$sort": { "count": -1 } },
            { "$group": { "_id": "$_id.k", "total": { "$sum": "$count" }, "distinct_values": { "$sum": 1 }, "values": { "$push": { "value": "$_id.v", "count": "$count" } } } },
            { "$project": { "_id": 0, "key": "$_id", "total": 1, "distinct_values": 1, "values": { "$slice": ["$values", 10] } } },
            { "$sort": { "total": -1 } },
            { "$limit": 30 },
        ],
        "recent": [
            { "$sort": { "events.timestamp_ms": -1 } },
            { "$limit": 25 },
            { "$project": {
                "_id": 0, "timestamp_ms": "$events.timestamp_ms", "params": "$events.params", "session_id": "$events.session_id",
                "installation_id": "$uid", "user_id": 1, "screen_name": 1, "app_version": "$app.version_name", "device_model": "$device.model",
            }},
        ],
    }});
    let facet = aggregate(&state, pipeline).await?.pop().map(to_json).unwrap_or(json!({}));
    ok(json!({
        "app_id": app_id,
        "name": name,
        "since": from.try_to_rfc3339_string().unwrap_or_default(),
        "totals": facet["totals"].get(0).cloned().unwrap_or(json!({ "count": 0, "users": 0 })),
        "daily": facet["daily"],
        "params": facet["params"],
        "recent": facet["recent"],
    }))
}

pub async fn screens(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<RangeQuery>) -> ApiResult {
    let zone = tz(&query)?;
    let from = since(query.days);
    let mut pipeline = event_stages(&app_id, from, &zone, Some("screen_view"));
    pipeline.push(doc! { "$group": {
        "_id": { "name": "$events.params.screen_name", "class": "$events.params.screen_class" },
        "views": { "$sum": 1 },
        "users": { "$addToSet": "$uid" },
    }});
    pipeline.push(doc! { "$project": { "_id": 0, "screen_name": "$_id.name", "screen_class": "$_id.class", "views": 1, "users": { "$size": "$users" } } });
    pipeline.push(doc! { "$sort": { "views": -1 } });
    pipeline.push(doc! { "$limit": 100 });
    let screens = rows(aggregate(&state, pipeline).await?);
    ok(json!({ "app_id": app_id, "screens": screens }))
}

#[derive(Deserialize)]
pub struct StreamQuery {
    limit: Option<i64>,
    installation_id: Option<String>,
    event: Option<String>,
}

pub async fn stream(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<StreamQuery>) -> ApiResult {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let mut first = app_filter(&app_id);
    if let Some(id) = query.installation_id.as_deref().filter(|s| !s.is_empty()) {
        first = doc! { "$and": [first, { "$or": [{ "installation_id": id }, { "instance_id": id }] }] };
    }
    let mut pipeline = vec![
        doc! { "$match": first },
        doc! { "$sort": { "received_at": -1 } },
        doc! { "$limit": 200 },
        doc! { "$unwind": "$events" },
    ];
    if let Some(name) = query.event.as_deref().filter(|s| !s.is_empty()) {
        pipeline.push(doc! { "$match": { "events.name": name } });
    }
    pipeline.push(doc! { "$sort": { "events.timestamp_ms": -1 } });
    pipeline.push(doc! { "$limit": limit });
    pipeline.push(doc! { "$project": {
        "_id": 0,
        "name": "$events.name",
        "params": "$events.params",
        "timestamp_ms": "$events.timestamp_ms",
        "session_id": "$events.session_id",
        "installation_id": { "$ifNull": ["$installation_id", "$instance_id"] },
        "user_id": 1, "screen_name": 1, "user_properties": 1,
        "app_version": "$app.version_name", "device_model": "$device.model", "os_version": "$device.os_version",
        "received_at": 1,
    }});
    let events = rows(aggregate(&state, pipeline).await?);
    ok(json!({ "app_id": app_id, "events": events }))
}

pub async fn user_detail(State(state): State<AppState>, Path((app_id, installation_id)): Path<(String, String)>) -> ApiResult {
    let filter = doc! { "$and": [app_filter(&app_id), { "$or": [{ "installation_id": &installation_id }, { "instance_id": &installation_id }] }] };
    let latest = state
        .db
        .event_data
        .find_one(filter.clone())
        .sort(doc! { "received_at": -1 })
        .projection(doc! { "events": 0 })
        .await
        .map_err(db_error)?;
    let first = state
        .db
        .event_data
        .find_one(filter.clone())
        .sort(doc! { "received_at": 1 })
        .projection(doc! { "received_at": 1 })
        .await
        .map_err(db_error)?;
    let timeline = rows(aggregate(&state, vec![
        doc! { "$match": filter },
        doc! { "$sort": { "received_at": -1 } },
        doc! { "$limit": 100 },
        doc! { "$unwind": "$events" },
        doc! { "$sort": { "events.timestamp_ms": -1 } },
        doc! { "$limit": 300 },
        doc! { "$project": { "_id": 0, "name": "$events.name", "params": "$events.params", "timestamp_ms": "$events.timestamp_ms", "session_id": "$events.session_id", "screen_name": 1, "app_version": "$app.version_name" } },
    ]).await?);
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
    if latest.is_none() && crashes.is_empty() && device.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "no data for this installation"));
    }
    ok(json!({
        "app_id": app_id,
        "installation_id": installation_id,
        "first_seen": first.and_then(|d| d.get_datetime("received_at").ok().and_then(|t| t.try_to_rfc3339_string().ok())),
        "latest_batch": latest.map(to_json),
        "push_registration": device.map(to_json),
        "timeline": timeline,
        "crashes": crashes,
    }))
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
