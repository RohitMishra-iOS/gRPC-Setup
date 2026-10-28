//! Events batch ingest
//! ─────────────────────────────────────────────────────────────────────────────
//! Accepts an analytics-style event batch as JSON:
//!   { events: [{ timestamp_ms, session_id, name, params }, ...], instance_id, ... }
//! Validation: every event must have a numeric timestamp_ms and a non-blank name.

use serde_json::{json, Value as Json};

use crate::db::Db;

pub fn validate_events_batch(body: &Json) -> Vec<String> {
    if !(body.is_object() || body.is_array()) {
        return vec!["Body must be a JSON object".into()];
    }
    let Some(events) = body.get("events").and_then(Json::as_array) else {
        return vec![r#""events" must be an array"#.into()];
    };

    let mut errors = Vec::new();
    for (i, event) in events.iter().enumerate() {
        if !event.get("timestamp_ms").is_some_and(Json::is_number) {
            errors.push(format!("events[{i}].timestamp_ms must be a number"));
        }
        let name = event.get("name").and_then(Json::as_str);
        if name.map_or(true, |n| n.trim().is_empty()) {
            errors.push(format!("events[{i}].name must not be blank"));
        }
    }
    errors
}

/// `{ ...body, ...extra }` — echoes the incoming JSON back with extra fields merged in.
pub fn merge(body: &Json, extra: Json) -> Json {
    let mut out = body.as_object().cloned().unwrap_or_default();
    if let Json::Object(extra) = extra {
        out.extend(extra);
    }
    Json::Object(out)
}

/// Validates + persists an already-parsed batch.
/// Returns (status_code, payload) where payload is the body merged with
/// a status_code field (and error/details on failure).
pub async fn process_batch(db: &Db, body: &Json) -> (u16, Json) {
    let errors = validate_events_batch(body);
    if !errors.is_empty() {
        return (400, merge(body, json!({ "status_code": 400, "error": "Validation failed", "details": errors })));
    }

    if let Err(err) = db.save_event_batch(body).await {
        eprintln!("[DB] save error: {err}");
        return (500, merge(body, json!({ "status_code": 500, "error": "Database save failed", "details": [err.to_string()] })));
    }

    (200, merge(body, json!({ "status_code": 200 })))
}

/// Parses a raw JSON string, then validates + persists it.
pub async fn process_events_json(db: &Db, raw: &str) -> (u16, Json) {
    let raw = if raw.is_empty() { "{}" } else { raw };
    match serde_json::from_str::<Json>(raw) {
        Ok(body) => process_batch(db, &body).await,
        Err(err) => (400, json!({ "status_code": 400, "error": "Invalid JSON", "details": [err.to_string()] })),
    }
}

/// Mirrors JS `body.batch_id || 'n/a'` for log lines.
pub fn batch_id_label(body: &Json) -> String {
    match body.get("batch_id") {
        None | Some(Json::Null) | Some(Json::Bool(false)) => "n/a".into(),
        Some(Json::String(s)) if s.is_empty() => "n/a".into(),
        Some(Json::Number(n)) if n.as_f64() == Some(0.0) => "n/a".into(),
        Some(Json::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

pub fn events_len(body: &Json) -> usize {
    body.get("events").and_then(Json::as_array).map_or(0, Vec::len)
}
