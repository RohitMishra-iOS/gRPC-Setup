//! Events batch ingest
//! ─────────────────────────────────────────────────────────────────────────────
//! Accepts an analytics-style event batch as JSON:
//!   { events: [{ timestamp_ms, session_id, name, params }, ...], instance_id, ... }
//! Validation (Firebase limits, see crate::validate::limits): structural problems reject the batch
//! with 400; an invalid event is dropped and listed in `rejected_events`; an invalid parameter or
//! user property is dropped and listed in `warnings`. Everything else is stored.
//!
//! Store-and-forward contract (Firebase-style, see the "Applying this to analytics-sdk-ios" notes):
//!  * `batch_id` is an idempotency key. The first copy is stored; a retried copy (client timed out
//!    after the server saved it) is acknowledged with `"duplicate": true` and not stored again.
//!  * Every response carries `"accepted": true|false` — clients delete queued rows only on true.
//!  * One `analytics_users` document per (app_id, installation_id) keeps first/last seen, the
//!    latest user id, user properties, app/device snapshot and the last session.
//!  * `"debug": true` batches (debug builds / launch argument) are stored with the flag so
//!    DebugView can filter on them.

use serde_json::{json, Map, Value as Json};

use crate::db::Db;
use crate::validate::{is_analytics_name, is_safe_id, limits};

/// Parameters the SDKs add on their own; they don't count against the 25-parameter limit.
const AUTOMATIC_PARAMS: &[&str] = &["screen_name", "screen_class", "session_number", "ga_session_id", "engagement_time_msec", "previous_screen_name", "previous_screen_class"];

/// Result of validating a batch: what will be stored, plus what was dropped and why.
pub struct CheckedBatch {
    /// The batch with invalid events / parameters / user properties removed.
    pub body: Json,
    /// `{"index", "name", "errors"}` for every event that was dropped.
    pub rejected_events: Vec<Json>,
    /// Problems that only removed a part (a parameter, a user property, a consent entry).
    pub warnings: Vec<String>,
}

/// Batch-level problems reject the whole request (400); event-level problems drop just that
/// event, so one bad event never costs the client the rest of its queue.
pub fn check_batch(body: &Json, now_ms: i64) -> Result<CheckedBatch, Vec<String>> {
    let Some(root) = body.as_object() else {
        return Err(vec!["Body must be a JSON object".into()]);
    };
    let Some(events) = root.get("events").and_then(Json::as_array) else {
        return Err(vec![r#""events" must be an array"#.into()]);
    };
    let mut errors = Vec::new();
    if events.len() > limits::EVENTS_PER_BATCH {
        errors.push(format!("at most {} events per batch (got {})", limits::EVENTS_PER_BATCH, events.len()));
    }
    for key in ["batch_id", "app_id", "instance_id", "installation_id"] {
        match root.get(key) {
            None | Some(Json::Null) => {}
            Some(Json::String(v)) if v.is_empty() || (v.len() <= limits::ID && is_safe_id(v)) => {}
            Some(_) => errors.push(format!("{key} must be 1-{} characters of A-Z a-z 0-9 . _ : -", limits::ID)),
        }
    }
    match root.get("user_id") {
        None | Some(Json::Null) => {}
        Some(Json::String(v)) if v.chars().count() <= limits::USER_ID && !v.chars().any(char::is_control) => {}
        Some(_) => errors.push(format!("user_id must be a string of at most {} characters", limits::USER_ID)),
    }
    if !matches!(root.get("debug"), None | Some(Json::Null) | Some(Json::Bool(_))) {
        errors.push("debug must be a boolean".into());
    }
    for key in ["app", "device", "user_properties", "consent"] {
        if !matches!(root.get(key), None | Some(Json::Null) | Some(Json::Object(_))) {
            errors.push(format!("{key} must be an object"));
        }
    }
    for key in ["app", "device"] {
        if let Some(Json::Object(fields)) = root.get(key) {
            for (field, value) in fields {
                if let Json::String(text) = value {
                    if text.chars().count() > limits::SHORT_TEXT * 2 {
                        errors.push(format!("{key}.{field} is longer than {} characters", limits::SHORT_TEXT * 2));
                    }
                } else if value.is_array() || value.is_object() {
                    errors.push(format!("{key}.{field} must be a string, number or boolean"));
                }
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    let mut warnings = Vec::new();
    let mut out = root.clone();

    // User properties: drop invalid ones.
    if let Some(Json::Object(props)) = root.get("user_properties") {
        let mut clean = Map::new();
        for (name, value) in props {
            let valid_name = is_analytics_name(name, limits::USER_PROPERTY_NAME);
            let value_ok = match value {
                Json::String(v) => v.chars().count() <= limits::USER_PROPERTY_VALUE,
                Json::Number(_) | Json::Bool(_) | Json::Null => true,
                _ => false,
            };
            if !valid_name || !value_ok {
                warnings.push(format!("user_properties.{name} dropped (names ≤ {} chars, values ≤ {} chars)", limits::USER_PROPERTY_NAME, limits::USER_PROPERTY_VALUE));
            } else if clean.len() >= limits::USER_PROPERTIES * 2 {
                warnings.push(format!("user_properties.{name} dropped (too many user properties)"));
            } else {
                clean.insert(name.clone(), value.clone());
            }
        }
        out.insert("user_properties".into(), Json::Object(clean));
    }
    // Consent: only known statuses.
    if let Some(Json::Object(consent)) = root.get("consent") {
        let clean: Map<String, Json> = consent
            .iter()
            .filter(|(k, v)| {
                let ok = k.len() <= 64 && matches!(v.as_str(), Some("granted" | "denied"));
                if !ok {
                    warnings.push(format!("consent.{k} dropped (must be \"granted\" or \"denied\")"));
                }
                ok
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.insert("consent".into(), Json::Object(clean));
    }

    let mut kept = Vec::with_capacity(events.len());
    let mut rejected = Vec::new();
    for (i, event) in events.iter().enumerate() {
        match check_event(event, now_ms, &mut warnings, i) {
            Ok(clean) => kept.push(clean),
            Err(problems) => rejected.push(json!({ "index": i, "name": event.get("name"), "errors": problems })),
        }
    }
    out.insert("events".into(), Json::Array(kept));
    Ok(CheckedBatch { body: Json::Object(out), rejected_events: rejected, warnings })
}

fn check_event(event: &Json, now_ms: i64, warnings: &mut Vec<String>, index: usize) -> Result<Json, Vec<String>> {
    let Some(obj) = event.as_object() else { return Err(vec!["event must be an object".into()]) };
    let mut errors = Vec::new();
    let name = obj.get("name").and_then(Json::as_str).unwrap_or("");
    if name.trim().is_empty() {
        errors.push("name must not be blank".into());
    } else if !is_analytics_name(name, limits::EVENT_NAME) {
        errors.push(format!("name must start with a letter and use only letters, digits and _ (≤ {} characters)", limits::EVENT_NAME));
    }
    match obj.get("timestamp_ms").and_then(Json::as_f64) {
        None => errors.push("timestamp_ms must be a number".into()),
        Some(ts) if ts.fract() != 0.0 => errors.push("timestamp_ms must be an integer (epoch milliseconds)".into()),
        Some(ts) => {
            let ts = ts as i64;
            if ts < now_ms - limits::EVENT_MAX_AGE_MS || ts > now_ms + limits::EVENT_MAX_FUTURE_MS {
                errors.push("timestamp_ms is more than 30 days in the past or 1 day in the future".into());
            }
        }
    }
    match obj.get("session_id") {
        None | Some(Json::Null) | Some(Json::Number(_)) => {}
        Some(Json::String(s)) if s.len() <= 64 => {}
        Some(_) => errors.push("session_id must be a string of at most 64 characters or a number".into()),
    }
    if !matches!(obj.get("params"), None | Some(Json::Null) | Some(Json::Object(_))) {
        errors.push("params must be an object".into());
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    let mut clean_params = Map::new();
    let mut custom = 0;
    if let Some(Json::Object(params)) = obj.get("params") {
        for (key, value) in params {
            let field = format!("events[{index}].params.{key}");
            if !is_analytics_name(key, limits::PARAM_NAME) {
                warnings.push(format!("{field} dropped (invalid parameter name)"));
                continue;
            }
            if !AUTOMATIC_PARAMS.contains(&key.as_str()) {
                custom += 1;
                if custom > limits::EVENT_PARAMS {
                    warnings.push(format!("{field} dropped (more than {} parameters)", limits::EVENT_PARAMS));
                    continue;
                }
            }
            match check_param_value(key, value) {
                Ok(()) => {
                    clean_params.insert(key.clone(), value.clone());
                }
                Err(problem) => warnings.push(format!("{field} dropped ({problem})")),
            }
        }
    }
    let mut out = obj.clone();
    out.insert("params".into(), Json::Object(clean_params));
    Ok(Json::Object(out))
}

fn check_param_value(key: &str, value: &Json) -> Result<(), String> {
    match value {
        Json::Null | Json::Bool(_) => Ok(()),
        Json::Number(n) => if n.as_f64().is_some_and(f64::is_finite) { Ok(()) } else { Err("not a finite number".into()) },
        // Firebase limit is 100; allow some headroom for URLs (page_location) before rejecting.
        Json::String(s) if s.chars().count() <= limits::PARAM_STRING_VALUE * 5 => Ok(()),
        Json::String(_) => Err(format!("longer than {} characters", limits::PARAM_STRING_VALUE * 5)),
        Json::Array(items) if key == "items" || items.iter().all(|i| i.is_object()) => {
            if items.len() > limits::ITEMS_PER_EVENT {
                return Err(format!("more than {} items", limits::ITEMS_PER_EVENT));
            }
            for item in items {
                let Some(fields) = item.as_object() else { return Err("items must be objects".into()) };
                if fields.len() > 27 {
                    return Err("an item has more than 27 parameters".into());
                }
                for (k, v) in fields {
                    if !is_analytics_name(k, limits::PARAM_NAME) || v.is_object() || v.is_array() {
                        return Err(format!("item parameter {k} is invalid"));
                    }
                    if v.as_str().is_some_and(|s| s.chars().count() > limits::PARAM_STRING_VALUE * 5) {
                        return Err(format!("item parameter {k} is too long"));
                    }
                }
            }
            Ok(())
        }
        Json::Array(_) => Err("arrays are only allowed for items (lists of objects)".into()),
        Json::Object(_) => Err("objects are not allowed as parameter values".into()),
    }
}

/// Batch-level problems only (kept for callers/tests that just need a yes/no).
pub fn validate_events_batch(body: &Json) -> Vec<String> {
    match check_batch(body, chrono::Utc::now().timestamp_millis()) {
        Ok(_) => Vec::new(),
        Err(errors) => errors,
    }
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
    let checked = match check_batch(body, chrono::Utc::now().timestamp_millis()) {
        Ok(checked) => checked,
        Err(errors) => {
            return (400, merge(body, json!({ "status_code": 400, "accepted": false, "error": "Validation failed", "details": errors })));
        }
    };
    if !checked.rejected_events.is_empty() || !checked.warnings.is_empty() {
        println!("[Events] batch_id={} dropped {} event(s), {} warning(s)", batch_id_label(body), checked.rejected_events.len(), checked.warnings.len());
    }
    let report = json!({ "rejected_events": checked.rejected_events, "warnings": checked.warnings });
    let body = &checked.body;

    match db.claim_batch(body).await {
        Ok(false) => {
            println!("[Events] duplicate batch_id={} ignored", batch_id_label(body));
            return (200, merge(&merge(body, report), json!({ "status_code": 200, "accepted": true, "duplicate": true })));
        }
        Ok(true) => {}
        Err(err) => {
            eprintln!("[DB] batch claim error: {err}");
            return (500, merge(body, json!({ "status_code": 500, "accepted": false, "error": "Database save failed", "details": [err.to_string()] })));
        }
    }

    if let Err(err) = db.save_event_batch(body).await {
        eprintln!("[DB] save error: {err}");
        db.release_batch(body).await; // let the client's retry store it
        return (500, merge(body, json!({ "status_code": 500, "accepted": false, "error": "Database save failed", "details": [err.to_string()] })));
    }
    if let Err(err) = db.upsert_analytics_user(body).await {
        eprintln!("[DB] analytics user upsert error: {err}"); // the batch itself is safe
    }

    (200, merge(&merge(body, report), json!({ "status_code": 200, "accepted": true, "duplicate": false })))
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
