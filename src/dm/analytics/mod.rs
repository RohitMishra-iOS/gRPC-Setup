//! Analytics in the GA4 / Firebase BigQuery export layout (dm-specs ANA-010, TBL-001…014).
//!
//! `AnalyticsService/UploadEvents` (gRPC, gRPC-Web, `/v1/rpc` JSON) is validated here, then every
//! event becomes one row of `analytics_<app>.events_YYYYMMDD` (row.rs), the user's lifetime
//! profile in `analytics_users` is updated, and the day's `pseudonymous_users_` / `users_`
//! snapshots are rewritten. Uploads from older SDK builds (`/events`, `CheckStatus`) are converted
//! by legacy.rs and take the same path.

pub mod countries;
pub mod export;
pub mod legacy;
pub mod reference;
pub mod rename;
pub mod row;
pub mod tables;

use std::collections::{BTreeMap, HashSet};

use mongodb::bson::{doc, Bson, DateTime, Document};
use tonic::Status;

use super::{authorize, require_context, AppState, RequestMeta};
use crate::pb_dm::analytics_value::Value;
use crate::pb_dm::{AnalyticsParam, AnalyticsValue, ClientContext, RejectedEvent, UploadEventsRequest, UploadEventsResponse};
use crate::validate::{is_analytics_name, is_safe_id, limits};
use reference::{is_reserved, renamed_param, AUTOMATIC_PARAMS};
use row::Profile;
use tables::Kind;

/// Strings in the device / app_info records.
const RECORD_TEXT: usize = 256;
/// Items keep at most this many parameters (Firebase: 27).
const ITEM_PARAMS: usize = 27;

pub fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// The validated upload: invalid events removed (listed in `rejected`), invalid parameters and
/// user properties removed (listed in `warnings`). `indices[i]` is the original position of
/// `request.events[i]` (stored as batch_event_index).
pub struct Checked {
    pub request: UploadEventsRequest,
    pub indices: Vec<usize>,
    pub rejected: Vec<RejectedEvent>,
    pub warnings: Vec<String>,
}

pub async fn upload_events(state: &AppState, meta: &RequestMeta, mut request: UploadEventsRequest) -> Result<UploadEventsResponse, Status> {
    let context = require_context(request.context.take(), meta)?;
    authorize(&state.config, meta, &context.app_id)?;
    let now = now_micros();
    let checked = check_request(request, now).map_err(|errors| Status::invalid_argument(errors.join("; ")))?;
    let replaced = replaced_installs(&checked.request);
    let response = ingest(state, meta, &context, checked, now).await?;
    if !response.duplicate {
        after_reinstall(state, &context, replaced);
    }
    Ok(response)
}

/// Installs a `first_open` says it replaced on the same device (`previous_installation_id`, from
/// the iOS SDK's Keychain; dm-specs ANA-017), each with the time to log its `app_remove` at
/// (1 s before the first open).
pub fn replaced_installs(request: &UploadEventsRequest) -> Vec<(String, i64)> {
    use crate::pb_dm::analytics_value::Value;
    request
        .events
        .iter()
        .filter(|e| e.name == "first_open")
        .filter_map(|e| {
            let p = e.params.iter().find(|p| p.key == "previous_installation_id")?;
            match p.value.as_ref()?.value.as_ref()? {
                Value::StringValue(id) if !id.is_empty() => Some((id.clone(), e.timestamp_micros - 1_000_000)),
                _ => None,
            }
        })
        .collect()
}

/// Logs `app_remove` for the replaced installs, in the background.
pub fn after_reinstall(state: &AppState, context: &ClientContext, replaced: Vec<(String, i64)>) {
    for (previous, at) in replaced {
        if previous == context.installation_id {
            continue;
        }
        let (state, app_id) = (state.clone(), context.app_id.clone());
        tokio::spawn(async move {
            match crate::dm::uninstall::record_replaced(&state, &app_id, &previous, at).await {
                Ok(true) => println!("[Analytics] reinstall: app_remove logged for {app_id}/{previous}"),
                Ok(false) => {}
                Err(err) => eprintln!("[Analytics] reinstall app_remove for {previous}: {err}"),
            }
        });
    }
}

// ── Validation (ANA-001, ANA-002, ANA-010) ──────────────────────────────────────────────────────

fn text_ok(value: &str, max: usize) -> bool {
    value.chars().count() <= max && !value.chars().any(char::is_control)
}

fn id_ok(value: &str) -> bool {
    value.is_empty() || (value.len() <= limits::ID && is_safe_id(value))
}

/// Batch-level problems → Err (INVALID_ARGUMENT). Event-level → rejected; parameter-level → warnings.
pub fn check_request(mut request: UploadEventsRequest, now_micros: i64) -> Result<Checked, Vec<String>> {
    let mut errors = Vec::new();
    if request.events.len() > limits::EVENTS_PER_BATCH {
        errors.push(format!("at most {} events per batch (got {})", limits::EVENTS_PER_BATCH, request.events.len()));
    }
    for (name, value) in [("batch_id", &request.batch_id), ("user_pseudo_id", &request.user_pseudo_id)] {
        if !id_ok(value) {
            errors.push(format!("{name} must be 1-{} characters of A-Z a-z 0-9 . _ : -", limits::ID));
        }
    }
    if !text_ok(&request.user_id, limits::USER_ID) {
        errors.push(format!("user_id must be at most {} characters without control characters", limits::USER_ID));
    }
    for (name, value) in [
        ("bundle_sequence_id", request.bundle_sequence_id),
        ("upload_timestamp_micros", request.upload_timestamp_micros),
        ("user_first_touch_timestamp_micros", request.user_first_touch_timestamp_micros),
        ("page_id", request.page_id),
    ] {
        if value < 0 {
            errors.push(format!("{name} must not be negative"));
        }
    }
    if let Some(device) = &request.device {
        let fields = [
            ("category", &device.category), ("mobile_brand_name", &device.mobile_brand_name), ("mobile_model_name", &device.mobile_model_name),
            ("mobile_marketing_name", &device.mobile_marketing_name), ("mobile_os_hardware_model", &device.mobile_os_hardware_model),
            ("operating_system", &device.operating_system), ("operating_system_version", &device.operating_system_version),
            ("vendor_id", &device.vendor_id), ("advertising_id", &device.advertising_id), ("language", &device.language),
            ("browser", &device.browser), ("browser_version", &device.browser_version), ("hostname", &device.hostname),
        ];
        for (name, value) in fields {
            if !text_ok(value, RECORD_TEXT) {
                errors.push(format!("device.{name} must be at most {RECORD_TEXT} characters"));
            }
        }
        if !matches!(device.is_limited_ad_tracking.as_str(), "" | "Yes" | "No") {
            errors.push("device.is_limited_ad_tracking must be \"Yes\" or \"No\"".into());
        }
        if device.time_zone_offset_seconds.abs() > 14 * 3600 {
            errors.push("device.time_zone_offset_seconds must be within ±50400".into());
        }
    }
    if let Some(app) = &request.app_info {
        for (name, value) in [("id", &app.id), ("version", &app.version), ("install_store", &app.install_store), ("install_source", &app.install_source)] {
            if !text_ok(value, RECORD_TEXT) {
                errors.push(format!("app_info.{name} must be at most {RECORD_TEXT} characters"));
            }
        }
    }
    if let Some(privacy) = &request.privacy_info {
        for (name, value) in [
            ("analytics_storage", &privacy.analytics_storage),
            ("ads_storage", &privacy.ads_storage),
            ("uses_transient_token", &privacy.uses_transient_token),
        ] {
            if !matches!(value.as_str(), "" | "Yes" | "No") {
                errors.push(format!("privacy_info.{name} must be \"Yes\", \"No\" or empty"));
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    let mut warnings = Vec::new();

    // User properties: name rules, reserved prefixes (dm_exp_ is the SDK's own), value limits.
    let mut seen = HashSet::new();
    let mut custom = 0;
    request.user_properties.retain(|p| {
        let field = format!("user_properties.{}", p.key);
        let sdk_owned = p.key.starts_with("dm_exp_");
        let problem = if !is_analytics_name(&p.key, if sdk_owned { limits::PARAM_NAME } else { limits::USER_PROPERTY_NAME }) {
            Some(format!("names start with a letter, use letters, digits and _, ≤ {} characters", limits::USER_PROPERTY_NAME))
        } else if is_reserved(&p.key) && !sdk_owned {
            Some("reserved prefix".to_string())
        } else if !seen.insert(p.key.clone()) {
            Some("duplicate".to_string())
        } else if p.set_timestamp_micros < 0 {
            Some("set_timestamp_micros must not be negative".to_string())
        } else {
            match p.value.as_ref().and_then(|v| v.value.as_ref()) {
                None => Some("no value".to_string()),
                Some(Value::StringValue(s)) if s.chars().count() > limits::USER_PROPERTY_VALUE => Some(format!("values ≤ {} characters", limits::USER_PROPERTY_VALUE)),
                Some(Value::DoubleValue(d)) if !d.is_finite() => Some("not a finite number".to_string()),
                Some(Value::FloatValue(f)) if !f.is_finite() => Some("not a finite number".to_string()),
                _ => None,
            }
        };
        if let Some(problem) = problem {
            warnings.push(format!("{field} dropped ({problem})"));
            return false;
        }
        if !sdk_owned {
            custom += 1;
            if custom > limits::USER_PROPERTIES {
                warnings.push(format!("{field} dropped (more than {} user properties)", limits::USER_PROPERTIES));
                return false;
            }
        }
        true
    });

    let events = std::mem::take(&mut request.events);
    let mut indices = Vec::with_capacity(events.len());
    let mut rejected = Vec::new();
    for (index, mut event) in events.into_iter().enumerate() {
        // The silent uninstall check is not a message the user received (MSG-021): SDKs that
        // predate it report it as notification_receive; drop those quietly.
        if super::uninstall::is_probe_report(&event) {
            continue;
        }
        let mut problems = Vec::new();
        if event.name.trim().is_empty() {
            problems.push("name must not be blank".to_string());
        } else if !is_analytics_name(&event.name, limits::EVENT_NAME) {
            problems.push(format!("name must start with a letter and use only letters, digits and _ (≤ {} characters)", limits::EVENT_NAME));
        } else if is_reserved(&event.name) {
            problems.push("name uses a reserved prefix (firebase_, google_, ga_, dm_, DM_, apple_)".to_string());
        }
        let ts = event.timestamp_micros;
        if ts < now_micros - limits::EVENT_MAX_AGE_MS * 1000 || ts > now_micros + limits::EVENT_MAX_FUTURE_MS * 1000 {
            problems.push("timestamp_micros is more than 30 days in the past or 1 day in the future".to_string());
        }
        if !problems.is_empty() {
            rejected.push(RejectedEvent { index: index as i32, name: event.name.clone(), errors: problems });
            continue;
        }
        if event.previous_timestamp_micros < 0 || event.previous_timestamp_micros > ts {
            warnings.push(format!("events[{index}].previous_timestamp_micros ignored (must be ≤ timestamp_micros)"));
            event.previous_timestamp_micros = 0;
        }
        event.params = clean_params(std::mem::take(&mut event.params), &format!("events[{index}].params"), true, limits::EVENT_PARAMS, &mut warnings);
        if event.items.len() > limits::ITEMS_PER_EVENT {
            warnings.push(format!("events[{index}].items: {} items dropped (more than {})", event.items.len() - limits::ITEMS_PER_EVENT, limits::ITEMS_PER_EVENT));
            event.items.truncate(limits::ITEMS_PER_EVENT);
        }
        for (i, item) in event.items.iter_mut().enumerate() {
            item.params = clean_params(std::mem::take(&mut item.params), &format!("events[{index}].items[{i}]"), false, ITEM_PARAMS, &mut warnings);
        }
        indices.push(index);
        request.events.push(event);
    }
    Ok(Checked { request, indices, rejected, warnings })
}

/// Drops invalid parameters. `event_level`: reserved prefixes are refused except the SDK's
/// automatic parameters, which also don't count against `max`.
fn clean_params(params: Vec<AnalyticsParam>, field: &str, event_level: bool, max: usize, warnings: &mut Vec<String>) -> Vec<AnalyticsParam> {
    let mut out: Vec<AnalyticsParam> = Vec::with_capacity(params.len());
    let mut seen = HashSet::new();
    let mut custom = 0;
    for mut p in params {
        // SDK versions before the DM_ rename still send firebase_screen etc. (ANA-015).
        if event_level {
            if let Some(current) = renamed_param(&p.key) {
                p.key = current.to_string();
            }
        }
        let name = format!("{field}.{}", p.key);
        let automatic = event_level && AUTOMATIC_PARAMS.contains(&p.key.as_str());
        let problem = if !is_analytics_name(&p.key, limits::PARAM_NAME) {
            Some("invalid parameter name".to_string())
        } else if event_level && is_reserved(&p.key) && !automatic {
            Some("reserved prefix".to_string())
        } else if !seen.insert(p.key.clone()) {
            Some("duplicate key".to_string())
        } else {
            match p.value.as_ref().and_then(|v| v.value.as_ref()) {
                None => Some("no value".to_string()),
                Some(Value::StringValue(s)) if s.chars().count() > limits::PARAM_STRING_VALUE * 5 => Some(format!("longer than {} characters", limits::PARAM_STRING_VALUE * 5)),
                Some(Value::DoubleValue(d)) if !d.is_finite() => Some("not a finite number".to_string()),
                Some(Value::FloatValue(f)) if !f.is_finite() => Some("not a finite number".to_string()),
                _ => None,
            }
        };
        if let Some(problem) = problem {
            warnings.push(format!("{name} dropped ({problem})"));
            continue;
        }
        if !automatic {
            custom += 1;
            if custom > max {
                warnings.push(format!("{name} dropped (more than {max} parameters)"));
                continue;
            }
        }
        out.push(p);
    }
    out
}

// ── Storing ─────────────────────────────────────────────────────────────────────────────────────

fn response(duplicate: bool, stored: usize, rejected: Vec<RejectedEvent>, warnings: Vec<String>) -> UploadEventsResponse {
    UploadEventsResponse { accepted: true, duplicate, stored_events: stored as i32, rejected_events: rejected, warnings }
}

/// Stores a checked upload once per batch id (WIRE-005). Database failures are UNAVAILABLE, so the
/// SDK keeps the events and retries.
pub async fn ingest(state: &AppState, meta: &RequestMeta, context: &ClientContext, checked: Checked, received_micros: i64) -> Result<UploadEventsResponse, Status> {
    let Checked { request, indices, rejected, warnings } = checked;
    let db = &state.db;
    let claimed = !request.batch_id.is_empty();
    if claimed {
        match db.claim_batch_id(&context.app_id, &request.batch_id).await {
            Ok(false) => {
                println!("[Analytics] duplicate batch_id={} ignored", request.batch_id);
                return Ok(response(true, 0, rejected, warnings));
            }
            Ok(true) => {}
            Err(err) => return Err(super::unavailable(err)),
        }
    }
    match store(state, meta, context, &request, &indices, received_micros).await {
        Ok(stored) => {
            if !rejected.is_empty() || !warnings.is_empty() {
                println!("[Analytics] batch_id={} dropped {} event(s), {} warning(s)", request.batch_id, rejected.len(), warnings.len());
            }
            Ok(response(false, stored, rejected, warnings))
        }
        Err(err) => {
            eprintln!("[Analytics] store failed: {err}");
            if claimed {
                db.release_batch_id(&request.batch_id).await;
            }
            Err(super::unavailable(err))
        }
    }
}

/// Writes rows without the batch-id ledger (migration of old data: their ids are already in it).
pub async fn store_unclaimed(state: &AppState, meta: &RequestMeta, context: &ClientContext, checked: &Checked, received_micros: i64) -> Result<usize, crate::BoxError> {
    store(state, meta, context, &checked.request, &checked.indices, received_micros).await
}

async fn store(state: &AppState, meta: &RequestMeta, context: &ClientContext, request: &UploadEventsRequest, indices: &[usize], received: i64) -> Result<usize, crate::BoxError> {
    if request.events.is_empty() {
        return Ok(0);
    }
    let db = &state.db;
    let cfg = &state.config.analytics;
    let cookieless = request.privacy_info.as_ref().is_some_and(|p| p.analytics_storage == "No");
    // No profile without an installation (very old uploads): rows only.
    let profiled = !cookieless && !context.installation_id.is_empty();
    let key = doc! { "app_id": &context.app_id, "installation_id": &context.installation_id };
    let before = if !profiled {
        Profile::default()
    } else {
        db.analytics_users.find_one(key.clone()).await?.map(|d| Profile::from_document(&d)).unwrap_or_default()
    };

    let geo = if cfg.trust_geo_headers { row::geo(&meta.geo) } else { row::Geo::default() };
    let built = row::build(cfg, context, request, indices, &geo, &before, received, cookieless);

    // One insert per daily table; retries of a stored row (same _id) are ignored.
    let mut by_table: BTreeMap<String, Vec<Document>> = BTreeMap::new();
    for (date, document) in built.rows {
        by_table.entry(tables::table(&context.app_id, Kind::Events, &date)).or_default().push(document);
    }
    let mut stored = 0;
    for (name, documents) in by_table {
        tables::ensure(db, &name).await?;
        let count = documents.len();
        match db.coll(&name).insert_many(documents).ordered(false).await {
            Ok(_) => stored += count,
            Err(err) if only_duplicates(&err) => {}
            Err(err) => return Err(err.into()),
        }
    }

    if !profiled {
        return Ok(stored);
    }
    let after = built.profile;
    update_profile(state, context, request, &key, &before, &after, &geo).await?;
    if cfg.user_tables {
        for date in &built.dates {
            write_user_tables(state, context, request, date, &after, &geo).await?;
        }
    }
    Ok(stored)
}

pub fn only_duplicates(err: &mongodb::error::Error) -> bool {
    use mongodb::error::ErrorKind;
    match err.kind.as_ref() {
        ErrorKind::InsertMany(e) => {
            e.write_concern_error.is_none() && e.write_errors.as_ref().is_some_and(|errors| errors.iter().all(|w| w.code == 11000))
        }
        ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code == 11000,
        _ => false,
    }
}

fn opt_str(value: &Option<String>) -> Bson {
    value.as_ref().map_or(Bson::Null, |s| Bson::String(s.clone()))
}

/// Lifetime counters with $inc (safe with parallel uploads), firsts with $min / only-if-missing.
async fn update_profile(
    state: &AppState,
    context: &ClientContext,
    request: &UploadEventsRequest,
    key: &Document,
    before: &Profile,
    after: &Profile,
    geo: &row::Geo,
) -> Result<(), crate::BoxError> {
    let mut set = doc! {
        "app_id": &context.app_id,
        "installation_id": &context.installation_id,
        "last_seen": DateTime::now(),
        "last_active_micros": after.last_active_micros,
        "user_pseudo_id": if request.user_pseudo_id.is_empty() { Bson::Null } else { Bson::String(request.user_pseudo_id.clone()) },
        "user_properties": row::user_properties_map(&request.user_properties),
        "context": {
            "platform": &context.platform, "app_version": &context.app_version, "app_build": &context.app_build,
            "os_version": &context.os_version, "device_model": &context.device_model, "locale": &context.locale,
            "timezone": &context.timezone, "network_type": &context.network_type, "bundle_id": &context.bundle_id,
        },
        "device": row::device_record(request, context),
        "privacy_info": row::privacy_record(request),
        "last_screen": opt_str(&after.last_screen),
        "last_engaged_session": after.last_engaged_session.map_or(Bson::Null, Bson::Int64),
    };
    if !request.user_id.is_empty() {
        set.insert("user_id", &request.user_id);
    }
    if geo.country.is_some() {
        set.insert("geo", row::geo_document(geo));
    }
    if let Some(app_info) = row::app_info_record(request, context) {
        set.insert("app_info", app_info);
    }
    if before.first_source.is_none() {
        if let Some(source) = &after.first_source {
            set.insert("first_source", source.to_document());
        }
    }
    if after.last_click_micros > before.last_click_micros {
        if let Some(click) = &after.last_click {
            set.insert("last_click", click.to_document());
            set.insert("last_click_micros", after.last_click_micros);
        }
    }
    let mut min = Document::new();
    if let Some(first) = after.first_touch_micros {
        min.insert("first_touch_micros", first);
    }
    if let Some(date) = &after.first_purchase_date {
        min.insert("first_purchase_date", date);
    }
    let mut update = doc! {
        "$set": set,
        "$setOnInsert": { "first_seen": DateTime::now() },
        "$inc": {
            "ltv_revenue": after.ltv_revenue - before.ltv_revenue,
            "ltv_revenue_usd": after.ltv_revenue_usd - before.ltv_revenue_usd,
            "purchases": after.purchases - before.purchases,
            "sessions": after.sessions - before.sessions,
            "engaged_sessions": after.engaged_sessions - before.engaged_sessions,
            "engagement_ms": after.engagement_ms - before.engagement_ms,
            "event_count": request.events.len() as i64,
            "batch_count": 1_i64,
        },
    };
    if !min.is_empty() {
        update.insert("$min", min);
    }
    state.db.analytics_users.update_one(key.clone(), update).upsert(true).await?;
    Ok(())
}

/// pseudonymous_users_YYYYMMDD (and users_YYYYMMDD with a user id), TBL-011.
async fn write_user_tables(state: &AppState, context: &ClientContext, request: &UploadEventsRequest, date: &str, profile: &Profile, geo: &row::Geo) -> Result<(), crate::BoxError> {
    let today = tables::event_date(now_micros(), state.config.analytics.timezone);
    let pseudo = if request.user_pseudo_id.is_empty() { &context.installation_id } else { &request.user_pseudo_id };
    let common = row::user_snapshot(&state.config.analytics, context, request, profile, geo, date, &today);
    if !pseudo.is_empty() {
        let name = tables::table(&context.app_id, Kind::PseudonymousUsers, date);
        tables::ensure(&state.db, &name).await?;
        let mut document = doc! { "_id": pseudo, "pseudo_user_id": pseudo };
        document.extend(common.clone());
        state.db.coll(&name).replace_one(doc! { "_id": pseudo }, document).upsert(true).await?;
    }
    if !request.user_id.is_empty() {
        let name = tables::table(&context.app_id, Kind::Users, date);
        tables::ensure(&state.db, &name).await?;
        let mut document = doc! { "_id": &request.user_id, "user_id": &request.user_id };
        document.extend(common);
        state.db.coll(&name).replace_one(doc! { "_id": &request.user_id }, document).upsert(true).await?;
    }
    Ok(())
}

/// Builds a typed value (used by legacy.rs and tests).
pub fn value(v: Value) -> Option<AnalyticsValue> {
    Some(AnalyticsValue { value: Some(v) })
}

pub fn param(key: &str, v: Value) -> AnalyticsParam {
    AnalyticsParam { key: key.to_string(), value: value(v) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb_dm::{AnalyticsEvent, AnalyticsItem, AnalyticsUserProperty};

    fn event(name: &str, params: Vec<AnalyticsParam>) -> AnalyticsEvent {
        AnalyticsEvent { name: name.into(), timestamp_micros: now_micros(), params, ..Default::default() }
    }

    // spec: ANA-015
    #[test]
    fn firebase_named_params_from_older_sdks_are_renamed() {
        let now = now_micros();
        let mut request = UploadEventsRequest { batch_id: "b-2".into(), ..Default::default() };
        request.events = vec![event("screen_view", vec![
            param("firebase_screen", Value::StringValue("Home".into())),
            param("firebase_screen_id", Value::IntValue(7)),
            param("firebase_event_origin", Value::StringValue("auto".into())),
            param("firebase_custom", Value::IntValue(1)),
        ])];
        let checked = check_request(request, now).expect("batch is valid");
        let keys: Vec<&str> = checked.request.events[0].params.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(keys, vec!["DM_screen", "DM_screen_id", "DM_event_origin"]); // firebase_custom: still reserved
        assert_eq!(reference::renamed_param("firebase_previous_class"), Some("DM_previous_class"));
        assert_eq!(reference::renamed_param("DM_screen"), None);
    }

    // spec: ANA-010, ANA-001, ANA-002
    #[test]
    fn validation_levels() {
        let now = now_micros();
        let mut request = UploadEventsRequest { batch_id: "b-1".into(), ..Default::default() };
        request.events = vec![
            event("add_to_cart", vec![param("qty", Value::IntValue(2)), param("ga_custom", Value::IntValue(1)), param("DM_screen", Value::StringValue("Home".into())), param("DM_fake", Value::IntValue(1))]),
            event("bad name!", vec![]),
            event("firebase_thing", vec![]),
            AnalyticsEvent { name: "old".into(), timestamp_micros: now - 40 * 86_400_000_000, ..Default::default() },
        ];
        request.user_properties = vec![
            AnalyticsUserProperty { key: "city".into(), value: value(Value::StringValue("Ahmedabad".into())), set_timestamp_micros: now },
            AnalyticsUserProperty { key: "ga_x".into(), value: value(Value::StringValue("1".into())), set_timestamp_micros: now },
            AnalyticsUserProperty { key: "dm_exp_home_banner".into(), value: value(Value::StringValue("b".into())), set_timestamp_micros: now },
            AnalyticsUserProperty { key: "long".into(), value: value(Value::StringValue("x".repeat(37))), set_timestamp_micros: now },
        ];
        let checked = check_request(request, now).expect("batch is valid");
        assert_eq!(checked.request.events.len(), 1);
        assert_eq!(checked.indices, vec![0]);
        assert_eq!(checked.rejected.iter().map(|r| r.index).collect::<Vec<_>>(), vec![1, 2, 3]);
        let keys: Vec<&str> = checked.request.events[0].params.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(keys, vec!["qty", "DM_screen"]); // ga_custom and DM_fake dropped, automatic kept
        let props: Vec<&str> = checked.request.user_properties.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(props, vec!["city", "dm_exp_home_banner"]);
        assert!(checked.warnings.iter().any(|w| w.contains("ga_custom")));

        let too_many = UploadEventsRequest { events: (0..501).map(|_| event("e", vec![])).collect(), ..Default::default() };
        assert!(check_request(too_many, now).is_err());
        let bad_id = UploadEventsRequest { batch_id: "has space".into(), ..Default::default() };
        assert!(check_request(bad_id, now).is_err());
        let bad_privacy = UploadEventsRequest {
            privacy_info: Some(crate::pb_dm::AnalyticsPrivacyInfo { analytics_storage: "granted".into(), ..Default::default() }),
            ..Default::default()
        };
        assert!(check_request(bad_privacy, now).is_err());
    }

    // spec: ANA-001
    #[test]
    fn params_limits() {
        let now = now_micros();
        let mut params: Vec<AnalyticsParam> = (0..30).map(|i| param(&format!("p{i}"), Value::IntValue(i))).collect();
        params.push(param("ga_session_id", Value::IntValue(1)));
        params.push(AnalyticsParam { key: "unset".into(), value: Some(AnalyticsValue { value: None }) });
        let items = vec![AnalyticsItem { params: (0..30).map(|i| param(&format!("k{i}"), Value::IntValue(i))).collect() }];
        let request = UploadEventsRequest { events: vec![AnalyticsEvent { items, ..event("purchase", params) }], ..Default::default() };
        let checked = check_request(request, now).unwrap();
        let e = &checked.request.events[0];
        assert_eq!(e.params.iter().filter(|p| p.key.starts_with('p')).count(), 25);
        assert!(e.params.iter().any(|p| p.key == "ga_session_id"));
        assert!(!e.params.iter().any(|p| p.key == "unset"));
        assert_eq!(e.items[0].params.len(), ITEM_PARAMS);
    }
}
