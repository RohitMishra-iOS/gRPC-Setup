//! Uninstall detection through push tokens (dm-specs MSG-020, MSG-021).
//!
//! An app can't report its own removal, so the server notices it the way attribution tools do:
//! Apple answers 410 Unregistered (with the time the token stopped being valid) and Google answers
//! UNREGISTERED once the app is gone. That happens on any send (campaigns, Remote Config signals)
//! and on a silent check this module sends to every active iOS / Android device once per
//! `DM_UNINSTALL_PROBE_HOURS` (default 24; 0 turns the check off). The check is a background
//! push the SDKs ignore: no notification, no `notification_receive`.
//!
//! An uninstall deactivates the device and logs one GA4 `app_remove` event for the install
//! (through the normal analytics ingest, so it lands in the daily tables, reports and exports).
//! It is logged once per install, and not at all when the install was active after the token
//! stopped working (a token that rotated, not an uninstall).

use std::time::Duration;

use axum::extract::State;
use axum::response::Response;
use futures_util::StreamExt;
use mongodb::bson::{doc, Bson, DateTime, Document};
use serde_json::{json, Map, Value as Json};

use super::analytics::{self, now_micros, Checked};
use super::apns::PushRequest;
use crate::pb_dm::{AnalyticsAppInfo, AnalyticsDevice, AnalyticsEvent, AnalyticsParam, ClientContext, UploadEventsRequest};
use super::{AppState, RequestMeta};

/// `dm.label` of the silent check; servers drop `notification_*` events carrying it, so SDK
/// versions that don't know the check yet don't report it as a received message.
pub const PROBE_LABEL: &str = "dm_uninstall_probe";
const STATUS: &str = "dm_uninstall_probe";
/// Devices checked per tick (at most 64 in flight).
const BATCH: i64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Outcome {
    /// The app is still installed (or the push service accepted the message).
    Delivered,
    /// The token was rejected because the app is gone: when, in µs.
    Uninstalled(i64),
    /// Rejected for another reason (configuration, transient error).
    Failed,
}

pub fn probe_hours() -> u64 {
    crate::env_nonempty("DM_UNINSTALL_PROBE_HOURS").and_then(|v| v.parse().ok()).unwrap_or(24)
}

fn tick_seconds() -> u64 {
    crate::env_nonempty("DM_UNINSTALL_PROBE_TICK_SECONDS").and_then(|v| v.parse().ok()).filter(|s| *s > 0).unwrap_or(3600)
}

/// APNs payload of the silent check: a background push with nothing to show.
pub fn probe_apns_payload(message_id: &str) -> Json {
    json!({ "aps": { "content-available": 1 }, "dm": { "message_id": message_id, "label": PROBE_LABEL, "probe": "1" } })
}

/// FCM data of the silent check (data-only message, normal priority).
pub fn probe_fcm_data(message_id: &str) -> Map<String, Json> {
    let mut data = Map::new();
    data.insert("dm".into(), Json::String(json!({ "message_id": message_id, "label": PROBE_LABEL, "probe": "1" }).to_string()));
    data
}

/// True for an incoming `notification_*` event that reports the silent check (older SDKs).
pub fn is_probe_report(event: &AnalyticsEvent) -> bool {
    event.name.starts_with("notification_")
        && event.params.iter().any(|p| p.key == "label" && matches!(p.value.as_ref().and_then(|v| v.value.as_ref()), Some(crate::pb_dm::analytics_value::Value::StringValue(s)) if s == PROBE_LABEL))
}

fn text(doc: &Document, key: &str) -> String {
    doc.get_str(key).unwrap_or_default().to_string()
}

fn sparam(key: &str, value: &str) -> AnalyticsParam {
    analytics::param(key, crate::pb_dm::analytics_value::Value::StringValue(value.to_string()))
}

/// Records an uninstall seen on the device row `token_id` at `at_micros`, detected by `via`
/// ("apns" / "fcm"). Deactivates the device; returns true when `app_remove` was logged.
pub async fn record(state: &AppState, token_id: &str, at_micros: i64, via: &str) -> Result<bool, crate::BoxError> {
    let Some(device) = state.db.devices.find_one(doc! { "_id": token_id }).await? else { return Ok(false) };
    state
        .db
        .devices
        .update_one(
            doc! { "_id": token_id },
            doc! { "$set": { "active": false, "invalid_reason": "uninstalled", "uninstalled_at": DateTime::from_millis(at_micros / 1000), "updated_at": DateTime::now() } },
        )
        .await?;
    log_remove(state, &text(&device, "app_id"), &text(&device, "installation_id"), Some(&device), at_micros, via).await
}

/// A new install named the install it replaced on the same device (iOS keeps the previous
/// installation id in the Keychain, which survives deleting the app; dm-specs ANA-017). Logs that
/// install's `app_remove` and deactivates its push devices. Only for an install the server has a
/// profile for, in the same app. Returns whether an app_remove was logged.
pub async fn record_replaced(state: &AppState, app_id: &str, installation_id: &str, at_micros: i64) -> Result<bool, crate::BoxError> {
    if installation_id.is_empty() {
        return Ok(false);
    }
    let device = state.db.devices.find_one(doc! { "app_id": app_id, "installation_id": installation_id }).sort(doc! { "updated_at": -1 }).await?;
    state
        .db
        .devices
        .update_many(
            doc! { "app_id": app_id, "installation_id": installation_id, "active": true },
            doc! { "$set": { "active": false, "invalid_reason": "uninstalled", "uninstalled_at": DateTime::from_millis(at_micros / 1000), "updated_at": DateTime::now() } },
        )
        .await?;
    log_remove(state, app_id, installation_id, device.as_ref(), at_micros, "reinstall").await
}

/// Collection: which install last sent each Android device install key, `_id = "<app>:<key>"`.
const DEVICE_KEYS: &str = "dm_device_install_keys";

/// Records that `installation_id` now holds the device install key (Android, ANA-017) and returns
/// the install that held it before, if a different one did.
pub async fn swap_device_key(state: &AppState, app_id: &str, key: &str, installation_id: &str) -> Result<Option<String>, crate::BoxError> {
    if key.len() > 128 || installation_id.is_empty() {
        return Ok(None);
    }
    let before = state
        .db
        .coll(DEVICE_KEYS)
        .find_one_and_update(
            doc! { "_id": format!("{app_id}:{key}") },
            doc! { "$set": { "app_id": app_id, "installation_id": installation_id, "updated_at": DateTime::now() } },
        )
        .upsert(true)
        .await?;
    Ok(before
        .and_then(|d| d.get_str("installation_id").ok().map(str::to_string))
        .filter(|previous| previous != installation_id))
}

/// Logs one `app_remove` for an install (once ever, through the batch ledger), from its device row
/// when there is one and otherwise from its analytics profile.
async fn log_remove(state: &AppState, app_id: &str, installation_id: &str, device: Option<&Document>, at_micros: i64, via: &str) -> Result<bool, crate::BoxError> {
    if app_id.is_empty() || installation_id.is_empty() {
        return Ok(false);
    }
    let Some(profile) = state.db.analytics_users.find_one(doc! { "app_id": app_id, "installation_id": installation_id }).await? else {
        // Never uploaded anything: nothing to attribute an uninstall to (also stops made-up ids).
        if device.is_none() {
            return Ok(false);
        }
        return log_remove_with(state, app_id, installation_id, device, &Document::new(), at_micros, via).await;
    };
    // Used after the token died: the token rotated (restore, re-registration), not an uninstall.
    let last_active = profile.get_i64("last_active_micros").unwrap_or(0);
    if last_active > at_micros + 60_000_000 {
        return Ok(false);
    }
    log_remove_with(state, app_id, installation_id, device, &profile, at_micros, via).await
}

async fn log_remove_with(state: &AppState, app_id: &str, installation_id: &str, device: Option<&Document>, profile: &Document, at_micros: i64, via: &str) -> Result<bool, crate::BoxError> {
    let profile_context = profile.get_document("context").cloned().unwrap_or_default();
    // Device row first, then the profile's last upload context.
    let field = |key: &str| {
        let from_device = device.map(|d| text(d, key)).unwrap_or_default();
        if from_device.is_empty() { text(&profile_context, key) } else { from_device }
    };
    let user_id = device.map(|d| text(d, "user_id")).filter(|u| !u.is_empty()).unwrap_or_else(|| text(profile, "user_id"));
    let context = ClientContext {
        app_id: app_id.to_string(),
        installation_id: installation_id.to_string(),
        user_id: user_id.clone(),
        platform: field("platform"),
        sdk_version: field("sdk_version"),
        bundle_id: field("bundle_id"),
        app_version: field("app_version"),
        app_build: field("app_build"),
        os_version: field("os_version"),
        device_model: field("device_model"),
        locale: field("locale"),
        timezone: field("timezone"),
        ..Default::default()
    };
    let profile_device = profile.get_document("device").cloned().unwrap_or_default();
    let profile_app = profile.get_document("app_info").cloned().unwrap_or_default();
    let d = |k: &str| text(&profile_device, k);
    let request = UploadEventsRequest {
        context: Some(context.clone()),
        // One app_remove per install, ever: the batch id goes through the batch ledger.
        batch_id: format!("app_remove:{app_id}:{installation_id}"),
        upload_timestamp_micros: now_micros(),
        user_id,
        user_pseudo_id: profile.get_str("user_pseudo_id").unwrap_or(installation_id).to_string(),
        device: Some(AnalyticsDevice {
            category: d("category"),
            mobile_brand_name: d("mobile_brand_name"),
            mobile_model_name: d("mobile_model_name"),
            mobile_marketing_name: d("mobile_marketing_name"),
            mobile_os_hardware_model: d("mobile_os_hardware_model"),
            operating_system: d("operating_system"),
            operating_system_version: d("operating_system_version"),
            vendor_id: d("vendor_id"),
            language: d("language"),
            ..Default::default()
        }),
        app_info: Some(AnalyticsAppInfo {
            id: text(&profile_app, "id"),
            version: text(&profile_app, "version"),
            install_store: text(&profile_app, "install_store"),
            install_source: text(&profile_app, "install_source"),
        }),
        events: vec![AnalyticsEvent {
            name: "app_remove".into(),
            timestamp_micros: at_micros,
            params: vec![sparam("DM_event_origin", "auto"), sparam("detected_by", via)],
            ..Default::default()
        }],
        ..Default::default()
    };
    let checked = Checked { request, indices: vec![0], rejected: vec![], warnings: vec![] };
    let response = analytics::ingest(state, &RequestMeta::default(), &context, checked, now_micros()).await.map_err(|s| s.message().to_string())?;
    Ok(!response.duplicate)
}

/// Sends the silent check to one device row.
async fn probe_one(state: &AppState, device: &Document, message_id: &str) -> Outcome {
    let now = now_micros();
    match device.get_str("transport").unwrap_or("apns") {
        "fcm" => {
            let Some(fcm) = state.fcm.as_ref() else { return Outcome::Failed };
            let message = super::fcm::FcmMessage {
                token: device.get_str("fcm_token").unwrap_or_default(),
                notification: None,
                data: probe_fcm_data(message_id),
                ttl_seconds: 0,
                collapse_key: Some(PROBE_LABEL),
                high_priority: false,
            };
            match fcm.send(&message).await {
                Ok(()) => Outcome::Delivered,
                Err(e) if e.is_uninstalled() => Outcome::Uninstalled(now),
                Err(_) => Outcome::Failed,
            }
        }
        "webpush" => Outcome::Failed,
        _ => {
            let Some(apns) = state.apns.as_ref() else { return Outcome::Failed };
            let payload = probe_apns_payload(message_id);
            let topic = device.get_str("bundle_id").ok().filter(|t| !t.is_empty()).or(apns.default_topic()).unwrap_or_default();
            let request = PushRequest {
                device_token: device.get_str("apns_token").unwrap_or_default(),
                environment: device.get_str("apns_environment").unwrap_or("production"),
                topic,
                payload: &payload,
                silent: true,
                collapse_id: Some(PROBE_LABEL),
                ttl_seconds: 0,
            };
            match apns.send(request).await {
                Ok(()) => Outcome::Delivered,
                Err(e) if e.is_uninstalled() => Outcome::Uninstalled(e.timestamp_ms.map_or(now, |ms| ms * 1000).min(now)),
                Err(_) => Outcome::Failed,
            }
        }
    }
}

/// One round: every active iOS / Android device not checked (and not seen) within the interval.
/// Returns {checked, delivered, uninstalls, failed}.
pub async fn run_once(state: &AppState, app_id: Option<&str>, force: bool) -> Result<Json, crate::BoxError> {
    let hours = probe_hours().max(1) as i64;
    let cutoff = DateTime::from_millis(DateTime::now().timestamp_millis() - hours * 3_600_000);
    let mut transports = vec![];
    if state.apns.is_some() {
        transports.push("apns");
    }
    if state.fcm.is_some() {
        transports.push("fcm");
    }
    if transports.is_empty() {
        return Ok(json!({ "checked": 0, "delivered": 0, "uninstalls": 0, "failed": 0, "note": "no push credentials configured (APNs / FCM)" }));
    }
    // iOS and Android only (web push has no install); rows without a transport are APNs.
    let filter = if state.apns.is_some() {
        doc! { "active": true, "$or": [ { "transport": { "$in": &transports } }, { "transport": { "$exists": false } } ] }
    } else {
        doc! { "active": true, "transport": { "$in": &transports } }
    };
    let mut and = vec![Bson::Document(filter)];
    if let Some(app) = app_id {
        and.push(Bson::Document(doc! { "app_id": app }));
    }
    if !force {
        and.push(Bson::Document(doc! { "$or": [ { "probed_at": { "$exists": false } }, { "probed_at": { "$lt": cutoff } } ] }));
        and.push(Bson::Document(doc! { "$or": [ { "last_seen_at": { "$exists": false } }, { "last_seen_at": { "$lt": cutoff } } ] }));
    }
    let devices: Vec<Document> = state.db.devices.find(doc! { "$and": and }).limit(BATCH).await?.filter_map(|d| async move { d.ok() }).collect().await;
    let message_id = format!("{PROBE_LABEL}-{}", uuid::Uuid::new_v4().simple());
    let results: Vec<(Document, Outcome)> = futures_util::stream::iter(devices)
        .map(|device| {
            let message_id = message_id.clone();
            async move {
                let outcome = probe_one(state, &device, &message_id).await;
                (device, outcome)
            }
        })
        .buffer_unordered(64)
        .collect()
        .await;
    let (mut delivered, mut uninstalls, mut failed) = (0u64, 0u64, 0u64);
    for (device, outcome) in &results {
        let id = device.get_str("_id").unwrap_or_default();
        let _ = state.db.devices.update_one(doc! { "_id": id }, doc! { "$set": { "probed_at": DateTime::now() } }).await;
        match outcome {
            Outcome::Delivered => delivered += 1,
            Outcome::Uninstalled(at) => {
                let via = device.get_str("transport").unwrap_or("apns");
                match record(state, id, *at, via).await {
                    Ok(true) => uninstalls += 1,
                    Ok(false) => {} // token gone, install used since (rotated): deactivated, not an uninstall
                    Err(err) => eprintln!("[Uninstall] recording {id}: {err}"),
                }
            }
            Outcome::Failed => failed += 1,
        }
    }
    let summary = json!({ "checked": results.len(), "delivered": delivered, "uninstalls": uninstalls, "failed": failed });
    let _ = state
        .db
        .database
        .collection::<Document>(STATUS)
        .update_one(
            doc! { "_id": "status" },
            doc! { "$set": { "last_run_at": DateTime::now(), "last_run": mongodb::bson::to_bson(&summary).unwrap_or(Bson::Null) },
                   "$inc": { "total_checked": results.len() as i64, "total_uninstalls": uninstalls as i64 } },
        )
        .upsert(true)
        .await;
    Ok(summary)
}

/// Background loop: one round per tick while the check is enabled.
pub fn spawn(state: AppState) {
    if probe_hours() == 0 {
        println!("[Uninstall] silent check disabled (DM_UNINSTALL_PROBE_HOURS=0); uninstalls are still seen on regular sends");
        return;
    }
    tokio::spawn(async move {
        // First round a few minutes after start (not during a deploy's restart storm).
        tokio::time::sleep(Duration::from_secs(tick_seconds().min(300))).await;
        loop {
            match run_once(&state, None, false).await {
                Ok(summary) if summary["checked"].as_u64().unwrap_or(0) > 0 => println!("[Uninstall] checked {summary}"),
                Ok(_) => {}
                Err(err) => eprintln!("[Uninstall] round failed: {err}"),
            }
            tokio::time::sleep(Duration::from_secs(tick_seconds())).await;
        }
    });
}

/// GET /admin/messaging/uninstall-check — settings and the last round.
pub async fn status(State(state): State<AppState>) -> Result<Response, Response> {
    let doc = state.db.database.collection::<Document>(STATUS).find_one(doc! { "_id": "status" }).await.map_err(super::admin::db_error)?.unwrap_or_default();
    super::admin::ok(json!({
        "enabled": probe_hours() > 0,
        "interval_hours": probe_hours(),
        "apns_configured": state.apns.is_some(),
        "fcm_configured": state.fcm.is_some(),
        "last_run_at": doc.get_datetime("last_run_at").ok().map(|d| d.timestamp_millis()),
        "last_run": doc.get("last_run").map(|b| b.clone().into_relaxed_extjson()),
        "total_checked": doc.get_i64("total_checked").unwrap_or(0),
        "total_uninstalls": doc.get_i64("total_uninstalls").unwrap_or(0),
    }))
}

#[derive(serde::Deserialize, Default)]
pub struct RunQuery {
    app_id: Option<String>,
    /// true: check every active device now, even those checked or seen recently.
    force: Option<bool>,
}

/// POST /admin/messaging/uninstall-check?app_id=&force=true — runs a round now.
pub async fn run_now(State(state): State<AppState>, axum::extract::Query(q): axum::extract::Query<RunQuery>) -> Result<Response, Response> {
    let summary = run_once(&state, q.app_id.as_deref(), q.force.unwrap_or(false)).await.map_err(|e| super::admin::error(axum::http::StatusCode::SERVICE_UNAVAILABLE, &e.to_string()))?;
    super::admin::ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb_dm::analytics_value::Value;

    // spec: MSG-021
    #[test]
    fn probe_payloads_are_silent_and_labelled() {
        let apns = probe_apns_payload("m1");
        assert_eq!(apns["aps"]["content-available"], 1);
        assert!(apns["aps"].get("alert").is_none() && apns["aps"].get("sound").is_none());
        assert_eq!(apns["dm"]["label"], PROBE_LABEL);
        let fcm = probe_fcm_data("m1");
        let dm: Json = serde_json::from_str(fcm["dm"].as_str().unwrap()).unwrap();
        assert_eq!(dm["probe"], "1");
    }

    // spec: MSG-021
    #[test]
    fn older_sdk_reports_of_the_check_are_recognised() {
        let event = |name: &str, label: &str| AnalyticsEvent { name: name.into(), params: vec![analytics::param("label", Value::StringValue(label.into()))], ..Default::default() };
        assert!(is_probe_report(&event("notification_receive", PROBE_LABEL)));
        assert!(!is_probe_report(&event("notification_receive", "promo")));
        assert!(!is_probe_report(&event("purchase", PROBE_LABEL)));
    }
}
