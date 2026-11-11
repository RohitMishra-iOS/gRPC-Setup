//! Uploads from SDK builds before 2.1 (`POST /events`, `status.StatusService/CheckStatus`) and batches
//! stored by server versions before 3.2 (`event_data`) → `UploadEventsRequest` (dm-specs TBL-003, TBL-004, TBL-013).
//!
//! Old batch: `{batch_id, app_id, installation_id, instance_id, user_id, screen_name,
//! user_properties: {k: v}, consent: {analytics_storage: granted|denied, ad_storage},
//! app: {bundle_id, version_name, version_code, sdk_version},
//! device: {os_version, manufacturer, model, locale, timezone, network_type, platform},
//! debug, events: [{name, timestamp_ms, session_id, params: {k: v, items: [{…}]}}]}`.

use chrono::Offset;
use serde_json::{Map, Value as Json};

use super::reference::AUTOMATIC_EVENTS;
use super::{param, value};
use crate::dm::RequestMeta;
use crate::pb_dm::analytics_value::Value;
use crate::pb_dm::{
    AnalyticsAppInfo, AnalyticsDevice, AnalyticsEvent, AnalyticsItem, AnalyticsParam, AnalyticsPrivacyInfo, AnalyticsUserProperty,
    ClientContext, UploadEventsRequest,
};

/// Old parameter names → GA4 names (TBL-004).
const RENAMES: &[(&str, &str)] = &[
    ("screen_name", "DM_screen"),
    ("screen_class", "DM_screen_class"),
    ("previous_screen_name", "DM_previous_screen"),
    ("previous_screen_class", "DM_previous_class"),
    ("session_number", "ga_session_number"),
];

fn text(object: &Map<String, Json>, key: &str) -> String {
    match object.get(key) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn object<'a>(root: &'a Map<String, Json>, key: &str) -> Map<String, Json> {
    root.get(key).and_then(Json::as_object).cloned().unwrap_or_default()
}

/// A JSON scalar as a typed value: strings, whole numbers → int, other numbers → double, booleans → 1/0.
pub fn typed(value: &Json) -> Option<Value> {
    match value {
        Json::String(s) => Some(Value::StringValue(s.clone())),
        Json::Bool(b) => Some(Value::IntValue(i64::from(*b))),
        Json::Number(n) => n.as_i64().map(Value::IntValue).or_else(|| n.as_f64().filter(|v| v.is_finite()).map(Value::DoubleValue)),
        _ => None,
    }
}

/// A session id as GA4's integer ga_session_id: numbers as they are, other text hashed (FNV-1a) to a
/// positive 10-digit number, so the same id always maps to the same number.
pub fn session_number(id: &Json) -> Option<i64> {
    match id {
        Json::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|v| v as i64)),
        Json::String(s) if s.is_empty() => None,
        Json::String(s) => s.parse().ok().or_else(|| {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in s.bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
            Some((hash % 9_000_000_000 + 1_000_000_000) as i64)
        }),
        _ => None,
    }
}

fn platform_of(root: &Map<String, Json>, device: &Map<String, Json>) -> String {
    let declared = text(root, "platform");
    let declared = if declared.is_empty() { text(device, "platform") } else { declared };
    match declared.to_ascii_lowercase().as_str() {
        "android" | "jvm" => "android".into(),
        "web" => "web".into(),
        "ios" => "ios".into(),
        _ if text(device, "manufacturer").eq_ignore_ascii_case("web") => "web".into(),
        _ if text(device, "manufacturer").eq_ignore_ascii_case("apple") => "ios".into(),
        _ if !declared.is_empty() => declared,
        _ => "ios".into(),
    }
}

fn consent(value: &str) -> String {
    match value {
        "granted" => "Yes".into(),
        "denied" => "No".into(),
        _ => String::new(),
    }
}

fn tz_offset_seconds(name: &str) -> Option<i64> {
    let tz: chrono_tz::Tz = name.parse().ok()?;
    Some(i64::from(chrono::Utc::now().with_timezone(&tz).offset().fix().local_minus_utc()))
}

fn item(object: &Map<String, Json>) -> AnalyticsItem {
    AnalyticsItem { params: object.iter().filter_map(|(k, v)| typed(v).map(|t| param(k, t))).collect() }
}

/// Converts an old batch (already checked by `events::check_batch`) for `super::check_request`.
pub fn convert(body: &Json, meta: &RequestMeta) -> (ClientContext, UploadEventsRequest) {
    let root = body.as_object().cloned().unwrap_or_default();
    let app = object(&root, "app");
    let device = object(&root, "device");
    let platform = platform_of(&root, &device);
    let mut app_id = text(&root, "app_id");
    if app_id.is_empty() {
        app_id = meta.app_id.clone().unwrap_or_default();
    }
    let instance = text(&root, "instance_id");
    let installation = Some(text(&root, "installation_id")).filter(|s| !s.is_empty()).unwrap_or_else(|| instance.clone());
    let context = ClientContext {
        app_id,
        installation_id: installation.clone(),
        user_id: text(&root, "user_id"),
        platform: platform.clone(),
        sdk_version: text(&app, "sdk_version"),
        bundle_id: text(&app, "bundle_id"),
        app_version: text(&app, "version_name"),
        app_build: text(&app, "version_code"),
        os_version: if platform == "web" { String::new() } else { text(&device, "os_version") },
        device_model: if platform == "web" { String::new() } else { text(&device, "model") },
        locale: text(&device, "locale"),
        timezone: text(&device, "timezone"),
        network_type: text(&device, "network_type"),
    };

    let manufacturer = text(&device, "manufacturer");
    let model = text(&device, "model");
    let analytics_device = match platform.as_str() {
        "web" => AnalyticsDevice {
            category: if model.contains("mobile") { "mobile".into() } else { "desktop".into() },
            language: text(&device, "locale").to_ascii_lowercase(),
            time_zone_offset_seconds: tz_offset_seconds(&text(&device, "timezone")).unwrap_or(0),
            hostname: text(&app, "bundle_id"),
            ..Default::default()
        },
        "android" => AnalyticsDevice {
            category: "mobile".into(),
            mobile_brand_name: manufacturer.clone(),
            mobile_model_name: model.clone(),
            mobile_marketing_name: model.clone(),
            mobile_os_hardware_model: model.clone(),
            operating_system: "Android".into(),
            operating_system_version: text(&device, "os_version"),
            language: text(&device, "locale").replace('_', "-").to_ascii_lowercase(),
            time_zone_offset_seconds: tz_offset_seconds(&text(&device, "timezone")).unwrap_or(0),
            ..Default::default()
        },
        _ => AnalyticsDevice {
            mobile_brand_name: "Apple".into(),
            mobile_os_hardware_model: model.clone(),
            operating_system: if model.starts_with("iPad") { "iPadOS".into() } else { "iOS".into() },
            operating_system_version: text(&device, "os_version"),
            language: text(&device, "locale").replace('_', "-").to_ascii_lowercase(),
            time_zone_offset_seconds: tz_offset_seconds(&text(&device, "timezone")).unwrap_or(0),
            ..Default::default()
        },
    };
    let consents = object(&root, "consent");
    let privacy = AnalyticsPrivacyInfo {
        analytics_storage: consent(&text(&consents, "analytics_storage")),
        ads_storage: consent(&text(&consents, "ad_storage")),
        uses_transient_token: "No".into(),
    };
    let user_properties = object(&root, "user_properties")
        .iter()
        .filter_map(|(k, v)| typed(v).map(|t| AnalyticsUserProperty { key: k.clone(), value: value(t), set_timestamp_micros: 0 }))
        .collect();
    let batch_screen = text(&root, "screen_name");

    let events = root
        .get("events")
        .and_then(Json::as_array)
        .map(|events| events.iter().filter_map(Json::as_object).map(|e| convert_event(e, &batch_screen)).collect())
        .unwrap_or_default();

    let request = UploadEventsRequest {
        context: None,
        batch_id: text(&root, "batch_id"),
        bundle_sequence_id: 0,
        upload_timestamp_micros: 0,
        user_id: text(&root, "user_id"),
        user_pseudo_id: instance,
        user_first_touch_timestamp_micros: 0,
        user_properties,
        device: Some(analytics_device),
        app_info: if platform == "web" {
            None
        } else {
            Some(AnalyticsAppInfo { id: text(&app, "bundle_id"), version: text(&app, "version_name"), ..Default::default() })
        },
        privacy_info: Some(privacy),
        debug: root.get("debug").and_then(Json::as_bool).unwrap_or(false),
        page_id: 0,
        events,
    };
    (context, request)
}

fn convert_event(event: &Map<String, Json>, batch_screen: &str) -> AnalyticsEvent {
    let name = text(event, "name");
    let timestamp_ms = event.get("timestamp_ms").and_then(Json::as_f64).unwrap_or(0.0) as i64;
    let mut params: Vec<AnalyticsParam> = Vec::new();
    let mut items = Vec::new();
    let mut session = event.get("session_id").and_then(session_number);
    if let Some(Json::Object(source)) = event.get("params") {
        for (key, v) in source {
            if key == "items" {
                if let Json::Array(list) = v {
                    items = list.iter().filter_map(Json::as_object).map(item).collect();
                }
                continue;
            }
            if key == "ga_session_id" {
                session = session.or_else(|| session_number(v));
                continue;
            }
            let key = RENAMES.iter().find(|(old, _)| old == key).map_or(key.as_str(), |(_, new)| new);
            if params.iter().any(|p: &AnalyticsParam| p.key == key) {
                continue;
            }
            if let Some(t) = typed(v) {
                params.push(param(key, t));
            }
        }
    }
    if let Some(id) = session {
        params.push(param("ga_session_id", Value::IntValue(id)));
    }
    if !batch_screen.is_empty() && !params.iter().any(|p| p.key == "DM_screen") {
        params.push(param("DM_screen", Value::StringValue(batch_screen.to_string())));
    }
    if !params.iter().any(|p| p.key == "DM_event_origin") {
        let origin = if AUTOMATIC_EVENTS.contains(&name.as_str()) { "auto" } else { "app" };
        params.push(param("DM_event_origin", Value::StringValue(origin.into())));
    }
    AnalyticsEvent { name, timestamp_micros: timestamp_ms.saturating_mul(1000), previous_timestamp_micros: 0, params, items }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn get<'a>(e: &'a AnalyticsEvent, key: &str) -> Option<&'a Value> {
        e.params.iter().find(|p| p.key == key).and_then(|p| p.value.as_ref()).and_then(|v| v.value.as_ref())
    }

    // spec: TBL-003, TBL-004
    #[test]
    fn converts_old_batches() {
        let body = json!({
            "batch_id": "b1", "app_id": "dmart-ready", "installation_id": "inst", "instance_id": "inst-a", "user_id": "u1",
            "screen_name": "Home",
            "user_properties": { "city": "Ahmedabad", "visits": 3 },
            "consent": { "analytics_storage": "granted", "ad_storage": "denied" },
            "app": { "bundle_id": "com.dmart", "version_name": "2.0", "version_code": "7", "sdk_version": "2.0.0" },
            "device": { "os_version": "18.1", "manufacturer": "Apple", "model": "iPhone16,2", "locale": "en_IN", "timezone": "Asia/Kolkata" },
            "events": [
                { "name": "screen_view", "timestamp_ms": 1_700_000_000_000_i64, "session_id": "abc", "params": { "screen_name": "Cart", "screen_class": "CartVC", "previous_screen_name": "Home" } },
                { "name": "add_to_cart", "timestamp_ms": 1_700_000_000_500_i64, "session_id": 1700000000, "params": { "price": 10.5, "qty": 2, "fast": true, "items": [{ "item_id": "A", "price": 5 }] } }
            ]
        });
        let (context, request) = convert(&body, &RequestMeta::default());
        assert_eq!(context.platform, "ios");
        assert_eq!(context.installation_id, "inst");
        assert_eq!(request.user_pseudo_id, "inst-a");
        let privacy = request.privacy_info.clone().unwrap();
        assert_eq!((privacy.analytics_storage.as_str(), privacy.ads_storage.as_str()), ("Yes", "No"));
        assert_eq!(request.device.as_ref().unwrap().language, "en-in");
        assert_eq!(request.device.as_ref().unwrap().time_zone_offset_seconds, 19_800);
        let sv = &request.events[0];
        assert_eq!(sv.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(get(sv, "DM_screen"), Some(&Value::StringValue("Cart".into())));
        assert_eq!(get(sv, "DM_previous_screen"), Some(&Value::StringValue("Home".into())));
        assert!(get(sv, "screen_name").is_none());
        assert!(matches!(get(sv, "ga_session_id"), Some(Value::IntValue(n)) if *n >= 1_000_000_000));
        assert_eq!(get(sv, "DM_event_origin"), Some(&Value::StringValue("auto".into())));
        let cart = &request.events[1];
        assert_eq!(get(cart, "price"), Some(&Value::DoubleValue(10.5)));
        assert_eq!(get(cart, "qty"), Some(&Value::IntValue(2)));
        assert_eq!(get(cart, "fast"), Some(&Value::IntValue(1)));
        assert_eq!(get(cart, "ga_session_id"), Some(&Value::IntValue(1_700_000_000)));
        assert_eq!(get(cart, "DM_screen"), Some(&Value::StringValue("Home".into())));
        assert_eq!(get(cart, "DM_event_origin"), Some(&Value::StringValue("app".into())));
        assert_eq!(cart.items.len(), 1);
        assert_eq!(session_number(&json!("abc")), session_number(&json!("abc")));
    }
}
