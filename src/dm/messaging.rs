//! MessagingService — push registrations and topics.
//!
//! One `devices` document per (app_id, installation_id), keyed by the registration token the
//! SDK receives (`_id`). Re-registering the same install returns the same token, as the proto
//! contract promises. Sending is done by the admin API (admin.rs → apns.rs).

use futures_util::StreamExt;
use mongodb::bson::{doc, DateTime};
use tonic::Status;

use super::{authorize, non_empty, require_context, unavailable, AppState, RequestMeta};
use crate::db::is_duplicate_key;
use crate::pb_dm::{
    Ack, RegisterDeviceRequest, RegisterDeviceResponse, UnregisterDeviceRequest, UpdateTopicSubscriptionRequest,
};

pub async fn register_device(state: &AppState, meta: &RequestMeta, request: RegisterDeviceRequest) -> Result<RegisterDeviceResponse, Status> {
    let context = require_context(request.context, meta)?;
    authorize(&state.config, meta, &context.app_id)?;

    let transport = if request.push_transport.is_empty() { "apns" } else { request.push_transport.as_str() };
    // The transport-specific address of this install, and the field that must be unique per app.
    let (address_field, address, extra): (&str, String, mongodb::bson::Document) = match transport {
        "apns" => {
            let apns_token = request.apns_token.to_lowercase();
            if !is_valid_apns_token(&apns_token) {
                return Err(Status::invalid_argument("apns_token must be a hex string"));
            }
            let environment = match request.apns_environment.as_str() {
                "sandbox" | "production" => request.apns_environment.clone(),
                "" => "production".to_string(),
                other => return Err(Status::invalid_argument(format!("unknown apns_environment '{other}'"))),
            };
            ("apns_token", apns_token, doc! { "apns_environment": environment })
        }
        "fcm" => {
            let token = request.fcm_token.trim().to_string();
            if token.len() < 20 || token.len() > 4096 || token.contains(char::is_whitespace) {
                return Err(Status::invalid_argument("fcm_token is required for push_transport=fcm"));
            }
            ("fcm_token", token, doc! {})
        }
        "webpush" => {
            let sub = request.web_push.clone().ok_or_else(|| Status::invalid_argument("web_push subscription is required for push_transport=webpush"))?;
            let insecure_ok = crate::env_nonempty("DM_WEBPUSH_ALLOW_HTTP_ENDPOINTS").is_some_and(|v| v == "true"); // local testing only
            if !(sub.endpoint.starts_with("https://") || (insecure_ok && sub.endpoint.starts_with("http://"))) || sub.p256dh.is_empty() || sub.auth.is_empty() {
                return Err(Status::invalid_argument("web_push needs an https endpoint, p256dh and auth"));
            }
            let mut v = crate::validate::V::new();
            v.url("web_push.endpoint", &sub.endpoint, !insecure_ok);
            v.base64("web_push.p256dh", &sub.p256dh, Some(65)); // uncompressed P-256 point
            v.base64("web_push.auth", &sub.auth, Some(16));
            v.into_status()?;
            ("web_push.endpoint", sub.endpoint.clone(), doc! { "web_push": { "endpoint": &sub.endpoint, "p256dh": &sub.p256dh, "auth": &sub.auth } })
        }
        other => return Err(Status::invalid_argument(format!("unknown push_transport '{other}' (apns, fcm, webpush)"))),
    };
    if let Some(bad) = request.topics.iter().find(|t| !is_valid_topic(t)) {
        return Err(Status::invalid_argument(format!("invalid topic '{bad}'")));
    }
    let mut v = crate::validate::V::new();
    v.max_count("topics", request.topics.len(), crate::validate::limits::TOPICS_PER_DEVICE);
    if !request.notification_authorization.is_empty() {
        v.one_of("notification_authorization", &request.notification_authorization, &["authorized", "denied", "notDetermined", "provisional", "ephemeral"]);
    }
    if !request.previous_registration_token.is_empty() {
        v.check(is_registration_token(&request.previous_registration_token), "previous_registration_token", "is not a registration token");
    }
    v.into_status()?;

    // Two attempts: a concurrent first registration of the same install can lose the unique
    // (app_id, installation_id) race; the retry then finds and reuses the winner's token.
    for _ in 0..2 {
        let token = existing_token(state, &context.app_id, &context.installation_id, &request.previous_registration_token).await?;
        let now = DateTime::now();
        let result = state
            .db
            .devices
            .update_one(
                doc! { "_id": &token },
                doc! {
                    "$set": {
                        "app_id": &context.app_id,
                        "installation_id": &context.installation_id,
                        "user_id": non_empty(&context.user_id),
                        "transport": transport,
                        "platform": &context.platform,
                        "notification_authorization": &request.notification_authorization,
                        "topics": &request.topics,
                        "bundle_id": &context.bundle_id,
                        "app_version": &context.app_version,
                        "app_build": &context.app_build,
                        "os_version": &context.os_version,
                        "device_model": &context.device_model,
                        "locale": &context.locale,
                        "timezone": &context.timezone,
                        "sdk_version": &context.sdk_version,
                        "active": true,
                        "invalid_reason": null,
                        "updated_at": now,
                        "last_seen_at": now,
                    },
                    "$setOnInsert": { "created_at": now },
                },
            )
            .upsert(true)
            .await;
        if result.is_ok() {
            let mut set = extra.clone();
            if transport != "webpush" {
                set.insert(address_field, &address);
            }
            let _ = state.db.devices.update_one(doc! { "_id": &token }, doc! { "$set": set }).await;
        }
        match result {
            Ok(_) => {
                // The same push token can't belong to two live installs; deactivate stale rows so a
                // push isn't delivered twice. A live row of *another* install with this token means
                // the app was deleted and installed again on this device (iOS often hands the
                // reinstall the same APNs token): that earlier install is an uninstall (MSG-020).
                let reinstalled: Vec<String> = match state
                    .db
                    .devices
                    .find(doc! { "app_id": &context.app_id, address_field: &address, "_id": { "$ne": &token }, "active": true, "installation_id": { "$ne": &context.installation_id } })
                    .await
                {
                    Ok(cursor) => cursor.filter_map(|d| async move { d.ok().and_then(|d| d.get_str("_id").ok().map(str::to_string)) }).collect().await,
                    Err(_) => vec![],
                };
                let _ = state
                    .db
                    .devices
                    .update_many(
                        doc! { "app_id": &context.app_id, address_field: &address, "_id": { "$ne": &token } },
                        doc! { "$set": { "active": false, "invalid_reason": "superseded", "updated_at": now } },
                    )
                    .await;
                for stale in reinstalled {
                    // Deleted at the latest just before this install registered.
                    let at = super::analytics::now_micros() - 1_000_000;
                    if let Err(err) = super::uninstall::record(state, &stale, at, "reinstall").await {
                        eprintln!("[Uninstall] recording reinstall of {stale}: {err}");
                    }
                }
                return Ok(RegisterDeviceResponse { registration_token: token });
            }
            Err(err) if is_duplicate_key(&err) => continue,
            Err(err) => return Err(unavailable(err)),
        }
    }
    Err(Status::aborted("registration conflict, retry"))
}

/// The install's current token, else the client's previous token (if it's this install's), else
/// a new one.
async fn existing_token(state: &AppState, app_id: &str, installation_id: &str, previous: &str) -> Result<String, Status> {
    let found = state
        .db
        .devices
        .find_one(doc! { "app_id": app_id, "installation_id": installation_id })
        .projection(doc! { "_id": 1 })
        .await
        .map_err(unavailable)?;
    if let Some(id) = found.as_ref().and_then(|d| d.get_str("_id").ok()) {
        return Ok(id.to_string());
    }
    if !previous.is_empty() {
        let owned = state
            .db
            .devices
            .find_one(doc! { "_id": previous, "app_id": app_id })
            .await
            .map_err(unavailable)?;
        // Only reuse if it isn't bound to another live installation.
        if owned.is_some_and(|d| d.get_str("installation_id").map_or(true, |i| i == installation_id)) {
            return Ok(previous.to_string());
        }
    }
    Ok(new_token())
}

pub fn new_token() -> String {
    format!("dm1_{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

pub async fn update_topic_subscription(state: &AppState, meta: &RequestMeta, request: UpdateTopicSubscriptionRequest) -> Result<Ack, Status> {
    let context = require_context(request.context, meta)?;
    authorize(&state.config, meta, &context.app_id)?;
    if !is_valid_topic(&request.topic) {
        return Err(Status::invalid_argument(format!("invalid topic '{}'", request.topic)));
    }
    if !is_registration_token(&request.registration_token) {
        return Err(Status::invalid_argument("registration_token is not a registration token"));
    }
    let update = if request.subscribe {
        doc! { "$addToSet": { "topics": &request.topic }, "$set": { "updated_at": DateTime::now() } }
    } else {
        doc! { "$pull": { "topics": &request.topic }, "$set": { "updated_at": DateTime::now() } }
    };
    let result = state
        .db
        .devices
        .update_one(doc! { "_id": &request.registration_token, "app_id": &context.app_id }, update)
        .await
        .map_err(unavailable)?;
    if result.matched_count == 0 {
        return Err(Status::not_found("unknown registration token — register the device again"));
    }
    Ok(Ack {})
}

pub async fn unregister_device(state: &AppState, meta: &RequestMeta, request: UnregisterDeviceRequest) -> Result<Ack, Status> {
    let context = require_context(request.context, meta)?;
    authorize(&state.config, meta, &context.app_id)?;
    if !is_registration_token(&request.registration_token) {
        return Err(Status::invalid_argument("registration_token is not a registration token"));
    }
    // Idempotent: unknown tokens are fine.
    state
        .db
        .devices
        .update_one(
            doc! { "_id": &request.registration_token, "app_id": &context.app_id },
            doc! { "$set": { "active": false, "invalid_reason": "unregistered", "updated_at": DateTime::now() } },
        )
        .await
        .map_err(unavailable)?;
    Ok(Ack {})
}

/// Same rule as the SDK and FCM: [a-zA-Z0-9-_.~%]{1,900}
pub fn is_valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= 900
        && topic.chars().all(|c| c.is_ascii_alphanumeric() || "-_.~%".contains(c))
}

fn is_valid_apns_token(token: &str) -> bool {
    (32..=200).contains(&token.len()) && token.len() % 2 == 0 && token.chars().all(|c| c.is_ascii_hexdigit())
}

/// Tokens this server issues: "dm1_" + 64 hex characters.
pub fn is_registration_token(token: &str) -> bool {
    token.strip_prefix("dm1_").is_some_and(|hex| hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()))
}
