//! MessagingService — push registrations and topics.
//!
//! One `devices` document per (app_id, installation_id), keyed by the registration token the
//! SDK receives (`_id`). Re-registering the same install returns the same token, as the proto
//! contract promises. Sending is done by the admin API (admin.rs → apns.rs).

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

    let apns_token = request.apns_token.to_lowercase();
    if !is_valid_apns_token(&apns_token) {
        return Err(Status::invalid_argument("apns_token must be a hex string"));
    }
    let environment = match request.apns_environment.as_str() {
        "sandbox" | "production" => request.apns_environment.clone(),
        "" => "production".to_string(),
        other => return Err(Status::invalid_argument(format!("unknown apns_environment '{other}'"))),
    };
    if let Some(bad) = request.topics.iter().find(|t| !is_valid_topic(t)) {
        return Err(Status::invalid_argument(format!("invalid topic '{bad}'")));
    }

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
                        "apns_token": &apns_token,
                        "apns_environment": &environment,
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
        match result {
            Ok(_) => {
                // The same APNs token can't belong to two live installs (e.g. after a reinstall);
                // deactivate stale rows so a push isn't delivered twice.
                let _ = state
                    .db
                    .devices
                    .update_many(
                        doc! { "app_id": &context.app_id, "apns_token": &apns_token, "_id": { "$ne": &token } },
                        doc! { "$set": { "active": false, "invalid_reason": "superseded", "updated_at": now } },
                    )
                    .await;
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
