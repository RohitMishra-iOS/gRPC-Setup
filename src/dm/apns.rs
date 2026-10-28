//! Apple Push Notification service client — token-based auth (.p8 key, ES256 JWT) over HTTP/2.
//!
//! Configure with APNS_KEY_P8 (or APNS_KEY_PATH), APNS_KEY_ID, APNS_TEAM_ID. The JWT is cached and
//! refreshed every 50 minutes (Apple rejects tokens older than an hour and throttles refreshing
//! more often than every 20 minutes).

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::Value as Json;

use crate::config::ApnsConfig;

pub struct ApnsClient {
    config: ApnsConfig,
    key: EncodingKey,
    http: reqwest::Client,
    token: Mutex<Option<(String, Instant)>>,
}

#[derive(Debug)]
pub struct ApnsError {
    /// HTTP status (0 = transport error).
    pub status: u16,
    /// Apple's `reason`, e.g. "BadDeviceToken", "Unregistered", "TopicDisallowed".
    pub reason: String,
}

impl ApnsError {
    /// The token will never work again — deactivate the device.
    pub fn is_invalid_token(&self) -> bool {
        self.status == 410
            || matches!(self.reason.as_str(), "BadDeviceToken" | "Unregistered" | "DeviceTokenNotForTopic")
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    iat: u64,
}

pub struct PushRequest<'a> {
    pub device_token: &'a str,
    /// "sandbox" | "production"
    pub environment: &'a str,
    /// apns-topic: the app's bundle id.
    pub topic: &'a str,
    pub payload: &'a Json,
    /// true → background (content-available) push, priority 5.
    pub silent: bool,
    pub collapse_id: Option<&'a str>,
    /// Seconds Apple should keep retrying delivery to an offline device (0 = once).
    pub ttl_seconds: u64,
}

impl ApnsClient {
    pub fn new(config: ApnsConfig) -> Result<Self, crate::BoxError> {
        let key = EncodingKey::from_ec_pem(config.key_pem.as_bytes())
            .map_err(|e| format!("APNS key is not a valid .p8 (PKCS#8 EC) key: {e}"))?;
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .http2_prior_knowledge()
            .timeout(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(600))
            .build()?;
        Ok(Self { config, key, http, token: Mutex::new(None) })
    }

    pub fn default_topic(&self) -> Option<&str> {
        self.config.default_topic.as_deref()
    }

    fn bearer(&self) -> Result<String, ApnsError> {
        let mut cached = self.token.lock().expect("apns token");
        if let Some((token, at)) = cached.as_ref() {
            if at.elapsed() < Duration::from_secs(50 * 60) {
                return Ok(token.clone());
            }
        }
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.config.key_id.clone());
        let iat = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let token = jsonwebtoken::encode(&header, &Claims { iss: &self.config.team_id, iat }, &self.key)
            .map_err(|e| ApnsError { status: 0, reason: format!("JWT signing failed: {e}") })?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    pub async fn send(&self, request: PushRequest<'_>) -> Result<(), ApnsError> {
        let host = if request.environment == "sandbox" { "api.sandbox.push.apple.com" } else { "api.push.apple.com" };
        let url = format!("https://{host}/3/device/{}", request.device_token);
        let expiration = if request.ttl_seconds == 0 {
            0
        } else {
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) + request.ttl_seconds
        };

        let mut builder = self
            .http
            .post(url)
            .bearer_auth(self.bearer()?)
            .header("apns-topic", request.topic)
            .header("apns-push-type", if request.silent { "background" } else { "alert" })
            .header("apns-priority", if request.silent { "5" } else { "10" })
            .header("apns-expiration", expiration.to_string())
            .json(request.payload);
        if let Some(collapse) = request.collapse_id {
            builder = builder.header("apns-collapse-id", collapse);
        }

        let response = builder.send().await.map_err(|e| ApnsError { status: 0, reason: e.to_string() })?;
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(());
        }
        let body: Json = response.json().await.unwrap_or(Json::Null);
        let reason = body.get("reason").and_then(Json::as_str).unwrap_or("Unknown").to_string();
        if status == 403 && reason == "ExpiredProviderToken" {
            *self.token.lock().expect("apns token") = None;
        }
        Err(ApnsError { status, reason })
    }
}
