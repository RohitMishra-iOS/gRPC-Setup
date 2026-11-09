//! Firebase Cloud Messaging HTTP v1 sender — Android's only push channel on Play-services devices
//! (just as APNs is iOS's). Configure DM_FCM_SERVICE_ACCOUNT with a service-account JSON from the
//! Firebase project your Android app's google-services.json belongs to (role: Firebase Cloud
//! Messaging API Admin). The project id is read from the JSON.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value as Json};

pub struct FcmClient {
    project_id: String,
    client_email: String,
    key: jsonwebtoken::EncodingKey,
    http: reqwest::Client,
    token: Mutex<Option<(String, Instant)>>,
}

#[derive(Debug)]
pub struct FcmError {
    pub status: u16,
    pub reason: String,
}

impl FcmError {
    pub fn is_invalid_token(&self) -> bool {
        self.status == 404 || self.reason.contains("UNREGISTERED") || (self.status == 400 && self.reason.contains("registration token"))
    }
}

pub struct FcmMessage<'a> {
    pub token: &'a str,
    pub notification: Option<&'a Json>,
    /// FCM data values must be strings.
    pub data: Map<String, Json>,
    pub ttl_seconds: u64,
    pub collapse_key: Option<&'a str>,
    pub high_priority: bool,
}

impl FcmClient {
    pub fn from_env() -> Option<Result<Self, String>> {
        let raw = crate::env_nonempty("DM_FCM_SERVICE_ACCOUNT")
            .or_else(|| crate::env_nonempty("DM_FCM_SERVICE_ACCOUNT_PATH").and_then(|p| std::fs::read_to_string(p).ok()))?;
        Some(Self::new(&raw))
    }

    pub fn new(service_account: &str) -> Result<Self, String> {
        let sa: Json = serde_json::from_str(service_account).map_err(|e| format!("FCM service account JSON: {e}"))?;
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(sa["private_key"].as_str().unwrap_or_default().as_bytes()).map_err(|e| format!("FCM service account key: {e}"))?;
        Ok(Self {
            project_id: sa["project_id"].as_str().ok_or("service account has no project_id")?.to_string(),
            client_email: sa["client_email"].as_str().ok_or("service account has no client_email")?.to_string(),
            key,
            http: reqwest::Client::builder().timeout(Duration::from_secs(15)).build().map_err(|e| e.to_string())?,
            token: Mutex::new(None),
        })
    }

    async fn access_token(&self) -> Result<String, FcmError> {
        if let Some((token, at)) = self.token.lock().unwrap().as_ref() {
            if at.elapsed() < Duration::from_secs(50 * 60) {
                return Ok(token.clone());
            }
        }
        let now = chrono::Utc::now().timestamp();
        let assertion = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &json!({ "iss": self.client_email, "scope": "https://www.googleapis.com/auth/firebase.messaging", "aud": "https://oauth2.googleapis.com/token", "iat": now, "exp": now + 3600 }),
            &self.key,
        )
        .map_err(|e| FcmError { status: 0, reason: format!("OAuth JWT: {e}") })?;
        let response: Json = self
            .http
            .post("https://oauth2.googleapis.com/token")
            .form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", assertion.as_str())])
            .send()
            .await
            .map_err(|e| FcmError { status: 0, reason: format!("OAuth: {e}") })?
            .json()
            .await
            .map_err(|e| FcmError { status: 0, reason: format!("OAuth: {e}") })?;
        let token = response["access_token"].as_str().ok_or_else(|| FcmError { status: 0, reason: format!("OAuth: {response}") })?.to_string();
        *self.token.lock().unwrap() = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    pub fn build(message: &FcmMessage) -> Json {
        let data: Map<String, Json> = message.data.iter().map(|(k, v)| (k.clone(), Json::String(match v { Json::String(s) => s.clone(), other => other.to_string() }))).collect();
        let mut android = json!({ "priority": if message.high_priority { "HIGH" } else { "NORMAL" }, "ttl": format!("{}s", message.ttl_seconds) });
        if let Some(key) = message.collapse_key {
            android["collapse_key"] = json!(key);
        }
        let mut body = json!({ "token": message.token, "data": data, "android": android });
        if let Some(n) = message.notification {
            let mut notification = Map::new();
            for (from, to) in [("title", "title"), ("body", "body"), ("image_url", "image")] {
                if let Some(v) = n.get(from).filter(|v| !v.is_null()) {
                    notification.insert(to.into(), v.clone());
                }
            }
            body["notification"] = Json::Object(notification);
            if let Some(channel) = n.get("android_channel_id").and_then(Json::as_str) {
                body["android"]["notification"] = json!({ "channel_id": channel });
            }
        }
        json!({ "message": body })
    }

    pub async fn send(&self, message: &FcmMessage<'_>) -> Result<(), FcmError> {
        let response = self
            .http
            .post(format!("https://fcm.googleapis.com/v1/projects/{}/messages:send", self.project_id))
            .bearer_auth(self.access_token().await?)
            .json(&Self::build(message))
            .send()
            .await
            .map_err(|e| FcmError { status: 0, reason: e.to_string() })?;
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(());
        }
        let body: Json = response.json().await.unwrap_or(Json::Null);
        let code = body["error"]["details"].as_array().and_then(|d| d.iter().find_map(|x| x["errorCode"].as_str())).unwrap_or("");
        let message = body["error"]["message"].as_str().unwrap_or("unknown error");
        Err(FcmError { status, reason: format!("{code} {message}").trim().to_string() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MSG-003
    #[test]
    fn message_shape() {
        let notification = json!({ "title": "Hi", "body": "There", "image_url": "https://x/y.png" });
        let mut data = Map::new();
        data.insert("dm".into(), json!({ "message_id": "m-1" }));
        data.insert("count".into(), json!(3));
        let m = FcmClient::build(&FcmMessage { token: "tok", notification: Some(&notification), data, ttl_seconds: 60, collapse_key: Some("c"), high_priority: true });
        assert_eq!(m["message"]["notification"]["image"], "https://x/y.png");
        assert_eq!(m["message"]["data"]["count"], "3");
        assert_eq!(m["message"]["data"]["dm"], "{\"message_id\":\"m-1\"}");
        assert_eq!(m["message"]["android"]["ttl"], "60s");
        assert_eq!(m["message"]["android"]["collapse_key"], "c");
    }
}
