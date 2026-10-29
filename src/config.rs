//! Runtime configuration, read once from the environment (.env locally, Render env vars in prod).

use crate::env_nonempty;

#[derive(Clone, Debug)]
pub struct ApnsConfig {
    /// Contents of the .p8 key (PEM). Either APNS_KEY_P8 (inline, `\n` escapes allowed) or
    /// APNS_KEY_PATH (file path) must be set.
    pub key_pem: String,
    pub key_id: String,
    pub team_id: String,
    /// apns-topic fallback (normally the device's own bundle id is used).
    pub default_topic: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// CORS — comma-separated origins or "*".
    pub allowed_origins: String,
    /// API keys accepted on the dm.sdk.v1 services and /v1/symbols. Each entry is either `key`
    /// (valid for every app) or `app_id:key` (valid for that app only). Empty = auth disabled.
    pub api_keys: Vec<(Option<String>, String)>,
    /// Key for /admin/* routes. None = admin API disabled.
    pub admin_key: Option<String>,
    /// Raw crash/performance/analytics documents are deleted after this many days (TTL index).
    /// 0 = keep forever.
    pub retention_days: u32,
    /// Same for analytics batches (event_data). Default 0 — 1.x data is kept forever.
    pub analytics_retention_days: u32,
    /// Remote Config fetches allowed per installation per hour (app-initiated / real-time).
    pub rc_fetches_per_hour: u32,
    pub rc_realtime_checks_per_hour: u32,
    /// How long a real-time Remote Config check waits for a publish before answering NO_CHANGE.
    pub rc_realtime_hold_seconds: u64,
    /// Crash alerts: Slack-compatible webhook (POST {"text": …}). None = log only.
    pub alert_webhook_url: Option<String>,
    /// Crashes of one issue within an hour that trigger a velocity alert.
    pub velocity_alert_threshold: u64,
    pub apns: Option<ApnsConfig>,
    /// Max upload size for /v1/symbols (dSYM zips can be large).
    pub symbols_max_bytes: usize,
    /// Local cache for extracted dSYM DWARF files.
    pub dsym_cache_dir: std::path::PathBuf,
    /// Platform services (Installations, App Check, Auth, Storage, Functions, Database).
    pub platform: PlatformConfig,
}

/// Settings for the /v1 platform services. See README "Platform services".
#[derive(Clone, Debug)]
pub struct PlatformConfig {
    /// Signs every token the server issues (installation, App Check, ID, custom, refresh).
    /// Unset → a random secret per process (tokens stop working after a restart).
    pub token_secret: String,
    pub token_secret_is_random: bool,
    /// `iss`/`aud` of ID tokens.
    pub project_id: String,
    /// Products that reject requests without a valid App Check token:
    /// any of auth, storage, functions, database, rpc (or "all").
    pub appcheck_enforce: Vec<String>,
    /// Debug tokens accepted by the debug provider (also manageable from the console).
    pub appcheck_debug_tokens: Vec<String>,
    pub appcheck_ttl_seconds: u64,
    /// DeviceCheck (.p8 with DeviceCheck enabled; falls back to the APNs key).
    pub devicecheck_key_pem: Option<String>,
    pub devicecheck_key_id: Option<String>,
    pub devicecheck_team_id: Option<String>,
    pub devicecheck_development: bool,
    /// Play Integrity: service-account JSON with the Play Integrity API enabled.
    pub play_integrity_service_account: Option<String>,
    pub auth_allow_anonymous: bool,
    pub auth_allow_signup: bool,
    /// Google OAuth client ids accepted as `aud` of Google ID tokens.
    pub auth_google_client_ids: Vec<String>,
    /// Apple Sign-In audiences (bundle ids / service ids).
    pub auth_apple_audiences: Vec<String>,
    /// POST {type, email, link, code} here to send verification / reset emails.
    pub auth_email_webhook: Option<String>,
    /// Base URL of the page that handles email action links (?mode=…&oobCode=…).
    pub auth_action_url: Option<String>,
    /// HMAC secret sent to callable-function endpoints (X-DM-Signature).
    pub functions_secret: Option<String>,
    pub storage_max_bytes: usize,
}

impl Config {
    pub fn from_env() -> Self {
        let api_keys = env_nonempty("DM_API_KEYS")
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|entry| match entry.split_once(':') {
                        Some((app, key)) => (Some(app.to_string()), key.to_string()),
                        None => (None, entry.to_string()),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let apns = Self::apns_from_env();

        Self {
            allowed_origins: env_nonempty("ALLOWED_ORIGINS").unwrap_or_else(|| "*".into()),
            api_keys,
            admin_key: env_nonempty("DM_ADMIN_KEY"),
            retention_days: parse_env("DM_RETENTION_DAYS", 90),
            analytics_retention_days: parse_env("DM_ANALYTICS_RETENTION_DAYS", 0),
            rc_fetches_per_hour: parse_env("DM_RC_FETCHES_PER_HOUR", 30),
            rc_realtime_checks_per_hour: parse_env("DM_RC_REALTIME_CHECKS_PER_HOUR", 120),
            rc_realtime_hold_seconds: parse_env("DM_RC_REALTIME_HOLD_SECONDS", 45),
            alert_webhook_url: env_nonempty("DM_ALERT_WEBHOOK_URL"),
            velocity_alert_threshold: parse_env("DM_VELOCITY_ALERT_THRESHOLD", 25),
            apns,
            symbols_max_bytes: parse_env::<usize>("DM_SYMBOLS_MAX_MB", 512) * 1024 * 1024,
            dsym_cache_dir: env_nonempty("DM_DSYM_CACHE_DIR")
                .map(Into::into)
                .unwrap_or_else(|| std::env::temp_dir().join("dm-dsym-cache")),
            platform: PlatformConfig::from_env(),
        }
    }

    fn apns_from_env() -> Option<ApnsConfig> {
        let key_pem = env_nonempty("APNS_KEY_P8")
            .map(|k| k.replace("\\n", "\n"))
            .or_else(|| env_nonempty("APNS_KEY_PATH").and_then(|p| std::fs::read_to_string(p).ok()))?;
        Some(ApnsConfig {
            key_pem,
            key_id: env_nonempty("APNS_KEY_ID")?,
            team_id: env_nonempty("APNS_TEAM_ID")?,
            default_topic: env_nonempty("APNS_TOPIC"),
        })
    }

    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }

    /// True if `key` may act for `app_id`.
    pub fn key_allows(&self, key: &str, app_id: &str) -> bool {
        self.api_keys.iter().any(|(app, k)| {
            constant_time_eq(k.as_bytes(), key.as_bytes()) && app.as_deref().map_or(true, |a| a == app_id)
        })
    }
}

impl PlatformConfig {
    pub fn from_env() -> Self {
        let list = |key: &str| -> Vec<String> {
            env_nonempty(key)
                .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
                .unwrap_or_default()
        };
        let (token_secret, token_secret_is_random) = match env_nonempty("DM_TOKEN_SECRET") {
            Some(secret) => (secret, false),
            None => {
                use rand::RngCore;
                let mut bytes = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut bytes);
                (hex::encode(bytes), true)
            }
        };
        let pem = |inline: &str, path: &str| {
            env_nonempty(inline)
                .map(|k| k.replace("\\n", "\n"))
                .or_else(|| env_nonempty(path).and_then(|p| std::fs::read_to_string(p).ok()))
        };
        Self {
            token_secret,
            token_secret_is_random,
            project_id: env_nonempty("DM_PROJECT_ID").unwrap_or_else(|| "dm-project".into()),
            appcheck_enforce: list("DM_APPCHECK_ENFORCE").into_iter().map(|s| s.to_lowercase()).collect(),
            appcheck_debug_tokens: list("DM_APPCHECK_DEBUG_TOKENS"),
            appcheck_ttl_seconds: parse_env("DM_APPCHECK_TTL_SECONDS", 3600u64).clamp(1800, 7 * 86400),
            devicecheck_key_pem: pem("DM_DEVICECHECK_KEY_P8", "DM_DEVICECHECK_KEY_PATH")
                .or_else(|| pem("APNS_KEY_P8", "APNS_KEY_PATH")),
            devicecheck_key_id: env_nonempty("DM_DEVICECHECK_KEY_ID").or_else(|| env_nonempty("APNS_KEY_ID")),
            devicecheck_team_id: env_nonempty("DM_DEVICECHECK_TEAM_ID").or_else(|| env_nonempty("APNS_TEAM_ID")),
            devicecheck_development: env_nonempty("DM_DEVICECHECK_DEVELOPMENT").is_some_and(|v| v == "true" || v == "1"),
            play_integrity_service_account: env_nonempty("DM_PLAY_INTEGRITY_SERVICE_ACCOUNT")
                .or_else(|| env_nonempty("DM_PLAY_INTEGRITY_SERVICE_ACCOUNT_PATH").and_then(|p| std::fs::read_to_string(p).ok())),
            auth_allow_anonymous: env_nonempty("DM_AUTH_ALLOW_ANONYMOUS").map_or(true, |v| v != "false" && v != "0"),
            auth_allow_signup: env_nonempty("DM_AUTH_ALLOW_SIGNUP").map_or(true, |v| v != "false" && v != "0"),
            auth_google_client_ids: list("DM_AUTH_GOOGLE_CLIENT_IDS"),
            auth_apple_audiences: list("DM_AUTH_APPLE_AUDIENCES"),
            auth_email_webhook: env_nonempty("DM_AUTH_EMAIL_WEBHOOK"),
            auth_action_url: env_nonempty("DM_AUTH_ACTION_URL"),
            functions_secret: env_nonempty("DM_FUNCTIONS_SECRET"),
            storage_max_bytes: parse_env::<usize>("DM_STORAGE_MAX_MB", 100) * 1024 * 1024,
        }
    }

    pub fn enforces(&self, product: &str) -> bool {
        self.appcheck_enforce.iter().any(|p| p == product || p == "all")
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env_nonempty(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
