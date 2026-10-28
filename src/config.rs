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
    /// Crash alerts: Slack-compatible webhook (POST {"text": …}). None = log only.
    pub alert_webhook_url: Option<String>,
    /// Crashes of one issue within an hour that trigger a velocity alert.
    pub velocity_alert_threshold: u64,
    pub apns: Option<ApnsConfig>,
    /// Max upload size for /v1/symbols (dSYM zips can be large).
    pub symbols_max_bytes: usize,
    /// Local cache for extracted dSYM DWARF files.
    pub dsym_cache_dir: std::path::PathBuf,
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
            alert_webhook_url: env_nonempty("DM_ALERT_WEBHOOK_URL"),
            velocity_alert_threshold: parse_env("DM_VELOCITY_ALERT_THRESHOLD", 25),
            apns,
            symbols_max_bytes: parse_env::<usize>("DM_SYMBOLS_MAX_MB", 512) * 1024 * 1024,
            dsym_cache_dir: env_nonempty("DM_DSYM_CACHE_DIR")
                .map(Into::into)
                .unwrap_or_else(|| std::env::temp_dir().join("dm-dsym-cache")),
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

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env_nonempty(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
