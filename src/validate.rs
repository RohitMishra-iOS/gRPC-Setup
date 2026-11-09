//! Input validation shared by every endpoint (REST, proto3-JSON and gRPC).
//!
//! Collect every problem of a request with [`V`], then turn it into one error: HTTP 400
//! INVALID_ARGUMENT whose `details` list each field (Google's `BadRequest.fieldViolations` shape),
//! or a gRPC INVALID_ARGUMENT status. All limits live in [`limits`] so the SDKs can mirror them.
//!
//! ```ignore
//! let mut v = V::new();
//! v.len("displayName", &name, 1, limits::DISPLAY_NAME);
//! v.email("email", &email);
//! v.into_api()?;
//! ```

use std::collections::HashMap;

use serde_json::{json, Value as Json};

/// Every size/count limit the server enforces (kept in one place; SDKs use the same numbers).
pub mod limits {
    // Shared
    pub const ID: usize = 128; // app ids, installation ids, report ids, batch ids …
    pub const SHORT_TEXT: usize = 256; // versions, models, locales …
    pub const URL: usize = 2048;

    // Analytics (Firebase/GA4 limits)
    pub const EVENTS_PER_BATCH: usize = 500;
    pub const EVENT_NAME: usize = 40;
    pub const EVENT_PARAMS: usize = 25;
    pub const PARAM_NAME: usize = 40;
    pub const PARAM_STRING_VALUE: usize = 100;
    pub const ITEMS_PER_EVENT: usize = 200;
    pub const USER_PROPERTIES: usize = 25;
    pub const USER_PROPERTY_NAME: usize = 24;
    pub const USER_PROPERTY_VALUE: usize = 36;
    pub const USER_ID: usize = 256;
    /// Events may be queued on the device for days; accept a month back and a day of clock skew.
    pub const EVENT_MAX_AGE_MS: i64 = 30 * 24 * 3600 * 1000;
    pub const EVENT_MAX_FUTURE_MS: i64 = 24 * 3600 * 1000;

    // Crashlytics
    pub const CRASH_KEYS: usize = 64;
    pub const CRASH_KEY_LENGTH: usize = 1024;
    pub const CRASH_LOG_BYTES: usize = 64 * 1024;
    pub const CRASH_BREADCRUMBS: usize = 200;
    pub const CRASH_FRAMES: usize = 1024;
    pub const CRASH_IMAGES: usize = 4096;
    pub const CRASH_REASON: usize = 16 * 1024;
    pub const METRIC_KIT_JSON: usize = 2 * 1024 * 1024;

    // Performance (Firebase limits)
    pub const TRACE_NAME: usize = 100;
    pub const TRACE_METRICS: usize = 32;
    pub const TRACE_ATTRIBUTES: usize = 5;
    pub const ATTRIBUTE_NAME: usize = 40;
    pub const ATTRIBUTE_VALUE: usize = 100;
    pub const TRACE_MAX_DURATION_US: i64 = 24 * 3600 * 1_000_000;

    // Remote Config
    pub const CUSTOM_SIGNALS: usize = 100;
    pub const CUSTOM_SIGNAL_KEY: usize = 250;
    pub const CUSTOM_SIGNAL_VALUE: usize = 500;

    // Messaging
    pub const TOPIC: usize = 900;
    pub const TOPICS_PER_DEVICE: usize = 2000;
    pub const PUSH_TOKEN: usize = 4096;
    pub const MESSAGE_DATA_BYTES: usize = 4096;

    // Auth
    pub const EMAIL: usize = 254;
    pub const PASSWORD_MAX: usize = 4096;
    pub const DISPLAY_NAME: usize = 256;
    pub const UID: usize = 128;
    pub const CUSTOM_CLAIMS_BYTES: usize = 1000;
    pub const ID_TOKEN: usize = 16 * 1024;

    // Storage (Cloud Storage object name rules)
    pub const OBJECT_NAME_BYTES: usize = 1024;
    pub const CUSTOM_METADATA_BYTES: usize = 8 * 1024;
    pub const CUSTOM_METADATA_KEY: usize = 256;
    pub const LIST_MAX_RESULTS: i64 = 1000;

    // Functions
    pub const FUNCTION_NAME: usize = 63;
    pub const CALLABLE_BODY_BYTES: usize = 10 * 1024 * 1024;

    // Database (Firestore limits)
    pub const DOCUMENT_BYTES: usize = 1024 * 1024;
    pub const PATH_BYTES: usize = 6 * 1024;
    pub const SEGMENT_BYTES: usize = 1500;
    pub const FIELD_NAME_BYTES: usize = 1500;
    pub const NESTING_DEPTH: usize = 20;
    pub const WRITES_PER_COMMIT: usize = 500;
    pub const DISJUNCTION_VALUES: usize = 30;
    pub const ORDER_BY: usize = 32;
    pub const FILTERS: usize = 100;
    pub const QUERY_LIMIT: i64 = 10_000;
    pub const AGGREGATIONS: usize = 5;
    pub const DOCUMENT_ID: usize = 1500;

    // JSON in general
    pub const JSON_DEPTH: usize = 64;
}

/// One invalid field.
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    pub field: String,
    pub description: String,
}

/// Collects violations; nothing fails until [`V::into_api`] / [`V::into_status`].
#[derive(Debug, Default)]
pub struct V {
    pub violations: Vec<Violation>,
}

impl V {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }

    /// Records a violation unless `ok`.
    pub fn check(&mut self, ok: bool, field: impl AsRef<str>, description: impl Into<String>) -> bool {
        if !ok {
            self.violations.push(Violation { field: field.as_ref().to_string(), description: description.into() });
        }
        ok
    }

    pub fn fail(&mut self, field: impl AsRef<str>, description: impl Into<String>) {
        self.check(false, field, description);
    }

    pub fn required(&mut self, field: &str, value: &str) -> bool {
        self.check(!value.trim().is_empty(), field, "is required")
    }

    /// Length in characters within [min, max]; no control characters.
    pub fn len(&mut self, field: &str, value: &str, min: usize, max: usize) -> bool {
        let n = value.chars().count();
        if n < min {
            return self.check(false, field, if min == 1 { "is required".to_string() } else { format!("must be at least {min} characters") });
        }
        self.check(n <= max, field, format!("must be at most {max} characters (got {n})")) && self.printable(field, value)
    }

    /// Optional text: empty is fine, otherwise at most `max` characters without control characters.
    pub fn text(&mut self, field: &str, value: &str, max: usize) -> bool {
        value.is_empty() || self.len(field, value, 0, max)
    }

    pub fn max_bytes(&mut self, field: &str, value: &str, max: usize) -> bool {
        self.check(value.len() <= max, field, format!("must be at most {max} bytes (got {})", value.len()))
    }

    /// No control characters except tab/newline (which are allowed only when `multiline`).
    pub fn printable(&mut self, field: &str, value: &str) -> bool {
        self.check(!value.chars().any(|c| c.is_control()), field, "must not contain control characters")
    }

    pub fn multiline(&mut self, field: &str, value: &str, max_bytes: usize) -> bool {
        self.max_bytes(field, value, max_bytes) && self.check(!value.chars().any(|c| c.is_control() && c != '\n' && c != '\r' && c != '\t'), field, "must not contain control characters")
    }

    /// Identifier: 1..=max of [A-Za-z0-9._:-].
    pub fn id(&mut self, field: &str, value: &str, max: usize) -> bool {
        if !self.required(field, value) {
            return false;
        }
        self.check(value.len() <= max && is_safe_id(value), field, format!("must be 1-{max} characters of A-Z a-z 0-9 . _ : -"))
    }

    /// Optional identifier.
    pub fn opt_id(&mut self, field: &str, value: &str, max: usize) -> bool {
        value.is_empty() || self.id(field, value, max)
    }

    pub fn email(&mut self, field: &str, value: &str) -> bool {
        self.check(is_email(value), field, "must be a valid email address")
    }

    /// http(s) URL (https only when `https_only`), at most `limits::URL` bytes.
    pub fn url(&mut self, field: &str, value: &str, https_only: bool) -> bool {
        let ok = value.len() <= limits::URL && is_url(value, https_only);
        self.check(ok, field, if https_only { "must be an https:// URL" } else { "must be an http(s):// URL" })
    }

    pub fn one_of(&mut self, field: &str, value: &str, allowed: &[&str]) -> bool {
        self.check(allowed.contains(&value), field, format!("must be one of: {}", allowed.join(", ")))
    }

    pub fn range(&mut self, field: &str, value: i64, min: i64, max: i64) -> bool {
        self.check((min..=max).contains(&value), field, format!("must be between {min} and {max} (got {value})"))
    }

    pub fn max_count(&mut self, field: &str, count: usize, max: usize) -> bool {
        self.check(count <= max, field, format!("must have at most {max} entries (got {count})"))
    }

    /// String map: entry count, key and value lengths.
    pub fn string_map(&mut self, field: &str, map: &HashMap<String, String>, max_entries: usize, key_max: usize, value_max: usize) -> bool {
        let mut ok = self.max_count(field, map.len(), max_entries);
        for (k, v) in map {
            ok &= self.len(&format!("{field}.{k}"), k, 1, key_max);
            ok &= self.text(&format!("{field}.{k}"), v, value_max);
        }
        ok
    }

    pub fn json_size(&mut self, field: &str, value: &Json, max_bytes: usize) -> bool {
        let size = serde_json::to_vec(value).map(|v| v.len()).unwrap_or(usize::MAX);
        self.check(size <= max_bytes, field, format!("must be at most {max_bytes} bytes of JSON (got {size})"))
    }

    pub fn json_depth(&mut self, field: &str, value: &Json, max: usize) -> bool {
        self.check(depth(value) <= max, field, format!("must not be nested more than {max} levels"))
    }

    /// Standard or URL-safe base64; `decoded` = exact decoded length when given.
    pub fn base64(&mut self, field: &str, value: &str, decoded: Option<usize>) -> bool {
        use base64::Engine;
        let trimmed = value.trim_end_matches('=');
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(trimmed)
            .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed));
        match (bytes, decoded) {
            (Ok(b), Some(n)) => self.check(b.len() == n, field, format!("must decode to {n} bytes")),
            (Ok(_), None) => true,
            (Err(_), _) => self.check(false, field, "must be base64"),
        }
    }

    pub fn hex(&mut self, field: &str, value: &str, min: usize, max: usize) -> bool {
        self.check(value.len() >= min && value.len() <= max && value.chars().all(|c| c.is_ascii_hexdigit()), field, format!("must be {min}-{max} hex characters"))
    }

    /// One error for all violations: HTTP 400 INVALID_ARGUMENT with field details.
    pub fn into_api(self) -> Result<(), crate::platform::ApiError> {
        if self.violations.is_empty() {
            return Ok(());
        }
        Err(crate::platform::ApiError::bad_request("INVALID_ARGUMENT", self.summary()).with_details(self.details()))
    }

    /// Same as [`V::into_api`] with a specific reason (e.g. "INVALID_EMAIL").
    pub fn into_api_reason(self, reason: &str) -> Result<(), crate::platform::ApiError> {
        if self.violations.is_empty() {
            return Ok(());
        }
        Err(crate::platform::ApiError::bad_request(reason, self.summary()).with_details(self.details()))
    }

    pub fn into_status(self) -> Result<(), tonic::Status> {
        if self.violations.is_empty() {
            return Ok(());
        }
        Err(tonic::Status::invalid_argument(self.summary()))
    }

    pub fn summary(&self) -> String {
        let mut parts: Vec<String> = self.violations.iter().take(5).map(|v| format!("{} {}", v.field, v.description)).collect();
        if self.violations.len() > 5 {
            parts.push(format!("and {} more", self.violations.len() - 5));
        }
        parts.join("; ")
    }

    pub fn details(&self) -> Json {
        json!([{
            "@type": "type.googleapis.com/google.rpc.BadRequest",
            "fieldViolations": self.violations.iter().map(|v| json!({ "field": v.field, "description": v.description })).collect::<Vec<_>>(),
        }])
    }

    /// Plain list ("field description") for responses that report per-item problems.
    pub fn messages(&self) -> Vec<String> {
        self.violations.iter().map(|v| format!("{} {}", v.field, v.description)).collect()
    }
}

// ── Predicates ───────────────────────────────────────────────────────────────

pub fn is_safe_id(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

/// Pragmatic RFC 5322 subset: local@domain.tld, no spaces/control chars, sane lengths.
pub fn is_email(value: &str) -> bool {
    if value.len() > limits::EMAIL || value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return false;
    }
    let Some((local, domain)) = value.rsplit_once('@') else { return false };
    if local.is_empty() || local.len() > 64 || local.contains('@') || local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    let labels: Vec<&str> = domain.split('.').collect();
    labels.len() >= 2
        && labels.iter().all(|l| !l.is_empty() && l.len() <= 63 && !l.starts_with('-') && !l.ends_with('-') && l.chars().all(|c| c.is_alphanumeric() || c == '-'))
        && labels.last().is_some_and(|tld| tld.len() >= 2 && tld.chars().all(char::is_alphabetic))
}

pub fn is_url(value: &str, https_only: bool) -> bool {
    let rest = if let Some(r) = value.strip_prefix("https://") {
        r
    } else if let Some(r) = value.strip_prefix("http://") {
        if https_only {
            return false;
        }
        r
    } else {
        return false;
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = host.rsplit_once('@').map_or(host, |(_, h)| h); // drop userinfo
    let hostname = host.split(':').next().unwrap_or("");
    !hostname.is_empty() && !value.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Host of an http(s) URL (lowercase, without port).
pub fn url_host(value: &str) -> Option<String> {
    let rest = value.strip_prefix("https://").or_else(|| value.strip_prefix("http://"))?;
    let host = rest.split(['/', '?', '#']).next()?;
    let host = host.rsplit_once('@').map_or(host, |(_, h)| h);
    Some(host.split(':').next()?.to_lowercase())
}

pub fn depth(value: &Json) -> usize {
    match value {
        Json::Array(items) => 1 + items.iter().map(depth).max().unwrap_or(0),
        Json::Object(map) => 1 + map.values().map(depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Analytics event / parameter name: starts with a letter, then letters, digits, underscores.
pub fn is_analytics_name(value: &str, max: usize) -> bool {
    let mut chars = value.chars();
    value.len() <= max && chars.next().is_some_and(|c| c.is_ascii_alphabetic()) && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Messaging topic: [a-zA-Z0-9-_.~%]{1,900}.
pub fn is_topic(value: &str) -> bool {
    !value.is_empty() && value.len() <= limits::TOPIC && value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '%'))
}

/// Firebase-shaped installation id: 22 url-safe base64 characters, first one c/d/e/f.
pub fn is_fid(value: &str) -> bool {
    value.len() == 22 && value.starts_with(['c', 'd', 'e', 'f']) && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// "type/subtype" with token characters, optional parameters.
pub fn is_content_type(value: &str) -> bool {
    let main = value.split(';').next().unwrap_or("").trim();
    let Some((t, s)) = main.split_once('/') else { return false };
    let token = |x: &str| !x.is_empty() && x.chars().all(|c| c.is_ascii_alphanumeric() || "!#$&-^_.+".contains(c));
    value.len() <= 256 && token(t) && token(s) && !value.chars().any(char::is_control)
}

/// Platforms the SDKs report.
pub const PLATFORMS: &[&str] = &["ios", "android", "web", "jvm", "macos", "tvos", "watchos", "flutter", "react-native", "unity", "server"];

#[cfg(test)]
mod tests {
    use super::*;

    // spec: AUT-003
    #[test]
    fn emails() {
        for good in ["a@b.co", "rohit.k+tag@dmart.in", "x_y@mail.example.com", "राहुल@उदाहरण.भारत"] {
            assert!(is_email(good), "{good}");
        }
        for bad in ["", "a", "a@b", "a@.com", "a@b.", "a b@c.com", "a@@b.com", ".a@b.com", "a..b@c.com", "a@-b.com", "a@b.c0m", "a@b.c"] {
            assert!(!is_email(bad), "{bad}");
        }
    }

    // spec: AUT-005
    #[test]
    fn urls() {
        assert!(is_url("https://dmart.in/x?y#z", true));
        assert!(!is_url("http://dmart.in", true));
        assert!(is_url("http://localhost:8080/a", false));
        assert!(!is_url("javascript:alert(1)", false));
        assert!(!is_url("https://", false));
        assert!(!is_url("https://a b.com", false));
        assert_eq!(url_host("https://user@Shop.DMart.in:443/p").as_deref(), Some("shop.dmart.in"));
    }

    // spec: ERR-002
    #[test]
    fn collects_violations() {
        let mut v = V::new();
        v.len("name", "", 1, 10);
        v.len("bio", "x\u{0}", 0, 10);
        v.email("email", "nope");
        v.range("n", 11, 0, 10);
        v.one_of("p", "x", &["a", "b"]);
        assert_eq!(v.violations.len(), 5);
        assert!(v.summary().starts_with("name is required"));
        assert!(V::new().into_status().is_ok());
    }

    // spec: KEY-005
    #[test]
    fn names_and_ids() {
        assert!(is_analytics_name("add_to_cart", 40));
        assert!(!is_analytics_name("1st", 40) && !is_analytics_name("a-b", 40) && !is_analytics_name(&"a".repeat(41), 40));
        assert!(is_topic("offers.in~1") && !is_topic("a b"));
        assert!(is_fid("cAbcdefghijklmnopqrs_-") && !is_fid("aAbcdefghijklmnopqrs_-"));
        assert!(is_content_type("image/png") && is_content_type("text/plain; charset=utf-8") && !is_content_type("png"));
        assert!(is_safe_id("dmart-ready-ios") && !is_safe_id("a b") && !is_safe_id(""));
        let mut v = V::new();
        assert!(v.base64("k", "AAEC", Some(3)) && !v.base64("k", "%%%", None));
    }
}
