//! Remote Config condition expressions — JSON, evaluated server-side against what the device
//! sent in FetchConfigRequest. A condition is one node:
//!
//!   {"all": [node, …]}                   every child true
//!   {"any": [node, …]}                   at least one child true
//!   {"not": node}
//!   {"always": true | false}
//!   {"<field>": {<ops>}}                 field = app_version | app_build | os_version | device_model |
//!                                        platform | country | language | locale | app_id | bundle_id |
//!                                        user_id | installation_id
//!   {"user_property": {"name": "membership_tier", <ops>}}
//!   {"custom_signal": {"name": "city", <ops>}}
//!   {"percent": {"seed": "beta", "lt": 10}}        stable 0–100 bucket per installation + seed
//!   {"time": {"after": "2026-11-01T00:00:00+05:30", "before": "…"}}
//!
//! <ops> (all must hold): eq, ne, in, not_in, gt, gte, lt, lte, contains, prefix, exists.
//! Comparisons are version-aware ("2.10.0" > "2.9.1"), numeric when the operand is a number, and
//! otherwise string comparisons. country/language/platform/locale compare case-insensitively, and
//! language "en" matches "en-IN".

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::Value as Json;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Default)]
pub struct EvalContext {
    pub app_id: String,
    pub installation_id: String,
    pub user_id: String,
    pub bundle_id: String,
    pub platform: String,
    pub app_version: String,
    pub app_build: String,
    pub os_version: String,
    pub device_model: String,
    pub locale: String,
    pub language: String,
    pub country: String,
    pub user_properties: HashMap<String, String>,
    pub custom_signals: HashMap<String, String>,
    /// Override for tests / previews; None = now.
    pub now_ms: Option<i64>,
}

const OPS: &[&str] = &["eq", "ne", "in", "not_in", "gt", "gte", "lt", "lte", "contains", "prefix", "exists"];
const CASE_INSENSITIVE: &[&str] = &["country", "language", "platform", "locale"];

pub fn evaluate(node: &Json, ctx: &EvalContext) -> Result<bool, String> {
    let object = node.as_object().ok_or("condition must be a JSON object")?;
    if object.len() != 1 {
        return Err(format!("condition object must have exactly one key, got {:?}", object.keys().collect::<Vec<_>>()));
    }
    let (key, value) = object.iter().next().expect("one entry");
    match key.as_str() {
        "all" => {
            let mut result = true;
            for child in value.as_array().ok_or("\"all\" takes an array")? {
                result &= evaluate(child, ctx)?; // evaluate every child so validation sees all errors
            }
            Ok(result)
        }
        "any" => {
            let mut result = false;
            for child in value.as_array().ok_or("\"any\" takes an array")? {
                result |= evaluate(child, ctx)?;
            }
            Ok(result)
        }
        "not" => Ok(!evaluate(value, ctx)?),
        "always" => value.as_bool().ok_or_else(|| "\"always\" takes true/false".to_string()),
        "user_property" | "custom_signal" => {
            let ops = value.as_object().ok_or(format!("\"{key}\" takes an object"))?;
            let name = ops.get("name").and_then(Json::as_str).ok_or(format!("\"{key}\" needs \"name\""))?;
            let map = if key == "user_property" { &ctx.user_properties } else { &ctx.custom_signals };
            apply_ops(key, map.get(name).map(String::as_str), ops, &["name"])
        }
        "percent" => {
            let ops = value.as_object().ok_or("\"percent\" takes an object")?;
            let seed = ops.get("seed").and_then(Json::as_str).unwrap_or("");
            let bucket = percent_bucket(seed, &ctx.installation_id);
            let bucket_text = format!("{bucket}");
            if let Some(range) = ops.get("between") {
                let range = range.as_array().filter(|r| r.len() == 2).ok_or("\"between\" takes [low, high]")?;
                let low = range[0].as_f64().ok_or("between bounds must be numbers")?;
                let high = range[1].as_f64().ok_or("between bounds must be numbers")?;
                return Ok(bucket >= low && bucket < high);
            }
            apply_ops("percent", Some(&bucket_text), ops, &["seed"])
        }
        "time" => {
            let ops = value.as_object().ok_or("\"time\" takes an object")?;
            let now = ctx.now_ms.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            let parse = |k: &str| -> Result<Option<i64>, String> {
                match ops.get(k) {
                    None => Ok(None),
                    Some(v) => {
                        let text = v.as_str().ok_or(format!("time.{k} must be an RFC 3339 string"))?;
                        chrono::DateTime::parse_from_rfc3339(text)
                            .map(|t| Some(t.timestamp_millis()))
                            .map_err(|e| format!("time.{k}: {e}"))
                    }
                }
            };
            let after = parse("after")?;
            let before = parse("before")?;
            if after.is_none() && before.is_none() {
                return Err("\"time\" needs \"after\" and/or \"before\"".into());
            }
            Ok(after.map_or(true, |a| now >= a) && before.map_or(true, |b| now < b))
        }
        field => {
            let actual = match field {
                "app_version" => &ctx.app_version,
                "app_build" => &ctx.app_build,
                "os_version" => &ctx.os_version,
                "device_model" => &ctx.device_model,
                "platform" => &ctx.platform,
                "country" => &ctx.country,
                "language" => &ctx.language,
                "locale" => &ctx.locale,
                "app_id" => &ctx.app_id,
                "bundle_id" => &ctx.bundle_id,
                "user_id" => &ctx.user_id,
                "installation_id" => &ctx.installation_id,
                other => return Err(format!("unknown condition key \"{other}\"")),
            };
            let ops = value.as_object().ok_or(format!("\"{field}\" takes an object of operators"))?;
            let actual = if actual.is_empty() { None } else { Some(actual.as_str()) };
            apply_ops(field, actual, ops, &[])
        }
    }
}

/// Validates an expression (syntax, keys, operand types) without a real device.
pub fn validate(node: &Json) -> Result<(), String> {
    evaluate(node, &EvalContext::default()).map(|_| ())
}

fn apply_ops(field: &str, actual: Option<&str>, ops: &serde_json::Map<String, Json>, ignore: &[&str]) -> Result<bool, String> {
    let insensitive = CASE_INSENSITIVE.contains(&field);
    let mut result = true;
    let mut any_op = false;
    for (op, operand) in ops {
        if ignore.contains(&op.as_str()) {
            continue;
        }
        if !OPS.contains(&op.as_str()) {
            return Err(format!("unknown operator \"{op}\" on \"{field}\""));
        }
        any_op = true;
        let holds = match (op.as_str(), actual) {
            ("exists", value) => value.is_some() == operand.as_bool().ok_or("exists takes true/false")?,
            ("ne", None) | ("not_in", None) => true,
            (_, None) => {
                check_operand(op, operand)?;
                false
            }
            ("eq", Some(a)) => equals(field, a, operand, insensitive)?,
            ("ne", Some(a)) => !equals(field, a, operand, insensitive)?,
            ("in", Some(a)) => {
                let list = operand.as_array().ok_or(format!("\"in\" on \"{field}\" takes an array"))?;
                let mut found = false;
                for item in list {
                    found |= equals(field, a, item, insensitive)?;
                }
                found
            }
            ("not_in", Some(a)) => {
                let list = operand.as_array().ok_or(format!("\"not_in\" on \"{field}\" takes an array"))?;
                let mut found = false;
                for item in list {
                    found |= equals(field, a, item, insensitive)?;
                }
                !found
            }
            ("gt", Some(a)) => compare(a, operand)? == Ordering::Greater,
            ("gte", Some(a)) => compare(a, operand)? != Ordering::Less,
            ("lt", Some(a)) => compare(a, operand)? == Ordering::Less,
            ("lte", Some(a)) => compare(a, operand)? != Ordering::Greater,
            ("contains", Some(a)) => {
                let needle = operand.as_str().ok_or("contains takes a string")?;
                fold(a, insensitive).contains(&fold(needle, insensitive))
            }
            ("prefix", Some(a)) => {
                let needle = operand.as_str().ok_or("prefix takes a string")?;
                fold(a, insensitive).starts_with(&fold(needle, insensitive))
            }
            _ => unreachable!("op list checked above"),
        };
        result &= holds;
    }
    if !any_op {
        return Err(format!("\"{field}\" has no operator"));
    }
    Ok(result)
}

fn check_operand(op: &str, operand: &Json) -> Result<(), String> {
    match op {
        "in" | "not_in" if !operand.is_array() => Err(format!("\"{op}\" takes an array")),
        "contains" | "prefix" if !operand.is_string() => Err(format!("\"{op}\" takes a string")),
        "gt" | "gte" | "lt" | "lte" | "eq" | "ne" if !(operand.is_string() || operand.is_number() || operand.is_boolean()) => {
            Err(format!("\"{op}\" takes a string, number or boolean"))
        }
        _ => Ok(()),
    }
}

fn fold(text: &str, insensitive: bool) -> String {
    if insensitive { text.to_lowercase() } else { text.to_string() }
}

fn operand_text(operand: &Json) -> Result<String, String> {
    match operand {
        Json::String(s) => Ok(s.clone()),
        Json::Number(n) => Ok(n.to_string()),
        Json::Bool(b) => Ok(b.to_string()),
        other => Err(format!("unsupported operand {other}")),
    }
}

fn equals(field: &str, actual: &str, operand: &Json, insensitive: bool) -> Result<bool, String> {
    let expected = operand_text(operand)?;
    if field == "language" {
        // "en" matches "en-IN" / "en_IN".
        let actual = actual.to_lowercase().replace('_', "-");
        let expected = expected.to_lowercase().replace('_', "-");
        return Ok(actual == expected || actual.split('-').next() == Some(expected.as_str()));
    }
    if operand.is_number() {
        return Ok(compare(actual, operand)? == Ordering::Equal);
    }
    Ok(fold(actual, insensitive) == fold(&expected, insensitive))
}

/// Number if the operand is a number; version-aware if both look like versions; else string.
pub fn compare(actual: &str, operand: &Json) -> Result<Ordering, String> {
    if let Some(expected) = operand.as_f64() {
        let value: f64 = actual.trim().parse().map_err(|_| format!("'{actual}' is not a number"))?;
        return Ok(value.partial_cmp(&expected).unwrap_or(Ordering::Equal));
    }
    let expected = operand_text(operand)?;
    match (parse_version(actual), parse_version(&expected)) {
        (Some(a), Some(b)) => Ok(compare_versions(&a, &b)),
        _ => Ok(actual.cmp(&expected)),
    }
}

fn parse_version(text: &str) -> Option<Vec<u64>> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    text.split('.').map(|part| part.parse::<u64>().ok()).collect()
}

fn compare_versions(a: &[u64], b: &[u64]) -> Ordering {
    for i in 0..a.len().max(b.len()) {
        let ordering = a.get(i).unwrap_or(&0).cmp(b.get(i).unwrap_or(&0));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Stable bucket in [0, 100) with two decimals of resolution.
pub fn percent_bucket(seed: &str, installation_id: &str) -> f64 {
    let digest = Sha256::digest(format!("{seed}.{installation_id}").as_bytes());
    let value = u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"));
    (value % 10_000) as f64 / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> EvalContext {
        EvalContext {
            installation_id: "install-1".into(),
            app_version: "2.10.0".into(),
            country: "IN".into(),
            language: "en-IN".into(),
            platform: "ios".into(),
            user_properties: [("membership_tier".to_string(), "gold".to_string())].into(),
            custom_signals: [("store_count".to_string(), "12".to_string())].into(),
            now_ms: Some(1_800_000_000_000),
            ..Default::default()
        }
    }

    #[test]
    fn versions_compare_numerically() {
        assert!(evaluate(&json!({"app_version": {"gte": "2.9.1"}}), &ctx()).unwrap());
        assert!(!evaluate(&json!({"app_version": {"lt": "2.10"}}), &ctx()).unwrap());
    }

    #[test]
    fn combinators_and_case_insensitive_fields() {
        let c = json!({"all": [
            {"country": {"in": ["in", "AE"]}},
            {"language": {"eq": "en"}},
            {"not": {"platform": {"eq": "android"}}},
            {"user_property": {"name": "membership_tier", "eq": "gold"}},
            {"custom_signal": {"name": "store_count", "gt": 10}}
        ]});
        assert!(evaluate(&c, &ctx()).unwrap());
        assert!(!evaluate(&json!({"user_property": {"name": "missing", "eq": "x"}}), &ctx()).unwrap());
        assert!(evaluate(&json!({"user_property": {"name": "missing", "exists": false}}), &ctx()).unwrap());
    }

    #[test]
    fn percent_is_stable_and_bounded() {
        let a = percent_bucket("beta", "install-1");
        assert_eq!(a, percent_bucket("beta", "install-1"));
        assert!((0.0..100.0).contains(&a));
        let below = evaluate(&json!({"percent": {"seed": "beta", "lt": a + 0.01}}), &ctx()).unwrap();
        assert!(below);
    }

    #[test]
    fn time_windows() {
        assert!(evaluate(&json!({"time": {"after": "2026-01-01T00:00:00Z"}}), &ctx()).unwrap());
        assert!(!evaluate(&json!({"time": {"before": "2026-01-01T00:00:00Z"}}), &ctx()).unwrap());
    }

    #[test]
    fn validation_rejects_bad_expressions() {
        assert!(validate(&json!({"app_versio": {"eq": "1"}})).is_err());
        assert!(validate(&json!({"app_version": {"equals": "1"}})).is_err());
        assert!(validate(&json!({"country": {"in": "IN"}})).is_err());
        assert!(validate(&json!({"a": 1, "b": 2})).is_err());
        assert!(validate(&json!({"all": [{"country": {"in": ["IN"]}}, {"percent": {"seed": "x", "lt": 50}}]})).is_ok());
    }
}
