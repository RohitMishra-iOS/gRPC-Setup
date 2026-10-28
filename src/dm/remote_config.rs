//! RemoteConfigService/FetchConfig + template storage.
//!
//! A template (published via PUT /admin/remote-config/:app_id) looks like:
//! {
//!   "description": "Diwali 2026",
//!   "conditions": [                                   // priority order: first match wins
//!     {"name": "india_v2", "expression": {"all": [{"country": {"in": ["IN"]}}, {"app_version": {"gte": "2.0.0"}}]}},
//!     {"name": "beta_10", "expression": {"percent": {"seed": "beta", "lt": 10}}}
//!   ],
//!   "parameters": {
//!     "show_diwali_banner":   {"default_value": false, "conditional_values": [{"condition": "india_v2", "value": true}]},
//!     "express_slot_minutes": {"default_value": 30},
//!     "home_layout":          {"default_value": {"rows": 3}},           // JSON values are sent as JSON text
//!     "beta_only_flag":       {"conditional_values": [{"condition": "beta_10", "value": "on"}]}  // no default →
//!   },                                                                    // absent → app's in-app default
//!   "experiments": [{
//!     "experiment_id": "checkout_button", "active": true, "condition": "india_v2", "traffic_percent": 50,
//!     "variants": [{"variant_id": "control", "weight": 50, "values": {}},
//!                  {"variant_id": "orange",  "weight": 50, "values": {"checkout_color": "orange"}}]
//!   }]
//! }
//! Conditions use the JSON expression language in conditions.rs. Every publish is a new,
//! immutable version (rc_templates); FetchConfig always serves the latest.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongodb::bson::{self, doc, DateTime, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as Json;
use tonic::metadata::MetadataValue;
use tonic::Status;

use super::conditions::{self, percent_bucket, EvalContext};
use super::{authorize, require_context, unavailable, AppState, RequestMeta};
use crate::db::is_duplicate_key;
use crate::pb_dm::{Experiment, FetchConfigRequest, FetchConfigResponse, FetchState};

// ── Template model ───────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Template {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub conditions: Vec<NamedCondition>,
    #[serde(default)]
    pub parameters: BTreeMap<String, Parameter>,
    #[serde(default)]
    pub experiments: Vec<ExperimentDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamedCondition {
    pub name: String,
    pub expression: Json,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Parameter {
    #[serde(default, deserialize_with = "optional_value_text", skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(default)]
    pub conditional_values: Vec<ConditionalValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConditionalValue {
    pub condition: String,
    #[serde(deserialize_with = "value_text")]
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExperimentDef {
    pub experiment_id: String,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default = "default_hundred")]
    pub traffic_percent: f64,
    pub variants: Vec<Variant>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Variant {
    pub variant_id: String,
    #[serde(default = "default_one")]
    pub weight: f64,
    #[serde(default, deserialize_with = "value_text_map")]
    pub values: BTreeMap<String, String>,
}

fn default_true() -> bool {
    true
}
fn default_hundred() -> f64 {
    100.0
}
fn default_one() -> f64 {
    1.0
}

/// Admins may write `true`, `30` or `{"rows":3}`; on the wire every value is a string.
fn to_text(value: Json) -> String {
    match value {
        Json::String(s) => s,
        other => other.to_string(),
    }
}
fn value_text<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(to_text(Json::deserialize(d)?))
}
fn optional_value_text<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(match Json::deserialize(d)? {
        Json::Null => None,
        other => Some(to_text(other)),
    })
}
fn value_text_map<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, String>, D::Error> {
    let raw = BTreeMap::<String, Json>::deserialize(d)?;
    Ok(raw.into_iter().map(|(k, v)| (k, to_text(v))).collect())
}

impl Template {
    /// Rejects templates that would misbehave at fetch time.
    pub fn validate(&self) -> Result<(), String> {
        let mut names = std::collections::HashSet::new();
        for condition in &self.conditions {
            if condition.name.is_empty() || !names.insert(condition.name.as_str()) {
                return Err(format!("condition names must be unique and non-empty ('{}')", condition.name));
            }
            conditions::validate(&condition.expression).map_err(|e| format!("condition '{}': {e}", condition.name))?;
        }
        for (key, parameter) in &self.parameters {
            if key.is_empty() || key.len() > 256 {
                return Err(format!("invalid parameter key '{key}'"));
            }
            for cv in &parameter.conditional_values {
                if !names.contains(cv.condition.as_str()) {
                    return Err(format!("parameter '{key}' references unknown condition '{}'", cv.condition));
                }
            }
        }
        let mut ids = std::collections::HashSet::new();
        for experiment in &self.experiments {
            if experiment.experiment_id.is_empty() || !ids.insert(experiment.experiment_id.as_str()) {
                return Err(format!("experiment ids must be unique and non-empty ('{}')", experiment.experiment_id));
            }
            if experiment.variants.is_empty() || experiment.variants.iter().any(|v| v.weight <= 0.0 || v.variant_id.is_empty()) {
                return Err(format!("experiment '{}' needs variants with ids and positive weights", experiment.experiment_id));
            }
            if !(0.0..=100.0).contains(&experiment.traffic_percent) {
                return Err(format!("experiment '{}' traffic_percent must be 0–100", experiment.experiment_id));
            }
            if let Some(condition) = &experiment.condition {
                if !names.contains(condition.as_str()) {
                    return Err(format!("experiment '{}' references unknown condition '{condition}'", experiment.experiment_id));
                }
            }
        }
        Ok(())
    }

    /// The config one device gets: parameter values (conditions in template order, first match
    /// wins) with experiment variant values layered on top.
    pub fn evaluate(&self, ctx: &EvalContext) -> (HashMap<String, String>, Vec<(String, String)>) {
        let matched: HashMap<&str, bool> = self
            .conditions
            .iter()
            .map(|c| {
                let result = conditions::evaluate(&c.expression, ctx).unwrap_or_else(|err| {
                    eprintln!("[RemoteConfig] condition '{}' failed to evaluate: {err}", c.name);
                    false
                });
                (c.name.as_str(), result)
            })
            .collect();
        let priority: HashMap<&str, usize> = self.conditions.iter().enumerate().map(|(i, c)| (c.name.as_str(), i)).collect();

        let mut entries = HashMap::new();
        for (key, parameter) in &self.parameters {
            let conditional = parameter
                .conditional_values
                .iter()
                .filter(|cv| matched.get(cv.condition.as_str()).copied().unwrap_or(false))
                .min_by_key(|cv| priority.get(cv.condition.as_str()).copied().unwrap_or(usize::MAX));
            if let Some(cv) = conditional {
                entries.insert(key.clone(), cv.value.clone());
            } else if let Some(default) = &parameter.default_value {
                entries.insert(key.clone(), default.clone());
            }
        }

        let mut enrolled = Vec::new();
        for experiment in self.experiments.iter().filter(|e| e.active) {
            if let Some(condition) = &experiment.condition {
                if !matched.get(condition.as_str()).copied().unwrap_or(false) {
                    continue;
                }
            }
            if percent_bucket(&format!("{}.traffic", experiment.experiment_id), &ctx.installation_id) >= experiment.traffic_percent {
                continue;
            }
            let total: f64 = experiment.variants.iter().map(|v| v.weight).sum();
            let point = percent_bucket(&format!("{}.variant", experiment.experiment_id), &ctx.installation_id) / 100.0 * total;
            let mut cumulative = 0.0;
            let variant = experiment
                .variants
                .iter()
                .find(|v| {
                    cumulative += v.weight;
                    point < cumulative
                })
                .unwrap_or_else(|| experiment.variants.last().expect("validated non-empty"));
            for (key, value) in &variant.values {
                entries.insert(key.clone(), value.clone());
            }
            enrolled.push((experiment.experiment_id.clone(), variant.variant_id.clone()));
        }
        (entries, enrolled)
    }
}

// ── Storage ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct StoredTemplate {
    pub version: i64,
    pub template: Template,
}

pub async fn latest_template(state: &AppState, app_id: &str) -> Result<Option<StoredTemplate>, mongodb::error::Error> {
    let found = state
        .db
        .rc_templates
        .find_one(doc! { "app_id": app_id })
        .sort(doc! { "version": -1 })
        .await?;
    Ok(found.and_then(|d| parse_stored(&d)))
}

pub async fn template_version(state: &AppState, app_id: &str, version: i64) -> Result<Option<StoredTemplate>, mongodb::error::Error> {
    let found = state.db.rc_templates.find_one(doc! { "app_id": app_id, "version": version }).await?;
    Ok(found.and_then(|d| parse_stored(&d)))
}

fn parse_stored(document: &Document) -> Option<StoredTemplate> {
    // Accept any numeric type, so templates inserted by hand (mongosh, Compass) work too.
    let version = match document.get("version")? {
        bson::Bson::Int64(v) => *v,
        bson::Bson::Int32(v) => i64::from(*v),
        bson::Bson::Double(v) => *v as i64,
        _ => return None,
    };
    let template_doc = document.get_document("template").ok()?;
    let json: Json = bson::from_document(template_doc.clone()).ok()?;
    let template: Template = serde_json::from_value(json).ok()?;
    Some(StoredTemplate { version, template })
}

/// Stores `template` as the next version for `app_id` and returns that version.
pub async fn publish(state: &AppState, app_id: &str, template: &Template, published_by: &str) -> Result<i64, crate::BoxError> {
    template.validate()?;
    let template_json = serde_json::to_value(template)?;
    let template_doc = bson::to_document(&template_json)?;
    for _ in 0..5 {
        let next = latest_template(state, app_id).await?.map_or(1, |t| t.version + 1);
        let insert = state
            .db
            .rc_templates
            .insert_one(doc! {
                "app_id": app_id,
                "version": next,
                "template": &template_doc,
                "description": template.description.clone(),
                "published_by": published_by,
                "created_at": DateTime::now(),
            })
            .await;
        match insert {
            Ok(_) => {
                state.rc.invalidate(app_id);
                return Ok(next);
            }
            Err(err) if is_duplicate_key(&err) => continue, // concurrent publish took this number
            Err(err) => return Err(err.into()),
        }
    }
    Err("could not allocate a template version (concurrent publishes)".into())
}

// ── Cache + throttling ───────────────────────────────────────────────────────

const CACHE_TTL: Duration = Duration::from_secs(30);
const WINDOW: Duration = Duration::from_secs(3600);

#[derive(Default)]
pub struct RcState {
    cache: Mutex<HashMap<String, (Instant, Option<Arc<StoredTemplate>>)>>,
    fetches: Mutex<HashMap<(String, bool), Vec<Instant>>>,
}

impl RcState {
    pub fn invalidate(&self, app_id: &str) {
        self.cache.lock().expect("rc cache").remove(app_id);
    }

    async fn template(&self, state: &AppState, app_id: &str) -> Result<Option<Arc<StoredTemplate>>, mongodb::error::Error> {
        if let Some((at, cached)) = self.cache.lock().expect("rc cache").get(app_id) {
            if at.elapsed() < CACHE_TTL {
                return Ok(cached.clone());
            }
        }
        let fresh = latest_template(state, app_id).await?.map(Arc::new);
        self.cache.lock().expect("rc cache").insert(app_id.to_string(), (Instant::now(), fresh.clone()));
        Ok(fresh)
    }

    /// Sliding one-hour window per installation. Err(seconds until a slot frees up).
    fn admit(&self, installation_id: &str, realtime: bool, limit: u32) -> Result<(), u64> {
        if limit == 0 {
            return Ok(());
        }
        let now = Instant::now();
        let mut map = self.fetches.lock().expect("rc throttle");
        if map.len() > 100_000 {
            map.retain(|_, times| times.last().is_some_and(|t| now.duration_since(*t) < WINDOW));
        }
        let times = map.entry((installation_id.to_string(), realtime)).or_default();
        times.retain(|t| now.duration_since(*t) < WINDOW);
        if times.len() >= limit as usize {
            let wait = WINDOW.saturating_sub(now.duration_since(times[0]));
            return Err(wait.as_secs().max(1));
        }
        times.push(now);
        Ok(())
    }
}

// ── RPC ──────────────────────────────────────────────────────────────────────

pub async fn fetch_config(state: &AppState, meta: &RequestMeta, request: FetchConfigRequest) -> Result<FetchConfigResponse, Status> {
    let context = require_context(request.context.clone(), meta)?;
    authorize(&state.config, meta, &context.app_id)?;

    let limit = if request.is_realtime_check { state.config.rc_realtime_checks_per_hour } else { state.config.rc_fetches_per_hour };
    if let Err(retry_after) = state.rc.admit(&context.installation_id, request.is_realtime_check, limit) {
        let mut status = Status::resource_exhausted("too many Remote Config fetches for this installation");
        if let Ok(value) = MetadataValue::try_from(retry_after.to_string()) {
            status.metadata_mut().insert("retry-after-seconds", value);
        }
        return Err(status);
    }

    let Some(stored) = state.rc.template(state, &context.app_id).await.map_err(unavailable)? else {
        return Ok(FetchConfigResponse { state: FetchState::NoTemplate as i32, ..Default::default() });
    };

    // Real-time checks are cheap when nothing was published since the device's last fetch.
    if request.is_realtime_check && stored.version <= request.last_template_version {
        return Ok(FetchConfigResponse {
            state: FetchState::NoChange as i32,
            template_version: stored.version,
            ..Default::default()
        });
    }

    let ctx = eval_context(&request);
    let (entries, experiments) = stored.template.evaluate(&ctx);
    Ok(FetchConfigResponse {
        state: FetchState::Updated as i32,
        entries,
        template_version: stored.version,
        experiments: experiments
            .into_iter()
            .map(|(experiment_id, variant_id)| Experiment { experiment_id, variant_id })
            .collect(),
    })
}

pub fn eval_context(request: &FetchConfigRequest) -> EvalContext {
    let c = request.context.clone().unwrap_or_default();
    let country = if request.country_code.is_empty() {
        // Fall back to the locale's region ("en_IN" → "IN").
        c.locale.rsplit(['_', '-']).next().filter(|r| r.len() == 2).unwrap_or_default().to_string()
    } else {
        request.country_code.clone()
    };
    EvalContext {
        app_id: c.app_id,
        installation_id: c.installation_id,
        user_id: c.user_id,
        bundle_id: c.bundle_id,
        platform: c.platform,
        app_version: c.app_version,
        app_build: c.app_build,
        os_version: c.os_version,
        device_model: c.device_model,
        locale: c.locale,
        language: request.language_code.clone(),
        country,
        user_properties: request.user_properties.clone(),
        custom_signals: request.custom_signals.clone(),
        now_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn template() -> Template {
        serde_json::from_value(json!({
            "conditions": [
                {"name": "india", "expression": {"country": {"in": ["IN"]}}},
                {"name": "gold", "expression": {"user_property": {"name": "tier", "eq": "gold"}}}
            ],
            "parameters": {
                "banner": {"default_value": false, "conditional_values": [
                    {"condition": "gold", "value": "gold"}, {"condition": "india", "value": true}
                ]},
                "slots": {"default_value": 30},
                "layout": {"default_value": {"rows": 3}},
                "gold_only": {"conditional_values": [{"condition": "gold", "value": "yes"}]}
            },
            "experiments": [{"experiment_id": "exp", "variants": [
                {"variant_id": "A", "weight": 1, "values": {"color": "green"}},
                {"variant_id": "B", "weight": 1, "values": {"color": "orange"}}
            ]}]
        }))
        .unwrap()
    }

    #[test]
    fn first_matching_condition_in_template_order_wins() {
        let t = template();
        t.validate().unwrap();
        let mut ctx = EvalContext { installation_id: "i1".into(), country: "IN".into(), ..Default::default() };
        ctx.user_properties.insert("tier".into(), "gold".into());
        let (entries, experiments) = t.evaluate(&ctx);
        assert_eq!(entries["banner"], "true"); // "india" is listed before "gold"
        assert_eq!(entries["slots"], "30");
        assert_eq!(entries["layout"], "{\"rows\":3}");
        assert_eq!(entries["gold_only"], "yes");
        assert_eq!(experiments.len(), 1);
        assert!(["green", "orange"].contains(&entries["color"].as_str()));
    }

    #[test]
    fn no_default_means_absent() {
        let (entries, _) = template().evaluate(&EvalContext { installation_id: "i2".into(), ..Default::default() });
        assert_eq!(entries["banner"], "false");
        assert!(!entries.contains_key("gold_only"));
    }

    #[test]
    fn variant_assignment_is_sticky_and_split() {
        let t = template();
        let mut counts = HashMap::new();
        for i in 0..2000 {
            let ctx = EvalContext { installation_id: format!("install-{i}"), ..Default::default() };
            let (_, a) = t.evaluate(&ctx);
            let (_, b) = t.evaluate(&ctx);
            assert_eq!(a, b);
            *counts.entry(a[0].1.clone()).or_insert(0) += 1;
        }
        assert!(counts["A"] > 800 && counts["B"] > 800, "{counts:?}");
    }

    #[test]
    fn validation_catches_dangling_condition() {
        let bad: Template = serde_json::from_value(json!({
            "parameters": {"x": {"conditional_values": [{"condition": "nope", "value": 1}]}}
        }))
        .unwrap();
        assert!(bad.validate().is_err());
    }

    #[test]
    fn throttle_window() {
        let rc = RcState::default();
        assert!(rc.admit("i", false, 2).is_ok());
        assert!(rc.admit("i", false, 2).is_ok());
        assert!(rc.admit("i", false, 2).is_err());
        assert!(rc.admit("i", true, 2).is_ok()); // separate budget for realtime checks
    }
}
