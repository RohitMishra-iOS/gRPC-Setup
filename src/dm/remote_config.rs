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
//!
//! Console metadata (all optional, ignored by devices): parameter `value_type`
//! (STRING | NUMBER | BOOLEAN | JSON, validated on publish), condition `color`, experiment
//! `name`/`description`, and `updated_at`/`updated_by` on parameters and conditions, which the
//! server stamps on publish for whatever changed since the previous version.
//!
//! Rollouts progressively release parameter values to a percentage of devices:
//!   "rollouts": [{"rollout_id": "new_checkout", "name": "New checkout", "active": true,
//!                 "condition": "india_v2", "percent": 10, "values": {"new_checkout": true}}]
//! Evaluation order per parameter: experiment variant > rollout > first matching conditional
//! value > default.

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
    #[serde(default)]
    pub rollouts: Vec<RolloutDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamedCondition {
    pub name: String,
    pub expression: Json,
    /// Console tag colour (e.g. "indigo").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Parameter {
    #[serde(default, deserialize_with = "optional_value_text", skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(default)]
    pub conditional_values: Vec<ConditionalValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// STRING | NUMBER | BOOLEAN | JSON — values are checked against it on publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutDef {
    pub rollout_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// false = paused or completed (no longer served).
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// Share of (matching) devices that get the values, 0–100. Raising it keeps earlier devices in.
    pub percent: f64,
    #[serde(default, deserialize_with = "value_text_map")]
    pub values: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
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
            if let Some(kind) = &parameter.value_type {
                let values = parameter.default_value.iter().chain(parameter.conditional_values.iter().map(|cv| &cv.value));
                for value in values {
                    check_type(key, kind, value)?;
                }
            }
        }
        let check_override = |owner: &str, key: &str, value: &String| -> Result<(), String> {
            let parameter = self
                .parameters
                .get(key)
                .ok_or_else(|| format!("{owner} sets unknown parameter '{key}' (add the parameter first)"))?;
            match &parameter.value_type {
                Some(kind) => check_type(key, kind, value),
                None => Ok(()),
            }
        };
        let mut rollout_ids = std::collections::HashSet::new();
        for rollout in &self.rollouts {
            if rollout.rollout_id.is_empty() || !rollout_ids.insert(rollout.rollout_id.as_str()) {
                return Err(format!("rollout ids must be unique and non-empty ('{}')", rollout.rollout_id));
            }
            if !(0.0..=100.0).contains(&rollout.percent) {
                return Err(format!("rollout '{}' percent must be 0–100", rollout.rollout_id));
            }
            if rollout.values.is_empty() {
                return Err(format!("rollout '{}' must set at least one parameter", rollout.rollout_id));
            }
            if let Some(condition) = &rollout.condition {
                if !names.contains(condition.as_str()) {
                    return Err(format!("rollout '{}' references unknown condition '{condition}'", rollout.rollout_id));
                }
            }
            for (key, value) in &rollout.values {
                check_override(&format!("rollout '{}'", rollout.rollout_id), key, value)?;
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
            // Experiments may introduce parameters of their own (as in 2.0); typed ones are checked.
            for variant in &experiment.variants {
                for (key, value) in &variant.values {
                    if let Some(kind) = self.parameters.get(key).and_then(|p| p.value_type.as_ref()) {
                        check_type(key, kind, value)?;
                    }
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

        for rollout in self.rollouts.iter().filter(|r| r.active) {
            if let Some(condition) = &rollout.condition {
                if !matched.get(condition.as_str()).copied().unwrap_or(false) {
                    continue;
                }
            }
            if percent_bucket(&format!("{}.rollout", rollout.rollout_id), &ctx.installation_id) < rollout.percent {
                for (key, value) in &rollout.values {
                    entries.insert(key.clone(), value.clone());
                }
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

fn check_type(key: &str, kind: &str, value: &str) -> Result<(), String> {
    let ok = match kind {
        "STRING" => true,
        "NUMBER" => value.trim().parse::<f64>().is_ok(),
        "BOOLEAN" => matches!(value, "true" | "false"),
        "JSON" => serde_json::from_str::<Json>(value).is_ok(),
        other => return Err(format!("parameter '{key}' has unknown value_type '{other}'")),
    };
    if ok {
        Ok(())
    } else {
        Err(format!("parameter '{key}' is {kind} but has value {value:?}"))
    }
}

/// Copies `updated_at`/`updated_by` from `previous` for parameters and conditions that didn't
/// change, and stamps the ones that did with `now`/`by`.
pub fn stamp_changes(template: &mut Template, previous: Option<&Template>, by: &str, now: &str) {
    fn content<T: Serialize + Clone>(value: &T, strip: impl Fn(&mut T)) -> Json {
        let mut copy = value.clone();
        strip(&mut copy);
        serde_json::to_value(copy).unwrap_or(Json::Null)
    }
    let strip_param = |p: &mut Parameter| {
        p.updated_at = None;
        p.updated_by = None;
    };
    let strip_condition = |c: &mut NamedCondition| {
        c.updated_at = None;
        c.updated_by = None;
    };
    for (key, parameter) in template.parameters.iter_mut() {
        let old = previous.and_then(|t| t.parameters.get(key));
        match old {
            Some(old) if content(old, strip_param) == content(parameter, strip_param) && old.updated_at.is_some() => {
                parameter.updated_at = old.updated_at.clone();
                parameter.updated_by = old.updated_by.clone();
            }
            _ => {
                parameter.updated_at = Some(now.to_string());
                parameter.updated_by = Some(by.to_string());
            }
        }
    }
    for condition in template.conditions.iter_mut() {
        let old = previous.and_then(|t| t.conditions.iter().find(|c| c.name == condition.name));
        match old {
            Some(old) if content(old, strip_condition) == content(condition, strip_condition) && old.updated_at.is_some() => {
                condition.updated_at = old.updated_at.clone();
                condition.updated_by = old.updated_by.clone();
            }
            _ => {
                condition.updated_at = Some(now.to_string());
                condition.updated_by = Some(by.to_string());
            }
        }
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
    for _ in 0..5 {
        let previous = latest_template(state, app_id).await?;
        let next = previous.as_ref().map_or(1, |t| t.version + 1);
        let mut template = template.clone();
        let now = chrono_now();
        stamp_changes(&mut template, previous.as_ref().map(|p| &p.template), published_by, &now);
        let template_json = serde_json::to_value(&template)?;
        let template_doc = bson::to_document(&template_json)?;
        let insert = state
            .db
            .rc_templates
            .insert_one(doc! {
                "app_id": app_id,
                "version": next,
                "template": template_doc,
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

/// RFC 3339 UTC timestamp without pulling in chrono.
fn chrono_now() -> String {
    DateTime::now().try_to_rfc3339_string().unwrap_or_default()
}

// ── Cache + throttling ───────────────────────────────────────────────────────

const CACHE_TTL: Duration = Duration::from_secs(30);
const WINDOW: Duration = Duration::from_secs(3600);

#[derive(Default)]
pub struct RcState {
    cache: Mutex<HashMap<String, (Instant, Option<Arc<StoredTemplate>>)>>,
    fetches: Mutex<HashMap<(String, bool), Vec<Instant>>>,
    /// Wakes real-time checks that are waiting for a publish (long polling).
    published: tokio::sync::Notify,
}

impl RcState {
    /// Called after every publish/rollback: drops the cached template and wakes waiting devices.
    pub fn invalidate(&self, app_id: &str) {
        self.cache.lock().expect("rc cache").remove(app_id);
        self.published.notify_waiters();
    }

    /// Test hook: serve `template` for `app_id` from the cache (no MongoDB) and wake waiters.
    #[cfg(test)]
    pub(crate) fn publish_cached(&self, app_id: &str, template: Option<StoredTemplate>) {
        let entry = (Instant::now() + Duration::from_secs(3600), template.map(Arc::new));
        self.cache.lock().expect("rc cache").insert(app_id.to_string(), entry);
        self.published.notify_waiters();
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

    // Real-time checks long-poll: when nothing newer than the device's version is published, hold
    // the request open (up to DM_RC_REALTIME_HOLD_SECONDS) and answer as soon as a publish lands.
    // Devices therefore see console changes within a second, without APNs.
    let hold = if request.is_realtime_check { Duration::from_secs(state.config.rc_realtime_hold_seconds) } else { Duration::ZERO };
    let deadline = tokio::time::Instant::now() + hold;
    let stored = loop {
        // Register for the wake-up *before* reading, so a publish in between can't be missed.
        let notified = state.rc.published.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let stored = state.rc.template(state, &context.app_id).await.map_err(unavailable)?;
        let current = stored.as_ref().map_or(0, |t| t.version);
        if hold.is_zero() || current > request.last_template_version {
            break stored;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            break stored; // held long enough: answer NO_CHANGE, the device asks again
        }
    };
    if !request.is_realtime_check {
        log_fetch(state, &context.app_id, &context.installation_id);
    }
    let Some(stored) = stored else {
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

/// Records one successful fetch for the console's "Fetches" chart (fire-and-forget; 7-day TTL).
fn log_fetch(state: &AppState, app_id: &str, installation_id: &str) {
    let collection = state.db.rc_fetches.clone();
    let document = doc! { "app_id": app_id, "installation_id": installation_id, "received_at": DateTime::now() };
    tokio::spawn(async move {
        if let Err(err) = collection.insert_one(document).await {
            eprintln!("[RemoteConfig] could not log fetch: {err}");
        }
    });
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

    fn ctx_for(installation: &str) -> EvalContext {
        EvalContext { installation_id: installation.into(), ..Default::default() }
    }

    #[test]
    fn rollouts_reach_their_percentage_and_override_conditions() {
        let template: Template = serde_json::from_value(serde_json::json!({
            "conditions": [{"name": "all", "expression": {"always": true}}],
            "parameters": {"new_checkout": {"default_value": false, "value_type": "BOOLEAN",
                                            "conditional_values": [{"condition": "all", "value": false}]}},
            "rollouts": [{"rollout_id": "nc", "percent": 25, "values": {"new_checkout": true}}]
        }))
        .unwrap();
        template.validate().unwrap();
        let on = (0..2000)
            .filter(|i| template.evaluate(&ctx_for(&format!("install-{i}"))).0["new_checkout"] == "true")
            .count();
        assert!((400..600).contains(&on), "{on} of 2000 in a 25% rollout");

        let mut paused = template.clone();
        paused.rollouts[0].active = false;
        assert!((0..200).all(|i| paused.evaluate(&ctx_for(&format!("install-{i}"))).0["new_checkout"] == "false"));
    }

    #[test]
    fn value_types_are_enforced() {
        let bad: Template = serde_json::from_value(serde_json::json!({
            "parameters": {"slots": {"default_value": "thirty", "value_type": "NUMBER"}}
        }))
        .unwrap();
        assert!(bad.validate().unwrap_err().contains("NUMBER"));
        let bad_rollout: Template = serde_json::from_value(serde_json::json!({
            "parameters": {"flag": {"default_value": false, "value_type": "BOOLEAN"}},
            "rollouts": [{"rollout_id": "r", "percent": 5, "values": {"flag": "yes"}}]
        }))
        .unwrap();
        assert!(bad_rollout.validate().is_err());
        let unknown: Template = serde_json::from_value(serde_json::json!({
            "rollouts": [{"rollout_id": "r", "percent": 5, "values": {"missing": "1"}}]
        }))
        .unwrap();
        assert!(unknown.validate().unwrap_err().contains("unknown parameter"));
    }

    #[test]
    fn publish_stamps_only_changed_entries() {
        let v1: Template = serde_json::from_value(serde_json::json!({
            "conditions": [{"name": "in", "expression": {"country": {"eq": "IN"}}, "color": "orange"}],
            "parameters": {"a": {"default_value": "1"}, "b": {"default_value": "2"}}
        }))
        .unwrap();
        let mut first = v1.clone();
        stamp_changes(&mut first, None, "alice", "2026-10-01T00:00:00Z");
        let mut second = first.clone();
        second.parameters.get_mut("b").unwrap().default_value = Some("3".into());
        stamp_changes(&mut second, Some(&first), "bob", "2026-10-02T00:00:00Z");
        assert_eq!(second.parameters["a"].updated_by.as_deref(), Some("alice"));
        assert_eq!(second.parameters["b"].updated_by.as_deref(), Some("bob"));
        assert_eq!(second.conditions[0].updated_at.as_deref(), Some("2026-10-01T00:00:00Z"));
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
