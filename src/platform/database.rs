//! Realtime document database — the core of Cloud Firestore (and what Realtime Database is used
//! for): documents in collections, subcollections, typed values, atomic batched writes,
//! optimistic transactions, compound queries with cursors, aggregation, and live listeners over a
//! WebSocket. Every read and write is checked against the database security rules (rules.rs).
//!
//! Storage: MongoDB `db_documents`, one BSON document per database document:
//!   {_id: "users/u1/posts/p1", collection: "users/u1/posts", collection_id: "posts",
//!    data: {...}, create_time: Date, update_time: i64 (µs, the version)}
//!
//! ── Values ──
//! Plain JSON (strings, numbers, booleans, null, arrays, maps) plus typed values:
//!   {"__type": "timestamp", "value": "2026-10-29T10:00:00.000Z"}
//!   {"__type": "bytes", "base64": "…"}     {"__type": "ref", "path": "users/u1"}
//!   {"__type": "geopoint", "latitude": 19.07, "longitude": 72.87}
//! Field transforms inside written data:
//!   {"__op": "serverTimestamp"}   {"__op": "increment", "by": 1}   {"__op": "delete"}
//!   {"__op": "arrayUnion", "values": [...]}   {"__op": "arrayRemove", "values": [...]}
//!
//! ── REST ──
//! GET    /v1/db/documents/*path[?transaction=id]          one document
//! POST   /v1/db/documents/*collection     {data}          create with an auto id
//! POST   /v1/db/commit       {writes: [Write…], transaction?}          atomic (≤ 500 writes)
//!        Write = {op: "set", path, data, merge?: bool, mergeFields?: [..]}
//!              | {op: "update", path, data}        (keys may be dotted field paths "a.b")
//!              | {op: "delete", path}
//!              + optional precondition {exists: bool} | {updateTime: µs}
//! POST   /v1/db/query        Query                       → {documents: [...]}
//! POST   /v1/db/aggregate    {query: Query, aggregations: [{alias, op: count|sum|avg, field?}]}
//! POST   /v1/db/beginTransaction                          → {transaction}
//! POST   /v1/db/rollback     {transaction}
//!   Query = {collection: "users/u1/posts" | collectionGroup: "posts",
//!            where?: Filter, orderBy?: [{field, direction: "asc"|"desc"}],
//!            limit?, limitToLast?, startAt?|startAfter?: [values], endAt?|endBefore?: [values],
//!            select?: [fields]}
//!   Filter = {field, op, value} | {and: [Filter…]} | {or: [Filter…]}
//!   op: == != < <= > >= array-contains array-contains-any in not-in
//!   Use field "__name__" for the document path.
//!
//! ── Live listeners ──
//! WebSocket /v1/db/listen. Browsers can't set headers on a WebSocket, so the first message
//! authenticates: {"type": "auth", "apiKey", "appId", "idToken"?, "appCheck"?}. Then:
//!   → {"type": "listen", "targetId": 1, "document": "users/u1"}
//!   → {"type": "listen", "targetId": 2, "query": Query}
//!   → {"type": "unlisten", "targetId": 2}         → {"type": "auth", "idToken"} (token refresh)
//!   ← {"type": "snapshot", "targetId", "documents": [...], "changes": [{type: added|modified|removed, path, oldIndex, newIndex}], "readTime"}
//!   ← {"type": "error", "targetId", "code", "message"}   (the target is removed)
//! Changes are fanned out in-process; with DM_DB_CHANGE_STREAMS=true they come from a MongoDB
//! change stream instead, so several server instances stay in sync.

use std::collections::{BTreeMap, HashMap};

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use base64::Engine;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use mongodb::bson::{self, doc, Binary, Bson, DateTime, Document};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json};
use tokio::sync::broadcast;

use super::rules::{self, Op};
use super::{caller, ok, ApiError, ApiResult, Caller};
use crate::dm::AppState;

pub const DOCUMENTS: &str = "db_documents";
pub const TRANSACTIONS: &str = "db_transactions";
const MAX_WRITES: usize = 500;
const MAX_DOC_BYTES: usize = 1024 * 1024;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/db/documents/*path", get(get_document).post(add_document))
        .route("/db/commit", post(commit_route))
        .route("/db/query", post(query_route))
        .route("/db/aggregate", post(aggregate_route))
        .route("/db/beginTransaction", post(begin_transaction))
        .route("/db/rollback", post(rollback))
        .route("/db/listen", get(listen))
}

pub async fn ensure_schema(db: &crate::db::Db) -> Result<(), crate::BoxError> {
    use crate::db::index;
    index(&db.coll(DOCUMENTS), doc! { "collection": 1 }, "collection", false).await?;
    index(&db.coll(DOCUMENTS), doc! { "collection_id": 1 }, "collection_id", false).await?;
    index(&db.coll(DOCUMENTS), doc! { "collection": 1, "update_time": -1 }, "collection_updated", false).await?;
    super::ttl_on(db, TRANSACTIONS, "expires_at").await;
    Ok(())
}

// ── Paths ────────────────────────────────────────────────────────────────────

fn segments(path: &str) -> Vec<&str> {
    path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect()
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty() && s.len() <= 1500 && s != "." && s != ".." && !(s.starts_with("__") && s.ends_with("__")) && !s.contains('\0')
}

/// "users/u1" → ("users", "users", "u1"). Document paths have an even number of segments.
pub fn document_path(path: &str) -> ApiResult<(String, String, String)> {
    let segs = segments(path);
    if segs.is_empty() || segs.len() % 2 != 0 || !segs.iter().all(|s| valid_segment(s)) {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("'{path}' is not a document path (collection/doc[/collection/doc…])")));
    }
    let collection = segs[..segs.len() - 1].join("/");
    Ok((segs.join("/"), collection, segs[segs.len() - 2].to_string()))
}

pub fn collection_path(path: &str) -> ApiResult<String> {
    let segs = segments(path);
    if segs.is_empty() || segs.len() % 2 != 1 || !segs.iter().all(|s| valid_segment(s)) {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("'{path}' is not a collection path")));
    }
    Ok(segs.join("/"))
}

pub fn auto_id() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..20).map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char).collect()
}

// ── JSON ⇄ BSON values ───────────────────────────────────────────────────────

fn valid_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('.') && !key.starts_with('$') && !(key.starts_with("__") && key != "__type")
}

/// JSON → BSON (typed values decoded). Transforms must already have been extracted.
pub fn to_bson(value: &Json) -> Result<Bson, String> {
    Ok(match value {
        Json::Null => Bson::Null,
        Json::Bool(b) => Bson::Boolean(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Bson::Int64(i),
            None => Bson::Double(n.as_f64().unwrap_or(0.0)),
        },
        Json::String(s) => Bson::String(s.clone()),
        Json::Array(items) => {
            if items.iter().any(Json::is_array) {
                return Err("arrays cannot directly contain arrays".into());
            }
            Bson::Array(items.iter().map(to_bson).collect::<Result<_, _>>()?)
        }
        Json::Object(map) => match map.get("__type").and_then(Json::as_str) {
            Some("timestamp") => {
                let text = map.get("value").and_then(Json::as_str).ok_or("timestamp needs value")?;
                let ms = chrono::DateTime::parse_from_rfc3339(text).map_err(|e| format!("timestamp: {e}"))?.timestamp_millis();
                Bson::DateTime(DateTime::from_millis(ms))
            }
            Some("bytes") => {
                let text = map.get("base64").and_then(Json::as_str).ok_or("bytes needs base64")?;
                let bytes = base64::engine::general_purpose::STANDARD.decode(text).map_err(|e| format!("bytes: {e}"))?;
                Bson::Binary(Binary { subtype: bson::spec::BinarySubtype::Generic, bytes })
            }
            Some("ref") => {
                let path = map.get("path").and_then(Json::as_str).ok_or("ref needs path")?;
                document_path(path).map_err(|e| e.message)?;
                Bson::Document(doc! { "__type": "ref", "path": path })
            }
            Some("geopoint") => {
                let lat = map.get("latitude").and_then(Json::as_f64).ok_or("geopoint needs latitude")?;
                let lng = map.get("longitude").and_then(Json::as_f64).ok_or("geopoint needs longitude")?;
                if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lng) {
                    return Err("geopoint out of range".into());
                }
                Bson::Document(doc! { "__type": "geopoint", "latitude": lat, "longitude": lng })
            }
            Some(other) => return Err(format!("unknown __type '{other}'")),
            None => {
                let mut out = Document::new();
                for (k, v) in map {
                    if !valid_key(k) {
                        return Err(format!("invalid field name '{k}' (no dots, no leading $ or __)"));
                    }
                    if v.get("__op").is_some() {
                        return Err(format!("field transform at '{k}' is not allowed here"));
                    }
                    out.insert(k.clone(), to_bson(v)?);
                }
                Bson::Document(out)
            }
        },
    })
}

/// BSON → JSON (typed values encoded).
pub fn to_json(value: &Bson) -> Json {
    match value {
        Bson::Null | Bson::Undefined => Json::Null,
        Bson::Boolean(b) => Json::Bool(*b),
        Bson::Int32(i) => json!(i),
        Bson::Int64(i) => json!(i),
        Bson::Double(d) => json!(d),
        Bson::String(s) => Json::String(s.clone()),
        Bson::Array(items) => Json::Array(items.iter().map(to_json).collect()),
        Bson::DateTime(dt) => json!({ "__type": "timestamp", "value": dt.try_to_rfc3339_string().unwrap_or_default() }),
        Bson::Binary(b) => json!({ "__type": "bytes", "base64": base64::engine::general_purpose::STANDARD.encode(&b.bytes) }),
        Bson::Document(d) => Json::Object(d.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
        other => Json::String(other.to_string()),
    }
}

/// Values for rules and comparisons: timestamps as epoch ms, refs as their path.
fn rules_value(value: &Bson) -> Json {
    match value {
        Bson::DateTime(dt) => json!(dt.timestamp_millis()),
        Bson::Document(d) if d.get_str("__type") == Ok("ref") => json!(d.get_str("path").unwrap_or_default()),
        Bson::Document(d) => Json::Object(d.iter().map(|(k, v)| (k.clone(), rules_value(v))).collect()),
        Bson::Array(items) => Json::Array(items.iter().map(rules_value).collect()),
        other => to_json(other),
    }
}

pub fn document_json(stored: &Document) -> Json {
    let data = stored.get_document("data").map(|d| to_json(&Bson::Document(d.clone()))).unwrap_or(json!({}));
    json!({
        "path": stored.get_str("_id").ok(),
        "id": stored.get_str("_id").ok().and_then(|p| p.rsplit('/').next()),
        "data": data,
        "createTime": stored.get_datetime("create_time").ok().and_then(|d| d.try_to_rfc3339_string().ok()),
        "updateTime": stored.get_i64("update_time").unwrap_or(0),
    })
}

// ── Transforms ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Transform {
    ServerTimestamp,
    Increment(Json),
    ArrayUnion(Vec<Json>),
    ArrayRemove(Vec<Json>),
    Delete,
}

/// Splits written data into plain values (dotted path → value) and transforms. `nested` keys
/// are flattened into dotted paths when `flatten` (update semantics) is set.
fn split_write(prefix: &str, data: &Map<String, Json>, flatten_all: bool, out: &mut Vec<(String, Json)>, transforms: &mut Vec<(String, Transform)>) -> Result<(), String> {
    for (key, value) in data {
        let path = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
        if value.get("__op").is_some() {
            transforms.push((path, parse_transform(value)?));
        } else if flatten_all && value.is_object() && value.get("__type").is_none() && contains_transform(value) {
            split_write(&path, value.as_object().unwrap(), flatten_all, out, transforms)?;
        } else if !flatten_all && value.is_object() && value.get("__type").is_none() && contains_transform(value) {
            // set(): nested maps that contain transforms are written as maps with the transforms
            // applied afterwards.
            let mut plain = Map::new();
            let mut inner_out = Vec::new();
            split_write(&path, value.as_object().unwrap(), false, &mut inner_out, transforms)?;
            for (p, v) in inner_out {
                plain.insert(p[path.len() + 1..].to_string(), v);
            }
            out.push((path, Json::Object(unflatten(plain))));
        } else {
            out.push((path, value.clone()));
        }
    }
    Ok(())
}

fn parse_transform(value: &Json) -> Result<Transform, String> {
    Ok(match value.get("__op").and_then(Json::as_str).unwrap_or("") {
        "serverTimestamp" => Transform::ServerTimestamp,
        "increment" => Transform::Increment(value.get("by").cloned().filter(Json::is_number).ok_or("increment needs a numeric 'by'")?),
        "arrayUnion" => Transform::ArrayUnion(value.get("values").and_then(Json::as_array).cloned().ok_or("arrayUnion needs values")?),
        "arrayRemove" => Transform::ArrayRemove(value.get("values").and_then(Json::as_array).cloned().ok_or("arrayRemove needs values")?),
        "delete" => Transform::Delete,
        other => return Err(format!("unknown field transform '{other}'")),
    })
}

fn contains_transform(value: &Json) -> bool {
    match value {
        Json::Object(map) => map.contains_key("__op") || map.values().any(contains_transform),
        _ => false,
    }
}

fn unflatten(flat: Map<String, Json>) -> Map<String, Json> {
    let mut out = Map::new();
    for (path, value) in flat {
        let parts: Vec<&str> = path.split('.').collect();
        let mut cursor = &mut out;
        for part in &parts[..parts.len() - 1] {
            cursor = cursor.entry(part.to_string()).or_insert_with(|| json!({})).as_object_mut().unwrap();
        }
        cursor.insert(parts[parts.len() - 1].to_string(), value);
    }
    out
}

fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let mut current = doc.get(parts.next()?)?;
    for part in parts {
        current = current.as_document()?.get(part)?;
    }
    Some(current)
}

fn set_path(doc: &mut Document, path: &str, value: Bson) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut cursor = doc;
    for part in &parts[..parts.len() - 1] {
        if !matches!(cursor.get(*part), Some(Bson::Document(_))) {
            cursor.insert(part.to_string(), Document::new());
        }
        cursor = cursor.get_document_mut(part).unwrap();
    }
    cursor.insert(parts[parts.len() - 1].to_string(), value);
}

fn remove_path(doc: &mut Document, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut cursor = doc;
    for part in &parts[..parts.len() - 1] {
        match cursor.get_document_mut(part) {
            Ok(next) => cursor = next,
            Err(_) => return,
        }
    }
    cursor.remove(parts[parts.len() - 1]);
}

fn apply_transform(doc: &mut Document, path: &str, transform: &Transform, now: DateTime) -> Result<(), String> {
    match transform {
        Transform::ServerTimestamp => set_path(doc, path, Bson::DateTime(now)),
        Transform::Delete => remove_path(doc, path),
        Transform::Increment(by) => {
            let current = get_path(doc, path).cloned();
            let value = match (current, by.as_i64()) {
                (Some(Bson::Int64(a)), Some(b)) => Bson::Int64(a.saturating_add(b)),
                (Some(Bson::Int32(a)), Some(b)) => Bson::Int64(i64::from(a).saturating_add(b)),
                (Some(Bson::Double(a)), _) => Bson::Double(a + by.as_f64().unwrap_or(0.0)),
                (Some(Bson::Int64(a)), None) => Bson::Double(a as f64 + by.as_f64().unwrap_or(0.0)),
                (_, _) => to_bson(by)?, // missing / non-numeric → set to the increment
            };
            set_path(doc, path, value);
        }
        Transform::ArrayUnion(values) => {
            let mut items = match get_path(doc, path) {
                Some(Bson::Array(a)) => a.clone(),
                _ => vec![],
            };
            for v in values {
                let b = to_bson(v)?;
                if !items.contains(&b) {
                    items.push(b);
                }
            }
            set_path(doc, path, Bson::Array(items));
        }
        Transform::ArrayRemove(values) => {
            let remove: Vec<Bson> = values.iter().map(to_bson).collect::<Result<_, _>>()?;
            let items = match get_path(doc, path) {
                Some(Bson::Array(a)) => a.iter().filter(|x| !remove.contains(x)).cloned().collect(),
                _ => vec![],
            };
            set_path(doc, path, Bson::Array(items));
        }
    }
    Ok(())
}

// ── Writes ───────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Write {
    pub op: String,
    pub path: String,
    #[serde(default)]
    pub data: Option<Map<String, Json>>,
    #[serde(default)]
    pub merge: bool,
    #[serde(default)]
    pub merge_fields: Option<Vec<String>>,
    #[serde(default)]
    pub precondition: Option<Precondition>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Precondition {
    pub exists: Option<bool>,
    pub update_time: Option<i64>,
}

/// One planned write: the final stored document (None = delete) and what rules need.
struct Planned {
    path: String,
    collection: String,
    collection_id: String,
    after: Option<Document>,
    op: Op,
}

fn plan(write: &Write, before: Option<Document>, now: DateTime) -> ApiResult<Planned> {
    let (path, collection, collection_id) = document_path(&write.path)?;
    let bad = |m: String| ApiError::bad_request("INVALID_ARGUMENT", format!("{}: {m}", write.path));
    if let Some(pre) = &write.precondition {
        if let Some(exists) = pre.exists {
            if exists != before.is_some() {
                return Err(ApiError::new(if exists { StatusCode::NOT_FOUND } else { StatusCode::CONFLICT }, if exists { "NOT_FOUND" } else { "ALREADY_EXISTS" }, format!("{path} {}", if exists { "does not exist" } else { "already exists" })));
            }
        }
        if let Some(version) = pre.update_time {
            if before.as_ref().and_then(|b| b.get_i64("update_time").ok()) != Some(version) {
                return Err(ApiError::new(StatusCode::PRECONDITION_FAILED, "FAILED_PRECONDITION", format!("{path} was modified")));
            }
        }
    }
    let previous_data = before.as_ref().and_then(|b| b.get_document("data").ok().cloned());
    let (after, op) = match write.op.as_str() {
        "delete" => (None, Op::Delete),
        "set" | "update" => {
            let data = write.data.clone().unwrap_or_default();
            let mut plain = Vec::new();
            let mut transforms = Vec::new();
            let is_update = write.op == "update";
            if is_update && before.is_none() {
                return Err(ApiError::not_found(format!("{path} does not exist (use set to create it)")));
            }
            if is_update {
                // Keys are field paths ("a.b.c"); values replace whole fields.
                for (key, value) in &data {
                    if key.split('.').any(|p| !valid_key(p)) {
                        return Err(bad(format!("invalid field path '{key}'")));
                    }
                    if value.get("__op").is_some() {
                        transforms.push((key.clone(), parse_transform(value).map_err(bad)?));
                    } else if value.is_object() && value.get("__type").is_none() && contains_transform(value) {
                        split_write(key, value.as_object().unwrap(), true, &mut plain, &mut transforms).map_err(bad)?;
                    } else {
                        plain.push((key.clone(), value.clone()));
                    }
                }
            } else {
                split_write("", &data, false, &mut plain, &mut transforms).map_err(bad)?;
            }
            let merge = write.merge || write.merge_fields.is_some() || is_update;
            let mut result = if merge { previous_data.clone().unwrap_or_default() } else { Document::new() };
            let allowed = write.merge_fields.clone();
            for (field, value) in plain {
                if let Some(fields) = &allowed {
                    if !fields.iter().any(|f| f == &field || field.starts_with(&format!("{f}."))) {
                        continue;
                    }
                }
                let bson_value = to_bson(&value).map_err(bad)?;
                if merge && !is_update {
                    // set(merge): nested maps merge field by field.
                    merge_into(&mut result, &field, bson_value);
                } else {
                    set_path(&mut result, &field, bson_value);
                }
            }
            for (field, transform) in &transforms {
                apply_transform(&mut result, field, transform, now).map_err(bad)?;
            }
            let op = if before.is_some() { Op::Update } else { Op::Create };
            (Some(result), op)
        }
        other => return Err(bad(format!("unknown op '{other}' (set, update, delete)"))),
    };
    if let Some(after) = &after {
        let size = bson::to_vec(after).map(|v| v.len()).unwrap_or(0);
        if size > MAX_DOC_BYTES {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("{path} exceeds 1 MiB")));
        }
    }
    Ok(Planned { path, collection, collection_id, after, op })
}

fn merge_into(target: &mut Document, path: &str, value: Bson) {
    match (get_path(target, path).cloned(), value) {
        (Some(Bson::Document(mut existing)), Bson::Document(incoming)) => {
            for (k, v) in incoming {
                merge_into(&mut existing, &k, v);
            }
            set_path(target, path, Bson::Document(existing));
        }
        (_, value) => set_path(target, path, value),
    }
}

fn data_for_rules(document: Option<&Document>) -> Json {
    match document {
        Some(d) => json!({ "data": rules_value(&Bson::Document(d.clone())), "id": Json::Null }),
        None => Json::Null,
    }
}

fn rules_context(caller: &Caller, op: Op, incoming: Json, existing: Json) -> Json {
    json!({
        "auth": rules::auth_value(caller.user.as_ref()),
        "request": { "time": chrono::Utc::now().timestamp_millis(), "method": op.name(), "resource": incoming,
                     "app": { "id": caller.app_id, "appcheck": caller.appcheck.is_some() } },
        "resource": existing,
    })
}

async fn check_rules(state: &AppState, caller: &Caller, path: &str, op: Op, incoming: Option<&Document>, existing: Option<&Document>) -> ApiResult<()> {
    if caller.app_id == "__admin__" {
        return Ok(());
    }
    let rules = super::storage::load_rules(state, "database").await;
    let mut incoming_json = data_for_rules(incoming);
    let mut existing_json = data_for_rules(existing);
    let id = Json::String(path.rsplit('/').next().unwrap_or_default().to_string());
    if !incoming_json.is_null() {
        incoming_json["id"] = id.clone();
    }
    if !existing_json.is_null() {
        existing_json["id"] = id;
    }
    rules::check(&rules, path, op, &rules_context(caller, op, incoming_json, existing_json)).map_err(ApiError::denied)
}

/// Maps MongoDB errors inside a commit: transient transaction conflicts are retried by `commit`.
fn tx_err(e: mongodb::error::Error) -> ApiError {
    if e.contains_label(mongodb::error::TRANSIENT_TRANSACTION_ERROR) || e.contains_label(mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT) {
        ApiError::new(StatusCode::CONFLICT, "TRANSIENT", "transient write conflict")
    } else {
        ApiError::db(e)
    }
}

/// Applies writes atomically (MongoDB transaction when available). Transient conflicts between
/// concurrent commits are retried here; a transaction whose reads changed fails with ABORTED
/// (the SDKs then re-run the transaction function).
pub async fn commit(state: &AppState, caller: &Caller, writes: Vec<Write>, transaction: Option<&str>) -> ApiResult<(i64, Vec<Option<Document>>)> {
    let mut delay = 10u64;
    for attempt in 1..=8 {
        match commit_once(state, caller, writes.clone(), transaction).await {
            Err(e) if e.reason == "TRANSIENT" && attempt < 8 => {
                tokio::time::sleep(std::time::Duration::from_millis(delay + rand::random::<u64>() % delay)).await;
                delay = (delay * 2).min(500);
            }
            Err(e) if e.reason == "TRANSIENT" => return Err(ApiError::new(StatusCode::CONFLICT, "ABORTED", "too much contention on these documents; retry")),
            other => return other,
        }
    }
    unreachable!()
}

async fn commit_once(state: &AppState, caller: &Caller, writes: Vec<Write>, transaction: Option<&str>) -> ApiResult<(i64, Vec<Option<Document>>)> {
    if writes.is_empty() {
        return Ok((micros_now(), vec![]));
    }
    if writes.len() > MAX_WRITES {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("at most {MAX_WRITES} writes per commit")));
    }
    let coll = state.db.coll(DOCUMENTS);
    let client = state.db.database.client();
    let mut session = client.start_session().await.map_err(ApiError::db)?;
    let use_tx = session.start_transaction().await.is_ok();

    let result: ApiResult<(i64, Vec<Planned>)> = async {
        // Optimistic transaction: every document read inside it must be unchanged.
        if let Some(id) = transaction {
            let tx = state.db.coll(TRANSACTIONS).find_one(doc! { "_id": id }).await.map_err(ApiError::db)?.ok_or_else(|| ApiError::new(StatusCode::CONFLICT, "ABORTED", "transaction expired; retry"))?;
            if tx.get_str("uid").ok() != caller.uid() {
                return Err(ApiError::denied("transaction belongs to another user"));
            }
            for read in tx.get_array("reads").cloned().unwrap_or_default().iter().filter_map(Bson::as_document) {
                let path = read.get_str("path").unwrap_or_default();
                let expected = read.get_i64("version").unwrap_or(-1);
                let current = coll.find_one(doc! { "_id": path }).session(&mut session).await.map_err(tx_err)?.map(|d| d.get_i64("update_time").unwrap_or(0)).unwrap_or(-1);
                if current != expected {
                    return Err(ApiError::new(StatusCode::CONFLICT, "ABORTED", format!("{path} changed during the transaction; retry")));
                }
            }
        }
        let now_dt = DateTime::now();
        let version = micros_now();
        let mut planned: Vec<Planned> = Vec::new();
        // Later writes in the batch see earlier ones (same doc written twice).
        let mut pending: HashMap<String, Option<Document>> = HashMap::new();
        for write in &writes {
            let (path, _, _) = document_path(&write.path)?;
            let before_data = match pending.get(&path) {
                Some(state) => state.clone(),
                None => coll.find_one(doc! { "_id": &path }).session(&mut session).await.map_err(tx_err)?.map(|d| {
                    let mut stored = d.get_document("data").cloned().unwrap_or_default();
                    stored.insert("__version", d.get_i64("update_time").unwrap_or(0));
                    stored
                }),
            };
            // Re-wrap for plan(): it expects the stored shape.
            let before = before_data.map(|mut data| {
                let v = data.remove("__version").and_then(|b| b.as_i64()).unwrap_or(0);
                doc! { "data": data, "update_time": v }
            });
            let p = plan(write, before.clone(), now_dt)?;
            check_rules(state, caller, &p.path, p.op, p.after.as_ref(), before.as_ref().and_then(|b| b.get_document("data").ok())).await?;
            pending.insert(path.clone(), p.after.clone().map(|mut d| {
                d.insert("__version", version);
                d
            }));
            planned.push(p);
        }
        for p in &planned {
            match &p.after {
                None => {
                    coll.delete_one(doc! { "_id": &p.path }).session(&mut session).await.map_err(tx_err)?;
                }
                Some(data) => {
                    coll.update_one(
                        doc! { "_id": &p.path },
                        doc! {
                            "$set": { "data": data.clone(), "collection": &p.collection, "collection_id": &p.collection_id, "update_time": version },
                            "$setOnInsert": { "create_time": now_dt },
                        },
                    )
                    .upsert(true)
                    .session(&mut session)
                    .await
                    .map_err(tx_err)?;
                }
            }
        }
        Ok((version, planned))
    }
    .await;

    match result {
        Ok((version, planned)) => {
            if use_tx {
                session.commit_transaction().await.map_err(tx_err)?;
            }
            if let Some(id) = transaction {
                let _ = state.db.coll(TRANSACTIONS).delete_one(doc! { "_id": id }).await;
            }
            let mut stored = Vec::new();
            for p in &planned {
                let after = p.after.as_ref().map(|data| doc! { "_id": &p.path, "collection": &p.collection, "collection_id": &p.collection_id, "data": data.clone(), "update_time": version });
                state.live.publish(Change { path: p.path.clone(), collection: p.collection.clone(), collection_id: p.collection_id.clone() });
                stored.push(after);
            }
            Ok((version, stored))
        }
        Err(e) => {
            if use_tx {
                let _ = session.abort_transaction().await;
            }
            Err(e)
        }
    }
}

fn micros_now() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

#[derive(Deserialize)]
struct CommitBody {
    writes: Vec<Write>,
    #[serde(default)]
    transaction: Option<String>,
}

async fn commit_route(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let body: CommitBody = super::parse_json(&body)?;
    let (version, stored) = commit(&state, &caller, body.writes, body.transaction.as_deref()).await?;
    ok(json!({ "commitTime": version, "results": stored.iter().map(|_| json!({ "updateTime": version })).collect::<Vec<_>>() }))
}

async fn add_document(State(state): State<AppState>, Path(collection): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let collection = collection_path(&collection)?;
    let body: Json = super::parse_json(&body)?;
    let data = body.get("data").and_then(Json::as_object).cloned().ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "body must be {\"data\": {...}}"))?;
    let path = format!("{collection}/{}", auto_id());
    let write = Write { op: "set".into(), path: path.clone(), data: Some(data), merge: false, merge_fields: None, precondition: Some(Precondition { exists: Some(false), update_time: None }) };
    let (_, stored) = commit(&state, &caller, vec![write], None).await?;
    ok(stored.into_iter().next().flatten().map(|d| document_json(&d)).unwrap_or(Json::Null))
}

// ── Reads ────────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
struct ReadParams {
    transaction: Option<String>,
}

async fn record_read(state: &AppState, transaction: &str, path: &str, version: i64) -> ApiResult<()> {
    state
        .db
        .coll(TRANSACTIONS)
        .update_one(doc! { "_id": transaction, "reads.path": { "$ne": path } }, doc! { "$push": { "reads": { "path": path, "version": version } } })
        .await
        .map_err(ApiError::db)?;
    Ok(())
}

pub async fn read_document(state: &AppState, caller: &Caller, path: &str) -> ApiResult<Option<Document>> {
    let (path, _, _) = document_path(path)?;
    let stored = state.db.coll(DOCUMENTS).find_one(doc! { "_id": &path }).await.map_err(ApiError::db)?;
    check_rules(state, caller, &path, Op::Get, None, stored.as_ref().and_then(|d| d.get_document("data").ok())).await?;
    Ok(stored)
}

async fn get_document(State(state): State<AppState>, Path(path): Path<String>, QueryParams(params): QueryParams<ReadParams>, headers: HeaderMap) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let stored = read_document(&state, &caller, &path).await?;
    if let Some(tx) = &params.transaction {
        record_read(&state, tx, path.trim_matches('/'), stored.as_ref().map(|d| d.get_i64("update_time").unwrap_or(0)).unwrap_or(-1)).await?;
    }
    match stored {
        Some(d) => ok(json!({ "exists": true, "document": document_json(&d) })),
        None => ok(json!({ "exists": false, "path": path.trim_matches('/') })),
    }
}

async fn begin_transaction(State(state): State<AppState>, headers: HeaderMap) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let id = super::tokens::random_token();
    state
        .db
        .coll(TRANSACTIONS)
        .insert_one(doc! { "_id": &id, "uid": caller.uid(), "reads": [], "expires_at": DateTime::from_millis(chrono::Utc::now().timestamp_millis() + 270_000) })
        .await
        .map_err(ApiError::db)?;
    ok(json!({ "transaction": id }))
}

async fn rollback(State(state): State<AppState>, body: Bytes) -> ApiResult {
    let body: Json = super::parse_json(&body)?;
    if let Some(id) = body.get("transaction").and_then(Json::as_str) {
        let _ = state.db.coll(TRANSACTIONS).delete_one(doc! { "_id": id }).await;
    }
    ok(json!({}))
}

// ── Queries ──────────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct Query {
    pub collection: Option<String>,
    pub collection_group: Option<String>,
    #[serde(rename = "where")]
    pub filter: Option<Json>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<i64>,
    pub limit_to_last: Option<i64>,
    pub start_at: Option<Vec<Json>>,
    pub start_after: Option<Vec<Json>>,
    pub end_at: Option<Vec<Json>>,
    pub end_before: Option<Vec<Json>>,
    pub select: Option<Vec<String>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OrderBy {
    pub field: String,
    #[serde(default)]
    pub direction: Option<String>,
}

impl OrderBy {
    fn desc(&self) -> bool {
        matches!(self.direction.as_deref(), Some("desc") | Some("DESCENDING"))
    }
}

fn field_key(field: &str) -> ApiResult<String> {
    if field == "__name__" {
        return Ok("_id".into());
    }
    if field.split('.').any(|p| !valid_key(p)) {
        return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("invalid field path '{field}'")));
    }
    Ok(format!("data.{field}"))
}

fn query_value(field: &str, value: &Json) -> ApiResult<Bson> {
    if field == "__name__" {
        // Accept a full path or a ref.
        let path = value.as_str().map(str::to_string).or_else(|| value.get("path").and_then(Json::as_str).map(str::to_string)).ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "__name__ compares to a document path"))?;
        return Ok(Bson::String(path));
    }
    to_bson(value).map_err(|e| ApiError::bad_request("INVALID_ARGUMENT", e))
}

fn filter_doc(filter: &Json) -> ApiResult<Document> {
    if let Some(all) = filter.get("and").and_then(Json::as_array) {
        let parts: Vec<Document> = all.iter().map(filter_doc).collect::<ApiResult<_>>()?;
        return Ok(if parts.is_empty() { doc! {} } else { doc! { "$and": parts } });
    }
    if let Some(any) = filter.get("or").and_then(Json::as_array) {
        let parts: Vec<Document> = any.iter().map(filter_doc).collect::<ApiResult<_>>()?;
        return Ok(if parts.is_empty() { doc! {} } else { doc! { "$or": parts } });
    }
    let field = filter.get("field").and_then(Json::as_str).ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "filter needs field/op/value, and or or"))?;
    let op = filter.get("op").and_then(Json::as_str).unwrap_or("==");
    let value = filter.get("value").cloned().unwrap_or(Json::Null);
    let key = field_key(field)?;
    let list = |v: &Json| -> ApiResult<Vec<Bson>> {
        let items = v.as_array().ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", format!("'{op}' takes an array")))?;
        if items.len() > 30 {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("'{op}' takes at most 30 values")));
        }
        items.iter().map(|i| query_value(field, i)).collect()
    };
    Ok(match op {
        "==" => doc! { key: query_value(field, &value)? },
        "!=" => doc! { key.clone(): { "$ne": query_value(field, &value)?, "$exists": true } },
        "<" => doc! { key: { "$lt": query_value(field, &value)? } },
        "<=" => doc! { key: { "$lte": query_value(field, &value)? } },
        ">" => doc! { key: { "$gt": query_value(field, &value)? } },
        ">=" => doc! { key: { "$gte": query_value(field, &value)? } },
        "array-contains" => doc! { key: { "$elemMatch": { "$eq": query_value(field, &value)? } } },
        "array-contains-any" => doc! { key: { "$elemMatch": { "$in": list(&value)? } } },
        "in" => doc! { key: { "$in": list(&value)? } },
        "not-in" => doc! { key.clone(): { "$nin": list(&value)?, "$exists": true } },
        other => return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("unknown operator '{other}'"))),
    })
}

/// Cursor bound over the ordering fields (lexicographic), e.g. startAfter [a, b] →
/// f1 > a OR (f1 == a AND f2 > b).
fn cursor_doc(order: &[(String, bool)], values: &[Json], after: bool, inclusive: bool) -> ApiResult<Document> {
    let n = values.len().min(order.len());
    if n == 0 {
        return Ok(doc! {});
    }
    let mut ors = Vec::new();
    for i in 0..n {
        let mut and = Document::new();
        for (j, value) in values.iter().enumerate().take(i) {
            and.insert(order[j].0.clone(), query_value(if order[j].0 == "_id" { "__name__" } else { "x" }, value)?);
        }
        let (key, desc) = &order[i];
        let forward = after != *desc;
        let last = i == n - 1;
        let op = match (forward, last && inclusive) {
            (true, true) => "$gte",
            (true, false) => "$gt",
            (false, true) => "$lte",
            (false, false) => "$lt",
        };
        and.insert(key.clone(), doc! { op: query_value(if key == "_id" { "__name__" } else { "x" }, &values[i])? });
        ors.push(and);
    }
    Ok(doc! { "$or": ors })
}

/// Runs a query (no rules) and returns stored documents in order.
pub async fn run_query(state: &AppState, query: &Query) -> ApiResult<Vec<Document>> {
    let mut filter = match (&query.collection, &query.collection_group) {
        (Some(c), None) => doc! { "collection": collection_path(c)? },
        (None, Some(g)) if valid_segment(g) => doc! { "collection_id": g },
        _ => return Err(ApiError::bad_request("INVALID_ARGUMENT", "query needs exactly one of collection / collectionGroup")),
    };
    let mut ands = Vec::new();
    if let Some(f) = &query.filter {
        let d = filter_doc(f)?;
        if !d.is_empty() {
            ands.push(d);
        }
    }
    // Ordering: explicit fields, then document path for stable cursors.
    let mut order: Vec<(String, bool)> = query.order_by.iter().map(|o| Ok((field_key(&o.field)?, o.desc()))).collect::<ApiResult<_>>()?;
    let last_desc = order.last().map(|(_, d)| *d).unwrap_or(false);
    if !order.iter().any(|(k, _)| k == "_id") {
        order.push(("_id".into(), last_desc));
    }
    // Firestore semantics: ordering by a field only returns documents that have it.
    for (key, _) in &order {
        if key != "_id" {
            ands.push(doc! { key.clone(): { "$exists": true } });
        }
    }
    if let Some(v) = &query.start_at {
        ands.push(cursor_doc(&order, v, true, true)?);
    }
    if let Some(v) = &query.start_after {
        ands.push(cursor_doc(&order, v, true, false)?);
    }
    if let Some(v) = &query.end_at {
        ands.push(cursor_doc(&order, v, false, true)?);
    }
    if let Some(v) = &query.end_before {
        ands.push(cursor_doc(&order, v, false, false)?);
    }
    if !ands.is_empty() {
        filter.insert("$and", ands);
    }
    let reverse = query.limit_to_last.is_some();
    let mut sort = Document::new();
    for (key, desc) in &order {
        let desc = if reverse { !desc } else { *desc };
        sort.insert(key.clone(), if desc { -1 } else { 1 });
    }
    let limit = query.limit_to_last.or(query.limit).unwrap_or(1000).clamp(1, 10_000);
    let documents = state.db.coll(DOCUMENTS);
    let mut find = documents.find(filter).sort(sort).limit(limit);
    if let Some(fields) = &query.select {
        let mut projection = doc! { "collection": 1, "collection_id": 1, "update_time": 1, "create_time": 1 };
        for f in fields {
            projection.insert(field_key(f)?, 1);
        }
        find = find.projection(projection);
    }
    let mut docs: Vec<Document> = find.await.map_err(ApiError::db)?.try_collect().await.map_err(ApiError::db)?;
    if reverse {
        docs.reverse();
    }
    Ok(docs)
}

/// Query + rules: every returned document must pass the `list` rule (otherwise the whole query is
/// denied, as Firestore does — narrow the query to what the rules allow).
pub async fn query_documents(state: &AppState, caller: &Caller, query: &Query) -> ApiResult<Vec<Document>> {
    let docs = run_query(state, query).await?;
    if caller.app_id != "__admin__" {
        let probe_path = query.collection.clone().unwrap_or_else(|| format!("{}/*", query.collection_group.clone().unwrap_or_default()));
        if docs.is_empty() {
            // Check the list rule against the collection itself.
            let probe = format!("{}/__probe__", probe_path.trim_end_matches("/*"));
            check_rules(state, caller, &probe, Op::List, None, None).await?;
        }
        for d in &docs {
            check_rules(state, caller, d.get_str("_id").unwrap_or_default(), Op::List, None, d.get_document("data").ok()).await?;
        }
    }
    Ok(docs)
}

async fn query_route(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let body: Json = super::parse_json(&body)?;
    let query: Query = serde_json::from_value(body.get("query").cloned().unwrap_or(body.clone())).map_err(|e| ApiError::bad_request("INVALID_ARGUMENT", e.to_string()))?;
    let docs = query_documents(&state, &caller, &query).await?;
    if let Some(tx) = body.get("transaction").and_then(Json::as_str) {
        for d in &docs {
            record_read(&state, tx, d.get_str("_id").unwrap_or_default(), d.get_i64("update_time").unwrap_or(0)).await?;
        }
    }
    ok(json!({ "documents": docs.iter().map(document_json).collect::<Vec<_>>(), "readTime": micros_now() }))
}

async fn aggregate_route(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let caller = caller(&state, &headers, None, "database").await?;
    let body: Json = super::parse_json(&body)?;
    let mut query: Query = serde_json::from_value(body.get("query").cloned().unwrap_or_default()).map_err(|e| ApiError::bad_request("INVALID_ARGUMENT", e.to_string()))?;
    query.limit = Some(query.limit.unwrap_or(10_000));
    let docs = query_documents(&state, &caller, &query).await?;
    let mut result = Map::new();
    for agg in body.get("aggregations").and_then(Json::as_array).cloned().unwrap_or_else(|| vec![json!({ "alias": "count", "op": "count" })]) {
        let alias = agg["alias"].as_str().unwrap_or("value").to_string();
        let op = agg["op"].as_str().unwrap_or("count");
        let values: Vec<f64> = match agg["field"].as_str() {
            Some(field) => docs.iter().filter_map(|d| d.get_document("data").ok().and_then(|data| get_path(data, field)).and_then(|b| match b {
                Bson::Int64(i) => Some(*i as f64),
                Bson::Int32(i) => Some(f64::from(*i)),
                Bson::Double(f) => Some(*f),
                _ => None,
            })).collect(),
            None => vec![],
        };
        let value = match op {
            "count" => json!(docs.len()),
            "sum" => json!(values.iter().sum::<f64>()),
            "avg" if values.is_empty() => Json::Null,
            "avg" => json!(values.iter().sum::<f64>() / values.len() as f64),
            other => return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("unknown aggregation '{other}'"))),
        };
        result.insert(alias, value);
    }
    ok(json!({ "result": result, "readTime": micros_now() }))
}

// ── Live listeners ───────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Change {
    pub path: String,
    pub collection: String,
    pub collection_id: String,
}

/// Fan-out of committed writes to listeners.
pub struct LiveHub {
    sender: broadcast::Sender<Change>,
}

impl Default for LiveHub {
    fn default() -> Self {
        Self { sender: broadcast::channel(4096).0 }
    }
}

impl LiveHub {
    pub fn publish(&self, change: Change) {
        let _ = self.sender.send(change);
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.sender.subscribe()
    }
}

enum Target {
    Document { path: String, last: Option<i64>, sent: bool },
    Query { query: Query, last: Vec<(String, i64)> },
}

impl Target {
    fn affected_by(&self, change: &Change) -> bool {
        match self {
            Target::Document { path, .. } => *path == change.path,
            Target::Query { query, .. } => {
                query.collection.as_deref().map(|c| c.trim_matches('/')) == Some(change.collection.as_str())
                    || query.collection_group.as_deref() == Some(change.collection_id.as_str())
            }
        }
    }
}

async fn listen(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| serve_listener(state, socket))
}

async fn serve_listener(state: AppState, socket: WebSocket) {
    let (mut tx, mut rx) = socket.split();
    let mut changes = state.live.subscribe();
    let mut targets: BTreeMap<i64, Target> = BTreeMap::new();
    let mut caller: Option<Caller> = None;
    let mut ping = tokio::time::interval(std::time::Duration::from_secs(30));

    macro_rules! send {
        ($value:expr) => {
            if tx.send(Message::Text($value.to_string())).await.is_err() {
                return;
            }
        };
    }

    loop {
        tokio::select! {
            incoming = rx.next() => {
                let Some(Ok(message)) = incoming else { return };
                let text = match message {
                    Message::Text(t) => t,
                    Message::Close(_) => return,
                    _ => continue,
                };
                let Ok(msg) = serde_json::from_str::<Json>(&text) else {
                    send!(json!({ "type": "error", "code": "INVALID_ARGUMENT", "message": "messages are JSON" }));
                    continue;
                };
                match msg["type"].as_str().unwrap_or("") {
                    "auth" => match authenticate(&state, &msg).await {
                        Ok(c) => {
                            caller = Some(c);
                            send!(json!({ "type": "authenticated" }));
                            // Re-evaluate every target under the new identity.
                            let ids: Vec<i64> = targets.keys().copied().collect();
                            for id in ids {
                                if let Some(out) = refresh_target(&state, caller.as_ref().unwrap(), id, &mut targets, true).await { send!(out); }
                            }
                        }
                        Err(e) => send!(json!({ "type": "error", "code": e.reason, "message": e.message })),
                    },
                    "listen" => {
                        let Some(c) = caller.as_ref() else {
                            send!(json!({ "type": "error", "targetId": msg["targetId"], "code": "UNAUTHENTICATED", "message": "send {\"type\":\"auth\"} first" }));
                            continue;
                        };
                        let id = msg["targetId"].as_i64().unwrap_or(0);
                        let target = if let Some(path) = msg["document"].as_str() {
                            match document_path(path) {
                                Ok((p, _, _)) => Target::Document { path: p, last: None, sent: false },
                                Err(e) => { send!(json!({ "type": "error", "targetId": id, "code": e.reason, "message": e.message })); continue; }
                            }
                        } else {
                            match serde_json::from_value::<Query>(msg["query"].clone()) {
                                Ok(q) => Target::Query { query: q, last: vec![] },
                                Err(e) => { send!(json!({ "type": "error", "targetId": id, "code": "INVALID_ARGUMENT", "message": e.to_string() })); continue; }
                            }
                        };
                        targets.insert(id, target);
                        if let Some(out) = refresh_target(&state, c, id, &mut targets, true).await { send!(out); }
                    }
                    "unlisten" => { targets.remove(&msg["targetId"].as_i64().unwrap_or(-1)); }
                    "ping" => send!(json!({ "type": "pong" })),
                    other => send!(json!({ "type": "error", "code": "INVALID_ARGUMENT", "message": format!("unknown message type '{other}'") })),
                }
            }
            change = changes.recv() => {
                let Some(c) = caller.as_ref() else { continue };
                let mut affected: Vec<i64> = Vec::new();
                match change {
                    Ok(change) => {
                        affected.extend(targets.iter().filter(|(_, t)| t.affected_by(&change)).map(|(id, _)| *id));
                        // Coalesce a burst of writes into one refresh per target.
                        while let Ok(more) = changes.try_recv() {
                            for (id, t) in &targets { if t.affected_by(&more) && !affected.contains(id) { affected.push(*id); } }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => affected.extend(targets.keys().copied()),
                    Err(broadcast::error::RecvError::Closed) => return,
                }
                for id in affected {
                    if let Some(out) = refresh_target(&state, c, id, &mut targets, false).await { send!(out); }
                }
            }
            _ = ping.tick() => {
                if tx.send(Message::Ping(vec![])).await.is_err() { return; }
            }
        }
    }
}

async fn authenticate(state: &AppState, msg: &Json) -> ApiResult<Caller> {
    let mut headers = HeaderMap::new();
    let mut put = |name: &'static str, value: Option<&str>| {
        if let Some(v) = value.filter(|v| !v.is_empty()) {
            if let Ok(h) = v.parse() {
                headers.insert(name, h);
            }
        }
    };
    put("x-api-key", msg["apiKey"].as_str());
    put("x-dm-app-id", msg["appId"].as_str());
    put("x-dm-appcheck", msg["appCheck"].as_str());
    let bearer = msg["idToken"].as_str().map(|t| format!("Bearer {t}"));
    put("authorization", bearer.as_deref());
    caller(state, &headers, None, "database").await
}

/// Re-reads a target; returns the message to send if it changed (or `force`).
async fn refresh_target(state: &AppState, caller: &Caller, id: i64, targets: &mut BTreeMap<i64, Target>, force: bool) -> Option<Json> {
    let target = targets.get_mut(&id)?;
    let result = match target {
        Target::Document { path, last, sent } => match read_document(state, caller, path).await {
            Ok(stored) => {
                let version = stored.as_ref().map(|d| d.get_i64("update_time").unwrap_or(0));
                if !force && *sent && version == *last {
                    return None;
                }
                *last = version;
                *sent = true;
                Ok(json!({ "type": "snapshot", "targetId": id, "exists": stored.is_some(), "document": stored.as_ref().map(document_json), "path": path, "readTime": micros_now() }))
            }
            Err(e) => Err(e),
        },
        Target::Query { query, last } => match query_documents(state, caller, query).await {
            Ok(docs) => {
                let current: Vec<(String, i64)> = docs.iter().map(|d| (d.get_str("_id").unwrap_or_default().to_string(), d.get_i64("update_time").unwrap_or(0))).collect();
                if !force && current == *last {
                    return None;
                }
                let old_index: HashMap<&str, (usize, i64)> = last.iter().enumerate().map(|(i, (p, v))| (p.as_str(), (i, *v))).collect();
                let new_index: HashMap<&str, usize> = current.iter().enumerate().map(|(i, (p, _))| (p.as_str(), i)).collect();
                let mut changes = Vec::new();
                for (path, _) in last.iter() {
                    if !new_index.contains_key(path.as_str()) {
                        changes.push(json!({ "type": "removed", "path": path, "oldIndex": old_index[path.as_str()].0, "newIndex": -1 }));
                    }
                }
                for (i, (path, version)) in current.iter().enumerate() {
                    match old_index.get(path.as_str()) {
                        None => changes.push(json!({ "type": "added", "path": path, "oldIndex": -1, "newIndex": i })),
                        Some((old, v)) if v != version || *old != i => changes.push(json!({ "type": "modified", "path": path, "oldIndex": old, "newIndex": i })),
                        _ => {}
                    }
                }
                *last = current;
                Ok(json!({ "type": "snapshot", "targetId": id, "documents": docs.iter().map(document_json).collect::<Vec<_>>(), "changes": changes, "readTime": micros_now() }))
            }
            Err(e) => Err(e),
        },
    };
    match result {
        Ok(message) => Some(message),
        Err(e) => {
            targets.remove(&id);
            Some(json!({ "type": "error", "targetId": id, "code": e.reason, "message": e.message }))
        }
    }
}

/// Optional: feed listeners from a MongoDB change stream (multi-instance deployments).
pub fn start_change_stream(state: AppState) {
    if !crate::env_nonempty("DM_DB_CHANGE_STREAMS").is_some_and(|v| v == "true" || v == "1") {
        return;
    }
    tokio::spawn(async move {
        loop {
            match state.db.coll(DOCUMENTS).watch().await {
                Ok(mut stream) => {
                    println!("✅  Database listeners  → MongoDB change stream");
                    while let Ok(Some(event)) = stream.try_next().await {
                        if let Some(key) = event.document_key.as_ref().and_then(|k| k.get_str("_id").ok()) {
                            if let Ok((path, collection, collection_id)) = document_path(key) {
                                state.live.publish(Change { path, collection, collection_id });
                            }
                        }
                    }
                }
                Err(e) => eprintln!("⚠️  change stream unavailable ({e}); retrying in 10 s"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!(document_path("users/u1").unwrap(), ("users/u1".into(), "users".into(), "users".into()));
        assert_eq!(document_path("/users/u1/posts/p1/").unwrap().1, "users/u1/posts");
        assert!(document_path("users").is_err());
        assert!(collection_path("users/u1/posts").is_ok());
        assert!(collection_path("users/u1").is_err());
        assert_eq!(auto_id().len(), 20);
    }

    #[test]
    fn typed_values_round_trip() {
        let value = json!({ "t": { "__type": "timestamp", "value": "2026-10-29T10:00:00Z" }, "b": { "__type": "bytes", "base64": "AQI=" },
                            "r": { "__type": "ref", "path": "users/u1" }, "g": { "__type": "geopoint", "latitude": 19.0, "longitude": 72.8 },
                            "n": 3, "f": 1.5, "s": "x", "a": [1, "two"], "m": { "k": true } });
        let back = to_json(&to_bson(&value).unwrap());
        assert_eq!(back["t"]["value"], "2026-10-29T10:00:00Z");
        assert_eq!(back["b"]["base64"], "AQI=");
        assert_eq!(back["r"]["path"], "users/u1");
        assert_eq!(back["n"], 3);
        assert!(to_bson(&json!({ "a.b": 1 })).is_err());
        assert!(to_bson(&json!({ "x": [[1]] })).is_err());
    }

    #[test]
    fn set_update_and_transforms() {
        let now = DateTime::from_millis(1_800_000_000_000);
        let w = |op: &str, data: Json, merge: bool| Write { op: op.into(), path: "c/d".into(), data: data.as_object().cloned(), merge, merge_fields: None, precondition: None };
        let created = plan(&w("set", json!({ "a": 1, "m": { "x": 1 }, "tags": ["a"], "at": { "__op": "serverTimestamp" } }), false), None, now).unwrap().after.unwrap();
        assert_eq!(created.get_i64("a").unwrap(), 1);
        assert_eq!(created.get_datetime("at").unwrap().timestamp_millis(), 1_800_000_000_000);
        let stored = doc! { "data": created.clone(), "update_time": 5_i64 };
        let updated = plan(&w("update", json!({ "a": { "__op": "increment", "by": 2 }, "m.y": 2, "tags": { "__op": "arrayUnion", "values": ["b", "a"] } }), false), Some(stored.clone()), now).unwrap().after.unwrap();
        assert_eq!(updated.get_i64("a").unwrap(), 3);
        assert_eq!(updated.get_document("m").unwrap().get_i64("x").unwrap(), 1);
        assert_eq!(updated.get_document("m").unwrap().get_i64("y").unwrap(), 2);
        assert_eq!(updated.get_array("tags").unwrap().len(), 2);
        let merged = plan(&w("set", json!({ "m": { "z": 3 }, "a": { "__op": "delete" } }), true), Some(stored.clone()), now).unwrap().after.unwrap();
        assert!(merged.get("a").is_none());
        assert_eq!(merged.get_document("m").unwrap().len(), 2, "merge keeps m.x");
        let replaced = plan(&w("set", json!({ "only": true }), false), Some(stored), now).unwrap().after.unwrap();
        assert_eq!(replaced.len(), 1);
        assert!(plan(&w("update", json!({ "a": 1 }), false), None, now).is_err(), "update needs an existing doc");
    }

    #[test]
    fn cursors() {
        let order = vec![("data.score".to_string(), true), ("_id".to_string(), true)];
        let d = cursor_doc(&order, &[json!(10), json!("c/x")], true, false).unwrap();
        let text = format!("{d}");
        assert!(text.contains("$lt"), "descending startAfter uses $lt: {text}");
    }
}
