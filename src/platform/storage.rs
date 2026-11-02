//! Cloud Storage equivalent: files in MongoDB GridFS (bucket `storage_files`) with their metadata
//! in `storage_objects`, guarded by the storage security rules (rules.rs).
//!
//! Object names are '/'-separated paths, URL-encoded into a single path segment (like Firebase).
//!
//! POST   /v1/storage/b/:bucket/o?name=<path>            upload — raw body (Content-Type = file type)
//!                                                       or multipart/related|form-data (metadata JSON + file)
//! POST   /v1/storage/b/:bucket/o?name=<path>            with X-Goog-Upload-Protocol: resumable and
//!                                                       X-Goog-Upload-Command: start → X-Goog-Upload-URL
//! PUT    /v1/storage/upload/:session                    X-Goog-Upload-Command: upload | upload, finalize | finalize | query
//!                                                       X-Goog-Upload-Offset: <bytes already sent>
//! GET    /v1/storage/b/:bucket/o/:object                metadata (JSON)
//! GET    /v1/storage/b/:bucket/o/:object?alt=media      contents (Range supported); &token=<download token>
//!                                                       skips the rules — that is what download URLs use
//! PATCH  /v1/storage/b/:bucket/o/:object                update metadata {contentType, cacheControl,
//!                                                       contentDisposition, customMetadata}
//! DELETE /v1/storage/b/:bucket/o/:object
//! GET    /v1/storage/b/:bucket/o?prefix=a/&delimiter=/&maxResults=100&pageToken=…   list

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use futures_util::TryStreamExt;
use mongodb::bson::{self, doc, Binary, Bson, DateTime, Document};
use mongodb::gridfs::GridFsBucket;
use mongodb::options::GridFsBucketOptions;
use serde::Deserialize;
use serde_json::{json, Value as Json};

use super::rules::{self, Op};
use super::{caller, ok, ApiError, ApiResult, Caller};
use crate::dm::AppState;

pub const OBJECTS: &str = "storage_objects";
pub const UPLOADS: &str = "storage_uploads";
pub const CHUNKS: &str = "storage_upload_chunks";
pub const GRANULARITY: usize = 256 * 1024;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/storage/b/:bucket/o", get(list).post(upload))
        // POST = PATCH for clients whose HTTP stack has no PATCH (java.net.HttpURLConnection sends
        // POST + X-HTTP-Method-Override: PATCH).
        .route("/storage/b/:bucket/o/:object", get(get_object).patch(update_metadata).post(update_metadata).delete(delete_object))
        .route("/storage/upload/:session", put(resumable).post(resumable))
}

pub async fn ensure_schema(db: &crate::db::Db) -> Result<(), crate::BoxError> {
    use crate::db::index;
    index(&db.coll(OBJECTS), doc! { "bucket": 1, "name": 1 }, "bucket_name", true).await?;
    index(&db.coll(CHUNKS), doc! { "session": 1, "offset": 1 }, "session_offset", true).await?;
    super::ttl_on(db, UPLOADS, "expires_at").await;
    super::ttl_on(db, CHUNKS, "expires_at").await;
    Ok(())
}

fn files(state: &AppState) -> GridFsBucket {
    state.db.database.gridfs_bucket(GridFsBucketOptions::builder().bucket_name("storage_files".to_string()).build())
}

/// Cloud Storage object-name rules: 1–1024 bytes of UTF-8, no control characters (CR/LF
/// included), no leading slash, no empty / "." / ".." segments, not under .well-known/acme-challenge.
fn valid_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || name.len() > crate::validate::limits::OBJECT_NAME_BYTES
        || name.starts_with('/')
        || name.contains("//")
        || name.split('/').any(|s| s == ".." || s == ".")
        || name.chars().any(char::is_control)
        || name.starts_with(".well-known/acme-challenge/")
    {
        return Err(ApiError::bad_request(
            "INVALID_ARGUMENT",
            "invalid object name (1-1024 bytes, no control characters, no leading '/', no empty, '.' or '..' segments)",
        ));
    }
    Ok(())
}

/// Bucket names: 1–222 characters of a-z 0-9 . _ - starting and ending with a letter or digit.
fn valid_bucket(bucket: &str) -> Result<(), ApiError> {
    let ok = !bucket.is_empty()
        && bucket.len() <= 222
        && bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
        && bucket.starts_with(|c: char| c.is_ascii_alphanumeric())
        && bucket.ends_with(|c: char| c.is_ascii_alphanumeric());
    if ok { Ok(()) } else { Err(ApiError::bad_request("INVALID_ARGUMENT", "invalid bucket name (a-z 0-9 . _ -, at most 222 characters)")) }
}

/// Settable metadata fields (sizes and formats).
fn validate_metadata(v: &mut crate::validate::V, content_type: Option<&str>, cache_control: Option<&str>, disposition: Option<&str>, encoding: Option<&str>, language: Option<&str>, custom: Option<&Json>) {
    use crate::validate::limits;
    if let Some(ct) = content_type.filter(|c| !c.is_empty()) {
        v.check(crate::validate::is_content_type(ct), "contentType", "must be a MIME type like image/png");
    }
    for (field, value, max) in [("cacheControl", cache_control, 1024), ("contentDisposition", disposition, 1024), ("contentEncoding", encoding, 64), ("contentLanguage", language, 35)] {
        if let Some(value) = value {
            v.text(field, value, max);
        }
    }
    match custom {
        None | Some(Json::Null) => {}
        Some(Json::Object(map)) => {
            let mut total = 0;
            for (k, value) in map {
                v.len(&format!("metadata.{k}"), k, 1, limits::CUSTOM_METADATA_KEY);
                match value {
                    Json::String(text) => {
                        total += k.len() + text.len();
                        v.printable(&format!("metadata.{k}"), text);
                    }
                    Json::Null => {}
                    _ => v.fail(format!("metadata.{k}"), "custom metadata values must be strings"),
                }
            }
            v.check(total <= limits::CUSTOM_METADATA_BYTES, "metadata", format!("custom metadata must total at most {} bytes", limits::CUSTOM_METADATA_BYTES));
        }
        Some(_) => v.fail("metadata", "must be an object of strings"),
    }
}

fn check_meta_body(meta: &MetadataBody) -> ApiResult<()> {
    let mut v = crate::validate::V::new();
    let custom = meta.metadata.clone().map(Json::Object);
    validate_metadata(&mut v, meta.content_type.as_deref(), meta.cache_control.as_deref(), meta.content_disposition.as_deref(), meta.content_encoding.as_deref(), meta.content_language.as_deref(), custom.as_ref());
    v.into_api()
}

// ── Rules ────────────────────────────────────────────────────────────────────

pub async fn load_rules(state: &AppState, kind: &str) -> Vec<rules::Rule> {
    static CACHE: std::sync::OnceLock<Mutex<HashMap<String, (Instant, Vec<rules::Rule>)>>> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some((at, rules)) = cache.lock().unwrap().get(kind) {
        if at.elapsed() < Duration::from_secs(5) {
            return rules.clone();
        }
    }
    let loaded = match state.db.coll("security_rules").find_one(doc! { "_id": kind }).await {
        Ok(Some(d)) => d
            .get_array("rules")
            .ok()
            .and_then(|a| serde_json::to_value(a).ok())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_else(rules::default_rules),
        _ => rules::default_rules(),
    };
    cache.lock().unwrap().insert(kind.to_string(), (Instant::now(), loaded.clone()));
    loaded
}

fn rules_context(caller: &Caller, op: Op, incoming: Json, existing: Json) -> Json {
    json!({
        "auth": rules::auth_value(caller.user.as_ref()),
        "request": { "time": chrono::Utc::now().timestamp_millis(), "method": op.name(), "resource": incoming,
                     "app": { "id": caller.app_id, "appcheck": caller.appcheck.is_some() } },
        "resource": existing,
    })
}

async fn authorize(state: &AppState, caller: &Caller, path: &str, op: Op, incoming: Json, existing: Json) -> ApiResult<()> {
    let rules = load_rules(state, "storage").await;
    rules::check(&rules, path, op, &rules_context(caller, op, incoming, existing)).map_err(ApiError::denied)
}

fn rules_resource(meta: &Document) -> Json {
    json!({
        "name": meta.get_str("name").ok(),
        "bucket": meta.get_str("bucket").ok(),
        "size": meta.get_i64("size").unwrap_or(0),
        "contentType": meta.get_str("content_type").ok(),
        "metadata": meta.get_document("custom_metadata").ok().map(|d| serde_json::to_value(d).unwrap_or_default()),
        "timeCreated": meta.get_datetime("created_at").ok().map(|d| d.timestamp_millis()),
        "updated": meta.get_datetime("updated_at").ok().map(|d| d.timestamp_millis()),
    })
}

/// Firebase-style object metadata JSON.
fn metadata_json(meta: &Document) -> Json {
    let iso = |key: &str| meta.get_datetime(key).ok().and_then(|d| d.try_to_rfc3339_string().ok());
    let tokens: Vec<String> = meta.get_array("download_tokens").map(|a| a.iter().filter_map(|t| t.as_str().map(str::to_string)).collect()).unwrap_or_default();
    json!({
        "name": meta.get_str("name").ok(),
        "bucket": meta.get_str("bucket").ok(),
        "generation": meta.get_i64("generation").unwrap_or(0).to_string(),
        "metageneration": meta.get_i64("metageneration").unwrap_or(1).to_string(),
        "contentType": meta.get_str("content_type").ok(),
        "size": meta.get_i64("size").unwrap_or(0).to_string(),
        "md5Hash": meta.get_str("md5").ok(),
        "timeCreated": iso("created_at"),
        "updated": iso("updated_at"),
        "cacheControl": meta.get_str("cache_control").ok(),
        "contentDisposition": meta.get_str("content_disposition").ok(),
        "contentEncoding": meta.get_str("content_encoding").ok(),
        "contentLanguage": meta.get_str("content_language").ok(),
        "metadata": meta.get_document("custom_metadata").ok().map(|d| serde_json::to_value(d).unwrap_or_default()),
        "downloadTokens": if tokens.is_empty() { Json::Null } else { Json::String(tokens.join(",")) },
    })
}

async fn find(state: &AppState, bucket: &str, name: &str) -> ApiResult<Option<Document>> {
    state.db.coll(OBJECTS).find_one(doc! { "bucket": bucket, "name": name }).await.map_err(ApiError::db)
}

// ── Upload ───────────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct UploadQuery {
    name: String,
    key: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
struct MetadataBody {
    name: Option<String>,
    content_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    metadata: Option<serde_json::Map<String, Json>>,
}

/// Splits a multipart body (multipart/related from the SDKs, or multipart/form-data from forms)
/// into (metadata JSON, file bytes, file content type).
fn split_multipart(content_type: &str, body: &[u8]) -> ApiResult<(MetadataBody, Vec<u8>, Option<String>)> {
    let boundary = content_type
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("boundary="))
        .next()
        .map(|b| b.trim_matches('"').to_string())
        .ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "multipart boundary missing"))?;
    let delimiter = format!("--{boundary}").into_bytes();
    let mut parts = Vec::new();
    let mut rest = body;
    while let Some(start) = find_bytes(rest, &delimiter) {
        rest = &rest[start + delimiter.len()..];
        if rest.starts_with(b"--") {
            break;
        }
        let end = find_bytes(rest, &delimiter).unwrap_or(rest.len());
        let mut part = &rest[..end];
        part = part.strip_prefix(b"\r\n").unwrap_or(part);
        part = part.strip_suffix(b"\r\n").unwrap_or(part);
        let split = find_bytes(part, b"\r\n\r\n").ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "malformed multipart part"))?;
        let headers = String::from_utf8_lossy(&part[..split]).to_lowercase();
        parts.push((headers, part[split + 4..].to_vec()));
        rest = &rest[end..];
    }
    let mut metadata = MetadataBody::default();
    let mut file = None;
    for (headers, data) in parts {
        let is_json = headers.contains("application/json") && file.is_none() && metadata.name.is_none() && (headers.contains("name=\"metadata\"") || !headers.contains("filename="));
        if is_json && file.is_none() {
            if let Ok(parsed) = serde_json::from_slice::<MetadataBody>(&data) {
                metadata = parsed;
                continue;
            }
        }
        let ctype = headers.lines().find_map(|l| l.strip_prefix("content-type:")).map(|v| v.trim().to_string());
        file = Some((data, ctype));
    }
    let (data, ctype) = file.ok_or_else(|| ApiError::bad_request("INVALID_ARGUMENT", "multipart body has no file part"))?;
    Ok((metadata, data, ctype))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Writes `data` as the new contents of `bucket/name` (replacing any previous version).
async fn store(state: &AppState, caller: &Caller, bucket: &str, name: &str, data: Vec<u8>, meta: MetadataBody, content_type: Option<String>) -> ApiResult<Document> {
    if data.len() > state.config.platform.storage_max_bytes {
        return Err(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "OBJECT_TOO_LARGE", format!("max {} MB", state.config.platform.storage_max_bytes / 1024 / 1024)));
    }
    let existing = find(state, bucket, name).await?;
    let content_type = meta.content_type.clone().or(content_type).filter(|c| !c.is_empty()).unwrap_or_else(|| guess_type(name).to_string());
    let incoming = json!({ "name": name, "bucket": bucket, "size": data.len(), "contentType": content_type, "metadata": meta.metadata });
    let op = if existing.is_some() { Op::Update } else { Op::Create };
    authorize(state, caller, name, op, incoming, existing.as_ref().map(rules_resource).unwrap_or(Json::Null)).await?;

    let md5 = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(md5_digest(&data))
    };
    let bucket_fs = files(state);
    let mut upload = bucket_fs.open_upload_stream(format!("{bucket}/{name}")).await.map_err(ApiError::db)?;
    upload.write_all(&data).await.map_err(ApiError::db)?;
    upload.close().await.map_err(ApiError::db)?;
    let file_id = upload.id().clone();

    let now = DateTime::now();
    let token = uuid::Uuid::new_v4().to_string();
    let mut set = doc! {
        "bucket": bucket, "name": name, "size": data.len() as i64, "content_type": &content_type, "md5": md5,
        "gridfs_id": file_id, "generation": chrono::Utc::now().timestamp_micros(), "metageneration": 1_i64,
        "updated_at": now, "owner_uid": caller.uid(),
    };
    for (field, value) in [("cache_control", &meta.cache_control), ("content_disposition", &meta.content_disposition), ("content_encoding", &meta.content_encoding), ("content_language", &meta.content_language)] {
        if let Some(v) = value {
            set.insert(field, v);
        }
    }
    if let Some(custom) = &meta.metadata {
        set.insert("custom_metadata", bson::to_bson(custom).unwrap_or(Bson::Null));
    }
    let objects = state.db.coll(OBJECTS);
    let updated = objects
        .find_one_and_update(
            doc! { "bucket": bucket, "name": name },
            doc! { "$set": set, "$setOnInsert": { "_id": format!("{bucket}/{name}"), "created_at": now, "download_tokens": [token] } },
        )
        .upsert(true)
        .return_document(mongodb::options::ReturnDocument::After)
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| ApiError::db("upsert returned nothing"))?;
    if let Some(old) = existing.and_then(|e| e.get("gridfs_id").cloned()) {
        let _ = bucket_fs.delete(old).await;
    }
    Ok(updated)
}

fn md5_digest(data: &[u8]) -> [u8; 16] {
    // MD5 is only an integrity hint here (Firebase reports md5Hash); compute it without a crate.
    md5_compute(data)
}

pub async fn upload(State(state): State<AppState>, Path(bucket): Path<String>, Query(query): Query<UploadQuery>, headers: HeaderMap, body: Bytes) -> ApiResult {
    valid_bucket(&bucket)?;
    let caller = caller(&state, &headers, query.key.as_deref(), "storage").await?;
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let content_type = header("content-type");

    if header("x-goog-upload-protocol") == "resumable" {
        if header("x-goog-upload-command") != "start" {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", "resumable uploads start with X-Goog-Upload-Command: start"));
        }
        let meta: MetadataBody = if body.is_empty() { MetadataBody::default() } else { super::parse_json(&body)? };
        let name = meta.name.clone().filter(|n| !n.is_empty()).unwrap_or(query.name.clone());
        valid_name(&name)?;
        check_meta_body(&meta)?;
        let declared = header("x-goog-upload-header-content-length");
        let size: Option<i64> = declared.parse().ok();
        if !declared.is_empty() && size.map_or(true, |s| s < 0) {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", "X-Goog-Upload-Header-Content-Length must be a non-negative integer"));
        }
        if size.is_some_and(|s| s as usize > state.config.platform.storage_max_bytes) {
            return Err(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "OBJECT_TOO_LARGE", "file exceeds the size limit"));
        }
        let ctype = meta.content_type.clone().or_else(|| Some(header("x-goog-upload-header-content-type")).filter(|s| !s.is_empty()));
        let existing = find(&state, &bucket, &name).await?;
        let op = if existing.is_some() { Op::Update } else { Op::Create };
        let incoming = json!({ "name": name, "bucket": bucket, "size": size, "contentType": ctype, "metadata": meta.metadata });
        authorize(&state, &caller, &name, op, incoming, existing.as_ref().map(rules_resource).unwrap_or(Json::Null)).await?;
        let session = super::tokens::random_token();
        state
            .db
            .coll(UPLOADS)
            .insert_one(doc! {
                "_id": &session, "bucket": &bucket, "name": &name, "content_type": ctype, "expected_size": size,
                "metadata": bson::to_bson(&json!({ "cacheControl": meta.cache_control, "contentDisposition": meta.content_disposition, "metadata": meta.metadata })).unwrap_or(Bson::Null),
                "received": 0_i64, "uid": caller.uid(), "app_id": &caller.app_id,
                "expires_at": DateTime::from_millis(chrono::Utc::now().timestamp_millis() + 7 * 86_400_000),
            })
            .await
            .map_err(ApiError::db)?;
        let mut response = json!({}).to_string().into_response();
        let h = response.headers_mut();
        h.insert("x-goog-upload-url", HeaderValue::from_str(&format!("/v1/storage/upload/{session}")).unwrap());
        h.insert("x-goog-upload-status", HeaderValue::from_static("active"));
        h.insert("x-goog-upload-chunk-granularity", HeaderValue::from(GRANULARITY));
        h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        return Ok(response);
    }

    let (meta, data, part_type) = if content_type.starts_with("multipart/") {
        split_multipart(&content_type, &body)?
    } else {
        (MetadataBody::default(), body.to_vec(), Some(content_type).filter(|c| !c.is_empty()))
    };
    let name = meta.name.clone().filter(|n| !n.is_empty()).unwrap_or(query.name.clone());
    valid_name(&name)?;
    check_meta_body(&meta)?;
    let stored = store(&state, &caller, &bucket, &name, data, meta, part_type).await?;
    ok(metadata_json(&stored))
}

pub async fn resumable(State(state): State<AppState>, Path(session): Path<String>, headers: HeaderMap, body: Bytes) -> ApiResult {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_lowercase();
    let command = header("x-goog-upload-command");
    let uploads = state.db.coll(UPLOADS);
    let record = uploads.find_one(doc! { "_id": &session }).await.map_err(ApiError::db)?.ok_or_else(|| ApiError::not_found("upload session not found or expired"))?;
    let received = record.get_i64("received").unwrap_or(0);
    let status_response = |status: &'static str, received: i64, body: Json| {
        let mut response = body.to_string().into_response();
        let h = response.headers_mut();
        h.insert("x-goog-upload-status", HeaderValue::from_static(status));
        h.insert("x-goog-upload-size-received", HeaderValue::from(received));
        h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
    };
    if command == "query" {
        return Ok(status_response("active", received, json!({})));
    }
    if command == "cancel" {
        let _ = state.db.coll(CHUNKS).delete_many(doc! { "session": &session }).await;
        let _ = uploads.delete_one(doc! { "_id": &session }).await;
        return Ok(status_response("cancelled", received, json!({})));
    }
    let commands: Vec<&str> = command.split(',').map(str::trim).collect();
    if commands.contains(&"upload") {
        let offset: i64 = header("x-goog-upload-offset").parse().map_err(|_| ApiError::bad_request("INVALID_ARGUMENT", "X-Goog-Upload-Offset required"))?;
        if offset != received {
            // Client and server disagree (lost response): tell the client where to continue.
            let mut response = status_response("active", received, json!({ "error": { "message": format!("offset {offset} != received {received}") } }));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
        if !commands.contains(&"finalize") && body.len() % GRANULARITY != 0 {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("non-final chunks must be multiples of {GRANULARITY} bytes")));
        }
        if received as usize + body.len() > state.config.platform.storage_max_bytes {
            return Err(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "OBJECT_TOO_LARGE", "file exceeds the size limit"));
        }
        if !body.is_empty() {
            let inserted = state
                .db
                .coll(CHUNKS)
                .insert_one(doc! { "session": &session, "offset": offset, "data": Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: body.to_vec() }, "expires_at": record.get_datetime("expires_at").cloned().unwrap_or(DateTime::now()) })
                .await
                .map_err(|e| if crate::db::is_duplicate_key(&e) { ApiError::new(StatusCode::CONFLICT, "CHUNK_ALREADY_RECEIVED", "chunk already received") } else { ApiError::db(e) });
            if let Err(e) = inserted {
                if e.status == StatusCode::CONFLICT {
                    // A concurrent/duplicate request already stored this chunk: report the real
                    // offset so the client resumes from there.
                    let now = uploads.find_one(doc! { "_id": &session }).await.map_err(ApiError::db)?.and_then(|r| r.get_i64("received").ok()).unwrap_or(received);
                    let mut response = status_response("active", now, json!({ "error": { "message": "chunk already received" } }));
                    *response.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(response);
                }
                return Err(e);
            }
            uploads.update_one(doc! { "_id": &session }, doc! { "$inc": { "received": body.len() as i64 } }).await.map_err(ApiError::db)?;
        }
    }
    let received = received + if commands.contains(&"upload") { body.len() as i64 } else { 0 };
    if !commands.contains(&"finalize") {
        return Ok(status_response("active", received, json!({})));
    }

    // Finalize: assemble the chunks and store the object.
    let mut data = Vec::with_capacity(received as usize);
    let mut cursor = state.db.coll(CHUNKS).find(doc! { "session": &session }).sort(doc! { "offset": 1 }).await.map_err(ApiError::db)?;
    while let Some(chunk) = cursor.try_next().await.map_err(ApiError::db)? {
        if chunk.get_i64("offset").unwrap_or(-1) != data.len() as i64 {
            return Err(ApiError::db("upload chunks are not contiguous"));
        }
        data.extend_from_slice(&chunk.get_binary_generic("data").cloned().unwrap_or_default());
    }
    if let Ok(expected) = record.get_i64("expected_size") {
        if expected != data.len() as i64 {
            return Err(ApiError::bad_request("INVALID_ARGUMENT", format!("received {} bytes, expected {expected}", data.len())));
        }
    }
    let meta_json: Json = record.get_document("metadata").ok().and_then(|d| serde_json::to_value(d).ok()).unwrap_or_default();
    let meta: MetadataBody = serde_json::from_value(meta_json).unwrap_or_default();
    // The rules were checked at start; re-check with the real size now (as the uploader).
    let uploader = Caller {
        app_id: record.get_str("app_id").unwrap_or_default().to_string(),
        user: match super::bearer(&headers) {
            Some(token) => Some(super::auth::verify_id_token(&state, token).await?),
            None => None,
        },
        ..Default::default()
    };
    if uploader.uid() != record.get_str("uid").ok() {
        return Err(ApiError::denied("finalize must come from the user who started the upload"));
    }
    let stored = store(&state, &uploader, record.get_str("bucket").unwrap_or_default(), record.get_str("name").unwrap_or_default(), data, meta, record.get_str("content_type").ok().map(str::to_string)).await?;
    let _ = state.db.coll(CHUNKS).delete_many(doc! { "session": &session }).await;
    let _ = uploads.delete_one(doc! { "_id": &session }).await;
    let size = stored.get_i64("size").unwrap_or(0);
    Ok(status_response("final", size, metadata_json(&stored)))
}

// ── Read / update / delete / list ────────────────────────────────────────────

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ObjectQuery {
    alt: Option<String>,
    token: Option<String>,
    key: Option<String>,
}

pub async fn get_object(State(state): State<AppState>, Path((bucket, object)): Path<(String, String)>, Query(query): Query<ObjectQuery>, method: Method, headers: HeaderMap) -> ApiResult {
    valid_bucket(&bucket)?;
    valid_name(&object)?;
    let name = object;
    let meta = find(&state, &bucket, &name).await?;
    // A matching download token bypasses the rules (download URLs are capabilities).
    let token_ok = match (&meta, &query.token) {
        (Some(m), Some(t)) => m.get_array("download_tokens").is_ok_and(|a| a.iter().any(|x| x.as_str() == Some(t.as_str()))),
        _ => false,
    };
    if !token_ok {
        let caller = caller(&state, &headers, query.key.as_deref(), "storage").await?;
        authorize(&state, &caller, &name, Op::Get, Json::Null, meta.as_ref().map(rules_resource).unwrap_or(Json::Null)).await?;
    }
    let meta = meta.ok_or_else(|| ApiError::not_found(format!("object {name} does not exist")))?;
    if query.alt.as_deref() != Some("media") {
        return ok(metadata_json(&meta));
    }

    let size = meta.get_i64("size").unwrap_or(0) as usize;
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok()).and_then(|r| parse_range(r, size));
    let file_id = meta.get("gridfs_id").cloned().ok_or_else(|| ApiError::db("object has no data"))?;
    let mut stream = files(&state).open_download_stream(file_id).await.map_err(ApiError::db)?;
    let mut data = Vec::with_capacity(size);
    stream.read_to_end(&mut data).await.map_err(ApiError::db)?;

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, meta.get_str("content_type").unwrap_or("application/octet-stream"))
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, format!("\"{}\"", meta.get_str("md5").unwrap_or_default()));
    if let Ok(cc) = meta.get_str("cache_control") {
        builder = builder.header(header::CACHE_CONTROL, cc);
    }
    if let Ok(cd) = meta.get_str("content_disposition") {
        builder = builder.header(header::CONTENT_DISPOSITION, cd);
    }
    let body = match range {
        Some((start, end)) => {
            builder = builder.status(StatusCode::PARTIAL_CONTENT).header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"));
            data[start..=end].to_vec()
        }
        None => data,
    };
    if method == Method::HEAD {
        return Ok(builder.body(Body::empty()).unwrap());
    }
    Ok(builder.body(Body::from(body)).unwrap())
}

fn parse_range(header: &str, size: usize) -> Option<(usize, usize)> {
    let spec = header.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    if size == 0 {
        return None;
    }
    let (start, end) = if a.is_empty() {
        let suffix: usize = b.parse().ok()?;
        (size.saturating_sub(suffix), size - 1)
    } else {
        let start: usize = a.parse().ok()?;
        let end: usize = if b.is_empty() { size - 1 } else { b.parse::<usize>().ok()?.min(size - 1) };
        (start, end)
    };
    (start <= end && start < size).then_some((start, end))
}

pub async fn update_metadata(State(state): State<AppState>, Path((bucket, name)): Path<(String, String)>, Query(query): Query<ObjectQuery>, headers: HeaderMap, body: Bytes) -> ApiResult {
    valid_bucket(&bucket)?;
    valid_name(&name)?;
    let caller = caller(&state, &headers, query.key.as_deref(), "storage").await?;
    let update: Json = super::parse_json(&body)?;
    {
        let mut v = crate::validate::V::new();
        if !update.is_object() {
            v.fail("body", "must be a JSON object");
        }
        let text = |k: &str| update.get(k).and_then(Json::as_str);
        for key in ["contentType", "cacheControl", "contentDisposition", "contentEncoding", "contentLanguage"] {
            v.check(matches!(update.get(key), None | Some(Json::Null) | Some(Json::String(_))), key, "must be a string or null");
        }
        validate_metadata(&mut v, text("contentType"), text("cacheControl"), text("contentDisposition"), text("contentEncoding"), text("contentLanguage"), update.get("metadata"));
        v.into_api()?;
    }
    let meta = find(&state, &bucket, &name).await?.ok_or_else(|| ApiError::not_found(format!("object {name} does not exist")))?;
    let mut incoming = rules_resource(&meta);
    if let Some(ct) = update.get("contentType") {
        incoming["contentType"] = ct.clone();
    }
    if let Some(m) = update.get("metadata") {
        incoming["metadata"] = m.clone();
    }
    authorize(&state, &caller, &name, Op::Update, incoming, rules_resource(&meta)).await?;
    let mut set = doc! { "updated_at": DateTime::now() };
    let mut unset = doc! {};
    for (json_key, field) in [("contentType", "content_type"), ("cacheControl", "cache_control"), ("contentDisposition", "content_disposition"), ("contentEncoding", "content_encoding"), ("contentLanguage", "content_language")] {
        match update.get(json_key) {
            Some(Json::Null) => {
                unset.insert(field, "");
            }
            Some(Json::String(v)) => {
                set.insert(field, v);
            }
            _ => {}
        }
    }
    match update.get("metadata") {
        Some(Json::Null) => {
            unset.insert("custom_metadata", "");
        }
        Some(m @ Json::Object(_)) => {
            set.insert("custom_metadata", bson::to_bson(m).unwrap_or(Bson::Null));
        }
        _ => {}
    }
    let mut ops = doc! { "$set": set, "$inc": { "metageneration": 1_i64 } };
    if !unset.is_empty() {
        ops.insert("$unset", unset);
    }
    let updated = state
        .db
        .coll(OBJECTS)
        .find_one_and_update(doc! { "bucket": &bucket, "name": &name }, ops)
        .return_document(mongodb::options::ReturnDocument::After)
        .await
        .map_err(ApiError::db)?
        .ok_or_else(|| ApiError::not_found("object disappeared"))?;
    ok(metadata_json(&updated))
}

pub async fn delete_object(State(state): State<AppState>, Path((bucket, name)): Path<(String, String)>, Query(query): Query<ObjectQuery>, headers: HeaderMap) -> ApiResult {
    valid_bucket(&bucket)?;
    valid_name(&name)?;
    let caller = caller(&state, &headers, query.key.as_deref(), "storage").await?;
    let meta = find(&state, &bucket, &name).await?;
    authorize(&state, &caller, &name, Op::Delete, Json::Null, meta.as_ref().map(rules_resource).unwrap_or(Json::Null)).await?;
    let meta = meta.ok_or_else(|| ApiError::not_found(format!("object {name} does not exist")))?;
    delete_stored(&state, &meta).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn delete_stored(state: &AppState, meta: &Document) -> ApiResult<()> {
    state.db.coll(OBJECTS).delete_one(doc! { "_id": meta.get("_id").cloned().unwrap_or(Bson::Null) }).await.map_err(ApiError::db)?;
    if let Some(id) = meta.get("gridfs_id").cloned() {
        let _ = files(state).delete(id).await;
    }
    Ok(())
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ListQuery {
    prefix: String,
    delimiter: String,
    max_results: Option<i64>,
    page_token: Option<String>,
    key: Option<String>,
}

pub async fn list(State(state): State<AppState>, Path(bucket): Path<String>, Query(query): Query<ListQuery>, headers: HeaderMap) -> ApiResult {
    valid_bucket(&bucket)?;
    let mut v = crate::validate::V::new();
    v.check(query.prefix.len() <= crate::validate::limits::OBJECT_NAME_BYTES && !query.prefix.chars().any(char::is_control) && !query.prefix.starts_with('/'), "prefix", "must be at most 1024 bytes without control characters or a leading '/'");
    v.check(query.delimiter.is_empty() || query.delimiter == "/", "delimiter", "must be empty or \"/\"");
    if let Some(max) = query.max_results {
        v.range("maxResults", max, 1, crate::validate::limits::LIST_MAX_RESULTS);
    }
    if let Some(token) = &query.page_token {
        v.text("pageToken", token, crate::validate::limits::OBJECT_NAME_BYTES + 64);
    }
    v.into_api()?;
    let caller = caller(&state, &headers, query.key.as_deref(), "storage").await?;
    let list_path = query.prefix.trim_end_matches('/');
    authorize(&state, &caller, if list_path.is_empty() { "/" } else { list_path }, Op::List, Json::Null, Json::Null).await?;
    let (prefixes, items, next) = list_objects(&state, &bucket, &query.prefix, &query.delimiter, query.max_results.unwrap_or(1000).clamp(1, 1000), query.page_token.as_deref()).await?;
    ok(json!({ "prefixes": prefixes, "items": items.iter().map(|m| json!({ "name": m.get_str("name").ok(), "bucket": m.get_str("bucket").ok() })).collect::<Vec<_>>(), "nextPageToken": next }))
}

/// (prefixes, items, next page token) — shared with the admin API.
pub async fn list_objects(state: &AppState, bucket: &str, prefix: &str, delimiter: &str, max: i64, page_token: Option<&str>) -> ApiResult<(Vec<String>, Vec<Document>, Option<String>)> {
    let escaped = regex_escape(prefix);
    let mut filter = doc! { "bucket": bucket, "name": { "$regex": format!("^{escaped}") } };
    if let Some(after) = page_token {
        filter.insert("name", doc! { "$regex": format!("^{escaped}"), "$gt": after });
    }
    let mut cursor = state.db.coll(OBJECTS).find(filter).sort(doc! { "name": 1 }).await.map_err(ApiError::db)?;
    let mut prefixes = std::collections::BTreeSet::new();
    let mut items = Vec::new();
    let mut last = None;
    while let Some(meta) = cursor.try_next().await.map_err(ApiError::db)? {
        if (items.len() + prefixes.len()) as i64 >= max {
            return Ok((prefixes.into_iter().collect(), items, last));
        }
        let name = meta.get_str("name").unwrap_or_default().to_string();
        let rest = &name[prefix.len().min(name.len())..];
        if !delimiter.is_empty() {
            if let Some(i) = rest.find(delimiter) {
                prefixes.insert(format!("{prefix}{}", &rest[..i + delimiter.len()]));
                last = Some(name);
                continue;
            }
        }
        last = Some(name);
        items.push(meta);
    }
    Ok((prefixes.into_iter().collect(), items, None))
}

fn regex_escape(s: &str) -> String {
    s.chars().map(|c| if "\\^$.|?*+()[]{}".contains(c) { format!("\\{c}") } else { c.to_string() }).collect()
}

fn guess_type(name: &str) -> &'static str {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "heic" => "image/heic",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "txt" => "text/plain",
        "html" => "text/html",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
}

// ── MD5 (RFC 1321), only for md5Hash metadata ─────────────────────────────────

fn md5_compute(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21];
    let k: Vec<u32> = (0..64).map(|i| ((i as f64 + 1.0).sin().abs() * 4294967296.0) as u32).collect();
    let (mut a0, mut b0, mut c0, mut d0) = (0x67452301u32, 0xefcdab89u32, 0x98badcfeu32, 0x10325476u32);
    let mut msg = input.to_vec();
    let bit_len = (input.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());
    for chunk in msg.chunks(64) {
        let m: Vec<u32> = chunk.chunks(4).map(|w| u32::from_le_bytes([w[0], w[1], w[2], w[3]])).collect();
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    out[..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..].copy_from_slice(&d0.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_known_vectors() {
        assert_eq!(hex::encode(md5_compute(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex::encode(md5_compute(b"The quick brown fox jumps over the lazy dog")), "9e107d9d372bb6826bd81d3542a419d6");
    }

    #[test]
    fn ranges() {
        assert_eq!(parse_range("bytes=0-9", 100), Some((0, 9)));
        assert_eq!(parse_range("bytes=90-", 100), Some((90, 99)));
        assert_eq!(parse_range("bytes=-10", 100), Some((90, 99)));
        assert_eq!(parse_range("bytes=100-", 100), None);
    }

    #[test]
    fn multipart_related() {
        let body = b"--b1\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{\"name\":\"a/b.txt\",\"metadata\":{\"k\":\"v\"}}\r\n--b1\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b1--\r\n";
        let (meta, data, ctype) = split_multipart("multipart/related; boundary=b1", body).unwrap();
        assert_eq!(meta.name.as_deref(), Some("a/b.txt"));
        assert_eq!(data, b"hello");
        assert_eq!(ctype.as_deref(), Some("text/plain"));
    }

    #[test]
    fn names() {
        assert!(valid_name("users/u1/a.png").is_ok());
        assert!(valid_name("../x").is_err());
        assert!(valid_name("/x").is_err());
        assert!(valid_name("a//b").is_err());
        assert!(valid_name("a/b\nc").is_err());
        assert!(valid_name(&"x".repeat(1025)).is_err());
        assert!(valid_bucket("default").is_ok() && valid_bucket("dmart-app.appspot.com").is_ok());
        assert!(valid_bucket("Bad").is_err() && valid_bucket("-x").is_err() && valid_bucket("").is_err());
    }
}
