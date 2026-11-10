//! CrashlyticsService/UploadCrashReports
//!
//! Each report is stored once (`_id` = report id, so client retries are harmless), symbolicated
//! when a matching dSYM is on file, and grouped into an issue (`crash_issues`) by a signature
//! built from the error type plus the top in-app frames — the same idea as Crashlytics issues.
//!
//! Release monitoring signals produced here:
//!   • new issue          → alert (fatal) / log (non-fatal)
//!   • regression         → a crash on an issue an admin marked "closed" reopens it as "regressed" + alert
//!   • velocity           → ≥ DM_VELOCITY_ALERT_THRESHOLD crashes of one issue within an hour + alert
//! Crash-free users/sessions per version are computed on demand by GET /admin/stability/:app_id.

use mongodb::bson::{self, doc, Bson, DateTime, Document};
use mongodb::options::ReturnDocument;
use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use tonic::Status;

use super::symbols::{self, ImageInfo, SymFrame};
use super::{alerts, authorize, context_doc, non_empty, require_context, unavailable, AppState, RequestMeta};
use crate::db::{hex_addr, is_duplicate_key, ms_to_datetime};
use crate::pb_dm::{Ack, ClientContext, CrashKind, CrashReport, UploadCrashReportsRequest};

const MAX_REPORTS_PER_CALL: usize = 50;

pub async fn upload_crash_reports(state: &AppState, meta: &RequestMeta, request: UploadCrashReportsRequest) -> Result<Ack, Status> {
    let context = require_context(request.context, meta)?;
    authorize(&state.config, meta, &context.app_id)?;
    if request.reports.len() > MAX_REPORTS_PER_CALL {
        return Err(Status::invalid_argument(format!("at most {MAX_REPORTS_PER_CALL} reports per call")));
    }
    for report in request.reports {
        // An invalid report is dropped (and logged) rather than failing the call: the client
        // deletes what it sent on success, so one bad report can't block the others forever.
        let problems = validate_report(&report);
        if !problems.is_ok() {
            eprintln!("[Crashlytics] dropped report {}: {}", report.id, problems.summary());
            continue;
        }
        ingest(state, &context, report).await?;
    }
    Ok(Ack {})
}

/// Size and format rules for one crash report (see crate::validate::limits).
pub fn validate_report(r: &CrashReport) -> crate::validate::V {
    use crate::validate::{limits, V};
    let mut v = V::new();
    v.id("id", &r.id, limits::ID);
    v.check(CrashKind::try_from(r.kind).is_ok_and(|k| k != CrashKind::Unspecified), "kind", "must be a known CrashKind");
    if r.timestamp_ms != 0 {
        let now = chrono::Utc::now().timestamp_millis();
        v.check(r.timestamp_ms > now - 365 * 86_400_000 && r.timestamp_ms < now + 86_400_000, "timestamp_ms", "must be within the last year (and not in the future)");
    }
    v.text("launch_id", &r.launch_id, limits::ID);
    for (field, value) in [("app_version", &r.app_version), ("app_build", &r.app_build), ("os_version", &r.os_version), ("device_model", &r.device_model), ("bundle_id", &r.bundle_id), ("signal_name", &r.signal_name)] {
        v.text(field, value, limits::SHORT_TEXT);
    }
    v.text("user_id", &r.user_id, limits::USER_ID);
    v.max_count("custom_keys", r.custom_keys.len(), limits::CRASH_KEYS);
    for (k, val) in &r.custom_keys {
        v.check(!k.is_empty() && k.len() <= limits::CRASH_KEY_LENGTH && val.len() <= limits::CRASH_KEY_LENGTH, format!("custom_keys.{k}"), format!("keys and values must be at most {} bytes", limits::CRASH_KEY_LENGTH));
    }
    let log_bytes: usize = r.logs.iter().map(String::len).sum();
    v.check(log_bytes <= limits::CRASH_LOG_BYTES * 2, "logs", format!("must total at most {} bytes", limits::CRASH_LOG_BYTES * 2));
    v.max_count("breadcrumbs", r.breadcrumbs.len(), limits::CRASH_BREADCRUMBS);
    v.check(r.exception_name.len() <= 1024, "exception_name", "must be at most 1024 bytes");
    v.check(r.exception_reason.len() <= limits::CRASH_REASON, "exception_reason", format!("must be at most {} bytes", limits::CRASH_REASON));
    v.max_count("error_user_info", r.error_user_info.len(), limits::CRASH_KEYS);
    v.max_count("return_addresses", r.return_addresses.len(), limits::CRASH_FRAMES);
    v.max_count("frames", r.frames.len(), limits::CRASH_FRAMES);
    for (i, f) in r.frames.iter().enumerate() {
        v.check(f.symbol.len() <= 4096 && f.file.len() <= 1024 && f.library.len() <= 1024 && f.line >= 0, format!("frames[{i}]"), "symbol ≤ 4096 bytes, file/library ≤ 1024 bytes, line ≥ 0");
    }
    v.max_count("binary_images", r.binary_images.len(), limits::CRASH_IMAGES);
    for (i, image) in r.binary_images.iter().enumerate() {
        v.check(image.name.len() <= 1024 && image.uuid.len() <= 64, format!("binary_images[{i}]"), "name ≤ 1024 bytes, uuid ≤ 64");
    }
    v.check(r.metric_kit_json.len() <= limits::METRIC_KIT_JSON, "metric_kit_json", format!("must be at most {} bytes", limits::METRIC_KIT_JSON));
    v
}

pub fn kind_name(kind: CrashKind) -> &'static str {
    match kind {
        CrashKind::Signal => "signal",
        CrashKind::Exception => "exception",
        CrashKind::NonFatalError => "non_fatal_error",
        CrashKind::CustomException => "custom_exception",
        CrashKind::MetricKitDiagnostic => "metric_kit_diagnostic",
        CrashKind::Unspecified => "unspecified",
    }
}

async fn ingest(state: &AppState, context: &ClientContext, mut report: CrashReport) -> Result<(), Status> {
    if report.id.is_empty() {
        return Err(Status::invalid_argument("report.id is required"));
    }
    // Already stored (client retried after a lost response) → nothing to do, and the issue
    // counters must not be incremented twice.
    if state.db.crash_reports.find_one(doc! { "_id": &report.id }).projection(doc! { "_id": 1 }).await.map_err(unavailable)?.is_some() {
        return Ok(());
    }

    let kind = CrashKind::try_from(report.kind).unwrap_or(CrashKind::Unspecified);
    let images: Vec<ImageInfo> = report
        .binary_images
        .iter()
        .map(|i| ImageInfo {
            name: i.name.clone(),
            uuid: i.uuid.to_uppercase(),
            load_address: i.load_address,
            size: i.size,
            is_main_executable: i.is_main_executable,
        })
        .collect();

    let mut retraced = false;
    let frames = if !report.return_addresses.is_empty() {
        symbols::symbolicate(state, &images, &report.return_addresses, kind == CrashKind::Signal).await
    } else if !report.frames.is_empty() && kind != CrashKind::CustomException {
        // Android / web: frames arrive symbolicated by the runtime; R8-obfuscated ones are
        // retraced with the build's mapping file when one has been uploaded.
        let version = non_empty(&report.app_version).unwrap_or(&context.app_version).to_string();
        let build = non_empty(&report.app_build).unwrap_or(&context.app_build).to_string();
        let mapping = super::mapping::load(state, &context.app_id, &version, &build).await;
        let client: Vec<super::mapping::ClientFrame> = report
            .frames
            .iter()
            .map(|f| super::mapping::ClientFrame { symbol: &f.symbol, file: &f.file, line: f.line, library: &f.library })
            .collect();
        let (frames, any) = super::mapping::sym_frames(&client, mapping.as_deref());
        retraced = any;
        if let Some(name) = super::mapping::class_name(mapping.as_deref(), &report.exception_name) {
            report.exception_name = name;
        }
        frames
    } else {
        Vec::new()
    };
    let symbolicated = frames.iter().any(|f| f.symbol.is_some());
    let metric_kit: Option<Json> = non_empty(&report.metric_kit_json).and_then(|j| serde_json::from_str(j).ok());

    let issue = signature(&report, kind, &frames, metric_kit.as_ref());
    let issue_id = issue_id(&context.app_id, &issue.signature);

    let app_version = non_empty(&report.app_version).unwrap_or(&context.app_version).to_string();
    let timestamp = if report.timestamp_ms > 0 { ms_to_datetime(report.timestamp_ms) } else { DateTime::now() };

    let mut document = report_document(context, &report, kind, &frames, symbolicated, metric_kit, &issue_id, &app_version, timestamp);
    if retraced {
        document.insert("retraced", true);
    }
    let location = super::symbols::crash_location(&frames);
    if let Some(location) = &location {
        document.insert("crash_location", location);
    }
    match state.db.crash_reports.insert_one(document).await {
        Ok(_) => {}
        Err(err) if is_duplicate_key(&err) => return Ok(()), // concurrent retry won the race
        Err(err) => return Err(unavailable(err)),
    }

    update_issue(state, context, &report, kind, &issue, &issue_id, &app_version, timestamp, location.as_deref()).await
}

// ── Grouping ─────────────────────────────────────────────────────────────────

pub struct IssueSignature {
    pub signature: String,
    pub title: String,
    pub subtitle: String,
}

/// Up to two frame keys, preferring in-app frames (main executable or images with a dSYM).
fn top_frames(frames: &[SymFrame]) -> Vec<&SymFrame> {
    let in_app: Vec<&SymFrame> = frames.iter().filter(|f| f.in_app).take(2).collect();
    if in_app.is_empty() { frames.iter().take(2).collect() } else { in_app }
}

pub fn signature(report: &CrashReport, kind: CrashKind, frames: &[SymFrame], metric_kit: Option<&Json>) -> IssueSignature {
    let tops = top_frames(frames);
    let frame_keys = tops.iter().map(|f| f.key()).collect::<Vec<_>>().join(" > ");
    let top_display = tops.first().map(|f| f.display()).unwrap_or_default();

    match kind {
        CrashKind::Signal => {
            let name = non_empty(&report.signal_name).map(str::to_string).unwrap_or_else(|| format!("SIG{}", report.signal_number));
            IssueSignature {
                signature: format!("signal|{name}|{frame_keys}"),
                title: name,
                subtitle: if top_display.is_empty() { report.exception_reason.clone() } else { top_display },
            }
        }
        CrashKind::Exception => IssueSignature {
            // The reason often contains object addresses/ids, so it isn't part of the signature.
            signature: format!("exception|{}|{frame_keys}", report.exception_name),
            title: report.exception_name.clone(),
            subtitle: truncate(&report.exception_reason, 160),
        },
        CrashKind::NonFatalError => IssueSignature {
            signature: format!("error|{}|{}|{frame_keys}", report.exception_name, report.error_code),
            // NSError domain + code on Apple; JVM/JS exceptions have no code.
            title: if report.error_code == 0 { report.exception_name.clone() } else { format!("{} ({})", report.exception_name, report.error_code) },
            subtitle: if top_display.is_empty() { truncate(&report.exception_reason, 160) } else { top_display },
        },
        CrashKind::CustomException => {
            let custom = report
                .frames
                .iter()
                .take(2)
                .map(|f| if f.symbol.is_empty() { format!("{}:{}", f.file, f.line) } else { f.symbol.clone() })
                .collect::<Vec<_>>()
                .join(" > ");
            IssueSignature {
                signature: format!("custom|{}|{custom}", report.exception_name),
                title: report.exception_name.clone(),
                subtitle: truncate(&report.exception_reason, 160),
            }
        }
        CrashKind::MetricKitDiagnostic | CrashKind::Unspecified => metric_kit_signature(report, metric_kit),
    }
}

/// MetricKit payloads: classify by diagnostic type, and for crashes use the signal/exception
/// type plus the attributed thread's top frame (binary + offset).
fn metric_kit_signature(report: &CrashReport, payload: Option<&Json>) -> IssueSignature {
    let Some(payload) = payload else {
        return IssueSignature {
            signature: format!("metrickit|{}", report.exception_reason),
            title: "MetricKit diagnostic".into(),
            subtitle: report.exception_reason.clone(),
        };
    };
    for (key, label) in [
        ("crashDiagnostics", "crash"),
        ("hangDiagnostics", "hang"),
        ("cpuExceptionDiagnostics", "CPU exception"),
        ("diskWriteExceptionDiagnostics", "disk write exception"),
        ("appLaunchDiagnostics", "slow launch"),
    ] {
        let Some(first) = payload.get(key).and_then(Json::as_array).and_then(|a| a.first()) else { continue };
        let meta = first.get("diagnosticMetaData");
        let detail = ["signal", "exceptionType", "exceptionCode", "hangDuration"]
            .iter()
            .filter_map(|k| meta.and_then(|m| m.get(*k)).map(|v| v.to_string().trim_matches('"').to_string()))
            .collect::<Vec<_>>()
            .join(" ");
        let top = metric_kit_top_frame(first).unwrap_or_default();
        return IssueSignature {
            signature: format!("metrickit|{key}|{detail}|{top}"),
            title: format!("MetricKit {label}{}", if detail.is_empty() { String::new() } else { format!(" ({detail})") }),
            subtitle: top,
        };
    }
    IssueSignature { signature: "metrickit|other".into(), title: "MetricKit diagnostic".into(), subtitle: String::new() }
}

fn metric_kit_top_frame(diagnostic: &Json) -> Option<String> {
    let stacks = diagnostic.get("callStackTree")?.get("callStacks")?.as_array()?;
    let stack = stacks
        .iter()
        .find(|s| s.get("threadAttributed").and_then(Json::as_bool) == Some(true))
        .or_else(|| stacks.first())?;
    let frame = stack.get("callStackRootFrames")?.as_array()?.first()?;
    let binary = frame.get("binaryName")?.as_str()?;
    let offset = frame.get("offsetIntoBinaryTextSegment")?.as_u64()?;
    Some(format!("{binary}+0x{offset:x}"))
}

pub fn issue_id(app_id: &str, signature: &str) -> String {
    let digest = Sha256::digest(format!("{app_id}\n{signature}").as_bytes());
    hex::encode(&digest[..10])
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max { text.to_string() } else { format!("{}…", text.chars().take(max).collect::<String>()) }
}

// ── Storage ──────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn report_document(
    context: &ClientContext,
    report: &CrashReport,
    kind: CrashKind,
    frames: &[SymFrame],
    symbolicated: bool,
    metric_kit: Option<Json>,
    issue_id: &str,
    app_version: &str,
    timestamp: DateTime,
) -> Document {
    let breadcrumbs: Vec<Document> = report
        .breadcrumbs
        .iter()
        .map(|b| doc! { "timestamp": ms_to_datetime(b.timestamp_ms), "name": &b.name, "params": string_map(&b.params) })
        .collect();
    let custom_frames: Vec<Document> = report
        .frames
        .iter()
        .map(|f| doc! {
            "symbol": non_empty(&f.symbol), "file": non_empty(&f.file), "line": f.line,
            "address": if f.address == 0 { Bson::Null } else { Bson::String(hex_addr(f.address)) },
            "library": non_empty(&f.library),
        })
        .collect();
    let images: Vec<Document> = report
        .binary_images
        .iter()
        .map(|i| doc! {
            "name": &i.name, "uuid": i.uuid.to_uppercase(), "load_address": hex_addr(i.load_address),
            "size": i.size as i64, "is_main_executable": i.is_main_executable,
        })
        .collect();

    let mut document = doc! {
        "_id": &report.id,
        "app_id": &context.app_id,
        "installation_id": &context.installation_id,
        "user_id": non_empty(&report.user_id).or(non_empty(&context.user_id)),
        "kind": kind_name(kind),
        "is_fatal": report.is_fatal,
        "timestamp": timestamp,
        "launch_id": &report.launch_id,
        "app_version": app_version,
        "app_build": non_empty(&report.app_build).unwrap_or(&context.app_build),
        "os_version": non_empty(&report.os_version).unwrap_or(&context.os_version),
        "device_model": non_empty(&report.device_model).unwrap_or(&context.device_model),
        "bundle_id": non_empty(&report.bundle_id).unwrap_or(&context.bundle_id),
        "issue_id": issue_id,
        "custom_keys": string_map(&report.custom_keys),
        "logs": &report.logs,
        "breadcrumbs": breadcrumbs,
        "exception_name": non_empty(&report.exception_name),
        "exception_reason": non_empty(&report.exception_reason),
        "error_code": if kind == CrashKind::NonFatalError { Bson::Int64(report.error_code) } else { Bson::Null },
        "error_user_info": string_map(&report.error_user_info),
        "return_addresses": report.return_addresses.iter().map(|a| hex_addr(*a)).collect::<Vec<_>>(),
        "frames": custom_frames,
        "binary_images": images,
        "symbolicated": symbolicated,
        "symbolicated_frames": bson::to_bson(frames).unwrap_or(Bson::Array(vec![])),
        "uploaded_by": context_doc(context),
        "received_at": DateTime::now(),
    };
    if kind == CrashKind::Signal {
        document.insert("signal", doc! {
            "number": report.signal_number,
            "name": &report.signal_name,
            "code": report.signal_code,
            "fault_address": hex_addr(report.fault_address),
        });
    }
    if let Some(json) = metric_kit {
        document.insert("metric_kit", bson::to_bson(&json).unwrap_or(Bson::Null));
    } else if !report.metric_kit_json.is_empty() {
        document.insert("metric_kit_raw", &report.metric_kit_json);
    }
    document
}

fn string_map(map: &std::collections::HashMap<String, String>) -> Document {
    // Mongo field names can't contain '.' or start with '$'.
    map.iter()
        .map(|(k, v)| (k.replace('.', "_").trim_start_matches('$').to_string(), Bson::String(v.clone())))
        .collect()
}

/// Reads the stack back out of a stored report (for re-symbolication).
pub fn stored_stack(report: &Document) -> (Vec<ImageInfo>, Vec<u64>, bool) {
    let parse_hex = |s: &str| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok();
    let images = report
        .get_array("binary_images")
        .map(|items| {
            items
                .iter()
                .filter_map(Bson::as_document)
                .filter_map(|i| {
                    Some(ImageInfo {
                        name: i.get_str("name").ok()?.to_string(),
                        uuid: i.get_str("uuid").ok()?.to_string(),
                        load_address: parse_hex(i.get_str("load_address").ok()?)?,
                        size: i.get_i64("size").ok()? as u64,
                        is_main_executable: i.get_bool("is_main_executable").unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let addresses = report
        .get_array("return_addresses")
        .map(|items| items.iter().filter_map(Bson::as_str).filter_map(parse_hex).collect())
        .unwrap_or_default();
    let first_is_pc = report.get_str("kind").map(|k| k == "signal").unwrap_or(false);
    (images, addresses, first_is_pc)
}

#[allow(clippy::too_many_arguments)]
async fn update_issue(
    state: &AppState,
    context: &ClientContext,
    report: &CrashReport,
    kind: CrashKind,
    issue: &IssueSignature,
    issue_id: &str,
    app_version: &str,
    timestamp: DateTime,
    location: Option<&str>,
) -> Result<(), Status> {
    let now = DateTime::now();
    let mut latest = doc! { "last_report_id": &report.id, "last_version": app_version, "updated_at": now };
    if let Some(location) = location {
        // File:line of the newest symbolicated crash (dm-specs CRA-005).
        latest.insert("crash_location", location);
    }
    let previous = state
        .db
        .crash_issues
        .find_one_and_update(
            doc! { "_id": issue_id },
            doc! {
                "$setOnInsert": {
                    "app_id": &context.app_id,
                    "title": &issue.title,
                    "subtitle": &issue.subtitle,
                    "kind": kind_name(kind),
                    "is_fatal": report.is_fatal,
                    "signature": &issue.signature,
                    "first_seen": timestamp,
                    "first_version": app_version,
                    "status": "open",
                },
                "$inc": { "count": 1_i64 },
                "$max": { "last_seen": timestamp },
                "$addToSet": { "versions": app_version },
                "$set": latest,
            },
        )
        .upsert(true)
        .return_document(ReturnDocument::Before)
        .await
        .map_err(unavailable)?;

    let label = format!("[{}] {} — {} (v{app_version})", context.app_id, issue.title, issue.subtitle);
    match previous {
        None => {
            if report.is_fatal {
                alerts::notify(state, format!("🆕 New crash issue {label}  id={issue_id}"));
            } else {
                println!("[Crashlytics] new non-fatal issue {label} id={issue_id}");
            }
        }
        Some(previous) if previous.get_str("status").ok() == Some("closed") => {
            let _ = state
                .db
                .crash_issues
                .update_one(
                    doc! { "_id": issue_id },
                    doc! { "$set": { "status": "regressed", "regressed_at": now, "regressed_in": app_version } },
                )
                .await;
            alerts::notify(state, format!("♻️ Regression: closed issue is back {label}  id={issue_id}"));
        }
        Some(previous) => {
            if report.is_fatal {
                check_velocity(state, &previous, issue_id, &label).await;
            }
        }
    }
    Ok(())
}

async fn check_velocity(state: &AppState, issue: &Document, issue_id: &str, label: &str) {
    let now_ms = DateTime::now().timestamp_millis();
    let hour_ago = DateTime::from_millis(now_ms - 3_600_000);
    if let Ok(last) = issue.get_datetime("last_velocity_alert_at") {
        if last.timestamp_millis() > hour_ago.timestamp_millis() {
            return;
        }
    }
    let Ok(count) = state
        .db
        .crash_reports
        .count_documents(doc! { "issue_id": issue_id, "timestamp": { "$gte": hour_ago } })
        .await
    else {
        return;
    };
    if count >= state.config.velocity_alert_threshold {
        let _ = state
            .db
            .crash_issues
            .update_one(doc! { "_id": issue_id }, doc! { "$set": { "last_velocity_alert_at": DateTime::now() } })
            .await;
        alerts::notify(state, format!("🔥 Velocity alert: {count} crashes in the last hour {label}  id={issue_id}"));
    }
}
