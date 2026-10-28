//! PerformanceService/UploadPerformance — one document per record (`_id` = record id, so
//! retries are idempotent). Summaries are computed by GET /admin/performance/:app_id.

use mongodb::bson::{self, doc, Bson, DateTime, Document};
use mongodb::error::ErrorKind;
use serde_json::Value as Json;
use tonic::Status;

use super::{authorize, context_doc, non_empty, require_context, unavailable, AppState, RequestMeta};
use crate::db::ms_to_datetime;
use crate::pb_dm::{Ack, ClientContext, PerfKind, PerfRecord, UploadPerformanceRequest};

const MAX_RECORDS_PER_CALL: usize = 500;

pub async fn upload_performance(state: &AppState, meta: &RequestMeta, request: UploadPerformanceRequest) -> Result<Ack, Status> {
    let context = require_context(request.context, meta)?;
    authorize(&state.config, meta, &context.app_id)?;
    if request.records.len() > MAX_RECORDS_PER_CALL {
        return Err(Status::invalid_argument(format!("at most {MAX_RECORDS_PER_CALL} records per call")));
    }
    let documents: Vec<Document> = request
        .records
        .iter()
        .filter(|r| !r.id.is_empty())
        .map(|r| record_document(&context, r))
        .collect();
    if documents.is_empty() {
        return Ok(Ack {});
    }

    match state.db.perf_records.insert_many(documents).ordered(false).await {
        Ok(_) => Ok(Ack {}),
        Err(err) if only_duplicates(&err) => Ok(Ack {}), // some/all were already stored by a retry
        Err(err) => Err(unavailable(err)),
    }
}

/// True if an unordered insert_many failed only because some ids already existed.
fn only_duplicates(err: &mongodb::error::Error) -> bool {
    match err.kind.as_ref() {
        ErrorKind::InsertMany(failure) => {
            failure.write_concern_error.is_none()
                && failure.write_errors.as_ref().is_some_and(|errors| errors.iter().all(|e| e.code == 11000))
        }
        _ => false,
    }
}

pub fn kind_name(kind: PerfKind) -> &'static str {
    match kind {
        PerfKind::Trace => "trace",
        PerfKind::NetworkRequest => "network_request",
        PerfKind::ScreenRendering => "screen_rendering",
        PerfKind::AppStart => "app_start",
        PerfKind::AppForeground => "app_foreground",
        PerfKind::AppBackground => "app_background",
        PerfKind::MetricKit => "metric_kit",
        PerfKind::Unspecified => "unspecified",
    }
}

fn record_document(context: &ClientContext, record: &PerfRecord) -> Document {
    let kind = PerfKind::try_from(record.kind).unwrap_or(PerfKind::Unspecified);
    let counters: Document = record
        .counters
        .iter()
        .map(|(k, v)| (k.replace('.', "_"), Bson::Int64(*v)))
        .collect();
    let attributes: Document = record
        .attributes
        .iter()
        .map(|(k, v)| (k.replace('.', "_"), Bson::String(v.clone())))
        .collect();

    let mut document = doc! {
        "_id": &record.id,
        "app_id": &context.app_id,
        "installation_id": &context.installation_id,
        "kind": kind_name(kind),
        "name": &record.name,
        "start_time": ms_to_datetime(record.start_time_ms),
        "duration_us": record.duration_us,
        "duration_ms": record.duration_us as f64 / 1000.0,
        "counters": counters,
        "attributes": attributes,
        "app_version": non_empty(&record.app_version).unwrap_or(&context.app_version),
        "os_version": non_empty(&record.os_version).unwrap_or(&context.os_version),
        "device_model": &context.device_model,
        "network_type": &context.network_type,
        "uploaded_by": context_doc(context),
        "received_at": DateTime::now(),
    };

    if kind == PerfKind::NetworkRequest {
        document.insert("network", doc! {
            "url": &record.request_url,
            "host": host_of(&record.request_url),
            "method": &record.http_method,
            "response_code": record.response_code,
            "success": (200..400).contains(&record.response_code),
            "request_bytes": record.request_payload_bytes,
            "response_bytes": record.response_payload_bytes,
            "content_type": non_empty(&record.response_content_type),
        });
    }
    if kind == PerfKind::ScreenRendering {
        let get = |k: &str| record.counters.get(k).copied().unwrap_or(0);
        let total = get("total_frames").max(1) as f64;
        document.insert("rendering", doc! {
            "slow_rate": get("slow_frames") as f64 / total,
            "frozen_rate": get("frozen_frames") as f64 / total,
        });
    }
    if !record.metric_kit_json.is_empty() {
        match serde_json::from_str::<Json>(&record.metric_kit_json) {
            Ok(json) => document.insert("metric_kit", bson::to_bson(&json).unwrap_or(Bson::Null)),
            Err(_) => document.insert("metric_kit_raw", &record.metric_kit_json),
        };
    }
    document
}

fn host_of(url: &str) -> String {
    url.split("://").nth(1).unwrap_or(url).split(['/', ':']).next().unwrap_or_default().to_string()
}
