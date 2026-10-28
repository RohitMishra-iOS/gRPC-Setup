//! gRPC StatusService implementation (shared by native gRPC and gRPC-Web).

use prost_types::{value::Kind, ListValue, Struct, Value};
use serde_json::{Map, Number, Value as Json};
use tonic::{Request, Response, Status};

use crate::db::Db;
use crate::events;
use crate::pb::status_service_server::StatusService;
use crate::pb::{StatusRequest, StatusResponse};

// ── google.protobuf.Struct ⇄ serde_json::Value conversion ─────────────────────

fn value_to_json(value: &Value) -> Json {
    match &value.kind {
        Some(Kind::NumberValue(n)) => number_to_json(*n),
        Some(Kind::StringValue(s)) => Json::String(s.clone()),
        Some(Kind::BoolValue(b)) => Json::Bool(*b),
        Some(Kind::StructValue(s)) => struct_to_json(s),
        Some(Kind::ListValue(l)) => Json::Array(l.values.iter().map(value_to_json).collect()),
        Some(Kind::NullValue(_)) | None => Json::Null,
    }
}

// Struct numbers are always f64; keep whole numbers as integers so they log and
// store the same way as the /events JSON path (e.g. 1700000000000, not 1.7e12).
fn number_to_json(n: f64) -> Json {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
    if n.fract() == 0.0 && n.abs() <= MAX_SAFE {
        Json::Number((n as i64).into())
    } else {
        Number::from_f64(n).map_or(Json::Null, Json::Number)
    }
}

pub fn struct_to_json(s: &Struct) -> Json {
    Json::Object(s.fields.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect())
}

fn json_to_value(v: &Json) -> Value {
    let kind = match v {
        Json::Null => Kind::NullValue(0),
        Json::Bool(b) => Kind::BoolValue(*b),
        Json::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        Json::String(s) => Kind::StringValue(s.clone()),
        Json::Array(a) => Kind::ListValue(ListValue { values: a.iter().map(json_to_value).collect() }),
        Json::Object(o) => Kind::StructValue(map_to_struct(o)),
    };
    Value { kind: Some(kind) }
}

fn map_to_struct(o: &Map<String, Json>) -> Struct {
    Struct { fields: o.iter().map(|(k, v)| (k.clone(), json_to_value(v))).collect() }
}

pub fn json_to_struct(v: &Json) -> Struct {
    v.as_object().map(map_to_struct).unwrap_or_default()
}

// ── CheckStatus ───────────────────────────────────────────────────────────────

pub async fn check_status(db: &Db, request: StatusRequest) -> StatusResponse {
    let body = request.message.as_ref().map_or_else(|| Json::Object(Map::new()), struct_to_json);
    println!("[gRPC] CheckStatus ← {body}");

    let (code, payload) = events::process_batch(db, &body).await;
    if code == 200 {
        println!("[gRPC] ✓ batch_id={} events={}", events::batch_id_label(&body), events::events_len(&body));
    }

    StatusResponse {
        code: code.into(),
        status: if code == 200 { "SUCCESS" } else { "ERROR" }.into(),
        message: Some(json_to_struct(&payload)),
    }
}

pub struct StatusSvc {
    pub db: Db,
}

#[tonic::async_trait]
impl StatusService for StatusSvc {
    async fn check_status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(check_status(&self.db, request.into_inner()).await))
    }
}
