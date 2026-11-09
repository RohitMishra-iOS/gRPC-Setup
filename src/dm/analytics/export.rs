//! Admin API for the GA4 tables (dm-specs TBL-012, TBL-013):
//!
//!   GET  /admin/analytics/:app_id/tables                  tables of the app's dataset with row counts
//!   GET  /admin/analytics/:app_id/tables/:table?limit=    newest rows of one table
//!   GET  /admin/analytics/:app_id/tables/:table/export    NDJSON for `bq load --source_format=NEWLINE_DELIMITED_JSON`
//!   GET  /admin/analytics/schema/:kind                    BigQuery schema JSON (events | pseudonymous_users | users)
//!   POST /admin/analytics/:app_id/migrate-legacy?days=    old `event_data` batches → tables (idempotent)

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::bson::{doc, Bson, Document};
use serde::Deserialize;
use serde_json::{json, Value as Json};

use super::tables::{self, parse_table, Kind};
use crate::dm::admin::{db_error, error, ok, since, to_json};
use crate::dm::{AppState, RequestMeta};

type ApiResult = Result<Response, Response>;

fn checked_table(app_id: &str, table: &str) -> Result<String, Response> {
    if parse_table(table).is_none() {
        return Err(error(StatusCode::BAD_REQUEST, "table must be events_YYYYMMDD, pseudonymous_users_YYYYMMDD or users_YYYYMMDD"));
    }
    Ok(format!("{}.{table}", tables::dataset(app_id)))
}

pub async fn tables(State(state): State<AppState>, Path(app_id): Path<String>) -> ApiResult {
    let names = tables::list(&state.db, &app_id).await.map_err(db_error)?;
    let mut out = Vec::new();
    for name in names.iter().rev() {
        let short = name.split_once('.').map_or(name.as_str(), |(_, t)| t);
        let (kind, date) = parse_table(short).expect("listed tables parse");
        let rows = state.db.coll(name).estimated_document_count().await.unwrap_or(0);
        let kind = match kind {
            Kind::Events => "events",
            Kind::PseudonymousUsers => "pseudonymous_users",
            Kind::Users => "users",
        };
        out.push(json!({ "table": short, "kind": kind, "date": date.format("%Y-%m-%d").to_string(), "rows": rows }));
    }
    ok(json!({
        "app_id": app_id,
        "dataset": tables::dataset(&app_id),
        "timezone": state.config.analytics.timezone.name(),
        "currency": state.config.analytics.currency,
        "tables": out,
    }))
}

#[derive(Deserialize)]
pub struct RowsQuery {
    limit: Option<i64>,
}

pub async fn rows(State(state): State<AppState>, Path((app_id, table)): Path<(String, String)>, Query(query): Query<RowsQuery>) -> ApiResult {
    let name = checked_table(&app_id, &table)?;
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let sort = if table.starts_with("events_") { doc! { "event_timestamp": -1 } } else { doc! { "user_info.last_active_timestamp_micros": -1 } };
    let rows: Vec<Json> = state
        .db
        .coll(&name)
        .find(doc! {})
        .sort(sort)
        .limit(limit)
        .await
        .map_err(db_error)?
        .map_ok(to_json)
        .try_collect()
        .await
        .map_err(db_error)?;
    ok(json!({ "app_id": app_id, "table": table, "rows": rows }))
}

/// One JSON object per line, without `_id`, in the BigQuery export's field order.
pub async fn export(State(state): State<AppState>, Path((app_id, table)): Path<(String, String)>) -> ApiResult {
    let name = checked_table(&app_id, &table)?;
    let cursor = state.db.coll(&name).find(doc! {}).projection(doc! { "_id": 0 }).await.map_err(db_error)?;
    let stream = cursor.map(|row| match row {
        Ok(document) => {
            let mut line = Bson::Document(document).into_relaxed_extjson().to_string();
            line.push('\n');
            Ok::<_, std::io::Error>(line)
        }
        Err(err) => Err(std::io::Error::other(err.to_string())),
    });
    let disposition = format!("attachment; filename=\"{}.{table}.ndjson\"", tables::dataset(&app_id));
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-ndjson".to_string()), (header::CONTENT_DISPOSITION, disposition)],
        Body::from_stream(stream),
    )
        .into_response())
}

// ── BigQuery schema ──────────────────────────────────────────────────────────────────────────────

fn field(name: &str, kind: &str) -> Json {
    json!({ "name": name, "type": kind, "mode": "NULLABLE" })
}

fn record(name: &str, fields: Vec<Json>) -> Json {
    json!({ "name": name, "type": "RECORD", "mode": "NULLABLE", "fields": fields })
}

fn repeated(name: &str, fields: Vec<Json>) -> Json {
    json!({ "name": name, "type": "RECORD", "mode": "REPEATED", "fields": fields })
}

fn strings(names: &[&str]) -> Vec<Json> {
    names.iter().map(|n| field(n, "STRING")).collect()
}

fn value_fields(with_set_time: bool) -> Vec<Json> {
    let mut v = vec![field("string_value", "STRING"), field("int_value", "INTEGER"), field("float_value", "FLOAT"), field("double_value", "FLOAT")];
    if with_set_time {
        v.push(field("set_timestamp_micros", "INTEGER"));
    }
    v
}

fn key_value(name: &str, with_set_time: bool) -> Json {
    repeated(name, vec![field("key", "STRING"), record("value", value_fields(with_set_time))])
}

pub fn events_schema() -> Json {
    let manual = strings(&["campaign_id", "campaign_name", "source", "medium", "term", "content", "source_platform", "creative_format", "marketing_tactic"]);
    json!([
        field("event_date", "STRING"),
        field("event_timestamp", "INTEGER"),
        field("event_name", "STRING"),
        key_value("event_params", false),
        field("event_previous_timestamp", "INTEGER"),
        field("event_value_in_usd", "FLOAT"),
        field("event_bundle_sequence_id", "INTEGER"),
        field("event_server_timestamp_offset", "INTEGER"),
        field("user_id", "STRING"),
        field("user_pseudo_id", "STRING"),
        record("privacy_info", strings(&["analytics_storage", "ads_storage", "uses_transient_token"])),
        key_value("user_properties", true),
        field("user_first_touch_timestamp", "INTEGER"),
        record("user_ltv", vec![field("revenue", "FLOAT"), field("currency", "STRING")]),
        record("device", {
            let mut d = strings(&[
                "category", "mobile_brand_name", "mobile_model_name", "mobile_marketing_name", "mobile_os_hardware_model",
                "operating_system", "operating_system_version", "vendor_id", "advertising_id", "language", "is_limited_ad_tracking",
            ]);
            d.push(field("time_zone_offset_seconds", "INTEGER"));
            d.extend(strings(&["browser", "browser_version"]));
            d.push(record("web_info", strings(&["browser", "browser_version", "hostname"])));
            d
        }),
        record("geo", strings(&["continent", "sub_continent", "country", "region", "city", "metro"])),
        record("app_info", strings(&["id", "version", "install_store", "firebase_app_id", "install_source"])),
        record("traffic_source", strings(&["name", "medium", "source"])),
        field("stream_id", "STRING"),
        field("platform", "STRING"),
        record("event_dimensions", strings(&["hostname"])),
        record("ecommerce", vec![
            field("total_item_quantity", "INTEGER"),
            field("purchase_revenue_in_usd", "FLOAT"),
            field("purchase_revenue", "FLOAT"),
            field("refund_value_in_usd", "FLOAT"),
            field("refund_value", "FLOAT"),
            field("shipping_value_in_usd", "FLOAT"),
            field("shipping_value", "FLOAT"),
            field("tax_value_in_usd", "FLOAT"),
            field("tax_value", "FLOAT"),
            field("unique_items", "INTEGER"),
            field("transaction_id", "STRING"),
        ]),
        repeated("items", {
            let mut i = strings(&[
                "item_id", "item_name", "item_brand", "item_variant", "item_category", "item_category2", "item_category3",
                "item_category4", "item_category5",
            ]);
            i.extend([field("price_in_usd", "FLOAT"), field("price", "FLOAT"), field("quantity", "INTEGER")]);
            i.extend([field("item_revenue_in_usd", "FLOAT"), field("item_revenue", "FLOAT"), field("item_refund_in_usd", "FLOAT"), field("item_refund", "FLOAT")]);
            i.extend(strings(&["coupon", "affiliation", "location_id", "item_list_id", "item_list_name", "item_list_index", "promotion_id", "promotion_name", "creative_name", "creative_slot"]));
            i.push(key_value("item_params", false));
            i
        }),
        record("collected_traffic_source", strings(&[
            "manual_campaign_id", "manual_campaign_name", "manual_source", "manual_medium", "manual_term", "manual_content",
            "manual_source_platform", "manual_creative_format", "manual_marketing_tactic", "gclid", "dclid", "srsltid",
        ])),
        field("is_active_user", "BOOLEAN"),
        field("batch_event_index", "INTEGER"),
        field("batch_page_id", "INTEGER"),
        field("batch_ordering_id", "INTEGER"),
        record("session_traffic_source_last_click", vec![
            record("manual_campaign", manual),
            record("google_ads_campaign", strings(&["customer_id", "account_name", "campaign_id", "campaign_name", "ad_group_id", "ad_group_name"])),
            record("cross_channel_campaign", strings(&["campaign_id", "campaign_name", "source", "medium", "source_platform", "default_channel_group", "primary_channel_group"])),
            record("sa360_campaign", strings(&["campaign_id", "campaign_name", "source", "medium", "ad_group_id", "ad_group_name", "engine_account_name", "engine_account_type", "manager_account_name"])),
            record("cm360_campaign", strings(&["campaign_id", "campaign_name", "source", "medium", "account_id", "account_name", "advertiser_id", "advertiser_name", "creative_id", "creative_format", "creative_name", "creative_type", "creative_type_id", "creative_version", "placement_id", "placement_cost_structure", "placement_name", "rendering_id", "site_id", "site_name"])),
            record("dv360_campaign", strings(&["campaign_id", "campaign_name", "source", "medium", "advertiser_id", "advertiser_name", "creative_id", "creative_format", "creative_name", "exchange_id", "exchange_name", "insertion_order_id", "insertion_order_name", "line_item_id", "line_item_name", "partner_id", "partner_name"])),
        ]),
        record("publisher", vec![field("ad_revenue_in_usd", "FLOAT"), field("ad_format", "STRING"), field("ad_source_name", "STRING"), field("ad_unit_id", "STRING")]),
    ])
}

fn user_schema(id_field: &str) -> Json {
    json!([
        field(id_field, "STRING"),
        field("stream_id", "STRING"),
        record("user_info", vec![field("last_active_timestamp_micros", "INTEGER"), field("user_first_touch_timestamp_micros", "INTEGER"), field("first_purchase_date", "STRING")]),
        record("device", strings(&["operating_system", "category", "mobile_brand_name", "mobile_model_name", "unified_screen_name"])),
        record("geo", strings(&["city", "country", "continent", "region"])),
        repeated("audiences", vec![field("id", "INTEGER"), field("name", "STRING"), field("membership_start_timestamp_micros", "INTEGER"), field("membership_expiry_timestamp_micros", "INTEGER"), field("npa", "BOOLEAN")]),
        repeated("user_properties", vec![field("key", "STRING"), record("value", vec![field("string_value", "STRING"), field("set_timestamp_micros", "INTEGER"), field("user_property_name", "STRING")])]),
        record("user_ltv", vec![
            field("revenue_in_usd", "FLOAT"), field("sessions", "INTEGER"), field("engagement_time_millis", "INTEGER"),
            field("purchases", "INTEGER"), field("engaged_sessions", "INTEGER"), field("session_duration_micros", "INTEGER"),
        ]),
        record("privacy_info", strings(&["is_limited_ad_tracking", "is_ads_personalization_allowed"])),
        field("occurrence_date", "STRING"),
        field("last_updated_date", "STRING"),
    ])
}

pub async fn schema(Path(kind): Path<String>) -> ApiResult {
    let schema = match kind.as_str() {
        "events" => events_schema(),
        "pseudonymous_users" => user_schema("pseudo_user_id"),
        "users" => user_schema("user_id"),
        _ => return Err(error(StatusCode::BAD_REQUEST, "kind must be events, pseudonymous_users or users")),
    };
    ok(schema)
}

// ── Migration of old batches ─────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct MigrateQuery {
    days: Option<i64>,
}

/// Converts `event_data` batches (server versions before 3.2) received in the last `days` (default 7, max 365) into the
/// tables. Rows keep `_id = <batch_id>:<index>` (batches without an id use their document id), so a
/// second run stores nothing new.
pub async fn migrate_legacy(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<MigrateQuery>) -> ApiResult {
    let from = since(query.days);
    let filter = doc! {
        "received_at": { "$gte": from },
        "$or": [ { "app_id": &app_id }, { "app_id": { "$exists": false } }, { "app_id": "" } ],
    };
    let mut cursor = state.db.event_data.find(filter).await.map_err(db_error)?;
    let (mut batches, mut stored, mut failed) = (0_u64, 0_u64, 0_u64);
    let meta = RequestMeta { app_id: Some(app_id.clone()), ..Default::default() };
    while let Some(batch) = cursor.try_next().await.map_err(db_error)? {
        batches += 1;
        let received_ms = batch.get_datetime("received_at").map(|t| t.timestamp_millis()).unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
        let id = match batch.get("_id") {
            Some(Bson::ObjectId(oid)) => oid.to_hex(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        let mut body = to_json(strip(batch));
        if let Some(object) = body.as_object_mut() {
            if object.get("batch_id").and_then(Json::as_str).is_none_or(str::is_empty) {
                object.insert("batch_id".into(), json!(format!("legacy-{id}")));
            }
            object.entry("app_id").or_insert(json!(app_id));
        }
        // Validate against the time the batch arrived, not today.
        let Ok(checked) = crate::events::check_batch(&body, received_ms) else {
            failed += 1;
            continue;
        };
        let (context, request) = super::legacy::convert(&checked.body, &meta);
        let Ok(converted) = super::check_request(request, received_ms * 1000) else {
            failed += 1;
            continue;
        };
        match super::store_unclaimed(&state, &meta, &context, &converted, received_ms * 1000).await {
            Ok(count) => stored += count as u64,
            Err(err) => {
                eprintln!("[Analytics] migrate-legacy: {err}");
                failed += 1;
            }
        }
    }
    ok(json!({ "app_id": app_id, "since": from.try_to_rfc3339_string().unwrap_or_default(), "batches": batches, "events_stored": stored, "batches_failed": failed }))
}

fn strip(mut batch: Document) -> Document {
    for key in ["_id", "received_at", "status_code", "accepted", "duplicate", "rejected_events", "warnings"] {
        batch.remove(key);
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(schema: &Json) -> Vec<String> {
        schema.as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap().to_string()).collect()
    }

    // spec: TBL-012, TBL-002
    #[test]
    fn schema_matches_rows() {
        let top = names(&events_schema());
        assert_eq!(top.len(), 30);
        for column in ["event_date", "event_params", "user_ltv", "device", "geo", "app_info", "ecommerce", "items", "publisher", "session_traffic_source_last_click"] {
            assert!(top.contains(&column.to_string()), "{column}");
        }
        assert!(checked_table("dmart-ready", "events_x").is_err());
        assert_eq!(checked_table("dmart-ready", "events_20261109").unwrap(), "analytics_dmart-ready.events_20261109");
        assert_eq!(names(&user_schema("pseudo_user_id"))[0], "pseudo_user_id");
    }
}
