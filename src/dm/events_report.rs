//! Admin API — the GA4 "Events: Event name" report (dm-specs EVR-001…EVR-004).
//!
//!   GET /admin/analytics/:app_id/events-report
//!       ?start=YYYY-MM-DD&end=YYYY-MM-DD   inclusive dates in `tz` (default: the last 28 days before today)
//!       &tz=Asia/Kolkata                   default: the property time zone (DM_ANALYTICS_TIMEZONE)
//!       &granularity=day|week|month        chart buckets (weeks start on Sunday, as in GA4)
//!       &series=a,b,c                      event names to plot (≤ 10); default: the top 5 by count
//!       &platform=IOS|ANDROID|WEB          optional filter
//!
//! Answers one row per event name (event count, total users, active users, total revenue in the
//! property currency) with totals, plus the over-time series: the total of every event and one
//! line per plotted event, with zero-filled buckets.

use std::collections::{BTreeMap, HashMap};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use mongodb::bson::{doc, Bson, Document};
use serde::Deserialize;
use serde_json::{json, Value as Json};

use super::admin::{db_error, error, ok};
use super::admin_analytics::param;
use super::analytics::tables::{self, Kind};
use super::AppState;

type ApiResult = Result<Response, Response>;

/// Most dates a report may span (GA4 allows 14 months of standard data).
const MAX_DAYS: i64 = 400;
const MAX_SERIES: usize = 10;
const MAX_ROWS: i64 = 5000;

#[derive(Deserialize)]
pub struct ReportQuery {
    start: Option<String>,
    end: Option<String>,
    tz: Option<String>,
    granularity: Option<String>,
    series: Option<String>,
    platform: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Granularity {
    Day,
    Week,
    Month,
}

impl Granularity {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.unwrap_or("day") {
            "day" => Ok(Self::Day),
            "week" => Ok(Self::Week),
            "month" => Ok(Self::Month),
            other => Err(format!("granularity must be day, week or month (got '{other}')")),
        }
    }
    fn unit(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
        }
    }
    fn name(self) -> &'static str {
        self.unit()
    }
    /// The first date of the bucket that holds `day`.
    fn bucket_start(self, day: NaiveDate) -> NaiveDate {
        match self {
            Self::Day => day,
            Self::Week => day - Duration::days(i64::from(day.weekday().num_days_from_sunday())),
            Self::Month => day.with_day(1).unwrap_or(day),
        }
    }
    fn next(self, day: NaiveDate) -> NaiveDate {
        match self {
            Self::Day => day + Duration::days(1),
            Self::Week => day + Duration::days(7),
            Self::Month => {
                let (y, m) = if day.month() == 12 { (day.year() + 1, 1) } else { (day.year(), day.month() + 1) };
                NaiveDate::from_ymd_opt(y, m, 1).unwrap_or(day + Duration::days(31))
            }
        }
    }
}

fn bad(message: impl AsRef<str>) -> Response {
    error(StatusCode::BAD_REQUEST, message.as_ref())
}

/// Midnight of `day` in `tz`, as epoch microseconds (the earliest instant if the zone skips it).
fn midnight_micros(day: NaiveDate, tz: Tz) -> i64 {
    let naive = day.and_hms_opt(0, 0, 0).expect("midnight");
    let local = tz.from_local_datetime(&naive).earliest().unwrap_or_else(|| tz.from_utc_datetime(&naive));
    local.timestamp_micros()
}

/// Validated request: dates, zone, buckets.
struct Plan {
    start: NaiveDate,
    end: NaiveDate,
    tz: Tz,
    granularity: Granularity,
    series: Option<Vec<String>>,
    platform: Option<String>,
}

fn plan(query: &ReportQuery, property_tz: Tz) -> Result<Plan, Response> {
    let tz: Tz = match query.tz.as_deref().filter(|s| !s.is_empty()) {
        Some(name) => name.parse().map_err(|_| bad(format!("unknown time zone '{name}'")))?,
        None => property_tz,
    };
    let today = Utc::now().with_timezone(&tz).date_naive();
    let date = |raw: &Option<String>, field: &str, fallback: NaiveDate| -> Result<NaiveDate, Response> {
        match raw.as_deref().filter(|s| !s.is_empty()) {
            Some(text) => NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| bad(format!("{field} must be YYYY-MM-DD (got '{text}')"))),
            None => Ok(fallback),
        }
    };
    let end = date(&query.end, "end", today - Duration::days(1))?;
    let start = date(&query.start, "start", end - Duration::days(27))?;
    if start > end {
        return Err(bad("start must be on or before end"));
    }
    if (end - start).num_days() + 1 > MAX_DAYS {
        return Err(bad(format!("a report covers at most {MAX_DAYS} days")));
    }
    let granularity = Granularity::parse(query.granularity.as_deref()).map_err(bad)?;
    let series = match query.series.as_deref() {
        None => None,
        Some(raw) => {
            let mut names: Vec<String> = Vec::new();
            for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if name.len() > 100 {
                    return Err(bad("series names are at most 100 characters"));
                }
                if !names.iter().any(|n| n == name) {
                    names.push(name.to_string());
                }
            }
            if names.len() > MAX_SERIES {
                return Err(bad(format!("plot at most {MAX_SERIES} events")));
            }
            Some(names)
        }
    };
    let platform = match query.platform.as_deref().filter(|s| !s.is_empty()) {
        None => None,
        Some(p) => {
            let p = p.to_ascii_uppercase();
            if !["IOS", "ANDROID", "WEB"].contains(&p.as_str()) {
                return Err(bad("platform must be IOS, ANDROID or WEB"));
            }
            Some(p)
        }
    };
    Ok(Plan { start, end, tz, granularity, series, platform })
}

/// Daily event tables that can hold rows of [start, end] in `tz` (tables are named by the
/// property time zone, so one day of margin on each side).
fn tables_for(app_id: &str, plan: &Plan) -> Vec<String> {
    let mut out = Vec::new();
    let mut day = plan.start - Duration::days(1);
    while day <= plan.end + Duration::days(1) {
        out.push(tables::table(app_id, Kind::Events, &day.format("%Y%m%d").to_string()));
        day += Duration::days(1);
    }
    out
}

/// A row counts its user as active when it shows engagement: first open/visit, engagement time,
/// or an engaged session (GA4's active-user definition).
fn engaged_flag() -> Bson {
    Bson::Document(doc! { "$cond": [
        { "$or": [
            { "$in": ["$event_name", ["first_open", "first_visit"]] },
            { "$gt": [{ "$ifNull": [param("engagement_time_msec"), 0] }, 0] },
            { "$in": [param("session_engaged"), [1, "1", true]] },
        ]},
        1, 0,
    ]})
}

/// Revenue of one row in USD: purchases + ad revenue − refunds (GA4 "Total revenue").
fn revenue_usd() -> Bson {
    Bson::Document(doc! { "$subtract": [
        { "$add": [
            { "$ifNull": ["$ecommerce.purchase_revenue_in_usd", 0] },
            { "$ifNull": ["$publisher.ad_revenue_in_usd", 0] },
        ]},
        { "$ifNull": ["$ecommerce.refund_value_in_usd", 0] },
    ]})
}

fn num(doc: &Document, key: &str) -> f64 {
    match doc.get(key) {
        Some(Bson::Int32(v)) => f64::from(*v),
        Some(Bson::Int64(v)) => *v as f64,
        Some(Bson::Double(v)) => *v,
        _ => 0.0,
    }
}

fn int(doc: &Document, key: &str) -> i64 {
    num(doc, key).round() as i64
}

pub async fn events_report(State(state): State<AppState>, Path(app_id): Path<String>, Query(query): Query<ReportQuery>) -> ApiResult {
    let cfg = &state.config.analytics;
    let plan = plan(&query, cfg.timezone)?;
    let from_us = midnight_micros(plan.start, plan.tz);
    let to_us = midnight_micros(plan.end + Duration::days(1), plan.tz);
    let names = tables_for(&app_id, &plan);
    let mut first = doc! { "event_timestamp": { "$gte": from_us, "$lt": to_us } };
    if let Some(platform) = &plan.platform {
        first.insert("platform", platform);
    }

    // 1. Per event name: count, users, active users, revenue — and totals over distinct users.
    let by_event = vec![
        doc! { "$group": {
            "_id": { "u": { "$ifNull": ["$user_pseudo_id", "$user_id"] }, "e": "$event_name" },
            "c": { "$sum": 1 }, "eng": { "$max": engaged_flag() }, "rev": { "$sum": revenue_usd() },
        }},
        doc! { "$group": { "_id": "$_id.u", "events": { "$push": { "e": "$_id.e", "c": "$c", "rev": "$rev" } }, "active": { "$max": "$eng" } } },
        doc! { "$facet": {
            "rows": [
                { "$unwind": "$events" },
                { "$group": {
                    "_id": "$events.e",
                    "count": { "$sum": "$events.c" },
                    "users": { "$sum": 1 },
                    "active_users": { "$sum": "$active" },
                    "revenue_usd": { "$sum": "$events.rev" },
                }},
                { "$sort": { "count": -1, "_id": 1 } },
                { "$limit": MAX_ROWS },
            ],
            "totals": [
                { "$group": { "_id": Bson::Null, "users": { "$sum": 1 }, "active_users": { "$sum": "$active" } } },
            ],
        }},
    ];
    // 2. Over time: per bucket and event name (small: buckets × names). Runs alongside query 1.
    let tz_name = plan.tz.name();
    let date = doc! { "$toDate": { "$toLong": { "$divide": ["$event_timestamp", 1000] } } };
    let mut trunc = doc! { "date": date, "unit": plan.granularity.unit(), "timezone": tz_name };
    if plan.granularity == Granularity::Week {
        trunc.insert("startOfWeek", "sunday");
    }
    let over_time = vec![
        doc! { "$group": {
            "_id": {
                "b": { "$dateToString": { "format": "%Y-%m-%d", "date": { "$dateTrunc": trunc }, "timezone": tz_name } },
                "e": "$event_name",
            },
            "c": { "$sum": 1 },
        }},
    ];
    let (by_event, points) = tokio::try_join!(
        tables::aggregate_union(&state.db, &names, first.clone(), by_event),
        tables::aggregate_union(&state.db, &names, first, over_time),
    )
    .map_err(db_error)?;
    let facet = by_event.into_iter().next().unwrap_or_default();

    // Property currency: USD amounts are converted back with DM_USD_RATES; without a rate, USD.
    let (currency, per_usd) = match cfg.usd_rates.get(&cfg.currency) {
        Some(rate) if *rate > 0.0 => (cfg.currency.clone(), 1.0 / rate),
        _ => ("USD".to_string(), 1.0),
    };
    let money = |usd: f64| (usd * per_usd * 100.0).round() / 100.0;

    let rows: Vec<Document> = facet.get_array("rows").map(|a| a.iter().filter_map(|b| b.as_document().cloned()).collect()).unwrap_or_default();
    let totals = facet.get_array("totals").ok().and_then(|a| a.first()).and_then(|b| b.as_document()).cloned().unwrap_or_default();
    let total_count: i64 = rows.iter().map(|r| int(r, "count")).sum();
    let total_revenue_usd: f64 = rows.iter().map(|r| num(r, "revenue_usd")).sum();
    let row_json: Vec<Json> = rows
        .iter()
        .map(|r| {
            json!({
                "name": r.get_str("_id").unwrap_or("(not set)"),
                "count": int(r, "count"),
                "users": int(r, "users"),
                "active_users": int(r, "active_users"),
                "revenue": money(num(r, "revenue_usd")),
            })
        })
        .collect();

    // 3. The plotted events: given, or the top 5 by count.
    let series_names: Vec<String> = match &plan.series {
        Some(names) => names.clone(),
        None => rows.iter().take(5).filter_map(|r| r.get_str("_id").ok().map(str::to_string)).collect(),
    };

    let mut by_bucket: BTreeMap<String, (i64, HashMap<String, i64>)> = BTreeMap::new();
    let mut day = plan.granularity.bucket_start(plan.start);
    while day <= plan.end {
        by_bucket.insert(day.format("%Y-%m-%d").to_string(), (0, HashMap::new()));
        day = plan.granularity.next(day);
    }
    for point in points {
        let Ok(id) = point.get_document("_id") else { continue };
        let Ok(bucket) = id.get_str("b") else { continue };
        let Some(entry) = by_bucket.get_mut(bucket) else { continue };
        let count = int(&point, "c");
        entry.0 += count;
        if let Ok(name) = id.get_str("e") {
            if series_names.iter().any(|n| n == name) {
                *entry.1.entry(name.to_string()).or_default() += count;
            }
        }
    }
    let buckets: Vec<Json> = by_bucket
        .into_iter()
        .map(|(date, (total, values))| {
            let values: serde_json::Map<String, Json> = series_names.iter().map(|n| (n.clone(), json!(values.get(n).copied().unwrap_or(0)))).collect();
            json!({ "date": date, "total": total, "values": values })
        })
        .collect();

    ok(json!({
        "app_id": app_id,
        "start": plan.start.format("%Y-%m-%d").to_string(),
        "end": plan.end.format("%Y-%m-%d").to_string(),
        "tz": tz_name,
        "granularity": plan.granularity.name(),
        "platform": plan.platform,
        "currency": currency,
        "totals": {
            "count": total_count,
            "users": int(&totals, "users"),
            "active_users": int(&totals, "active_users"),
            "revenue": money(total_revenue_usd),
        },
        "rows": row_json,
        "row_count": row_json.len(),
        "series": { "names": series_names, "buckets": buckets },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weeks_start_on_sunday_and_months_on_the_first() {
        let d = NaiveDate::from_ymd_opt(2026, 11, 11).unwrap(); // a Wednesday
        assert_eq!(Granularity::Week.bucket_start(d), NaiveDate::from_ymd_opt(2026, 11, 8).unwrap());
        assert_eq!(Granularity::Month.bucket_start(d), NaiveDate::from_ymd_opt(2026, 11, 1).unwrap());
        assert_eq!(Granularity::Month.next(NaiveDate::from_ymd_opt(2026, 12, 1).unwrap()), NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());
    }

    #[test]
    fn plan_defaults_to_the_last_28_days_and_validates() {
        let q = |s: Option<&str>, e: Option<&str>, g: Option<&str>| ReportQuery {
            start: s.map(Into::into), end: e.map(Into::into), tz: Some("Asia/Kolkata".into()), granularity: g.map(Into::into), series: None, platform: None,
        };
        let p = plan(&q(None, None, None), chrono_tz::UTC).unwrap();
        assert_eq!((p.end - p.start).num_days(), 27);
        assert!(plan(&q(Some("2026-11-09"), Some("2026-11-01"), None), chrono_tz::UTC).is_err());
        assert!(plan(&q(Some("2026-13-01"), None, None), chrono_tz::UTC).is_err());
        assert!(plan(&q(None, None, Some("hour")), chrono_tz::UTC).is_err());
        assert!(plan(&q(Some("2024-01-01"), Some("2026-01-01"), None), chrono_tz::UTC).is_err());
    }

    #[test]
    fn midnight_is_in_the_requested_zone() {
        let d = NaiveDate::from_ymd_opt(2026, 11, 9).unwrap();
        // 2026-11-09 00:00 IST = 2026-11-08 18:30 UTC
        assert_eq!(midnight_micros(d, chrono_tz::Asia::Kolkata), Utc.with_ymd_and_hms(2026, 11, 8, 18, 30, 0).unwrap().timestamp_micros());
    }
}
