//! Dataset / table naming, per-table indexes, retention and multi-day reads (dm-specs TBL-001).
//!
//!   analytics_<app_id>.events_YYYYMMDD
//!   analytics_<app_id>.pseudonymous_users_YYYYMMDD
//!   analytics_<app_id>.users_YYYYMMDD

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use mongodb::bson::{doc, Document};

use crate::db::{index, Db};
use crate::BoxError;

/// Table kinds of the GA4 export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Events,
    PseudonymousUsers,
    Users,
}

impl Kind {
    pub fn prefix(self) -> &'static str {
        match self {
            Kind::Events => "events_",
            Kind::PseudonymousUsers => "pseudonymous_users_",
            Kind::Users => "users_",
        }
    }
}

/// Collections whose indexes were ensured by this process.
#[derive(Clone, Default)]
pub struct Ensured(Arc<Mutex<HashSet<String>>>);

/// `analytics_<app_id>`: the app id with `.` and `:` replaced, so it stays one name segment.
pub fn dataset(app_id: &str) -> String {
    let safe: String = app_id.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect();
    format!("analytics_{}", if safe.is_empty() { "default".into() } else { safe })
}

pub fn table(app_id: &str, kind: Kind, date: &str) -> String {
    format!("{}.{}{}", dataset(app_id), kind.prefix(), date)
}

/// `YYYYMMDD` of a µs timestamp in the property time zone.
pub fn event_date(micros: i64, tz: Tz) -> String {
    let utc = DateTime::<Utc>::from_timestamp_micros(micros).unwrap_or_else(Utc::now);
    utc.with_timezone(&tz).format("%Y%m%d").to_string()
}

/// Parses a table name (`events_20261109`) into its kind and date. None for anything else.
pub fn parse_table(name: &str) -> Option<(Kind, NaiveDate)> {
    for kind in [Kind::PseudonymousUsers, Kind::Users, Kind::Events] {
        if let Some(date) = name.strip_prefix(kind.prefix()) {
            if date.len() == 8 && date.chars().all(|c| c.is_ascii_digit()) {
                return NaiveDate::parse_from_str(date, "%Y%m%d").ok().map(|d| (kind, d));
            }
        }
    }
    None
}

/// Daily event tables covering `[from_ms, now]` in the property time zone (plus tomorrow, for
/// devices whose clock runs ahead), newest last.
pub fn event_tables_since(app_id: &str, from_ms: i64, tz: Tz) -> Vec<String> {
    let start = Utc.timestamp_millis_opt(from_ms).single().unwrap_or_else(Utc::now).with_timezone(&tz).date_naive();
    let end = Utc::now().with_timezone(&tz).date_naive() + Duration::days(1);
    let mut out = Vec::new();
    let mut day = start;
    while day <= end && out.len() < 400 {
        out.push(table(app_id, Kind::Events, &format!("{:04}{:02}{:02}", day.year(), day.month(), day.day())));
        day += Duration::days(1);
    }
    out
}

/// Every table of an app's dataset (full collection names), sorted.
pub async fn list(db: &Db, app_id: &str) -> Result<Vec<String>, BoxError> {
    let prefix = format!("{}.", dataset(app_id));
    let mut names: Vec<String> = db
        .database
        .list_collection_names()
        .filter(doc! { "name": { "$regex": format!("^{}", regex_escape(&prefix)) } })
        .await?;
    names.retain(|n| n.strip_prefix(&prefix).is_some_and(|t| parse_table(t).is_some()));
    names.sort();
    Ok(names)
}

fn regex_escape(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c.to_string() } else { format!("\\{c}") }).collect()
}

/// Creates the indexes of a table the first time this process writes to it.
pub async fn ensure(db: &Db, name: &str) -> Result<(), BoxError> {
    if db.analytics_tables.0.lock().map(|set| set.contains(name)).unwrap_or(false) {
        return Ok(());
    }
    let coll = db.coll(name);
    let short = name.rsplit_once('.').map_or(name, |(_, t)| t);
    if short.starts_with(Kind::Events.prefix()) {
        index(&coll, doc! { "event_name": 1, "event_timestamp": -1 }, "event_name_time", false).await?;
        index(&coll, doc! { "user_pseudo_id": 1, "event_timestamp": -1 }, "pseudo_time", false).await?;
        index(&coll, doc! { "user_id": 1 }, "user_id", false).await?;
        index(&coll, doc! { "event_timestamp": -1 }, "time", false).await?;
    } else if short.starts_with(Kind::PseudonymousUsers.prefix()) {
        index(&coll, doc! { "user_info.last_active_timestamp_micros": -1 }, "last_active", false).await?;
    }
    if let Ok(mut set) = db.analytics_tables.0.lock() {
        set.insert(name.to_string());
    }
    Ok(())
}

/// Drops daily tables older than `days` (0 = keep everything). Returns the dropped names.
pub async fn drop_expired(db: &Db, days: u32, tz: Tz) -> Result<Vec<String>, BoxError> {
    if days == 0 {
        return Ok(Vec::new());
    }
    let cutoff = Utc::now().with_timezone(&tz).date_naive() - Duration::days(i64::from(days));
    let names = db.database.list_collection_names().filter(doc! { "name": { "$regex": "^analytics_" } }).await?;
    let mut dropped = Vec::new();
    for name in names {
        let Some((_, short)) = name.split_once('.') else { continue };
        if let Some((_, date)) = parse_table(short) {
            if date < cutoff {
                db.coll(&name).drop().await?;
                if let Ok(mut set) = db.analytics_tables.0.lock() {
                    set.remove(&name);
                }
                dropped.push(name);
            }
        }
    }
    Ok(dropped)
}

/// Background job: retention check every 6 hours.
pub fn spawn_retention(db: Db, days: u32, tz: Tz) {
    if days == 0 {
        return;
    }
    tokio::spawn(async move {
        loop {
            match drop_expired(&db, days, tz).await {
                Ok(dropped) if !dropped.is_empty() => println!("[Analytics] retention: dropped {}", dropped.join(", ")),
                Ok(_) => {}
                Err(err) => eprintln!("[Analytics] retention check failed: {err}"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
        }
    });
}

/// Runs `stages` over the union of the existing tables in `names`: the first table is the
/// aggregation source, the others are `$unionWith`-ed, each pre-filtered with `first_match`.
pub async fn aggregate_union(db: &Db, names: &[String], first_match: Document, stages: Vec<Document>) -> Result<Vec<Document>, BoxError> {
    use futures_util::TryStreamExt;
    let existing: HashSet<String> = match names.first().and_then(|n| n.split_once('.')).map(|(d, _)| d.to_string()) {
        Some(dataset) => db
            .database
            .list_collection_names()
            .filter(doc! { "name": { "$regex": format!("^{}\\.", regex_escape(&dataset)) } })
            .await?
            .into_iter()
            .collect(),
        None => HashSet::new(),
    };
    let tables: Vec<&String> = names.iter().filter(|n| existing.contains(*n)).collect();
    let Some((first, rest)) = tables.split_first() else { return Ok(Vec::new()) };
    let mut pipeline = vec![doc! { "$match": first_match.clone() }];
    for name in rest {
        pipeline.push(doc! { "$unionWith": { "coll": name.as_str(), "pipeline": [ { "$match": first_match.clone() } ] } });
    }
    pipeline.extend(stages);
    Ok(db.coll(first).aggregate(pipeline).await?.try_collect().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: TBL-001
    #[test]
    fn names_and_dates() {
        assert_eq!(dataset("dmart-ready"), "analytics_dmart-ready");
        assert_eq!(dataset("com.dmart:ios"), "analytics_com_dmart_ios");
        assert_eq!(table("dmart-ready", Kind::Events, "20261109"), "analytics_dmart-ready.events_20261109");
        // 2026-11-08T20:00:00Z is already the 9th in India (UTC+05:30).
        let micros = 1_794_168_000_000_000; // 2026-11-08T20:00:00Z
        assert_eq!(event_date(micros, chrono_tz::Asia::Kolkata), "20261109");
        assert_eq!(event_date(micros, chrono_tz::UTC), "20261108");
        assert_eq!(parse_table("events_20261109").map(|t| t.0), Some(Kind::Events));
        assert_eq!(parse_table("pseudonymous_users_20261109").map(|t| t.0), Some(Kind::PseudonymousUsers));
        assert!(parse_table("events_x").is_none());
        assert!(parse_table("events_20261399").is_none());
    }
}
