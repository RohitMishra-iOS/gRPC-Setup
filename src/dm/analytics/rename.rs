//! One-time migration of stored rows to the DM_ parameter names (dm-specs ANA-015).
//!
//! Rows written before the rename hold `firebase_screen`, `firebase_event_origin` … in
//! `event_params` and `app_info.firebase_app_id`. This renames them in every daily events table
//! (`analytics_<app>.events_YYYYMMDD`) in place. It runs in the background at start-up until it
//! has finished once (marker in `dm_migrations`), and on demand from
//! `POST /admin/analytics/migrate-dm-keys`. Running it again changes nothing.

use axum::extract::State;
use axum::response::Response;
use mongodb::bson::{doc, Bson, DateTime, Document};
use serde_json::json;

use super::reference::RENAMED_PARAMS;
use super::tables::{parse_table, Kind};
use crate::db::Db;
use crate::dm::admin::{db_error, ok};
use crate::dm::AppState;
use crate::BoxError;

const MARKER: &str = "rename_firebase_keys_v1";
const MIGRATIONS: &str = "dm_migrations";

#[derive(Default, Debug)]
pub struct Summary {
    pub tables: u64,
    pub rows: u64,
}

/// `{"$switch"}` that maps one parameter key to its current name.
fn key_expression() -> Bson {
    let branches: Vec<Bson> = RENAMED_PARAMS
        .iter()
        .map(|(old, new)| Bson::Document(doc! { "case": { "$eq": ["$$p.key", *old] }, "then": *new }))
        .collect();
    Bson::Document(doc! { "$switch": { "branches": branches, "default": "$$p.key" } })
}

/// Renames the keys in every events table of every app. Returns how many rows changed.
pub async fn run(db: &Db) -> Result<Summary, BoxError> {
    let old: Vec<&str> = RENAMED_PARAMS.iter().map(|(o, _)| *o).collect();
    let names = db.database.list_collection_names().filter(doc! { "name": { "$regex": "^analytics_" } }).await?;
    let mut summary = Summary::default();
    for name in names {
        let Some((_, short)) = name.split_once('.') else { continue };
        if !matches!(parse_table(short), Some((Kind::Events, _))) {
            continue;
        }
        let coll = db.coll(&name);
        let params = coll
            .update_many(
                doc! { "event_params.key": { "$in": &old } },
                vec![doc! { "$set": { "event_params": { "$map": {
                    "input": "$event_params", "as": "p",
                    "in": { "$mergeObjects": ["$$p", { "key": key_expression() }] },
                }}}}],
            )
            .await?;
        let app = coll
            .update_many(doc! { "app_info.firebase_app_id": { "$exists": true } }, doc! { "$rename": { "app_info.firebase_app_id": "app_info.DM_app_id" } })
            .await?;
        let changed = params.modified_count.max(app.modified_count);
        if changed > 0 {
            summary.tables += 1;
            summary.rows += changed;
        }
    }
    Ok(summary)
}

/// Start-up: runs once in the background (the server answers meanwhile; rows still holding the
/// old names are read correctly only after it finishes, which is seconds for most databases).
pub fn spawn_once(db: Db) {
    tokio::spawn(async move {
        let markers = db.coll(MIGRATIONS);
        match markers.find_one(doc! { "_id": MARKER }).await {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(err) => return eprintln!("[Analytics] DM_ key migration: {err}"),
        }
        match run(&db).await {
            Ok(summary) => {
                if summary.rows > 0 {
                    println!("[Analytics] renamed firebase_* keys to DM_* in {} rows of {} tables", summary.rows, summary.tables);
                }
                let _ = markers.insert_one(doc! { "_id": MARKER, "done_at": DateTime::now(), "rows": summary.rows as i64 }).await;
            }
            Err(err) => eprintln!("[Analytics] DM_ key migration failed (retried at next start): {err}"),
        }
    });
}

/// POST /admin/analytics/migrate-dm-keys — runs the rename now (idempotent).
pub async fn migrate_now(State(state): State<AppState>) -> Result<Response, Response> {
    let summary = run(&state.db).await.map_err(db_error)?;
    ok(json!({ "tables": summary.tables, "rows": summary.rows }))
}
