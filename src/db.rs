//! db.rs
//! ─────────────────────────────────────────────────────────────────────────────
//! MongoDB persistence for every DM SDK module.
//!
//! The schema is created/updated on every startup (`ensure_schema`), idempotently: collections
//! are created if missing and indexes are (re)declared, so pointing the server at an empty
//! Atlas database is all it takes to "create the DB". `mongo/setup.js` does the same from
//! mongosh, for creating it ahead of the first deploy.
//!
//! Collections (database = MONGODB_DB_NAME, default "grpc_status"):
//!   event_data      analytics batches stored by server versions before 3.2 (read by
//!                   POST /admin/analytics/:app/migrate-legacy; nothing new is written here)
//!   analytics_<app>.events_YYYYMMDD / pseudonymous_users_YYYYMMDD / users_YYYYMMDD
//!                   analytics in the GA4 BigQuery export layout (dm/analytics, dm-specs TBL-*)
//!   analytics_users one profile per (app_id, installation_id): lifetime values for user_ltv,
//!                   first traffic source, last click
//!   crash_reports   one document per crash / non-fatal (_id = report id → idempotent uploads)
//!   crash_issues    reports grouped by signature (_id = issue id): counts, versions, status
//!   perf_records    one document per performance record (_id = record id)
//!   devices         push registrations (_id = registration token), topics, APNs token
//!   rc_templates    Remote Config template versions (one document per published version)
//!   symbol_files    dSYM index (_id = Mach-O UUID) → GridFS file in bucket "dsyms"
//!   push_messages   log of messages sent through the admin API
//!   dsyms.files / dsyms.chunks   GridFS storage for dSYM DWARF binaries

use mongodb::bson::{doc, DateTime, Document};
use mongodb::gridfs::GridFsBucket;
use mongodb::options::{GridFsBucketOptions, IndexOptions};
use mongodb::{Client, Collection, Database, IndexModel};

use crate::BoxError;

pub const EVENT_DATA: &str = "event_data";
pub const CRASH_REPORTS: &str = "crash_reports";
pub const CRASH_ISSUES: &str = "crash_issues";
pub const PERF_RECORDS: &str = "perf_records";
pub const DEVICES: &str = "devices";
pub const RC_TEMPLATES: &str = "rc_templates";
pub const RC_FETCHES: &str = "rc_fetches";
pub const ANALYTICS_BATCHES: &str = "analytics_batches";
pub const ANALYTICS_USERS: &str = "analytics_users";
pub const SYMBOL_FILES: &str = "symbol_files";
pub const PUSH_MESSAGES: &str = "push_messages";

#[derive(Clone)]
pub struct Db {
    pub database: Database,
    pub event_data: Collection<Document>,
    pub crash_reports: Collection<Document>,
    pub crash_issues: Collection<Document>,
    pub perf_records: Collection<Document>,
    pub devices: Collection<Document>,
    pub rc_templates: Collection<Document>,
    /// One document per Remote Config fetch (console statistics), expires after 7 days.
    pub rc_fetches: Collection<Document>,
    /// batch_id → received_at; the idempotency ledger for analytics batches (30-day TTL).
    pub analytics_batches: Collection<Document>,
    /// One document per (app_id, installation_id): first/last seen, user id, properties, session.
    pub analytics_users: Collection<Document>,
    pub symbol_files: Collection<Document>,
    pub push_messages: Collection<Document>,
    /// Analytics tables whose indexes this process has ensured (dm/analytics/tables.rs).
    pub analytics_tables: crate::dm::analytics::tables::Ensured,
}

impl Db {
    pub async fn connect(retention_days: u32, analytics_retention_days: u32) -> Result<Self, BoxError> {
        let uri = crate::env_nonempty("MONGODB_URI")
            .ok_or("MONGODB_URI is not set (check server/.env)")?;
        let db_name = crate::env_nonempty("MONGODB_DB_NAME").unwrap_or_else(|| "grpc_status".into());

        let client = Client::with_uri_str(&uri).await?;
        let db = Self::from_database(client.database(&db_name));
        // Also forces the (otherwise lazy) driver to actually reach the server.
        db.ensure_schema(retention_days, analytics_retention_days).await?;
        println!("✅  MongoDB connected   → db={db_name} (schema ensured)");
        Ok(db)
    }

    fn from_database(database: Database) -> Self {
        Self {
            event_data: database.collection(EVENT_DATA),
            crash_reports: database.collection(CRASH_REPORTS),
            crash_issues: database.collection(CRASH_ISSUES),
            perf_records: database.collection(PERF_RECORDS),
            devices: database.collection(DEVICES),
            rc_templates: database.collection(RC_TEMPLATES),
            rc_fetches: database.collection(RC_FETCHES),
            analytics_batches: database.collection(ANALYTICS_BATCHES),
            analytics_users: database.collection(ANALYTICS_USERS),
            symbol_files: database.collection(SYMBOL_FILES),
            push_messages: database.collection(PUSH_MESSAGES),
            analytics_tables: Default::default(),
            database,
        }
    }

    /// Tests: handles that never connect until used (the driver is lazy).
    #[cfg(test)]
    pub async fn lazy(uri: &str) -> Self {
        Self::from_database(Client::with_uri_str(uri).await.expect("uri").database("dm_test"))
    }

    /// Any collection by name (platform services keep their collection names in their modules).
    pub fn coll(&self, name: &str) -> Collection<Document> {
        self.database.collection(name)
    }

    pub fn dsym_bucket(&self) -> GridFsBucket {
        self.database
            .gridfs_bucket(GridFsBucketOptions::builder().bucket_name("dsyms".to_string()).build())
    }

    /// Creates missing collections and declares every index. Safe to run on every startup.
    pub async fn ensure_schema(&self, retention_days: u32, analytics_retention_days: u32) -> Result<(), BoxError> {
        let existing = self.database.list_collection_names().await?;
        for name in [
            EVENT_DATA, CRASH_REPORTS, CRASH_ISSUES, PERF_RECORDS, DEVICES, RC_TEMPLATES, RC_FETCHES, SYMBOL_FILES,
            PUSH_MESSAGES, ANALYTICS_BATCHES, ANALYTICS_USERS,
        ] {
            if !existing.iter().any(|e| e == name) {
                self.database.create_collection(name).await?;
                println!("    created collection {name}");
            }
        }

        // event_data (analytics) — batch_id index kept from 1.x.
        index(&self.event_data, doc! { "batch_id": 1 }, "batch_id_1", false).await?;
        index(&self.event_data, doc! { "app_id": 1, "received_at": -1 }, "app_received", false).await?;
        index(&self.event_data, doc! { "installation_id": 1 }, "installation", false).await?;
        index(&self.event_data, doc! { "events.name": 1, "received_at": -1 }, "event_name_received", false).await?;

        index(&self.crash_reports, doc! { "app_id": 1, "issue_id": 1, "timestamp": -1 }, "app_issue_time", false).await?;
        index(&self.crash_reports, doc! { "app_id": 1, "app_version": 1, "timestamp": -1 }, "app_version_time", false).await?;
        index(&self.crash_reports, doc! { "installation_id": 1, "timestamp": -1 }, "installation_time", false).await?;
        index(&self.crash_reports, doc! { "binary_images.uuid": 1, "symbolicated": 1 }, "image_uuid_symbolicated", false).await?;

        index(&self.crash_issues, doc! { "app_id": 1, "last_seen": -1 }, "app_last_seen", false).await?;
        index(&self.crash_issues, doc! { "app_id": 1, "count": -1 }, "app_count", false).await?;

        index(&self.perf_records, doc! { "app_id": 1, "kind": 1, "name": 1, "start_time": -1 }, "app_kind_name_time", false).await?;
        index(&self.perf_records, doc! { "app_id": 1, "app_version": 1, "kind": 1 }, "app_version_kind", false).await?;

        index(&self.devices, doc! { "app_id": 1, "installation_id": 1 }, "app_installation_unique", true).await?;
        index(&self.devices, doc! { "app_id": 1, "topics": 1, "active": 1 }, "app_topics", false).await?;
        index(&self.devices, doc! { "app_id": 1, "user_id": 1 }, "app_user", false).await?;
        index(&self.devices, doc! { "apns_token": 1 }, "apns_token", false).await?;

        index(&self.rc_templates, doc! { "app_id": 1, "version": -1 }, "app_version_unique", true).await?;
        index(&self.rc_fetches, doc! { "app_id": 1, "received_at": -1 }, "app_received", false).await?;
        ttl(&self.database, &self.rc_fetches, 7).await;
        ttl(&self.database, &self.analytics_batches, 30).await;
        index(&self.analytics_users, doc! { "app_id": 1, "installation_id": 1 }, "app_installation_unique", true).await?;
        index(&self.analytics_users, doc! { "app_id": 1, "last_seen": -1 }, "app_last_seen", false).await?;
        index(&self.analytics_users, doc! { "app_id": 1, "user_id": 1 }, "app_user", false).await?;
        index(&self.event_data, doc! { "debug": 1, "received_at": -1 }, "debug_received", false).await?;
        index(&self.symbol_files, doc! { "bundle_id": 1, "app_version": 1 }, "bundle_version", false).await?;
        index(&self.push_messages, doc! { "app_id": 1, "created_at": -1 }, "app_created", false).await?;

        ttl(&self.database, &self.crash_reports, retention_days).await;
        ttl(&self.database, &self.perf_records, retention_days).await;
        ttl(&self.database, &self.push_messages, retention_days).await;
        // Analytics retention drops whole daily tables instead (dm/analytics/tables.rs); the old
        // batch collection keeps its TTL setting.
        ttl(&self.database, &self.event_data, analytics_retention_days).await;
        crate::platform::ensure_schema(self).await?;
        Ok(())
    }

    /// Records an analytics batch id. Ok(false) = already stored (a retry, WIRE-005).
    pub async fn claim_batch_id(&self, app_id: &str, batch_id: &str) -> Result<bool, BoxError> {
        let ledger = doc! { "_id": batch_id, "app_id": app_id, "received_at": DateTime::now() };
        match self.analytics_batches.insert_one(ledger).await {
            Ok(_) => Ok(true),
            Err(err) if crate::db::is_duplicate_key(&err) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Undo `claim_batch_id` after a failed save, so the client's retry is stored.
    pub async fn release_batch_id(&self, batch_id: &str) {
        let _ = self.analytics_batches.delete_one(doc! { "_id": batch_id }).await;
    }
}

pub async fn index(coll: &Collection<Document>, keys: Document, name: &str, unique: bool) -> Result<(), BoxError> {
    let options = IndexOptions::builder().name(name.to_string()).unique(unique.then_some(true)).build();
    match coll.create_index(IndexModel::builder().keys(keys).options(options).build()).await {
        Ok(_) => Ok(()),
        // 85/86: an index with this name/keys exists with different options — keep the existing one.
        Err(err) if matches!(error_code(&err), Some(85) | Some(86)) => {
            eprintln!("⚠️  index {name} on {} exists with different options: {err}", coll.name());
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

/// TTL on `received_at`. days == 0 removes it. Retention changes are applied with collMod, which
/// needs the dbAdmin role — without it a warning is logged and the old TTL stays.
async fn ttl(database: &Database, coll: &Collection<Document>, days: u32) {
    const NAME: &str = "ttl_received_at";
    if days == 0 {
        let _ = coll.drop_index(NAME).await;
        return;
    }
    let seconds = std::time::Duration::from_secs(u64::from(days) * 86_400);
    let options = IndexOptions::builder().name(NAME.to_string()).expire_after(seconds).build();
    let model = IndexModel::builder().keys(doc! { "received_at": 1 }).options(options).build();
    match coll.create_index(model).await {
        Ok(_) => {}
        Err(err) if matches!(error_code(&err), Some(85) | Some(86)) => {
            let command = doc! {
                "collMod": coll.name(),
                "index": { "name": NAME, "expireAfterSeconds": seconds.as_secs() as i64 },
            };
            if let Err(err) = database.run_command(command).await {
                eprintln!("⚠️  could not change TTL on {} to {days} days: {err}", coll.name());
            }
        }
        Err(err) => eprintln!("⚠️  could not create TTL index on {}: {err}", coll.name()),
    }
}

pub fn error_code(err: &mongodb::error::Error) -> Option<i32> {
    use mongodb::error::ErrorKind;
    match err.kind.as_ref() {
        ErrorKind::Command(command) => Some(command.code),
        ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write)) => Some(write.code),
        ErrorKind::InsertMany(insert) => insert.write_errors.as_ref().and_then(|e| e.first()).map(|e| e.code),
        _ => None,
    }
}

pub fn is_duplicate_key(err: &mongodb::error::Error) -> bool {
    error_code(err) == Some(11000)
}

/// u64 addresses don't fit BSON's signed int64 losslessly for display; store as "0x…" strings.
pub fn hex_addr(address: u64) -> String {
    format!("0x{address:x}")
}

pub fn ms_to_datetime(ms: i64) -> DateTime {
    DateTime::from_millis(ms)
}
