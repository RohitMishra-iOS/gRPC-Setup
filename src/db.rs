//! db.rs
//! ─────────────────────────────────────────────────────────────────────────────
//! MongoDB persistence for incoming event-batch JSON payloads.
//!
//! Each incoming JSON object (app, device, screen_name, events, batch_id,
//! instance_id, user_properties, ...) is stored as a single document, as-is,
//! plus a received_at timestamp.

use mongodb::bson::{self, doc, DateTime, Document};
use mongodb::{Client, Collection, IndexModel};
use serde_json::Value as Json;

use crate::BoxError;

const COLLECTION: &str = "event_data";

#[derive(Clone)]
pub struct Db {
    collection: Collection<Document>,
}

impl Db {
    pub async fn connect() -> Result<Self, BoxError> {
        let uri = crate::env_nonempty("MONGODB_URI")
            .ok_or("MONGODB_URI is not set (check server/.env)")?;
        let db_name = crate::env_nonempty("MONGODB_DB_NAME").unwrap_or_else(|| "grpc_status".into());

        let client = Client::with_uri_str(&uri).await?;
        let collection = client.database(&db_name).collection::<Document>(COLLECTION);
        // Also forces the (otherwise lazy) driver to actually reach the server.
        collection
            .create_index(IndexModel::builder().keys(doc! { "batch_id": 1 }).build())
            .await?;

        println!("✅  MongoDB connected   → db={db_name} collection={COLLECTION}");
        Ok(Self { collection })
    }

    /// Inserts the whole incoming JSON object as a single document.
    pub async fn save_event_batch(&self, obj: &Json) -> Result<(), BoxError> {
        let mut doc = bson::to_document(obj)?;
        doc.insert("received_at", DateTime::now());
        self.collection.insert_one(doc).await?;
        Ok(())
    }
}
