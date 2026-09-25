/**
 * db.js
 * ─────────────────────────────────────────────────────────────────────────────
 * MongoDB persistence for incoming event-batch JSON payloads.
 *
 * Each incoming JSON object (app, device, screen_name, events, batch_id,
 * instance_id, user_properties, ...) is stored as a single document, as-is,
 * plus a received_at timestamp.
 */

'use strict';

const { MongoClient } = require('mongodb');

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME      = process.env.MONGODB_DB_NAME || 'grpc_status';
const COLLECTION   = 'event_data';

let client;
let collection;

async function connect() {
  if (collection) return collection;
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI is not set (check server/.env)');
  }

  client = new MongoClient(MONGODB_URI);
  await client.connect();
  collection = client.db(DB_NAME).collection(COLLECTION);
  await collection.createIndex({ batch_id: 1 });

  console.log(`✅  MongoDB connected   → db=${DB_NAME} collection=${COLLECTION}`);
  return collection;
}

// Inserts the whole incoming JSON object as a single document.
async function saveEventBatch(obj) {
  const col = await connect();

  const doc = {
    ...obj,
    received_at: new Date(),
  };

  return col.insertOne(doc);
}

async function close() {
  if (client) await client.close();
}

module.exports = { connect, saveEventBatch, close };
