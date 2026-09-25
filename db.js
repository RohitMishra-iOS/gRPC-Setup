/**
 * db.js
 * ─────────────────────────────────────────────────────────────────────────────
 * MongoDB persistence for incoming event-batch JSON payloads.
 *
 * Each top-level key of the incoming JSON object is stored as its own
 * document: { key, value, batch_id, received_at }.
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

// Flattens the top-level keys of `obj` into individual key/value documents
// and inserts them, tagged with the batch they arrived in.
async function saveKeyValues(obj, batchId) {
  const col = await connect();
  const received_at = new Date();

  const docs = Object.entries(obj || {}).map(([key, value]) => ({
    key,
    value,
    batch_id: batchId ?? null,
    received_at,
  }));

  if (docs.length === 0) return { insertedCount: 0 };
  return col.insertMany(docs);
}

async function close() {
  if (client) await client.close();
}

module.exports = { connect, saveKeyValues, close };
