// mongo/setup.js — creates the DM SDK database on MongoDB (Atlas or self-hosted).
//
//   MONGODB_DB_NAME=grpc_status mongosh "mongodb+srv://<user>:<pass>@cluster0.xxxxx.mongodb.net/" mongo/setup.js
//
// Optional environment variables:
//   MONGODB_DB_NAME          database name (default grpc_status — same default as the server)
//   DM_RETENTION_DAYS        TTL for crash/performance/push documents (default 90, 0 = keep forever)
//   SEED_SAMPLE_TEMPLATE=1   also publish a sample Remote Config template (v1) for DM_APP_ID
//   DM_APP_ID                app id for the sample template (default dmart-ready-ios)
//
// Idempotent: safe to re-run. The server performs the same collection/index setup on every start
// (src/db.rs), with identical index names, so the two never conflict. This script additionally
// attaches $jsonSchema validators in "warn" mode: they document the schema inside the database
// and log mismatches, but never reject a write.

const dbName = process.env.MONGODB_DB_NAME || "grpc_status";
const retentionDays = parseInt(process.env.DM_RETENTION_DAYS || "90", 10);
const target = db.getSiblingDB(dbName);
print(`Setting up database "${dbName}" (retention ${retentionDays} days)`);

const str = { bsonType: "string" };
const strOrNull = { bsonType: ["string", "null"] };
const date = { bsonType: "date" };
const bool = { bsonType: "bool" };
const long = { bsonType: ["long", "int"] };

const collections = {
  event_data: {
    description: "Analytics batches (status.StatusService/CheckStatus and POST /events), one document per batch",
    required: ["events", "received_at"],
    properties: { events: { bsonType: "array" }, batch_id: strOrNull, installation_id: strOrNull, app_id: strOrNull, received_at: date },
  },
  crash_reports: {
    description: "One document per crash / non-fatal / MetricKit diagnostic. _id = report id (idempotent uploads)",
    required: ["_id", "app_id", "installation_id", "kind", "is_fatal", "timestamp", "issue_id", "received_at"],
    properties: {
      _id: str, app_id: str, installation_id: str, user_id: strOrNull,
      kind: { enum: ["signal", "exception", "non_fatal_error", "custom_exception", "metric_kit_diagnostic", "unspecified"] },
      is_fatal: bool, timestamp: date, launch_id: str, app_version: str, app_build: str, os_version: str,
      device_model: str, bundle_id: str, issue_id: str, custom_keys: { bsonType: "object" },
      logs: { bsonType: "array" }, breadcrumbs: { bsonType: "array" }, return_addresses: { bsonType: "array" },
      binary_images: { bsonType: "array" }, symbolicated: bool, symbolicated_frames: { bsonType: "array" },
      received_at: date,
    },
  },
  crash_issues: {
    description: "Crash reports grouped by signature. _id = issue id. status: open | closed | muted | regressed",
    required: ["_id", "app_id", "title", "status", "count", "first_seen", "last_seen"],
    properties: {
      _id: str, app_id: str, title: str, subtitle: str, kind: str, is_fatal: bool, signature: str,
      status: { enum: ["open", "closed", "muted", "regressed"] }, count: long, first_seen: date, last_seen: date,
      versions: { bsonType: "array" }, first_version: str, last_version: str,
    },
  },
  perf_records: {
    description: "Performance records (traces, network, screen rendering, app start, MetricKit). _id = record id",
    required: ["_id", "app_id", "installation_id", "kind", "name", "start_time", "duration_us", "received_at"],
    properties: {
      _id: str, app_id: str, installation_id: str,
      kind: { enum: ["trace", "network_request", "screen_rendering", "app_start", "app_foreground", "app_background", "metric_kit", "unspecified"] },
      name: str, start_time: date, duration_us: long, duration_ms: { bsonType: ["double", "int", "long"] },
      counters: { bsonType: "object" }, attributes: { bsonType: "object" }, app_version: str, received_at: date,
    },
  },
  devices: {
    description: "Push registrations. _id = registration token; unique per (app_id, installation_id)",
    required: ["_id", "app_id", "installation_id", "apns_token", "apns_environment", "active"],
    properties: {
      _id: str, app_id: str, installation_id: str, user_id: strOrNull, apns_token: str,
      apns_environment: { enum: ["sandbox", "production"] }, notification_authorization: str,
      topics: { bsonType: "array", items: str }, active: bool, invalid_reason: strOrNull,
      created_at: date, updated_at: date, last_seen_at: date,
    },
  },
  rc_templates: {
    description: "Remote Config template versions (immutable). The newest version per app_id is served",
    required: ["app_id", "version", "template", "created_at"],
    properties: { app_id: str, version: long, template: { bsonType: "object" }, description: strOrNull, published_by: str, created_at: date },
  },
  rc_fetches: {
    description: "One document per Remote Config fetch, for the console's Fetches chart (7-day TTL)",
    required: ["app_id", "installation_id", "received_at"],
    properties: { app_id: str, installation_id: str, received_at: date },
  },
  symbol_files: {
    description: "dSYM index. _id = Mach-O UUID; the DWARF binary lives in GridFS bucket 'dsyms'",
    required: ["_id", "gridfs_id", "text_vmaddr", "uploaded_at"],
    properties: { _id: str, name: str, arch: str, text_vmaddr: long, size: long, bundle_id: str, app_version: str, app_build: str, uploaded_at: date },
  },
  push_messages: {
    description: "Log of pushes sent through POST /admin/messaging/send",
    required: ["_id", "app_id", "created_at"],
    properties: { _id: str, app_id: str, target: str, silent: bool, sent: long, failed: long, created_at: date },
  },
};

const existing = target.getCollectionNames();
for (const [name, spec] of Object.entries(collections)) {
  const validator = {
    $jsonSchema: { bsonType: "object", description: spec.description, required: spec.required, properties: spec.properties },
  };
  if (!existing.includes(name)) {
    target.createCollection(name, { validator, validationLevel: "moderate", validationAction: "warn" });
    print(`  created ${name}`);
  } else {
    try {
      target.runCommand({ collMod: name, validator, validationLevel: "moderate", validationAction: "warn" });
      print(`  updated validator on ${name}`);
    } catch (e) {
      print(`  ${name} exists (validator not updated: ${e.codeName || e.message})`);
    }
  }
}

function index(collection, keys, name, unique) {
  const options = { name };
  if (unique) options.unique = true;
  try {
    target.getCollection(collection).createIndex(keys, options);
  } catch (e) {
    print(`  ! ${collection}.${name}: ${e.codeName || e.message}`);
  }
}

index("event_data", { batch_id: 1 }, "batch_id_1");
index("event_data", { app_id: 1, received_at: -1 }, "app_received");
index("event_data", { installation_id: 1 }, "installation");
index("event_data", { "events.name": 1, received_at: -1 }, "event_name_received");

index("crash_reports", { app_id: 1, issue_id: 1, timestamp: -1 }, "app_issue_time");
index("crash_reports", { app_id: 1, app_version: 1, timestamp: -1 }, "app_version_time");
index("crash_reports", { installation_id: 1, timestamp: -1 }, "installation_time");
index("crash_reports", { "binary_images.uuid": 1, symbolicated: 1 }, "image_uuid_symbolicated");

index("crash_issues", { app_id: 1, last_seen: -1 }, "app_last_seen");
index("crash_issues", { app_id: 1, count: -1 }, "app_count");

index("perf_records", { app_id: 1, kind: 1, name: 1, start_time: -1 }, "app_kind_name_time");
index("perf_records", { app_id: 1, app_version: 1, kind: 1 }, "app_version_kind");

index("devices", { app_id: 1, installation_id: 1 }, "app_installation_unique", true);
index("devices", { app_id: 1, topics: 1, active: 1 }, "app_topics");
index("devices", { app_id: 1, user_id: 1 }, "app_user");
index("devices", { apns_token: 1 }, "apns_token");

index("rc_templates", { app_id: 1, version: -1 }, "app_version_unique", true);
index("rc_fetches", { app_id: 1, received_at: -1 }, "app_received");
try {
  target.rc_fetches.createIndex({ received_at: 1 }, { name: "ttl_received_at", expireAfterSeconds: 7 * 86400 });
} catch (e) { print(`  ! rc_fetches TTL: ${e.codeName || e.message}`); }
index("symbol_files", { bundle_id: 1, app_version: 1 }, "bundle_version");
index("push_messages", { app_id: 1, created_at: -1 }, "app_created");

// TTL (raw data expiry). Analytics (event_data) is left without TTL, as in 1.x.
for (const name of ["crash_reports", "perf_records", "push_messages"]) {
  const coll = target.getCollection(name);
  if (retentionDays <= 0) {
    try { coll.dropIndex("ttl_received_at"); } catch (e) { /* none */ }
    continue;
  }
  const seconds = retentionDays * 86400;
  try {
    coll.createIndex({ received_at: 1 }, { name: "ttl_received_at", expireAfterSeconds: seconds });
  } catch (e) {
    target.runCommand({ collMod: name, index: { name: "ttl_received_at", expireAfterSeconds: seconds } });
  }
}

// GridFS bucket for dSYMs — the driver creates these indexes on first upload; created here too
// so the bucket shows up immediately.
if (!target.getCollectionNames().includes("dsyms.files")) target.createCollection("dsyms.files");
if (!target.getCollectionNames().includes("dsyms.chunks")) target.createCollection("dsyms.chunks");
target.getCollection("dsyms.files").createIndex({ filename: 1, uploadDate: 1 });
target.getCollection("dsyms.chunks").createIndex({ files_id: 1, n: 1 }, { unique: true });

if (process.env.SEED_SAMPLE_TEMPLATE === "1") {
  const appId = process.env.DM_APP_ID || "dmart-ready-ios";
  if (target.rc_templates.countDocuments({ app_id: appId }) === 0) {
    target.rc_templates.insertOne({
      app_id: appId,
      version: NumberLong(1),
      description: "Sample template created by mongo/setup.js",
      published_by: "setup.js",
      created_at: new Date(),
      template: {
        description: "Sample template created by mongo/setup.js",
        conditions: [
          { name: "india", expression: { country: { in: ["IN"] } } },
          { name: "beta_10_percent", expression: { percent: { seed: "beta", lt: 10 } } },
        ],
        parameters: {
          show_diwali_banner: { default_value: "false", conditional_values: [{ condition: "india", value: "true" }] },
          express_slot_minutes: { default_value: "30" },
          new_checkout_enabled: { default_value: "false", conditional_values: [{ condition: "beta_10_percent", value: "true" }] },
        },
        experiments: [],
      },
    });
    print(`  seeded sample Remote Config template v1 for ${appId}`);
  } else {
    print(`  ${appId} already has a template — sample not seeded`);
  }
}

print("Collections:");
for (const name of target.getCollectionNames().sort()) {
  print(`  ${name.padEnd(16)} ${target.getCollection(name).getIndexes().map((i) => i.name).join(", ")}`);
}
print("Done.");
