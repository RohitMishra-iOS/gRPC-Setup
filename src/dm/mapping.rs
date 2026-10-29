//! Android (R8/ProGuard) mapping files + retrace, the Android counterpart of dSYMs.
//!
//! Upload (POST /v1/symbols, multipart): field `mapping` = build/outputs/mapping/release/mapping.txt
//! plus `app_version` (versionName) and `app_build` (versionCode); the app id comes from
//! x-dm-app-id. Stored in GridFS bucket "mappings", indexed in `mapping_files` by
//! "<app_id>|<version>|<build>".
//!
//! Retrace: Android crash reports carry pre-symbolicated JVM frames ("a.b.c" in "SourceFile":12).
//! With a mapping for the report's version/build each frame becomes the original class/method/
//! line (expanding inlined methods), and obfuscated exception class names are restored. Reports
//! that arrived before their mapping are retraced in the background on upload.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use futures_util::io::AsyncWriteExt;
use futures_util::{AsyncReadExt, TryStreamExt};
use mongodb::bson::{doc, Bson, DateTime, Document};
use mongodb::options::GridFsBucketOptions;

use super::symbols::SymFrame;
use super::AppState;

const COLLECTION: &str = "mapping_files";

// ── Parsing ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Member {
    obf_start: Option<u32>,
    obf_end: Option<u32>,
    /// Original method name; "pkg.Class.method" when inlined from another class.
    original: String,
    orig_start: Option<u32>,
    orig_end: Option<u32>,
}

#[derive(Debug, Default)]
struct ClassMap {
    original: String,
    file: Option<String>,
    methods: HashMap<String, Vec<Member>>,
}

#[derive(Debug, Default)]
pub struct Mapping {
    classes: HashMap<String, ClassMap>,
    /// Original class name → source file (for frames inlined from other classes).
    files: HashMap<String, String>,
}

/// One retraced frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Retraced {
    pub class: String,
    pub method: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

impl Mapping {
    pub fn parse(text: &str) -> Mapping {
        let mut mapping = Mapping::default();
        let mut current: Option<String> = None;
        for raw in text.lines() {
            let line = raw.trim_end();
            if line.is_empty() {
                continue;
            }
            let indented = line.starts_with(' ') || line.starts_with('\t');
            let trimmed = line.trim();
            if trimmed.starts_with('#') {
                // # {"id":"sourceFile","fileName":"Checkout.kt"} after a class line
                if let (Some(obf), Some(start)) = (&current, trimmed.find("\"fileName\":\"")) {
                    let rest = &trimmed[start + 12..];
                    if let Some(end) = rest.find('"') {
                        if let Some(class) = mapping.classes.get_mut(obf) {
                            class.file = Some(rest[..end].to_string());
                        }
                    }
                }
                continue;
            }
            if !indented {
                // original.Class -> obf.Class:
                let Some((original, obf)) = trimmed.trim_end_matches(':').split_once(" -> ") else { continue };
                let obf = obf.trim().to_string();
                mapping.classes.insert(obf.clone(), ClassMap { original: original.trim().to_string(), ..Default::default() });
                current = Some(obf);
                continue;
            }
            let Some(obf_class) = &current else { continue };
            let Some((left, obf_name)) = trimmed.split_once(" -> ") else { continue };
            if !left.contains('(') {
                continue; // field
            }
            let member = parse_member(left);
            if let (Some(member), Some(class)) = (member, mapping.classes.get_mut(obf_class)) {
                class.methods.entry(obf_name.trim().to_string()).or_default().push(member);
            }
        }
        mapping.files = mapping.classes.values().filter_map(|c| c.file.clone().map(|f| (c.original.clone(), f))).collect();
        mapping
    }

    /// Original name of an (obfuscated) class, if mapped.
    pub fn class(&self, obf: &str) -> Option<&str> {
        self.classes.get(obf).map(|c| c.original.as_str())
    }

    /// Retraces "obf.Class.method" at `line` (0 = unknown) → frames innermost first, or None if
    /// the class isn't in the mapping.
    pub fn retrace(&self, symbol: &str, line: u32) -> Option<Vec<Retraced>> {
        let (obf_class, obf_method) = symbol.rsplit_once('.')?;
        let class = self.classes.get(obf_class)?;
        let candidates = class.methods.get(obf_method);
        let file_for = |cls: &str| -> Option<String> { self.files.get(cls).cloned() };
        let Some(candidates) = candidates else {
            // Class renamed but method kept (or not in the mapping).
            return Some(vec![Retraced { class: class.original.clone(), method: obf_method.to_string(), file: class.file.clone(), line: (line > 0).then_some(line) }]);
        };
        // Members whose obfuscated range contains the line (an inline chain is consecutive
        // entries with the same range, innermost first); else members without ranges.
        let mut chosen: Vec<&Member> = Vec::new();
        if line > 0 {
            chosen = candidates.iter().filter(|m| matches!((m.obf_start, m.obf_end), (Some(s), Some(e)) if s <= line && line <= e)).collect();
        }
        if chosen.is_empty() {
            chosen = candidates.iter().filter(|m| m.obf_start.is_none()).collect();
        }
        if chosen.is_empty() {
            // Ambiguous: keep the first member's name.
            chosen = candidates.iter().take(1).collect();
        }
        let frames = chosen
            .iter()
            .map(|m| {
                let (cls, method) = match m.original.rsplit_once('.') {
                    Some((c, meth)) => (c.to_string(), meth.to_string()),
                    None => (class.original.clone(), m.original.clone()),
                };
                let original_line = match (m.obf_start, m.obf_end, m.orig_start, m.orig_end) {
                    (Some(s), Some(e), Some(os), Some(oe)) if line > 0 && oe >= os && (oe - os) == (e - s) => Some(os + (line - s)),
                    (_, _, Some(os), _) => Some(os),
                    _ => (line > 0).then_some(line),
                };
                let file = file_for(&cls);
                Retraced { class: cls, method, file, line: original_line }
            })
            .collect();
        Some(frames)
    }
}

/// "12:15:void foo(int):40:43" / "void foo()" / "12:12:void a.B.foo():40" → Member.
fn parse_member(left: &str) -> Option<Member> {
    let mut obf_start = None;
    let mut obf_end = None;
    let mut rest = left;
    // Leading obfuscated line range.
    let parts: Vec<&str> = left.splitn(3, ':').collect();
    if parts.len() == 3 && parts[0].chars().all(|c| c.is_ascii_digit()) && parts[1].chars().all(|c| c.is_ascii_digit()) && !parts[0].is_empty() {
        obf_start = parts[0].parse().ok();
        obf_end = parts[1].parse().ok();
        rest = parts[2];
    }
    // "returnType name(args)" + optional ":origStart[:origEnd]"
    let close = rest.find(')')?;
    let signature = &rest[..=close];
    let after = &rest[close + 1..];
    let mut orig_start = None;
    let mut orig_end = None;
    let mut numbers = after.split(':').filter(|s| !s.is_empty());
    if let Some(n) = numbers.next() {
        orig_start = n.parse().ok();
    }
    if let Some(n) = numbers.next() {
        orig_end = n.parse().ok();
    }
    if orig_end.is_none() {
        orig_end = orig_start;
    }
    let before_paren = &signature[..signature.find('(')?];
    let name = before_paren.rsplit(' ').next()?.to_string();
    Some(Member { obf_start, obf_end, original: name, orig_start, orig_end })
}

// ── Storage ──────────────────────────────────────────────────────────────────

fn bucket(state: &AppState) -> mongodb::gridfs::GridFsBucket {
    state.db.database.gridfs_bucket(GridFsBucketOptions::builder().bucket_name("mappings".to_string()).build())
}

pub fn key(app_id: &str, version: &str, build: &str) -> String {
    format!("{app_id}|{version}|{build}")
}

/// Stores a mapping file (replacing a previous one for the same version/build) and retraces
/// stored reports of that build in the background.
pub async fn store(state: &AppState, app_id: &str, version: &str, build: &str, text: Vec<u8>) -> Result<serde_json::Value, crate::BoxError> {
    let id = key(app_id, version, build);
    let mut upload = bucket(state).open_upload_stream(&id).await?;
    upload.write_all(&text).await?;
    upload.close().await?;
    let gridfs_id = upload.id().clone();
    let coll = state.db.coll(COLLECTION);
    let previous = coll.find_one(doc! { "_id": &id }).await?;
    coll.replace_one(
        doc! { "_id": &id },
        doc! { "_id": &id, "app_id": app_id, "app_version": version, "app_build": build, "size": text.len() as i64, "gridfs_id": gridfs_id, "uploaded_at": DateTime::now() },
    )
    .upsert(true)
    .await?;
    if let Some(old) = previous.and_then(|p| p.get("gridfs_id").cloned()) {
        let _ = bucket(state).delete(old).await;
    }
    cache().lock().unwrap().remove(&id);
    let classes = Mapping::parse(&String::from_utf8_lossy(&text)).classes.len();
    let background = state.clone();
    let (a, v, b) = (app_id.to_string(), version.to_string(), build.to_string());
    tokio::spawn(async move { retrace_stored(&background, &a, &v, &b).await });
    Ok(serde_json::json!({ "mapping": id, "classes": classes, "bytes": text.len() }))
}

type Cache = Mutex<HashMap<String, (Instant, Option<Arc<Mapping>>)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The parsed mapping for a build (cached; misses are cached for a minute).
pub async fn load(state: &AppState, app_id: &str, version: &str, build: &str) -> Option<Arc<Mapping>> {
    let id = key(app_id, version, build);
    if let Some((at, mapping)) = cache().lock().unwrap().get(&id).cloned() {
        if mapping.is_some() || at.elapsed().as_secs() < 60 {
            return mapping;
        }
    }
    let record = state.db.coll(COLLECTION).find_one(doc! { "_id": &id }).await.ok().flatten();
    let mapping = match record.and_then(|r| r.get("gridfs_id").cloned()) {
        Some(gridfs_id) => {
            let mut bytes = Vec::new();
            match bucket(state).open_download_stream(gridfs_id).await {
                Ok(mut stream) => {
                    let _ = stream.read_to_end(&mut bytes).await;
                    let text = String::from_utf8_lossy(&bytes).to_string();
                    tokio::task::spawn_blocking(move || Mapping::parse(&text)).await.ok().map(Arc::new)
                }
                Err(_) => None,
            }
        }
        None => None,
    };
    let mut c = cache().lock().unwrap();
    if c.len() >= 8 {
        // Keep memory bounded: drop the oldest entry.
        if let Some(oldest) = c.iter().min_by_key(|(_, (at, _))| *at).map(|(k, _)| k.clone()) {
            c.remove(&oldest);
        }
    }
    c.insert(id, (Instant::now(), mapping.clone()));
    mapping
}

// ── Frames ───────────────────────────────────────────────────────────────────

/// A pre-symbolicated frame from a client (Android / web / custom).
pub struct ClientFrame<'a> {
    pub symbol: &'a str,
    pub file: &'a str,
    pub line: i64,
    pub library: &'a str,
}

/// Client frames → SymFrames (retraced with `mapping` when given). Marker frames
/// ("Caused by: …") are kept as-is.
pub fn sym_frames(frames: &[ClientFrame], mapping: Option<&Mapping>) -> (Vec<SymFrame>, bool) {
    let mut out = Vec::new();
    let mut retraced_any = false;
    for (index, f) in frames.iter().enumerate() {
        let line = u32::try_from(f.line.max(0)).unwrap_or(0);
        let retraced = if f.symbol.starts_with("Caused by:") { None } else { mapping.and_then(|m| m.retrace(f.symbol, line)) };
        match retraced {
            Some(chain) if !chain.is_empty() => {
                retraced_any = true;
                let inner = &chain[0];
                out.push(SymFrame {
                    index,
                    address: String::new(),
                    image: (!f.library.is_empty()).then(|| f.library.to_string()),
                    image_offset: None,
                    in_app: true,
                    symbol: Some(format!("{}.{}", inner.class, inner.method)),
                    file: inner.file.clone().or_else(|| (!f.file.is_empty()).then(|| f.file.to_string())),
                    line: inner.line,
                    inlined_into: chain[1..].iter().map(|r| format!("{}.{}{}", r.class, r.method, r.line.map(|l| format!(":{l}")).unwrap_or_default())).collect(),
                });
            }
            _ => out.push(SymFrame {
                index,
                address: String::new(),
                image: (!f.library.is_empty()).then(|| f.library.to_string()),
                image_offset: None,
                in_app: f.library == "app",
                symbol: (!f.symbol.is_empty()).then(|| f.symbol.to_string()),
                file: (!f.file.is_empty()).then(|| f.file.to_string()),
                line: (line > 0).then_some(line),
                inlined_into: Vec::new(),
            }),
        }
    }
    (out, retraced_any)
}

/// Restores an obfuscated exception class name ("a.b.c" → "com.app.PaymentException").
pub fn class_name(mapping: Option<&Mapping>, name: &str) -> Option<String> {
    mapping.and_then(|m| m.class(name)).map(str::to_string)
}

/// Retraces stored reports of a build that arrived before its mapping.
async fn retrace_stored(state: &AppState, app_id: &str, version: &str, build: &str) {
    let Some(mapping) = load(state, app_id, version, build).await else { return };
    let filter = doc! { "app_id": app_id, "app_version": version, "app_build": build, "retraced": { "$ne": true }, "frames.0": { "$exists": true } };
    let Ok(mut cursor) = state.db.crash_reports.find(filter).limit(5000).await else { return };
    let mut updated = 0;
    while let Ok(Some(report)) = cursor.try_next().await {
        let stored: Vec<Document> = report.get_array("frames").map(|a| a.iter().filter_map(|b| b.as_document().cloned()).collect()).unwrap_or_default();
        let frames: Vec<ClientFrame> = stored
            .iter()
            .map(|d| ClientFrame {
                symbol: d.get_str("symbol").unwrap_or(""),
                file: d.get_str("file").unwrap_or(""),
                line: d.get_i64("line").or_else(|_| d.get_i32("line").map(i64::from)).unwrap_or(0),
                library: d.get_str("library").unwrap_or(""),
            })
            .collect();
        let (sym, retraced) = sym_frames(&frames, Some(&mapping));
        if !retraced {
            continue;
        }
        let mut set = doc! { "symbolicated_frames": mongodb::bson::to_bson(&sym).unwrap_or(Bson::Array(vec![])), "symbolicated": true, "retraced": true };
        if let Some(name) = report.get_str("exception_name").ok().and_then(|n| class_name(Some(&mapping), n)) {
            set.insert("exception_name", name);
        }
        let id = report.get("_id").cloned().unwrap_or(Bson::Null);
        if state.db.crash_reports.update_one(doc! { "_id": id }, doc! { "$set": set }).await.is_ok() {
            updated += 1;
        }
    }
    if updated > 0 {
        println!("[Mapping] retraced {updated} stored report(s) of {app_id} {version} ({build})");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAPPING: &str = r#"# compiler: R8
com.dmart.checkout.CheckoutActivity -> a.b:
# {"id":"sourceFile","fileName":"CheckoutActivity.kt"}
    android.widget.Button pay -> a
    1:1:void <init>():12:12 -> <init>
    1:3:void onPay(android.view.View):40:42 -> c
    4:4:void com.dmart.checkout.PriceCalculator.total(java.util.List):88:88 -> c
    4:4:void onPay(android.view.View):43 -> c
    5:9:void submit():60:64 -> d
    void unused() -> e
com.dmart.checkout.PaymentException -> a.c:
# {"id":"sourceFile","fileName":"PaymentException.kt"}
com.dmart.checkout.PriceCalculator -> a.d:
# {"id":"sourceFile","fileName":"PriceCalculator.kt"}
"#;

    #[test]
    fn retraces_lines_inlining_and_classes() {
        let m = Mapping::parse(MAPPING);
        assert_eq!(m.class("a.c"), Some("com.dmart.checkout.PaymentException"));
        let f = m.retrace("a.b.c", 2).unwrap();
        assert_eq!(f, vec![Retraced { class: "com.dmart.checkout.CheckoutActivity".into(), method: "onPay".into(), file: Some("CheckoutActivity.kt".into()), line: Some(41) }]);
        let inline = m.retrace("a.b.c", 4).unwrap();
        assert_eq!(inline.len(), 2);
        assert_eq!(inline[0].class, "com.dmart.checkout.PriceCalculator");
        assert_eq!(inline[0].method, "total");
        assert_eq!(inline[0].line, Some(88));
        assert_eq!(inline[0].file.as_deref(), Some("PriceCalculator.kt"));
        assert_eq!(inline[1].method, "onPay");
        assert_eq!(inline[1].line, Some(43));
        assert_eq!(m.retrace("a.b.d", 7).unwrap()[0].line, Some(62));
        assert_eq!(m.retrace("a.b.e", 0).unwrap()[0].method, "unused");
        assert_eq!(m.retrace("a.b.zz", 3).unwrap()[0].class, "com.dmart.checkout.CheckoutActivity");
        assert!(m.retrace("x.y.z", 1).is_none());
    }

    #[test]
    fn client_frames_without_mapping() {
        let frames = [ClientFrame { symbol: "com.dmart.Main.run", file: "Main.kt", line: 10, library: "app" }, ClientFrame { symbol: "android.os.Handler.dispatch", file: "Handler.java", line: 0, library: "android" }];
        let (sym, retraced) = sym_frames(&frames, None);
        assert!(!retraced);
        assert!(sym[0].in_app && !sym[1].in_app);
        assert_eq!(sym[0].line, Some(10));
        assert_eq!(sym[1].line, None);
    }
}
