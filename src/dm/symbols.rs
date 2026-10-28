//! dSYM upload + symbolication.
//!
//! Upload (POST /v1/symbols, multipart, sent by Scripts/dm_upload_symbols.sh):
//!   the zipped .dSYM is unpacked, every DWARF binary inside (Contents/Resources/DWARF/*) is
//!   split into its architecture slices, and each slice is stored in GridFS bucket "dsyms" under
//!   its own Mach-O UUID, indexed in `symbol_files` together with its __TEXT vmaddr.
//!
//! Symbolication (pure Rust, addr2line + object — no Xcode/atos needed on the server):
//!   runtime address A in image I (loaded at L) → file address = A - L + __TEXT.vmaddr(dSYM),
//!   looked up for function, file and line. Return addresses use A - 1 (the call instruction),
//!   except frame 0 of a signal crash, which is the exact PC.
//!   Reports that arrived before their dSYM are re-symbolicated in the background on upload.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;

use axum::extract::{Multipart, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::io::AsyncWriteExt;
use futures_util::{AsyncReadExt, TryStreamExt};
use mongodb::bson::{doc, Bson, DateTime, Document};
use object::read::macho::{FatArch, MachOFatFile32, MachOFatFile64};
use object::{Object, ObjectSegment};
use serde::Serialize;
use serde_json::json;
use tokio::io::AsyncWriteExt as _;

use super::{AppState, RequestMeta};

/// One architecture slice of a DWARF binary.
struct DwarfSlice {
    uuid: String,
    arch: String,
    name: String,
    text_vmaddr: u64,
    data: Vec<u8>,
}

/// Image info as stored on a crash report.
#[derive(Clone, Debug)]
pub struct ImageInfo {
    pub name: String,
    pub uuid: String,
    pub load_address: u64,
    pub size: u64,
    pub is_main_executable: bool,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct SymFrame {
    pub index: usize,
    pub address: String,
    pub image: Option<String>,
    pub image_offset: Option<String>,
    pub in_app: bool,
    pub symbol: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    /// Functions this frame's code was inlined into, innermost first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inlined_into: Vec<String>,
}

impl SymFrame {
    /// Stable-ish grouping key: symbol if known, else image+offset.
    pub fn key(&self) -> String {
        match (&self.symbol, &self.image, &self.image_offset) {
            (Some(symbol), _, _) => symbol.clone(),
            (None, Some(image), Some(offset)) => format!("{image}+{offset}"),
            _ => self.address.clone(),
        }
    }

    pub fn display(&self) -> String {
        let mut out = self.key();
        if let (Some(file), Some(line)) = (&self.file, self.line) {
            let short = file.rsplit('/').next().unwrap_or(file);
            out.push_str(&format!(" ({short}:{line})"));
        }
        out
    }
}

// ── Upload ────────────────────────────────────────────────────────────────────

pub async fn upload(State(state): State<AppState>, headers: HeaderMap, mut multipart: Multipart) -> Response {
    let meta = RequestMeta::from_headers(&headers);
    if state.config.auth_enabled() {
        let app_id = meta.app_id.clone().unwrap_or_default();
        if !meta.api_key.as_deref().is_some_and(|k| state.config.key_allows(k, &app_id)) {
            return error(StatusCode::UNAUTHORIZED, "missing or invalid x-api-key");
        }
    }

    let mut fields: HashMap<String, String> = HashMap::new();
    let mut archive: Option<tempfile::NamedTempFile> = None;

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(err) => return error(StatusCode::BAD_REQUEST, &format!("bad multipart body: {err}")),
        };
        let name = field.name().unwrap_or_default().to_string();
        if name == "dsym" {
            match spool_to_temp(field).await {
                Ok(file) => archive = Some(file),
                Err(err) => return error(StatusCode::BAD_REQUEST, &format!("could not read dsym: {err}")),
            }
        } else {
            match field.text().await {
                Ok(text) => {
                    fields.insert(name, text);
                }
                Err(err) => return error(StatusCode::BAD_REQUEST, &format!("bad field {name}: {err}")),
            }
        }
    }

    let Some(archive) = archive else {
        return error(StatusCode::BAD_REQUEST, "multipart field `dsym` (zipped .dSYM or DWARF binary) is required");
    };

    let slices = match tokio::task::spawn_blocking(move || extract_slices(archive.path())).await {
        Ok(Ok(slices)) => slices,
        Ok(Err(err)) => return error(StatusCode::UNPROCESSABLE_ENTITY, &format!("not a valid dSYM: {err}")),
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    };
    if slices.is_empty() {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "no Mach-O DWARF binaries with an LC_UUID found");
    }

    let mut stored = Vec::new();
    for slice in slices {
        match store_slice(&state, &slice, &fields).await {
            Ok(()) => stored.push(json!({ "uuid": slice.uuid, "name": slice.name, "arch": slice.arch })),
            Err(err) => return error(StatusCode::SERVICE_UNAVAILABLE, &format!("storage failed: {err}")),
        }
    }

    let uuids: Vec<String> = stored.iter().filter_map(|s| s["uuid"].as_str().map(str::to_string)).collect();
    println!("[Symbols] stored {} slice(s): {}", uuids.len(), uuids.join(", "));
    let background = state.clone();
    let pending = uuids.clone();
    tokio::spawn(async move { resymbolicate(&background, &pending).await });

    (StatusCode::OK, axum::Json(json!({ "stored": stored }))).into_response()
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({ "error": message }))).into_response()
}

async fn spool_to_temp(mut field: axum::extract::multipart::Field<'_>) -> Result<tempfile::NamedTempFile, crate::BoxError> {
    let temp = tempfile::NamedTempFile::new()?;
    let mut file = tokio::fs::File::create(temp.path()).await?;
    while let Some(chunk) = field.chunk().await? {
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(temp)
}

/// Accepts a zip of one or more .dSYM bundles, or a bare DWARF Mach-O.
fn extract_slices(path: &std::path::Path) -> Result<Vec<DwarfSlice>, crate::BoxError> {
    let mut data = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut data)?;

    let mut slices = Vec::new();
    if data.starts_with(b"PK") {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(data))?;
        for index in 0..zip.len() {
            let mut entry = zip.by_index(index)?;
            if !entry.is_file() || !entry.name().contains("/Contents/Resources/DWARF/") {
                continue;
            }
            let name = entry.name().rsplit('/').next().unwrap_or_default().to_string();
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut bytes)?;
            slices.extend(macho_slices(&name, &bytes)?);
        }
    } else {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("dwarf").to_string();
        slices.extend(macho_slices(&name, &data)?);
    }
    Ok(slices)
}

/// Splits a (possibly universal) Mach-O into per-architecture slices with their UUIDs.
fn macho_slices(name: &str, data: &[u8]) -> Result<Vec<DwarfSlice>, crate::BoxError> {
    let mut parts: Vec<&[u8]> = Vec::new();
    match object::FileKind::parse(data)? {
        object::FileKind::MachOFat32 => {
            for arch in MachOFatFile32::parse(data)?.arches() {
                parts.push(arch.data(data)?);
            }
        }
        object::FileKind::MachOFat64 => {
            for arch in MachOFatFile64::parse(data)?.arches() {
                parts.push(arch.data(data)?);
            }
        }
        object::FileKind::MachO32 | object::FileKind::MachO64 => parts.push(data),
        other => return Err(format!("{name}: unsupported file kind {other:?}").into()),
    }

    let mut slices = Vec::new();
    for part in parts {
        let file = object::File::parse(part)?;
        let Some(uuid) = file.mach_uuid()? else { continue };
        let text_vmaddr = file
            .segments()
            .find(|segment| segment.name().ok().flatten() == Some("__TEXT"))
            .map(|segment| segment.address())
            .unwrap_or(0);
        slices.push(DwarfSlice {
            uuid: format_uuid(&uuid),
            arch: format!("{:?}", file.architecture()).to_lowercase(),
            name: name.to_string(),
            text_vmaddr,
            data: part.to_vec(),
        });
    }
    Ok(slices)
}

/// Uppercase with dashes — the format `dwarfdump --uuid` prints and the iOS SDK reports.
pub fn format_uuid(bytes: &[u8; 16]) -> String {
    let hex = hex::encode_upper(bytes);
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

async fn store_slice(state: &AppState, slice: &DwarfSlice, fields: &HashMap<String, String>) -> Result<(), crate::BoxError> {
    let bucket = state.db.dsym_bucket();
    let mut upload = bucket.open_upload_stream(&slice.uuid).await?;
    upload.write_all(&slice.data).await?;
    upload.close().await?;
    let gridfs_id = upload.id().clone();

    let previous = state.db.symbol_files.find_one(doc! { "_id": &slice.uuid }).await?;
    let field = |key: &str| fields.get(key).cloned().unwrap_or_default();
    state
        .db
        .symbol_files
        .replace_one(
            doc! { "_id": &slice.uuid },
            doc! {
                "_id": &slice.uuid,
                "name": &slice.name,
                "arch": &slice.arch,
                "text_vmaddr": slice.text_vmaddr as i64,
                "size": slice.data.len() as i64,
                "gridfs_id": gridfs_id,
                "bundle_id": field("bundle_id"),
                "app_version": field("app_version"),
                "app_build": field("app_build"),
                "uploaded_at": DateTime::now(),
            },
        )
        .upsert(true)
        .await?;

    if let Some(old_id) = previous.and_then(|p| p.get("gridfs_id").cloned()) {
        let _ = bucket.delete(old_id).await;
    }
    // Drop any stale cached copy.
    let _ = tokio::fs::remove_file(state.config.dsym_cache_dir.join(&slice.uuid)).await;
    Ok(())
}

// ── Lookup ────────────────────────────────────────────────────────────────────

/// UUIDs (of `uuids`) that have a dSYM on file.
pub async fn known_uuids(state: &AppState, uuids: &[String]) -> HashSet<String> {
    if uuids.is_empty() {
        return HashSet::new();
    }
    let mut known = HashSet::new();
    if let Ok(mut cursor) = state.db.symbol_files.find(doc! { "_id": { "$in": uuids } }).projection(doc! { "_id": 1 }).await {
        while let Ok(Some(found)) = cursor.try_next().await {
            if let Ok(id) = found.get_str("_id") {
                known.insert(id.to_string());
            }
        }
    }
    known
}

/// Local path of the DWARF binary for `uuid` (downloaded from GridFS on first use) and its
/// __TEXT vmaddr.
async fn dwarf_file(state: &AppState, uuid: &str) -> Option<(PathBuf, u64)> {
    let record = state.db.symbol_files.find_one(doc! { "_id": uuid }).await.ok()??;
    let text_vmaddr = record.get_i64("text_vmaddr").unwrap_or(0) as u64;
    let path = state.config.dsym_cache_dir.join(uuid);
    if tokio::fs::metadata(&path).await.is_ok() {
        return Some((path, text_vmaddr));
    }

    let gridfs_id = record.get("gridfs_id")?.clone();
    let mut stream = state.db.dsym_bucket().open_download_stream(gridfs_id).await.ok()?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.ok()?;

    tokio::fs::create_dir_all(&state.config.dsym_cache_dir).await.ok()?;
    let temp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp, &bytes).await.ok()?;
    tokio::fs::rename(&temp, &path).await.ok()?;
    Some((path, text_vmaddr))
}

// ── Symbolication ─────────────────────────────────────────────────────────────

/// Resolves every address. Frames in images without a dSYM keep image+offset only.
/// `first_is_pc`: frame 0 is an exact program counter (signal crashes), not a return address.
pub async fn symbolicate(state: &AppState, images: &[ImageInfo], addresses: &[u64], first_is_pc: bool) -> Vec<SymFrame> {
    let uuids: Vec<String> = images.iter().map(|i| i.uuid.clone()).collect();
    let known = known_uuids(state, &uuids).await;

    let mut frames: Vec<SymFrame> = addresses
        .iter()
        .enumerate()
        .map(|(index, &address)| {
            let image = images.iter().find(|i| address >= i.load_address && address < i.load_address.saturating_add(i.size));
            SymFrame {
                index,
                address: format!("0x{address:x}"),
                image: image.map(|i| i.name.clone()),
                image_offset: image.map(|i| format!("0x{:x}", address - i.load_address)),
                in_app: image.is_some_and(|i| i.is_main_executable || known.contains(&i.uuid)),
                ..Default::default()
            }
        })
        .collect();

    // Group probes per image that has a dSYM, then resolve each group in one blocking task.
    let mut by_image: HashMap<String, Vec<(usize, u64)>> = HashMap::new();
    for (index, &address) in addresses.iter().enumerate() {
        let Some(image) = images.iter().find(|i| address >= i.load_address && address < i.load_address.saturating_add(i.size)) else {
            continue;
        };
        if !known.contains(&image.uuid) {
            continue;
        }
        let relative = address - image.load_address;
        let relative = if index == 0 && first_is_pc { relative } else { relative.saturating_sub(1) };
        by_image.entry(image.uuid.clone()).or_default().push((index, relative));
    }

    for (uuid, probes) in by_image {
        let Some((path, text_vmaddr)) = dwarf_file(state, &uuid).await else { continue };
        let resolved = tokio::task::spawn_blocking(move || resolve(&path, text_vmaddr, &probes)).await;
        if let Ok(Ok(results)) = resolved {
            for (index, symbol, file, line, inlined) in results {
                let frame = &mut frames[index];
                frame.symbol = symbol;
                frame.file = file;
                frame.line = line;
                frame.inlined_into = inlined;
            }
        }
    }
    frames
}

type Resolved = (usize, Option<String>, Option<String>, Option<u32>, Vec<String>);

fn resolve(path: &std::path::Path, text_vmaddr: u64, probes: &[(usize, u64)]) -> Result<Vec<Resolved>, crate::BoxError> {
    let loader = addr2line::Loader::new(path).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(probes.len());
    for &(index, relative) in probes {
        let probe = text_vmaddr + relative;
        let mut names: Vec<String> = Vec::new();
        let mut file = None;
        let mut line = None;
        if let Ok(mut frames) = loader.find_frames(probe) {
            while let Ok(Some(frame)) = frames.next() {
                if let Some(function) = &frame.function {
                    if let Ok(name) = function.demangle() {
                        names.push(name.into_owned());
                    }
                }
                if file.is_none() {
                    if let Some(location) = &frame.location {
                        file = location.file.map(str::to_string);
                        line = location.line;
                    }
                }
            }
        }
        if names.is_empty() {
            if let Some(symbol) = loader.find_symbol(probe) {
                names.push(symbol.to_string());
            }
        }
        let symbol = names.first().cloned();
        let inlined = names.into_iter().skip(1).collect();
        out.push((index, symbol, file, line, inlined));
    }
    Ok(out)
}

/// Fills in frames for stored reports that were waiting on these dSYMs.
pub async fn resymbolicate(state: &AppState, uuids: &[String]) {
    let filter = doc! { "binary_images.uuid": { "$in": uuids }, "symbolicated": false };
    let Ok(mut cursor) = state.db.crash_reports.find(filter).limit(1000).await else { return };
    let mut updated = 0;
    while let Ok(Some(report)) = cursor.try_next().await {
        let (images, addresses, first_is_pc) = super::crashlytics::stored_stack(&report);
        if addresses.is_empty() {
            continue;
        }
        let frames = symbolicate(state, &images, &addresses, first_is_pc).await;
        let symbolicated = frames.iter().any(|f| f.symbol.is_some());
        let frames_bson = mongodb::bson::to_bson(&frames).unwrap_or(Bson::Array(vec![]));
        let id = report.get("_id").cloned().unwrap_or(Bson::Null);
        let update: Document = doc! { "$set": { "symbolicated_frames": frames_bson, "symbolicated": symbolicated } };
        if state.db.crash_reports.update_one(doc! { "_id": id }, update).await.is_ok() && symbolicated {
            updated += 1;
        }
    }
    if updated > 0 {
        println!("[Symbols] re-symbolicated {updated} stored report(s)");
    }
}
