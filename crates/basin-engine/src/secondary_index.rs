//! Per-project secondary B-tree indexes (Phase 5.7 B1).
//!
//! # Overview
//!
//! Maps `(table, indexed_col, value) → Vec<(file_path, row_group, row)>` so
//! that a point query on an indexed column can skip directly to the right
//! file+row-group+row without scanning any other files.
//!
//! # Data layout
//!
//! One index file per `(project, table, col)`, stored at:
//! ```text
//! <object_store_root>/<project>/secondary_indexes/<table>/<col>.idx
//! ```
//!
//! The file is a little-endian binary blob encoded as:
//! ```text
//! [header: magic(4) + version(1) + entry_count(u64 LE)]
//! [entries: { key_len(u32 LE) | key_bytes | locations_len(u32 LE)
//!             | locations: { file_len(u32 LE) | file_bytes | rg(u32 LE) | row(u64 LE) }* }*]
//! ```
//!
//! Strings are UTF-8.  Integer scalar values are serialised as their decimal
//! text representation so the format stays self-describing and diff-friendly.
//!
//! # In-RAM structure
//!
//! One [`ProjectIndexRegistry`] is constructed at `Engine::new` time and
//! stored in `EngineInner`. The registry holds one
//! `BTreeMap<IndexKey, Vec<IndexLocation>>` per loaded `(table, col)` pair,
//! wrapped in a `Mutex` so concurrent sessions can probe / insert concurrently.
//!
//! LRU eviction: if the registry exceeds [`MAX_INDEX_ENTRIES_PER_COL`] entries
//! for a single column index, the oldest 25% are evicted (key-count based, not
//! byte-based — sufficient for the PoC).
//!
//! # Persistence
//!
//! After every DML operation that touches an indexed table, the affected
//! column indexes are serialised and flushed to the object store. On engine
//! restart the index is loaded lazily (on first access for a given table/col).
//!
//! # Cross-project isolation
//!
//! The registry is keyed by `(ProjectId, TableName, col_name)`. There is no
//! path through the public API that lets project A query B's index — every
//! caller supplies their own `ProjectId` and the registry enforces it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{BasinError, ProjectId, Result, TableName};
use tracing::{debug, warn};


/// Serialisation magic for the index file format.
const MAGIC: [u8; 4] = *b"BIDX";
/// Current format version.
const VERSION: u8 = 2;

/// Maximum number of distinct key values kept in RAM per `(table, col)` pair.
///
/// When the count exceeds this limit we evict the oldest 25% of entries.  This
/// keeps memory bounded while still covering the hot working set for most
/// workloads.  In production a proper LRU would key on entry-weight (bytes),
/// but for the PoC a simple count cap is sufficient.
const MAX_INDEX_ENTRIES_PER_COL: usize = 100_000;

/// One physical location pointer: a (file_path, row_group_index, row_index).
///
/// `row_group` is the 0-based index of the Parquet row group within the file.
/// `row` is the 0-based index of the row within that row group.
///
/// The integer row pointer lets point queries skip directly to the right row
/// once the file has been opened; the primary win, however, is file-level
/// pruning: any file NOT listed in the index for the given key is skipped
/// entirely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexLocation {
    /// Full object-store path of the Parquet/Vortex file.
    pub file_path: String,
    /// 0-based row-group index within the file.
    pub row_group: u32,
    /// 0-based row index within the row group.
    pub row: u64,
}

/// An in-RAM B-tree index for one `(table, col)` pair.
///
/// Keys are the serialised text form of the indexed column values; values are
/// the list of physical locations where that key appears.  The B-tree gives
/// O(log n) lookup and supports ordered iteration (useful for range queries in
/// a future phase).
#[derive(Debug, Default)]
pub struct ColumnIndex {
    /// `key_text → Vec<(file, row_group, row)>`.
    ///
    /// `key_text` is the decimal representation for integer values and the
    /// raw UTF-8 string for text values. This allows heterogeneous column
    /// types to coexist in the same data structure without per-type
    /// discrimination at the index layer.
    pub entries: BTreeMap<String, Vec<IndexLocation>>,

    /// Insertion-order sequence per key for simple FIFO eviction.  We only
    /// track distinct key order (not individual locations), which is an
    /// approximation but keeps overhead tiny.
    insert_order: Vec<String>,

    /// Set of data files this index FULLY covers — every row of these files has
    /// been extracted into `entries`. This is the coverage ground-truth a reader
    /// consults before trusting a probe HIT as an authoritative file allowlist:
    /// pruning to a HIT's files is only correct when the index covers ALL live
    /// files, else a key whose rows span a covered and an uncovered file would
    /// be served a subset (dropping the uncovered file's rows). A file is added
    /// only when fully indexed (INSERT extractor, CREATE INDEX backfill, overlay
    /// drain re-index); it is removed when the file is replaced/deleted
    /// (`remove_file`) and the whole set is cleared on FIFO eviction (eviction
    /// drops keys, making per-file coverage indeterminate — clearing is the
    /// conservative, always-safe choice: an empty set ⇒ "trust no prune").
    indexed_files: HashSet<String>,
}

impl ColumnIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            insert_order: Vec::new(),
            indexed_files: HashSet::new(),
        }
    }

    /// Mark `file_path` as FULLY indexed (every row extracted into `entries`).
    /// Call this only after all of a file's rows are inserted.
    pub fn mark_file_indexed(&mut self, file_path: &str) {
        self.indexed_files.insert(file_path.to_string());
    }

    /// True if `file_path` is recorded as fully covered by this index.
    pub fn is_file_indexed(&self, file_path: &str) -> bool {
        self.indexed_files.contains(file_path)
    }

    /// The set of files this index fully covers (see [`Self::indexed_files`]).
    pub fn indexed_files(&self) -> &HashSet<String> {
        &self.indexed_files
    }

    /// Add a location entry for `key`. If `key` is new, it is appended to
    /// `insert_order` for eviction tracking.
    pub fn insert(&mut self, key: String, loc: IndexLocation) {
        let entry = self.entries.entry(key.clone()).or_default();
        entry.push(loc);
        if entry.len() == 1 {
            // Newly seen key — track in insertion order.
            self.insert_order.push(key);
        }
        // Evict if we have grown past the cap.
        if self.entries.len() > MAX_INDEX_ENTRIES_PER_COL {
            let evict_count = MAX_INDEX_ENTRIES_PER_COL / 4; // 25%
            let to_evict: Vec<String> = self.insert_order.drain(..evict_count).collect();
            for k in &to_evict {
                self.entries.remove(k);
            }
            // Eviction drops keys, not whole files, so per-file coverage becomes
            // indeterminate (an evicted key may have been a file's only entry).
            // Clear the coverage set: the index degrades to "trust no prune"
            // (correct, full-scan) until files are re-indexed. Coarse but always
            // safe — the alternative (tracking which files lost a key) is far
            // more bookkeeping for a cold path.
            self.indexed_files.clear();
        }
    }

    /// Remove all locations that reference `file_path`. Called by the
    /// DELETE / DROP maintenance path. Returns the number of entries removed.
    pub fn remove_file(&mut self, file_path: &str) -> usize {
        let mut removed = 0usize;
        let mut dead_keys: Vec<String> = Vec::new();
        for (key, locs) in self.entries.iter_mut() {
            let before = locs.len();
            locs.retain(|l| l.file_path != file_path);
            removed += before - locs.len();
            if locs.is_empty() {
                dead_keys.push(key.clone());
            }
        }
        for k in &dead_keys {
            self.entries.remove(k);
            self.insert_order.retain(|x| x != k);
        }
        // The file is no longer present in this index → it is no longer covered.
        self.indexed_files.remove(file_path);
        removed
    }

    /// Probe the index for an exact-match key.
    /// Returns the file paths that contain the key (deduplicated for the fast
    /// file-pruning path). Returns `None` if the key is not in the index (the
    /// index may be incomplete — callers MUST fall back to a full scan on
    /// `None`). Returns `Some(empty)` when the key is known-absent (was
    /// indexed and then deleted).
    pub fn probe(&self, key: &str) -> Option<Vec<String>> {
        self.entries.get(key).map(|locs| {
            let mut files: Vec<String> = locs.iter().map(|l| l.file_path.clone()).collect();
            files.sort_unstable();
            files.dedup();
            files
        })
    }

    /// Probe the index for an exact-match key, returning the full
    /// `(file_path, row_group, row)` location tuples.
    ///
    /// Returns `None` when the key is absent from the index (fall back to full
    /// scan). Returns `Some(vec![])` when the key was indexed and later deleted
    /// (known-absent — no files to scan).
    pub fn probe_locations(&self, key: &str) -> Option<Vec<IndexLocation>> {
        self.entries.get(key).map(|locs| locs.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Serialisation helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a `ColumnIndex` to bytes.
///
/// Format (little-endian):
/// ```text
/// magic[4] version[1] entry_count[u64]
/// for each entry:
///   key_len[u32] key[key_len bytes]
///   loc_count[u32]
///   for each loc:
///     file_len[u32] file[file_len bytes] rg[u32] row[u64]
/// ```
pub fn encode_index(idx: &ColumnIndex) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);
    let entry_count = idx.entries.len() as u64;
    buf.extend_from_slice(&entry_count.to_le_bytes());
    for (key, locs) in &idx.entries {
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(locs.len() as u32).to_le_bytes());
        for loc in locs {
            let fb = loc.file_path.as_bytes();
            buf.extend_from_slice(&(fb.len() as u32).to_le_bytes());
            buf.extend_from_slice(fb);
            buf.extend_from_slice(&loc.row_group.to_le_bytes());
            buf.extend_from_slice(&loc.row.to_le_bytes());
        }
    }
    // v2 trailer: the indexed-file coverage set.
    buf.extend_from_slice(&(idx.indexed_files.len() as u64).to_le_bytes());
    for f in &idx.indexed_files {
        let fb = f.as_bytes();
        buf.extend_from_slice(&(fb.len() as u32).to_le_bytes());
        buf.extend_from_slice(fb);
    }
    buf
}

/// Decode bytes produced by [`encode_index`] back into a [`ColumnIndex`].
/// Returns an error on any truncation or format mismatch.
pub fn decode_index(data: &[u8]) -> Result<ColumnIndex> {
    let mut pos = 0usize;

    macro_rules! read_bytes {
        ($n:expr) => {{
            if pos + $n > data.len() {
                return Err(BasinError::internal("secondary index: truncated file"));
            }
            let slice = &data[pos..pos + $n];
            pos += $n;
            slice
        }};
    }
    macro_rules! read_u32 {
        () => {{
            let b = read_bytes!(4);
            u32::from_le_bytes(b.try_into().unwrap())
        }};
    }
    macro_rules! read_u64 {
        () => {{
            let b = read_bytes!(8);
            u64::from_le_bytes(b.try_into().unwrap())
        }};
    }

    let magic = read_bytes!(4);
    if magic != MAGIC {
        return Err(BasinError::internal("secondary index: bad magic"));
    }
    let ver = read_bytes!(1)[0];
    // v1 has no coverage trailer (decodes as uncovered → "trust no prune", safe);
    // v2 appends the indexed-file set. Accept both.
    if ver != 1 && ver != 2 {
        return Err(BasinError::internal(format!(
            "secondary index: unsupported version {ver}"
        )));
    }
    let entry_count = read_u64!() as usize;
    let mut idx = ColumnIndex::new();
    for _ in 0..entry_count {
        let klen = read_u32!() as usize;
        let kb = read_bytes!(klen);
        let key = std::str::from_utf8(kb)
            .map_err(|e| BasinError::internal(format!("secondary index: bad key utf8: {e}")))?
            .to_string();
        let loc_count = read_u32!() as usize;
        for _ in 0..loc_count {
            let flen = read_u32!() as usize;
            let fb = read_bytes!(flen);
            let file_path = std::str::from_utf8(fb)
                .map_err(|e| BasinError::internal(format!("secondary index: bad file utf8: {e}")))?
                .to_string();
            let row_group = read_u32!();
            let row = read_u64!();
            idx.insert(key.clone(), IndexLocation { file_path, row_group, row });
        }
    }
    // v2 trailer: indexed-file coverage set (read AFTER inserts, since insert's
    // eviction path clears the set). v1 has no trailer → coverage stays empty.
    if ver >= 2 {
        let file_count = read_u64!() as usize;
        for _ in 0..file_count {
            let flen = read_u32!() as usize;
            let fb = read_bytes!(flen);
            let f = std::str::from_utf8(fb)
                .map_err(|e| BasinError::internal(format!("secondary index: bad coverage utf8: {e}")))?
                .to_string();
            idx.indexed_files.insert(f);
        }
    }
    Ok(idx)
}

// ─────────────────────────────────────────────────────────────────────────────
// Registry
// ─────────────────────────────────────────────────────────────────────────────

/// Key into the in-RAM index registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide registry of loaded column indexes.
///
/// Each entry is a `ColumnIndex` under a `Mutex` so concurrent sessions can
/// probe and insert without writer-starvation. The outer `Mutex` wrapping the
/// whole `HashMap` is only held briefly to look up or insert an `Arc`.
pub struct ProjectIndexRegistry {
    /// `(project, table, col) → Arc<Mutex<ColumnIndex>>`.
    ///
    /// Entries are never deleted from the map itself (the ColumnIndex inside
    /// may be empty after all rows are removed from the index). The map is
    /// bounded by the number of distinct `(project, table, col)` triples ever
    /// indexed in this process lifetime, which is tiny relative to the number
    /// of rows.
    inner: Mutex<HashMap<RegKey, Arc<Mutex<ColumnIndex>>>>,
}

impl ProjectIndexRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Return the `Arc<Mutex<ColumnIndex>>` for `(project, table, col)`,
    /// creating an empty one if it doesn't exist yet.
    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<ColumnIndex>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("index registry lock poisoned");
        map.entry(key).or_insert_with(|| Arc::new(Mutex::new(ColumnIndex::new()))).clone()
    }

    /// Probe the index for `(project, table, col)` with an exact-match key.
    ///
    /// Returns `Some(file_paths)` if the key is in the index (even if the list
    /// is empty — meaning the key existed and was deleted). Returns `None` if:
    /// * The index for this column has never been loaded into RAM, OR
    /// * The entry is absent (key was never indexed).
    ///
    /// Callers MUST treat `None` as "unknown — fall through to full scan".
    pub fn probe(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        key: &str,
    ) -> Option<Vec<String>> {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            map.get(&RegKey { project: *project, table: table.clone(), col: col.to_string() })?
                .clone()
        };
        let idx = arc.lock().expect("column index lock poisoned");
        idx.probe(key)
    }

    /// Probe the index for `(project, table, col)` with an exact-match key,
    /// returning full `(file_path, row_group, row)` location tuples.
    ///
    /// Returns `Some(locations)` when the key is present in the index.
    /// Returns `None` when the index is not loaded or the key is absent.
    /// Callers MUST treat `None` as "unknown — fall through to full scan".
    pub fn probe_locations(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        key: &str,
    ) -> Option<Vec<IndexLocation>> {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            map.get(&RegKey { project: *project, table: table.clone(), col: col.to_string() })?
                .clone()
        };
        let idx = arc.lock().expect("column index lock poisoned");
        idx.probe_locations(key)
    }

    /// Check whether an index has been loaded into RAM for `(project, table, col)`.
    pub fn is_loaded(&self, project: &ProjectId, table: &TableName, col: &str) -> bool {
        let map = self.inner.lock().expect("index registry lock poisoned");
        map.contains_key(&RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        })
    }

    /// Insert a batch of `(key, location)` pairs into the in-RAM index for
    /// `(project, table, col)`. Creates the entry if it doesn't exist.
    pub fn insert_batch(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        entries: Vec<(String, IndexLocation)>,
    ) {
        if entries.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut idx = arc.lock().expect("column index lock poisoned");
        for (key, loc) in entries {
            idx.insert(key, loc);
        }
    }

    /// Remove all index locations that reference `file_path` for
    /// `(project, table, col)`. Used when a file is deleted or replaced.
    pub fn remove_file_from_index(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            match map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            }) {
                Some(a) => a.clone(),
                None => return,
            }
        };
        let mut idx = arc.lock().expect("column index lock poisoned");
        let _ = idx.remove_file(file_path);
    }

    /// Mark `file_path` as fully indexed for `(project, table, col)`. No-op if
    /// the column index isn't loaded (nothing to mark).
    pub fn mark_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            match map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            }) {
                Some(a) => a.clone(),
                None => return,
            }
        };
        arc.lock()
            .expect("column index lock poisoned")
            .mark_file_indexed(file_path);
    }

    /// The set of files the `(project, table, col)` index fully covers. Returns
    /// an empty set when the index isn't loaded — an unloaded index covers
    /// nothing, so a caller's completeness check correctly declines to prune.
    pub fn indexed_files_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> HashSet<String> {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            match map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            }) {
                Some(a) => a.clone(),
                None => return HashSet::new(),
            }
        };
        let guard = arc.lock().expect("column index lock poisoned");
        let files = guard.indexed_files().clone();
        files
    }

    /// Populate the registry from a serialised blob (loaded from disk).
    /// If an entry already exists it is **replaced** with the deserialized
    /// data — this is the restart-load path.
    pub fn load_from_bytes(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        data: &[u8],
    ) -> Result<()> {
        let loaded = decode_index(data)?;
        let arc = self.get_or_create(project, table, col);
        let mut idx = arc.lock().expect("column index lock poisoned");
        *idx = loaded;
        Ok(())
    }

    /// Serialise the in-RAM index for `(project, table, col)` to bytes.
    /// Returns `None` if the index is not loaded (nothing to flush).
    pub fn encode_to_bytes(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Vec<u8>> {
        let arc = {
            let map = self.inner.lock().expect("index registry lock poisoned");
            map.get(&RegKey { project: *project, table: table.clone(), col: col.to_string() })?
                .clone()
        };
        let idx = arc.lock().expect("column index lock poisoned");
        Some(encode_index(&idx))
    }
}

/// FIX 2 — bridge the engine's `ProjectIndexRegistry` to the shard compactor.
///
/// `basin-shard` cannot name `ProjectIndexRegistry` (it would close the
/// `basin-engine -> basin-shard` dependency cycle), so the compactor talks to
/// the registry through the [`basin_shard::SecondaryIndexSink`] trait. The
/// shard passes primitive `(key, file, row_group, row)` tuples — exactly the
/// fields of [`IndexLocation`] — which we reassemble here. The `key`/`row_group`
/// semantics match [`extract_entries_from_batch`] (the engine-side INSERT-path
/// extractor) so locations registered by the compactor are probe-compatible
/// with locations registered on the direct-INSERT path.
impl basin_shard::SecondaryIndexSink for ProjectIndexRegistry {
    fn insert_batch_locations(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        entries: Vec<(String, String, u32, u64)>,
    ) {
        let mapped: Vec<(String, IndexLocation)> = entries
            .into_iter()
            .map(|(key, file_path, row_group, row)| {
                (key, IndexLocation { file_path, row_group, row })
            })
            .collect();
        self.insert_batch(project, table, col, mapped);
    }

    fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        self.remove_file_from_index(project, table, col, file_path);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Object-store path helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Object-store path under which the index file for `(project, table, col)` is
/// stored. Follows the same `projects/{project}/...` prefix convention as all
/// other basin-storage paths so project isolation is maintained.
///
/// Format: `projects/{project}/secondary_indexes/{table}/{col}.idx`
pub fn index_object_path(
    project: &ProjectId,
    table: &TableName,
    col: &str,
) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "projects/{}/secondary_indexes/{}/{}.idx",
        project, table, col
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalar-value → key-text helper
// ─────────────────────────────────────────────────────────────────────────────

/// Convert a `basin_storage::ScalarValue` to the canonical text key used in
/// the index. Returns `None` for `Null` or unsupported types.
pub fn scalar_to_key(val: &basin_storage::ScalarValue) -> Option<String> {
    match val {
        basin_storage::ScalarValue::Int64(v) => Some(v.to_string()),
        basin_storage::ScalarValue::UInt64(v) => Some(v.to_string()),
        basin_storage::ScalarValue::Float64(v) => {
            // Use bit-exact representation so equal floats map to the same key.
            Some(format!("{v:?}"))
        }
        basin_storage::ScalarValue::Utf8(s) => Some(s.clone()),
        basin_storage::ScalarValue::Boolean(b) => Some(if *b { "t" } else { "f" }.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Persistence: flush + load
// ─────────────────────────────────────────────────────────────────────────────

/// Flush the in-RAM index for `(project, table, col)` to the object store.
///
/// Best-effort: errors are logged but not propagated (a stale/missing on-disk
/// index falls back to full-scan naturally; correctness is not affected).
///
/// Uses the raw `ObjectStore` handle from `storage` so no per-project permit
/// is consumed — this is a background maintenance write, not a data-plane op.
pub async fn flush_index(
    registry: &ProjectIndexRegistry,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    col: &str,
) {
    let Some(bytes) = registry.encode_to_bytes(project, table, col) else {
        return;
    };
    let path = index_object_path(project, table, col);
    let store = storage.project_object_store(project);
    let payload = object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes));
    // Use put_opts (the dyn-compatible method) with default options.
    match store.put_opts(&path, payload, Default::default()).await {
        Ok(_) => {
            debug!(%project, %table, col = %col, "secondary index flushed");
        }
        Err(e) => {
            warn!(%project, %table, col = %col, err = %e, "secondary index flush failed (non-fatal)");
        }
    }
}

/// Load the index for `(project, table, col)` from the object store into the
/// registry.  No-op if the file doesn't exist (first-run or no index created
/// yet). Returns `true` if the load succeeded, `false` on any error or
/// absence.
pub async fn load_index(
    registry: &ProjectIndexRegistry,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    col: &str,
) -> bool {
    let path = index_object_path(project, table, col);
    let store = storage.project_object_store(project);
    // Use get_opts (the dyn-compatible method) with default options.
    let data = match store.get_opts(&path, Default::default()).await {
        Ok(result) => match result.bytes().await {
            Ok(b) => b,
            Err(_) => return false,
        },
        Err(_) => return false,
    };
    match registry.load_from_bytes(project, table, col, &data) {
        Ok(()) => {
            debug!(%project, %table, col = %col, "secondary index loaded from disk");
            true
        }
        Err(e) => {
            warn!(%project, %table, col = %col, err = %e, "secondary index load failed (non-fatal)");
            false
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Arrow-batch → index entries
// ─────────────────────────────────────────────────────────────────────────────

/// Extract index entries for `col_name` from `batch` at `file_path`, treating
/// the whole batch as row_group=0 (for a freshly-written single-batch file).
///
/// Supports `Int64`, `UInt64`, `Float64`, `Utf8`, `LargeUtf8`, and `Boolean`
/// columns. Other types are silently skipped (no index for that column).
///
/// Returns a `Vec<(key_text, IndexLocation)>` suitable for
/// [`ProjectIndexRegistry::insert_batch`].
pub fn extract_entries_from_batch(
    batch: &arrow_array::RecordBatch,
    col_name: &str,
    file_path: &str,
    row_group: u32,
) -> Vec<(String, IndexLocation)> {
    use arrow_array::Array;

    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return vec![];
    };
    let col = batch.column(col_idx);
    let mut out = Vec::with_capacity(batch.num_rows());

    macro_rules! extract_typed {
        ($arr_ty:ty, $col:expr, $fmt:expr) => {{
            if let Some(arr) = $col.as_any().downcast_ref::<$arr_ty>() {
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let key = $fmt(arr.value(row));
                    out.push((
                        key,
                        IndexLocation {
                            file_path: file_path.to_string(),
                            row_group,
                            row: row as u64,
                        },
                    ));
                }
            }
        }};
    }

    use arrow_array::{
        BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray, UInt64Array,
    };

    extract_typed!(Int64Array, col, |v: i64| v.to_string());
    extract_typed!(UInt64Array, col, |v: u64| v.to_string());
    extract_typed!(Float64Array, col, |v: f64| format!("{v:?}"));
    extract_typed!(StringArray, col, |v: &str| v.to_string());
    extract_typed!(LargeStringArray, col, |v: &str| v.to_string());
    extract_typed!(BooleanArray, col, |v: bool| if v { "t" } else { "f" }.to_string());

    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Counter: secondary-index file-open reductions
// ─────────────────────────────────────────────────────────────────────────────

/// Counts the number of files skipped because the secondary index proved the
/// queried key is absent. Exposed on `Engine` for integration test assertions.
///
/// This is a process-global counter; cross-project isolation is not an issue
/// here because the counter only measures "how many files did we skip", not
/// "which project's files". Resetting it between tests is the caller's
/// responsibility (create a fresh `Engine`).
pub struct IndexSkipCounter {
    count: std::sync::atomic::AtomicU64,
}

impl IndexSkipCounter {
    pub const fn new() -> Self {
        Self { count: std::sync::atomic::AtomicU64::new(0) }
    }

    pub fn increment(&self) {
        self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn load(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_encode_decode_empty() {
        let idx = ColumnIndex::new();
        let bytes = encode_index(&idx);
        let decoded = decode_index(&bytes).unwrap();
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn roundtrip_encode_decode_with_entries() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "alice@example.com".to_string(),
            IndexLocation {
                file_path: "project/tables/users/data/f1.parquet".to_string(),
                row_group: 0,
                row: 42,
            },
        );
        idx.insert(
            "bob@example.com".to_string(),
            IndexLocation {
                file_path: "project/tables/users/data/f2.parquet".to_string(),
                row_group: 1,
                row: 7,
            },
        );
        // Second location for same key
        idx.insert(
            "alice@example.com".to_string(),
            IndexLocation {
                file_path: "project/tables/users/data/f3.parquet".to_string(),
                row_group: 0,
                row: 0,
            },
        );

        let bytes = encode_index(&idx);
        let decoded = decode_index(&bytes).unwrap();
        assert_eq!(decoded.entries.len(), 2);
        let alice_locs = decoded.entries.get("alice@example.com").unwrap();
        assert_eq!(alice_locs.len(), 2);
        let bob_locs = decoded.entries.get("bob@example.com").unwrap();
        assert_eq!(bob_locs.len(), 1);
        assert_eq!(bob_locs[0].row_group, 1);
        assert_eq!(bob_locs[0].row, 7);
    }

    #[test]
    fn probe_returns_none_for_absent_key() {
        let idx = ColumnIndex::new();
        assert!(idx.probe("missing").is_none());
    }

    #[test]
    fn indexed_file_coverage_roundtrips_and_tracks_lifecycle() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "k".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 0 },
        );
        idx.mark_file_indexed("f1.parquet");
        idx.mark_file_indexed("f2.parquet");
        assert!(idx.is_file_indexed("f1.parquet") && idx.is_file_indexed("f2.parquet"));

        // v2 serde preserves the coverage set.
        let decoded = decode_index(&encode_index(&idx)).unwrap();
        assert_eq!(decoded.indexed_files().len(), 2);
        assert!(decoded.is_file_indexed("f1.parquet"));

        // Removing a file drops its coverage.
        let mut idx2 = decoded;
        idx2.remove_file("f1.parquet");
        assert!(!idx2.is_file_indexed("f1.parquet"));
        assert!(idx2.is_file_indexed("f2.parquet"));
    }

    #[test]
    fn v1_blob_decodes_as_uncovered() {
        // A version-1 index blob (no coverage trailer) must decode with an empty
        // coverage set — "trust no prune" until files are re-indexed.
        let mut idx = ColumnIndex::new();
        idx.insert(
            "k".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 0 },
        );
        // Build a v1 blob: same as encode_index but version byte = 1 and no trailer.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&MAGIC);
        v1.push(1u8);
        v1.extend_from_slice(&(idx.entries.len() as u64).to_le_bytes());
        for (key, locs) in &idx.entries {
            v1.extend_from_slice(&(key.len() as u32).to_le_bytes());
            v1.extend_from_slice(key.as_bytes());
            v1.extend_from_slice(&(locs.len() as u32).to_le_bytes());
            for l in locs {
                v1.extend_from_slice(&(l.file_path.len() as u32).to_le_bytes());
                v1.extend_from_slice(l.file_path.as_bytes());
                v1.extend_from_slice(&l.row_group.to_le_bytes());
                v1.extend_from_slice(&l.row.to_le_bytes());
            }
        }
        let decoded = decode_index(&v1).unwrap();
        assert_eq!(decoded.entries.len(), 1);
        assert!(decoded.indexed_files().is_empty(), "v1 must decode as uncovered");
    }

    #[test]
    fn probe_returns_files_for_known_key() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "hello".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 0 },
        );
        let files = idx.probe("hello").unwrap();
        assert_eq!(files, vec!["f1.parquet".to_string()]);
    }

    #[test]
    fn remove_file_cleans_up_entries() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "k1".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 0 },
        );
        idx.insert(
            "k1".to_string(),
            IndexLocation { file_path: "f2.parquet".to_string(), row_group: 0, row: 1 },
        );
        idx.insert(
            "k2".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 2 },
        );
        let removed = idx.remove_file("f1.parquet");
        assert_eq!(removed, 2);
        // k1 should still have f2 entry
        assert_eq!(idx.entries.get("k1").unwrap().len(), 1);
        // k2 should be gone entirely
        assert!(idx.entries.get("k2").is_none());
    }

    #[test]
    fn extract_entries_int64() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![10i64, 20, 30]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let entries = extract_entries_from_batch(&batch, "id", "data/f1.parquet", 0);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, "10");
        assert_eq!(entries[1].0, "20");
        assert_eq!(entries[2].0, "30");
        assert_eq!(entries[0].1.file_path, "data/f1.parquet");
        assert_eq!(entries[0].1.row, 0);
        assert_eq!(entries[1].1.row, 1);
    }

    #[test]
    fn probe_locations_returns_row_groups() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "val".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 2, row: 5 },
        );
        let locs = idx.probe_locations("val").unwrap();
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].file_path, "f1.parquet");
        assert_eq!(locs[0].row_group, 2);
        assert_eq!(locs[0].row, 5);
        assert!(idx.probe_locations("absent").is_none());
    }

    #[test]
    fn probe_locations_groups_by_file() {
        let mut idx = ColumnIndex::new();
        idx.insert(
            "k".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 0, row: 1 },
        );
        idx.insert(
            "k".to_string(),
            IndexLocation { file_path: "f1.parquet".to_string(), row_group: 3, row: 7 },
        );
        idx.insert(
            "k".to_string(),
            IndexLocation { file_path: "f2.parquet".to_string(), row_group: 1, row: 0 },
        );
        let locs = idx.probe_locations("k").unwrap();
        assert_eq!(locs.len(), 3);
        // Collect distinct row groups per file using a simple grouping.
        let mut rg_map: std::collections::HashMap<String, Vec<u32>> =
            std::collections::HashMap::new();
        for l in &locs {
            rg_map.entry(l.file_path.clone()).or_default().push(l.row_group);
        }
        let mut f1_rgs = rg_map["f1.parquet"].clone();
        f1_rgs.sort_unstable();
        assert_eq!(f1_rgs, vec![0, 3]);
        assert_eq!(rg_map["f2.parquet"], vec![1]);
    }

    #[test]
    fn probe_locations_dedups_same_rg() {
        let mut idx = ColumnIndex::new();
        // Two rows in the same (file, row_group) — different rows.
        idx.insert(
            "x".to_string(),
            IndexLocation { file_path: "f.parquet".to_string(), row_group: 0, row: 10 },
        );
        idx.insert(
            "x".to_string(),
            IndexLocation { file_path: "f.parquet".to_string(), row_group: 0, row: 20 },
        );
        let locs = idx.probe_locations("x").unwrap();
        assert_eq!(locs.len(), 2); // Two raw location entries...
        // ...but when deduped by (file, rg) → only one row-group entry.
        let mut rg_set: std::collections::HashSet<(String, u32)> =
            std::collections::HashSet::new();
        for l in &locs {
            rg_set.insert((l.file_path.clone(), l.row_group));
        }
        assert_eq!(rg_set.len(), 1, "same (file, rg) should collapse to one row-group entry");
    }

    #[test]
    fn extract_entries_string() {
        use arrow_array::{RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let arr = StringArray::from(vec![Some("a@b.com"), None, Some("c@d.com")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let entries = extract_entries_from_batch(&batch, "email", "data/f2.parquet", 1);
        assert_eq!(entries.len(), 2); // NULL is skipped
        assert_eq!(entries[0].0, "a@b.com");
        assert_eq!(entries[1].0, "c@d.com");
        assert_eq!(entries[1].1.row_group, 1);
    }
}
