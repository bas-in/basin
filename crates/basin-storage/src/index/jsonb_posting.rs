//! Inv-W5 / W9 — GIN posting-list structure for JSONB `@>` containment.
//!
//! Mirrors [`super::gin_tsvector::GinTsvectorRegistry`] for the JSONB
//! containment operator.  Whereas the existing
//! [`super::gin_rowgroup::GinRowGroupRegistry`] uses a per-row-group bloom
//! filter — which cannot prune a workload where every row-group contains
//! the searched term — this module stores, per `(key, value)` atom, the
//! set of `(file, row_group)` locations the atom was observed in.  At
//! query time the posting lists for the needle's `(key, value)` pairs are
//! AND-merged: only files (and only the row-groups within those files)
//! that contain *every* needle pair survive.
//!
//! # Why a second JSONB posting list
//!
//! The existing JSONB GIN bloom in [`super::gin_rowgroup`] is correct but
//! ineffective for workloads where the searched term is present in every
//! row-group.  The bloom answers "maybe contains" for every row-group, so
//! the read path opens every file in full.  For a uniformly-distributed
//! workload (e.g. category cycling through 3 values, 1M rows) this
//! degrades the prune to a no-op.
//!
//! This module mirrors the FTS tsvector posting-list pattern: instead of
//! summarising each row-group with a bloom, store the *inverted index*
//! — `(key, value) → set of (file, row_group)` locations.  AND-merging
//! across the needle's pairs yields the exact row-group set that must be
//! re-evaluated.
//!
//! # The existing bloom registry is preserved
//!
//! [`super::gin_rowgroup::GinRowGroupRegistry`] continues to serve the
//! `?` (key-exists) family of operators, which the bloom handles well.
//! Only the `@>` (and `<@` reverse) path routes to this new posting list.
//!
//! # Atom extraction
//!
//! A JSONB needle `{"category": "purchase", "region": "us-east-1"}`
//! produces two atoms: `("category", "purchase")` and
//! `("region", "us-east-1")`.  Nested objects flatten into dotted paths:
//! `{"a": {"b": 1}}` → `("a.b", "1")`.  Top-level arrays and scalars
//! produce no atoms (the same convention `gin_extract_terms` follows).
//!
//! # Correctness contract
//!
//! The posting list is a *conservative superset*: it returns exactly the
//! `(file, row_group)` pairs whose stored value had a matching `(key,
//! value)` pair.  Containment semantics (`@>`) requires that *all* needle
//! pairs match — the AND-merge across atoms achieves that file-level
//! AND-intersection (a file is a candidate only when each atom's posting
//! list contains it).  The full `jsonb_contains` predicate is still
//! re-evaluated at the read layer; false positives there are filtered out
//! row-by-row.  No false negatives are possible.
//!
//! # Memory budget
//!
//! Per-file posting count is capped at `DEFAULT_POSTING_BUDGET` entries
//! (env-tunable via `BASIN_GIN_POSTING_BUDGET`, shared with the FTS
//! registry).  On overflow the file is marked un-indexed and falls back
//! to the bloom registry (which is registered separately by the existing
//! GIN code).  Correctness is scale-independent.
//!
//! # Sidecar storage
//!
//! Posting lists serialise to bincode under
//! `projects/{p}/tables/{t}/index/{col}.jsonb_post/{ulid}.bin`.  Loaded
//! lazily at probe time when a file is in the live set but not yet
//! resident in the in-RAM registry (mirrors the rtree lazy-load path).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use basin_common::{ProjectId, TableName};
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Maximum total posting entries (file+rg tuples) kept per `(table, col)`
/// posting list.  Beyond this threshold the file is marked un-indexed and
/// the caller falls back to the bloom registry / full scan.
///
/// This default can be overridden at process start via the
/// `BASIN_GIN_POSTING_BUDGET` environment variable (shared with the FTS
/// tsvector GIN registry).
const DEFAULT_POSTING_BUDGET: usize = 500_000;

fn posting_budget() -> usize {
    use std::sync::OnceLock;
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("BASIN_GIN_POSTING_BUDGET")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_POSTING_BUDGET)
    })
}

// ── Atom + entry types ────────────────────────────────────────────────────────

/// A normalised `(key, value)` atom extracted from a JSONB document.
///
/// `key` is a dotted path (`"category"` / `"meta.region"`); `value` is the
/// compact JSON-serialised form of the leaf scalar (string, number, bool,
/// null) or a truncated repr of a sub-object/array.
pub type JsonbAtom = (String, String);

/// One physical location for a posting entry: file path + row-group.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JsonbPostingEntry {
    pub file_path: String,
    pub row_group: u32,
}

// ── Atom extraction ───────────────────────────────────────────────────────────

/// Extract the `(key, value)` atoms from a JSONB value.
///
/// Top-level keys produce `(k, compact(v))` atoms.  Nested objects flatten
/// into dotted paths: `{"a": {"b": 1}}` → `("a.b", "1")`.  Top-level
/// arrays and scalars produce no atoms (mirrors
/// `gin_extract_terms` for `jsonb_ops`).
///
/// The compact value form matches `gin_compact_value` in `gin_rowgroup.rs`
/// so probe-side atoms generated from the WHERE-clause needle align with
/// index-side atoms generated at write time.
pub fn extract_atoms(value: &serde_json::Value) -> Vec<JsonbAtom> {
    let mut out = Vec::new();
    extract_atoms_inner(value, "", &mut out);
    out
}

fn extract_atoms_inner(value: &serde_json::Value, prefix: &str, out: &mut Vec<JsonbAtom>) {
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let path = if prefix.is_empty() {
                k.clone()
            } else {
                format!("{prefix}.{k}")
            };
            let leaf = compact_value(v);
            out.push((path.clone(), leaf));
            if matches!(v, serde_json::Value::Object(_)) {
                extract_atoms_inner(v, &path, out);
            }
        }
    }
    // Top-level arrays and scalars produce no atoms (consistent with
    // `gin_extract_terms`).
}

/// Extract the *probe-side* atoms of a `@>` needle.
///
/// Unlike [`extract_atoms`] (the index side), every atom returned here must
/// be a **necessary condition** on a containing document — the probe
/// AND-merges them and prunes any file missing one.  Exact-value atoms for
/// object/array needle values are NOT necessary: `{"a":{"x":1,"y":2}}`
/// contains the needle `{"a":{"x":1}}` but its indexed atom is
/// `("a", "{\"x\":1,\"y\":2}")`, not the needle's `("a", "{\"x\":1}")` —
/// AND-merging the latter would prune a file holding a real match (dropped
/// rows).  Likewise array containment is element-subset, so the exact
/// compact repr of an array value is not required to appear.
///
/// Therefore: scalar values → `(path, compact)` atoms; object values →
/// recurse to their scalar leaves; array values → no atom.  An empty result
/// means nothing is provable and the probe must return `NoIndex`.
pub fn needle_atoms(value: &serde_json::Value) -> Vec<JsonbAtom> {
    let mut out = Vec::new();
    needle_atoms_inner(value, "", &mut out);
    out
}

fn needle_atoms_inner(value: &serde_json::Value, prefix: &str, out: &mut Vec<JsonbAtom>) {
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let path = if prefix.is_empty() {
                k.clone()
            } else {
                format!("{prefix}.{k}")
            };
            match v {
                serde_json::Value::Object(_) => needle_atoms_inner(v, &path, out),
                serde_json::Value::Array(_) => {
                    // Exact array repr is not a necessary condition — skip.
                }
                _ => out.push((path, compact_value(v))),
            }
        }
    }
}

/// Compact JSON repr for a leaf value, truncated to 200 chars for
/// composite values.  Mirrors `gin_compact_value` so probe/index atoms
/// match.
fn compact_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = v.to_string();
            if s.len() > 200 {
                s[..200].to_string()
            } else {
                s
            }
        }
    }
}

// ── Per-column posting list ───────────────────────────────────────────────────

/// In-memory posting list: `atom → set of (file, row_group)`.
#[derive(Debug, Default)]
struct AtomPostingList {
    entries: HashMap<JsonbAtom, HashSet<JsonbPostingEntry>>,
    total_count: usize,
    /// Files that overflowed the budget at any point and are therefore not
    /// fully covered by this registry.  Probes for these files fall back to
    /// the bloom registry.
    over_budget_files: HashSet<String>,
}

impl AtomPostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Add an atom occurrence at `(file, row_group)`.
    /// Returns `true` when the budget would be exceeded by this insert; the
    /// caller marks the file as over-budget so probes never use partial
    /// coverage.
    fn insert(&mut self, atom: JsonbAtom, entry: JsonbPostingEntry) -> bool {
        if self.total_count >= posting_budget() {
            self.over_budget_files.insert(entry.file_path.clone());
            return true;
        }
        let set = self.entries.entry(atom).or_default();
        if set.insert(entry) {
            self.total_count += 1;
        }
        false
    }

    fn probe_atom(&self, atom: &JsonbAtom) -> Option<&HashSet<JsonbPostingEntry>> {
        self.entries.get(atom)
    }

    fn remove_file(&mut self, file_path: &str) {
        for set in self.entries.values_mut() {
            let before = set.len();
            set.retain(|e| e.file_path != file_path);
            self.total_count = self.total_count.saturating_sub(before - set.len());
        }
        self.over_budget_files.remove(file_path);
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide JSONB containment posting-list registry.
///
/// One `AtomPostingList` per `(project, table, col)`.  Mirrors the
/// concurrency layout of [`super::gin_tsvector::GinTsvectorRegistry`]: an
/// outer mutex over the registry map plus a per-column mutex over the
/// posting list itself.
pub struct JsonbPostingRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<AtomPostingList>>>>,
    indexed_files: Mutex<HashMap<RegKey, HashSet<String>>>,
}

impl JsonbPostingRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            indexed_files: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<AtomPostingList>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let mut map = self
            .inner
            .lock()
            .expect("JsonbPostingRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(AtomPostingList::new())))
            .clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<AtomPostingList>>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let map = self
            .inner
            .lock()
            .expect("JsonbPostingRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index one row of JSONB data.  `atoms` are the `(key, value)` pairs
    /// produced by [`extract_atoms`].  Empty `atoms` is a no-op.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        atoms: &[JsonbAtom],
        file_path: &str,
        row_group: u32,
    ) {
        if atoms.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("AtomPostingList lock poisoned");
        let entry = JsonbPostingEntry {
            file_path: file_path.to_string(),
            row_group,
        };
        for atom in atoms {
            if list.insert(atom.clone(), entry.clone()) {
                // Budget exhausted: the file is now over-budget.  Subsequent
                // probes for this file return Unknown so we never serve
                // partial coverage.
                break;
            }
        }
    }

    /// Build a posting list from a full RecordBatch.  Iterates all rows in
    /// `col_idx` (assumed `LargeBinary` JSONB), parses each value, extracts
    /// atoms, and indexes them at the row's row-group.  Marks the file as
    /// fully indexed at the end.
    pub fn index_batch(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        batch: &RecordBatch,
        col_idx: usize,
        row_group_size: usize,
        file_path: &str,
    ) {
        use arrow_array::Array;
        let col_arr = batch.column(col_idx);
        let Some(arr) = col_arr
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
        else {
            return;
        };
        let rg_size = row_group_size.max(1);
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            // The stored JSONB cell may have a 1-byte version tag (0x01 PG
            // wire prefix, 0x02 ADR-0027 v2 offset table).  Strip it before
            // parsing so the atom extraction sees pure JSON.
            let json_bytes = strip_jsonb_version_tag(bytes);
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(json_bytes) else {
                continue;
            };
            let atoms = extract_atoms(&value);
            if atoms.is_empty() {
                continue;
            }
            let row_group = (row / rg_size) as u32;
            self.index_row(project, table, col, &atoms, file_path, row_group);
        }
        self.mark_file_indexed(project, table, col, file_path);
    }

    /// Probe the registry for a JSONB containment needle.
    ///
    /// The `needle`'s atoms are AND-merged at file granularity: a file is a
    /// candidate only when every atom's posting list contains it.  For
    /// surviving files, the row-groups are unioned across atoms (a
    /// row-group is a candidate when any atom touches it — the row-level
    /// re-evaluation pass then filters false positives).  The union
    /// strategy mirrors `GinTsvectorRegistry::probe_query_with_row_groups`.
    ///
    /// Returns:
    /// * [`JsonbProbeResult::NoIndex`] — no posting list, or the needle has
    ///   no provable atoms (caller must full-scan).
    /// * [`JsonbProbeResult::AtomAbsent`] — a posting list exists but some
    ///   needle atom was never indexed: no *fully-covered* file can match.
    ///   Files with partial coverage (over-budget / un-ingested) must still
    ///   be scanned by the caller.
    /// * [`JsonbProbeResult::Empty`] — the AND-intersection is empty.
    /// * [`JsonbProbeResult::FileRowGroups`] — `file → sorted row-groups`.
    ///
    /// Probe atoms come from [`needle_atoms`] (scalar leaves only): every
    /// atom is a necessary condition on a containing document, so the
    /// AND-merge can never prune a file holding a real match.
    pub fn probe(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        needle: &serde_json::Value,
    ) -> JsonbProbeResult {
        let atoms = needle_atoms(needle);
        if atoms.is_empty() {
            // Empty needle (`{}`) matches every row — nothing to prune on.
            return JsonbProbeResult::NoIndex;
        }
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return JsonbProbeResult::NoIndex,
        };
        let list = arc.lock().expect("AtomPostingList lock poisoned");

        // Per-atom: file -> set of row-groups.  Then file-level
        // AND-intersection with per-file row-group union.
        let mut per_atom: Vec<HashMap<String, HashSet<u32>>> = Vec::with_capacity(atoms.len());
        for atom in &atoms {
            match list.probe_atom(atom) {
                None => {
                    // Atom never indexed: no fully-covered file contains it.
                    return JsonbProbeResult::AtomAbsent;
                }
                Some(entries) => {
                    let mut by_file: HashMap<String, HashSet<u32>> = HashMap::new();
                    for e in entries {
                        by_file
                            .entry(e.file_path.clone())
                            .or_default()
                            .insert(e.row_group);
                    }
                    per_atom.push(by_file);
                }
            }
        }

        let mut iter = per_atom.into_iter();
        let first = match iter.next() {
            Some(m) => m,
            None => return JsonbProbeResult::NoIndex,
        };
        let mut merged: HashMap<String, HashSet<u32>> = first;
        for next in iter {
            let kept_keys: Vec<String> = merged
                .keys()
                .filter(|k| next.contains_key(*k))
                .cloned()
                .collect();
            let mut new_merged: HashMap<String, HashSet<u32>> =
                HashMap::with_capacity(kept_keys.len());
            for k in kept_keys {
                let mut rgs = merged.remove(&k).unwrap_or_default();
                if let Some(other) = next.get(&k) {
                    rgs.extend(other.iter().copied());
                }
                new_merged.insert(k, rgs);
            }
            merged = new_merged;
        }

        // Skip files that overflowed the budget — partial coverage is unsafe.
        for over in list.over_budget_files.iter() {
            merged.remove(over);
        }

        if merged.is_empty() {
            return JsonbProbeResult::Empty;
        }

        let out: HashMap<String, Vec<u32>> = merged
            .into_iter()
            .map(|(file, rgs)| {
                let mut v: Vec<u32> = rgs.into_iter().collect();
                v.sort_unstable();
                (file, v)
            })
            .collect();
        JsonbProbeResult::FileRowGroups(out)
    }

    pub fn remove_file(&self, project: &ProjectId, table: &TableName, col: &str, file_path: &str) {
        if let Some(arc) = self.get(project, table, col) {
            let mut list = arc.lock().expect("AtomPostingList lock poisoned");
            list.remove_file(file_path);
        }
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        if let Ok(mut map) = self.indexed_files.lock() {
            if let Some(set) = map.get_mut(&key) {
                set.remove(file_path);
            }
        }
    }

    pub fn mark_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        if let Ok(mut map) = self.indexed_files.lock() {
            map.entry(key).or_default().insert(file_path.to_string());
        }
    }

    pub fn is_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) -> bool {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        if let Ok(map) = self.indexed_files.lock() {
            map.get(&key).is_some_and(|s| s.contains(file_path))
        } else {
            false
        }
    }

    /// Files whose ingest overflowed the posting budget and therefore have
    /// only PARTIAL atom coverage.  `index_batch` / `ingest_sidecar` still
    /// mark these files "indexed" (so the lazy sidecar loader does not
    /// re-fetch them every query), which means callers MUST subtract this set
    /// before treating a file's absence from a probe result as proof of
    /// no-match: a partially-covered file can hold a matching row whose atoms
    /// were never inserted.  Treating an over-budget file as prunable drops
    /// rows.
    pub fn over_budget_files_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> HashSet<String> {
        match self.get(project, table, col) {
            None => HashSet::new(),
            Some(arc) => arc
                .lock()
                .expect("AtomPostingList lock poisoned")
                .over_budget_files
                .clone(),
        }
    }

    pub fn indexed_files_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> HashSet<String> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        if let Ok(map) = self.indexed_files.lock() {
            map.get(&key).cloned().unwrap_or_default()
        } else {
            HashSet::new()
        }
    }

    /// Total posting entries for `(project, table, col)`.  Primarily for
    /// tests and diagnostics.
    pub fn total_entries(&self, project: &ProjectId, table: &TableName, col: &str) -> usize {
        match self.get(project, table, col) {
            None => 0,
            Some(arc) => {
                arc.lock()
                    .expect("AtomPostingList lock poisoned")
                    .total_count
            }
        }
    }

    // ── Sidecar serialisation (bincode) ───────────────────────────────────

    /// Serialise the file's posting entries (atom → entries) as a flat
    /// `Vec<(atom, file, rg)>` for sidecar storage.  Returns `None` when
    /// the file is over-budget or not indexed.
    pub fn serialize_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) -> Option<Vec<u8>> {
        let arc = self.get(project, table, col)?;
        let list = arc.lock().expect("AtomPostingList lock poisoned");
        if list.over_budget_files.contains(file_path) {
            return None;
        }
        let mut out: Vec<(JsonbAtom, JsonbPostingEntry)> = Vec::new();
        for (atom, entries) in list.entries.iter() {
            for e in entries.iter() {
                if e.file_path == file_path {
                    out.push((atom.clone(), e.clone()));
                }
            }
        }
        let payload = SidecarPayload {
            magic: SIDECAR_MAGIC,
            version: 1,
            entries: out,
        };
        bincode::serialize(&payload).ok()
    }

    /// Deserialise sidecar bytes and merge entries into the registry.
    pub fn ingest_sidecar(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        bytes: &[u8],
    ) -> bool {
        let payload: SidecarPayload = match bincode::deserialize(bytes) {
            Ok(p) => p,
            Err(_) => return false,
        };
        if payload.magic != SIDECAR_MAGIC {
            return false;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("AtomPostingList lock poisoned");
        let mut last_file: Option<String> = None;
        for (atom, entry) in payload.entries {
            last_file = Some(entry.file_path.clone());
            let _overflow = list.insert(atom, entry);
            // If we overflow during sidecar ingest, the file gets marked
            // over-budget by `insert`; we keep going so other files in the
            // same registry stay coherent.
        }
        drop(list);
        if let Some(f) = last_file {
            self.mark_file_indexed(project, table, col, &f);
        }
        true
    }
}

impl Default for JsonbPostingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Sidecar wire format ───────────────────────────────────────────────────────

const SIDECAR_MAGIC: u32 = 0x4A4F_5031; // "JOP1"

#[derive(Serialize, Deserialize)]
struct SidecarPayload {
    magic: u32,
    version: u32,
    entries: Vec<(JsonbAtom, JsonbPostingEntry)>,
}

/// Build the prefix at which all JSONB posting-list sidecars for a given
/// `(project, table, column)` live.
pub fn column_posting_prefix(project: &ProjectId, table: &TableName, column: &str) -> ObjectPath {
    let mut p = ObjectPath::from(crate::paths::PROJECTS_SEGMENT);
    p = p.child(project.as_prefix());
    p = p.child("tables");
    p = p.child(table.as_str());
    p = p.child("index");
    p.child(format!("{column}.jsonb_post"))
}

/// Given a data file path under the canonical
/// `projects/{p}/tables/{t}/.../{ulid}.{parquet,vortex}` layout, return
/// the `.bin` sidecar path under `index/{column}.jsonb_post/{ulid}.bin`.
/// Returns `None` for non-canonical paths.
pub fn posting_sidecar_key_for_data_file(
    project: &ProjectId,
    table: &TableName,
    column: &str,
    data_file_path: &str,
) -> Option<ObjectPath> {
    let last = data_file_path.rsplit('/').next()?;
    let stem = last
        .strip_suffix(".parquet")
        .or_else(|| last.strip_suffix(".vortex"))?;
    let _ulid: Ulid = stem.parse().ok()?;
    Some(column_posting_prefix(project, table, column).child(format!("{stem}.bin")))
}

// ── Probe result ──────────────────────────────────────────────────────────────

/// Result of a JSONB `@>` probe against the posting list.
#[derive(Debug)]
pub enum JsonbProbeResult {
    /// No posting list, or the needle has no provable atoms.  Caller must
    /// fall through to a full scan (no false negatives).
    NoIndex,
    /// A posting list exists but some needle atom was never indexed.  No
    /// FULLY-COVERED file can match (an indexed+within-budget file has every
    /// atom of every row inserted).  Files with partial coverage — see
    /// [`JsonbPostingRegistry::over_budget_files_for`] — or files never
    /// ingested must still be scanned.
    AtomAbsent,
    /// AND-intersection is empty — no rows in fully-covered files can match.
    Empty,
    /// `file_path → sorted row-group indices`.  Files absent from this map
    /// are either provably empty for the needle (fully-covered files) or
    /// unknown (partially-covered / un-ingested files) — the CALLER must
    /// distinguish via the indexed/over-budget sets and read unknown files
    /// in full (no false negatives).
    FileRowGroups(HashMap<String, Vec<u32>>),
}

// ── JSONB version-tag stripping ───────────────────────────────────────────────

/// Strip the JSONB cell's version tag prefix so atom extraction sees pure
/// JSON bytes.  Mirrors the stripping done by the engine's
/// `jsonb_contains` UDF and the test harness's `extract_json_payload`.
fn strip_jsonb_version_tag(stored: &[u8]) -> &[u8] {
    match stored.first() {
        Some(&0x01) if stored.len() > 1 => &stored[1..],
        Some(&0x02) => {
            // ADR 0027 Phase 2 binary offset table:
            // [0x02][u32_le num_entries][entries...][json_bytes]
            // Entry: [u16_le key_len][key_bytes][u32_le val_start][u32_le val_end]
            if stored.len() < 5 {
                return stored;
            }
            let num_entries =
                u32::from_le_bytes(stored[1..5].try_into().unwrap_or([0; 4])) as usize;
            let mut p = 5usize;
            for _ in 0..num_entries {
                if p + 2 > stored.len() {
                    return stored;
                }
                let kl = u16::from_le_bytes(stored[p..p + 2].try_into().unwrap_or([0; 2])) as usize;
                p += 2 + kl + 4 + 4;
                if p > stored.len() {
                    return stored;
                }
            }
            if p <= stored.len() {
                &stored[p..]
            } else {
                stored
            }
        }
        _ => stored,
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_atoms_flat_object() {
        let v = json!({"category": "purchase", "region": "us-east-1"});
        let atoms: HashSet<JsonbAtom> = extract_atoms(&v).into_iter().collect();
        assert!(atoms.contains(&("category".to_string(), "\"purchase\"".to_string())));
        assert!(atoms.contains(&("region".to_string(), "\"us-east-1\"".to_string())));
        assert_eq!(atoms.len(), 2);
    }

    #[test]
    fn extract_atoms_nested_object() {
        let v = json!({"meta": {"region": "eu"}});
        let atoms: HashSet<JsonbAtom> = extract_atoms(&v).into_iter().collect();
        // We get both the parent placeholder and the dotted leaf.
        assert!(atoms.contains(&("meta.region".to_string(), "\"eu\"".to_string())));
    }

    #[test]
    fn extract_atoms_numeric_and_bool() {
        let v = json!({"n": 42, "b": true});
        let atoms: HashSet<JsonbAtom> = extract_atoms(&v).into_iter().collect();
        assert!(atoms.contains(&("n".to_string(), "42".to_string())));
        assert!(atoms.contains(&("b".to_string(), "true".to_string())));
    }

    #[test]
    fn extract_atoms_top_level_array_empty() {
        let v = json!([1, 2, 3]);
        let atoms = extract_atoms(&v);
        assert!(atoms.is_empty());
    }

    #[test]
    fn probe_no_index_empty_registry() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let needle = json!({"category": "purchase"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::NoIndex => {}
            other => panic!("expected NoIndex, got {other:?}"),
        }
    }

    #[test]
    fn probe_single_atom_hit() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let atoms = extract_atoms(&json!({"category": "purchase"}));
        reg.index_row(&proj, &tbl, "payload", &atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");

        let needle = json!({"category": "purchase"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::FileRowGroups(m) => {
                let rgs = m.get("f1.parquet").expect("f1 missing");
                assert_eq!(rgs, &vec![0u32]);
            }
            other => panic!("expected FileRowGroups, got {other:?}"),
        }
    }

    #[test]
    fn probe_and_merge_excludes_partial_files() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        // f1 has both pairs.
        let f1_atoms = extract_atoms(&json!({"category": "purchase", "region": "us-east-1"}));
        reg.index_row(&proj, &tbl, "payload", &f1_atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");
        // f2 has only category.
        let f2_atoms = extract_atoms(&json!({"category": "purchase"}));
        reg.index_row(&proj, &tbl, "payload", &f2_atoms, "f2.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f2.parquet");

        let needle = json!({"category": "purchase", "region": "us-east-1"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::FileRowGroups(m) => {
                assert!(m.contains_key("f1.parquet"), "f1 must be candidate: {m:?}");
                assert!(!m.contains_key("f2.parquet"), "f2 must be excluded: {m:?}");
            }
            other => panic!("expected FileRowGroups, got {other:?}"),
        }
    }

    #[test]
    fn probe_row_group_union_per_file() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        // f1: category in rg 0; region in rg 1.  Need both → union {0, 1}.
        reg.index_row(
            &proj,
            &tbl,
            "payload",
            &extract_atoms(&json!({"category": "purchase"})),
            "f1.parquet",
            0,
        );
        reg.index_row(
            &proj,
            &tbl,
            "payload",
            &extract_atoms(&json!({"region": "us-east-1"})),
            "f1.parquet",
            1,
        );
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");

        let needle = json!({"category": "purchase", "region": "us-east-1"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::FileRowGroups(m) => {
                let rgs = m.get("f1.parquet").expect("f1 missing");
                assert_eq!(
                    rgs,
                    &vec![0u32, 1u32],
                    "row-groups should be union: {rgs:?}"
                );
            }
            other => panic!("expected FileRowGroups, got {other:?}"),
        }
    }

    #[test]
    fn probe_empty_when_no_common_file() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        reg.index_row(
            &proj,
            &tbl,
            "payload",
            &extract_atoms(&json!({"category": "purchase"})),
            "f1.parquet",
            0,
        );
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");
        reg.index_row(
            &proj,
            &tbl,
            "payload",
            &extract_atoms(&json!({"region": "us-east-1"})),
            "f2.parquet",
            0,
        );
        reg.mark_file_indexed(&proj, &tbl, "payload", "f2.parquet");

        let needle = json!({"category": "purchase", "region": "us-east-1"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::Empty => {}
            other => panic!("expected Empty, got {other:?}"),
        }
    }

    #[test]
    fn remove_file_drops_entries() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let atoms = extract_atoms(&json!({"category": "purchase"}));
        reg.index_row(&proj, &tbl, "payload", &atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");
        reg.remove_file(&proj, &tbl, "payload", "f1.parquet");

        let needle = json!({"category": "purchase"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::NoIndex | JsonbProbeResult::Empty => {}
            other => panic!("expected NoIndex or Empty after remove, got {other:?}"),
        }
    }

    #[test]
    fn sidecar_round_trip() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let atoms = extract_atoms(&json!({"category": "purchase", "region": "us-east-1"}));
        reg.index_row(&proj, &tbl, "payload", &atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");

        let bytes = reg
            .serialize_file(&proj, &tbl, "payload", "f1.parquet")
            .expect("serialize");
        assert!(!bytes.is_empty());

        let reg2 = JsonbPostingRegistry::new();
        let ok = reg2.ingest_sidecar(&proj, &tbl, "payload", &bytes);
        assert!(ok);

        // Probe the rehydrated registry — same shape result.
        let needle = json!({"category": "purchase", "region": "us-east-1"});
        match reg2.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::FileRowGroups(m) => {
                assert!(m.contains_key("f1.parquet"));
            }
            other => panic!("expected FileRowGroups after sidecar load, got {other:?}"),
        }
    }

    #[test]
    fn needle_atoms_nested_object_uses_scalar_leaves_only() {
        // Doc {"a":{"x":1,"y":2}} CONTAINS needle {"a":{"x":1}} (recursive
        // subset).  The probe must not require the exact ("a","{\"x\":1}")
        // atom — only the scalar leaf ("a.x","1").
        let needle = json!({"a": {"x": 1}});
        let atoms = needle_atoms(&needle);
        assert_eq!(atoms, vec![("a.x".to_string(), "1".to_string())]);

        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let doc_atoms = extract_atoms(&json!({"a": {"x": 1, "y": 2}}));
        reg.index_row(&proj, &tbl, "payload", &doc_atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");

        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::FileRowGroups(m) => {
                assert!(
                    m.contains_key("f1.parquet"),
                    "superset doc must stay a candidate: {m:?}"
                );
            }
            other => panic!("expected FileRowGroups, got {other:?}"),
        }
    }

    #[test]
    fn needle_atoms_array_value_yields_no_atom() {
        // Array containment is element-subset; the exact compact repr is not
        // a necessary condition.  No provable atom → probe must NoIndex.
        let needle = json!({"tags": [1, 2]});
        assert!(needle_atoms(&needle).is_empty());

        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let doc_atoms = extract_atoms(&json!({"tags": [1, 2, 3]}));
        reg.index_row(&proj, &tbl, "payload", &doc_atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::NoIndex => {}
            other => panic!("expected NoIndex (full scan), got {other:?}"),
        }
    }

    #[test]
    fn probe_absent_atom_reports_atom_absent() {
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let atoms = extract_atoms(&json!({"category": "purchase"}));
        reg.index_row(&proj, &tbl, "payload", &atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");

        let needle = json!({"category": "refund"});
        match reg.probe(&proj, &tbl, "payload", &needle) {
            JsonbProbeResult::AtomAbsent => {}
            other => panic!("expected AtomAbsent, got {other:?}"),
        }
    }

    #[test]
    fn over_budget_file_is_reported_and_still_marked_indexed() {
        // The env-cached budget cannot be shrunk per test, so drive the
        // internal list directly: simulate the overflow marking.
        let reg = JsonbPostingRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let atoms = extract_atoms(&json!({"category": "purchase"}));
        reg.index_row(&proj, &tbl, "payload", &atoms, "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "payload", "f1.parquet");
        {
            let arc = reg.get(&proj, &tbl, "payload").unwrap();
            arc.lock()
                .unwrap()
                .over_budget_files
                .insert("f1.parquet".to_string());
        }
        // The caller-facing contract: indexed_files still contains the file
        // (no sidecar re-fetch), but over_budget_files_for reports it so
        // prune consumers force-scan it.
        assert!(reg.is_file_indexed(&proj, &tbl, "payload", "f1.parquet"));
        let over = reg.over_budget_files_for(&proj, &tbl, "payload");
        assert!(over.contains("f1.parquet"), "over-budget set: {over:?}");
    }

    #[test]
    fn sidecar_key_helpers() {
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let ulid = Ulid::new();
        let file = format!(
            "projects/{}/tables/docs/part=a/{ulid}.parquet",
            proj.as_prefix()
        );
        let key = posting_sidecar_key_for_data_file(&proj, &tbl, "payload", &file)
            .expect("key derivation");
        let s = key.to_string();
        assert!(s.contains("index/payload.jsonb_post"), "got {s}");
        assert!(s.ends_with(".bin"), "got {s}");
    }
}
