//! Phase 5.19.C — GIN containment probe for JSONB columns.
//!
//! Builds and queries in-memory posting lists for JSONB columns that have a
//! `CREATE INDEX … USING gin` declaration in the catalog.  The posting lists
//! enable file-level pruning for containment predicates (`@>`, `<@`) instead
//! of always falling through to a full DataFusion scan.
//!
//! # Terminology
//!
//! * **term** — a unit of indexable content extracted from a JSONB document.
//!   For `jsonb_ops` this is every top-level `"key"` and `"key"="value"` pair
//!   in the document.  For `jsonb_path_ops` it is a hash of each root-to-leaf
//!   path (`"a.b.c"=<value>`).  Both opclasses produce `String` terms that map
//!   into the same posting-list structure.
//!
//! * **posting list** — a sorted set of `PostingEntry` values (file path +
//!   row-group + row) for each distinct term.  AND-merging two posting lists
//!   yields the rows that contain BOTH terms — the correct semantics for
//!   compound containment (`{"a":1,"b":2}` must have both term "a" and term
//!   "b" indexed).
//!
//! # Correctness contract
//!
//! The posting list is a *conservative superset*: it may return false
//! positives (rows that don't actually contain the probe document) due to
//! JSONB structural ambiguity (array vs. scalar containment, nested object
//! paths).  Every candidate row returned by a probe is re-evaluated by the
//! `jsonb_contains` / `jsonb_contained_by` UDF at the storage read layer.
//! The posting list only prunes *files* (and future: row-groups) that contain
//! NO matching terms — an empty intersection guarantees absence.
//!
//! # Storage / eviction
//!
//! The posting list lives entirely in RAM; there is no on-disk serialisation
//! in this phase (5.19.E handles persistence).  The registry caps each
//! per-column posting list at [`DEFAULT_POSTING_BUDGET`] total entries (operator-
//! tunable via `BASIN_GIN_POSTING_BUDGET`); oldest terms are evicted in 25%
//! batches when the cap is exceeded.  On an engine restart the registry starts
//! empty and rebuilds lazily from writes.
//!
//! **Per-file completeness (Phase 5.19.C+):** eviction marks only the
//! *affected files* (those whose terms were dropped) as un-indexed.  Files
//! that still have all their terms in the posting list remain fully indexed and
//! continue to benefit from file-level pruning.  Un-indexed files are treated
//! as forced candidates (must-scan) — correctness is never compromised.  This
//! design degrades gracefully at scale: a large table prunes the majority of
//! files that are indexed; correctness is scale-independent.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};
use serde_json::Value;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Maximum total posting entries (file+rg+row tuples) kept per `(table, col)`
/// posting list.  Beyond this threshold the oldest 25% of terms are evicted.
///
/// This default can be overridden at process start via the
/// `BASIN_GIN_POSTING_BUDGET` environment variable.  Example:
/// ```text
/// BASIN_GIN_POSTING_BUDGET=2000000 basin-server
/// ```
/// Operators should raise this for hot, large tables where the 500k default
/// causes excessive eviction.  However, even without raising the budget,
/// **per-file completeness** (see [`GinIndexRegistry`]) ensures that eviction
/// only de-indexes the files whose terms were evicted, not the entire column.
const DEFAULT_POSTING_BUDGET: usize = 500_000;

/// Return the effective per-column posting-entry budget.
///
/// Reads `BASIN_GIN_POSTING_BUDGET` once and caches the result.
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

// ── Data types ────────────────────────────────────────────────────────────────

/// One physical location for a posting entry: file path + row-group + row.
/// Deliberately mirrors `secondary_index::IndexLocation` to allow future
/// sharing, but kept separate so the two registries can evolve independently.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PostingEntry {
    pub file_path: String,
    pub row_group: u32,
    pub row: u64,
}

/// One posting list for a single `(term)`.
/// Maps each term → set of `PostingEntry`.
#[derive(Debug, Default)]
struct TermPostingList {
    /// `term → set of (file, rg, row)`.
    entries: HashMap<String, HashSet<PostingEntry>>,
    /// Ordered sequence of insertions for LRU eviction (keys, not entries).
    insert_order: Vec<String>,
    /// Total entry count (sum of all set sizes).
    total_count: usize,
}

impl TermPostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Add a single `(term, entry)` pair.
    ///
    /// Returns the set of file paths whose posting entries were **evicted** by
    /// this insert (empty when no eviction occurred).  The caller
    /// (`GinIndexRegistry::index_row`) uses this set to mark *only* the
    /// affected files as un-indexed in the per-file completeness map — leaving
    /// files that still have complete posting coverage prunable.  This is the
    /// key difference from the old global-wipe approach: eviction is now
    /// file-scoped, not column-scoped.
    fn insert(&mut self, term: String, entry: PostingEntry) -> HashSet<String> {
        let set = self.entries.entry(term.clone()).or_default();
        if set.is_empty() {
            self.insert_order.push(term);
        }
        set.insert(entry);
        self.total_count += 1;

        if self.total_count > posting_budget() {
            self.evict_oldest()
        } else {
            HashSet::new()
        }
    }

    /// Remove the oldest 25% of terms.
    ///
    /// Returns the set of file paths that were referenced by the evicted
    /// posting entries.  These files must be marked un-indexed by the caller
    /// because at least one of their terms is no longer in the posting list.
    fn evict_oldest(&mut self) -> HashSet<String> {
        let evict_count = (self.insert_order.len() / 4).max(1);
        let to_evict: Vec<String> =
            self.insert_order.drain(..evict_count.min(self.insert_order.len())).collect();
        let mut evicted_files: HashSet<String> = HashSet::new();
        for k in &to_evict {
            if let Some(set) = self.entries.remove(k) {
                for e in &set {
                    evicted_files.insert(e.file_path.clone());
                }
                self.total_count = self.total_count.saturating_sub(set.len());
            }
        }
        evicted_files
    }

    /// Probe for `term`. Returns `None` when the term has never been indexed
    /// (caller must treat as "unknown → full scan").  Returns `Some(set)` for
    /// a known term; the set may be empty when all posting entries for this
    /// term were evicted.
    fn probe_term(&self, term: &str) -> Option<&HashSet<PostingEntry>> {
        self.entries.get(term)
    }

    /// Remove all entries that reference `file_path`. Called when a file is
    /// compacted or deleted.
    fn remove_file(&mut self, file_path: &str) {
        for set in self.entries.values_mut() {
            let before = set.len();
            set.retain(|e| e.file_path != file_path);
            self.total_count = self.total_count.saturating_sub(before - set.len());
        }
    }
}

// ── Term extraction ───────────────────────────────────────────────────────────

/// Extract GIN terms from a JSONB value.
///
/// Two opclass modes are supported:
/// * `jsonb_ops` (default): top-level keys (`"k"`) AND key=value pairs
///   (`"k"="v"`).  For nested objects the sub-document itself becomes a
///   top-level term value (matching PG's `jsonb_ops` GIN extraction behaviour
///   for the case relevant to `@>` / `<@`).
/// * `jsonb_path_ops`: each root-to-leaf path is hashed into a stable string
///   `"path_hash:<hash>"` so the probe can still AND-merge posting lists
///   without storing full path strings.
///
/// Only top-level key / key=scalar-value pairs are extracted; array elements
/// inside values are not decomposed further (conservative: may produce false
/// positives for deep nesting, which the re-evaluation filter will catch).
pub fn extract_terms(value: &Value, opclass: &str) -> Vec<String> {
    let mut terms = Vec::new();
    extract_terms_inner(value, opclass, "", &mut terms);
    terms
}

fn extract_terms_inner(value: &Value, opclass: &str, path_prefix: &str, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let path = if path_prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{path_prefix}.{k}")
                };

                if opclass == "jsonb_path_ops" {
                    // Path-hash term: stable hash of the full root-to-leaf path + value.
                    let leaf_repr = compact_value(v);
                    let hash_input = format!("{path}={leaf_repr}");
                    let hash = simple_hash(&hash_input);
                    out.push(format!("path_hash:{hash}"));

                    // Recurse into nested objects so compound paths are indexed.
                    if matches!(v, Value::Object(_)) {
                        extract_terms_inner(v, opclass, &path, out);
                    }
                } else {
                    // jsonb_ops: key presence term.
                    out.push(format!("key:{k}"));

                    // key=scalar-value term.
                    let leaf_repr = compact_value(v);
                    out.push(format!("kv:{k}={leaf_repr}"));

                    // Recurse for nested objects (adds more key/kv terms under the
                    // same flat namespace, matching PG's @> semantics for nested
                    // object containment).
                    if matches!(v, Value::Object(_)) {
                        extract_terms_inner(v, opclass, &path, out);
                    }
                }
            }
        }
        // Top-level arrays and scalars produce no terms (containment for
        // arrays is handled by re-evaluation; scalars are not `@>` targets).
        _ => {}
    }
}

/// Compact JSON representation used as part of the GIN term key.
fn compact_value(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{s}\""),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => {
            // For nested complex values use a truncated JSON repr as the term.
            let s = v.to_string();
            if s.len() > 200 { s[..200].to_string() } else { s }
        }
    }
}

/// Non-cryptographic hash of a string (FNV-1a, 64-bit).
fn simple_hash(s: &str) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Extract GIN probe terms from a JSONB *needle* document for a `@>` query.
///
/// For `jsonb_ops`:   every `key:k` and `kv:k=v` term in the needle.
/// For `jsonb_path_ops`: every `path_hash:<h>` term in the needle.
///
/// This mirrors `extract_terms` — the probe terms must use the same key space
/// as the indexed terms so the AND-merge works correctly.
pub fn needle_terms(needle: &Value, opclass: &str) -> Vec<String> {
    // For containment needle, we use the same extraction logic as for indexed docs.
    // This is correct: if the needle has `{"tag":"nested"}`, the indexed doc must
    // have both `key:tag` and `kv:tag="nested"` in its posting list.
    extract_terms(needle, opclass)
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Key into the registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide GIN posting list registry.
///
/// One `TermPostingList` per `(project, table, col)`.  Concurrent access is
/// serialised by an inner `Mutex` per posting list, with the outer `Mutex`
/// only held briefly to look up or insert an `Arc`.
///
/// `indexed_files` tracks, for each `(project, table, col)`, the set of file
/// paths whose rows have been fully loaded into the posting list.  This is the
/// completeness guard required by Phase 5.19.C: file-level pruning is only
/// safe when every live data file appears in this set.
pub struct GinIndexRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<TermPostingList>>>>,
    /// File-completeness tracking: `RegKey → set of fully-indexed file paths`.
    indexed_files: Mutex<HashMap<RegKey, HashSet<String>>>,
}

impl GinIndexRegistry {
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
    ) -> Arc<Mutex<TermPostingList>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("GinIndexRegistry outer lock poisoned");
        map.entry(key).or_insert_with(|| Arc::new(Mutex::new(TermPostingList::new()))).clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<TermPostingList>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let map = self.inner.lock().expect("GinIndexRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index a JSONB value from `file_path` / `row_group` / `row`.
    ///
    /// Parses the raw JSONB bytes (`LargeBinary` payload) into a
    /// `serde_json::Value` and extracts GIN terms.  Silently skips null or
    /// unparseable values.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        jsonb_bytes: &[u8],
        file_path: &str,
        row_group: u32,
        row: u64,
    ) {
        let value: Value = match serde_json::from_slice(jsonb_bytes) {
            Ok(v) => v,
            Err(_) => return,
        };
        let terms = extract_terms(&value, opclass);
        if terms.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("TermPostingList lock poisoned");
        let entry = PostingEntry {
            file_path: file_path.to_string(),
            row_group,
            row,
        };
        let mut all_evicted_files: HashSet<String> = HashSet::new();
        for term in terms {
            let evicted_files = list.insert(term, entry.clone());
            all_evicted_files.extend(evicted_files);
        }
        // Per-file completeness: if any terms were evicted, only the files
        // whose posting entries were dropped need to be de-indexed.  Files
        // that still have complete posting coverage remain prunable.  This
        // degrades gracefully: a large table prunes the files that are
        // indexed, and treats any evicted-file as a forced full-file scan
        // (must-scan), which is safe (no false negatives).
        if !all_evicted_files.is_empty() {
            let key = RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            };
            if let Ok(mut map) = self.indexed_files.lock() {
                if let Some(set) = map.get_mut(&key) {
                    for f in &all_evicted_files {
                        set.remove(f);
                    }
                }
                // If the set is now empty or absent, leave it empty — future
                // mark_file_indexed calls will repopulate it for new files.
            }
        }
    }

    /// Probe the posting list for a `@>` (containment) predicate.
    ///
    /// `needle_bytes` is the raw JSONB of the right-hand literal, `opclass` is
    /// the index opclass (`"jsonb_ops"` or `"jsonb_path_ops"`).
    ///
    /// Returns:
    /// * `ProbeResult::NoIndex` — no posting list loaded for this column yet;
    ///   caller must fall through to full scan.
    /// * `ProbeResult::Empty` — the posting list exists and the intersection
    ///   is empty; no rows can match.  (Caller can short-circuit with zero rows.)
    /// * `ProbeResult::FileCandidates(set)` — the set of file paths that MIGHT
    ///   contain matching rows; caller reads only those files and re-applies the
    ///   full `jsonb_contains` predicate for correctness.
    pub fn probe_containment(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        needle_bytes: &[u8],
    ) -> ProbeResult {
        let needle: Value = match serde_json::from_slice(needle_bytes) {
            Ok(v) => v,
            Err(_) => return ProbeResult::NoIndex, // unparseable → conservative
        };
        let terms = needle_terms(&needle, opclass);
        if terms.is_empty() {
            // Empty needle matches everything (PG semantics: {} @> {} is true).
            return ProbeResult::NoIndex; // fall through to full scan
        }

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return ProbeResult::NoIndex,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");

        // AND-merge posting lists for each term.
        let mut candidate_files: Option<HashSet<String>> = None;

        for term in &terms {
            match list.probe_term(term) {
                None => {
                    // Term not in index: unknown state (may have been evicted or
                    // never inserted).  Conservative: include all files.
                    return ProbeResult::NoIndex;
                }
                Some(entries) => {
                    let files: HashSet<String> =
                        entries.iter().map(|e| e.file_path.clone()).collect();
                    candidate_files = Some(match candidate_files {
                        None => files,
                        Some(prev) => prev.intersection(&files).cloned().collect(),
                    });
                }
            }
        }

        match candidate_files {
            None => ProbeResult::NoIndex,
            Some(files) if files.is_empty() => ProbeResult::Empty,
            Some(files) => ProbeResult::FileCandidates(files),
        }
    }

    /// Remove all posting entries for `file_path` in `(project, table, col)`.
    /// Also removes `file_path` from the indexed-files completeness set so
    /// future probes do not erroneously claim full coverage after this file
    /// is gone.
    pub fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut list = arc.lock().expect("TermPostingList lock poisoned");
            list.remove_file(file_path);
        }
        // Remove from completeness tracking.
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        if let Ok(mut map) = self.indexed_files.lock() {
            if let Some(set) = map.get_mut(&key) {
                set.remove(file_path);
            }
        }
    }

    /// Record that `file_path` has been fully indexed for `(project, table,
    /// col)`.  Called immediately after all rows in a new file have been
    /// passed to [`index_row`].  This is the write side of the completeness
    /// guard: a file that appears here is safe to use as a prune boundary.
    pub fn mark_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        if let Ok(mut map) = self.indexed_files.lock() {
            map.entry(key).or_default().insert(file_path.to_string());
        }
    }

    /// Return the set of file paths that have been completely indexed for
    /// `(project, table, col)`.  The caller uses this to decide whether the
    /// GIN posting list provides FULL coverage of the live file set:
    ///
    /// ```text
    /// completeness = indexed_files ⊇ live_files
    /// ```
    ///
    /// If `completeness` is true, file-level pruning to `FileCandidates` is
    /// safe.  If any live file is missing from this set, pruning must NOT
    /// happen (full scan instead, no false negatives).
    pub fn indexed_files_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> HashSet<String> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        if let Ok(map) = self.indexed_files.lock() {
            map.get(&key).cloned().unwrap_or_default()
        } else {
            HashSet::new()
        }
    }

    /// Probe the posting list for a key-existence predicate (`?`, `?&`, `?|`).
    ///
    /// `keys` is the set of keys to check; `require_all` distinguishes
    /// `?&` (all keys must be present — AND-merge) from `?|` (any key
    /// suffices — OR-merge).  For `?` (single key) pass a one-element slice
    /// with `require_all = true`.
    ///
    /// Only works for `jsonb_ops` opclass where key-presence terms are
    /// stored as `"key:<k>"`.  For `jsonb_path_ops` this returns `NoIndex`
    /// (the path-hash encoding does not preserve individual key names).
    ///
    /// Returns the same `ProbeResult` variants as [`probe_containment`].
    pub fn probe_key_existence(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        keys: &[&str],
        require_all: bool,
    ) -> ProbeResult {
        if opclass != "jsonb_ops" {
            // jsonb_path_ops does not store plain key: terms.
            return ProbeResult::NoIndex;
        }
        if keys.is_empty() {
            return ProbeResult::NoIndex;
        }

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return ProbeResult::NoIndex,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");

        if require_all {
            // ?& / ? — all keys must be present: AND-merge posting lists.
            let mut candidate_files: Option<HashSet<String>> = None;
            for key in keys {
                let term = format!("key:{key}");
                match list.probe_term(&term) {
                    None => {
                        // Term not in index — unknown state; fall through to full scan.
                        return ProbeResult::NoIndex;
                    }
                    Some(entries) => {
                        let files: HashSet<String> =
                            entries.iter().map(|e| e.file_path.clone()).collect();
                        candidate_files = Some(match candidate_files {
                            None => files,
                            Some(prev) => prev.intersection(&files).cloned().collect(),
                        });
                    }
                }
            }
            match candidate_files {
                None => ProbeResult::NoIndex,
                Some(files) if files.is_empty() => ProbeResult::Empty,
                Some(files) => ProbeResult::FileCandidates(files),
            }
        } else {
            // ?| — any key suffices: OR-merge posting lists (union).
            let mut candidate_files: HashSet<String> = HashSet::new();
            let mut all_unknown = true;
            for key in keys {
                let term = format!("key:{key}");
                if let Some(entries) = list.probe_term(&term) {
                    all_unknown = false;
                    for e in entries {
                        candidate_files.insert(e.file_path.clone());
                    }
                }
                // If the term is unknown (None), we conservatively include all
                // files later by returning NoIndex; track via all_unknown.
            }
            if all_unknown {
                return ProbeResult::NoIndex;
            }
            if candidate_files.is_empty() {
                ProbeResult::Empty
            } else {
                ProbeResult::FileCandidates(candidate_files)
            }
        }
    }

    /// Rebuild posting list entries for `file_path` in `(project, table, col)`
    /// from a fresh batch of JSONB rows.  Called on the UPDATE/DELETE commit
    /// path after a copy-on-write replacement file is written: the old file's
    /// entries have already been removed via `remove_file`; this call adds the
    /// replacement file's entries.
    ///
    /// After rebuilding, `new_file_path` is marked as fully indexed so the
    /// completeness guard remains valid for file-level pruning.
    ///
    /// Silently skips non-JSONB (non-LargeBinary) columns.
    pub fn rebuild_file_entries(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        batches: &[arrow_array::RecordBatch],
        new_file_path: &str,
    ) {
        use arrow_array::Array;
        let mut indexed_any = false;
        for batch in batches {
            let Ok(col_idx) = batch.schema().index_of(col) else {
                continue;
            };
            let col_arr = batch.column(col_idx);
            if let Some(arr) = col_arr.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
                indexed_any = true;
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        continue;
                    }
                    self.index_row(
                        project,
                        table,
                        col,
                        opclass,
                        arr.value(row),
                        new_file_path,
                        0,
                        row as u64,
                    );
                }
            }
        }
        // Mark the new file as fully indexed only when the column was found
        // and processed.  If the column was absent (wrong schema), we cannot
        // claim coverage and leave the completeness set unchanged.
        if indexed_any {
            self.mark_file_indexed(project, table, col, new_file_path);
        }
    }
}

impl Default for GinIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a containment probe against the GIN posting list.
#[derive(Debug)]
pub enum ProbeResult {
    /// No posting list for this column, or term was evicted. Caller must
    /// fall through to a full scan (safe: no false negatives).
    NoIndex,
    /// The intersection of all term posting lists is empty.  No rows in the
    /// table can satisfy the containment predicate.
    Empty,
    /// The set of file paths that may contain matching rows.  The caller reads
    /// these files and re-applies the `jsonb_contains` predicate for
    /// correctness.
    FileCandidates(HashSet<String>),
}

// ── Planner detection ─────────────────────────────────────────────────────────

/// Detected GIN containment probe plan.
///
/// Produced by [`detect_gin_containment`] when the query matches the
/// `SELECT … FROM table WHERE col @> literal` (or `col <@ literal`) shape
/// and a GIN index exists on `col`.
#[derive(Debug)]
pub struct GinContainmentPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed JSONB column.
    pub col: String,
    /// The containment literal (raw JSON bytes).
    pub needle: Vec<u8>,
    /// Whether this is `@>` (true) or `<@` (false).
    pub is_contains: bool,
    /// Opclass from the catalog index declaration.
    pub opclass: String,
    /// Columns to project in the output.  `None` means `SELECT *`.
    pub projection: Option<Vec<String>>,
}

/// Detect the `SELECT … FROM table WHERE col @> 'literal'` shape.
///
/// Returns `Some(plan)` when:
/// 1. The SQL (before operator rewriting) contains `@>` or `<@`.
/// 2. sqlparser can parse it as a single-table SELECT with a WHERE clause
///    that is exactly `col @> 'literal'` or `col <@ 'literal'`.
/// 3. The table has a GIN index on `col` in the catalog.
///
/// Returns `None` for anything that doesn't match perfectly (caller falls
/// through to the normal DataFusion / rewrite path).
pub async fn detect_gin_containment(
    sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<GinContainmentPlan> {
    // Fast pre-check: must contain @> or <@ operator.
    let has_at_gt = sql.contains("@>");
    let has_lt_at = sql.contains("<@");
    if !has_at_gt && !has_lt_at {
        return None;
    }

    // Parse with sqlparser to extract the structural shape.
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.as_ref(),
        _ => return None,
    };

    // No CTEs, no LIMIT that changes semantics, no ORDER BY that matters.
    if query.with.is_some() {
        return None;
    }

    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return None,
    };

    // Single table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            use crate::pg_ast::ObjectNamePartExt;
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // WHERE clause must be `col @> literal` or `col <@ literal`.
    let (col_name, needle_str, is_contains) = match &select.selection {
        Some(sqlparser::ast::Expr::BinaryOp { left, op, right }) => {
            let op_str = op.to_string();
            let is_contains = op_str == "@>";
            let is_contained = op_str == "<@";
            if !is_contains && !is_contained {
                return None;
            }
            // LHS must be a bare column reference.
            let col = match left.as_ref() {
                sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
                _ => return None,
            };
            // RHS must be a quoted literal or cast.
            let literal = extract_json_literal(right)?;
            (col, literal, is_contains)
        }
        _ => return None,
    };

    // Projection: `*` or a list of bare column names.
    let projection = extract_simple_projection(&select.projection)?;

    // Catalog lookup: table must have a GIN index on `col_name`.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let gin_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
    })?;
    let opclass = gin_index.opclass.clone().unwrap_or_else(|| "jsonb_ops".to_string());

    // Parse needle to validate it's valid JSON.
    let needle_bytes = needle_str.as_bytes().to_vec();
    let _: serde_json::Value = serde_json::from_slice(&needle_bytes).ok()?;

    Some(GinContainmentPlan {
        table,
        col: col_name,
        needle: needle_bytes,
        is_contains,
        opclass,
        projection,
    })
}

/// Phase 5.19.D — detected GIN key-existence probe plan.
///
/// Produced by [`detect_gin_key_probe`] when the SQL matches the shape
/// `SELECT … FROM table WHERE col ? 'key'` (or the `?&` / `?|` variants)
/// and the column has a GIN index with `jsonb_ops` opclass.
#[derive(Debug)]
pub struct GinKeyPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed JSONB column.
    pub col: String,
    /// Opclass from the catalog index declaration (`jsonb_ops` only).
    pub opclass: String,
    /// The keys to probe.
    pub keys: Vec<String>,
    /// `true` when ALL keys must exist (`?` single key, `?&` all-keys).
    /// `false` when ANY key suffices (`?|`).
    pub require_all: bool,
}

/// Phase 5.19.D — detect `SELECT … FROM table WHERE col ? 'key'` (and
/// the `?&` / `?|` variants) for GIN key-existence probe acceleration.
///
/// The SQL arrives *before* the `rewrite_json_operators` pass that converts
/// `?` → `jsonb_has_key(...)`, so the raw `?` operator is still present in
/// the text. We sniff the raw SQL for `?` / `?&` / `?|` first, then parse
/// the rewritten SQL (which DataFusion will process) to validate the shape.
///
/// Returns `None` for any query that doesn't fit the supported shape or
/// when the column has no GIN index with `jsonb_ops`.
pub async fn detect_gin_key_probe(
    raw_sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<GinKeyPlan> {
    // Fast pre-check: must contain the ? family operators.
    let has_any = raw_sql.contains("?|");
    let has_all = raw_sql.contains("?&");
    let has_key = raw_sql.contains(" ? ") || raw_sql.contains(" ?'");
    if !has_any && !has_all && !has_key {
        return None;
    }

    // Parse the raw SQL (before operator rewriting).
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, raw_sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.as_ref(),
        _ => return None,
    };
    if query.with.is_some() {
        return None;
    }
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return None,
    };

    // Single table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            use crate::pg_ast::ObjectNamePartExt;
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // WHERE must be `col OP keys` where OP is one of `?`, `?&`, `?|`.
    let (col_name, keys, require_all) = extract_key_probe_where(&select.selection)?;

    // Projection: `*` or simple column list.
    let _projection = extract_simple_projection(&select.projection)?;

    // Catalog lookup: must have a GIN index on `col_name` with `jsonb_ops`.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let gin_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
    })?;
    let opclass = gin_index.opclass.clone().unwrap_or_else(|| "jsonb_ops".to_string());
    // Key probes only accelerate jsonb_ops (which stores key: terms).
    if opclass != "jsonb_ops" {
        return None;
    }

    Some(GinKeyPlan { table, col: col_name, opclass, keys, require_all })
}

/// Extract `(col_name, keys, require_all)` from a WHERE clause containing
/// `col ? 'key'`, `col ?& array['k1','k2']`, or `col ?| array['k1','k2']`.
///
/// sqlparser 0.52 parses `?` as a custom operator in PG dialect; we match the
/// AST shapes that actually appear.
fn extract_key_probe_where(
    selection: &Option<sqlparser::ast::Expr>,
) -> Option<(String, Vec<String>, bool)> {
    use sqlparser::ast::{Array, Expr, Value, ValueWithSpan};
    let expr = selection.as_ref()?;

    // sqlparser represents `col ? 'key'` and `col ?& ...` / `col ?| ...` as
    // `BinaryOp` nodes in PG dialect.
    let (left, op, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left, op, right),
        _ => return None,
    };

    // LHS must be a bare column name.
    let col_name = match left.as_ref() {
        Expr::Identifier(id) => id.value.clone(),
        _ => return None,
    };

    let op_str = op.to_string();

    // `?` — single key existence.
    if op_str == "?" {
        let key = extract_single_string_literal(right)?;
        return Some((col_name, vec![key], true));
    }

    // `?&` — all keys.
    if op_str == "?&" {
        let keys = extract_key_array_literal(right)?;
        return Some((col_name, keys, true));
    }

    // `?|` — any key.
    if op_str == "?|" {
        let keys = extract_key_array_literal(right)?;
        return Some((col_name, keys, false));
    }

    None
}

/// Extract a single string literal from an Expr (for the `?` operator RHS).
fn extract_single_string_literal(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s.clone()),
        Expr::Cast { expr: inner, .. } => extract_single_string_literal(inner),
        _ => None,
    }
}

/// Extract a list of string keys from a PG array literal or ARRAY constructor.
///
/// Handles:
/// * `array['k1','k2']` — DataFusion/sqlparser Array constructor
/// * `'{k1,k2}'` — PG text-array literal as a single-quoted string
fn extract_key_array_literal(expr: &sqlparser::ast::Expr) -> Option<Vec<String>> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        // `ARRAY['k1', 'k2']` or `array[...]`
        Expr::Array(arr) => {
            let mut keys = Vec::new();
            for elem in &arr.elem {
                if let Some(k) = extract_single_string_literal(elem) {
                    keys.push(k);
                }
            }
            if keys.is_empty() { None } else { Some(keys) }
        }
        // `'{k1,k2}'` — PG text-array literal as a quoted string.
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            let s = s.trim();
            if s.starts_with('{') && s.ends_with('}') {
                let keys: Vec<String> = s[1..s.len() - 1]
                    .split(',')
                    .map(|k| k.trim().trim_matches('"').to_string())
                    .filter(|k| !k.is_empty())
                    .collect();
                if keys.is_empty() { None } else { Some(keys) }
            } else {
                None
            }
        }
        Expr::Cast { expr: inner, .. } => extract_key_array_literal(inner),
        _ => None,
    }
}

/// Extract the JSON literal string from the RHS of a `@>` / `<@` expression.
///
/// Accepts:
/// * `'{"key":"val"}'` — single-quoted string literal
/// * `'{"key":"val"}'::jsonb` — with a jsonb cast suffix
fn extract_json_literal(expr: &sqlparser::ast::Expr) -> Option<String> {
    extract_json_literal_for_prune(expr)
}

/// Pub(crate) alias for [`extract_json_literal`].  Exposed so the Inv-W5
/// JSONB-posting-prune detector in `session.rs` can reuse the same shape
/// matcher without duplicating it.
pub(crate) fn extract_json_literal_for_prune(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            Some(s.clone())
        }
        // `'literal'::jsonb` cast
        Expr::Cast { expr: inner, .. } => extract_json_literal_for_prune(inner),
        _ => None,
    }
}

/// Extract a simple projection (either `*` → `None`, or a list of bare column names).
fn extract_simple_projection(
    items: &[sqlparser::ast::SelectItem],
) -> Option<Option<Vec<String>>> {
    if items.len() == 1 {
        if let sqlparser::ast::SelectItem::Wildcard(_) = &items[0] {
            return Some(None); // SELECT *
        }
    }
    let mut cols = Vec::new();
    for item in items {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) => {
                cols.push(id.value.clone());
            }
            _ => return None, // expressions or aliases → fall through
        }
    }
    Some(Some(cols))
}

// ── Phase 5.20.E — FTS (tsvector @@) probe detection ─────────────────────────

/// Detected GIN FTS probe plan for a `col @@ to_tsquery(...)` predicate.
///
/// Produced by [`detect_tsvector_match`] when the query (before the
/// `rewrite_tsvector_at_at` lowering) contains `@@` on a tsvector column
/// that has a GIN index.
#[derive(Debug)]
pub struct GinFtsPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed tsvector column.
    pub col: String,
    /// The tsquery string extracted from `to_tsquery(...)` / `plainto_tsquery(...)`
    /// for posting-list probing.  This is the canonical form as returned by the
    /// tsquery parser.
    pub tsquery_str: String,
}

/// Detect `SELECT … FROM table WHERE col @@ to_tsquery('…')` (and the
/// `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery` variants)
/// for GIN posting-list probe acceleration.
///
/// The SQL arrives *before* the `rewrite_tsvector_at_at` pass that converts
/// `col @@ expr` to `tsvector_match_udf(col, expr)`, so the raw `@@` operator
/// is still present in the text.
///
/// Returns `None` for any query that doesn't fit the supported shape or when
/// the column has no GIN index with `tsvector_ops`.
///
/// On any error or uncertainty returns `None` → full scan (safe).
pub async fn detect_tsvector_match(
    raw_sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<GinFtsPlan> {
    // Fast pre-check: must contain @@ to be relevant.
    if !raw_sql.contains("@@") {
        return None;
    }

    // Parse the raw SQL (before operator rewriting).
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, raw_sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.as_ref(),
        _ => return None,
    };
    if query.with.is_some() {
        return None;
    }
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return None,
    };

    // Single table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            use crate::pg_ast::ObjectNamePartExt;
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // WHERE must be `col @@ tsquery_expr` where:
    //   - LHS is a bare column name (or `alias.col`).
    //   - RHS is a call to `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery`,
    //     or `websearch_to_tsquery`.
    let (col_name, tsquery_str) = extract_fts_where(&select.selection)?;

    // Projection check — only `*` or bare column list; fall through otherwise.
    let _projection = extract_simple_projection(&select.projection)?;

    // Catalog lookup: must have a GIN index on `col_name` with tsvector_ops.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let _gin_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
            && idx.opclass.as_deref().map_or(false, |op| op == "tsvector_ops")
    })?;

    Some(GinFtsPlan { table, col: col_name, tsquery_str })
}

/// Extract `(col_name, tsquery_text)` from a WHERE clause of the form
/// `col @@ to_tsquery(...)` or `col @@ plainto_tsquery(...)`.
///
/// `tsquery_text` is the canonical tsquery string used for posting-list probing
/// (e.g. `"'cat' & 'dog'"`).
fn extract_fts_where(
    selection: &Option<sqlparser::ast::Expr>,
) -> Option<(String, String)> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value, ValueWithSpan};

    let expr = selection.as_ref()?;

    // sqlparser does not natively parse `@@` as BinaryOp in all versions.
    // `rewrite_tsvector_at_at` converts it to `tsvector_match_udf(lhs, rhs)`.
    // However, `detect_tsvector_match` is called with the *raw* SQL before the
    // rewriter runs.  We therefore parse the raw SQL — sqlparser's PG dialect
    // does handle `@@` as a custom operator in `BinaryOp`.
    let (left, op, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left, op, right),
        _ => return None,
    };

    if op.to_string() != "@@" {
        return None;
    }

    // LHS: bare column name or `alias.col`.
    let col_name = match left.as_ref() {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => parts[1].value.clone(),
        _ => return None,
    };

    // RHS: function call to a tsquery constructor.  We extract the text
    // argument and derive the canonical query lexemes via the same logic
    // used by `fts_udf`.
    let tsquery_str = extract_tsquery_from_expr(right)?;

    Some((col_name, tsquery_str))
}

/// Extract the canonical tsquery string from a `to_tsquery(...)` /
/// `plainto_tsquery(...)` / `phraseto_tsquery(...)` / `websearch_to_tsquery(...)`
/// call, or a bare `'lexeme'::tsquery` cast / string literal.
///
/// Returns the tsquery as the GIN posting-list probe lexemes (e.g. `"'cat' & 'dog'"`).
fn extract_tsquery_from_expr(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value, ValueWithSpan};

    match expr {
        // `to_tsquery('english', 'fox & dog')` or `to_tsquery('fox')`
        // `plainto_tsquery(...)` etc.
        Expr::Function(f) => {
            let fn_name = f.name.to_string().to_lowercase();
            if !matches!(
                fn_name.as_str(),
                "to_tsquery" | "plainto_tsquery" | "phraseto_tsquery" | "websearch_to_tsquery"
            ) {
                return None;
            }
            // Extract the last string argument as the query body.
            let args: Vec<&FunctionArg> = match &f.args {
                sqlparser::ast::FunctionArguments::List(l) => l.args.iter().collect(),
                _ => return None,
            };
            // Accept 1-arg (body) or 2-arg (config, body) forms.
            let body_arg = args.last()?;
            let body_str = match body_arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                    extract_string_literal(inner)?
                }
                _ => return None,
            };
            // For posting-list probe we treat the query body as a list of
            // AND-ed lexemes (conservative — see gin_tsvector module doc).
            // Return it verbatim; `extract_query_lexemes` in gin_tsvector will
            // parse the lexemes from the canonical form.
            //
            // For `plainto_tsquery` we want the stemmed canonical form so the
            // lexemes match what `to_tsvector` stored.  We delegate to the
            // same `to_tsquery_text` helper used by `fts_udf`.
            let canonical = crate::fts_udf::to_tsquery_text(&fn_name, None, &body_str).ok()?;
            Some(canonical)
        }
        // `'fox'::tsquery` bare cast — treat the literal as a plain tsquery.
        Expr::Cast { expr: inner, .. } => extract_tsquery_from_expr(inner),
        // Bare string literal `'fox'` — treat as plainto_tsquery body.
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            Some(s.clone())
        }
        _ => None,
    }
}

/// Extract a raw string value from a scalar expression (single-quoted literal
/// or `::text` / `::varchar` cast thereof).
fn extract_string_literal(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s.clone()),
        Expr::Cast { expr: inner, .. } => extract_string_literal(inner),
        _ => None,
    }
}

// ── Phase 5.24.D — range GIST probe detection ─────────────────────────────────

/// Which range operator triggered the interval-index probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeOp {
    /// `col @> scalar` — range contains element.
    ContainsElem,
    /// `col @> range` — range contains range (treated as overlap for pruning).
    ContainsRange,
    /// `col && range` — ranges overlap.
    Overlaps,
    /// `col <@ range` — range is contained by (treated as overlap for pruning).
    ContainedBy,
}

/// Detected GIST interval-index probe plan for a range operator predicate.
///
/// Produced by [`detect_range_index_probe`] when the query (before the
/// `rewrite_range_operators` lowering) contains `@>` / `&&` / `<@` on a
/// range-typed column that has a GIST index.
#[derive(Debug)]
pub struct IntervalIndexPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed range column.
    pub col: String,
    /// The operator that triggered the probe.
    pub op: RangeOp,
    /// For `ContainsElem`: the numeric point to probe (e.g. `5` in `r @> 5`).
    pub point: Option<f64>,
    /// For `ContainsRange`, `Overlaps`, `ContainedBy`: the range literal
    /// (raw PG text form, e.g. `'[1,10)'`) to convert to an `IndexInterval`.
    pub range_literal: Option<String>,
}

/// Detect `SELECT … FROM table WHERE col @> val` / `col && range` / `col <@ range`
/// on a GIST-indexed range column for interval-tree probe acceleration.
///
/// The SQL arrives *before* the `rewrite_range_operators` pass that converts
/// `@>` / `&&` / `<@` to UDF calls, so the raw operators are still present.
///
/// Returns `None` for any query that doesn't fit the supported shape or when
/// the column has no GIST index.  On any error or uncertainty returns `None`
/// → full scan (safe).
pub async fn detect_range_index_probe(
    raw_sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<IntervalIndexPlan> {
    // Fast pre-check: must contain at least one range operator.
    if !raw_sql.contains("@>") && !raw_sql.contains("&&") && !raw_sql.contains("<@") {
        return None;
    }

    // Parse the raw SQL (before operator rewriting).
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, raw_sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.as_ref(),
        _ => return None,
    };
    if query.with.is_some() {
        return None;
    }
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return None,
    };

    // Single table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            use crate::pg_ast::ObjectNamePartExt;
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // Parse WHERE `col op rhs` where op ∈ { @>, <@, && }.
    let (col_name, op, point, range_literal) =
        extract_range_where(&select.selection)?;

    // Catalog lookup: the column must have a GIST index.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let _gist_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gist"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
    })?;

    // Additionally verify the column itself is a range type (defensive).
    let _range_field = meta.schema.fields().iter().find(|f| {
        f.name() == &col_name && crate::types::field_is_range(f)
    })?;

    Some(IntervalIndexPlan { table, col: col_name, op, point, range_literal })
}

/// Parse the WHERE clause for a single range operator predicate.
///
/// Supported forms:
///   - `col @> numeric_literal`   → (`col`, `ContainsElem`, `Some(f64)`, `None`)
///   - `col @> range_literal`     → (`col`, `ContainsRange`, `None`, `Some(str)`)
///   - `col && range_literal`     → (`col`, `Overlaps`, `None`, `Some(str)`)
///   - `col <@ range_literal`     → (`col`, `ContainedBy`, `None`, `Some(str)`)
///   - `numeric <@ col`           → (`col`, `ContainsElem` reversed, `Some(f64)`, `None`)
///
/// Returns `None` for any shape that does not match.
fn extract_range_where(
    selection: &Option<sqlparser::ast::Expr>,
) -> Option<(String, RangeOp, Option<f64>, Option<String>)> {
    use sqlparser::ast::Expr;

    let expr = selection.as_ref()?;

    let (left, op_str, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left, op.to_string(), right),
        _ => return None,
    };

    match op_str.as_str() {
        "@>" => {
            // `col @> val` — LHS must be a column name.
            let col = extract_col_name(left)?;
            // RHS: numeric literal → ContainsElem; range literal → ContainsRange.
            if let Some(pt) = extract_numeric_literal(right) {
                Some((col, RangeOp::ContainsElem, Some(pt), None))
            } else if let Some(s) = extract_range_literal_str(right) {
                Some((col, RangeOp::ContainsRange, None, Some(s)))
            } else {
                None
            }
        }
        "<@" => {
            // Two forms:
            //   `col <@ range_literal` — col is contained by a literal range
            //   `numeric_literal <@ col` — numeric is contained by the range column
            if let Some(col) = extract_col_name(left) {
                // `col <@ range_literal`
                if let Some(s) = extract_range_literal_str(right) {
                    return Some((col, RangeOp::ContainedBy, None, Some(s)));
                }
                // `col <@ numeric` — unusual; skip
                None
            } else if let Some(pt) = extract_numeric_literal(left) {
                // `numeric <@ col` — equivalent to `col @> numeric`
                let col = extract_col_name(right)?;
                Some((col, RangeOp::ContainsElem, Some(pt), None))
            } else {
                None
            }
        }
        "&&" => {
            // `col && range_literal` — LHS must be a column name.
            let col = extract_col_name(left)?;
            let s = extract_range_literal_str(right)?;
            Some((col, RangeOp::Overlaps, None, Some(s)))
        }
        _ => None,
    }
}

/// Extract a bare column name (or `alias.col`) from an expression.
fn extract_col_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(parts[1].value.clone()),
        _ => None,
    }
}

/// Extract a numeric literal (integer or float) as `f64` from an expression.
fn extract_numeric_literal(expr: &sqlparser::ast::Expr) -> Option<f64> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. }) => s.parse().ok(),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => extract_numeric_literal(inner).map(|v| -v),
        Expr::Cast { expr: inner, .. } => extract_numeric_literal(inner),
        _ => None,
    }
}

/// Extract a PG range literal string (e.g. `'[1,10)'` or `'[1,10)'::int4range`)
/// from an expression.  Returns the inner string content (without quotes).
fn extract_range_literal_str(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            // Must look like a PG range literal: starts with `[` or `(` or `empty`.
            let trimmed = s.trim();
            if trimmed.starts_with('[')
                || trimmed.starts_with('(')
                || trimmed.eq_ignore_ascii_case("empty")
            {
                Some(s.clone())
            } else {
                None
            }
        }
        Expr::Cast { expr: inner, .. } => extract_range_literal_str(inner),
        _ => None,
    }
}

// ── Phase 5.x — PK point-probe (bloom + zone-map file prune) ──────────────────

/// Outcome of a single-column primary-key equality point probe against the
/// live data-file set.
///
/// The probe consults two per-file artefacts that the catalog already records
/// for every data file (Parquet *and* Vortex):
///   * the **zone-map** (`column_stats` min/max), and
///   * the **catalog bloom filter** (`bloom_filters`, when the PK column was
///     bloomed at write time).
///
/// It never opens a file body — it works purely off the catalog metadata that
/// `live_data_files()` returns.  The decisive answer is [`PkProbeOutcome::Absent`]:
/// when *every* live file's zone-map or bloom proves the key cannot be present,
/// a `WHERE pk = <lit>` lookup can return zero rows without a single file open.
///
/// CORRECTNESS: pruning is conservative.  A file is only dropped from the
/// candidate set when the zone-map says `NoMatch` *or* the bloom says
/// "definitely absent".  Any inconclusive answer keeps the file, so the probe
/// can never hide a row that actually exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PkProbeOutcome {
    /// Every live file was pruned — the key cannot be present in the cold tier.
    /// The caller returns an empty result set without opening any file body.
    /// The `usize` is the number of files that were skipped (for counters).
    Absent { files_pruned: usize },
    /// The key may be present in the listed files (a subset of the live set).
    /// The caller restricts its read to exactly these paths.  `files_pruned`
    /// counts how many live files the probe ruled out.
    Candidates {
        paths: Vec<object_store::path::Path>,
        files_pruned: usize,
    },
}

/// Probe the live data-file set for a single-column primary-key equality
/// lookup (`WHERE pk = <scalar>`), pruning files whose zone-map or catalog
/// bloom filter prove the key is absent.
///
/// `pk_eq` is the `(column, value)` pair from the recognised `Predicate::Eq`.
/// `live_files` is the catalog's live data-file set for the table.  `schema`
/// is the table's Arrow schema (used to decode the zone-map bytes).
///
/// Returns:
///   * [`PkProbeOutcome::Absent`] when no live file can contain the key — the
///     bloom-miss / out-of-range fast path: zero rows, zero file opens.
///   * [`PkProbeOutcome::Candidates`] with the surviving file paths otherwise.
///
/// This consolidates the two per-file prune checks (zone-map via
/// [`evaluate_compound_for_pruning`] and the catalog bloom via
/// [`bloom_from_bytes`]) into one focused, testable point-probe so the common
/// `WHERE pk = ?` shape gets a single, cheap metadata-only gate before any
/// Parquet/Vortex footer is touched.
pub(crate) fn pk_point_probe(
    pk_col: &str,
    pk_val: &basin_storage::ScalarValue,
    live_files: &[basin_catalog::DataFileRef],
    schema: &arrow_schema::Schema,
) -> PkProbeOutcome {
    use basin_storage::{
        bloom_from_bytes, evaluate_compound_for_pruning, CompoundPredicate, Predicate,
        PruneOutcome, ScalarValue,
    };

    let atom = Predicate::Eq(pk_col.to_string(), pk_val.clone());
    let cp = CompoundPredicate::Atom(atom);

    let mut paths: Vec<object_store::path::Path> = Vec::with_capacity(live_files.len());
    let mut pruned = 0usize;

    for f in live_files {
        // 1. Zone-map (min/max) prune: a `NoMatch` proves the key falls
        //    outside this file's recorded range for the PK column.
        if matches!(
            evaluate_compound_for_pruning(&cp, &f.column_stats, schema, f.row_count),
            PruneOutcome::NoMatch
        ) {
            pruned += 1;
            continue;
        }

        // 2. Catalog bloom prune: when the PK column was bloomed at write
        //    time, a definitive "not present" lets us skip the file body.
        //    A bloom hit (may-contain) only KEEPS the file; it never alone
        //    decides to keep — the zone-map already passed above.
        if let Some(bloom_bytes) = f.bloom_filters.get(pk_col) {
            if let Some(filter) = bloom_from_bytes(bloom_bytes) {
                let absent = match pk_val {
                    ScalarValue::Int64(v) => !filter.contains(v.to_le_bytes().as_ref()),
                    ScalarValue::UInt64(v) => !filter.contains(v.to_le_bytes().as_ref()),
                    ScalarValue::Utf8(s) => !filter.contains(s.as_bytes()),
                    // No bloom encoding defined for other types — don't prune.
                    _ => false,
                };
                if absent {
                    pruned += 1;
                    continue;
                }
            }
        }

        paths.push(object_store::path::Path::from(f.path.as_str()));
    }

    if paths.is_empty() {
        PkProbeOutcome::Absent {
            files_pruned: pruned,
        }
    } else {
        PkProbeOutcome::Candidates {
            paths,
            files_pruned: pruned,
        }
    }
}

/// Probe the live data-file set for a multi-value IN-list on a single-column
/// primary key (`WHERE pk IN (v1, v2, …, vN)`).
///
/// This is the IN-list generalisation of [`pk_point_probe`]: for each live
/// file we ask whether ANY of the `pk_vals` could be present (zone-map OR of
/// Eq atoms, plus bloom OR across every value).  Files where no value can
/// possibly be present are pruned; the remaining candidate set is the UNION
/// of all per-value surviving files — a conservative superset.
///
/// The per-row IN predicate is always re-applied after the read so false
/// positives from bloom or zone-map approximation are harmless.
///
/// Returns [`PkProbeOutcome::Absent`] when every live file was pruned (all
/// values definitively absent), or [`PkProbeOutcome::Candidates`] with the
/// surviving file paths otherwise.
pub(crate) fn pk_point_probe_multi(
    pk_col: &str,
    pk_vals: &[basin_storage::ScalarValue],
    live_files: &[basin_catalog::DataFileRef],
    schema: &arrow_schema::Schema,
) -> PkProbeOutcome {
    use basin_storage::{
        bloom_from_bytes, evaluate_compound_for_pruning, CompoundPredicate, Predicate,
        PruneOutcome, ScalarValue,
    };

    if pk_vals.is_empty() {
        // Empty IN-list is always false — no files needed.
        return PkProbeOutcome::Absent {
            files_pruned: live_files.len(),
        };
    }

    // Int64 lists answer the zone-map question — "does ANY IN value fall in
    // this file's [min, max]?" — exactly, in O(log n) per file, by binary
    // searching the sorted (deduped) key list. This replaces evaluating a
    // 1000-branch OR per file with identical prune decisions. (A plain
    // min/max range-overlap test would be a strictly weaker superset that
    // keeps middle files no key lands in — not used.) Non-Int64 lists keep
    // the OR-of-Eq compound prune verbatim.
    let sorted_int_keys: Option<Vec<i64>> = if pk_vals
        .iter()
        .all(|v| matches!(v, ScalarValue::Int64(_)))
    {
        let mut ks: Vec<i64> = pk_vals
            .iter()
            .filter_map(|v| match v {
                ScalarValue::Int64(n) => Some(*n),
                _ => None,
            })
            .collect();
        ks.sort_unstable();
        ks.dedup();
        Some(ks)
    } else {
        None
    };
    // 8-byte little-endian Int64 stat decode — the writer's min_bytes /
    // max_bytes encoding for Int64 columns (see ColumnStats docs).
    fn stat_i64(b: Option<&[u8]>) -> Option<i64> {
        let b = b?;
        let arr: [u8; 8] = b.try_into().ok()?;
        Some(i64::from_le_bytes(arr))
    }

    // OR-of-Eq compound predicate for the non-Int64 zone-map fallback.
    let or_pred: Option<CompoundPredicate> = if sorted_int_keys.is_some() {
        None
    } else {
        let alts: Vec<CompoundPredicate> = pk_vals
            .iter()
            .map(|v| {
                CompoundPredicate::Atom(Predicate::Eq(pk_col.to_string(), v.clone()))
            })
            .collect();
        Some(if alts.len() == 1 {
            alts.into_iter().next().unwrap()
        } else {
            CompoundPredicate::Or(alts)
        })
    };

    let mut paths: Vec<object_store::path::Path> = Vec::with_capacity(live_files.len());
    let mut pruned = 0usize;

    'file: for f in live_files {
        // 1. Zone-map (min/max) prune: if no value in the list falls within
        //    this file's recorded [min, max] range for the PK column, the
        //    whole file can be skipped. Int64: exact binary search on the
        //    sorted keys; other types: the OR-of-Eq compound prune. Missing
        //    or malformed stats fall through (keep the file — conservative).
        if let Some(keys) = &sorted_int_keys {
            let cs = f.column_stats.get(pk_col);
            let fmin = cs.and_then(|c| stat_i64(c.min_bytes.as_deref()));
            let fmax = cs.and_then(|c| stat_i64(c.max_bytes.as_deref()));
            if let (Some(fmin), Some(fmax)) = (fmin, fmax) {
                let idx = keys.partition_point(|k| *k < fmin);
                if idx == keys.len() || keys[idx] > fmax {
                    pruned += 1;
                    continue;
                }
            }
        } else if let Some(or_pred) = &or_pred {
            if matches!(
                evaluate_compound_for_pruning(or_pred, &f.column_stats, schema, f.row_count),
                PruneOutcome::NoMatch
            ) {
                pruned += 1;
                continue;
            }
        }

        // 2. Bloom prune: if the file carries a bloom for the PK column,
        //    probe the IN-list values. The file is a candidate as soon as
        //    ANY value is a maybe-hit — and because the caller consumes the
        //    UNION of candidate paths (no per-key attribution), the loop
        //    EARLY-EXITS on the first maybe-hit; only when every value is
        //    definitively absent can the file be skipped.
        if let Some(bloom_bytes) = f.bloom_filters.get(pk_col) {
            if let Some(filter) = bloom_from_bytes(bloom_bytes) {
                let mut maybe_present = false;
                for v in pk_vals {
                    let hit = match v {
                        ScalarValue::Int64(n) => filter.contains(n.to_le_bytes().as_ref()),
                        ScalarValue::UInt64(n) => filter.contains(n.to_le_bytes().as_ref()),
                        ScalarValue::Utf8(s) => filter.contains(s.as_bytes()),
                        // No bloom encoding for other types — treat as maybe.
                        _ => true,
                    };
                    if hit {
                        maybe_present = true;
                        break;
                    }
                }
                if !maybe_present {
                    pruned += 1;
                    continue 'file;
                }
            }
        }

        paths.push(object_store::path::Path::from(f.path.as_str()));
    }

    if paths.is_empty() {
        PkProbeOutcome::Absent {
            files_pruned: pruned,
        }
    } else {
        PkProbeOutcome::Candidates {
            paths,
            files_pruned: pruned,
        }
    }
}

// ── Capability 1 — C2 row-group GIN prune for JSONB `@>` ──────────────────────
//
// The engine's existing `@>` probe (`GinIndexRegistry::probe_containment`) is
// FILE-granular: it returns the set of files that *might* contain a matching
// row.  Under a uniform key distribution every file matches the searched term
// at least once, so file-level pruning achieves nothing (the measured 88x gap).
//
// basin-storage's `GinRowGroupRegistry` (gin_rowgroup.rs) stores a per-row-group
// bloom over the same GIN term atoms, so a `col @> '{…}'` query can be narrowed
// to just the surviving row-groups WITHIN each candidate file.  This helper is
// the engine-side glue: it extracts the SAME structure-keyed term atoms from the
// `@>` needle that the storage indexer recorded (via [`needle_terms`]), then asks
// the row-group registry which row-groups survive in each candidate file.
//
// FAIRNESS: the terms are derived purely from the query STRUCTURE (the JSONB
// needle's keys + key=value pairs, identical to `extract_terms`).  No table,
// column, or literal value is special-cased.
//
// CORRECTNESS: the row-group bloom is a conservative superset (AND semantics
// across all needle terms — identical to the file-level posting-list
// intersection, just finer).  A surviving row-group MUST still have the
// `jsonb_contains` predicate re-evaluated on its rows; an EMPTY survivor list
// for a file means that whole file is provably prune-able.  `Unknown` (file not
// summarised / not sealed) falls back to the caller's file-granular behaviour —
// never a false negative.

/// Outcome of a row-group containment prune across a set of candidate files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowGroupPrune {
    /// No row-group summaries exist for any candidate file (none are sealed),
    /// or the needle produced no probe terms.  The caller keeps its existing
    /// file-granular behaviour (read every candidate file in full).
    Unknown,
    /// Per-file surviving row-groups.  Every key is a candidate file path; the
    /// value is the ascending list of row-groups in that file that *might*
    /// contain a matching row (an empty vector means the whole file is
    /// prune-able).  Files absent from the map are NOT summarised and must be
    /// read in full by the caller (conservative: never a false negative).
    PerFile(HashMap<String, Vec<u32>>),
}

/// Compute the row-group-granular prune for a JSONB `@>` containment query.
///
/// `needle_bytes` is the raw JSONB of the right-hand `@>` literal; `opclass` is
/// the catalog index opclass (`"jsonb_ops"` / `"jsonb_path_ops"`).
/// `candidate_files` is the set of files the file-level GIN probe already
/// considers relevant (or the full live set when no file probe ran).
///
/// Returns [`RowGroupPrune::PerFile`] mapping each *summarised* candidate file to
/// its surviving row-groups, so the caller can read only those row-groups and
/// re-apply `jsonb_contains` on them.  Files that are [`RowGroupProbe::Unknown`]
/// are omitted (the caller must read them whole).  Returns
/// [`RowGroupPrune::Unknown`] when no candidate file is summarised at all (so
/// the caller's existing behaviour is unchanged) or the needle has no terms.
///
/// The engine wiring that consumes the surviving row-group ids (handing them to
/// the Parquet reader / DataFusion `ListingTable`) lives in `session.rs` /
/// `basin-storage` — see the module note in `gin_rowgroup.rs`.  This function is
/// the structure-keyed decision logic that wiring calls; it owns no I/O.
pub fn rowgroup_prune_for_containment(
    registry: &basin_storage::index::gin_rowgroup::GinRowGroupRegistry,
    project: &ProjectId,
    table: &TableName,
    col: &str,
    opclass: &str,
    needle_bytes: &[u8],
    candidate_files: &[String],
) -> RowGroupPrune {
    // Extract the SAME structure-keyed GIN term atoms the storage indexer used.
    let needle: Value = match serde_json::from_slice(needle_bytes) {
        Ok(v) => v,
        Err(_) => return RowGroupPrune::Unknown, // unparseable → conservative
    };
    let search_keys = needle_terms(&needle, opclass);
    if search_keys.is_empty() {
        // Empty needle (`{}`) matches everything — nothing to prune on.
        return RowGroupPrune::Unknown;
    }

    // Probe every candidate file.  `rowgroups_maybe_containing_multi` already
    // omits files that are Unknown (not sealed), so the resulting map contains
    // only files the caller may safely prune at row-group granularity.
    let per_file = registry.rowgroups_maybe_containing_multi(
        project,
        table,
        col,
        candidate_files,
        &search_keys,
    );

    if per_file.is_empty() {
        // No candidate file is summarised → caller keeps file-granular behaviour.
        RowGroupPrune::Unknown
    } else {
        RowGroupPrune::PerFile(per_file)
    }
}

// ── Capability 2 — B3 trigram candidate prune for ILIKE / LIKE ────────────────
//
// Today ILIKE/LIKE that is not a pure prefix (handled by `Predicate::StartsWith`
// in fast_select.rs) falls through to DataFusion's per-row sequential scan.  A
// trigram index over the column lets us prune to a candidate SUPERSET of rows
// first, then re-check the real LIKE pattern on just those candidates.
//
// basin-trgm (`gin_like.rs`) owns the persisted/standalone TrigramGinIndex.
// However `basin-engine` does NOT depend on `basin-trgm` (and adding that dep is
// outside this task's file boundary), and — more fundamentally — there is no
// storage-persisted trigram index for table columns yet (building one needs the
// write-path/storage integration that this task does not own).  So the wiring
// achievable WITHIN index_probe.rs is the IN-MEMORY candidate-prune: given a
// column's decoded string values, build a trigram inverted index on the fly,
// prune to candidate row-ids by the pattern's required trigrams, then re-check.
//
// This module ports the minimal, structure-keyed trigram logic from
// basin-trgm::gin_like so the prune is real and unit-testable here.  The
// persisted-build follow-up (storage-side) is documented in the task report.
//
// FAIRNESS: trigrams are derived purely from the pattern STRUCTURE (literal runs
// between `%`/`_` wildcards).  No column or literal value is special-cased.
//
// CORRECTNESS: `trigram_candidates` returns a SUPERSET of matching row-ids; the
// caller MUST re-run the real LIKE/ILIKE matcher ([`like_matches`]) on each
// candidate.  A pattern that yields no usable trigrams (every literal run < 3
// bytes) returns [`TrigramCandidates::All`] → caller scans everything (correct,
// if unhelpful).

/// Result of an in-memory trigram candidate prune.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrigramCandidates {
    /// Pattern produced usable trigrams; only these row-ids can match.  Always a
    /// superset of the true matches — the caller re-checks each with the real
    /// LIKE/ILIKE matcher.  An empty set means no row can match.
    Some(HashSet<u64>),
    /// Pattern too short / wildcard-heavy to prune (no literal run ≥ 3 bytes).
    /// The caller must consider every row a candidate (full scan).
    All,
}

/// Extract the *required* trigrams of a SQL `LIKE`/`ILIKE` pattern.
///
/// The pattern is split on `%` (any run) and `_` (any single char); `\` escapes
/// the next char.  Every maximal literal run of length ≥ 3 contributes each of
/// its contiguous 3-byte windows.  Runs shorter than 3 bytes contribute nothing
/// (they cannot pin down a trigram).  When `case_insensitive`, runs are
/// ASCII-lowercased first.  Output is sorted + deduped; empty means "no trigram
/// constraint" (caller falls back to a full scan).
///
/// Mirrors PostgreSQL `pg_trgm`'s `generate_wildcard_trgm` extraction and is the
/// in-engine port of `basin_trgm::gin_like::trigrams_for_pattern` (kept local
/// because basin-engine does not depend on basin-trgm).
pub fn trigrams_for_pattern(pattern: &str, case_insensitive: bool) -> Vec<[u8; 3]> {
    fn fold(b: u8, ci: bool) -> u8 {
        if ci { b.to_ascii_lowercase() } else { b }
    }
    let mut out: Vec<[u8; 3]> = Vec::new();
    let mut run: Vec<u8> = Vec::new();
    let flush = |run: &mut Vec<u8>, out: &mut Vec<[u8; 3]>| {
        if run.len() >= 3 {
            for w in run.windows(3) {
                out.push([w[0], w[1], w[2]]);
            }
        }
        run.clear();
    };
    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                if i + 1 < bytes.len() {
                    run.push(fold(bytes[i + 1], case_insensitive));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            b'%' | b'_' => {
                flush(&mut run, &mut out);
                i += 1;
            }
            b => {
                run.push(fold(b, case_insensitive));
                i += 1;
            }
        }
    }
    flush(&mut run, &mut out);
    out.sort_unstable();
    out.dedup();
    out
}

/// The substring trigrams of `text` for indexing: every contiguous 3-byte window
/// of the case-preserved input AND of its ASCII-lowercased form (sorted, deduped,
/// no word padding).  Emitting both casings lets one index serve case-sensitive
/// `LIKE` (query trigrams keep their case) and `ILIKE` (query trigrams lowered).
fn substring_trigrams(text: &str) -> Vec<[u8; 3]> {
    let raw = text.as_bytes();
    let lowered: Vec<u8> = raw.iter().map(|b| b.to_ascii_lowercase()).collect();
    let mut out: Vec<[u8; 3]> = Vec::with_capacity(raw.len().saturating_sub(2) * 2);
    out.extend(raw.windows(3).map(|w| [w[0], w[1], w[2]]));
    out.extend(lowered.windows(3).map(|w| [w[0], w[1], w[2]]));
    out.sort_unstable();
    out.dedup();
    out
}

/// Build an in-memory trigram inverted index over `rows` (`(row_id, text)`),
/// prune to the candidate superset that could match `pattern`, and return it.
///
/// `case_insensitive = true` is the `ILIKE` path.  The result is always a
/// SUPERSET of the true matches — the caller re-checks each candidate with
/// [`like_matches`].  Returns [`TrigramCandidates::All`] when the pattern yields
/// no usable trigrams (caller scans every row).
///
/// This builds the index on the fly for the candidate-prune; there is no
/// persisted trigram index for table columns yet (storage follow-up — see
/// module note).  Even built on the fly it lets the caller skip the per-row LIKE
/// evaluation on rows whose trigram set cannot be a superset of the pattern's.
pub fn trigram_candidates<'a, I>(
    rows: I,
    pattern: &str,
    case_insensitive: bool,
) -> TrigramCandidates
where
    I: IntoIterator<Item = (u64, &'a str)>,
{
    let required = trigrams_for_pattern(pattern, case_insensitive);
    if required.is_empty() {
        // No trigram constraint → cannot prune.  (Still consume the iterator
        // cheaply? No — caller will scan; avoid building the index at all.)
        return TrigramCandidates::All;
    }

    // Inverted index: trigram → set of row-ids whose text contains it.  We only
    // need posting lists for the required trigrams, but building the full set is
    // simpler and the prune is a one-shot per query.
    let mut inverted: HashMap<[u8; 3], HashSet<u64>> = HashMap::new();
    for (row_id, text) in rows {
        for tg in substring_trigrams(text) {
            inverted.entry(tg).or_default().insert(row_id);
        }
    }

    // Multi-key AND: a candidate must appear in EVERY required trigram's posting
    // list.  Seed from the smallest list, intersect the rest.
    let mut lists: Vec<&HashSet<u64>> = Vec::with_capacity(required.len());
    for tg in &required {
        match inverted.get(tg) {
            Some(set) => lists.push(set),
            // A required trigram with no posting list ⇒ no row contains it ⇒
            // empty candidate set (provably zero matches).
            None => return TrigramCandidates::Some(HashSet::new()),
        }
    }
    lists.sort_by_key(|s| s.len());
    let mut acc: HashSet<u64> = lists[0].clone();
    for list in &lists[1..] {
        acc.retain(|r| list.contains(r));
        if acc.is_empty() {
            break;
        }
    }
    TrigramCandidates::Some(acc)
}

/// Evaluate a SQL `LIKE`/`ILIKE` pattern against `text` — the re-check the
/// caller MUST run on every trigram candidate (the index returns a superset).
///
/// `%` matches any run (including empty), `_` matches exactly one char, `\`
/// escapes the next char.  `case_insensitive = true` is the `ILIKE` path
/// (ASCII case folding).  This is a straightforward backtracking matcher over
/// chars (Unicode-aware for `_` = one char); it is the correctness backstop and
/// is not a hot loop (it runs only on the pruned candidate set).
pub fn like_matches(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let fold = |c: char| -> char {
        if case_insensitive {
            c.to_ascii_lowercase()
        } else {
            c
        }
    };
    let t: Vec<char> = text.chars().map(fold).collect();
    // Decompose pattern into tokens: literal char, `%`, or `_`.
    enum Tok {
        Lit(char),
        Any,
        One,
    }
    let mut toks: Vec<Tok> = Vec::new();
    let mut pc = pattern.chars().peekable();
    while let Some(c) = pc.next() {
        match c {
            '\\' => {
                if let Some(n) = pc.next() {
                    toks.push(Tok::Lit(fold(n)));
                }
            }
            '%' => toks.push(Tok::Any),
            '_' => toks.push(Tok::One),
            other => toks.push(Tok::Lit(fold(other))),
        }
    }

    // Iterative backtracking match (handles `%` greedily with a fallback).
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;
    while ti < t.len() {
        match toks.get(pi) {
            Some(Tok::Lit(c)) if *c == t[ti] => {
                ti += 1;
                pi += 1;
            }
            Some(Tok::One) => {
                ti += 1;
                pi += 1;
            }
            Some(Tok::Any) => {
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
            }
            _ => {
                // Mismatch: backtrack to the last `%` if any.
                if let Some(sp) = star_pi {
                    pi = sp + 1;
                    star_ti += 1;
                    ti = star_ti;
                } else {
                    return false;
                }
            }
        }
    }
    // Consume trailing `%`s.
    while let Some(Tok::Any) = toks.get(pi) {
        pi += 1;
    }
    pi == toks.len()
}

// ── PG-Wave-β — spatial GIST probe detection ──────────────────────────────────
//
// Pattern-recognise WHERE-clause spatial predicates that an R-tree on a POINT
// column can prune at row-group granularity.  Three shapes are supported:
//
// 1. `ST_DWithin(col, ST_MakePoint(x, y), r)` (or commutative form) →
//    [`SpatialPredicate::DWithin`] — INEXACT: the R-tree probes a bounding
//    box of degrees on each side derived from `r` (meters); the Haversine UDF
//    re-runs above the scan to cull false positives.
//
// 2. `ST_Contains(box_literal, col)` → [`SpatialPredicate::BboxIntersects`] —
//    EXACT at row-group granularity.  Today the engine has no first-class
//    BOX2D literal syntax; we accept a parenthesised pair of `ST_MakePoint`
//    expressions as the bounding rectangle (lower-left, upper-right corners).
//
// 3. `col = ST_MakePoint(x, y)` → [`SpatialPredicate::PointEq`] — EXACT.
//
// CORRECTNESS: every shape falls through to a full scan when any condition
// fails to match (col-on-col comparisons, non-literal coordinates, missing
// GIST index — caller's responsibility) so the prune is purely additive.
// No false negatives are possible at this layer.

/// A spatial predicate the R-tree pushdown layer can act on.
#[derive(Debug, Clone, PartialEq)]
pub enum SpatialPredicate {
    /// `ST_DWithin(col, ST_MakePoint(x, y), r)` — INEXACT.
    ///
    /// R-tree probes an axis-aligned bounding box of the column expanded by
    /// the degree-equivalent of `radius_m` on every side; surviving rows are
    /// re-checked by the residual `st_dwithin` UDF (the engine keeps a
    /// FilterExec above the scan).
    DWithin { col: String, x: f64, y: f64, radius_m: f64 },
    /// `ST_Contains(bbox_literal, col)` (a BOX2D-shaped rectangle) — EXACT
    /// at row-group granularity.  The R-tree envelope test is the predicate.
    BboxIntersects {
        col: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    },
    /// `col = ST_MakePoint(x, y)` — EXACT, row-group-level point equality.
    PointEq { col: String, x: f64, y: f64 },
}

impl SpatialPredicate {
    /// The column referenced by this predicate (used to pick the catalog
    /// GIST index entry to consult).
    pub fn column(&self) -> &str {
        match self {
            SpatialPredicate::DWithin { col, .. }
            | SpatialPredicate::BboxIntersects { col, .. }
            | SpatialPredicate::PointEq { col, .. } => col,
        }
    }
}

/// Recognise a single spatial predicate from a WHERE expression.
///
/// Returns `Some(SpatialPredicate)` only for the EXACT shapes documented
/// above.  Sub-expressions, col-on-col comparisons, and non-literal
/// coordinates all fall through to `None` (full scan).
pub fn detect_spatial_predicate(expr: &sqlparser::ast::Expr) -> Option<SpatialPredicate> {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        // Recurse into `AND` and pick the first matching arm (today the
        // pushdown only fires for a single spatial predicate at a time;
        // multiple spatial predicates would compose via FilterExec residue).
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            detect_spatial_predicate(left).or_else(|| detect_spatial_predicate(right))
        }
        // `col = ST_MakePoint(x, y)` (and commutative).
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let (col, x, y) = match (extract_col_name(left), extract_make_point(right)) {
                (Some(c), Some((x, y))) => (c, x, y),
                _ => match (extract_make_point(left), extract_col_name(right)) {
                    (Some((x, y)), Some(c)) => (c, x, y),
                    _ => return None,
                },
            };
            Some(SpatialPredicate::PointEq { col, x, y })
        }
        // `ST_DWithin(a, b, r)` or `ST_Contains(box, col)`.
        Expr::Function(f) => detect_spatial_function(f),
        _ => None,
    }
}

/// Recognise a spatial function call (`ST_DWithin` / `ST_Contains`).
fn detect_spatial_function(f: &sqlparser::ast::Function) -> Option<SpatialPredicate> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let name = f.name.to_string().to_lowercase();
    let args: Vec<&FunctionArg> = match &f.args {
        FunctionArguments::List(l) => l.args.iter().collect(),
        _ => return None,
    };
    fn unwrap(a: &FunctionArg) -> Option<&sqlparser::ast::Expr> {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        }
    }
    match (name.as_str(), args.len()) {
        ("st_dwithin", 3) => {
            let (a, b, r) = (unwrap(args[0])?, unwrap(args[1])?, unwrap(args[2])?);
            let radius_m = extract_numeric_literal(r)?;
            if radius_m <= 0.0 || !radius_m.is_finite() {
                return None;
            }
            // Either (col, ST_MakePoint(...), r) or (ST_MakePoint(...), col, r).
            let (col, x, y) = match (extract_col_name(a), extract_make_point(b)) {
                (Some(c), Some((x, y))) => (c, x, y),
                _ => match (extract_make_point(a), extract_col_name(b)) {
                    (Some((x, y)), Some(c)) => (c, x, y),
                    _ => return None,
                },
            };
            Some(SpatialPredicate::DWithin { col, x, y, radius_m })
        }
        ("st_contains", 2) => {
            // `ST_Contains(box, col)` — the engine has no BOX2D literal
            // syntax today, so we accept the form
            // `ST_Contains(ST_MakeEnvelope(min_x, min_y, max_x, max_y), col)`
            // when the first arg is an envelope-shaped function call.
            let (env, col_e) = (unwrap(args[0])?, unwrap(args[1])?);
            let col = extract_col_name(col_e)?;
            let (min_x, min_y, max_x, max_y) = extract_envelope(env)?;
            Some(SpatialPredicate::BboxIntersects { col, min_x, min_y, max_x, max_y })
        }
        _ => None,
    }
}

/// Extract `(x, y)` from a literal `ST_MakePoint(x, y)` call.  Both
/// arguments must be numeric literals (or `-numeric` / cast thereof).
fn extract_make_point(expr: &sqlparser::ast::Expr) -> Option<(f64, f64)> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match expr {
        Expr::Cast { expr: inner, .. } => extract_make_point(inner),
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if name != "st_makepoint" && name != "st_point" {
                return None;
            }
            let args: Vec<&FunctionArg> = match &f.args {
                FunctionArguments::List(l) => l.args.iter().collect(),
                _ => return None,
            };
            if args.len() != 2 {
                return None;
            }
            fn unwrap(a: &FunctionArg) -> Option<&sqlparser::ast::Expr> {
                match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }
            }
            let x = extract_numeric_literal(unwrap(args[0])?)?;
            let y = extract_numeric_literal(unwrap(args[1])?)?;
            Some((x, y))
        }
        _ => None,
    }
}

/// Extract `(min_x, min_y, max_x, max_y)` from a PostGIS-style envelope
/// expression.  Accepted forms:
///   * `ST_MakeEnvelope(min_x, min_y, max_x, max_y)` (PostGIS canonical)
///   * `ST_MakeEnvelope(min_x, min_y, max_x, max_y, srid)` (5-arg ignored
///     SRID)
fn extract_envelope(expr: &sqlparser::ast::Expr) -> Option<(f64, f64, f64, f64)> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match expr {
        Expr::Cast { expr: inner, .. } => extract_envelope(inner),
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if name != "st_makeenvelope" && name != "st_envelope" {
                return None;
            }
            let args: Vec<&FunctionArg> = match &f.args {
                FunctionArguments::List(l) => l.args.iter().collect(),
                _ => return None,
            };
            if args.len() != 4 && args.len() != 5 {
                return None;
            }
            fn unwrap(a: &FunctionArg) -> Option<&sqlparser::ast::Expr> {
                match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }
            }
            let min_x = extract_numeric_literal(unwrap(args[0])?)?;
            let min_y = extract_numeric_literal(unwrap(args[1])?)?;
            let max_x = extract_numeric_literal(unwrap(args[2])?)?;
            let max_y = extract_numeric_literal(unwrap(args[3])?)?;
            Some((min_x, min_y, max_x, max_y))
        }
        _ => None,
    }
}

// ── PG-Wave KNN — `ORDER BY point_col <-> ST_MakePoint(x,y) LIMIT k` ──────────
//
// Nearest-neighbour spatial search over a POINT column backed by the R-tree
// sidecar.  Mirrors the pgvector HNSW planner (`vector_planner.rs`) but for
// POINT geometry: the order operator is `<->`, the right-hand side is a
// literal `ST_MakePoint(x, y)` (optionally wrapped in `ST_SetSRID(...)`), and
// the column is a `BASIN_TYPE=POINT` column (NOT a `vector(N)` column — that
// is the existing HNSW path).
//
// DISAMBIGUATION FROM pgvector: the vector planner runs first in the executor
// hot path; for a POINT column its `extract_distance_call` parses the RHS as a
// `vector(N)` literal, which fails (the RHS is an `ST_MakePoint(...)` call, not
// a quoted vector literal), so the vector planner returns `None`.  The KNN
// recognizer then keys on (a) RHS being `ST_MakePoint`/`ST_SetSRID(...)` and
// (b) the LHS column carrying `BASIN_TYPE=POINT` in the catalog schema.  The
// two paths are structurally exclusive at the RHS shape and confirmed exclusive
// at the column-type check.

/// Detected KNN nearest-neighbour plan for a POINT column.
///
/// Produced by [`detect_knn_predicate`] when the query matches
/// `SELECT <proj> FROM <table> ORDER BY <col> <-> ST_MakePoint(x, y) LIMIT k`
/// and `<col>` is a `BASIN_TYPE=POINT` column.
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialKnnPlan {
    /// Projected output columns. `None` means `SELECT *`.
    pub projection: Option<Vec<String>>,
    /// The table to scan.
    pub table: TableName,
    /// The POINT column being ordered by distance.
    pub col: String,
    /// Query-point longitude (x).
    pub qx: f64,
    /// Query-point latitude (y).
    pub qy: f64,
    /// `LIMIT k` — number of nearest rows to return.
    pub k: usize,
}

/// Detect `SELECT … FROM t ORDER BY <col> <-> ST_MakePoint(x,y) LIMIT k` on a
/// POINT column.
///
/// `raw_sql` MUST be the original SQL, before the `<->`→UDF rewrite (same
/// invariant as the pgvector planner — once `<->` becomes `l2_distance(...)`
/// the structural signal is gone).  The catalog is consulted to confirm the
/// ORDER BY column is a `BASIN_TYPE=POINT` column on the named table; a vector
/// column, a missing column, or a missing table all return `None` so the
/// existing pipeline (pgvector HNSW or brute-force) takes over unchanged.
///
/// Returns `None` (caller falls back) on ANY off-shape query.
pub async fn detect_knn_predicate(
    raw_sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<SpatialKnnPlan> {
    use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
    use sqlparser::ast::{
        Expr, GroupByExpr, SetExpr, Statement, TableFactor, Value, ValueWithSpan,
    };
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Fast pre-gate: must contain `<->` AND a point constructor.
    if !raw_sql.contains("<->") {
        return None;
    }
    let lower = raw_sql.to_lowercase();
    if !lower.contains("st_makepoint") && !lower.contains("st_point") {
        return None;
    }

    // sqlparser 0.52 does not natively parse `<->`; rewrite the single
    // `<col> <-> <rhs>` occurrence into `__basin_vop_l2(<col>, <rhs>)` so the
    // operator survives parsing as a function call. Unlike the pgvector
    // planner (whose RHS is always a quoted vector literal), the KNN RHS is an
    // `ST_MakePoint(...)` / `ST_SetSRID(ST_MakePoint(...), srid)` function
    // call, so the rewrite must balance parentheses on the RHS. The marker
    // namespace can never collide with a user UDF.
    let prepared = match mark_knn_distance_op(raw_sql) {
        Some(s) => s,
        None => return None,
    };
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, &prepared).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        Statement::Query(q) => q.as_ref(),
        _ => return None,
    };

    // LIMIT k — constant positive integer.
    let k = match query.ext_limit() {
        Some(Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })) => {
            match s.parse::<i64>() {
                Ok(n) if n > 0 => n as usize,
                _ => return None,
            }
        }
        _ => return None,
    };

    // Reject auxiliary clauses we'd silently lose when routing.
    if query.with.is_some()
        || !query.ext_limit_by().is_empty()
        || query.ext_offset().is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return None;
    }

    // Exactly one ORDER BY expression, ASC, no NULLS FIRST/LAST.
    let order = query.order_by.as_ref()?;
    if order.ext_exprs().len() != 1 {
        return None;
    }
    let order_expr = &order.ext_exprs()[0];
    if let Some(false) = order_expr.options.asc {
        return None;
    }
    if order_expr.options.nulls_first.is_some() {
        return None;
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => return None,
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return None;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return None,
    }

    // Single bare table, no joins/aliases.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // ORDER BY expr must be `__basin_vop_l2(<col>, ST_MakePoint(x,y))`.
    let (col, qx, qy) = extract_knn_distance_call(&order_expr.expr)?;

    // Projection: `*` or a list of bare identifiers.
    let projection = extract_simple_projection(&select.projection)?;

    // Confirm `col` is a POINT column (NOT a vector column → that is HNSW).
    let meta = catalog.load_table(project, &table).await.ok()?;
    let field = meta.schema.field_with_name(&col).ok()?;
    if !crate::types::field_is_point(field) {
        return None;
    }

    Some(SpatialKnnPlan { projection, table, col, qx, qy, k })
}

/// Rewrite the first `<col> <-> <rhs>` into `__basin_vop_l2(<col>, <rhs>)`,
/// where `<col>` is a bare identifier (or `alias.col`) and `<rhs>` is a
/// function-call expression with balanced parentheses (e.g.
/// `ST_MakePoint(0.0, 0.0)` or `ST_SetSRID(ST_MakePoint(0,0), 4326)`).
///
/// Returns `None` when no `<->` is present, the LHS is not a bare identifier,
/// or the RHS is not a balanced function call — all of which mean "not the KNN
/// shape", so the caller falls back.
fn mark_knn_distance_op(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let op_pos = sql.find("<->")?;
    let op_end = op_pos + 3;

    // ── LHS: scan left over whitespace, then an identifier run. ──
    let mut i = op_pos;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let lhs_end = i;
    while i > 0 {
        let b = bytes[i - 1];
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'.' {
            i -= 1;
        } else {
            break;
        }
    }
    let lhs_start = i;
    if lhs_start == lhs_end {
        return None;
    }
    let lhs = &sql[lhs_start..lhs_end];

    // ── RHS: skip whitespace, expect an identifier then a balanced `(...)`. ──
    let mut j = op_end;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    let rhs_start = j;
    while j < bytes.len() {
        let b = bytes[j];
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'.' {
            j += 1;
        } else {
            break;
        }
    }
    // Skip whitespace between the function name and its open paren.
    let mut k = j;
    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    if k >= bytes.len() || bytes[k] != b'(' {
        return None; // RHS is not a function call → not the KNN shape.
    }
    let mut depth = 0i32;
    let mut end = k;
    while end < bytes.len() {
        match bytes[end] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    end += 1;
                    break;
                }
            }
            _ => {}
        }
        end += 1;
    }
    if depth != 0 {
        return None; // unbalanced parens.
    }
    let rhs = &sql[rhs_start..end];

    let marker = format!("__basin_vop_l2({lhs}, {rhs})");
    let mut out = String::with_capacity(sql.len() + marker.len());
    out.push_str(&sql[..lhs_start]);
    out.push_str(&marker);
    out.push_str(&sql[end..]);
    Some(out)
}

/// Recognise `__basin_vop_l2(<col>, ST_MakePoint(x, y))` (the KNN order
/// expression).  The distance op must be `<->` (L2); `<#>`/`<=>` are not
/// spatial metrics and return `None`.  The RHS must be a literal
/// `ST_MakePoint` (optionally wrapped in `ST_SetSRID(...)`).
fn extract_knn_distance_call(expr: &sqlparser::ast::Expr) -> Option<(String, f64, f64)> {
    use crate::pg_ast::ObjectNamePartExt;
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    let func = match expr {
        Expr::Function(f) => f,
        _ => return None,
    };
    if func.name.0.len() != 1 {
        return None;
    }
    // Only L2 (`<->`) is the spatial NN metric.
    if func.name.0[0].id_val().as_str() != "__basin_vop_l2" {
        return None;
    }
    let args: Vec<&FunctionArg> = match &func.args {
        FunctionArguments::List(l) => l.args.iter().collect(),
        _ => return None,
    };
    if args.len() != 2 {
        return None;
    }
    fn unwrap(a: &FunctionArg) -> Option<&Expr> {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        }
    }
    let col = extract_col_name(unwrap(args[0])?)?;
    let (qx, qy) = extract_make_point_with_srid(unwrap(args[1])?)?;
    Some((col, qx, qy))
}

/// Extract `(x, y)` from `ST_MakePoint(x, y)`, also unwrapping a surrounding
/// `ST_SetSRID(ST_MakePoint(...), srid)` (the SRID arg is ignored — every
/// point is interpreted as WGS84).
fn extract_make_point_with_srid(expr: &sqlparser::ast::Expr) -> Option<(f64, f64)> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};
    match expr {
        Expr::Nested(inner) => extract_make_point_with_srid(inner),
        Expr::Cast { expr: inner, .. } => extract_make_point_with_srid(inner),
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if name == "st_setsrid" {
                // `ST_SetSRID(ST_MakePoint(x,y), srid)` — recurse into arg 0.
                let args: Vec<&FunctionArg> = match &f.args {
                    FunctionArguments::List(l) => l.args.iter().collect(),
                    _ => return None,
                };
                if args.is_empty() {
                    return None;
                }
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = args[0] {
                    return extract_make_point_with_srid(e);
                }
                None
            } else {
                // Delegate to the shared ST_MakePoint extractor.
                extract_make_point(expr)
            }
        }
        _ => None,
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_terms_jsonb_ops_flat() {
        let v = json!({"tag": "nested", "id": 42});
        let terms = extract_terms(&v, "jsonb_ops");
        assert!(terms.contains(&"key:tag".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"kv:tag=\"nested\"".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"key:id".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"kv:id=42".to_string()), "terms={terms:?}");
    }

    #[test]
    fn extract_terms_jsonb_path_ops_flat() {
        let v = json!({"tag": "nested"});
        let terms = extract_terms(&v, "jsonb_path_ops");
        // Should produce exactly one path_hash term for tag="nested"
        assert_eq!(terms.len(), 1, "terms={terms:?}");
        assert!(terms[0].starts_with("path_hash:"), "terms={terms:?}");
    }

    #[test]
    fn extract_terms_empty_object() {
        let v = json!({});
        let terms = extract_terms(&v, "jsonb_ops");
        assert!(terms.is_empty(), "empty object should produce no terms");
    }

    #[test]
    fn probe_containment_no_index() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        assert!(matches!(result, ProbeResult::NoIndex));
    }

    #[test]
    fn probe_containment_hit_after_insert() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index one document that matches.
        let doc = br#"{"tag":"nested","id":42}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        // Probe for {"tag":"nested"} — should find f1.parquet.
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "expected f1.parquet in {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn probe_containment_miss_after_insert() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index a document that does NOT contain the needle.
        let doc = br#"{"role":"user"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        // Probe for {"tag":"nested"} — must not find f1.parquet.
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        // term "key:tag" is not in the index at all → NoIndex (conservative),
        // OR term "kv:tag=\"nested\"" is not in index → NoIndex.
        // Either way the caller falls through; Empty would also be acceptable.
        assert!(
            matches!(result, ProbeResult::NoIndex | ProbeResult::Empty),
            "expected NoIndex or Empty, got {result:?}"
        );
    }

    #[test]
    fn probe_containment_and_merge() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // f1 has both "role":"admin" AND "tenant":"acme".
        let doc1 = br#"{"role":"admin","tenant":"acme"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc1, "f1.parquet", 0, 0);

        // f2 has only "role":"admin".
        let doc2 = br#"{"role":"admin"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc2, "f2.parquet", 0, 0);

        // Probe for compound needle: {"role":"admin","tenant":"acme"}.
        // f1 has both terms; f2 is missing "kv:tenant=..." so the AND-merge
        // should exclude f2.
        let needle = br#"{"role":"admin","tenant":"acme"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 should be in candidates: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 should be excluded: {files:?}");
            }
            ProbeResult::NoIndex => {
                // Acceptable: if any term was evicted or missing the registry
                // falls back to full-scan (conservative, not wrong).
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn remove_file_clears_entries() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        let doc = br#"{"tag":"nested"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        registry.remove_file(&project, &table, "payload", "f1.parquet");

        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        // After removal, either Empty (set exists but is empty) or NoIndex.
        assert!(
            matches!(result, ProbeResult::NoIndex | ProbeResult::Empty),
            "expected NoIndex or Empty after remove, got {result:?}"
        );
    }

    // ── Phase 5.19.D — key-existence probe tests ──────────────────────────────

    #[test]
    fn probe_key_existence_single_key_hit() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index a doc that has the key "tag".
        let doc = br#"{"tag":"nested","id":1}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_ops", &["tag"], true,
        );
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "expected f1.parquet in {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn probe_key_existence_single_key_miss() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index a doc that has "role" but NOT "tag".
        let doc = br#"{"role":"admin"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_ops", &["tag"], true,
        );
        // "key:tag" not in index → NoIndex (conservative).
        assert!(
            matches!(result, ProbeResult::NoIndex | ProbeResult::Empty),
            "expected NoIndex or Empty, got {result:?}"
        );
    }

    #[test]
    fn probe_key_existence_all_keys_and_merge() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // f1 has both "id" and "tag".
        let doc1 = br#"{"id":1,"tag":"x"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc1, "f1.parquet", 0, 0);

        // f2 has only "id".
        let doc2 = br#"{"id":2}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc2, "f2.parquet", 0, 0);

        // ?& ["id","tag"] — f2 lacks "tag", should be excluded.
        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_ops", &["id", "tag"], true,
        );
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 should match {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 should be excluded {files:?}");
            }
            ProbeResult::NoIndex => {
                // Conservative fall-through is also acceptable.
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn probe_key_existence_any_key_or_merge() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // f1 has "nullable".
        let doc1 = br#"{"nullable":null}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc1, "f1.parquet", 0, 0);

        // f2 has "large".
        let doc2 = br#"{"large":"x"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc2, "f2.parquet", 0, 0);

        // ?| ["nullable","large"] — both files should match.
        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_ops", &["nullable", "large"], false,
        );
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 should match {files:?}");
                assert!(files.contains("f2.parquet"), "f2 should match {files:?}");
            }
            ProbeResult::NoIndex => {
                // Conservative fall-through is also acceptable.
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn probe_key_existence_path_ops_returns_no_index() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Even if a doc is indexed, jsonb_path_ops doesn't support key probes.
        let doc = br#"{"tag":"nested"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_path_ops", doc, "f1.parquet", 0, 0);

        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_path_ops", &["tag"], true,
        );
        assert!(matches!(result, ProbeResult::NoIndex), "path_ops key probe must return NoIndex");
    }

    // ── Phase 5.19.C+ — per-file eviction correctness ───────────────────────

    /// Verify that eviction marks only the affected files as un-indexed, not
    /// the entire column.  Uses a tiny budget so eviction fires immediately.
    #[test]
    fn eviction_marks_only_affected_files_as_unindexed() {
        // Use a tiny budget: 4 entries. We'll index 3 files × 2 terms each =
        // 6 entries total, forcing eviction after file 2.
        //
        // We test via the internal TermPostingList::evict_oldest directly to
        // avoid needing to override the env var (which would be global state).
        let mut pl = TermPostingList::new();

        // Insert terms for f1 (2 entries).
        pl.entries.entry("term_a".to_string()).or_default().insert(PostingEntry {
            file_path: "f1.parquet".to_string(), row_group: 0, row: 0,
        });
        pl.entries.entry("term_b".to_string()).or_default().insert(PostingEntry {
            file_path: "f1.parquet".to_string(), row_group: 0, row: 1,
        });
        pl.insert_order.extend(["term_a".to_string(), "term_b".to_string()]);
        pl.total_count = 2;

        // Insert terms for f2 (2 entries).
        pl.entries.entry("term_c".to_string()).or_default().insert(PostingEntry {
            file_path: "f2.parquet".to_string(), row_group: 0, row: 0,
        });
        pl.entries.entry("term_d".to_string()).or_default().insert(PostingEntry {
            file_path: "f2.parquet".to_string(), row_group: 0, row: 1,
        });
        pl.insert_order.extend(["term_c".to_string(), "term_d".to_string()]);
        pl.total_count = 4;

        // Now evict the oldest 25% (1 term at minimum = evict term_a).
        // term_a was only in f1 → f1 becomes un-indexed.
        // term_c, term_d are in f2 → f2 stays indexed.
        let evicted_files = pl.evict_oldest();

        // f1 must be in evicted_files (its term was dropped).
        // f2 must NOT be in evicted_files (its terms remain).
        assert!(
            evicted_files.contains("f1.parquet"),
            "f1 should be evicted: {evicted_files:?}"
        );
        assert!(
            !evicted_files.contains("f2.parquet"),
            "f2 should NOT be evicted: {evicted_files:?}"
        );
    }

    /// Verify that after eviction, the registry's indexed_files set loses only
    /// the evicted files, leaving other files still indexed.
    #[test]
    fn registry_indexed_files_per_file_not_global_wipe() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index f1 and mark it as fully indexed.
        let doc1 = br#"{"role":"admin"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc1, "f1.parquet", 0, 0);
        registry.mark_file_indexed(&project, &table, "payload", "f1.parquet");

        // Index f2 and mark it as fully indexed.
        let doc2 = br#"{"role":"user"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc2, "f2.parquet", 0, 0);
        registry.mark_file_indexed(&project, &table, "payload", "f2.parquet");

        // Both files must be indexed before any eviction.
        let indexed = registry.indexed_files_for(&project, &table, "payload");
        assert!(indexed.contains("f1.parquet"), "f1 should be indexed: {indexed:?}");
        assert!(indexed.contains("f2.parquet"), "f2 should be indexed: {indexed:?}");

        // Simulate removal of f1 (compaction scenario — not eviction).
        registry.remove_file(&project, &table, "payload", "f1.parquet");

        // After removal, f1 must be gone but f2 must still be indexed.
        let indexed_after = registry.indexed_files_for(&project, &table, "payload");
        assert!(
            !indexed_after.contains("f1.parquet"),
            "f1 should be removed from indexed set: {indexed_after:?}"
        );
        assert!(
            indexed_after.contains("f2.parquet"),
            "f2 should remain indexed after f1 removal: {indexed_after:?}"
        );
    }

    // ── Phase 5.19.E — posting-list maintenance tests ────────────────────────

    // ── PK point-probe (zone-map + catalog bloom) unit tests ─────────────────

    fn i64_stat(min: i64, max: i64) -> basin_catalog::ColumnStats {
        basin_catalog::ColumnStats {
            null_count: Some(0),
            min_bytes: Some(min.to_le_bytes().to_vec()),
            max_bytes: Some(max.to_le_bytes().to_vec()),
            sum_bytes: None,
        }
    }

    fn file_with_pk_range(path: &str, min: i64, max: i64) -> basin_catalog::DataFileRef {
        let mut column_stats = std::collections::BTreeMap::new();
        column_stats.insert("pk".to_string(), i64_stat(min, max));
        basin_catalog::DataFileRef {
            path: path.to_string(),
            size_bytes: 0,
            row_count: (max - min + 1).max(1) as u64,
            column_stats,
            bloom_filters: std::collections::BTreeMap::new(),
            hll_sketches: std::collections::BTreeMap::new(),
            tdigest_sketches: std::collections::BTreeMap::new(),
        }
    }

    fn pk_schema() -> arrow_schema::Schema {
        use arrow_schema::{DataType, Field};
        arrow_schema::Schema::new(vec![Field::new("pk", DataType::Int64, false)])
    }

    #[test]
    fn pk_probe_out_of_range_key_is_absent() {
        // Two files covering [0,99] and [100,199]; key 500 is in neither.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
        ];
        let outcome = pk_point_probe(
            "pk",
            &basin_storage::ScalarValue::Int64(500),
            &files,
            &pk_schema(),
        );
        assert_eq!(outcome, PkProbeOutcome::Absent { files_pruned: 2 });
    }

    #[test]
    fn pk_probe_in_range_key_yields_single_candidate() {
        // Key 150 falls only inside f1's zone-map; f0 is pruned by min/max.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
        ];
        let outcome = pk_point_probe(
            "pk",
            &basin_storage::ScalarValue::Int64(150),
            &files,
            &pk_schema(),
        );
        match outcome {
            PkProbeOutcome::Candidates { paths, files_pruned } => {
                assert_eq!(files_pruned, 1, "f0 should be pruned by zone-map");
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0].as_ref(), "f1.parquet");
            }
            other => panic!("expected single candidate, got {other:?}"),
        }
    }

    #[test]
    fn pk_probe_empty_live_set_is_absent() {
        let outcome = pk_point_probe(
            "pk",
            &basin_storage::ScalarValue::Int64(1),
            &[],
            &pk_schema(),
        );
        assert_eq!(outcome, PkProbeOutcome::Absent { files_pruned: 0 });
    }

    // ── pk_point_probe_multi unit tests ──────────────────────────────────────

    #[test]
    fn pk_probe_multi_all_out_of_range_is_absent() {
        // Three files: [0,99], [100,199], [200,299].  All values in the
        // IN-list (500, 600) are outside every range → all pruned.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
            file_with_pk_range("f2.parquet", 200, 299),
        ];
        let vals = vec![
            basin_storage::ScalarValue::Int64(500),
            basin_storage::ScalarValue::Int64(600),
        ];
        let outcome = pk_point_probe_multi("pk", &vals, &files, &pk_schema());
        assert_eq!(outcome, PkProbeOutcome::Absent { files_pruned: 3 });
    }

    #[test]
    fn pk_probe_multi_spans_two_files() {
        // Values 50 (in f0) and 150 (in f1) — f2 should be pruned.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
            file_with_pk_range("f2.parquet", 200, 299),
        ];
        let vals = vec![
            basin_storage::ScalarValue::Int64(50),
            basin_storage::ScalarValue::Int64(150),
        ];
        let outcome = pk_point_probe_multi("pk", &vals, &files, &pk_schema());
        match outcome {
            PkProbeOutcome::Candidates { paths, files_pruned } => {
                assert_eq!(files_pruned, 1, "f2 should be pruned by zone-map");
                let path_strs: Vec<&str> = paths.iter().map(|p| p.as_ref()).collect();
                assert!(path_strs.contains(&"f0.parquet"), "f0 must be a candidate");
                assert!(path_strs.contains(&"f1.parquet"), "f1 must be a candidate");
                assert!(!path_strs.contains(&"f2.parquet"), "f2 must be pruned");
            }
            other => panic!("expected Candidates, got {other:?}"),
        }
    }

    #[test]
    fn pk_probe_multi_empty_vals_is_absent() {
        // Empty IN-list: SQL semantics → always false, prune all files.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
        ];
        let outcome = pk_point_probe_multi("pk", &[], &files, &pk_schema());
        assert_eq!(outcome, PkProbeOutcome::Absent { files_pruned: 2 });
    }

    #[test]
    fn pk_probe_multi_single_val_matches_single_file() {
        // One value that fits only in f1 — behaves identically to pk_point_probe.
        let files = vec![
            file_with_pk_range("f0.parquet", 0, 99),
            file_with_pk_range("f1.parquet", 100, 199),
        ];
        let vals = vec![basin_storage::ScalarValue::Int64(150)];
        let outcome = pk_point_probe_multi("pk", &vals, &files, &pk_schema());
        match outcome {
            PkProbeOutcome::Candidates { paths, files_pruned } => {
                assert_eq!(files_pruned, 1, "f0 should be pruned");
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0].as_ref(), "f1.parquet");
            }
            other => panic!("expected Candidates for single-val probe, got {other:?}"),
        }
    }

    #[test]
    fn rebuild_file_entries_repopulates_after_remove() {
        use arrow_array::{LargeBinaryArray, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index a document in f1.
        let doc = br#"{"tag":"v1"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        // Remove f1 (simulating DELETE path).
        registry.remove_file(&project, &table, "payload", "f1.parquet");

        // Build a replacement batch for f2.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::LargeBinary,
            true,
        )]));
        let new_doc = br#"{"tag":"v2"}"#;
        let arr = LargeBinaryArray::from_iter_values([new_doc.as_ref()]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        // Rebuild f2 entries.
        registry.rebuild_file_entries(
            &project, &table, "payload", "jsonb_ops", &[batch], "f2.parquet",
        );

        // Now probe for "tag": should find f2, not f1.
        let result = registry.probe_key_existence(
            &project, &table, "payload", "jsonb_ops", &["tag"], true,
        );
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f2.parquet"), "f2 should be in candidates: {files:?}");
                assert!(!files.contains("f1.parquet"), "f1 should NOT be in candidates: {files:?}");
            }
            ProbeResult::NoIndex => {
                // Conservative acceptable.
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    // ── Capability 1 — row-group GIN prune helper ────────────────────────────

    #[test]
    fn rowgroup_prune_narrows_to_holding_row_group() {
        use basin_storage::index::gin_rowgroup::GinRowGroupRegistry;
        let reg = GinRowGroupRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("docs").unwrap();
        let file = "f1.parquet";

        // Same structure-keyed atoms `extract_terms` produces for jsonb_ops.
        let rg0 = extract_terms(&json!({"role": "user"}), "jsonb_ops");
        let rg1 = extract_terms(&json!({"role": "admin"}), "jsonb_ops");
        reg.index_row(&project, &table, "payload", &rg0, file, 0);
        reg.index_row(&project, &table, "payload", &rg1, file, 1);
        reg.mark_file_indexed(&project, &table, "payload", file);

        // Probe `@> {"role":"admin"}` — only rg1 holds it.
        let needle = br#"{"role":"admin"}"#;
        let out = rowgroup_prune_for_containment(
            &reg, &project, &table, "payload", "jsonb_ops", needle,
            &[file.to_string()],
        );
        match out {
            RowGroupPrune::PerFile(m) => {
                assert_eq!(m.get(file), Some(&vec![1]), "expected only rg1, got {m:?}");
            }
            other => panic!("expected PerFile, got {other:?}"),
        }
    }

    #[test]
    fn rowgroup_prune_absent_key_prunes_whole_file() {
        use basin_storage::index::gin_rowgroup::GinRowGroupRegistry;
        let reg = GinRowGroupRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("docs").unwrap();
        let file = "f1.parquet";
        reg.index_row(
            &project, &table, "payload",
            &extract_terms(&json!({"role": "user"}), "jsonb_ops"), file, 0,
        );
        reg.mark_file_indexed(&project, &table, "payload", file);

        let needle = br#"{"role":"ghost"}"#;
        let out = rowgroup_prune_for_containment(
            &reg, &project, &table, "payload", "jsonb_ops", needle,
            &[file.to_string()],
        );
        match out {
            RowGroupPrune::PerFile(m) => {
                assert_eq!(m.get(file), Some(&Vec::<u32>::new()), "file must be prunable");
            }
            other => panic!("expected PerFile (empty), got {other:?}"),
        }
    }

    #[test]
    fn rowgroup_prune_unsummarised_file_is_unknown() {
        use basin_storage::index::gin_rowgroup::GinRowGroupRegistry;
        let reg = GinRowGroupRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("docs").unwrap();
        // Never indexed → no summarised candidate → Unknown (caller reads whole).
        let needle = br#"{"role":"admin"}"#;
        let out = rowgroup_prune_for_containment(
            &reg, &project, &table, "payload", "jsonb_ops", needle,
            &["ghost.parquet".to_string()],
        );
        assert_eq!(out, RowGroupPrune::Unknown);
    }

    #[test]
    fn rowgroup_prune_empty_needle_is_unknown() {
        use basin_storage::index::gin_rowgroup::GinRowGroupRegistry;
        let reg = GinRowGroupRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("docs").unwrap();
        let file = "f1.parquet";
        reg.index_row(
            &project, &table, "payload",
            &extract_terms(&json!({"k": "v"}), "jsonb_ops"), file, 0,
        );
        reg.mark_file_indexed(&project, &table, "payload", file);
        let out = rowgroup_prune_for_containment(
            &reg, &project, &table, "payload", "jsonb_ops", b"{}",
            &[file.to_string()],
        );
        assert_eq!(out, RowGroupPrune::Unknown, "empty needle prunes nothing");
    }

    // ── Capability 2 — trigram candidate prune ───────────────────────────────

    #[test]
    fn trigrams_for_pattern_prefix_suffix_middle() {
        // Prefix `foo%` → run "foo".
        assert!(trigrams_for_pattern("foo%", false).contains(&[b'f', b'o', b'o']));
        // Suffix `%bar` → run "bar".
        assert!(trigrams_for_pattern("%bar", false).contains(&[b'b', b'a', b'r']));
        // Middle `%mid%` → run "mid".
        assert!(trigrams_for_pattern("%mid%", false).contains(&[b'm', b'i', b'd']));
        // Short run < 3 → no constraint.
        assert!(trigrams_for_pattern("%ab%", false).is_empty());
        // ILIKE folds case.
        assert!(trigrams_for_pattern("%ABC%", true).contains(&[b'a', b'b', b'c']));
    }

    fn rows() -> Vec<(u64, &'static str)> {
        vec![
            (0, "alice@example.com"),
            (1, "bob@gmail.com"),
            (2, "carol@gmail.com"),
            (3, "dave@example.org"),
            (4, "EVE@GMAIL.COM"),
        ]
    }

    #[test]
    fn trigram_candidates_suffix_superset_recheck() {
        // `%@gmail.com` (case-sensitive): rows 1,2 match; row 4 is uppercase.
        let cand = trigram_candidates(rows(), "%@gmail.com", false);
        let set = match cand {
            TrigramCandidates::Some(s) => s,
            TrigramCandidates::All => panic!("expected pruned set"),
        };
        // Superset must include the true matches (1, 2). Re-check narrows.
        assert!(set.contains(&1) && set.contains(&2), "missing true matches: {set:?}");
        let matched: Vec<u64> = rows()
            .into_iter()
            .filter(|(id, t)| set.contains(id) && like_matches(t, "%@gmail.com", false))
            .map(|(id, _)| id)
            .collect();
        assert_eq!(matched, vec![1, 2], "case-sensitive LIKE re-check");
    }

    #[test]
    fn trigram_candidates_ilike_case_insensitive() {
        // ILIKE `%@gmail.com` matches rows 1,2,4 (4 is uppercase).
        let cand = trigram_candidates(rows(), "%@gmail.com", true);
        let set = match cand {
            TrigramCandidates::Some(s) => s,
            TrigramCandidates::All => panic!("expected pruned set"),
        };
        let mut matched: Vec<u64> = rows()
            .into_iter()
            .filter(|(id, t)| set.contains(id) && like_matches(t, "%@gmail.com", true))
            .map(|(id, _)| id)
            .collect();
        matched.sort_unstable();
        assert_eq!(matched, vec![1, 2, 4], "ILIKE re-check folds case");
    }

    #[test]
    fn trigram_candidates_no_match_pattern_empty() {
        let cand = trigram_candidates(rows(), "%zzz%", false);
        match cand {
            TrigramCandidates::Some(s) => assert!(s.is_empty(), "no row has 'zzz': {s:?}"),
            TrigramCandidates::All => panic!("'zzz' is a usable trigram; expected pruned empty set"),
        }
    }

    #[test]
    fn trigram_candidates_short_pattern_falls_back_to_all() {
        // `%a%` has no literal run ≥ 3 → cannot prune → All (scan everything).
        assert_eq!(trigram_candidates(rows(), "%a%", false), TrigramCandidates::All);
        assert_eq!(trigram_candidates(rows(), "_b_", false), TrigramCandidates::All);
    }

    #[test]
    fn like_matches_semantics() {
        assert!(like_matches("hello world", "hello%", false));
        assert!(like_matches("hello world", "%world", false));
        assert!(like_matches("hello world", "%lo wo%", false));
        assert!(like_matches("hello", "h_llo", false));
        assert!(!like_matches("hello", "h_lo", false));
        assert!(!like_matches("hello", "world%", false));
        // ILIKE folds case.
        assert!(like_matches("HELLO", "hello", true));
        assert!(!like_matches("HELLO", "hello", false));
        // Escaped wildcard is literal.
        assert!(like_matches("50%", "50\\%", false));
        assert!(!like_matches("500", "50\\%", false));
        // Trailing % matches empty.
        assert!(like_matches("foo", "foo%", false));
    }

    // ── PG-Wave-β — spatial predicate detection ──────────────────────────────

    fn parse_where(sql: &str) -> sqlparser::ast::Expr {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let q = match &stmts[0] {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("not a query"),
        };
        let sel = match q.body.as_ref() {
            sqlparser::ast::SetExpr::Select(s) => s,
            _ => panic!("not a select"),
        };
        sel.selection.clone().expect("WHERE missing")
    }

    #[test]
    fn detect_dwithin_col_lhs() {
        let w = parse_where(
            "SELECT * FROM t WHERE ST_DWithin(geom, ST_MakePoint(2.3, 48.8), 1000)",
        );
        match detect_spatial_predicate(&w) {
            Some(SpatialPredicate::DWithin { col, x, y, radius_m }) => {
                assert_eq!(col, "geom");
                assert!((x - 2.3).abs() < 1e-9);
                assert!((y - 48.8).abs() < 1e-9);
                assert!((radius_m - 1000.0).abs() < 1e-9);
            }
            other => panic!("expected DWithin, got {other:?}"),
        }
    }

    #[test]
    fn detect_dwithin_col_rhs_commutative() {
        let w = parse_where(
            "SELECT * FROM t WHERE ST_DWithin(ST_MakePoint(0.0, 0.0), p, 500)",
        );
        match detect_spatial_predicate(&w) {
            Some(SpatialPredicate::DWithin { col, x, y, radius_m }) => {
                assert_eq!(col, "p");
                assert_eq!(x, 0.0);
                assert_eq!(y, 0.0);
                assert_eq!(radius_m, 500.0);
            }
            other => panic!("expected DWithin, got {other:?}"),
        }
    }

    #[test]
    fn detect_point_eq() {
        let w = parse_where("SELECT * FROM t WHERE geom = ST_MakePoint(1.0, 2.0)");
        match detect_spatial_predicate(&w) {
            Some(SpatialPredicate::PointEq { col, x, y }) => {
                assert_eq!(col, "geom");
                assert_eq!(x, 1.0);
                assert_eq!(y, 2.0);
            }
            other => panic!("expected PointEq, got {other:?}"),
        }
    }

    #[test]
    fn detect_st_contains_envelope() {
        let w = parse_where(
            "SELECT * FROM t WHERE ST_Contains(ST_MakeEnvelope(0.0, 0.0, 10.0, 10.0), geom)",
        );
        match detect_spatial_predicate(&w) {
            Some(SpatialPredicate::BboxIntersects { col, min_x, min_y, max_x, max_y }) => {
                assert_eq!(col, "geom");
                assert_eq!(min_x, 0.0);
                assert_eq!(min_y, 0.0);
                assert_eq!(max_x, 10.0);
                assert_eq!(max_y, 10.0);
            }
            other => panic!("expected BboxIntersects, got {other:?}"),
        }
    }

    #[test]
    fn detect_dwithin_via_and_clause() {
        // The probe should pick the spatial arm out of an AND chain so that
        // composite WHERE clauses still get the row-group prune.
        let w = parse_where(
            "SELECT * FROM t WHERE id > 0 AND ST_DWithin(geom, ST_MakePoint(0.0, 0.0), 100)",
        );
        match detect_spatial_predicate(&w) {
            Some(SpatialPredicate::DWithin { col, .. }) => assert_eq!(col, "geom"),
            other => panic!("expected DWithin via AND, got {other:?}"),
        }
    }

    #[test]
    fn detect_no_match_col_on_col() {
        // col-on-col comparisons can't be pruned by an R-tree.
        let w = parse_where("SELECT * FROM t WHERE a = b");
        assert!(detect_spatial_predicate(&w).is_none());
    }

    #[test]
    fn detect_no_match_non_literal_radius() {
        // Non-literal radius (a column) can't be pruned.
        let w = parse_where(
            "SELECT * FROM t WHERE ST_DWithin(geom, ST_MakePoint(0.0, 0.0), some_col)",
        );
        assert!(detect_spatial_predicate(&w).is_none());
    }
}
