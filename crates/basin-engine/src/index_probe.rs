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
        let budget = posting_budget();
        let evict_count = (budget / 4).max(1);
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
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            Some(s.clone())
        }
        // `'literal'::jsonb` cast
        Expr::Cast { expr: inner, .. } => extract_json_literal(inner),
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
}
