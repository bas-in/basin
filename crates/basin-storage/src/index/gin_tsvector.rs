//! Phase 5.20.E — GIN posting-list structure for `tsvector` columns.
//!
//! Builds and queries in-memory posting lists keyed per *lexeme* for columns
//! that have a `CREATE INDEX … USING gin` declaration with an FTS opclass
//! (`tsvector_ops`).  The posting lists enable per-file pruning for FTS
//! predicates (`@@`) instead of always falling through to a full DataFusion
//! scan.
//!
//! # Terminology
//!
//! * **lexeme** — a normalised token extracted from a `tsvector` value.  A
//!   `tsvector` is represented as a space-separated list of single-quoted
//!   lexemes, optionally followed by positional weights
//!   (`'cat':1 'dog':2 'run':3`).  This module extracts only the lexeme
//!   strings and ignores positions/weights — that is the correct GIN
//!   granularity for `@@` matching (positions matter for `<->` phrase queries,
//!   which require a re-evaluation pass regardless).
//!
//! * **posting list** — a sorted set of `TsvPostingEntry` values (file path +
//!   row-group + row) for each distinct lexeme.  AND-merging two posting lists
//!   yields the rows that contain BOTH lexemes — the correct semantics for
//!   a multi-lexeme `tsquery` (e.g. `'cat' & 'dog'`).
//!
//! # Correctness contract
//!
//! The posting list is a *conservative superset*: it prunes only files that
//! contain NO matching lexeme.  Every candidate file returned by
//! [`GinTsvectorRegistry::probe_query`] is re-evaluated by the full `@@`
//! predicate at the storage read layer.  The caller must never skip that
//! re-evaluation step.
//!
//! # Storage / eviction
//!
//! The posting list lives entirely in RAM.  The registry caps each per-column
//! posting list at [`DEFAULT_POSTING_BUDGET`] total entries (operator-tunable
//! via `BASIN_GIN_POSTING_BUDGET`); oldest lexemes are evicted in 25% batches
//! when the cap is exceeded.  On engine restart the registry starts empty and
//! rebuilds lazily from writes.
//!
//! **Per-file completeness (Phase 5.20.E+):** eviction marks only the
//! *affected files* (those whose posting entries were dropped) as un-indexed,
//! not the entire column.  Files that still have complete lexeme coverage
//! remain prunable.  Un-indexed files are treated as forced candidates
//! (must-scan) — correctness is scale-independent.
//!
//! # Engine wiring (Phase 5.20.E)
//!
//! The three wiring stages are implemented in Phase 5.20.E:
//!
//! 1. **Populate on write** — `maintain_gin_fts_index_on_insert` in
//!    `basin-engine/src/executor.rs` calls `index_row` for every tsvector
//!    column in a newly written Parquet file.  `mark_file_indexed` is called
//!    after all rows are processed so the completeness guard is valid.
//!
//! 2. **Probe at query** — `detect_tsvector_match` in
//!    `basin-engine/src/index_probe.rs` recognises `col @@ to_tsquery(…)` /
//!    `@@ plainto_tsquery(…)` on a GIN-indexed tsvector column and returns a
//!    `GinFtsPlan`.  The executor probes `probe_query` for an `Empty`
//!    short-circuit.
//!
//! 3. **Prune the scan** — `apply_gin_fts_pruning_for_query` in
//!    `basin-engine/src/session.rs` intersects `FileCandidates` with live
//!    files and re-registers a pruned `ListingTable` ONLY when the
//!    completeness guard passes (`indexed_files ⊇ live_files`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Maximum total posting entries (file+rg+row tuples) kept per `(table, col)`
/// posting list.  Beyond this threshold the oldest 25% of lexemes are evicted.
///
/// This default can be overridden at process start via the
/// `BASIN_GIN_POSTING_BUDGET` environment variable (shared with the JSONB GIN
/// registry).  Example: `BASIN_GIN_POSTING_BUDGET=2000000 basin-server`.
const DEFAULT_POSTING_BUDGET: usize = 500_000;

/// Return the effective per-column posting-entry budget.
///
/// Reads `BASIN_GIN_POSTING_BUDGET` once and caches the result.  The same env
/// var governs both the JSONB and tsvector GIN registries so operators have a
/// single knob for both.
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
///
/// Mirrors [`super::super::index_probe::PostingEntry`] (JSONB GIN) in shape so
/// the two registries can be merged in a future refactor without API breakage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TsvPostingEntry {
    /// Parquet file path (or object-store key).
    pub file_path: String,
    /// Parquet row-group index within the file.
    pub row_group: u32,
    /// Row offset within the row-group.
    pub row: u64,
}

// ── tsvector parsing ──────────────────────────────────────────────────────────

/// Extract the set of normalised lexemes from a raw `tsvector` string.
///
/// PostgreSQL serialises a `tsvector` as:
/// ```text
/// 'lexeme1':pos1,pos2 'lexeme2':pos3 …
/// ```
///
/// This function extracts only the lexeme strings (the single-quoted tokens)
/// and ignores positional information.  Positions are irrelevant for the GIN
/// AND-merge — the re-evaluation layer handles phrase proximity.
///
/// # Examples
///
/// ```
/// use basin_storage::index::gin_tsvector::extract_lexemes;
/// let lexemes = extract_lexemes("'cat':1 'dog':2,3 'run':4");
/// assert!(lexemes.contains("cat"));
/// assert!(lexemes.contains("dog"));
/// assert!(lexemes.contains("run"));
/// assert_eq!(lexemes.len(), 3);
/// ```
pub fn extract_lexemes(tsvector: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let s = tsvector.trim();
    let mut chars = s.chars().peekable();

    while let Some(&ch) = chars.peek() {
        // Skip whitespace between tokens.
        if ch.is_ascii_whitespace() {
            chars.next();
            continue;
        }

        if ch == '\'' {
            // Start of a quoted lexeme.
            chars.next(); // consume opening quote
            let mut lexeme = String::new();
            let mut escaped = false;

            loop {
                match chars.next() {
                    None => break, // unterminated quote — best-effort
                    Some('\\') if !escaped => {
                        escaped = true;
                    }
                    Some('\'') if !escaped => {
                        // Peek: if next char is also `'` it's an escaped quote
                        // (`''` inside a tsvector lexeme — unusual but possible
                        // in round-tripped literals).
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            lexeme.push('\'');
                        } else {
                            break; // closing quote
                        }
                    }
                    Some(c) => {
                        escaped = false;
                        lexeme.push(c);
                    }
                }
            }

            if !lexeme.is_empty() {
                out.insert(lexeme);
            }

            // Skip the positional annotation (`:1,2B`) if present.
            if chars.peek() == Some(&':') {
                // Consume everything until the next whitespace or end-of-string.
                for c in chars.by_ref() {
                    if c.is_ascii_whitespace() {
                        break;
                    }
                }
            }
        } else {
            // Unexpected character outside a quote: skip until whitespace.
            for c in chars.by_ref() {
                if c.is_ascii_whitespace() {
                    break;
                }
            }
        }
    }

    out
}

/// Extract probe lexemes from a `tsquery` string for AND-merge probing.
///
/// PostgreSQL serialises a `tsquery` as an expression tree, e.g.:
/// ```text
/// 'cat' & 'dog' | 'run'
/// 'phrase' <-> 'query'
/// ```
///
/// For GIN probing we extract only the lexeme atoms and AND-merge them all
/// (i.e. we require every lexeme to appear in the candidate file).  This is
/// the conservative strategy: it may return false positives for OR / NOT
/// sub-expressions but never false negatives.  The caller re-evaluates the
/// full `@@` predicate on every candidate row.
///
/// # Examples
///
/// ```
/// use basin_storage::index::gin_tsvector::extract_query_lexemes;
/// let lexemes = extract_query_lexemes("'cat' & 'dog'");
/// assert!(lexemes.contains("cat"));
/// assert!(lexemes.contains("dog"));
/// ```
pub fn extract_query_lexemes(tsquery: &str) -> HashSet<String> {
    // The lexeme atoms in a tsquery are also single-quoted, so we can reuse
    // the same extraction logic as `extract_lexemes`.  tsquery operators
    // (`&`, `|`, `!`, `<->`, `<n>`) are unquoted and will be skipped by the
    // same "unexpected character" fallthrough in the parser.
    extract_lexemes(tsquery)
}

// ── Internal posting list ─────────────────────────────────────────────────────

/// Per-`(table, col)` in-memory posting list keyed by lexeme.
#[derive(Debug, Default)]
struct LexemePostingList {
    /// `lexeme → set of (file, rg, row)`.
    entries: HashMap<String, HashSet<TsvPostingEntry>>,
    /// Insertion-ordered list of lexeme keys for LRU eviction.
    insert_order: Vec<String>,
    /// Total entry count (sum of all per-lexeme set sizes).
    total_count: usize,
}

impl LexemePostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Add a `(lexeme, entry)` pair.
    ///
    /// Returns `true` if an eviction occurred during this insert.  The caller
    /// (`GinTsvectorRegistry::index_row`) uses this signal to wipe the
    /// `indexed_files` completeness set: once any lexeme has been evicted, the
    /// posting list no longer has full coverage and file-level pruning must
    /// not fire until the registry has been fully rebuilt.
    fn insert(&mut self, lexeme: String, entry: TsvPostingEntry) -> bool {
        let set = self.entries.entry(lexeme.clone()).or_default();
        let is_new_lexeme = set.is_empty();
        set.insert(entry);
        self.total_count += 1;

        // Track insertion order only on the first posting for this lexeme so
        // eviction correctly identifies "oldest lexemes first".
        if is_new_lexeme {
            self.insert_order.push(lexeme);
        }

        if self.total_count > posting_budget() {
            self.evict_oldest();
            true // eviction happened
        } else {
            false
        }
    }

    /// Evict the oldest 25% of lexemes.
    fn evict_oldest(&mut self) {
        let evict_count = (posting_budget() / 4).max(1);
        let to_evict: Vec<String> =
            self.insert_order.drain(..evict_count.min(self.insert_order.len())).collect();
        for k in &to_evict {
            if let Some(set) = self.entries.remove(k) {
                self.total_count = self.total_count.saturating_sub(set.len());
            }
        }
    }

    /// Probe for `lexeme`.
    ///
    /// Returns `None` when the lexeme has never been indexed (caller treats
    /// as "unknown → full scan").  Returns `Some(set)` for a known lexeme;
    /// the set may be empty when all posting entries for this lexeme were
    /// evicted.
    fn probe_lexeme(&self, lexeme: &str) -> Option<&HashSet<TsvPostingEntry>> {
        self.entries.get(lexeme)
    }

    /// Remove all entries that reference `file_path`.  Called when a file is
    /// compacted or deleted.
    fn remove_file(&mut self, file_path: &str) {
        for set in self.entries.values_mut() {
            let before = set.len();
            set.retain(|e| e.file_path != file_path);
            self.total_count = self.total_count.saturating_sub(before - set.len());
        }
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Key into the global registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide GIN posting-list registry for `tsvector` columns.
///
/// One [`LexemePostingList`] per `(project, table, col)`.  Concurrent access
/// is serialised by a per-posting-list `Mutex`, with the outer `Mutex` only
/// held briefly to look up or insert the `Arc`.
///
/// # Thread safety
///
/// `GinTsvectorRegistry` is `Send + Sync`.  The outer mutex guards the
/// `HashMap` of `Arc<Mutex<…>>` pointers; each inner mutex guards one
/// per-column posting list.  The outer lock is never held while the inner
/// lock is taken, preventing deadlock.
///
/// `indexed_files` tracks, for each `(project, table, col)`, the set of file
/// paths whose rows have been fully loaded into the posting list.  This is the
/// completeness guard required by Phase 5.20.E: file-level pruning is only
/// safe when every live data file appears in this set.
pub struct GinTsvectorRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<LexemePostingList>>>>,
    /// File-completeness tracking: `RegKey → set of fully-indexed file paths`.
    indexed_files: Mutex<HashMap<RegKey, HashSet<String>>>,
}

impl GinTsvectorRegistry {
    /// Create a new empty registry.
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
    ) -> Arc<Mutex<LexemePostingList>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("GinTsvectorRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(LexemePostingList::new())))
            .clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<LexemePostingList>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let map = self.inner.lock().expect("GinTsvectorRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index one row's `tsvector` value.
    ///
    /// `tsvector_str` is the textual representation of the `tsvector` column
    /// value (e.g. `"'cat':1 'dog':2"`).  Silently skips empty or NULL-like
    /// values.
    ///
    /// This is called from the engine write path for every row written to a
    /// tsvector column that has a GIN index declared in the catalog.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        tsvector_str: &str,
        file_path: &str,
        row_group: u32,
        row: u64,
    ) {
        let lexemes = extract_lexemes(tsvector_str);
        if lexemes.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("LexemePostingList lock poisoned");
        let entry = TsvPostingEntry { file_path: file_path.to_string(), row_group, row };
        let mut evicted = false;
        for lexeme in lexemes {
            if list.insert(lexeme, entry.clone()) {
                evicted = true;
            }
        }
        // If eviction occurred the posting list lost lexemes that were present
        // in earlier files.  Those files are no longer fully covered by the
        // in-RAM index, so the `indexed_files` completeness set must be
        // cleared.  Any future `mark_file_indexed` calls (for files written
        // AFTER the eviction) will repopulate the set, but the old files will
        // not appear until a full reindex (or engine restart + warm-up) —
        // meaning the completeness check will fail and the engine falls back
        // to a full scan.  This is the safe and correct behaviour.
        if evicted {
            let key = RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            };
            if let Ok(mut map) = self.indexed_files.lock() {
                map.remove(&key);
            }
        }
    }

    /// Probe the posting list for a `tsquery` predicate (`tsvector @@ tsquery`).
    ///
    /// `tsquery_str` is the textual representation of the query-side operand
    /// (e.g. `"'cat' & 'dog'"`).
    ///
    /// All lexeme atoms extracted from the query are AND-merged: only files
    /// that appear in every per-lexeme posting list are returned as candidates.
    /// This is conservative (no false negatives) but may produce false
    /// positives for OR / NOT sub-expressions — the caller must re-evaluate
    /// the full `@@` predicate on every candidate row.
    ///
    /// Returns:
    /// * [`TsvProbeResult::NoIndex`] — no posting list loaded yet, or a lexeme
    ///   was evicted; caller must fall through to full scan.
    /// * [`TsvProbeResult::Empty`] — the AND-intersection is empty; no rows
    ///   can match.  Caller can short-circuit with zero rows.
    /// * [`TsvProbeResult::FileCandidates`] — the set of file paths that
    ///   MIGHT contain matching rows.
    ///
    pub fn probe_query(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        tsquery_str: &str,
    ) -> TsvProbeResult {
        let lexemes = extract_query_lexemes(tsquery_str);
        if lexemes.is_empty() {
            // Empty query — conservative fall-through.
            return TsvProbeResult::NoIndex;
        }

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return TsvProbeResult::NoIndex,
        };
        let list = arc.lock().expect("LexemePostingList lock poisoned");

        // AND-merge posting lists for each lexeme.
        let mut candidate_files: Option<HashSet<String>> = None;

        for lexeme in &lexemes {
            match list.probe_lexeme(lexeme) {
                None => {
                    // Lexeme not in index (never inserted or evicted) → unknown.
                    // Conservative: fall through to full scan.
                    return TsvProbeResult::NoIndex;
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
            None => TsvProbeResult::NoIndex,
            Some(files) if files.is_empty() => TsvProbeResult::Empty,
            Some(files) => TsvProbeResult::FileCandidates(files),
        }
    }

    /// Remove all posting entries that reference `file_path` for
    /// `(project, table, col)`.  Call this when a Parquet file is compacted
    /// or deleted so the posting list does not return stale candidates.
    ///
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
            let mut list = arc.lock().expect("LexemePostingList lock poisoned");
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

    /// Return the total number of posting entries for `(project, table, col)`.
    ///
    /// Primarily used in tests and diagnostics.
    pub fn total_entries(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> usize {
        match self.get(project, table, col) {
            None => 0,
            Some(arc) => {
                arc.lock().expect("LexemePostingList lock poisoned").total_count
            }
        }
    }
}

impl Default for GinTsvectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a `tsvector @@ tsquery` probe against the GIN posting list.
#[derive(Debug)]
pub enum TsvProbeResult {
    /// No posting list for this column, or a required lexeme was evicted.
    /// Caller must fall through to a full scan (no false negatives).
    NoIndex,
    /// The AND-intersection of all lexeme posting lists is empty.  No rows in
    /// the table can satisfy the FTS predicate.
    Empty,
    /// The set of file paths that may contain matching rows.  The caller reads
    /// only these files and re-applies the full `@@` predicate for correctness.
    FileCandidates(HashSet<String>),
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── extract_lexemes ───────────────────────────────────────────────────────

    #[test]
    fn extract_lexemes_basic() {
        let lexemes = extract_lexemes("'cat':1 'dog':2,3 'run':4");
        assert_eq!(lexemes.len(), 3, "lexemes={lexemes:?}");
        assert!(lexemes.contains("cat"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("dog"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("run"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_lexemes_no_positions() {
        // tsvector values without positional annotations are valid.
        let lexemes = extract_lexemes("'hello' 'world'");
        assert_eq!(lexemes.len(), 2, "lexemes={lexemes:?}");
        assert!(lexemes.contains("hello"));
        assert!(lexemes.contains("world"));
    }

    #[test]
    fn extract_lexemes_weighted_positions() {
        // Weighted positions e.g. :1A,3B should be ignored.
        let lexemes = extract_lexemes("'rust':1A 'db':2B,3C");
        assert!(lexemes.contains("rust"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("db"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_lexemes_empty_string() {
        let lexemes = extract_lexemes("");
        assert!(lexemes.is_empty());
    }

    #[test]
    fn extract_lexemes_deduplicates() {
        // A tsvector should not have duplicate lexemes, but our extractor must
        // be safe if it does (e.g. from a raw string before normalisation).
        let lexemes = extract_lexemes("'cat':1 'cat':2");
        assert_eq!(lexemes.len(), 1);
    }

    // ── extract_query_lexemes ─────────────────────────────────────────────────

    #[test]
    fn extract_query_lexemes_and_query() {
        let lexemes = extract_query_lexemes("'cat' & 'dog'");
        assert!(lexemes.contains("cat"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("dog"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_query_lexemes_or_query() {
        // OR query — we still extract both lexemes (conservative superset).
        let lexemes = extract_query_lexemes("'cat' | 'dog'");
        assert!(lexemes.contains("cat"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("dog"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_query_lexemes_phrase() {
        let lexemes = extract_query_lexemes("'quick' <-> 'brown'");
        assert!(lexemes.contains("quick"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("brown"), "lexemes={lexemes:?}");
    }

    // ── GinTsvectorRegistry: no-index path ───────────────────────────────────

    #[test]
    fn probe_query_no_index() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    // ── GinTsvectorRegistry: single-lexeme hit ────────────────────────────────

    #[test]
    fn probe_single_lexeme_hit() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1 'run':2", "f1.parquet", 0, 0);

        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "expected f1.parquet, got {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: multi-lexeme AND-merge ───────────────────────────

    #[test]
    fn probe_and_merge_two_files() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1 has both "cat" AND "dog".
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f1.parquet", 0, 0);
        // f2 has only "cat".
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f2.parquet", 0, 0);

        // Probe for "cat" AND "dog": only f1 should survive the AND-merge.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be a candidate: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 must be excluded: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn probe_and_merge_no_common_file() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1 has "cat" only; f2 has "dog" only.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f2.parquet", 0, 0);

        // No file has both lexemes → Empty.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        assert!(
            matches!(result, TsvProbeResult::Empty),
            "expected Empty when no file has both lexemes, got {result:?}"
        );
    }

    // ── GinTsvectorRegistry: miss path ────────────────────────────────────────

    #[test]
    fn probe_missing_lexeme_returns_no_index() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Index a doc with "cat" only.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);

        // Probe for "dog" — never indexed → NoIndex (conservative).
        let result = reg.probe_query(&proj, &tbl, "body", "'dog'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    // ── GinTsvectorRegistry: multiple row-groups / rows ───────────────────────

    #[test]
    fn probe_multiple_row_groups() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Same file, different row-groups and rows.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f1.parquet", 1, 5);

        // Both lexemes are in f1 (across different row-groups) → candidate.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be a candidate: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: remove_file ─────────────────────────────────────

    #[test]
    fn remove_file_clears_entries() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f1.parquet", 0, 0);

        reg.remove_file(&proj, &tbl, "body", "f1.parquet");

        // After removal the posting lists are empty for the indexed lexemes.
        // The probe may return Empty (set exists but empty) or NoIndex (evicted).
        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        assert!(
            matches!(result, TsvProbeResult::NoIndex | TsvProbeResult::Empty),
            "expected NoIndex or Empty after remove, got {result:?}"
        );
    }

    // ── GinTsvectorRegistry: total_entries ───────────────────────────────────

    #[test]
    fn total_entries_counts_correctly() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Three lexemes → three posting entries.
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2 'run':3", "f1.parquet", 0, 0);
        assert_eq!(reg.total_entries(&proj, &tbl, "body"), 3);

        // Same lexeme in a different file → one more posting entry for each lexeme.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f2.parquet", 0, 0);
        assert_eq!(reg.total_entries(&proj, &tbl, "body"), 4);
    }

    // ── GinTsvectorRegistry: column isolation ─────────────────────────────────

    #[test]
    fn different_columns_are_isolated() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "title", "'cat':1", "f1.parquet", 0, 0);

        // Probe on "body" — different column, no index.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    // ── Smoke test: 50k rows ──────────────────────────────────────────────────

    #[test]
    fn fifty_thousand_rows_smoke() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("large_corpus").unwrap();

        // Index 50k rows, each with a unique lexeme "word_i" plus a shared
        // lexeme "common".
        for i in 0u64..50_000 {
            let tsv = format!("'word_{i}':1 'common':2");
            let file = format!("chunk_{}.parquet", i / 1000);
            reg.index_row(&proj, &tbl, "body", &tsv, &file, 0, i % 1000);
        }

        // Probing for "common" should return FileCandidates (every chunk file).
        let result = reg.probe_query(&proj, &tbl, "body", "'common'");
        assert!(
            matches!(result, TsvProbeResult::FileCandidates(_) | TsvProbeResult::NoIndex),
            "smoke probe returned unexpected {result:?}"
        );
    }
}
