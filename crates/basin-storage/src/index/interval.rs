//! Phase 5.24.D — interval-tree index for range containment/overlap pruning.
//!
//! # Overview
//!
//! Basin stores range-column values as Utf8 JSON strings. This module provides
//! a storage-side in-memory interval tree that allows the engine to prune files
//! (or row-groups) that cannot possibly satisfy a range-containment (`@>`) or
//! overlap (`&&`) predicate, without reading the Parquet data.
//!
//! # Data structure
//!
//! The index is a **segment / augmented interval tree** stored as a sorted
//! `Vec<IndexEntry>` keyed by `lo`. For each entry we also track the maximum
//! `hi` in the subtree to the right (the standard augmentation that allows
//! O(log N + k) stabbing queries). For our use-case (file-level pruning, not
//! row-level) we aggregate at the *file* granularity: one `IndexEntry` per
//! `(file, row_group)` pair, covering the union of all range intervals in that
//! row group.
//!
//! # Correctness contract
//!
//! The index is a *conservative superset*: it may return false positives (files
//! that look like candidates but whose rows don't actually match), but it never
//! returns false negatives. Every candidate file returned by
//! [`IntervalIndex::probe_contains_point`] or
//! [`IntervalIndex::probe_overlaps`] must be re-evaluated by the full
//! DataFusion predicate at the storage read layer.
//!
//! # Engine-probe wiring (Phase 5.24.D)
//!
//! Three wiring stages implemented in Phase 5.24.D:
//!
//! 1. **Populate on write** — `maintain_gist_index_on_insert` in
//!    `basin-engine/src/executor.rs` calls `index_row` for every range-typed
//!    column in a newly written Parquet file. `mark_file_indexed` is called
//!    after all rows are processed so the completeness guard is valid.
//!
//! 2. **Probe at query** — `detect_range_index_probe` in
//!    `basin-engine/src/index_probe.rs` recognises `col @> val` / `col && range`
//!    / `col <@ range` on a GIST-indexed range column and returns an
//!    `IntervalIndexPlan`. The executor probes the registry for an `Empty`
//!    short-circuit or `FileCandidates` for file-level pruning.
//!
//! 3. **Prune the scan** — `apply_gist_pruning_for_query` in
//!    `basin-engine/src/session.rs` intersects `FileCandidates` with live files
//!    and re-registers a pruned `ListingTable` ONLY when the completeness guard
//!    passes (`indexed_files ⊇ live_files`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::types::range::IndexInterval;
use basin_common::{ProjectId, TableName};

// ── Entry ─────────────────────────────────────────────────────────────────────

/// One interval entry in the index, associated with a storage location
/// (file + row-group).
#[derive(Clone, Debug, PartialEq)]
pub struct IntervalEntry {
    /// Low end of the half-open interval `[lo, hi)`.
    pub lo: f64,
    /// High end of the half-open interval `[lo, hi)`.
    pub hi: f64,
    /// Parquet file path (or object-store key).
    pub file_path: String,
    /// Parquet row-group index within the file.
    pub row_group: u32,
}

// ── Augmented interval tree ───────────────────────────────────────────────────

/// In-memory augmented interval tree for a single `(project, table, col)`.
///
/// Entries are kept sorted by `lo`. Each entry also contributes to a
/// running `max_hi` so that point / overlap probes can prune subtrees.
///
/// The tree is rebuilt (re-sorted) on demand via [`Self::rebuild`] after a
/// batch of insertions. This is appropriate because range-type columns are
/// written in bulk (whole Parquet row-groups) and the index is queried
/// frequently but updated infrequently.
#[derive(Debug, Default)]
pub struct IntervalTree {
    /// Entries sorted by `lo`. Rebuilt by [`Self::rebuild`].
    sorted: Vec<IntervalEntry>,
    /// Per-position running maximum `hi` (used for subtree pruning).
    /// `max_hi[i]` = max(`hi`) for all entries in `sorted[i..]`.
    max_hi: Vec<f64>,
    /// Entries accumulated since the last rebuild.
    pending: Vec<IntervalEntry>,
    /// Total entry count (sorted + pending).
    total: usize,
}

impl IntervalTree {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new interval.
    ///
    /// The entry is added to the `pending` list. Call [`Self::rebuild`] to
    /// sort and augment the tree before probing.
    pub fn insert(&mut self, entry: IntervalEntry) {
        self.total += 1;
        self.pending.push(entry);
        // Rebuild incrementally when the pending list grows large.
        if self.pending.len() >= 512 {
            self.rebuild();
        }
    }

    /// Merge pending entries into the sorted list and recompute augmentation.
    pub fn rebuild(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.sorted.append(&mut self.pending);
        self.sorted
            .sort_by(|a, b| a.lo.partial_cmp(&b.lo).unwrap_or(std::cmp::Ordering::Equal));
        // Compute running max_hi from right to left.
        let n = self.sorted.len();
        self.max_hi = vec![f64::NEG_INFINITY; n];
        let mut running = f64::NEG_INFINITY;
        for i in (0..n).rev() {
            running = running.max(self.sorted[i].hi);
            self.max_hi[i] = running;
        }
    }

    /// Probe: return all *distinct* file paths whose intervals contain `point`.
    ///
    /// `point` is inside `[lo, hi)` iff `lo <= point < hi`.
    pub fn probe_contains_point(&mut self, point: f64) -> ProbeResult {
        // Flush any pending entries.
        if !self.pending.is_empty() {
            self.rebuild();
        }
        if self.sorted.is_empty() {
            return ProbeResult::NoIndex;
        }
        let mut files = std::collections::HashSet::new();
        for (i, entry) in self.sorted.iter().enumerate() {
            // Prune: if max_hi[i] <= point, no entry from here on can contain point.
            if self.max_hi[i] <= point {
                break;
            }
            // Prune: if entry.lo > point, and entries are sorted by lo, we're done.
            if entry.lo > point {
                break;
            }
            if entry.lo <= point && point < entry.hi {
                files.insert(entry.file_path.clone());
            }
        }
        if files.is_empty() {
            ProbeResult::Empty
        } else {
            ProbeResult::FileCandidates(files)
        }
    }

    /// Probe: return all *distinct* file paths whose intervals overlap `query`.
    ///
    /// Two half-open intervals `[a,b)` and `[c,d)` overlap iff `a < d && c < b`.
    pub fn probe_overlaps(&mut self, query: &IndexInterval) -> ProbeResult {
        if !self.pending.is_empty() {
            self.rebuild();
        }
        if self.sorted.is_empty() {
            return ProbeResult::NoIndex;
        }
        let mut files = std::collections::HashSet::new();
        for (i, entry) in self.sorted.iter().enumerate() {
            // Prune: if all remaining max_hi <= query.lo, no overlap possible.
            if self.max_hi[i] <= query.lo {
                break;
            }
            // If entry.lo >= query.hi, all remaining entries start at or after
            // the query's upper bound — no overlap.
            if entry.lo >= query.hi {
                break;
            }
            if entry.lo < query.hi && query.lo < entry.hi {
                files.insert(entry.file_path.clone());
            }
        }
        if files.is_empty() {
            ProbeResult::Empty
        } else {
            ProbeResult::FileCandidates(files)
        }
    }

    /// Remove all entries for `file_path`. Call after a file is compacted or
    /// deleted so stale candidates are not returned.
    pub fn remove_file(&mut self, file_path: &str) {
        self.sorted.retain(|e| e.file_path != file_path);
        self.pending.retain(|e| e.file_path != file_path);
        self.total = self.sorted.len() + self.pending.len();
        // Recompute max_hi since entries changed.
        if !self.sorted.is_empty() {
            let n = self.sorted.len();
            self.max_hi = vec![f64::NEG_INFINITY; n];
            let mut running = f64::NEG_INFINITY;
            for i in (0..n).rev() {
                running = running.max(self.sorted[i].hi);
                self.max_hi[i] = running;
            }
        } else {
            self.max_hi.clear();
        }
    }

    /// Return the total number of indexed entries.
    pub fn total_entries(&self) -> usize {
        self.total
    }
}

// ── ProbeResult ───────────────────────────────────────────────────────────────

/// Result of an interval probe against the index.
#[derive(Debug)]
pub enum ProbeResult {
    /// No entries have been indexed yet for this column. The caller must fall
    /// through to a full scan (no false negatives).
    NoIndex,
    /// The intersection of candidate sets is empty — no rows can match.
    /// The caller can short-circuit with zero results.
    Empty,
    /// Set of file paths that *may* contain matching rows. The caller must
    /// re-evaluate the full predicate on every candidate row.
    FileCandidates(std::collections::HashSet<String>),
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Registry key — identifies one range-typed column in one table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide interval-tree registry for range-type columns.
///
/// One [`IntervalTree`] per `(project, table, col)`. Thread-safe via a
/// two-level locking scheme identical to [`super::gin_tsvector::GinTsvectorRegistry`]:
/// the outer `Mutex` is held only for `HashMap` lookups; the inner `Mutex`
/// guards the tree itself and is never held while the outer lock is taken.
///
/// `indexed_files` tracks, for each `(project, table, col)`, the set of file
/// paths whose rows have been fully loaded into the interval tree. This is the
/// completeness guard required by Phase 5.24.D: file-level pruning is only
/// safe when every live data file appears in this set.
pub struct IntervalRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<IntervalTree>>>>,
    /// File-completeness tracking: `RegKey → set of fully-indexed file paths`.
    indexed_files: Mutex<HashMap<RegKey, HashSet<String>>>,
}

impl IntervalRegistry {
    /// Create an empty registry.
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
    ) -> Arc<Mutex<IntervalTree>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let mut map = self
            .inner
            .lock()
            .expect("IntervalRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(IntervalTree::new())))
            .clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<IntervalTree>>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let map = self
            .inner
            .lock()
            .expect("IntervalRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index one row's range-type column value.
    ///
    /// `range_str` accepts both the Basin JSON storage form
    /// (e.g. `{"l":1,"u":10,"li":true,"ui":false}`) and the PG text form
    /// (e.g. `[1,10)` / `(1,10]`). The PG text form is what Basin stores
    /// in Parquet and what INSERT batches carry before any UDF conversion.
    /// Only finite-bound ranges can be indexed; infinite-bound ranges are
    /// silently skipped (they match everything — no pruning benefit).
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        range_str: &str,
        file_path: &str,
        row_group: u32,
    ) {
        use basin_common::types::range::{IndexInterval, RangeValue};
        let trimmed = range_str.trim();
        let rv = if trimmed.starts_with('{') {
            RangeValue::from_json_str(trimmed)
        } else {
            RangeValue::from_pg_text(trimmed)
        };
        let rv = match rv {
            Some(r) => r,
            None => return,
        };
        let iv = match IndexInterval::from_range(&rv) {
            Some(iv) => iv,
            None => return,
        };
        let arc = self.get_or_create(project, table, col);
        let mut tree = arc.lock().expect("IntervalTree lock poisoned");
        tree.insert(IntervalEntry {
            lo: iv.lo,
            hi: iv.hi,
            file_path: file_path.to_string(),
            row_group,
        });
    }

    /// Probe: return file paths whose indexed intervals contain `point`.
    /// Called from the engine `@> scalar` probe path.
    pub fn probe_contains_point(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        point: f64,
    ) -> ProbeResult {
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return ProbeResult::NoIndex,
        };
        let mut tree = arc.lock().expect("IntervalTree lock poisoned");
        tree.probe_contains_point(point)
    }

    /// Probe: return file paths whose indexed intervals overlap `query`.
    /// Called from the engine `&&` / `<@` probe path.
    pub fn probe_overlaps(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        query: &IndexInterval,
    ) -> ProbeResult {
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return ProbeResult::NoIndex,
        };
        let mut tree = arc.lock().expect("IntervalTree lock poisoned");
        tree.probe_overlaps(query)
    }

    /// Remove all indexed entries for a given file. Call when a Parquet file
    /// is compacted or deleted.
    ///
    /// Also removes `file_path` from the indexed-files completeness set so
    /// future probes do not erroneously claim full coverage after this file
    /// is gone.
    pub fn remove_file(&self, project: &ProjectId, table: &TableName, col: &str, file_path: &str) {
        if let Some(arc) = self.get(project, table, col) {
            let mut tree = arc.lock().expect("IntervalTree lock poisoned");
            tree.remove_file(file_path);
        }
        // Remove from completeness tracking.
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

    /// Record that `file_path` has been fully indexed for `(project, table,
    /// col)`. Called immediately after all rows in a new file have been passed
    /// to [`index_row`]. This is the write side of the completeness guard: a
    /// file that appears here is safe to use as a prune boundary.
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

    /// Return the set of file paths that have been completely indexed for
    /// `(project, table, col)`. The caller uses this to decide whether the
    /// interval index provides FULL coverage of the live file set:
    ///
    /// ```text
    /// completeness = indexed_files ⊇ live_files
    /// ```
    ///
    /// If `completeness` is true, file-level pruning to the interval candidate
    /// set is safe. If any live file is missing from this set, pruning must NOT
    /// happen (full scan instead, no false negatives).
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

    /// Return the total number of indexed entries for a `(project, table, col)`.
    pub fn total_entries(&self, project: &ProjectId, table: &TableName, col: &str) -> usize {
        match self.get(project, table, col) {
            None => 0,
            Some(arc) => arc
                .lock()
                .expect("IntervalTree lock poisoned")
                .total_entries(),
        }
    }
}

impl Default for IntervalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tree(intervals: &[(f64, f64, &str)]) -> IntervalTree {
        let mut tree = IntervalTree::new();
        for &(lo, hi, file) in intervals {
            tree.insert(IntervalEntry {
                lo,
                hi,
                file_path: file.to_string(),
                row_group: 0,
            });
        }
        tree.rebuild();
        tree
    }

    // ── IntervalTree: contains_point ─────────────────────────────────────────

    #[test]
    fn point_probe_basic_hit() {
        let mut tree = make_tree(&[(1.0, 10.0, "f1.parquet")]);
        let result = tree.probe_contains_point(5.0);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(
                    files.contains("f1.parquet"),
                    "expected f1.parquet: {files:?}"
                );
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn point_probe_exclusive_upper_miss() {
        let mut tree = make_tree(&[(1.0, 10.0, "f1.parquet")]);
        let result = tree.probe_contains_point(10.0);
        // 10.0 is the exclusive upper bound — must not match.
        assert!(
            matches!(result, ProbeResult::Empty),
            "10.0 is exclusive upper; expected Empty, got {result:?}"
        );
    }

    #[test]
    fn point_probe_miss() {
        let mut tree = make_tree(&[(1.0, 5.0, "f1.parquet"), (6.0, 10.0, "f2.parquet")]);
        let result = tree.probe_contains_point(5.5);
        // 5.5 is in [5,6) — gap between the two intervals.
        assert!(
            matches!(result, ProbeResult::Empty),
            "expected Empty for point in gap, got {result:?}"
        );
    }

    #[test]
    fn point_probe_multiple_files() {
        let mut tree = make_tree(&[(1.0, 10.0, "f1.parquet"), (5.0, 15.0, "f2.parquet")]);
        let result = tree.probe_contains_point(7.0);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(
                    files.contains("f1.parquet"),
                    "f1 must be a candidate: {files:?}"
                );
                assert!(
                    files.contains("f2.parquet"),
                    "f2 must be a candidate: {files:?}"
                );
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── IntervalTree: overlaps ────────────────────────────────────────────────

    #[test]
    fn overlap_probe_hit() {
        let mut tree = make_tree(&[(1.0, 5.0, "f1.parquet"), (7.0, 12.0, "f2.parquet")]);
        let query = IndexInterval { lo: 3.0, hi: 8.0 };
        let result = tree.probe_overlaps(&query);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(
                    files.contains("f1.parquet"),
                    "f1 overlaps [3,8) at [3,5): {files:?}"
                );
                assert!(
                    files.contains("f2.parquet"),
                    "f2 overlaps [3,8) at [7,8): {files:?}"
                );
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn overlap_probe_adjacent_no_overlap() {
        // [1,5) and [5,10) are adjacent — not overlapping.
        let mut tree = make_tree(&[(1.0, 5.0, "f1.parquet")]);
        let query = IndexInterval { lo: 5.0, hi: 10.0 };
        let result = tree.probe_overlaps(&query);
        assert!(
            matches!(result, ProbeResult::Empty),
            "adjacent ranges must not overlap: {result:?}"
        );
    }

    #[test]
    fn overlap_probe_no_index() {
        let mut tree = IntervalTree::new();
        let query = IndexInterval { lo: 1.0, hi: 5.0 };
        let result = tree.probe_overlaps(&query);
        assert!(matches!(result, ProbeResult::NoIndex));
    }

    // ── IntervalTree: remove_file ─────────────────────────────────────────────

    #[test]
    fn remove_file_clears_entries() {
        let mut tree = make_tree(&[(1.0, 10.0, "f1.parquet"), (5.0, 15.0, "f2.parquet")]);
        tree.remove_file("f1.parquet");
        let result = tree.probe_contains_point(3.0);
        match result {
            ProbeResult::Empty => {} // f1 gone, f2 starts at 5 so point 3 has no match
            ProbeResult::FileCandidates(files) => {
                assert!(
                    !files.contains("f1.parquet"),
                    "f1 should be removed: {files:?}"
                );
            }
            ProbeResult::NoIndex => {} // also acceptable if tree is now empty
        }
    }

    // ── IntervalRegistry ─────────────────────────────────────────────────────

    #[test]
    fn registry_no_index_before_insert() {
        let reg = IntervalRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("t").unwrap();
        let result = reg.probe_contains_point(&proj, &tbl, "r", 5.0);
        assert!(matches!(result, ProbeResult::NoIndex));
    }

    #[test]
    fn registry_index_and_probe() {
        let reg = IntervalRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("t").unwrap();

        // Index a range [1,10) in f1.
        reg.index_row(
            &proj,
            &tbl,
            "r",
            r#"{"l":1,"u":10,"li":true,"ui":false}"#,
            "f1.parquet",
            0,
        );

        let result = reg.probe_contains_point(&proj, &tbl, "r", 5.0);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(
                    files.contains("f1.parquet"),
                    "expected f1.parquet: {files:?}"
                );
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn registry_column_isolation() {
        let reg = IntervalRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("t").unwrap();

        reg.index_row(
            &proj,
            &tbl,
            "r1",
            r#"{"l":1,"u":10,"li":true,"ui":false}"#,
            "f1.parquet",
            0,
        );

        // Probe on a different column — should return NoIndex.
        let result = reg.probe_contains_point(&proj, &tbl, "r2", 5.0);
        assert!(matches!(result, ProbeResult::NoIndex));
    }

    #[test]
    fn registry_total_entries() {
        let reg = IntervalRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("t").unwrap();

        reg.index_row(
            &proj,
            &tbl,
            "r",
            r#"{"l":1,"u":5,"li":true,"ui":false}"#,
            "f1.parquet",
            0,
        );
        reg.index_row(
            &proj,
            &tbl,
            "r",
            r#"{"l":6,"u":10,"li":true,"ui":false}"#,
            "f1.parquet",
            1,
        );
        assert_eq!(reg.total_entries(&proj, &tbl, "r"), 2);
    }

    // ── Smoke: 50 000 non-overlapping ranges ──────────────────────────────────

    #[test]
    fn fifty_thousand_ranges_smoke() {
        let mut tree = IntervalTree::new();
        for i in 0u64..50_000 {
            let lo = (i * 10) as f64;
            let hi = lo + 10.0;
            let file = format!("chunk_{}.parquet", i / 1000);
            tree.insert(IntervalEntry {
                lo,
                hi,
                file_path: file,
                row_group: (i % 1000) as u32,
            });
        }
        tree.rebuild();

        // Point probe: 250_000 is in row 25000's range [250000, 250010).
        let result = tree.probe_contains_point(250_000.0);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(!files.is_empty(), "expected at least one candidate file");
                let expected = "chunk_25.parquet";
                assert!(files.contains(expected), "expected {expected}: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }

        // Point probe: 250_000.5 (inside the same range).
        let result2 = tree.probe_contains_point(250_000.5);
        assert!(matches!(result2, ProbeResult::FileCandidates(_)));
    }
}
