//! Phase 5.x (C2 storage half) — row-group-granular GIN summaries for JSONB
//! `@>` containment pruning.
//!
//! # Why this exists
//!
//! Basin's JSONB GIN posting list (`basin-engine/src/index_probe.rs`) prunes at
//! *file* granularity: a `col @> '{…}'` probe AND-merges per-term posting lists
//! and returns the set of files that *might* contain a matching row. For a
//! uniform-distribution workload every file contains at least one match for the
//! searched term, so no file is ever pruned (effectiveness ratio ≈ 1.0). The
//! GIN index then provides ~zero benefit while still costing a probe — measured
//! at 88x slower than PostgreSQL, whose GIN prunes at a much finer granularity.
//!
//! This module restores fine-grained selectivity by storing, **per row-group**,
//! a compact membership summary (a small bloom filter) over the GIN terms
//! present in that row-group. The reader can then return just the row-groups
//! that could contain *all* searched terms — letting the engine read only those
//! row-groups instead of every row-group in every candidate file.
//!
//! # Structure (per row-group)
//!
//! Each `(file, row_group)` gets one [`fastbloom::BloomFilter`] sized for the
//! distinct GIN terms present in that row-group at a 1% false-positive rate. A
//! term is the same string atom the engine's `extract_terms` produces from a
//! JSONB value (a top-level key `"k"`, a key=value pair `"k"="v"`, or a hashed
//! `jsonb_path_ops` path). The bloom is a *conservative superset*: a row-group
//! whose bloom reports "absent" for a searched term provably contains no
//! matching row and is safe to skip; a "present" answer may be a false positive
//! and MUST be re-evaluated by the full predicate at the read layer.
//!
//! `@>` containment requires *all* keys of the right-hand operand to be present,
//! so a row-group survives the probe only when its bloom reports every searched
//! term as possibly-present (AND semantics — identical to the engine's
//! per-term posting-list intersection, just at row-group rather than file
//! granularity).
//!
//! # Backward compatibility
//!
//! This is purely additive and opt-in. Files written before row-group GIN
//! summaries existed simply have no entry in the registry; a probe for such a
//! file returns [`RowGroupProbe::Unknown`] and the caller falls back to its
//! existing file-granular path (or a full scan) — never a false negative.
//!
//! # Engine wiring (separate, serialized task)
//!
//! This module is the STORAGE-side capability only. The engine's `@>` probe
//! path (`basin-engine/src/index_probe.rs`) is wired to call
//! [`GinRowGroupRegistry::rowgroups_maybe_containing`] in a later, serialized
//! engine wave. Nothing here touches the engine.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};
use fastbloom::BloomFilter;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Target false-positive rate for each per-row-group bloom filter.
///
/// 1% matches the per-file `point_eq` bloom (`writer::DEFAULT_BLOOM_FPP`) so the
/// two layers behave consistently. A lower rate would prune more aggressively at
/// the cost of more bytes per row-group; 1% is a good default given a row-group
/// is typically only a few thousand rows.
const DEFAULT_RG_BLOOM_FPP: f64 = 0.01;

/// Deterministic seed shared by every row-group bloom. Matches the per-file
/// bloom seed in `writer::compute_bloom_filters` so the same string hashes the
/// same way regardless of which layer built the filter — and so a deserialised
/// bloom reconstructs the identical hash function.
const RG_BLOOM_SEED: u128 = 0u128;

/// Identifier for a single row-group within a file: the Parquet row-group index.
pub type RowGroupId = u32;

// ── Per-row-group summary ───────────────────────────────────────────────────────

/// A compact membership summary over the GIN terms present in one row-group.
///
/// Wraps a [`BloomFilter`] plus the distinct-term count it was built for so the
/// summary can be serialised and (optionally) merged. The bloom is built lazily
/// once enough terms have accumulated; until [`Self::seal`] is called it stages
/// terms in a `HashSet` so the final bloom can be sized exactly for the actual
/// distinct-term count (fastbloom requires the expected item count up front).
#[derive(Debug, Default)]
struct RowGroupSummary {
    /// Distinct terms staged before the bloom is sealed. Cleared on [`Self::seal`].
    staged: HashSet<String>,
    /// The sealed bloom filter. `None` until [`Self::seal`] is first called.
    bloom: Option<BloomFilter>,
}

impl RowGroupSummary {
    /// Stage a term. No-op once the bloom has been sealed (sealing is final for
    /// a given row-group, which is immutable once written).
    fn add_term(&mut self, term: &str) {
        if self.bloom.is_none() {
            self.staged.insert(term.to_string());
        }
    }

    /// Build the bloom filter from the staged terms. Idempotent: a second call
    /// is a no-op once a bloom exists.
    fn seal(&mut self) {
        if self.bloom.is_some() {
            return;
        }
        // Size for at least one item to avoid a zero-capacity bloom.
        let n = self.staged.len().max(1);
        let mut bloom = BloomFilter::with_false_pos(DEFAULT_RG_BLOOM_FPP)
            .seed(&RG_BLOOM_SEED)
            .expected_items(n);
        for term in &self.staged {
            bloom.insert(term.as_bytes());
        }
        self.bloom = Some(bloom);
        // Release the staging set — the bloom is now the source of truth.
        self.staged = HashSet::new();
    }

    /// Test whether `term` might be present in this row-group.
    ///
    /// Returns `true` for "possibly present" (could be a false positive) and
    /// `false` for "provably absent". If the summary has not been sealed yet it
    /// answers from the staged set (exact).
    fn maybe_contains(&self, term: &str) -> bool {
        match &self.bloom {
            Some(b) => b.contains(term.as_bytes()),
            None => self.staged.contains(term),
        }
    }
}

// ── Per-file summary table ───────────────────────────────────────────────────────

/// All row-group summaries for a single file, keyed by row-group index.
#[derive(Debug, Default)]
struct FileSummaries {
    /// `row_group → summary`.
    by_rg: HashMap<RowGroupId, RowGroupSummary>,
    /// Whether every row-group in this file has been sealed (i.e. the file is
    /// fully indexed and safe to use as a prune boundary).
    sealed: bool,
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Key into the global registry: one summary table per `(project, table, col)`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Result of a row-group containment probe for a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowGroupProbe {
    /// No summary exists for this `(project, table, col, file)` — the file was
    /// written before row-group GIN summaries existed, or the column is not
    /// summarised. The caller must fall back to its file-granular path or a
    /// full scan. Never a false negative.
    Unknown,
    /// The file is summarised. The vector lists exactly the row-groups that
    /// *might* contain all searched terms (sorted ascending). An EMPTY vector
    /// means no row-group can match — the whole file is provably prune-able.
    RowGroups(Vec<RowGroupId>),
}

/// Process-wide registry of row-group-granular GIN summaries.
///
/// Mirrors the lifecycle of the engine's JSONB GIN posting list and the
/// storage-side interval/GIST index: populate on write
/// ([`Self::index_row`]), seal per file ([`Self::mark_file_indexed`]), probe at
/// query time ([`Self::rowgroups_maybe_containing`]), and evict on compaction
/// ([`Self::remove_file`]).
///
/// # Thread safety
///
/// `Send + Sync`. The outer mutex guards the `HashMap` of `Arc<Mutex<…>>`
/// pointers; each inner mutex guards one column's summary table. The outer lock
/// is never held while an inner lock is taken, preventing deadlock.
pub struct GinRowGroupRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<HashMap<String, FileSummaries>>>>>,
}

impl GinRowGroupRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<HashMap<String, FileSummaries>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("GinRowGroupRegistry outer lock poisoned");
        map.entry(key).or_insert_with(|| Arc::new(Mutex::new(HashMap::new()))).clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<HashMap<String, FileSummaries>>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let map = self.inner.lock().expect("GinRowGroupRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Record the GIN terms for one row's value into its row-group summary.
    ///
    /// `terms` are the GIN term atoms the engine extracts from a JSONB value
    /// (top-level keys, key=value pairs, or hashed `jsonb_path_ops` paths). This
    /// is called from the write path for every row of a freshly written file.
    /// Empty `terms` is a no-op.
    ///
    /// Must be called for all rows of a `(file, row_group)` BEFORE
    /// [`Self::mark_file_indexed`] seals that file.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        terms: &[String],
        file_path: &str,
        row_group: RowGroupId,
    ) {
        if terms.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut files = arc.lock().expect("GinRowGroup file table lock poisoned");
        let fs = files.entry(file_path.to_string()).or_default();
        // Indexing a new row into an already-sealed file is a logic error
        // (files are immutable). Defensively re-open it so we don't silently
        // drop terms — re-sealing happens at mark_file_indexed.
        if fs.sealed {
            fs.sealed = false;
        }
        let summary = fs.by_rg.entry(row_group).or_default();
        for term in terms {
            summary.add_term(term);
        }
    }

    /// Seal every row-group summary for `file_path` and mark the file fully
    /// indexed. Called once all rows of a newly written file have been passed to
    /// [`Self::index_row`]. After this the file is safe to use as a prune
    /// boundary; probes against it return [`RowGroupProbe::RowGroups`].
    pub fn mark_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut files = arc.lock().expect("GinRowGroup file table lock poisoned");
            if let Some(fs) = files.get_mut(file_path) {
                for summary in fs.by_rg.values_mut() {
                    summary.seal();
                }
                fs.sealed = true;
            }
        }
    }

    /// Return the row-groups of `file_path` that might contain *all* of
    /// `search_keys` (the `@>` AND semantics), so the engine can read only those
    /// row-groups.
    ///
    /// * [`RowGroupProbe::Unknown`] — the file is not summarised (or not yet
    ///   sealed). Caller falls back to file-granular pruning / full scan.
    /// * [`RowGroupProbe::RowGroups(rgs)`] — `rgs` is the ascending list of
    ///   surviving row-groups. An empty `rgs` means the whole file is prunable.
    ///
    /// `search_keys` are the same GIN term atoms produced by `extract_terms`.
    /// Empty `search_keys` yields `Unknown` (nothing to prune on).
    pub fn rowgroups_maybe_containing(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        search_keys: &[String],
    ) -> RowGroupProbe {
        if search_keys.is_empty() {
            return RowGroupProbe::Unknown;
        }
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return RowGroupProbe::Unknown,
        };
        let files = arc.lock().expect("GinRowGroup file table lock poisoned");
        let fs = match files.get(file_path) {
            Some(fs) if fs.sealed => fs,
            // Not summarised, or sealing has not completed: conservative
            // fall-through. Never a false negative.
            _ => return RowGroupProbe::Unknown,
        };

        let mut surviving: Vec<RowGroupId> = fs
            .by_rg
            .iter()
            .filter_map(|(rg, summary)| {
                // `@>` containment: every searched term must be possibly-present.
                let all_present = search_keys.iter().all(|k| summary.maybe_contains(k));
                if all_present {
                    Some(*rg)
                } else {
                    None
                }
            })
            .collect();
        surviving.sort_unstable();
        RowGroupProbe::RowGroups(surviving)
    }

    /// Convenience: probe across a set of candidate files at once, returning a
    /// map of `file_path → surviving row-groups` for every file that is
    /// summarised. Files that are [`RowGroupProbe::Unknown`] are omitted from
    /// the map so the caller can detect them (missing key ⇒ fall back for that
    /// file). A file present with an empty vector is provably fully prunable.
    pub fn rowgroups_maybe_containing_multi(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        files: &[String],
        search_keys: &[String],
    ) -> HashMap<String, Vec<RowGroupId>> {
        let mut out = HashMap::new();
        for f in files {
            if let RowGroupProbe::RowGroups(rgs) =
                self.rowgroups_maybe_containing(project, table, col, f, search_keys)
            {
                out.insert(f.clone(), rgs);
            }
        }
        out
    }

    /// Drop all summaries for `file_path` (called on compaction / deletion) so
    /// probes never return stale row-groups for a file that no longer exists.
    pub fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut files = arc.lock().expect("GinRowGroup file table lock poisoned");
            files.remove(file_path);
        }
    }

    /// Whether `file_path` has a sealed row-group summary for this column.
    /// Primarily for diagnostics and the engine's completeness guard.
    pub fn is_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) -> bool {
        match self.get(project, table, col) {
            None => false,
            Some(arc) => arc
                .lock()
                .expect("GinRowGroup file table lock poisoned")
                .get(file_path)
                .map(|fs| fs.sealed)
                .unwrap_or(false),
        }
    }
}

impl Default for GinRowGroupRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn proj_tbl() -> (ProjectId, TableName) {
        (ProjectId::new(), TableName::new("docs").unwrap())
    }

    fn terms(ts: &[&str]) -> Vec<String> {
        ts.iter().map(|s| s.to_string()).collect()
    }

    // ── Sub-file pruning: the headline property ─────────────────────────────

    /// Build an index over rows spread across THREE row-groups of one file
    /// where the searched key lives in only ONE row-group. The probe must
    /// return just that row-group — proving finer-than-file pruning works.
    #[test]
    fn probe_returns_only_the_row_group_holding_the_key() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        // Row-group 0: keys "a", "b".
        reg.index_row(&proj, &tbl, "doc", &terms(&["a", "b"]), file, 0);
        // Row-group 1: keys "b", "c".
        reg.index_row(&proj, &tbl, "doc", &terms(&["b", "c"]), file, 1);
        // Row-group 2: keys "c", "needle"  ← the only row-group with "needle".
        reg.index_row(&proj, &tbl, "doc", &terms(&["c", "needle"]), file, 2);
        reg.mark_file_indexed(&proj, &tbl, "doc", file);

        // Searching for "needle" must return ONLY row-group 2.
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["needle"]));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert_eq!(rgs, vec![2], "expected only rg 2 to survive, got {rgs:?}");
            }
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }

    /// `@>` AND semantics: a multi-key search keeps only row-groups whose bloom
    /// reports every key as possibly-present.
    #[test]
    fn probe_and_semantics_across_row_groups() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        // rg0 has both "x" and "y"; rg1 has only "x"; rg2 has only "y".
        reg.index_row(&proj, &tbl, "doc", &terms(&["x", "y"]), file, 0);
        reg.index_row(&proj, &tbl, "doc", &terms(&["x"]), file, 1);
        reg.index_row(&proj, &tbl, "doc", &terms(&["y"]), file, 2);
        reg.mark_file_indexed(&proj, &tbl, "doc", file);

        // `@> {x, y}` requires BOTH → only rg0 survives.
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["x", "y"]));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert_eq!(rgs, vec![0], "expected only rg0 (has both), got {rgs:?}");
            }
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }

    /// A key present in no row-group prunes the whole file (empty survivor set).
    #[test]
    fn probe_absent_key_prunes_entire_file() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        reg.index_row(&proj, &tbl, "doc", &terms(&["a"]), file, 0);
        reg.index_row(&proj, &tbl, "doc", &terms(&["b"]), file, 1);
        reg.mark_file_indexed(&proj, &tbl, "doc", file);

        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["zzz"]));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert!(rgs.is_empty(), "absent key must prune whole file, got {rgs:?}");
            }
            other => panic!("expected RowGroups (empty), got {other:?}"),
        }
    }

    /// A present key reports its row-group (no false negative). Run many
    /// distinct keys to keep the bloom honest about false negatives.
    #[test]
    fn probe_never_false_negative() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        for rg in 0u32..8 {
            let key = format!("key_{rg}");
            reg.index_row(&proj, &tbl, "doc", &terms(&[key.as_str()]), file, rg);
        }
        reg.mark_file_indexed(&proj, &tbl, "doc", file);

        for rg in 0u32..8 {
            let key = format!("key_{rg}");
            let probe =
                reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&[key.as_str()]));
            match probe {
                RowGroupProbe::RowGroups(rgs) => {
                    assert!(
                        rgs.contains(&rg),
                        "key_{rg} must keep rg {rg} (no false negative), got {rgs:?}"
                    );
                }
                other => panic!("expected RowGroups, got {other:?}"),
            }
        }
    }

    // ── Backward compatibility / fall-through ────────────────────────────────

    #[test]
    fn unknown_file_returns_unknown() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        // Never indexed → Unknown (caller falls back; never a false negative).
        let probe =
            reg.rowgroups_maybe_containing(&proj, &tbl, "doc", "ghost.parquet", &terms(&["a"]));
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    #[test]
    fn unsealed_file_returns_unknown() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        // Indexed but NOT sealed yet → Unknown (mid-write, not safe to prune on).
        reg.index_row(&proj, &tbl, "doc", &terms(&["a"]), file, 0);
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["a"]));
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    #[test]
    fn empty_search_keys_returns_unknown() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        reg.index_row(&proj, &tbl, "doc", &terms(&["a"]), file, 0);
        reg.mark_file_indexed(&proj, &tbl, "doc", file);
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &[]);
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    // ── remove_file ──────────────────────────────────────────────────────────

    #[test]
    fn remove_file_drops_summary() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        reg.index_row(&proj, &tbl, "doc", &terms(&["a"]), file, 0);
        reg.mark_file_indexed(&proj, &tbl, "doc", file);
        assert!(reg.is_file_indexed(&proj, &tbl, "doc", file));

        reg.remove_file(&proj, &tbl, "doc", file);
        assert!(!reg.is_file_indexed(&proj, &tbl, "doc", file));
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["a"]));
        assert_eq!(probe, RowGroupProbe::Unknown, "removed file must read as Unknown");
    }

    // ── multi-file convenience ───────────────────────────────────────────────

    #[test]
    fn multi_file_probe_omits_unknown_files() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();

        // f1: rg0 has "needle", rg1 doesn't.
        reg.index_row(&proj, &tbl, "doc", &terms(&["needle"]), "f1.parquet", 0);
        reg.index_row(&proj, &tbl, "doc", &terms(&["other"]), "f1.parquet", 1);
        reg.mark_file_indexed(&proj, &tbl, "doc", "f1.parquet");
        // f2: indexed, no "needle" anywhere → present with empty survivors.
        reg.index_row(&proj, &tbl, "doc", &terms(&["other"]), "f2.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "doc", "f2.parquet");
        // f3: never indexed → omitted from the map.

        let files = vec!["f1.parquet".to_string(), "f2.parquet".to_string(), "f3.parquet".to_string()];
        let map = reg.rowgroups_maybe_containing_multi(&proj, &tbl, "doc", &files, &terms(&["needle"]));

        assert_eq!(map.get("f1.parquet"), Some(&vec![0]), "f1 keeps only rg0");
        assert_eq!(map.get("f2.parquet"), Some(&Vec::<RowGroupId>::new()), "f2 fully prunable");
        assert!(!map.contains_key("f3.parquet"), "f3 unknown → omitted");
    }

    // ── column / project isolation ───────────────────────────────────────────

    #[test]
    fn columns_are_isolated() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        reg.index_row(&proj, &tbl, "title", &terms(&["a"]), "f1.parquet", 0);
        reg.mark_file_indexed(&proj, &tbl, "title", "f1.parquet");
        // Probe a different column → Unknown.
        let probe = reg.rowgroups_maybe_containing(&proj, &tbl, "body", "f1.parquet", &terms(&["a"]));
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    // ── scale smoke: sub-file pruning holds at 64 row-groups ─────────────────

    #[test]
    fn sub_file_pruning_at_scale() {
        let reg = GinRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "big.parquet";

        // 64 row-groups; a shared "common" term in all, plus a unique term per rg.
        for rg in 0u32..64 {
            let uniq = format!("uniq_{rg}");
            reg.index_row(&proj, &tbl, "doc", &terms(&["common", uniq.as_str()]), file, rg);
        }
        reg.mark_file_indexed(&proj, &tbl, "doc", file);

        // A unique term must select exactly one row-group (modulo bloom FP).
        let probe =
            reg.rowgroups_maybe_containing(&proj, &tbl, "doc", file, &terms(&["uniq_42"]));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert!(rgs.contains(&42), "uniq_42 must keep rg 42, got {rgs:?}");
                // With 1% FPP over 64 row-groups the expected survivors are ~1.6;
                // require strictly fewer than the whole file (proves pruning).
                assert!(
                    rgs.len() < 64,
                    "sub-file pruning must skip some row-groups, kept {} of 64",
                    rgs.len()
                );
            }
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }
}
