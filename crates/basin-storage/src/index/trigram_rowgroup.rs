//! Phase 5.x (B3 storage half) — row-group-granular trigram summaries for
//! `LIKE` / `ILIKE` substring pruning.
//!
//! # Why this exists
//!
//! Basin's in-memory trigram candidate prune
//! (`basin-engine/src/index_probe.rs::trigram_candidates`) builds an inverted
//! index on the fly per query — fine for correctness, but it must read every
//! row of every file in scope to harvest the trigrams. The headline I/O win
//! comes from skipping file BODIES whose row-groups *cannot* contain the
//! pattern's trigrams. This module is the persisted summary the engine probes
//! BEFORE opening a row-group, mirroring how
//! [`crate::index::gin_rowgroup::GinRowGroupRegistry`] prunes JSONB `@>`
//! probes at row-group granularity.
//!
//! # Structure (per row-group)
//!
//! Each `(file, row_group)` gets one [`fastbloom::BloomFilter`] sized for the
//! distinct 3-byte trigrams present in that row-group's text values at a 1%
//! false-positive rate. A trigram is any 3 consecutive bytes of the column
//! value. We insert BOTH the case-preserved bytes AND the ASCII-lowercased
//! bytes — matching the engine's `substring_trigrams` (which does the same)
//! so a single persisted summary serves both `LIKE` (query trigrams keep
//! their case) and `ILIKE` (query trigrams lower-folded) probes without a
//! second index. The bloom is a *conservative superset*: a row-group whose
//! bloom reports "absent" for any pattern trigram provably contains no
//! matching row; a "present" answer may be a false positive and MUST be
//! re-checked by the full `LIKE` predicate at the read layer.
//!
//! A wildcard `LIKE` pattern decomposes into a set of REQUIRED trigrams (see
//! `basin_trgm::gin_like::trigrams_for_pattern` /
//! `basin_engine::index_probe::trigrams_for_pattern`). For the row to match
//! the pattern at all, EVERY required trigram must be present in the row's
//! text — so a row-group survives the probe only when its bloom reports every
//! input trigram as possibly-present (AND semantics, identical to the
//! gin_rowgroup `@>` path).
//!
//! # Backward compatibility
//!
//! Purely additive. Files written before trigram row-group summaries existed
//! simply have no entry in the registry; a probe for such a file returns
//! [`RowGroupProbe::Unknown`] and the caller falls back to its existing
//! file-granular (or full-scan) path. Never a false negative.
//!
//! # Engine wiring (separate, serialised task)
//!
//! This module is the STORAGE-side capability only. The engine's `LIKE` /
//! `ILIKE` probe path is wired to call
//! [`TrigramRowGroupRegistry::rowgroups_maybe_containing_trigrams`] in a
//! later, serialised engine wave. Nothing here touches the engine.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};
use fastbloom::BloomFilter;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Target false-positive rate for each per-row-group trigram bloom.
///
/// 1% matches `gin_rowgroup` (and the per-file `point_eq` bloom) so the
/// layers behave consistently. A lower rate would prune more aggressively at
/// the cost of more bytes per row-group; 1% is a good default given a
/// row-group is typically a few thousand rows of bounded text.
const DEFAULT_RG_BLOOM_FPP: f64 = 0.01;

/// Deterministic seed shared by every row-group bloom. Matches the seed used
/// by the sibling `gin_rowgroup` and the per-file bloom in
/// `writer::compute_bloom_filters`, so the same trigram bytes hash the same
/// way regardless of which layer built the filter — and so a deserialised
/// bloom reconstructs the identical hash function.
const RG_BLOOM_SEED: u128 = 0u128;

/// Identifier for a single row-group within a file: the Parquet row-group index.
pub type RowGroupId = u32;

/// A 3-byte trigram — the indexing atom. Identical to the engine's
/// `trigrams_for_pattern` output element so callers can hand the result of
/// that function straight into [`TrigramRowGroupRegistry::rowgroups_maybe_containing_trigrams`].
pub type Trigram = [u8; 3];

// ── Per-row-group summary ────────────────────────────────────────────────────

/// A compact membership summary over the trigrams present in one row-group.
///
/// Wraps a [`BloomFilter`] plus a staging set so the bloom can be sized
/// exactly for the actual distinct-trigram count once the row-group is
/// sealed. fastbloom requires the expected item count up front, so we stage
/// in a `HashSet` until [`Self::seal`] is called.
#[derive(Debug, Default)]
struct RowGroupSummary {
    /// Distinct trigrams staged before the bloom is sealed. Cleared on
    /// [`Self::seal`].
    staged: HashSet<Trigram>,
    /// The sealed bloom filter. `None` until [`Self::seal`] is first called.
    bloom: Option<BloomFilter>,
}

impl RowGroupSummary {
    /// Stage every 3-byte window of `text` AND of its ASCII-lowercased form.
    /// No-op once the bloom has been sealed (sealing is final for a given
    /// row-group, which is immutable once written).
    fn add_text(&mut self, text: &str) {
        if self.bloom.is_some() {
            return;
        }
        let raw = text.as_bytes();
        if raw.len() < 3 {
            return;
        }
        // Case-preserved windows — serves case-sensitive LIKE.
        for w in raw.windows(3) {
            self.staged.insert([w[0], w[1], w[2]]);
        }
        // ASCII-lowercased windows — serves ILIKE. Only add if any byte
        // actually folds, otherwise it is identical to the raw set and we
        // skip the allocation. (Cheap branch: ASCII letters are the only
        // bytes for which `to_ascii_lowercase` differs.)
        if raw.iter().any(|b| b.is_ascii_uppercase()) {
            let lowered: Vec<u8> = raw.iter().map(|b| b.to_ascii_lowercase()).collect();
            for w in lowered.windows(3) {
                self.staged.insert([w[0], w[1], w[2]]);
            }
        }
    }

    /// Build the bloom filter from the staged trigrams. Idempotent: a second
    /// call is a no-op once a bloom exists.
    fn seal(&mut self) {
        if self.bloom.is_some() {
            return;
        }
        // Size for at least one item to avoid a zero-capacity bloom.
        let n = self.staged.len().max(1);
        let mut bloom = BloomFilter::with_false_pos(DEFAULT_RG_BLOOM_FPP)
            .seed(&RG_BLOOM_SEED)
            .expected_items(n);
        for tg in &self.staged {
            bloom.insert(tg);
        }
        self.bloom = Some(bloom);
        // Release the staging set — the bloom is now the source of truth.
        self.staged = HashSet::new();
    }

    /// Test whether `trigram` might be present in this row-group.
    ///
    /// Returns `true` for "possibly present" (could be a false positive) and
    /// `false` for "provably absent". If the summary has not been sealed yet
    /// it answers from the staged set (exact).
    fn maybe_contains(&self, trigram: &Trigram) -> bool {
        match &self.bloom {
            Some(b) => b.contains(trigram),
            None => self.staged.contains(trigram),
        }
    }
}

// ── Per-file summary table ───────────────────────────────────────────────────

/// All row-group summaries for a single file, keyed by row-group index.
#[derive(Debug, Default)]
struct FileSummaries {
    /// `row_group → summary`.
    by_rg: HashMap<RowGroupId, RowGroupSummary>,
    /// Whether every row-group in this file has been sealed (i.e. the file is
    /// fully indexed and safe to use as a prune boundary).
    sealed: bool,
}

// ── Registry ─────────────────────────────────────────────────────────────────

/// Key into the global registry: one summary table per `(project, table, col)`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Result of a row-group trigram probe for a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowGroupProbe {
    /// No summary exists for this `(project, table, col, file)` — the file
    /// was written before trigram row-group summaries existed, or the column
    /// is not summarised, or sealing has not completed yet. The caller must
    /// fall back to its file-granular path or a full scan. Never a false
    /// negative.
    Unknown,
    /// The file is summarised. The vector lists exactly the row-groups that
    /// might contain *all* requested trigrams (sorted ascending). An EMPTY
    /// vector means no row-group can match — the whole file is provably
    /// prune-able.
    RowGroups(Vec<RowGroupId>),
}

/// Process-wide registry of row-group-granular trigram summaries.
///
/// Mirrors the lifecycle of the sibling `GinRowGroupRegistry`: populate on
/// write ([`Self::index_row`]), seal per file ([`Self::seal_file_indexed`]),
/// probe at query time
/// ([`Self::rowgroups_maybe_containing_trigrams`]), and evict on compaction
/// ([`Self::remove_file`]).
///
/// # Thread safety
///
/// `Send + Sync`. The outer mutex guards the `HashMap` of `Arc<Mutex<…>>`
/// pointers; each inner mutex guards one column's summary table. The outer
/// lock is never held while an inner lock is taken, preventing deadlock.
pub struct TrigramRowGroupRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<HashMap<String, FileSummaries>>>>>,
}

impl TrigramRowGroupRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<HashMap<String, FileSummaries>>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let mut map = self
            .inner
            .lock()
            .expect("TrigramRowGroupRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<HashMap<String, FileSummaries>>>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let map = self
            .inner
            .lock()
            .expect("TrigramRowGroupRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Record the trigrams of one row's text value into its row-group summary.
    ///
    /// `text` is the row's column value; every 3-byte window of `text` and of
    /// its ASCII-lowercased form is inserted. This is called from the write
    /// path for every row of a freshly written file. Empty or sub-3-byte
    /// `text` is a no-op (no usable trigrams).
    ///
    /// Must be called for all rows of a `(file, row_group)` BEFORE
    /// [`Self::seal_file_indexed`] seals that file.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        row_group: RowGroupId,
        text: &str,
    ) {
        if text.len() < 3 {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut files = arc
            .lock()
            .expect("TrigramRowGroup file table lock poisoned");
        let fs = files.entry(file_path.to_string()).or_default();
        // Indexing a new row into an already-sealed file is a logic error
        // (files are immutable). Defensively re-open it so we don't silently
        // drop trigrams — re-sealing happens at seal_file_indexed.
        if fs.sealed {
            fs.sealed = false;
        }
        let summary = fs.by_rg.entry(row_group).or_default();
        summary.add_text(text);
    }

    /// Seal every row-group summary for `file_path` and mark the file fully
    /// indexed. Called once all rows of a newly written file have been
    /// passed to [`Self::index_row`]. After this the file is safe to use as
    /// a prune boundary; probes against it return
    /// [`RowGroupProbe::RowGroups`].
    pub fn seal_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut files = arc
                .lock()
                .expect("TrigramRowGroup file table lock poisoned");
            if let Some(fs) = files.get_mut(file_path) {
                for summary in fs.by_rg.values_mut() {
                    summary.seal();
                }
                fs.sealed = true;
            }
        }
    }

    /// Return the row-groups of `file_path` that might contain *all*
    /// `trigrams` (LIKE AND semantics: a row matches the pattern only if
    /// every required trigram is present), so the engine can read only those
    /// row-groups.
    ///
    /// * [`RowGroupProbe::Unknown`] — the file is not summarised (or not yet
    ///   sealed). Caller falls back to file-granular pruning / full scan.
    /// * [`RowGroupProbe::RowGroups(rgs)`](RowGroupProbe::RowGroups) — `rgs` is the ascending list of
    ///   surviving row-groups. An empty `rgs` means the whole file is
    ///   prunable.
    ///
    /// `trigrams` are the required pattern trigrams produced by
    /// `basin_engine::index_probe::trigrams_for_pattern` (or the equivalent
    /// `basin_trgm::gin_like::trigrams_for_pattern`). Empty `trigrams` yields
    /// `Unknown` (nothing to prune on — the pattern was too short or all
    /// wildcards).
    ///
    /// CORRECTNESS: the result is a SUPERSET filter. Callers MUST re-evaluate
    /// the full `LIKE` / `ILIKE` predicate on the surviving rows — this API
    /// only guarantees no false negatives, not no false positives.
    pub fn rowgroups_maybe_containing_trigrams(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        trigrams: &[Trigram],
    ) -> RowGroupProbe {
        if trigrams.is_empty() {
            return RowGroupProbe::Unknown;
        }
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return RowGroupProbe::Unknown,
        };
        let files = arc
            .lock()
            .expect("TrigramRowGroup file table lock poisoned");
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
                // AND semantics: every required trigram must be possibly-present.
                let all_present = trigrams.iter().all(|t| summary.maybe_contains(t));
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

    /// Convenience: probe across a set of candidate files at once, returning
    /// a map of `file_path → surviving row-groups` for every file that is
    /// summarised. Files that are [`RowGroupProbe::Unknown`] are omitted
    /// from the map so the caller can detect them (missing key ⇒ fall back
    /// for that file). A file present with an empty vector is provably fully
    /// prunable.
    pub fn rowgroups_maybe_containing_trigrams_multi(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        files: &[String],
        trigrams: &[Trigram],
    ) -> HashMap<String, Vec<RowGroupId>> {
        let mut out = HashMap::new();
        for f in files {
            if let RowGroupProbe::RowGroups(rgs) =
                self.rowgroups_maybe_containing_trigrams(project, table, col, f, trigrams)
            {
                out.insert(f.clone(), rgs);
            }
        }
        out
    }

    /// Drop all summaries for `file_path` (called on compaction / deletion)
    /// so probes never return stale row-groups for a file that no longer
    /// exists.
    pub fn remove_file(&self, project: &ProjectId, table: &TableName, col: &str, file_path: &str) {
        if let Some(arc) = self.get(project, table, col) {
            let mut files = arc
                .lock()
                .expect("TrigramRowGroup file table lock poisoned");
            files.remove(file_path);
        }
    }

    /// Whether `file_path` has a sealed trigram row-group summary for this
    /// column. Primarily for diagnostics and the engine's completeness guard.
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
                .expect("TrigramRowGroup file table lock poisoned")
                .get(file_path)
                .map(|fs| fs.sealed)
                .unwrap_or(false),
        }
    }
}

impl Default for TrigramRowGroupRegistry {
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

    /// Trigram windows of `s` (case-preserved). Tiny helper so tests stay
    /// readable — production callers get this from
    /// `basin_engine::index_probe::trigrams_for_pattern`.
    fn tg(s: &str) -> Vec<Trigram> {
        let b = s.as_bytes();
        b.windows(3).map(|w| [w[0], w[1], w[2]]).collect()
    }

    // ── Sub-file pruning at small scale ───────────────────────────────────

    #[test]
    fn probe_returns_only_the_row_group_holding_the_substring() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        // 4 row-groups. Only rg=2 contains the substring "kilo".
        reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple acorn");
        reg.index_row(&proj, &tbl, "doc", file, 1, "bravo banana berry");
        reg.index_row(&proj, &tbl, "doc", file, 2, "charlie kilowatt drink");
        reg.index_row(&proj, &tbl, "doc", file, 3, "delta donut dynamo");
        reg.seal_file_indexed(&proj, &tbl, "doc", file);

        // Pattern trigrams for "kilo".
        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("kilo"));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                // rg=2 must survive; the others have no "kil" / "ilo" windows.
                assert!(rgs.contains(&2), "expected rg 2 to survive, got {rgs:?}");
                // With short fixed text per rg the bloom is unlikely to fp,
                // but the strict assertion is: never false-negative AND fewer
                // than total (pruning happens).
                assert!(
                    rgs.len() < 4,
                    "expected sub-file pruning, kept {} of 4",
                    rgs.len()
                );
            }
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }

    /// All-rowgroups-match case: every row-group contains the substring.
    #[test]
    fn probe_returns_all_when_every_row_group_matches() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        for rg in 0u32..4 {
            // Each row-group contains "zulu" plus a unique suffix.
            let row = format!("zulu prefix uniform_{rg}");
            reg.index_row(&proj, &tbl, "doc", file, rg, &row);
        }
        reg.seal_file_indexed(&proj, &tbl, "doc", file);

        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("zulu"));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert_eq!(
                    rgs,
                    vec![0, 1, 2, 3],
                    "every rg should survive, got {rgs:?}"
                );
            }
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }

    /// Absent pattern: no row-group contains the trigram → whole file pruned.
    #[test]
    fn probe_absent_pattern_prunes_entire_file() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");
        reg.index_row(&proj, &tbl, "doc", file, 1, "bravo banana");
        reg.seal_file_indexed(&proj, &tbl, "doc", file);

        // "qqqq" trigrams ("qqq") cannot be present.
        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("qqqq"));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert!(
                    rgs.is_empty(),
                    "absent trigram must prune whole file, got {rgs:?}"
                );
            }
            other => panic!("expected RowGroups (empty), got {other:?}"),
        }
    }

    // ── Backward compatibility / fall-through ────────────────────────────

    #[test]
    fn unknown_file_returns_unknown() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        // Never indexed → Unknown (caller falls back; never false negative).
        let probe = reg.rowgroups_maybe_containing_trigrams(
            &proj,
            &tbl,
            "doc",
            "ghost.parquet",
            &tg("alpha"),
        );
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    #[test]
    fn pre_seal_returns_unknown() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        // Indexed but NOT sealed yet → Unknown (mid-write safety, matches
        // the gin_rowgroup pattern).
        reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");
        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("alpha"));
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    #[test]
    fn empty_trigrams_returns_unknown() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");
        reg.seal_file_indexed(&proj, &tbl, "doc", file);
        // Pattern yielded no trigrams (e.g. "%a%" — too short).
        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &[]);
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    // ── Case folding (LIKE / ILIKE single index) ─────────────────────────

    #[test]
    fn both_case_storage_serves_case_sensitive_and_insensitive() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";

        // Stored as "ALPHA" (uppercase). Both raw and lower trigrams indexed.
        reg.index_row(&proj, &tbl, "doc", file, 0, "ALPHA");
        reg.seal_file_indexed(&proj, &tbl, "doc", file);

        // Case-sensitive LIKE 'ALPHA%' → trigrams ALP, LPH, PHA (uppercase).
        let upper = tg("ALPHA");
        let probe_upper = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &upper);
        match probe_upper {
            RowGroupProbe::RowGroups(rgs) => assert!(rgs.contains(&0)),
            other => panic!("expected RowGroups, got {other:?}"),
        }

        // ILIKE 'alpha%' → trigrams alp, lph, pha (lowercase). Same bloom
        // must hit because we stored lowercased windows too.
        let lower = tg("alpha");
        let probe_lower = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &lower);
        match probe_lower {
            RowGroupProbe::RowGroups(rgs) => assert!(rgs.contains(&0)),
            other => panic!("expected RowGroups, got {other:?}"),
        }
    }

    // ── remove_file ───────────────────────────────────────────────────────

    #[test]
    fn remove_file_drops_summary() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "f1.parquet";
        reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");
        reg.seal_file_indexed(&proj, &tbl, "doc", file);
        assert!(reg.is_file_indexed(&proj, &tbl, "doc", file));

        reg.remove_file(&proj, &tbl, "doc", file);
        assert!(!reg.is_file_indexed(&proj, &tbl, "doc", file));
        let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("alpha"));
        assert_eq!(
            probe,
            RowGroupProbe::Unknown,
            "removed file must read as Unknown"
        );
    }

    // ── multi-file convenience ────────────────────────────────────────────

    #[test]
    fn multi_file_probe_omits_unknown_files() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();

        // f1: rg0 contains "kilo", rg1 does not.
        reg.index_row(&proj, &tbl, "doc", "f1.parquet", 0, "kilowatt hour");
        reg.index_row(&proj, &tbl, "doc", "f1.parquet", 1, "tango uniform");
        reg.seal_file_indexed(&proj, &tbl, "doc", "f1.parquet");
        // f2: indexed, no "kilo" anywhere → present with empty survivors.
        reg.index_row(&proj, &tbl, "doc", "f2.parquet", 0, "victor whiskey");
        reg.seal_file_indexed(&proj, &tbl, "doc", "f2.parquet");
        // f3: never indexed → omitted from the map.

        let files = vec![
            "f1.parquet".to_string(),
            "f2.parquet".to_string(),
            "f3.parquet".to_string(),
        ];
        let map =
            reg.rowgroups_maybe_containing_trigrams_multi(&proj, &tbl, "doc", &files, &tg("kilo"));

        // f1 must keep rg0 (and only rg0 — rg1 has no "kil" / "ilo").
        let f1 = map.get("f1.parquet").expect("f1 must be present");
        assert!(f1.contains(&0), "f1 keeps rg0, got {f1:?}");
        assert!(!f1.contains(&1), "f1 prunes rg1, got {f1:?}");
        // f2 fully prunable.
        assert_eq!(
            map.get("f2.parquet"),
            Some(&Vec::<RowGroupId>::new()),
            "f2 fully prunable"
        );
        // f3 unknown → omitted.
        assert!(!map.contains_key("f3.parquet"), "f3 unknown → omitted");
    }

    // ── Column / project isolation ────────────────────────────────────────

    #[test]
    fn columns_are_isolated() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        reg.index_row(&proj, &tbl, "title", "f1.parquet", 0, "alpha");
        reg.seal_file_indexed(&proj, &tbl, "title", "f1.parquet");
        // Probe a different column → Unknown.
        let probe = reg.rowgroups_maybe_containing_trigrams(
            &proj,
            &tbl,
            "body",
            "f1.parquet",
            &tg("alpha"),
        );
        assert_eq!(probe, RowGroupProbe::Unknown);
    }

    // ── Scale: sub-file pruning at ≥32 row-groups ─────────────────────────

    #[test]
    fn sub_file_pruning_at_scale() {
        let reg = TrigramRowGroupRegistry::new();
        let (proj, tbl) = proj_tbl();
        let file = "big.parquet";

        // 64 row-groups. Each row-group's text is a shared prefix plus a
        // synthetic unique token "uniform_<N>" — the unique token is the
        // discriminating substring.
        for rg in 0u32..64 {
            let row = format!("common shared body uniform_{rg:03} trailing words");
            reg.index_row(&proj, &tbl, "doc", file, rg, &row);
        }
        reg.seal_file_indexed(&proj, &tbl, "doc", file);

        // Probe for "uniform_042" — only rg=42 has this exact substring.
        let probe =
            reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("uniform_042"));
        match probe {
            RowGroupProbe::RowGroups(rgs) => {
                assert!(
                    rgs.contains(&42),
                    "uniform_042 must keep rg 42 (no false negative), got {rgs:?}"
                );
                // With 1% FPP × ~9 trigram conjuncts the expected survivors
                // are far below 64. Require strictly fewer than the whole
                // file (proves sub-file pruning works at scale).
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
