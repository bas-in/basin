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
//! * **posting list** — the set of file paths (interned `Arc<str>`) that
//!   contain at least one row with each distinct term.  AND-merging two
//!   posting lists yields the files that contain BOTH terms — the correct
//!   semantics for compound containment (`{"a":1,"b":2}` must have both term
//!   "a" and term "b" indexed).  Granularity is the FILE: this registry only
//!   feeds file-level pruning; row-group pruning is owned by
//!   `GinRowGroupRegistry` / `JsonbPostingRegistry`.
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

/// Maximum total posting entries kept per `(table, col)` posting list.
/// Beyond this threshold the oldest 25% of terms are evicted.
///
/// One "entry" is a distinct **(term, file)** posting pair.  The posting
/// list deduplicates rows: a term that occurs in 10k rows of one file costs
/// ONE entry, so a 1M-row backfill only approaches this budget when the
/// indexed documents carry high-cardinality (near-unique) values.
///
/// Memory arithmetic behind the default (measured against the data
/// structures in [`TermPostingList`], hashbrown load factor ≈ 0.875,
/// average table slack ≈ 1.3×):
///
/// * per posting pair: one `Arc<str>` (16 B inline) in a per-term
///   `HashSet<Arc<str>>` bucket + ctrl byte + slack ≈ **~22 B**;
/// * per *distinct term* (amortised over its pairs): `HashMap` bucket
///   (`String` key 24 B + `HashSet` header 48 B) × slack ≈ 95 B, plus the
///   term text heap (~16–32 B for `kv:key="value"`) and its `insert_order`
///   copy (24 B + text) ≈ **~170 B**;
/// * per distinct file: the interned path `Arc<str>` (~120 B incl. header),
///   allocated once per file — negligible (file counts are O(100s)).
///
/// At 5M pairs the pair-side cost is ≈ 110 MB.  Typical workloads (term
/// values shared by many rows, so pairs ≫ distinct terms) land in the
/// 64–128 MB envelope.  The adversarial ceiling — every term unique, one
/// pair each — is ≈ 5M × (22 + 170) B ≈ 1 GB; such workloads fire the
/// eviction warning below and operators should lower the budget via the
/// env knob (the index stays *correct* either way: eviction de-indexes
/// only the affected files and pruning degrades per-file).
///
/// This default can be overridden at process start via the
/// `BASIN_GIN_POSTING_BUDGET` environment variable.  Example:
/// ```text
/// BASIN_GIN_POSTING_BUDGET=2000000 basin-server
/// ```
const DEFAULT_POSTING_BUDGET: usize = 5_000_000;

/// Per-project floor: every project is guaranteed at least this many posting
/// pairs before another project's pressure can force it to evict. Overridable
/// via `BASIN_GIN_POSTING_FLOOR`. The floor is the noisy-neighbour guard: a
/// project that stays under its floor never evicts because of a sibling
/// project's JSONB churn, no matter how many projects are active. Default
/// 500_000 pairs (~11 MB at the ~22 B/pair pair-side cost in the arithmetic
/// above) — enough to keep a small/idle project's index fully resident.
const DEFAULT_POSTING_FLOOR: usize = 500_000;

// ── Row-granular postings (two-tier) ──────────────────────────────────────────
//
// The (term, file) postings above are the always-present COARSE tier: they
// prune whole files. At 1M rows with a needle that appears in EVERY file, the
// coarse tier prunes nothing — every file is a candidate, and the read decodes
// every file in full even though only a handful of rows per file actually
// match. PG wins this shape because its posting lists are row-granular: it
// skips straight to the matching rows.
//
// The row tier closes that gap WITHIN each candidate file. For a term, in a
// given file, we optionally keep the SORTED ABSOLUTE ROW INDICES (offsets) at
// which that term occurs. At probe time the per-term row-offset lists are
// AND-intersected per file, yielding a sorted row-offset list per file that is
// a SUPERSET of the true matches (raw-bytes containment, like the coarse tier —
// the `jsonb_contains` UDF still re-checks every emitted row). The reader turns
// that offset list into a Parquet `RowSelection` over the kept row groups, so
// non-matching rows are never decoded.
//
// Offsets, not a bitmap: a sorted `Vec<u32>` of row offsets intersects in O(n)
// and converts directly to a `RowSelection`; it costs ~4 B per matching row and
// is empty for files where the term is absent. A dense bitmap would cost
// `file_rows / 8` bytes regardless of selectivity — exactly backwards, since
// the row tier pays off for SELECTIVE terms within a file. (`u32` row offset
// caps a single file at ~4.29 B rows, far above any Basin data file.)
//
// DENSITY CAP — the inversion. Row postings help when FEW rows in a file match;
// if MOST of a file's rows match, decoding the whole file is already fine and
// the offset list is just overhead (and large). So a (term, file) row posting
// is DROPPED when its matching-row ratio for that file exceeds
// `BASIN_GIN_ROW_TIER_MAX_RATIO_PCT` (default 60%). A dropped (term, file) row
// posting means that file falls back to the coarse tier for that term — exactly
// the behaviour we want for the bench's everywhere-dense needle term: the win
// there comes from the OTHER, more selective needle terms (and page pruning)
// narrowing the row set, while a single near-universal term contributes no row
// posting (its file decode was already unavoidable).
//
// MEMORY — row postings are counted against the SAME per-project budget as the
// coarse tier, with a coarser weight (`ROW_POSTING_WEIGHT_SHIFT`): a block of
// row offsets costs `1 + (n_offsets >> shift)` budget units, so a file with
// 64k matching rows for a term costs ~64 budget units, not 64k. Under budget
// pressure the ROW tier is evicted FIRST (the coarse tier survives longer): the
// coarse postings are the correctness-preserving must-keep, the row tier is a
// pure accelerator.

/// Per-(term,file) density cap for the row tier, as a percentage of the file's
/// indexed (non-null) rows. Above this ratio the row posting is dropped and the
/// file falls back to coarse-tier decode for that term. Overridable via
/// `BASIN_GIN_ROW_TIER_MAX_RATIO_PCT`. Default 60.
const DEFAULT_ROW_TIER_MAX_RATIO_PCT: u64 = 60;

/// Budget weight shift for a row-posting block: a block of `n` row offsets
/// costs `1 + (n >> ROW_POSTING_WEIGHT_SHIFT)` units against the per-project
/// budget. Shift 10 ≈ one budget unit per 1024 offsets. This keeps a dense
/// table's row tier from dwarfing the coarse-tier accounting while still
/// growing with real memory.
const ROW_POSTING_WEIGHT_SHIFT: u32 = 10;

/// Read `BASIN_GIN_ROW_TIER_MAX_RATIO_PCT` once. Clamped to 1..=100. A value of
/// `0` is treated as 1 (never disables the tier entirely via a zero cap — set
/// `BASIN_GIN_ROW_TIER=0` to disable; see [`row_tier_enabled`]).
fn row_tier_max_ratio_pct() -> u64 {
    use std::sync::OnceLock;
    static PCT: OnceLock<u64> = OnceLock::new();
    *PCT.get_or_init(|| {
        std::env::var("BASIN_GIN_ROW_TIER_MAX_RATIO_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.clamp(1, 100))
            .unwrap_or(DEFAULT_ROW_TIER_MAX_RATIO_PCT)
    })
}

/// Whether the row tier is enabled at all (`BASIN_GIN_ROW_TIER`, default on).
/// Set `BASIN_GIN_ROW_TIER=0` to fall back to pure coarse-tier behaviour
/// (byte-identical to the pre-row-tier path).
fn row_tier_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("BASIN_GIN_ROW_TIER").ok().as_deref(), Some("0") | Some("false"))
    })
}

/// Budget cost (in per-project budget units) of a row-posting block holding
/// `n_offsets` row offsets.
#[inline]
fn row_block_budget_cost(n_offsets: usize) -> usize {
    1 + (n_offsets >> ROW_POSTING_WEIGHT_SHIFT)
}

/// Return the effective GLOBAL posting-entry budget (the process-wide ceiling
/// shared across every project, table and column).
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

/// Return the per-project posting floor (see [`DEFAULT_POSTING_FLOOR`]).
/// Reads `BASIN_GIN_POSTING_FLOOR` once and caches the result. Clamped to the
/// global budget — a floor larger than the whole budget is meaningless.
fn posting_floor() -> usize {
    use std::sync::OnceLock;
    static FLOOR: OnceLock<usize> = OnceLock::new();
    *FLOOR.get_or_init(|| {
        let raw = std::env::var("BASIN_GIN_POSTING_FLOOR")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_POSTING_FLOOR);
        raw.min(posting_budget())
    })
}

/// Effective per-project posting budget given how many projects currently hold
/// at least one posting list.
///
/// Fair share with a floor: each active project may keep up to
/// `max(floor, global_budget / active_projects)` posting pairs across ALL its
/// `(table, col)` lists. The `max(floor, …)` is what makes this a *partition*
/// rather than a global free-for-all: project A's churn drives the fair-share
/// term down as projects come and go, but can never push any project below its
/// floor, so A cannot drain B's resident postings. The sum of partitions can
/// exceed the global budget by up to `active_projects * floor` — that slack is
/// the deliberate price of the floor guarantee and is bounded because the floor
/// is clamped to the budget and `active_projects` is the count of projects that
/// actually hold lists.
fn per_project_budget(active_projects: usize) -> usize {
    let active = active_projects.max(1);
    let fair_share = posting_budget() / active;
    fair_share.max(posting_floor())
}

// ── Data types ────────────────────────────────────────────────────────────────

/// One posting list for a single column: `term → set of file paths`.
///
/// File paths are interned (`Arc<str>`, one allocation per distinct file)
/// because every term in a 1M-row backfill would otherwise clone a ~100-byte
/// path string per posting entry.  Posting granularity is the FILE: the only
/// consumer of this registry is file-level pruning (`ProbeResult::
/// FileCandidates`), so storing per-row locations was pure overhead — and it
/// made the posting budget burn ~rows×terms entries instead of the
/// (term, file) pairs that actually bound memory.  Row-group-granular pruning
/// is owned by `GinRowGroupRegistry` / `JsonbPostingRegistry`.
#[derive(Debug, Default)]
struct TermPostingList {
    /// `term → set of interned file paths containing ≥1 row with that term`.
    entries: HashMap<String, HashSet<Arc<str>>>,
    /// Ordered sequence of term insertions for FIFO eviction (keys).
    insert_order: Vec<String>,
    /// Total posting-pair count (sum of all set sizes).
    total_count: usize,
    /// Interner: one `Arc<str>` per distinct file path.
    files: HashSet<Arc<str>>,
    /// Set once the first eviction fires, so the operator warning is logged
    /// exactly once per `(table, col)` posting list.
    eviction_warned: bool,
    /// Files whose coarse terms were dropped by eviction since they were last
    /// (re)built. A tainted file is NOT completeness-sealable: some of its terms
    /// are missing from the posting list, so trusting it as "complete" would let
    /// a probe prune a file (or a sealed file's own rows) that actually match.
    /// This catches SELF-eviction during a single file's backfill — when a file
    /// large enough to overflow the per-project budget evicts its OWN earlier
    /// trigrams before its `mark_file_indexed` seal. Cleared on `remove_file`
    /// (a CoW replacement rebuilds the file from scratch and may re-seal).
    eviction_tainted: HashSet<Arc<str>>,

    // ── Row tier (optional, second granularity) ──────────────────────────────
    /// SEALED row-granular postings: `term → (file → sorted unique row
    /// offsets)`. Only present for `(term, file)` pairs that (a) were below the
    /// density cap at seal time and (b) survived row-tier budget eviction. A
    /// `(term, file)` absent here falls back to the coarse `entries` tier for
    /// that file — never a false negative.
    row_entries: HashMap<String, HashMap<Arc<str>, Vec<u32>>>,
    /// In-progress row-offset accumulation for the file currently being
    /// indexed, keyed `term → offsets` (appended in `index_row`, finalised in
    /// `seal_file_row_tier`). One file is sealed at a time on the build path,
    /// so this is scoped to `building_file`.
    row_build: HashMap<String, Vec<u32>>,
    /// The file path `row_build` is accumulating for; cleared on seal.
    building_file: Option<Arc<str>>,
    /// Count of indexed (non-null, term-bearing) rows seen for `building_file`,
    /// used to compute each term's per-file density at seal.
    building_rows: u64,
    /// Files that have a SEALED row tier (every term occurring in the file is
    /// either in `row_entries[term][file]` or was dropped by the density cap —
    /// in which case the file is recorded in `row_tier_dense_terms` so the
    /// probe knows the absence is "dense, fall back" not "no posting"). A file
    /// here is row-tier-trustworthy; a file absent here uses the coarse tier.
    row_tier_files: HashSet<Arc<str>>,
    /// `file → set of terms that were DROPPED by the density cap for that file`.
    /// At probe time, if a needle term is dense for a candidate file, that file
    /// cannot be row-narrowed (the term offers no row posting) and must be
    /// decoded in full for that term → the file is emitted with NO row
    /// selection (coarse decode). Distinguishes "dense, must decode" from
    /// "term absent → file prunable".
    row_tier_dense_terms: HashMap<Arc<str>, HashSet<String>>,
    /// Total row-tier budget cost (sum of `row_block_budget_cost` over every
    /// block in `row_entries`). Counted against the per-project budget.
    row_tier_cost: usize,
    /// FIFO order of `(term, file)` row blocks for row-tier eviction.
    row_insert_order: Vec<(String, Arc<str>)>,
}

impl TermPostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Return the interned `Arc<str>` for `file_path`, creating it on first use.
    fn intern_file(&mut self, file_path: &str) -> Arc<str> {
        if let Some(existing) = self.files.get(file_path) {
            return existing.clone();
        }
        let arc: Arc<str> = Arc::from(file_path);
        self.files.insert(arc.clone());
        arc
    }

    /// Add a single `(term, file)` posting pair.
    ///
    /// The pair is deduplicated: re-indexing a row (or another row of the same
    /// file with the same term) does NOT inflate `total_count` — the budget
    /// counts distinct pairs, which is what actually occupies memory.
    ///
    /// Returns the set of file paths whose posting entries were **evicted** by
    /// this insert (empty when no eviction occurred).  The caller
    /// (`GinIndexRegistry::index_row`) uses this set to mark *only* the
    /// affected files as un-indexed in the per-file completeness map — leaving
    /// files that still have complete posting coverage prunable.  This is the
    /// key difference from the old global-wipe approach: eviction is
    /// file-scoped, not column-scoped.
    ///
    /// `budget` is the effective per-list ceiling supplied by the registry. For
    /// a single-project process it is the global `posting_budget()`; under the
    /// per-project partition (Fix: noisy-neighbour GIN) it is the project's
    /// fair-share-with-floor allowance, so one project's churn evicts only its
    /// own postings and can never drain a sibling below its floor.
    ///
    /// Returns `(evicted_files, removed_pairs)`: `removed_pairs` is how many
    /// posting pairs this insert removed (after netting the +1 it may have
    /// added), so the registry can keep its per-project pair accounting exact.
    fn insert(&mut self, term: &str, file: &Arc<str>, budget: usize) -> (HashSet<String>, usize) {
        self.insert_with_row(term, file, None, budget)
    }

    /// Coarse-tier insert, plus optional ROW-tier accumulation.
    ///
    /// When `row_offset` is `Some(o)`, `o` is the absolute row index of this
    /// term occurrence within `file`. It is appended to the in-progress
    /// per-file row builder; the builder is finalised (density cap applied,
    /// sorted, deduped, moved into `row_entries`) by [`Self::seal_file_row_tier`]
    /// once every row of the file has been indexed. The coarse tier is updated
    /// identically whether or not a row offset is supplied — the row tier is
    /// strictly additive and never affects coarse correctness.
    fn insert_with_row(
        &mut self,
        term: &str,
        file: &Arc<str>,
        row_offset: Option<u64>,
        budget: usize,
    ) -> (HashSet<String>, usize) {
        // Row-tier accumulation. The builder is scoped to one file; if a
        // different file starts indexing without a seal (a logic error on the
        // build path), drop the stale builder rather than mixing offsets across
        // files (which would be unsound — offsets are file-relative).
        if let Some(off) = row_offset {
            match &self.building_file {
                Some(cur) if Arc::ptr_eq(cur, file) || cur.as_ref() == file.as_ref() => {}
                _ => {
                    self.row_build.clear();
                    self.building_rows = 0;
                    self.building_file = Some(file.clone());
                }
            }
            // A `u32` offset caps a file at ~4.29B rows; clamp defensively.
            let off32 = off.min(u32::MAX as u64) as u32;
            self.row_build.entry(term.to_string()).or_default().push(off32);
        }

        match self.entries.get_mut(term) {
            Some(set) => {
                if set.insert(file.clone()) {
                    self.total_count += 1;
                }
            }
            None => {
                self.insert_order.push(term.to_string());
                let mut set = HashSet::new();
                set.insert(file.clone());
                self.entries.insert(term.to_string(), set);
                self.total_count += 1;
            }
        }

        if self.total_count > budget {
            let before = self.total_count;
            let evicted_files = self.evict_oldest();
            let removed = before.saturating_sub(self.total_count);
            (evicted_files, removed)
        } else {
            (HashSet::new(), 0)
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
                for f in &set {
                    evicted_files.insert(f.as_ref().to_string());
                    // Taint the file: at least one of its terms is now gone, so
                    // it cannot be completeness-sealed until it is fully rebuilt
                    // (CoW `remove_file` clears the taint).
                    if let Some(arc) = self.files.get(f.as_ref()) {
                        self.eviction_tainted.insert(arc.clone());
                    } else {
                        self.eviction_tainted.insert(f.clone());
                    }
                }
                self.total_count = self.total_count.saturating_sub(set.len());
            }
        }
        evicted_files
    }

    /// `true` when `file`'s coarse terms were dropped by eviction since its last
    /// (re)build — i.e. it is NOT safe to seal as completeness-complete.
    fn is_eviction_tainted(&self, file: &str) -> bool {
        self.eviction_tainted.iter().any(|f| f.as_ref() == file)
    }

    /// Probe for `term`. Returns `None` when the term has never been indexed
    /// (caller must treat as "unknown → full scan").  Returns `Some(set)` for
    /// a known term; the set may be empty when all posting entries for this
    /// term were evicted.
    fn probe_term(&self, term: &str) -> Option<&HashSet<Arc<str>>> {
        self.entries.get(term)
    }

    /// Remove all entries that reference `file_path`. Called when a file is
    /// compacted or deleted. Returns the number of posting pairs removed so the
    /// registry can keep its per-project pair accounting exact.
    fn remove_file(&mut self, file_path: &str) -> usize {
        let mut removed = 0usize;
        for set in self.entries.values_mut() {
            if set.remove(file_path) {
                self.total_count = self.total_count.saturating_sub(1);
                removed += 1;
            }
        }
        self.files.remove(file_path);
        // Row tier: drop every block referencing this file, free its budget,
        // and clear its trustworthiness markers. A file that is gone can no
        // longer be row-narrowed (and must not be: its offsets are stale).
        let mut row_freed = 0usize;
        for blocks in self.row_entries.values_mut() {
            if let Some(offs) = blocks.remove(file_path) {
                row_freed += row_block_budget_cost(offs.len());
            }
        }
        self.row_entries.retain(|_, blocks| !blocks.is_empty());
        self.row_tier_cost = self.row_tier_cost.saturating_sub(row_freed);
        self.row_insert_order.retain(|(_, f)| f.as_ref() != file_path);
        self.row_tier_files.remove(file_path);
        self.row_tier_dense_terms.remove(file_path);
        // A removed file is gone; clear any eviction taint so a CoW replacement
        // written under the same path can be sealed fresh.
        self.eviction_tainted.retain(|f| f.as_ref() != file_path);
        removed
    }

    // ── Row-tier build / probe / evict ───────────────────────────────────────

    /// Record that one indexed (non-null, term-bearing) row was processed for
    /// the file currently being built. Drives the per-file density denominator.
    fn note_indexed_row(&mut self, file: &Arc<str>) {
        match &self.building_file {
            Some(cur) if cur.as_ref() == file.as_ref() => {}
            _ => {
                self.row_build.clear();
                self.building_rows = 0;
                self.building_file = Some(file.clone());
            }
        }
        self.building_rows += 1;
    }

    /// Finalise the row tier for `file`: apply the density cap per term, sort +
    /// dedup the surviving offset lists, move them into `row_entries`, record
    /// dropped (dense) terms, and mark the file row-tier-trustworthy. Returns
    /// the budget cost added (so the registry can fold it into per-project
    /// accounting). Called exactly once per file, after all its rows have been
    /// indexed.
    ///
    /// `max_ratio_pct` is the density cap (% of the file's indexed rows). A
    /// `(term, file)` block whose offset count exceeds that ratio is DROPPED —
    /// the file decodes in full for that term (recorded in
    /// `row_tier_dense_terms`). The block is also dropped (kept coarse-only)
    /// when `row_offset` was never supplied for this build (offsets disabled).
    fn seal_file_row_tier(&mut self, file: &Arc<str>, max_ratio_pct: u64) -> usize {
        // Only seal the file we actually accumulated. A mismatch means the row
        // tier was not driven for this file (e.g. offsets disabled) — leave the
        // file coarse-only (no row_tier_files entry → probe uses coarse tier).
        let building_matches =
            matches!(&self.building_file, Some(cur) if cur.as_ref() == file.as_ref());
        if !building_matches {
            self.row_build.clear();
            self.building_rows = 0;
            self.building_file = None;
            return 0;
        }
        let total_rows = self.building_rows;
        // Density threshold: floor(total_rows * pct / 100). A block with
        // strictly more offsets than this is "dense" → dropped (decode in full
        // for that term). Floor is the conservative choice — it drops a block
        // slightly sooner, never keeps one it should drop.
        let dense_threshold = total_rows.saturating_mul(max_ratio_pct) / 100;
        let mut added_cost = 0usize;
        let build = std::mem::take(&mut self.row_build);
        let mut dense: HashSet<String> = HashSet::new();
        for (term, mut offs) in build {
            offs.sort_unstable();
            offs.dedup();
            let n = offs.len() as u64;
            if n == 0 {
                continue;
            }
            if n > dense_threshold {
                // Too dense to be worth a row posting — coarse decode is fine.
                dense.insert(term);
                continue;
            }
            let cost = row_block_budget_cost(offs.len());
            self.row_entries
                .entry(term.clone())
                .or_default()
                .insert(file.clone(), offs);
            self.row_insert_order.push((term, file.clone()));
            self.row_tier_cost += cost;
            added_cost += cost;
        }
        if !dense.is_empty() {
            self.row_tier_dense_terms.insert(file.clone(), dense);
        }
        self.row_tier_files.insert(file.clone());
        self.building_file = None;
        self.building_rows = 0;
        added_cost
    }

    /// Evict the oldest row-tier blocks until `row_tier_cost <= budget`.
    /// Returns the budget cost freed. The COARSE tier is never touched here —
    /// row postings are pure accelerators and are sacrificed first under
    /// pressure. A file whose last row block is evicted loses
    /// `row_tier_files`/`row_tier_dense_terms` membership so the probe cleanly
    /// falls back to coarse decode for it (never a false negative).
    fn evict_row_tier_to(&mut self, budget: usize) -> usize {
        if self.row_tier_cost <= budget {
            return 0;
        }
        let mut freed = 0usize;
        let mut idx = 0usize;
        while self.row_tier_cost > budget && idx < self.row_insert_order.len() {
            let (term, file) = self.row_insert_order[idx].clone();
            idx += 1;
            if let Some(blocks) = self.row_entries.get_mut(&term) {
                if let Some(offs) = blocks.remove(&file) {
                    let cost = row_block_budget_cost(offs.len());
                    self.row_tier_cost = self.row_tier_cost.saturating_sub(cost);
                    freed += cost;
                    if blocks.is_empty() {
                        self.row_entries.remove(&term);
                    }
                    // COMPLETENESS: evicting ANY block of a file makes that
                    // file's row tier INCOMPLETE — a probe can no longer trust
                    // "term has no block ⇒ term absent" (the block may have been
                    // evicted, not genuinely absent), nor a per-row count (an
                    // evicted block under-counts a real match). Either error
                    // would drop a true match. So un-seal the file's row tier on
                    // the first evicted block: probes fall back to COARSE decode
                    // for it (the coarse tier — never row-evicted — is the
                    // correctness-preserving must-keep). The remaining blocks for
                    // this file are now dead weight; drop them too so the budget
                    // and probe state stay consistent.
                    if self.row_tier_files.remove(&file) {
                        // Purge this file's other surviving row blocks.
                        for b in self.row_entries.values_mut() {
                            if let Some(o) = b.remove(&file) {
                                let c = row_block_budget_cost(o.len());
                                self.row_tier_cost = self.row_tier_cost.saturating_sub(c);
                                freed += c;
                            }
                        }
                        self.row_entries.retain(|_, b| !b.is_empty());
                        self.row_tier_dense_terms.remove(&file);
                    }
                }
            }
        }
        // Compact the consumed prefix of the FIFO order.
        if idx > 0 {
            self.row_insert_order.drain(..idx.min(self.row_insert_order.len()));
        }
        freed
    }

    /// `true` when `file` has a sealed, trustworthy row tier.
    fn file_has_row_tier(&self, file: &str) -> bool {
        self.row_tier_files.iter().any(|f| f.as_ref() == file)
    }

    /// For a needle `terms` set and a single `file` that has a sealed row tier,
    /// compute the AND-intersection of the per-term row-offset lists, returning
    /// a sorted superset of the matching row offsets.
    ///
    /// Returns:
    /// * `RowProbe::Full` — the file cannot be row-narrowed for this needle
    ///   (EVERY needle term is DENSE for the file → no row posting offers a
    ///   constraint): decode the file in full (coarse).
    /// * `RowProbe::Rows(v)` — `v` is the sorted superset of matching row
    ///   offsets; the reader decodes only these rows. `v` empty ⇒ no row in the
    ///   file matches (the file is fully prunable, but the caller already knows
    ///   the file is a coarse candidate, so an empty row set safely yields zero
    ///   rows after the UDF recheck).
    /// * `RowProbe::Absent` — some needle term has NO posting AND is not dense
    ///   for this file: the term provably never occurs here, so the file holds
    ///   no match (fully prunable).
    ///
    /// A DENSE needle term contributes NO constraint and is SKIPPED — the
    /// remaining selective terms still bound the row set, and dropping a
    /// constraint only ever WIDENS the set (keeps the superset invariant). This
    /// is what lets the bench's everywhere-dense `key:tag` term coexist with a
    /// selective `kv:tag="rare"` term: the selective term still narrows decode.
    fn probe_row_offsets(&self, terms: &[String], file: &str) -> RowProbe {
        let dense_for_file = self
            .row_tier_dense_terms
            .iter()
            .find(|(f, _)| f.as_ref() == file)
            .map(|(_, s)| s);
        let mut acc: Option<Vec<u32>> = None;
        for term in terms {
            // Dense term for this file → no row posting → no constraint. Skip it
            // (dropping a conjunct only widens the result, preserving the
            // superset invariant). The file decodes the surviving rows of the
            // selective terms; if EVERY term is dense, `acc` stays None → Full.
            if dense_for_file.is_some_and(|s| s.contains(term)) {
                continue;
            }
            let block = self
                .row_entries
                .get(term)
                .and_then(|blocks| blocks.iter().find(|(f, _)| f.as_ref() == file).map(|(_, v)| v));
            match block {
                None => {
                    // Term has no row block for this file and is not dense:
                    // it provably never occurs in this (row-tier-sealed) file.
                    return RowProbe::Absent;
                }
                Some(offs) => {
                    acc = Some(match acc {
                        None => offs.clone(),
                        Some(prev) => intersect_sorted(&prev, offs),
                    });
                }
            }
        }
        match acc {
            None => RowProbe::Full,
            Some(v) => RowProbe::Rows(v),
        }
    }

    /// Trigram row-tier probe: COUNT-based OR-merge (not the AND-merge that
    /// `probe_row_offsets` uses for `@>`). For the supplied needle-trigram
    /// `terms` and a single `file` with a sealed row tier, return the sorted
    /// superset of row offsets whose name carries `>= min_shared` DISTINCT
    /// needle trigrams.
    ///
    /// Returns:
    /// * `RowProbe::Full` — the file cannot be row-narrowed: at least one needle
    ///   trigram is DENSE for the file (its row posting was dropped by the
    ///   density cap), so a row could clear `min_shared` partly via the dense
    ///   term without any sparse-term offset to enumerate it. Decoding in full
    ///   is the only safe choice — a dense term offers no offsets to union, and
    ///   omitting it could under-count a real match below `min_shared` and drop
    ///   it. (Coarse decode + the `similarity()` recheck is always correct.)
    /// * `RowProbe::Rows(v)` — sorted superset of offsets clearing `min_shared`.
    /// * `RowProbe::Absent` — every needle trigram is provably absent from this
    ///   sealed file (no posting and not dense): no row can share even one
    ///   needle trigram, so none can match (`min_shared >= 1`). Fully prunable.
    fn probe_trgm_row_offsets(
        &self,
        terms: &[String],
        file: &str,
        min_shared: usize,
    ) -> RowProbe {
        let min_shared = min_shared.max(1);
        let dense_for_file = self
            .row_tier_dense_terms
            .iter()
            .find(|(f, _)| f.as_ref() == file)
            .map(|(_, s)| s);
        // Per-offset count of distinct needle trigrams occurring at that row.
        let mut counts: HashMap<u32, u32> = HashMap::new();
        let mut any_present = false;
        for term in terms {
            // A dense needle trigram has no row posting but DOES occur in many
            // rows. We cannot enumerate them, so the file is not row-narrowable
            // for this needle → decode in full (Full). Omitting the dense term
            // from the count would risk under-counting a true match.
            if dense_for_file.is_some_and(|s| s.contains(term)) {
                return RowProbe::Full;
            }
            let block = self
                .row_entries
                .get(term)
                .and_then(|blocks| blocks.iter().find(|(f, _)| f.as_ref() == file).map(|(_, v)| v));
            if let Some(offs) = block {
                any_present = true;
                for &o in offs {
                    *counts.entry(o).or_insert(0) += 1;
                }
            }
            // A term with no block (and not dense) provably never occurs in this
            // sealed file → it contributes nothing (correct OR-merge behaviour).
        }
        if !any_present {
            // No needle trigram occurs anywhere in this sealed file → no row can
            // share even one trigram → none can match (min_shared >= 1).
            return RowProbe::Absent;
        }
        let mut out: Vec<u32> = counts
            .into_iter()
            .filter(|&(_, c)| c as usize >= min_shared)
            .map(|(o, _)| o)
            .collect();
        out.sort_unstable();
        RowProbe::Rows(out)
    }
}

/// Intersection of two ascending-sorted, deduplicated `u32` offset lists.
/// O(a + b). Result is ascending-sorted and deduplicated.
fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Result of a per-file row-tier probe (see
/// [`TermPostingList::probe_row_offsets`]).
#[derive(Debug, Clone, PartialEq, Eq)]
enum RowProbe {
    /// Decode the file in full (a needle term is dense / no row tier to apply).
    Full,
    /// Sorted superset of matching row offsets within the file.
    Rows(Vec<u32>),
    /// A needle term provably never occurs in this file — fully prunable.
    Absent,
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

// ── Trigram term extraction (gin_trgm_ops) ─────────────────────────────────────
//
// The trigram GIN opclass indexes a TEXT column's trigram SET (the same set
// `basin_trgm::similarity` computes: two-leading + one-trailing space padding
// per alphanumeric word, ASCII case-fold). Each trigram becomes one posting
// `term`, reusing the JSONB posting structures (interning / budget / row tier)
// verbatim. A trigram is exactly 3 bytes after case-folding, but those bytes can
// be the padding space (`0x20`) or any ASCII alnum, so we encode the term as a
// fixed-width lowercase-hex string `"tg:aabbcc"` — unambiguous, collision-free
// with the JSONB `key:`/`kv:`/`path_hash:` namespaces, and a valid UTF-8 map
// key. The prefix keeps the keyspace disjoint so a column can never be probed
// with the wrong opclass's terms.

/// Encode one 3-byte trigram as a stable posting term `"tg:aabbcc"`.
#[inline]
fn trigram_term(tg: &[u8; 3]) -> String {
    format!("tg:{:02x}{:02x}{:02x}", tg[0], tg[1], tg[2])
}

/// Extract trigram posting terms from a TEXT value, using `basin_trgm`'s exact
/// extraction (same padding/case-fold as `similarity()` — consistency is
/// correctness: the probe needle is extracted the same way, and the `%`
/// predicate that rechecks every candidate calls the same `similarity()`).
///
/// Returns a sorted, deduplicated `Vec<String>` (one term per distinct
/// trigram). Empty when `text` has no alphanumeric run (`pg_trgm` returns `{}`).
pub fn extract_trigram_terms(text: &str) -> Vec<String> {
    basin_trgm::extract(text).iter().map(trigram_term).collect()
}

/// Conservative count-based pruning threshold for `name % needle` at similarity
/// threshold `t`: the minimum number of DISTINCT needle trigrams a candidate
/// row's trigram set must share with the needle.
///
/// # Math (provably conservative vs. `similarity(name, needle) >= t`)
///
/// `similarity = i / u` where `i = |T(name) ∩ T(needle)|` and
/// `u = |T(name) ∪ T(needle)| = |T(name)| + Q - i` with `Q = |T(needle)|`.
///
/// Because `i <= |T(name)|`, we have `u = |T(name)| + Q - i >= Q`. So for any
/// matching row (`i / u >= t`):
///
/// ```text
///   i >= t * u >= t * Q   ⟹   i >= ceil(t * Q)
/// ```
///
/// Hence a row that shares fewer than `ceil(t * Q)` needle trigrams CANNOT
/// match and is safely pruned. This is exactly PostgreSQL's GIN trigram bound.
/// With `t > 0` the bound is `>= 1`, so the floor-1 superset (share ≥ 1
/// trigram) is the `t → 0+` special case; the count bound only ever prunes
/// MORE, never a real match. The `%` predicate re-evaluates `similarity()` on
/// every surviving candidate (recheck discipline), so over-inclusion is merely
/// slower, never wrong.
///
/// Returns `0` when the needle has no trigrams (caller declines to prune).
#[inline]
pub fn trgm_min_shared(needle_trgm_count: usize, threshold: f32) -> usize {
    if needle_trgm_count == 0 {
        return 0;
    }
    // ceil(t * Q). Clamp the threshold into [0, 1]; a non-positive threshold
    // degrades to the floor-1 superset (still conservative).
    let t = threshold.clamp(0.0, 1.0) as f64;
    let raw = t * needle_trgm_count as f64;
    let ceil = raw.ceil() as usize;
    ceil.max(1).min(needle_trgm_count)
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
/// For `jsonb_ops`:   the `key:k` terms of the needle, plus `kv:k=v` terms
/// for **scalar** values only.
/// For `jsonb_path_ops`: `path_hash:<h>` terms for **scalar leaf** paths only.
///
/// Probe terms share `extract_terms`' key space (the AND-merge requires it),
/// but they must each be a *necessary condition* on a containing document —
/// the AND-merge prunes every file missing any one of them.  Exact-value
/// terms for object/array needle values are NOT necessary conditions:
/// `{"a":{"x":1,"y":2}}` contains the needle `{"a":{"x":1}}` but does NOT
/// carry the needle's `kv:a={"x":1}` term (its term is `kv:a={"x":1,"y":2}`).
/// Emitting that term would prune the containing file — a dropped row.  So:
///
/// * scalar values   → `kv:k=v` (necessary: containment of a scalar is
///   equality, and the indexed doc emits the identical compact form);
/// * object values   → recurse (the nested `key:`/`kv:` terms of the
///   needle's sub-keys are emitted by the index side's recursion too);
/// * array values    → key-presence only (array containment is subset, the
///   exact compact repr is not necessary; elements aren't decomposed).
///
/// An empty result (e.g. `{"a":[1,2]}` under `jsonb_path_ops`) makes the
/// probe return `NoIndex` → full scan (safe).
pub fn needle_terms(needle: &Value, opclass: &str) -> Vec<String> {
    let mut terms = Vec::new();
    needle_terms_inner(needle, opclass, "", &mut terms);
    terms
}

fn needle_terms_inner(value: &Value, opclass: &str, path_prefix: &str, out: &mut Vec<String>) {
    let Value::Object(map) = value else { return };
    for (k, v) in map {
        let path = if path_prefix.is_empty() {
            k.clone()
        } else {
            format!("{path_prefix}.{k}")
        };
        if opclass == "jsonb_path_ops" {
            match v {
                Value::Object(_) => needle_terms_inner(v, opclass, &path, out),
                Value::Array(_) => {
                    // Exact array repr is not a necessary condition — skip.
                }
                _ => {
                    let leaf_repr = compact_value(v);
                    let hash = simple_hash(&format!("{path}={leaf_repr}"));
                    out.push(format!("path_hash:{hash}"));
                }
            }
        } else {
            out.push(format!("key:{k}"));
            match v {
                Value::Object(_) => needle_terms_inner(v, opclass, &path, out),
                Value::Array(_) => {
                    // Exact array repr is not a necessary condition — only the
                    // key-presence term constrains the candidate set.
                }
                _ => out.push(format!("kv:{k}={}", compact_value(v))),
            }
        }
    }
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
    /// Per-project posting-pair accounting (noisy-neighbour partition).
    ///
    /// `project → total posting pairs across ALL its (table, col) lists`. This
    /// is the quantity the per-project budget bounds. A project's entry is
    /// created on its first indexed pair and the number of distinct keys is the
    /// "active projects" count used to compute each project's fair share. The
    /// counter is approximate-but-eventually-exact: it is updated under the same
    /// posting-list lock as the pair mutation it reflects, so it never drifts.
    project_pairs: Mutex<HashMap<ProjectId, usize>>,
}

impl GinIndexRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            indexed_files: Mutex::new(HashMap::new()),
            project_pairs: Mutex::new(HashMap::new()),
        }
    }

    /// Number of projects that currently hold at least one posting pair.
    /// Used as the divisor in the fair-share budget. O(projects) — cheap (the
    /// project count is tiny relative to per-row indexing work).
    fn active_project_count(&self) -> usize {
        self.project_pairs
            .lock()
            .map(|m| m.values().filter(|&&n| n > 0).count())
            .unwrap_or(1)
    }

    /// Current effective per-project posting budget. Exposed for tests/diagnostics.
    pub fn effective_project_budget(&self) -> usize {
        per_project_budget(self.active_project_count())
    }

    /// Total posting pairs currently held for `project` across all its lists.
    /// Test/observability surface for the noisy-neighbour partition.
    pub fn project_pair_count(&self, project: &ProjectId) -> usize {
        self.project_pairs
            .lock()
            .map(|m| m.get(project).copied().unwrap_or(0))
            .unwrap_or(0)
    }

    /// Adjust a project's pair total by a signed delta, keeping the map free of
    /// zero/absent entries so `active_project_count` stays accurate.
    fn adjust_project_pairs(&self, project: &ProjectId, added: usize, removed: usize) {
        if added == 0 && removed == 0 {
            return;
        }
        if let Ok(mut map) = self.project_pairs.lock() {
            let cur = map.entry(*project).or_insert(0);
            *cur = cur.saturating_add(added).saturating_sub(removed);
            if *cur == 0 {
                map.remove(project);
            }
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
        // Row-group is accepted for call-site compatibility but not stored
        // (coarse granularity is the FILE). `row` is the absolute row index
        // within `file_path` and drives the optional ROW tier — it maps
        // directly to the reader's `RowSelection` coordinate space.
        _row_group: u32,
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
        self.index_terms(project, table, col, &terms, file_path, row);
    }

    /// Index a TEXT value's TRIGRAM set for a `gin_trgm_ops` column from
    /// `file_path` / `row`. Extracts trigrams via `basin_trgm` (same padding /
    /// case-fold as `similarity()`) and feeds the resulting terms through the
    /// SAME posting machinery as [`index_row`] — interning, the per-project
    /// budget, eviction, and the optional row tier are all shared. A text value
    /// with no alphanumeric run produces no trigrams and is skipped.
    pub fn index_text_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        text: &str,
        file_path: &str,
        row: u64,
    ) {
        let terms = extract_trigram_terms(text);
        if terms.is_empty() {
            return;
        }
        self.index_terms(project, table, col, &terms, file_path, row);
    }

    /// Shared posting-insertion body for one row's pre-extracted `terms`, used by
    /// both the JSONB ([`index_row`]) and trigram ([`index_text_row`]) build
    /// paths. From here the budget, interning, eviction, row-tier accumulation,
    /// and per-project accounting are identical regardless of opclass.
    fn index_terms(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        terms: &[String],
        file_path: &str,
        row: u64,
    ) {
        let want_row_tier = row_tier_enabled();
        // Per-project partition (noisy-neighbour GIN): the eviction trigger is
        // this project's fair share of the global budget, with a floor, NOT the
        // raw global budget. Computing it from the live active-project count
        // means project A's churn lowers everyone's fair share as projects come
        // and go but can never push project B below its floor — A evicts its own
        // postings first. The budget is read before taking the list lock so the
        // (cheap) project-count scan never nests under a posting-list lock.
        let budget = self.effective_project_budget();
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("TermPostingList lock poisoned");
        let file = list.intern_file(file_path);
        let mut all_evicted_files: HashSet<String> = HashSet::new();
        let mut added_pairs: usize = 0;
        let mut removed_pairs: usize = 0;
        // Row tier: count this row once for the per-file density denominator,
        // then feed each term its row offset. Coarse-tier accounting is
        // unchanged (the row offset is purely additive).
        if want_row_tier {
            list.note_indexed_row(&file);
        }
        for term in terms {
            let before = list.total_count;
            let row_off = if want_row_tier { Some(row) } else { None };
            let (evicted_files, removed) = list.insert_with_row(term, &file, row_off, budget);
            // `total_count` after = before + (added ∈ {0,1}) - removed. Recover
            // the +added term so the per-project accounting nets exactly:
            // added = after - (before - removed).
            let after = list.total_count;
            added_pairs += after.saturating_sub(before.saturating_sub(removed));
            removed_pairs += removed;
            all_evicted_files.extend(evicted_files);
        }
        // Per-file completeness: if any terms were evicted, only the files
        // whose posting entries were dropped need to be de-indexed.  Files
        // that still have complete posting coverage remain prunable.  This
        // degrades gracefully: a large table prunes the files that are
        // indexed, and treats any evicted-file as a forced full-file scan
        // (must-scan), which is safe (no false negatives).
        //
        // NOTE: the indexed_files update happens while the posting-list lock
        // is still held (the `list` guard lives to the end of this scope), so
        // a concurrent probe that snapshots both structures under the same
        // lock order can never observe "term evicted but file still marked".
        if !all_evicted_files.is_empty() {
            if !list.eviction_warned {
                list.eviction_warned = true;
                tracing::warn!(
                    table = %table.as_str(),
                    column = col,
                    budget = posting_budget(),
                    "GIN posting list exceeded its budget; oldest terms evicted. \
                     The index is now PARTIAL: evicted files fall back to full \
                     scans (results stay correct, pruning degrades per-file). \
                     Raise BASIN_GIN_POSTING_BUDGET for this workload."
                );
            }
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
        // Drop the posting-list lock BEFORE touching the per-project accounting
        // map: never hold a posting-list lock and `project_pairs` at once (a
        // strict lock order avoids any deadlock between this path and the
        // active-project scan in `effective_project_budget`, which never takes a
        // posting-list lock).
        drop(list);
        self.adjust_project_pairs(project, added_pairs, removed_pairs);
    }

    /// `true` when eviction has ever fired for `(project, table, col)` — the
    /// posting list is (or was) partial.  Exposed for tests and diagnostics.
    pub fn has_evicted(&self, project: &ProjectId, table: &TableName, col: &str) -> bool {
        match self.get(project, table, col) {
            Some(arc) => arc.lock().expect("TermPostingList lock poisoned").eviction_warned,
            None => false,
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
                        entries.iter().map(|f| f.as_ref().to_string()).collect();
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

    /// Per-file (partial-coverage) containment probe — Phase 5.19.F.
    ///
    /// Unlike [`probe_containment`] + the all-or-nothing completeness guard,
    /// this prunes *what is provable* when only SOME live files are fully
    /// indexed:
    ///
    /// * a live file **not** in the indexed-files set is a forced candidate
    ///   (must scan — we know nothing about it);
    /// * a live file **in** the indexed-files set can be pruned when some
    ///   needle term has no posting hit for it.  This is sound because the
    ///   registry maintains the invariant *"file marked indexed ⇒ every term
    ///   occurring in any of its rows is present in the posting list with
    ///   that file's entry"*: `mark_file_indexed` runs only after all of the
    ///   file's rows passed [`index_row`], and eviction un-marks every file
    ///   whose terms were dropped (inside the same posting-list critical
    ///   section).
    ///
    /// The posting-list lock is held while the indexed-files snapshot is
    /// taken, matching the lock order of `index_row`'s evict-then-unmark
    /// path, so the two structures are observed consistently (a torn view
    /// could otherwise prune a file whose terms were just evicted).
    ///
    /// Returns [`GinScanSet::NoIndex`] when nothing is provable (no posting
    /// list, unparseable/empty needle); otherwise the ordered subset of
    /// `live_paths` that must be scanned.  Under-pruning is always safe; the
    /// scan set never excludes a file that could hold a match.
    pub fn probe_containment_scan_set(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        needle_bytes: &[u8],
        live_paths: &[String],
    ) -> GinScanSet {
        let needle: Value = match serde_json::from_slice(needle_bytes) {
            Ok(v) => v,
            Err(_) => return GinScanSet::NoIndex, // unparseable → conservative
        };
        let terms = needle_terms(&needle, opclass);
        if terms.is_empty() {
            // Empty needle matches everything — nothing is provable.
            return GinScanSet::NoIndex;
        }
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return GinScanSet::NoIndex,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");
        // Snapshot indexed_files UNDER the posting-list lock (see doc above).
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let indexed: HashSet<String> = match self.indexed_files.lock() {
            Ok(map) => map.get(&key).cloned().unwrap_or_default(),
            Err(_) => return GinScanSet::NoIndex,
        };

        let mut scan: Vec<String> = Vec::with_capacity(live_paths.len());
        for path in live_paths {
            if !indexed.contains(path) {
                // Unknown coverage → forced candidate.
                scan.push(path.clone());
                continue;
            }
            // Fully indexed: candidate only when EVERY needle term has a
            // posting hit for this file.  A term absent from the map entirely
            // provably does not occur in any indexed file (it was never
            // inserted, or its eviction un-marked the files that had it).
            let all_terms_hit = terms
                .iter()
                .all(|t| list.probe_term(t).is_some_and(|s| s.contains(path.as_str())));
            if all_terms_hit {
                scan.push(path.clone());
            }
        }
        GinScanSet::ScanFiles(scan)
    }

    // ── Trigram (gin_trgm_ops) probe ─────────────────────────────────────────

    /// Per-file trigram-similarity probe for a `name % 'needle'` predicate
    /// (after the rewriter lowers `%` to `similarity(col,'needle') >= t`).
    ///
    /// `needle` is the literal RHS; `threshold` is the session's
    /// `pg_trgm.similarity_threshold` in effect (`t`). The probe prunes what is
    /// PROVABLE under the count-based bound (`trgm_min_shared`):
    ///
    /// * a live file NOT in the indexed-files set is a forced candidate
    ///   (must-scan — unknown coverage);
    /// * a fully-indexed file is a candidate iff it shares `>= ceil(t * Q)`
    ///   DISTINCT needle trigrams (`Q = |T(needle)|`), counted by OR-merging the
    ///   per-trigram posting lists. A file sharing fewer needle trigrams cannot
    ///   contain a row with `similarity >= t` (see `trgm_min_shared`'s proof) and
    ///   is pruned.
    ///
    /// Unlike the JSONB `@>` probe (AND-merge: every needle term required), the
    /// trigram probe is an OR-merge with a COUNT floor — a single missing needle
    /// trigram does not exclude a file, because a name can match the needle
    /// while missing some of its trigrams (Jaccard < 1). The shared-count floor
    /// is what makes the prune conservative without being a no-op.
    ///
    /// The `%` predicate ALWAYS re-evaluates `similarity(col, needle) >= t` on
    /// every surviving candidate row (recheck discipline, exactly like JSONB
    /// `@>`), so over-inclusion is only slower, never wrong.
    ///
    /// Returns [`GinScanSet::NoIndex`] when nothing is provable (no posting list
    /// or the needle has no trigrams). Under-pruning is always safe.
    pub fn probe_trgm_scan_set(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        needle: &str,
        threshold: f32,
        live_paths: &[String],
    ) -> GinScanSet {
        let needle_trgms = basin_trgm::extract(needle);
        let q = needle_trgms.len();
        if q == 0 {
            // No trigrams in the needle — decline to prune (the scan + recheck
            // decides; PG empty-needle semantics are subtle and rare).
            return GinScanSet::NoIndex;
        }
        let min_shared = trgm_min_shared(q, threshold);
        let terms: Vec<String> = needle_trgms.iter().map(trigram_term).collect();

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return GinScanSet::NoIndex,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let indexed: HashSet<String> = match self.indexed_files.lock() {
            Ok(map) => map.get(&key).cloned().unwrap_or_default(),
            Err(_) => return GinScanSet::NoIndex,
        };

        let mut scan: Vec<String> = Vec::with_capacity(live_paths.len());
        for path in live_paths {
            if !indexed.contains(path) {
                // Unknown coverage → forced candidate.
                scan.push(path.clone());
                continue;
            }
            // Count DISTINCT needle trigrams with a posting hit for this file. A
            // trigram term absent from the map provably never occurs in any
            // indexed file (never inserted, or eviction un-marked the files that
            // had it — and an un-marked file took the forced-candidate branch).
            let mut shared = 0usize;
            for t in &terms {
                if list.probe_term(t).is_some_and(|s| s.contains(path.as_str())) {
                    shared += 1;
                    if shared >= min_shared {
                        break;
                    }
                }
            }
            if shared >= min_shared {
                scan.push(path.clone());
            }
        }
        GinScanSet::ScanFiles(scan)
    }

    /// Row-tier trigram selection for `name % 'needle'`: for each candidate file
    /// with a sealed row tier, return the sorted SUPERSET of row offsets whose
    /// name may satisfy `similarity >= t`.
    ///
    /// A row matches only if it carries `>= ceil(t * Q)` DISTINCT needle
    /// trigrams (`min_shared`). The row tier counts, per offset, how many needle
    /// trigrams occur there and keeps offsets reaching `min_shared` — a sorted
    /// superset of the matching rows (the `similarity()` recheck still runs on
    /// each). Files without a sealed row tier (or where a needle trigram was
    /// dense and dropped) decode in full (left out of both maps — the safe
    /// default). A sealed file where no needle trigram has any row posting proves
    /// no row matches → `prunable`.
    ///
    /// The caller is responsible for the overlay / completeness gate (same gate
    /// the coarse path uses); this method assumes the handed file set is already
    /// overlay-free and coarse-trustworthy.
    pub fn probe_trgm_row_selection(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        needle: &str,
        threshold: f32,
        candidate_paths: &[String],
    ) -> RowSelectionPlan {
        let mut plan = RowSelectionPlan::default();
        if !row_tier_enabled() {
            return plan;
        }
        let needle_trgms = basin_trgm::extract(needle);
        let q = needle_trgms.len();
        if q == 0 {
            return plan;
        }
        let min_shared = trgm_min_shared(q, threshold);
        let terms: Vec<String> = needle_trgms.iter().map(trigram_term).collect();
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return plan,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");
        // Snapshot the COARSE completeness set UNDER the posting-list lock. The
        // row tier may only narrow files that are coarse-COMPLETE: coarse
        // eviction un-marks a file from `indexed_files` (making it a forced
        // must-scan candidate) WITHOUT touching `row_tier_files`, so a file can
        // be coarse-incomplete yet still carry a (now-stale-relative-to-coarse)
        // row tier. Narrowing such a file would drop the very rows the forced
        // full scan was meant to recover. Only row-narrow coarse-complete files;
        // coarse-incomplete ones fall through to full decode (the caller already
        // forced them into the candidate set).
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let indexed: HashSet<String> = match self.indexed_files.lock() {
            Ok(map) => map.get(&key).cloned().unwrap_or_default(),
            Err(_) => return plan,
        };
        for path in candidate_paths {
            if !indexed.contains(path) {
                continue; // coarse-incomplete (forced candidate) → full decode
            }
            if !list.file_has_row_tier(path) {
                continue; // no sealed row tier → coarse decode
            }
            match list.probe_trgm_row_offsets(&terms, path, min_shared) {
                RowProbe::Full => { /* coarse decode */ }
                RowProbe::Absent => {
                    plan.prunable.insert(path.clone());
                }
                RowProbe::Rows(offs) => {
                    let offs64: Vec<u64> = offs.into_iter().map(|o| o as u64).collect();
                    plan.row_offsets.insert(path.clone(), offs64);
                }
            }
        }
        plan
    }

    /// Trigram-kNN candidate selection for `ORDER BY col <-> 'needle' LIMIT k`.
    ///
    /// A row's trigram distance is `1 - similarity(col, needle)`. A row sharing
    /// ZERO needle trigrams has `similarity = 0`, hence distance `1` — the worst
    /// possible. So the exact top-k by distance is drawn entirely from rows
    /// sharing `>= 1` needle trigram (`min_shared = 1`), UNLESS fewer than `k`
    /// such candidate rows exist, in which case the remaining slots are filled
    /// by arbitrary distance-1 (zero-shared-trigram) rows. This method returns
    /// the candidate set; the executor computes the EXACT `<->` distance on the
    /// materialised candidate rows (the postings only narrow the SET, never the
    /// ranking) and handles the boundary fill.
    ///
    /// For each live path it reports one of:
    /// * a sorted superset of candidate row offsets (sealed-row-tier files where
    ///   every shared-trigram row is enumerable), via [`TrgmKnnFile::Rows`];
    /// * "scan the whole file" ([`TrgmKnnFile::Full`]) — an un-indexed (forced)
    ///   file, a file lacking a sealed row tier, or a file where a needle trigram
    ///   is dense (its row posting was dropped, so candidate rows can't be
    ///   enumerated): decode in full and let the exact distance re-rank;
    /// * "no candidate rows here" ([`TrgmKnnFile::None`]) — a coarse-complete
    ///   sealed file where no needle trigram occurs at all (every row is
    ///   distance-1). Such a file holds only fill rows, never top-k candidates.
    ///
    /// Returns `None` when there is no usable trigram index or the needle has no
    /// trigrams (needle too short) — the caller declines to the full scan + sort.
    /// The caller is responsible for the overlay / completeness gate.
    pub fn probe_trgm_knn_candidates(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        needle: &str,
        live_paths: &[String],
    ) -> Option<HashMap<String, TrgmKnnFile>> {
        let needle_trgms = basin_trgm::extract(needle);
        if needle_trgms.is_empty() {
            // Needle too short for any trigram → every row is distance-1 and the
            // index can prove nothing. Decline; the caller seq-scans.
            return None;
        }
        let terms: Vec<String> = needle_trgms.iter().map(trigram_term).collect();

        let arc = self.get(project, table, col)?;
        let list = arc.lock().expect("TermPostingList lock poisoned");
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let indexed: HashSet<String> = match self.indexed_files.lock() {
            Ok(map) => map.get(&key).cloned().unwrap_or_default(),
            Err(_) => return None,
        };
        // Row-tier narrowing is only safe on coarse-COMPLETE files (same snapshot
        // discipline as `probe_trgm_row_selection`): a coarse-incomplete file is
        // a forced full scan, and a file without a sealed row tier decodes in
        // full. `min_shared = 1` makes the OR-merge an "any shared trigram" set.
        let mut out: HashMap<String, TrgmKnnFile> = HashMap::with_capacity(live_paths.len());
        for path in live_paths {
            if !indexed.contains(path) {
                out.insert(path.clone(), TrgmKnnFile::Full); // forced full scan
                continue;
            }
            if !row_tier_enabled() || !list.file_has_row_tier(path) {
                out.insert(path.clone(), TrgmKnnFile::Full); // coarse decode
                continue;
            }
            match list.probe_trgm_row_offsets(&terms, path, 1) {
                RowProbe::Full => {
                    out.insert(path.clone(), TrgmKnnFile::Full);
                }
                RowProbe::Absent => {
                    out.insert(path.clone(), TrgmKnnFile::None);
                }
                RowProbe::Rows(offs) => {
                    let offs64: Vec<u64> = offs.into_iter().map(|o| o as u64).collect();
                    out.insert(path.clone(), TrgmKnnFile::Rows(offs64));
                }
            }
        }
        Some(out)
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
        let mut removed_pairs = 0usize;
        if let Some(arc) = self.get(project, table, col) {
            let mut list = arc.lock().expect("TermPostingList lock poisoned");
            removed_pairs = list.remove_file(file_path);
        }
        // Keep the per-project pair accounting exact so a compaction/CoW that
        // drops a file frees that project's partition headroom (lock dropped
        // above before touching project_pairs).
        self.adjust_project_pairs(project, 0, removed_pairs);
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
        // Completeness gate: a file whose terms were dropped by eviction since it
        // was built is INCOMPLETE and must NOT be sealed — sealing it would let a
        // probe trust missing terms and prune a file (or its own rows) that match.
        // This catches SELF-eviction during a single oversized file's backfill
        // (the file's earlier terms evicted by its own later terms before this
        // seal). A tainted file stays OUT of `indexed_files` → forced must-scan
        // candidate (correct, just unpruned). The row tier is likewise left
        // unsealed below so the file decodes in full.
        let tainted = self
            .get(project, table, col)
            .map(|arc| arc.lock().expect("TermPostingList lock poisoned").is_eviction_tainted(file_path))
            .unwrap_or(false);
        if tainted {
            return;
        }
        if let Ok(mut map) = self.indexed_files.lock() {
            map.entry(key.clone()).or_default().insert(file_path.to_string());
        }
        // Seal the ROW tier for this file: apply the density cap, finalise the
        // surviving offset lists, then enforce the per-project row-tier budget
        // (row blocks are evicted FIRST under pressure — they are accelerators,
        // the coarse tier is the must-keep). The row-tier budget shares the
        // same per-project allowance as the coarse pairs; sealing folds its
        // cost into the per-project accounting so a flood of selective row
        // postings cannot exceed a project's fair share.
        if row_tier_enabled() {
            if let Some(arc) = self.get(project, table, col) {
                let budget = self.effective_project_budget();
                let (added, freed) = {
                    let mut list = arc.lock().expect("TermPostingList lock poisoned");
                    // `intern_file` returns the canonical Arc the builder used,
                    // so `seal_file_row_tier`'s `building_file == file` check
                    // matches (same interned path).
                    let file = list.intern_file(file_path);
                    let added = list.seal_file_row_tier(&file, row_tier_max_ratio_pct());
                    let freed = list.evict_row_tier_to(budget);
                    (added, freed)
                };
                // Row-tier cost is counted in the SAME per-project budget units
                // as coarse pairs (both are "posting budget"); fold the net so
                // the partition stays honest.
                self.adjust_project_pairs(project, added, freed);
            }
        }
    }

    /// Probe the ROW tier for a `@>` needle across the supplied candidate
    /// files. For each file the registry returns one of:
    ///   * a sorted superset of matching row offsets (decode only those rows),
    ///   * "decode in full" (a needle term is dense for the file, or the file
    ///     has no sealed row tier),
    ///   * "prunable" (a needle term provably never occurs in the file).
    ///
    /// The result is a per-file `RowSelectionPlan`:
    ///   * `row_offsets`: `file → sorted absolute row offsets` for files that
    ///     can be row-narrowed (a SUPERSET of true matches — the UDF rechecks);
    ///   * `prunable`: files provably holding no match (caller may drop them);
    ///   * files NOT in either set decode in full (coarse tier).
    ///
    /// Trustworthiness mirrors the coarse tier exactly: a file is row-narrowed
    /// ONLY when it has a sealed row tier (`file_has_row_tier`). The caller is
    /// responsible for the overlay / completeness gate (the same gate the
    /// coarse path uses) — this method assumes the file set it is handed is
    /// already overlay-free and complete.
    pub fn probe_row_selection(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        needle_bytes: &[u8],
        candidate_paths: &[String],
    ) -> RowSelectionPlan {
        let mut plan = RowSelectionPlan::default();
        if !row_tier_enabled() {
            return plan;
        }
        let needle: Value = match serde_json::from_slice(needle_bytes) {
            Ok(v) => v,
            Err(_) => return plan,
        };
        let terms = needle_terms(&needle, opclass);
        if terms.is_empty() {
            return plan;
        }
        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return plan,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");
        for path in candidate_paths {
            if !list.file_has_row_tier(path) {
                // No sealed row tier → coarse decode (leave out of both maps).
                continue;
            }
            match list.probe_row_offsets(&terms, path) {
                RowProbe::Full => { /* coarse decode */ }
                RowProbe::Absent => {
                    plan.prunable.insert(path.clone());
                }
                RowProbe::Rows(offs) => {
                    // Convert to u64 for the reader's coordinate space.
                    let offs64: Vec<u64> = offs.into_iter().map(|o| o as u64).collect();
                    plan.row_offsets.insert(path.clone(), offs64);
                }
            }
        }
        plan
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
                            entries.iter().map(|f| f.as_ref().to_string()).collect();
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
                    for f in entries {
                        candidate_files.insert(f.as_ref().to_string());
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
    /// completeness guard remains valid for file-level pruning. A replacement
    /// file with zero rows (or an all-NULL JSONB column) is marked too: its
    /// empty posting set is exact, so pruning it on a probe miss is sound.
    /// Marking is withheld only when rows exist whose column could not be
    /// processed (absent from the batch schema / non-LargeBinary type).
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
        // Completeness decision: the new file may be marked fully indexed
        // unless some batch carries ROWS whose JSONB column we could not
        // process (column absent from the batch schema, or an unexpected
        // physical type) — claiming coverage there would let the probe prune
        // a file holding unindexed values (a false negative). Crucially, a
        // replacement file with ZERO rows, or whose JSONB column is entirely
        // NULL, IS fully indexed: its (empty) posting set is exact — a probe
        // for any term correctly never lists it. The old `indexed_any` flag
        // (set only after downcasting at least one batch's column) left such
        // files permanently OUT of the completeness set, breaking file-level
        // pruning for the whole table forever after e.g. a CoW rewrite that
        // emptied a file.
        let mut coverage_ok = true;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let Ok(col_idx) = batch.schema().index_of(col) else {
                coverage_ok = false;
                continue;
            };
            let col_arr = batch.column(col_idx);
            // Trigram GIN reads a TEXT column; JSONB GIN reads the LargeBinary
            // JSONB payload. Dispatch on opclass so the CoW replacement file is
            // re-indexed through the matching extraction (same machinery, same
            // completeness-seal discipline).
            if opclass == "gin_trgm_ops" {
                // TEXT column. Accept the physical string encodings a CoW
                // replacement batch can carry (Utf8 / LargeUtf8 / Utf8View); a
                // silent downcast miss must NOT seal the file as complete (it
                // would let the trgm probe prune rows that exist).
                enum StrCol<'a> {
                    Small(&'a arrow_array::StringArray),
                    Large(&'a arrow_array::LargeStringArray),
                    View(&'a arrow_array::StringViewArray),
                }
                let arr = if let Some(a) =
                    col_arr.as_any().downcast_ref::<arrow_array::StringArray>()
                {
                    Some(StrCol::Small(a))
                } else if let Some(a) =
                    col_arr.as_any().downcast_ref::<arrow_array::LargeStringArray>()
                {
                    Some(StrCol::Large(a))
                } else if let Some(a) =
                    col_arr.as_any().downcast_ref::<arrow_array::StringViewArray>()
                {
                    Some(StrCol::View(a))
                } else {
                    None
                };
                match arr {
                    Some(arr) => {
                        let n = match &arr {
                            StrCol::Small(a) => a.len(),
                            StrCol::Large(a) => a.len(),
                            StrCol::View(a) => a.len(),
                        };
                        for row in 0..n {
                            let val = match &arr {
                                StrCol::Small(a) => (!a.is_null(row)).then(|| a.value(row)),
                                StrCol::Large(a) => (!a.is_null(row)).then(|| a.value(row)),
                                StrCol::View(a) => (!a.is_null(row)).then(|| a.value(row)),
                            };
                            if let Some(text) = val {
                                self.index_text_row(
                                    project, table, col, text, new_file_path, row as u64,
                                );
                            }
                        }
                    }
                    None => coverage_ok = false,
                }
            } else if let Some(arr) =
                col_arr.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()
            {
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
            } else {
                // Rows exist but the column's physical type is not the JSONB
                // LargeBinary encoding — we indexed nothing for them, so we
                // cannot claim coverage.
                coverage_ok = false;
            }
        }
        if coverage_ok {
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

/// Result of a per-file (partial-coverage) GIN probe — see
/// [`GinIndexRegistry::probe_containment_scan_set`].
#[derive(Debug)]
pub enum GinScanSet {
    /// Nothing is provable (no posting list / unusable needle).  The caller
    /// must scan every live file.
    NoIndex,
    /// The ordered subset of the supplied live paths that must be scanned:
    /// un-indexed files (forced) plus indexed files with posting hits for
    /// every needle term.  May equal the live set (no pruning possible) or be
    /// empty (no live file can match).
    ScanFiles(Vec<String>),
}

/// Result of a ROW-tier probe — see [`GinIndexRegistry::probe_row_selection`].
///
/// `row_offsets` carries, per file, the sorted SUPERSET of absolute row offsets
/// that may match (the reader decodes only these rows; the `jsonb_contains` UDF
/// re-checks each). `prunable` lists files a needle term provably never touches.
/// Any candidate file in NEITHER map decodes in full (coarse tier) — that is
/// the safe default and the only behaviour for files without a sealed row tier.
#[derive(Debug, Default, Clone)]
pub struct RowSelectionPlan {
    /// `file → sorted ascending absolute row offsets` (superset of matches).
    pub row_offsets: HashMap<String, Vec<u64>>,
    /// Files provably holding no match for the needle.
    pub prunable: HashSet<String>,
}

impl RowSelectionPlan {
    /// `true` when the plan narrows nothing (no row offsets, no prunable file).
    /// The caller skips the row-selection delivery entirely in that case.
    pub fn is_empty(&self) -> bool {
        self.row_offsets.is_empty() && self.prunable.is_empty()
    }
}

/// Per-file verdict from [`GinIndexRegistry::probe_trgm_knn_candidates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrgmKnnFile {
    /// Decode the whole file (un-indexed/forced, no sealed row tier, or a needle
    /// trigram is dense so candidate rows can't be enumerated). Every decoded
    /// row is a candidate for the exact distance re-rank.
    Full,
    /// Sorted superset of absolute row offsets sharing `>= 1` needle trigram —
    /// the kNN candidate rows in this file. Other rows in the file are
    /// distance-1 (fill only).
    Rows(Vec<u64>),
    /// A coarse-complete sealed file where no needle trigram occurs at all —
    /// every row is distance-1 (fill only, never a top-k candidate).
    None,
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
    /// `true` when the projection is a bare aggregate over the whole relation
    /// (e.g. `COUNT(*)`).  The pruned-table path registers the full relation
    /// and DataFusion computes the aggregate over it, so the row-group prune
    /// still applies — but the Empty short-circuit in `executor.rs` must NOT
    /// short-circuit with zero rows for these shapes (a no-match `COUNT(*)`
    /// must still return a single `0` row).  See the gate at the short-circuit.
    pub is_aggregate: bool,
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

    // WHERE clause must be `col @> literal`.
    //
    // `col <@ literal` is deliberately NOT accelerated: the posting probe
    // AND-merges the literal's terms, which is a necessary condition only for
    // `@>` (the row must contain every needle term).  For `<@` the row is the
    // SUBSET side — a matching row need not contain ANY of the literal's
    // terms (`{} <@ x` is true for every x), so both the Empty short-circuit
    // and file pruning would drop real matches.  `<@` falls through to the
    // full scan + `jsonb_contained_by` UDF (correct, just not pruned).
    let (col_name, needle_str, is_contains) = match &select.selection {
        Some(sqlparser::ast::Expr::BinaryOp { left, op, right }) => {
            let op_str = op.to_string();
            let is_contains = op_str == "@>";
            if !is_contains {
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

    // Projection: `*`, a list of bare column names, or a whole-relation
    // aggregate (e.g. `COUNT(*)`).  An aggregate projection still routes
    // through the pruned-table path — DataFusion computes the aggregate over
    // the row-group-pruned relation — so we accept it here, recording the
    // `is_aggregate` flag so the executor's Empty short-circuit knows not to
    // collapse a no-match into zero rows (it must yield a single `0` row).
    //
    // Bare aggregates have no meaningful column projection, so we register the
    // full relation (`None`) and let DataFusion project/aggregate.
    let (projection, is_aggregate) = match extract_simple_projection(&select.projection) {
        Some(proj) => (proj, false),
        None => {
            // GROUP BY / HAVING / DISTINCT change the result shape in ways the
            // pruned-table registration does not special-case beyond "scan the
            // relation"; that is still correct (DataFusion does the rest), but
            // the Empty short-circuit only handles the bare whole-relation
            // aggregate, so restrict the aggregate acceptance to that shape.
            let group_by_empty = matches!(
                &select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(exprs, mods)
                    if exprs.is_empty() && mods.is_empty()
            );
            if group_by_empty
                && select.having.is_none()
                && select.distinct.is_none()
                && is_whole_relation_aggregate(&select.projection)
            {
                (None, true)
            } else {
                return None;
            }
        }
    };

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
        is_aggregate,
    })
}

// ── Trigram (gin_trgm_ops) probe detection ────────────────────────────────────

/// Detected trigram-similarity probe plan for a `col % 'needle'` predicate on a
/// column with a `gin_trgm_ops` GIN index.
#[derive(Debug)]
pub struct TrgmSimilarityPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed TEXT column.
    pub col: String,
    /// The similarity needle (RHS / LHS string literal of `%`).
    pub needle: String,
}

/// Detect `SELECT … FROM table WHERE col % 'needle'` (or `'needle' % col`) on a
/// column with a `gin_trgm_ops` GIN index.
///
/// The SQL arrives *before* the `rewrite_trgm_operators` pass that lowers `%` to
/// `similarity(col,'needle') >= t`, so the raw `%` operator is still present in
/// both the text and the parsed AST (sqlparser's PG dialect parses `%` as a
/// `BinaryOp`). We require ONE operand to be a bare column and the other a
/// single-quoted string literal — exactly the trgm form (a `num % num` modulo
/// has no string operand and is rejected).
///
/// Returns `None` for anything that doesn't match perfectly (caller falls
/// through to the normal scan path — always correct, just unpruned).
pub async fn detect_trgm_similarity(
    sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<TrgmSimilarityPlan> {
    // Fast pre-check: must contain a `%` to be relevant.
    if !sql.contains('%') {
        return None;
    }

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).ok()?;
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

    // WHERE must be `col % 'lit'` or `'lit' % col`.
    let (col_name, needle) = match &select.selection {
        Some(sqlparser::ast::Expr::BinaryOp { left, op, right }) => {
            if op.to_string() != "%" {
                return None;
            }
            let lit_of = |e: &sqlparser::ast::Expr| extract_json_literal_for_prune(e);
            let col_of = |e: &sqlparser::ast::Expr| match e {
                sqlparser::ast::Expr::Identifier(id) => Some(id.value.clone()),
                _ => None,
            };
            if let (Some(c), Some(n)) = (col_of(left), lit_of(right)) {
                (c, n)
            } else if let (Some(n), Some(c)) = (lit_of(left), col_of(right)) {
                (c, n)
            } else {
                return None;
            }
        }
        _ => return None,
    };

    // Catalog: table must have a GIN index on `col_name` with `gin_trgm_ops`.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let _idx = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
            && idx.opclass.as_deref() == Some("gin_trgm_ops")
    })?;

    Some(TrgmSimilarityPlan { table, col: col_name, needle })
}

/// True when every projected item is a bare (un-aliased, un-windowed)
/// aggregate function call — i.e. the whole `WHERE`-filtered relation collapses
/// to a single output row.  Used to let `COUNT(*) … WHERE col @> '…'` route
/// through the row-group-pruned table path.
///
/// Conservative: requires at least one item, all `UnnamedExpr`, all
/// `Expr::Function` with no window/FILTER/DISTINCT-arg modifiers that would
/// change the collapse-to-one-row shape.  Anything else returns `false` →
/// the caller falls through to a full scan (safe).
fn is_whole_relation_aggregate(items: &[sqlparser::ast::SelectItem]) -> bool {
    use sqlparser::ast::{Expr, FunctionArguments, SelectItem};
    if items.is_empty() {
        return false;
    }
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            _ => return false,
        };
        let func = match expr {
            Expr::Function(f) => f,
            _ => return false,
        };
        // Window functions, FILTER clauses, and WITHIN GROUP all change the
        // semantics away from a simple whole-relation collapse.
        if func.over.is_some() || func.filter.is_some() || !func.within_group.is_empty() {
            return false;
        }
        // Parametric aggregates (`agg(...) (...)`) and named-args forms are
        // out of scope.
        if !matches!(func.parameters, FunctionArguments::None) {
            return false;
        }
    }
    true
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
            let args: Vec<&FunctionArg> = match &f.args {
                sqlparser::ast::FunctionArguments::List(l) => l.args.iter().collect(),
                _ => return None,
            };
            let arg_literal = |arg: &&FunctionArg| -> Option<String> {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                        extract_string_literal(inner)
                    }
                    _ => None,
                }
            };
            // Accept 1-arg (body) or 2-arg (config, body) forms.  The config
            // MUST be threaded through: canonicalisation is config-sensitive
            // (`simple` skips stemming/stopwords) and the probe lexemes must
            // be byte-identical to what the runtime tsquery UDF produces —
            // a config mismatch here would stem the probe differently from
            // the `@@` evaluation and prune files that hold real matches.
            // A non-literal config (parameter, column) → None → full scan.
            let (config, body_str) = match args.len() {
                1 => (None, arg_literal(args.first()?)?),
                2 => (Some(arg_literal(args.first()?)?), arg_literal(args.last()?)?),
                _ => return None,
            };
            // Delegate to the same `to_tsquery_text` canonicalisation the
            // FTS UDFs use (Snowball stemming for `to_tsquery`/`plainto`
            // under non-simple configs).  The structural probe in
            // `gin_tsvector` parses this canonical form.
            let canonical =
                crate::fts_udf::to_tsquery_text(&fn_name, config.as_deref(), &body_str)
                    .ok()?;
            Some(canonical)
        }
        // `'fox & dog'::tsquery` bare cast — canonicalise exactly like the
        // runtime `@@` evaluator does (parse as a raw tsquery, NO stemming —
        // PG does not stem direct casts either, and the row-level
        // re-evaluation parses the same raw text).
        Expr::Cast { expr: inner, .. } => extract_tsquery_from_expr(inner),
        // Bare string literal `'fox'` — the rewrite lowers `col @@ 'fox'` to
        // `tsvector_match_udf(col, 'fox')`, which parses the RHS with the
        // raw (unstemmed) tsquery grammar.  Canonicalise with that same
        // grammar so probe lexemes match the evaluator's terms.
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            crate::fts_udf::canonicalize_tsquery_text(s).ok()
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

// ── Trigram kNN — `ORDER BY <text_col> <-> 'needle' LIMIT k` ─────────────────
//
// Index-assisted nearest-neighbour over a TEXT column backed by the trigram GIN
// posting list. The order operator is `<->` (trigram distance, `1 - similarity`),
// the right-hand side is a single-quoted string literal, and the column carries a
// `gin_trgm_ops` GIN index in the catalog.
//
// DISAMBIGUATION: this detector runs AFTER the pgvector planner (RHS is a quoted
// `'…'` string, not a `'[…]'` vector literal — the vector planner's literal parse
// fails) and AFTER the spatial KNN detector (RHS here is a string literal, not an
// `ST_MakePoint(...)` call). The three `<->` paths are structurally exclusive at
// the RHS shape and confirmed exclusive at the column-type / index check below.
//
// The trigram postings only NARROW the candidate row set — the `<->` distance is
// always recomputed exactly on every materialised candidate (and on the
// distance-1 fill rows when there are fewer than `k` candidates), so the result
// is EXACT top-k, never an approximation.

/// Detected trigram-kNN plan for a `gin_trgm_ops`-indexed TEXT column.
///
/// Produced by [`detect_trgm_knn`] when the query matches
/// `SELECT <proj> FROM <table> ORDER BY <col> <-> '<needle>' LIMIT k` and
/// `<col>` carries a single-column `gin_trgm_ops` GIN index.
#[derive(Debug, Clone, PartialEq)]
pub struct TrgmKnnPlan {
    /// Projected output columns. `None` means `SELECT *`.
    pub projection: Option<Vec<String>>,
    /// The table to scan.
    pub table: TableName,
    /// The indexed TEXT column being ordered by trigram distance.
    pub col: String,
    /// The needle (RHS string literal of `<->`).
    pub needle: String,
    /// `LIMIT k` — number of nearest rows to return.
    pub k: usize,
}

/// Detect `SELECT … FROM t ORDER BY <col> <-> '<needle>' LIMIT k` on a
/// `gin_trgm_ops`-indexed TEXT column.
///
/// `raw_sql` MUST be the original SQL, before the `<->`→`(1.0 - similarity(...))`
/// rewrite (once the operator is lowered the structural signal is gone — same
/// invariant as the pgvector and spatial KNN planners).
///
/// Returns `None` (caller falls back to the standard scan + sort) on ANY
/// off-shape query: no `<->`, a non-string-literal RHS, a missing/compound
/// table, a non-`gin_trgm_ops` column, auxiliary clauses (`OFFSET`, `WHERE`,
/// `GROUP BY`, …), descending order, or a non-positive `LIMIT`.
pub async fn detect_trgm_knn(
    raw_sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<TrgmKnnPlan> {
    use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
    use sqlparser::ast::{
        BinaryOperator, Expr, GroupByExpr, SetExpr, Statement, TableFactor, Value, ValueWithSpan,
    };
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Fast pre-gate: must contain `<->`.
    if !raw_sql.contains("<->") {
        return None;
    }

    // sqlparser parses `a <-> b` as `BinaryOp { op: PGCustomBinaryOperator(["<->"]) }`
    // in the PG dialect, so unlike the spatial path no pre-rewrite is needed.
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, raw_sql).ok()?;
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

    // ORDER BY expr must be `<col> <-> '<needle>'` (or `'<needle>' <-> <col>` —
    // trigram distance is symmetric, so either operand may carry the literal).
    let (col, needle) = match &order_expr.expr {
        Expr::BinaryOp { left, op, right } => {
            // sqlparser renders the `<->` operator as `BinaryOperator::LtDashGt`.
            if !matches!(op, BinaryOperator::LtDashGt) {
                return None;
            }
            let col_of = |e: &Expr| extract_col_name(e);
            let lit_of = |e: &Expr| extract_string_literal(e);
            if let (Some(c), Some(n)) = (col_of(left), lit_of(right)) {
                (c, n)
            } else if let (Some(n), Some(c)) = (lit_of(left), col_of(right)) {
                (c, n)
            } else {
                return None;
            }
        }
        _ => return None,
    };

    // Projection: `*` or a list of bare identifiers.
    let projection = extract_simple_projection(&select.projection)?;

    // Confirm `col` carries a single-column `gin_trgm_ops` GIN index.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let _idx = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col
            && idx.opclass.as_deref() == Some("gin_trgm_ops")
    })?;
    // Confirm the column exists (a dropped/renamed column → fall back).
    meta.schema.field_with_name(&col).ok()?;

    Some(TrgmKnnPlan { projection, table, col, needle, k })
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sqlparser_parses_trgm_distance_as_lt_dash_gt() {
        // The trgm-kNN detector relies on sqlparser rendering `col <-> 'lit'`
        // as `BinaryOp { op: BinaryOperator::LtDashGt }`. Pin that here so a
        // sqlparser bump that changes the AST shape fails loudly.
        use crate::pg_ast::OrderByExt;
        use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement};
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT id FROM t ORDER BY name <-> 'alice' LIMIT 5";
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse");
        let q = match &stmts[0] {
            Statement::Query(q) => q,
            _ => panic!("not a query"),
        };
        let order = q.order_by.as_ref().expect("order by");
        let exprs = order.ext_exprs();
        match &exprs[0].expr {
            Expr::BinaryOp { op: BinaryOperator::LtDashGt, .. } => {}
            other => panic!("expected LtDashGt binop, got {other:?}"),
        }
        // Sanity: the SELECT body parsed too.
        assert!(matches!(q.body.as_ref(), SetExpr::Select(_)));
    }

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
    /// the entire column.
    ///
    /// We test via the internal TermPostingList directly to avoid needing to
    /// override the env-var budget (which is process-global).
    #[test]
    fn eviction_marks_only_affected_files_as_unindexed() {
        let mut pl = TermPostingList::new();
        let f1 = pl.intern_file("f1.parquet");
        let f2 = pl.intern_file("f2.parquet");

        // f1 carries term_a + term_b; f2 carries term_c + term_d. A generous
        // budget keeps every pair resident so no eviction fires here.
        let big = usize::MAX;
        assert!(pl.insert("term_a", &f1, big).0.is_empty());
        assert!(pl.insert("term_b", &f1, big).0.is_empty());
        assert!(pl.insert("term_c", &f2, big).0.is_empty());
        assert!(pl.insert("term_d", &f2, big).0.is_empty());
        assert_eq!(pl.total_count, 4);

        // Evict the oldest 25% (1 term = term_a, only in f1).
        let evicted_files = pl.evict_oldest();
        assert!(
            evicted_files.contains("f1.parquet"),
            "f1 should be evicted: {evicted_files:?}"
        );
        assert!(
            !evicted_files.contains("f2.parquet"),
            "f2 should NOT be evicted: {evicted_files:?}"
        );
        assert_eq!(pl.total_count, 3, "one posting pair dropped");
    }

    /// The budget counts DISTINCT (term, file) pairs: re-inserting the same
    /// pair (re-index, or another row of the same file with the same term)
    /// must not inflate total_count — the old accounting counted every insert
    /// and burned the budget ~rows× faster than actual memory growth.
    #[test]
    fn posting_accounting_dedupes_pairs() {
        let mut pl = TermPostingList::new();
        let big = usize::MAX;
        let f1 = pl.intern_file("f1.parquet");
        for _ in 0..1000 {
            pl.insert("key:tag", &f1, big);
            pl.insert("kv:tag=\"a\"", &f1, big);
        }
        assert_eq!(pl.total_count, 2, "1000 rows × 2 shared terms = 2 pairs");
        let f2 = pl.intern_file("f2.parquet");
        pl.insert("key:tag", &f2, big);
        assert_eq!(pl.total_count, 3);
    }

    /// remove_file keeps the pair accounting in sync.
    #[test]
    fn remove_file_decrements_pair_count() {
        let mut pl = TermPostingList::new();
        let big = usize::MAX;
        let f1 = pl.intern_file("f1.parquet");
        let f2 = pl.intern_file("f2.parquet");
        pl.insert("t1", &f1, big);
        pl.insert("t1", &f2, big);
        pl.insert("t2", &f1, big);
        assert_eq!(pl.total_count, 3);
        assert_eq!(pl.remove_file("f1.parquet"), 2, "two pairs referenced f1");
        assert_eq!(pl.total_count, 1);
        assert!(pl.probe_term("t1").is_some_and(|s| s.contains("f2.parquet")));
        assert!(pl.probe_term("t2").is_some_and(|s| s.is_empty()));
    }

    // ── Phase 5.19.F — per-file partial-coverage scan set ────────────────────

    /// With one file fully indexed and one file un-indexed, the scan set must
    /// (a) force the un-indexed file in, and (b) prune the indexed file when
    /// a needle term has no posting hit for it.
    #[test]
    fn scan_set_prunes_indexed_misses_and_forces_unindexed() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // f1: fully indexed, contains {"role":"admin"}.
        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"role":"admin"}"#, "f1.parquet", 0, 0,
        );
        registry.mark_file_indexed(&project, &table, "payload", "f1.parquet");
        // f2: fully indexed, contains {"role":"user"}.
        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"role":"user"}"#, "f2.parquet", 0, 0,
        );
        registry.mark_file_indexed(&project, &table, "payload", "f2.parquet");
        // f3: NOT marked indexed (e.g. written before the index existed).

        let live = vec![
            "f1.parquet".to_string(),
            "f2.parquet".to_string(),
            "f3.parquet".to_string(),
        ];
        let scan = registry.probe_containment_scan_set(
            &project, &table, "payload", "jsonb_ops", br#"{"role":"admin"}"#, &live,
        );
        match scan {
            GinScanSet::ScanFiles(files) => {
                assert!(files.contains(&"f1.parquet".to_string()), "hit kept: {files:?}");
                assert!(
                    !files.contains(&"f2.parquet".to_string()),
                    "indexed miss pruned: {files:?}"
                );
                assert!(
                    files.contains(&"f3.parquet".to_string()),
                    "un-indexed file forced: {files:?}"
                );
            }
            other => panic!("expected ScanFiles, got {other:?}"),
        }
    }

    /// A needle term that was never indexed prunes every fully-indexed file
    /// (the term provably does not occur in them) while still forcing
    /// un-indexed files into the scan set.
    #[test]
    fn scan_set_absent_term_prunes_only_indexed_files() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"role":"admin"}"#, "f1.parquet", 0, 0,
        );
        registry.mark_file_indexed(&project, &table, "payload", "f1.parquet");

        let live = vec!["f1.parquet".to_string(), "f2.parquet".to_string()];
        let scan = registry.probe_containment_scan_set(
            &project, &table, "payload", "jsonb_ops", br#"{"zzz":"nope"}"#, &live,
        );
        match scan {
            GinScanSet::ScanFiles(files) => {
                assert_eq!(
                    files,
                    vec!["f2.parquet".to_string()],
                    "indexed f1 pruned (term provably absent), un-indexed f2 forced"
                );
            }
            other => panic!("expected ScanFiles, got {other:?}"),
        }
    }

    /// After eviction un-marks a file, the scan set must treat it as a
    /// forced candidate even when the (stale) probe would have pruned it.
    #[test]
    fn scan_set_evicted_file_is_forced_candidate() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"role":"admin"}"#, "f1.parquet", 0, 0,
        );
        registry.mark_file_indexed(&project, &table, "payload", "f1.parquet");

        // Simulate eviction of f1's terms: evict_oldest via direct access,
        // then un-mark as index_row would.
        {
            let arc = registry.get(&project, &table, "payload").unwrap();
            let mut list = arc.lock().unwrap();
            // Evict everything (4 rounds of 25% on a 2-term list).
            let mut evicted: HashSet<String> = HashSet::new();
            while !list.insert_order.is_empty() {
                evicted.extend(list.evict_oldest());
            }
            assert!(evicted.contains("f1.parquet"));
            drop(list);
            let key = RegKey {
                project,
                table: table.clone(),
                col: "payload".to_string(),
            };
            let mut map = registry.indexed_files.lock().unwrap();
            map.get_mut(&key).unwrap().remove("f1.parquet");
        }

        let live = vec!["f1.parquet".to_string()];
        let scan = registry.probe_containment_scan_set(
            &project, &table, "payload", "jsonb_ops", br#"{"role":"admin"}"#, &live,
        );
        match scan {
            GinScanSet::ScanFiles(files) => {
                assert_eq!(files, live, "evicted file must be scanned");
            }
            other => panic!("expected ScanFiles, got {other:?}"),
        }
    }

    // ── needle_terms necessary-condition tests ───────────────────────────────

    /// Nested-object needle values must NOT emit an exact `kv:` term: the doc
    /// `{"a":{"x":1,"y":2}}` contains the needle `{"a":{"x":1}}` but carries
    /// `kv:a={"x":1,"y":2}` — an exact-value probe term would prune it.
    #[test]
    fn needle_terms_nested_object_skips_exact_kv() {
        let needle = json!({"a": {"x": 1}});
        let terms = needle_terms(&needle, "jsonb_ops");
        assert!(terms.contains(&"key:a".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"key:x".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"kv:x=1".to_string()), "terms={terms:?}");
        assert!(
            !terms.iter().any(|t| t.starts_with("kv:a=")),
            "exact kv for an object value is not a necessary condition: {terms:?}"
        );

        // Verify end-to-end: a superset doc stays a candidate.
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"a":{"x":1,"y":2}}"#, "f1.parquet", 0, 0,
        );
        let result = registry.probe_containment(
            &project, &table, "payload", "jsonb_ops", br#"{"a":{"x":1}}"#,
        );
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "superset doc kept: {files:?}");
            }
            other => panic!("superset doc must stay a candidate, got {other:?}"),
        }
    }

    /// Array needle values: only the key-presence term is necessary.
    #[test]
    fn needle_terms_array_value_key_presence_only() {
        let needle = json!({"tags": [1, 2]});
        let terms = needle_terms(&needle, "jsonb_ops");
        assert_eq!(terms, vec!["key:tags".to_string()], "terms={terms:?}");

        // jsonb_path_ops: no scalar leaf → no terms → probe falls back.
        let terms_po = needle_terms(&needle, "jsonb_path_ops");
        assert!(terms_po.is_empty(), "terms={terms_po:?}");
    }

    /// The eviction warning latch flips exactly once per posting list.
    #[test]
    fn eviction_warn_latch_set_once() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        registry.index_row(
            &project, &table, "payload", "jsonb_ops",
            br#"{"a":1}"#, "f1.parquet", 0, 0,
        );
        assert!(!registry.has_evicted(&project, &table, "payload"));

        let arc = registry.get(&project, &table, "payload").unwrap();
        {
            let mut list = arc.lock().unwrap();
            assert!(!list.eviction_warned);
            // Simulate what index_row does when eviction fires.
            list.eviction_warned = true;
        }
        assert!(registry.has_evicted(&project, &table, "payload"));
        // Latch stays set; a second "fire" does not reset it.
        {
            let mut list = arc.lock().unwrap();
            let already = list.eviction_warned;
            assert!(already, "latch must stay set");
            list.eviction_warned = true;
        }
        assert!(registry.has_evicted(&project, &table, "payload"));
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

    /// A replacement file with ZERO rows (empty batch set, or zero-row /
    /// all-NULL batches) must still be marked fully indexed: its empty
    /// posting set is exact, so the completeness guard stays valid and
    /// file-level pruning keeps working for the rest of the table. The old
    /// `indexed_any` flag left such files permanently un-indexed —
    /// a permanent completeness break after any CoW rewrite that emptied a
    /// file's JSONB column. Conversely, a batch with ROWS whose column is
    /// absent must withhold the mark (coverage cannot be claimed).
    #[test]
    fn rebuild_file_entries_zero_row_replacement_is_marked_indexed() {
        use arrow_array::{Int64Array, LargeBinaryArray, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Empty batch set — e.g. a CoW replacement whose every row was
        // deleted. Must be marked (empty posting set is sound).
        registry.rebuild_file_entries(
            &project, &table, "payload", "jsonb_ops", &[], "empty.parquet",
        );
        assert!(
            registry
                .indexed_files_for(&project, &table, "payload")
                .contains("empty.parquet"),
            "zero-batch replacement file must be in the completeness set"
        );

        // All-NULL JSONB column with rows — also exact (NULLs are never
        // indexed and never match containment); must be marked.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::LargeBinary,
            true,
        )]));
        let arr = LargeBinaryArray::from_opt_vec(vec![None, None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        registry.rebuild_file_entries(
            &project, &table, "payload", "jsonb_ops", &[batch], "nulls.parquet",
        );
        assert!(
            registry
                .indexed_files_for(&project, &table, "payload")
                .contains("nulls.parquet"),
            "all-NULL replacement file must be in the completeness set"
        );

        // Rows present but the JSONB column is MISSING from the batch —
        // coverage cannot be claimed; the mark must be withheld.
        let other_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let ids = Int64Array::from(vec![1i64, 2]);
        let bad = RecordBatch::try_new(other_schema, vec![Arc::new(ids)]).unwrap();
        registry.rebuild_file_entries(
            &project, &table, "payload", "jsonb_ops", &[bad], "uncovered.parquet",
        );
        assert!(
            !registry
                .indexed_files_for(&project, &table, "payload")
                .contains("uncovered.parquet"),
            "a file with rows whose column was not indexed must NOT be marked"
        );
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

    // ── Noisy-neighbour: per-project posting partition ──────────────────────

    /// A TermPostingList evicts exactly when its pair count crosses the budget
    /// passed in — the per-list mechanic the registry drives with a per-project
    /// fair share. With a tiny budget, the oldest 25% of terms are dropped and
    /// the affected files are reported.
    #[test]
    fn insert_evicts_at_supplied_budget_not_global() {
        let mut pl = TermPostingList::new();
        let f1 = pl.intern_file("f1.parquet");
        let budget = 3usize;
        // 3 distinct terms — at budget.
        assert!(pl.insert("t1", &f1, budget).0.is_empty());
        assert!(pl.insert("t2", &f1, budget).0.is_empty());
        assert!(pl.insert("t3", &f1, budget).0.is_empty());
        assert_eq!(pl.total_count, 3);
        // 4th pair crosses the budget → eviction fires, f1 reported.
        let (evicted, removed) = pl.insert("t4", &f1, budget);
        assert!(evicted.contains("f1.parquet"));
        assert!(removed >= 1, "at least one pair evicted");
        assert!(pl.total_count <= budget, "back under budget after evict");
    }

    /// `per_project_budget` is a fair share with a floor: it divides the global
    /// budget by the active-project count but never returns less than the floor.
    /// (Uses the live env-derived budget/floor; only the *shape* is asserted so
    /// the test is independent of any env override another test set.)
    #[test]
    fn per_project_budget_is_fair_share_with_floor() {
        let one = per_project_budget(1);
        let many = per_project_budget(1_000_000);
        assert!(one >= posting_floor());
        assert!(many >= posting_floor(), "floor is never violated");
        assert!(one >= many, "more projects → no larger a share each");
        // Zero projects is treated as one (avoid div-by-zero).
        assert_eq!(per_project_budget(0), per_project_budget(1));
    }

    /// The registry tracks per-project pair totals and the active-project count,
    /// so one project's churn is measured against ITS partition, not a sibling's.
    /// Indexing into project A then B must leave each project's count isolated,
    /// and removing A's file must not perturb B's count.
    #[test]
    fn registry_tracks_per_project_pairs_in_isolation() {
        let registry = GinIndexRegistry::new();
        let table = TableName::new("t").unwrap();
        let pa = ProjectId::new();
        let pb = ProjectId::new();
        let doc_a = serde_json::to_vec(&json!({"tag": "a", "id": 1})).unwrap();
        let doc_b = serde_json::to_vec(&json!({"tag": "b", "id": 2})).unwrap();

        registry.index_row(&pa, &table, "payload", "jsonb_ops", &doc_a, "a1.parquet", 0, 0);
        registry.index_row(&pb, &table, "payload", "jsonb_ops", &doc_b, "b1.parquet", 0, 0);

        let a_count = registry.project_pair_count(&pa);
        let b_count = registry.project_pair_count(&pb);
        assert!(a_count > 0, "project A holds postings");
        assert!(b_count > 0, "project B holds postings");
        assert_eq!(registry.active_project_count(), 2, "two active projects");

        // Removing A's only file frees A's partition and drops A out of the
        // active set; B is untouched.
        registry.remove_file(&pa, &table, "payload", "a1.parquet");
        assert_eq!(registry.project_pair_count(&pa), 0, "A drained");
        assert_eq!(
            registry.project_pair_count(&pb),
            b_count,
            "B's postings survive A's removal"
        );
        assert_eq!(registry.active_project_count(), 1, "only B remains active");
    }

    // ── Row-tier unit tests ──────────────────────────────────────────────────

    /// `intersect_sorted` is a sorted set intersection: ascending, deduped,
    /// keeping only elements present in BOTH inputs.
    #[test]
    fn intersect_sorted_keeps_common_offsets() {
        assert_eq!(intersect_sorted(&[1, 3, 5, 7], &[3, 4, 5, 9]), vec![3, 5]);
        assert_eq!(intersect_sorted(&[], &[1, 2]), Vec::<u32>::new());
        assert_eq!(intersect_sorted(&[1, 2, 3], &[1, 2, 3]), vec![1, 2, 3]);
        assert_eq!(intersect_sorted(&[1, 2], &[3, 4]), Vec::<u32>::new());
    }

    /// A selective term (few matching rows in a large file) is kept as a row
    /// block; intersecting two selective needle terms narrows to their common
    /// rows. The block is a SUPERSET — never drops an offset present for both.
    #[test]
    fn row_tier_keeps_selective_term_and_intersects() {
        let mut pl = TermPostingList::new();
        let f = pl.intern_file("f.parquet");
        // Simulate a 100-row file: term "a" at rows {2,40}, "b" at {2,77}.
        // Density cap 60% of 100 = 60; both terms are well under.
        pl.note_indexed_row(&f); // bump building_rows to a representative count
        for _ in 0..99 {
            pl.note_indexed_row(&f);
        }
        let big = 1_000_000usize;
        pl.insert_with_row("a", &f, Some(2), big);
        pl.insert_with_row("a", &f, Some(40), big);
        pl.insert_with_row("b", &f, Some(2), big);
        pl.insert_with_row("b", &f, Some(77), big);
        let added = pl.seal_file_row_tier(&f, 60);
        assert!(added > 0, "selective blocks kept");
        assert!(pl.file_has_row_tier("f.parquet"));
        // AND-probe {a,b}: only row 2 is common.
        match pl.probe_row_offsets(&["a".to_string(), "b".to_string()], "f.parquet") {
            RowProbe::Rows(v) => assert_eq!(v, vec![2]),
            other => panic!("expected Rows([2]), got {other:?}"),
        }
        // Probe a term that never occurred → file provably prunable.
        match pl.probe_row_offsets(&["zzz".to_string()], "f.parquet") {
            RowProbe::Absent => {}
            other => panic!("expected Absent, got {other:?}"),
        }
    }

    /// The density cap drops a near-universal term: a needle that includes a
    /// dense term forces full decode (`RowProbe::Full`) — the inversion that
    /// keeps the row tier from bloating on the bench's everywhere-dense needle.
    #[test]
    fn row_tier_density_cap_drops_dense_term_to_full_decode() {
        let mut pl = TermPostingList::new();
        let f = pl.intern_file("f.parquet");
        // 10-row file. "dense" occurs in 9/10 rows (90% > 60% cap → dropped).
        // "rare" occurs in 1/10 rows (kept).
        for _ in 0..10 {
            pl.note_indexed_row(&f);
        }
        let big = 1_000_000usize;
        for r in 0..9u64 {
            pl.insert_with_row("dense", &f, Some(r), big);
        }
        pl.insert_with_row("rare", &f, Some(5), big);
        pl.seal_file_row_tier(&f, 60);
        assert!(pl.file_has_row_tier("f.parquet"));
        // A needle whose ONLY term is dense cannot row-narrow → Full.
        match pl.probe_row_offsets(&["dense".to_string()], "f.parquet") {
            RowProbe::Full => {}
            other => panic!("expected Full (only dense), got {other:?}"),
        }
        // The rare term alone still narrows.
        match pl.probe_row_offsets(&["rare".to_string()], "f.parquet") {
            RowProbe::Rows(v) => assert_eq!(v, vec![5]),
            other => panic!("expected Rows([5]), got {other:?}"),
        }
        // A needle combining dense+rare: the dense term is SKIPPED (no
        // constraint) and the selective `rare` term still narrows decode to its
        // rows — the key win for the bench's everywhere-dense needle term.
        match pl.probe_row_offsets(&["rare".to_string(), "dense".to_string()], "f.parquet") {
            RowProbe::Rows(v) => assert_eq!(v, vec![5]),
            other => panic!("expected Rows([5]) (dense skipped), got {other:?}"),
        }
    }

    /// Row-tier budget eviction frees the OLDEST blocks first and never touches
    /// the coarse tier. After eviction below budget, the evicted file falls
    /// back to coarse decode (no row tier) — never a false negative.
    #[test]
    fn row_tier_eviction_frees_oldest_and_preserves_coarse() {
        let mut pl = TermPostingList::new();
        let f1 = pl.intern_file("f1.parquet");
        let big = 1_000_000usize;
        // Two files, each with one selective term, each costing 1 budget unit.
        for _ in 0..100 {
            pl.note_indexed_row(&f1);
        }
        pl.insert_with_row("t1", &f1, Some(3), big);
        pl.seal_file_row_tier(&f1, 60);
        let f2 = pl.intern_file("f2.parquet");
        for _ in 0..100 {
            pl.note_indexed_row(&f2);
        }
        pl.insert_with_row("t2", &f2, Some(7), big);
        pl.seal_file_row_tier(&f2, 60);
        assert_eq!(pl.row_tier_cost, 2);
        // Coarse tier still has both files' terms.
        assert!(pl.probe_term("t1").is_some());
        assert!(pl.probe_term("t2").is_some());
        // Evict to budget 1 → the oldest (f1/t1) block goes.
        let freed = pl.evict_row_tier_to(1);
        assert!(freed >= 1);
        assert!(pl.row_tier_cost <= 1);
        assert!(!pl.file_has_row_tier("f1.parquet"), "f1 row tier evicted");
        assert!(pl.file_has_row_tier("f2.parquet"), "f2 survives (newer)");
        // Coarse tier untouched — both files still prunable at file granularity.
        assert!(pl.probe_term("t1").is_some(), "coarse tier preserved");
        assert!(pl.probe_term("t2").is_some());
    }

    /// `remove_file` drops the file's row blocks and frees their budget, so a
    /// compacted/CoW-replaced file leaves no stale offsets behind.
    #[test]
    fn remove_file_drops_row_tier_blocks() {
        let mut pl = TermPostingList::new();
        let f = pl.intern_file("f.parquet");
        let big = 1_000_000usize;
        for _ in 0..50 {
            pl.note_indexed_row(&f);
        }
        pl.insert_with_row("t", &f, Some(1), big);
        pl.seal_file_row_tier(&f, 60);
        assert!(pl.row_tier_cost > 0);
        pl.remove_file("f.parquet");
        assert_eq!(pl.row_tier_cost, 0);
        assert!(!pl.file_has_row_tier("f.parquet"));
        assert!(pl.row_entries.is_empty());
    }

    /// End-to-end through the registry: a selective `@>` needle yields a row
    /// selection that is a superset of the true match; a dense term yields full
    /// decode for its file; an absent term prunes the file.
    #[test]
    fn registry_row_selection_superset_and_prune() {
        let registry = GinIndexRegistry::new();
        let table = TableName::new("t").unwrap();
        let p = ProjectId::new();
        let opclass = "jsonb_ops";
        let file = "projects/x/data/f.parquet"; // path string is opaque here
        // 4 rows. row0 {tag:"x"}, row1 {tag:"y"}, row2 {tag:"x",extra:1},
        // row3 {tag:"y"}.  Needle {tag:"x"} → rows {0,2}.
        let docs = [
            serde_json::json!({"tag":"x"}),
            serde_json::json!({"tag":"y"}),
            serde_json::json!({"tag":"x","extra":1}),
            serde_json::json!({"tag":"y"}),
        ];
        for (r, d) in docs.iter().enumerate() {
            let bytes = serde_json::to_vec(d).unwrap();
            registry.index_row(&p, &table, "payload", opclass, &bytes, file, 0, r as u64);
        }
        registry.mark_file_indexed(&p, &table, "payload", file);

        let needle = serde_json::to_vec(&serde_json::json!({"tag":"x"})).unwrap();
        let plan = registry.probe_row_selection(
            &p, &table, "payload", opclass, &needle, &[file.to_string()],
        );
        // tag=x is selective (2/4 = 50% <= 60% cap) → row offsets kept.
        let offs = plan.row_offsets.get(file).expect("row offsets present");
        assert!(offs.contains(&0) && offs.contains(&2), "superset of true matches {offs:?}");
        assert!(!offs.contains(&1) && !offs.contains(&3), "non-matching rows excluded");
        assert!(plan.prunable.is_empty());

        // A needle naming a key that never appears prunes the file.
        let needle2 = serde_json::to_vec(&serde_json::json!({"absent":true})).unwrap();
        let plan2 = registry.probe_row_selection(
            &p, &table, "payload", opclass, &needle2, &[file.to_string()],
        );
        assert!(plan2.prunable.contains(file), "absent term prunes file");
    }
}
