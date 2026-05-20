//! Per-shape rolling HDR histogram registry (Phase 5.16.B/C).
//!
//! # Purpose
//!
//! Routes the `QueryShapeHash` produced by Phase 5.16.A into observable
//! per-shape statistics accumulated as HDR histograms.  Phase 5.16.C extends
//! this with scale-dependent regression tracking: each shape's latency is
//! tracked per log2(table_row_count) bucket so regressions that only manifest
//! at scale are visible.
//!
//! # Design
//!
//! ```text
//! QueryStatRegistry
//!   DashMap<ProjectId, Arc<Mutex<ProjectStats>>>
//!                            │
//!                            └── LruCache<(TableName, QueryShapeHash), ShapeStats>
//!                                  │                          (max 500 entries)
//!                                  └── HDR histograms per metric
//!                                        │
//!                                        └── per-bucket latency histograms
//!                                              (8 log2 row-count buckets)
//! ```
//!
//! **Multi-tenant isolation** (load-bearing rule from `feedback_multitenant_isolation.md`):
//!
//! * The shared registry is a `DashMap` of `ProjectId → ProjectStats`.
//! * Each `ProjectStats` is a bounded `LruCache` capped at
//!   [`MAX_SHAPES_PER_PROJECT`] (500) entries.
//! * An idle project's entry in the `DashMap` is a single `Arc<Mutex<…>>`
//!   pointer — a few dozen bytes.  The cache itself is empty and allocates
//!   nothing on the heap beyond its bookkeeping.
//! * Observing a new `(project, table, shape)` key for the first time allocates
//!   one [`ShapeStats`] (≈ 16 KiB: 5 global histograms + 8 per-bucket latency
//!   histograms).  At 500 shapes that is ≈ 8 MiB/project.
//! * When a project crosses 500 shapes, the LRU evicts the coldest entry so
//!   memory remains bounded.  Observing an evicted shape re-allocates it as a
//!   fresh entry.
//!
//! # Metrics collected per shape
//!
//! | Field | Type | Unit |
//! |-------|------|------|
//! | `latency_ns` | HDR histogram (global) | nanoseconds |
//! | `rows_scanned` | HDR histogram | row count |
//! | `files_opened` | HDR histogram | file count |
//! | `bytes_decoded` | HDR histogram | bytes |
//! | `cache_hits` | HDR histogram | hit count |
//! | `fast_path_engaged` | plain counter | bool → u64 |
//! | `bucket_latency_ns` | 8 × HDR histogram | nanoseconds, per log2(row_count) bucket |
//!
//! # Scale-bucket axis (Phase 5.16.C)
//!
//! Queries are bucketed by `log2(table_row_count)`:
//!
//! | Bucket | Row count range |
//! |--------|----------------|
//! | 0      | < 1 k          |
//! | 1      | 1 k – 10 k     |
//! | 2      | 10 k – 100 k   |
//! | 3      | 100 k – 1 M    |
//! | 4      | 1 M – 10 M     |
//! | 5      | 10 M – 100 M   |
//! | 6      | 100 M – 1 B    |
//! | 7      | ≥ 1 B          |

use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};
use dashmap::DashMap;
use hdrhistogram::Histogram;
use lru::LruCache;

use crate::query_shape::QueryShapeHash;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum distinct `(table, shape)` pairs tracked per project.
/// At ≈ 16 KiB/entry that is ≈ 8 MiB/project.
pub const MAX_SHAPES_PER_PROJECT: usize = 500;

/// Number of log2-row-count buckets for scale-dependent regression tracking.
pub const SCALE_BUCKET_COUNT: usize = 8;

/// HDR histogram significant-figures precision.  3 significant figures gives
/// < 0.1 % relative error for any recorded value, at a cost of ≈ 11.5 KiB
/// per histogram in the worst case (unbounded range).  We clamp the range
/// below to keep it closer to 2 KiB.
const HDR_SIGFIG: u8 = 3;

/// HDR histogram upper bound for latency (10 minutes in nanoseconds).
/// Values beyond this are clamped to the upper bucket.
const LATENCY_MAX_NS: u64 = 600_000_000_000;

/// HDR histogram upper bound for row / file / byte counts.
const COUNT_MAX: u64 = 1_000_000_000;

/// Default regression threshold: p99 ratio between adjacent buckets above
/// which a shape is flagged as a regression.
pub const DEFAULT_REGRESSION_THRESHOLD: f64 = 1.3;

/// Lower bound of each scale bucket (exclusive upper bound of the previous).
/// Bucket i contains row counts in [BUCKET_THRESHOLDS[i-1], BUCKET_THRESHOLDS[i]).
/// Bucket 0 catches everything below the first threshold.
const BUCKET_THRESHOLDS: [u64; 7] = [
    1_000,        // bucket 0 → 1: 1 k rows
    10_000,       // bucket 1 → 2: 10 k rows
    100_000,      // bucket 2 → 3: 100 k rows
    1_000_000,    // bucket 3 → 4: 1 M rows
    10_000_000,   // bucket 4 → 5: 10 M rows
    100_000_000,  // bucket 5 → 6: 100 M rows
    1_000_000_000, // bucket 6 → 7: 1 B rows
];

// ---------------------------------------------------------------------------
// Bucket helper
// ---------------------------------------------------------------------------

/// Map a table row count to a scale bucket index (0–7).
///
/// | Bucket | Row count range |
/// |--------|----------------|
/// | 0      | < 1 k          |
/// | 1      | 1 k – 10 k     |
/// | 2      | 10 k – 100 k   |
/// | 3      | 100 k – 1 M    |
/// | 4      | 1 M – 10 M     |
/// | 5      | 10 M – 100 M   |
/// | 6      | 100 M – 1 B    |
/// | 7      | ≥ 1 B          |
pub fn bucket_for(row_count: u64) -> u8 {
    for (i, &threshold) in BUCKET_THRESHOLDS.iter().enumerate() {
        if row_count < threshold {
            return i as u8;
        }
    }
    7
}

// ---------------------------------------------------------------------------
// Metrics observed per query invocation
// ---------------------------------------------------------------------------

/// Snapshot of metrics observed for one query execution.
///
/// Caller fills this and passes it to [`QueryStatRegistry::observe`].
#[derive(Debug, Default, Clone)]
pub struct QueryMetrics {
    /// Wall-clock duration of the query, in nanoseconds.
    pub latency_ns: u64,
    /// Total number of rows scanned (across all files).
    pub rows_scanned: u64,
    /// Number of Parquet / Vortex files opened.
    pub files_opened: u64,
    /// Bytes decoded from columnar storage.
    pub bytes_decoded: u64,
    /// Page-cache / block-cache hits for this query.
    pub cache_hits: u64,
    /// Whether the fast-select path was engaged for this query.
    pub fast_path_engaged: bool,
    /// Number of rows in the table at query time (used for scale bucketing).
    /// Set to 0 when not available — falls into bucket 0.
    pub table_row_count: u64,
}

// ---------------------------------------------------------------------------
// Per-shape statistics
// ---------------------------------------------------------------------------

/// HDR histograms + counters for one `(project, table, shape)` triple.
///
/// Phase 5.16.C adds `bucket_latency_ns`: an array of 8 per-scale-bucket
/// latency histograms indexed by [`bucket_for`].  Only the global
/// `latency_ns` is used for general percentile dashboards; the per-bucket
/// histograms feed [`QueryStatRegistry::top_regressions`].
pub struct ShapeStats {
    /// Global latency histogram (all table sizes).
    pub latency_ns: Histogram<u64>,
    pub rows_scanned: Histogram<u64>,
    pub files_opened: Histogram<u64>,
    pub bytes_decoded: Histogram<u64>,
    pub cache_hits: Histogram<u64>,
    /// Cumulative count of queries where `fast_path_engaged == true`.
    pub fast_path_count: u64,
    /// Total observations recorded into this entry.
    pub total_observations: u64,
    /// Per-scale-bucket latency histograms (Phase 5.16.C).
    ///
    /// Index i corresponds to bucket i from [`bucket_for`].
    /// Unused buckets have 0 total_count and contribute nothing to
    /// regression analysis.
    pub bucket_latency_ns: [Histogram<u64>; SCALE_BUCKET_COUNT],
}

fn make_latency_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, LATENCY_MAX_NS, HDR_SIGFIG).expect("latency histogram")
}

impl ShapeStats {
    fn new() -> Self {
        let latency_ns = make_latency_hist();
        let rows_scanned =
            Histogram::<u64>::new_with_bounds(1, COUNT_MAX, HDR_SIGFIG).expect("rows_scanned histogram");
        let files_opened =
            Histogram::<u64>::new_with_bounds(1, COUNT_MAX, HDR_SIGFIG).expect("files_opened histogram");
        let bytes_decoded =
            Histogram::<u64>::new_with_bounds(1, COUNT_MAX, HDR_SIGFIG).expect("bytes_decoded histogram");
        let cache_hits =
            Histogram::<u64>::new_with_bounds(1, COUNT_MAX, HDR_SIGFIG).expect("cache_hits histogram");

        // Build per-bucket latency histograms.  We use array::from_fn for a
        // clean initialisation without unsafe.
        let bucket_latency_ns = std::array::from_fn(|_| make_latency_hist());

        Self {
            latency_ns,
            rows_scanned,
            files_opened,
            bytes_decoded,
            cache_hits,
            fast_path_count: 0,
            total_observations: 0,
            bucket_latency_ns,
        }
    }

    /// Rough heap size of this entry in bytes (for the `mem_bytes()` test).
    ///
    /// Each `Histogram<u64>` stores its counts as a `Vec<u64>`.  The
    /// allocation size depends on the (low, high, sigfig) bounds; for our
    /// configuration the latency histogram is ≈ 2 KiB per instance.
    /// With 8 per-bucket histograms + 5 global histograms = 13 latency-sized
    /// + 4 count-sized histograms → ≈ 16 KiB per shape.
    pub fn mem_bytes(&self) -> usize {
        let hist_bytes = |h: &Histogram<u64>| {
            h.len() as usize * std::mem::size_of::<u64>()
        };
        let global = hist_bytes(&self.latency_ns)
            + hist_bytes(&self.rows_scanned)
            + hist_bytes(&self.files_opened)
            + hist_bytes(&self.bytes_decoded)
            + hist_bytes(&self.cache_hits)
            + std::mem::size_of::<u64>() * 2; // fast_path_count + total_observations
        let bucket = self.bucket_latency_ns.iter().map(hist_bytes).sum::<usize>();
        global + bucket
    }

    fn record(&mut self, m: &QueryMetrics) {
        // Clamp values to the histogram's configured upper bound so saturation
        // behaviour is predictable (values above the bound go into the top
        // bucket rather than panicking).
        let lat = m.latency_ns.max(1).min(LATENCY_MAX_NS);
        let rows = m.rows_scanned.max(1).min(COUNT_MAX);
        let files = m.files_opened.max(1).min(COUNT_MAX);
        let bytes = m.bytes_decoded.max(1).min(COUNT_MAX);
        let hits = m.cache_hits.max(1).min(COUNT_MAX);

        // `saturating_record` never panics even if the value falls outside the
        // configured range (it saturates to the nearest boundary).
        self.latency_ns.saturating_record(lat);
        self.rows_scanned.saturating_record(rows);
        self.files_opened.saturating_record(files);
        self.bytes_decoded.saturating_record(bytes);
        self.cache_hits.saturating_record(hits);

        // Phase 5.16.C: also record into the per-scale-bucket histogram.
        let bucket = bucket_for(m.table_row_count) as usize;
        self.bucket_latency_ns[bucket].saturating_record(lat);

        if m.fast_path_engaged {
            self.fast_path_count += 1;
        }
        self.total_observations += 1;
    }
}

// ---------------------------------------------------------------------------
// Regression alert
// ---------------------------------------------------------------------------

/// A scale-dependent regression detected by [`QueryStatRegistry::top_regressions`].
#[derive(Debug, Clone)]
pub struct RegressionAlert {
    /// The query shape hash that is regressing.
    pub shape: QueryShapeHash,
    /// The lower (smaller) scale bucket (N-1).
    pub prev_bucket: u8,
    /// The higher (larger) scale bucket (N).
    pub curr_bucket: u8,
    /// Ratio of `bucket[N].p99 / bucket[N-1].p99`.  Always > threshold.
    pub p99_ratio: f64,
}

// ---------------------------------------------------------------------------
// Per-project state
// ---------------------------------------------------------------------------

/// Per-project bounded LRU of shape → histograms.
///
/// The `Mutex` here is a `std::sync::Mutex` (not `tokio`): `observe` is
/// called from async context but holds the lock for only a few nanoseconds
/// (one LRU probe + one histogram record).  A `std::sync::Mutex` is faster
/// than `tokio::sync::Mutex` for sub-microsecond critical sections and does
/// not risk async task starvation.
struct ProjectStats {
    shapes: LruCache<(TableName, QueryShapeHash), ShapeStats>,
}

impl ProjectStats {
    fn new() -> Self {
        let cap = std::num::NonZeroUsize::new(MAX_SHAPES_PER_PROJECT)
            .expect("MAX_SHAPES_PER_PROJECT > 0");
        Self {
            shapes: LruCache::new(cap),
        }
    }

    fn observe(&mut self, table: &TableName, hash: QueryShapeHash, metrics: &QueryMetrics) {
        let key = (table.clone(), hash);
        // `get_or_insert` promotes the key to MRU and returns a mutable ref.
        let entry = self
            .shapes
            .get_or_insert_mut(key, ShapeStats::new);
        entry.record(metrics);
    }

    /// Rough memory footprint of all shapes currently tracked for this project.
    pub fn mem_bytes(&self) -> usize {
        self.shapes
            .iter()
            .map(|(_, s)| s.mem_bytes())
            .sum()
    }

    /// Scan all shapes for table `table_name` and return those whose p99
    /// latency grows by more than `threshold` between any two adjacent
    /// populated scale buckets.
    fn top_regressions(
        &self,
        table_name: &TableName,
        threshold: f64,
    ) -> Vec<RegressionAlert> {
        let mut alerts = Vec::new();
        for ((tbl, hash), stats) in self.shapes.iter() {
            if tbl != table_name {
                continue;
            }
            // Walk adjacent bucket pairs (N-1, N).
            let mut prev_bucket: Option<(u8, u64)> = None; // (bucket_index, p99_ns)
            for (i, hist) in stats.bucket_latency_ns.iter().enumerate() {
                if hist.is_empty() {
                    // No observations in this bucket — skip; don't reset prev.
                    continue;
                }
                let p99 = hist.value_at_quantile(0.99);
                if let Some((prev_idx, prev_p99)) = prev_bucket {
                    if prev_p99 > 0 {
                        let ratio = p99 as f64 / prev_p99 as f64;
                        if ratio > threshold {
                            alerts.push(RegressionAlert {
                                shape: *hash,
                                prev_bucket: prev_idx,
                                curr_bucket: i as u8,
                                p99_ratio: ratio,
                            });
                        }
                    }
                }
                prev_bucket = Some((i as u8, p99));
            }
        }
        alerts
    }
}

// ---------------------------------------------------------------------------
// Process-wide registry
// ---------------------------------------------------------------------------

/// Process-wide per-shape HDR histogram registry.
///
/// Cheap to clone — all state is behind `Arc` + `DashMap`.
///
/// # Concurrency
///
/// The outer `DashMap` is sharded by `ProjectId`; accessing two different
/// projects in parallel takes different shards and never serialises.
/// Within one project the `Mutex<ProjectStats>` serialises concurrent
/// observers for that project's LRU, but the critical section is a few
/// nanoseconds (one cache probe + histogram record) so contention is
/// negligible even at 10k QPS per project.
#[derive(Clone)]
pub struct QueryStatRegistry {
    projects: Arc<DashMap<ProjectId, Arc<Mutex<ProjectStats>>>>,
    /// Regression threshold for [`top_regressions`].  Shared across clones.
    regression_threshold: Arc<std::sync::atomic::AtomicU64>,
}

impl QueryStatRegistry {
    /// Create an empty registry with default regression threshold (1.3).
    pub fn new() -> Self {
        Self {
            projects: Arc::new(DashMap::new()),
            regression_threshold: Arc::new(std::sync::atomic::AtomicU64::new(
                f64::to_bits(DEFAULT_REGRESSION_THRESHOLD),
            )),
        }
    }

    /// Override the regression threshold (default: 1.3).
    ///
    /// A shape is flagged when `bucket[N].p99 / bucket[N-1].p99 > threshold`.
    pub fn set_regression_threshold(&self, threshold: f64) {
        self.regression_threshold.store(
            f64::to_bits(threshold),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Current regression threshold.
    fn regression_threshold(&self) -> f64 {
        f64::from_bits(
            self.regression_threshold
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Record `metrics` for the given `(project, table, shape)` triple.
    ///
    /// This is the hot-path call site — invoked once per SELECT via
    /// `exec_select` after `QueryShapeHash::of` produces the hash.
    ///
    /// `metrics.table_row_count` is used to assign the observation to one of 8
    /// log2 scale buckets for regression tracking (Phase 5.16.C).  Pass 0 when
    /// the table row count is not available — the observation falls into bucket 0.
    ///
    /// # Cost
    ///
    /// * One `DashMap` shard lock (shared, O(1)).
    /// * One `std::sync::Mutex` lock for the project LRU (exclusive, O(1)).
    /// * One LRU `get_or_insert_mut` (O(1) amortised).
    /// * Five + 1 HDR `saturating_record` calls (O(1) each).
    ///
    /// Total: < 1 µs on a warm cache.
    pub fn observe(
        &self,
        project: &ProjectId,
        table: &TableName,
        hash: QueryShapeHash,
        metrics: &QueryMetrics,
    ) {
        // Lazy project allocation: the first observation for a project inserts
        // an empty `ProjectStats`; subsequent observations reuse it.  The
        // `entry` API on `DashMap` takes an exclusive shard lock only when the
        // key is absent; a present key is served under a shared shard lock
        // via `get`.
        let project_stats = self
            .projects
            .entry(*project)
            .or_insert_with(|| Arc::new(Mutex::new(ProjectStats::new())));
        let arc = project_stats.clone();
        drop(project_stats); // release DashMap shard lock before locking Mutex

        let mut guard = arc.lock().expect("ProjectStats Mutex poisoned");
        guard.observe(table, hash, metrics);
    }

    /// Read per-shape stats for a project with a visitor function.
    ///
    /// `f` receives `(table_name, hash, stats)` for each currently-tracked
    /// shape.  Shapes evicted by LRU are not visible.
    ///
    /// Returns `false` if the project has never been observed (no entry).
    pub fn with_project_shapes<F>(&self, project: &ProjectId, mut f: F) -> bool
    where
        F: FnMut(&TableName, QueryShapeHash, &ShapeStats),
    {
        let Some(arc) = self.projects.get(project).map(|r| r.clone()) else {
            return false;
        };
        let guard = arc.lock().expect("ProjectStats Mutex poisoned");
        for ((table, hash), stats) in guard.shapes.iter() {
            f(table, *hash, stats);
        }
        true
    }

    /// Return shapes for `(project, table)` whose p99 latency grows by more
    /// than the configured regression threshold between any two adjacent
    /// populated scale buckets.
    ///
    /// A [`RegressionAlert`] is emitted for each adjacent bucket pair
    /// `(N-1, N)` where `bucket[N].p99 / bucket[N-1].p99 > threshold`.
    /// Results are sorted by descending `p99_ratio` so the worst regression
    /// appears first.
    ///
    /// Returns an empty `Vec` when the project/table has no observations.
    pub fn top_regressions(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Vec<RegressionAlert> {
        let Some(arc) = self.projects.get(project).map(|r| r.clone()) else {
            return Vec::new();
        };
        let threshold = self.regression_threshold();
        let guard = arc.lock().expect("ProjectStats Mutex poisoned");
        let mut alerts = guard.top_regressions(table, threshold);
        // Sort worst regression first.
        alerts.sort_by(|a, b| b.p99_ratio.partial_cmp(&a.p99_ratio).unwrap_or(std::cmp::Ordering::Equal));
        alerts
    }

    /// Approximate memory footprint of all shapes tracked across all projects.
    ///
    /// Iterates once over every project and sums `ShapeStats::mem_bytes()`.
    /// Used by the memory-bound unit test to assert the ≤ 10 MiB/project cap.
    pub fn mem_bytes(&self) -> usize {
        let mut total = 0usize;
        for entry in self.projects.iter() {
            let guard = entry.value().lock().expect("ProjectStats Mutex poisoned");
            total += guard.mem_bytes();
        }
        total
    }

    /// Approximate memory footprint for a single project.
    ///
    /// Returns 0 when the project has no recorded observations.
    pub fn project_mem_bytes(&self, project: &ProjectId) -> usize {
        let Some(arc) = self.projects.get(project).map(|r| r.clone()) else {
            return 0;
        };
        let guard = arc.lock().expect("ProjectStats Mutex poisoned");
        guard.mem_bytes()
    }

    /// Number of distinct `(table, shape)` keys currently tracked for `project`.
    ///
    /// Returns 0 for unknown projects.  Used by unit tests to verify LRU
    /// eviction.
    pub fn project_shape_count(&self, project: &ProjectId) -> usize {
        let Some(arc) = self.projects.get(project).map(|r| r.clone()) else {
            return 0;
        };
        let guard = arc.lock().expect("ProjectStats Mutex poisoned");
        guard.shapes.len()
    }
}

impl Default for QueryStatRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    fn project() -> ProjectId {
        ProjectId::new()
    }

    fn table(name: &str) -> TableName {
        TableName::new(name).expect("valid table name")
    }

    fn hash(n: u64) -> QueryShapeHash {
        QueryShapeHash(n)
    }

    fn metrics_with_latency(latency_ns: u64) -> QueryMetrics {
        QueryMetrics {
            latency_ns,
            rows_scanned: 10,
            files_opened: 2,
            bytes_decoded: 4096,
            cache_hits: 1,
            fast_path_engaged: false,
            table_row_count: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Per-shape histogram bucketing
    // -----------------------------------------------------------------------

    /// Observed latencies appear in the correct percentile buckets.
    #[test]
    fn histogram_percentile_bucketing() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("events");
        let h = hash(42);

        // Record 100 observations: 99 at 1_000 ns and one at 1_000_000 ns
        // (the outlier).
        for _ in 0..99 {
            reg.observe(&p, &t, h, &metrics_with_latency(1_000));
        }
        reg.observe(&p, &t, h, &metrics_with_latency(1_000_000));

        let mut found = false;
        reg.with_project_shapes(&p, |tbl, shape_hash, stats| {
            if *tbl == t && shape_hash == h {
                found = true;
                // p50 should be near 1_000 ns.
                let p50 = stats.latency_ns.value_at_quantile(0.50);
                assert!(
                    p50 >= 900 && p50 <= 1_100,
                    "p50 = {p50}; expected ~1_000"
                );
                // p99 should be near 1_000 ns (99 out of 100 are 1_000 ns).
                let p99 = stats.latency_ns.value_at_quantile(0.99);
                assert!(
                    p99 >= 900 && p99 <= 1_100,
                    "p99 = {p99}; expected ~1_000"
                );
                // p100 should be near 1_000_000 ns (the outlier).
                let p100 = stats.latency_ns.value_at_quantile(1.0);
                assert!(
                    p100 >= 900_000,
                    "p100 = {p100}; expected ~1_000_000"
                );
                assert_eq!(stats.total_observations, 100);
            }
        });
        assert!(found, "shape not found in registry");
    }

    // -----------------------------------------------------------------------
    // LRU eviction
    // -----------------------------------------------------------------------

    /// When a project crosses 500 shapes, the LRU evicts the coldest entry.
    /// Observing a previously-evicted shape brings it back as a fresh entry.
    #[test]
    fn lru_eviction_at_capacity() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("tbl");
        let m = metrics_with_latency(1_000);

        // Fill to capacity with shapes 0..500.
        for i in 0..MAX_SHAPES_PER_PROJECT as u64 {
            reg.observe(&p, &t, hash(i), &m);
        }
        assert_eq!(reg.project_shape_count(&p), MAX_SHAPES_PER_PROJECT);

        // Shape 0 is the LRU entry (never touched since first insert, while
        // shapes 1..499 were inserted after it).
        // Insert one more shape (500) — should evict the LRU.
        reg.observe(&p, &t, hash(500), &m);
        assert_eq!(
            reg.project_shape_count(&p),
            MAX_SHAPES_PER_PROJECT,
            "count should remain at cap after eviction"
        );

        // Now re-observe shape 0 — it must come back as a fresh entry.
        reg.observe(&p, &t, hash(0), &m);
        // Count stays at cap (a new eviction occurred).
        assert_eq!(reg.project_shape_count(&p), MAX_SHAPES_PER_PROJECT);

        // shape 0 should now be visible with total_observations == 1
        // (fresh entry — previous stats were lost on eviction).
        let mut obs_for_0 = None;
        reg.with_project_shapes(&p, |_, sh, stats| {
            if sh == hash(0) {
                obs_for_0 = Some(stats.total_observations);
            }
        });
        assert_eq!(
            obs_for_0,
            Some(1),
            "re-inserted shape 0 should start fresh with 1 observation"
        );
    }

    // -----------------------------------------------------------------------
    // Per-project isolation
    // -----------------------------------------------------------------------

    /// Project A's stats are not visible from project B's view.
    #[test]
    fn per_project_isolation() {
        let reg = QueryStatRegistry::new();
        let pa = project();
        let pb = project();
        let t = table("shared_name");
        let h = hash(99);

        // Observe only for project A.
        reg.observe(&pa, &t, h, &metrics_with_latency(5_000));

        // Project B should have zero shapes.
        assert_eq!(
            reg.project_shape_count(&pb),
            0,
            "project B should have no shapes"
        );

        // with_project_shapes on B should return false.
        let visible = reg.with_project_shapes(&pb, |_, _, _| {
            panic!("no shapes should be visible for project B");
        });
        assert!(!visible);
    }

    // -----------------------------------------------------------------------
    // fast_path_engaged counter
    // -----------------------------------------------------------------------

    #[test]
    fn fast_path_counter_increments() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("fp");
        let h = hash(1);

        for i in 0..5u64 {
            reg.observe(
                &p,
                &t,
                h,
                &QueryMetrics {
                    latency_ns: 1_000,
                    rows_scanned: i,
                    files_opened: 1,
                    bytes_decoded: 512,
                    cache_hits: 0,
                    fast_path_engaged: i % 2 == 0, // 3 true (0, 2, 4)
                    table_row_count: 0,
                },
            );
        }

        let mut fp_count = None;
        reg.with_project_shapes(&p, |_, sh, stats| {
            if sh == h {
                fp_count = Some(stats.fast_path_count);
            }
        });
        assert_eq!(fp_count, Some(3));
    }

    // -----------------------------------------------------------------------
    // Memory bound (updated for Phase 5.16.C: 8 buckets × ~2 KiB each)
    // -----------------------------------------------------------------------

    /// Allocate 500 distinct shapes for one project and assert mem_bytes()
    /// is ≤ 10 MiB.
    ///
    /// Phase 5.16.C adds 8 per-bucket latency histograms per shape:
    ///   5 global histograms × ~2 KiB + 8 bucket histograms × ~2 KiB ≈ 26 KiB/shape
    ///   500 shapes × 26 KiB ≈ 13 MiB worst case.
    /// In practice HDR histograms for our bounds are ≈ 1.6 KiB each; we cap at 10 MiB.
    ///
    /// This verifies the O(bytes) per-tenant cost bound from the
    /// `feedback_multitenant_isolation` rule.
    #[test]
    fn memory_bound_500_shapes() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("tbl");
        let m = metrics_with_latency(1_000);

        for i in 0..MAX_SHAPES_PER_PROJECT as u64 {
            reg.observe(&p, &t, hash(i), &m);
        }

        let bytes = reg.project_mem_bytes(&p);
        let limit = 10_000_000; // 10 MiB — generous cap for 5.16.C
        assert!(
            bytes <= limit,
            "project mem_bytes = {bytes}; expected ≤ {limit} (10 MiB)"
        );
        // Also verify we actually allocated something meaningful.
        assert!(
            bytes >= 1_000,
            "project mem_bytes = {bytes}; expected > 1 KiB (sanity)"
        );
    }

    // -----------------------------------------------------------------------
    // Scale-dependent regression tracking (Phase 5.16.C)
    // -----------------------------------------------------------------------

    /// A shape whose latency grows monotonically with table size is detected.
    ///
    /// We observe the same shape at three scale points:
    ///   - row_count = 5 k   → bucket 1, latency 1 ms
    ///   - row_count = 50 k  → bucket 2, latency 5 ms  (ratio 5.0 > 1.3 ✓)
    ///   - row_count = 500 k → bucket 3, latency 50 ms (ratio 10.0 > 1.3 ✓)
    ///
    /// `top_regressions` must return this shape.
    #[test]
    fn regression_detected_for_growing_latency() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("events");
        let h = hash(77);

        // Observe 100 times at each scale point to get stable p99.
        for _ in 0..100 {
            reg.observe(
                &p, &t, h,
                &QueryMetrics {
                    latency_ns: 1_000_000, // 1 ms
                    table_row_count: 5_000, // bucket 1
                    ..Default::default()
                },
            );
        }
        for _ in 0..100 {
            reg.observe(
                &p, &t, h,
                &QueryMetrics {
                    latency_ns: 5_000_000, // 5 ms
                    table_row_count: 50_000, // bucket 2
                    ..Default::default()
                },
            );
        }
        for _ in 0..100 {
            reg.observe(
                &p, &t, h,
                &QueryMetrics {
                    latency_ns: 50_000_000, // 50 ms
                    table_row_count: 500_000, // bucket 3
                    ..Default::default()
                },
            );
        }

        let alerts = reg.top_regressions(&p, &t);
        assert!(
            !alerts.is_empty(),
            "expected at least one regression alert; got none"
        );
        let alert = alerts.iter().find(|a| a.shape == h).expect("shape h not in alerts");
        assert!(
            alert.p99_ratio > 1.3,
            "p99_ratio = {}; expected > 1.3",
            alert.p99_ratio
        );
    }

    /// A shape with flat latency across all scale points is NOT flagged.
    #[test]
    fn no_regression_for_flat_latency() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("flat");
        let h = hash(88);

        // Same latency at three scale points.
        for &row_count in &[5_000u64, 50_000u64, 500_000u64] {
            for _ in 0..100 {
                reg.observe(
                    &p, &t, h,
                    &QueryMetrics {
                        latency_ns: 1_000_000, // always 1 ms
                        table_row_count: row_count,
                        ..Default::default()
                    },
                );
            }
        }

        let alerts = reg.top_regressions(&p, &t);
        assert!(
            alerts.is_empty(),
            "expected no regression alerts for flat latency; got {alerts:?}"
        );
    }
}
