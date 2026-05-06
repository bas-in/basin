//! Workload helpers for honest latency benchmarks.
//!
//! Background: until this module landed, every "warm read" benchmark in
//! `tests/integration/tests/viability_*.rs` looped the same `SELECT` 10
//! times and reported `warm = 1ms`. This is a cache-hit lottery, not a
//! workload — the 2nd through 10th iterations are trivial cache hits on
//! one row's worth of data, so the bar was being cleared by the
//! identity transformation `cache.get(k)` and not by anything resembling
//! a real point-query path.
//!
//! What honest looks like:
//!   1. **Random working set.** Build a fixed-size pool of "hot" ids
//!      (e.g. 1000) drawn at random from the table, and have each query
//!      pick uniformly from the pool. The cache must service the whole
//!      working set, not one row.
//!   2. **Enough samples for tail percentiles.** N >= 1000 iterations so
//!      p999 is meaningful (10 outliers). Median alone hides the tail.
//!   3. **Cold AND warm phases reported separately.** Cold = caches just
//!      built (fresh tempdir + fresh in-RAM cache); warm = working set
//!      already touched once. Bars assert against COLD because real
//!      workloads can't lean on a pre-warmed cache.
//!   4. **p50, p99, p999 reported.** A latency line with only median is
//!      a marketing number; bars need the tail.
//!
//! This module is the single shared implementation. Each cache /
//! point-query benchmark wires its own `query` closure into
//! [`run_workload`] and gets back a [`LatencyDistribution`] it can drop
//! into the benchmark JSON sidecar verbatim.
//!
//! Reproducibility: the working-set sampler uses `StdRng::seed_from_u64`
//! with a caller-supplied seed. Two runs with the same `(seed,
//! working_set_size, n_iterations, table_size)` tuple will issue exactly
//! the same query stream, so a CI-reported regression replays bit-for-bit
//! locally.

use std::future::Future;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Configuration knobs for [`run_workload`].
///
/// `working_set_size` is the number of distinct hot ids in the random
/// pool. The actual queries pick uniformly from this pool, so a smaller
/// working set means more cache hits and a larger one means more misses.
/// 1000 is the default for cache-hit-class benchmarks: large enough that
/// no implementation can pretend by caching one row, small enough that
/// it comfortably fits in any reasonable on-disk + in-RAM cache budget.
///
/// `n_iterations` is the total number of queries the workload runs.
/// Tail percentiles (p99, p999) are statistically meaningful at >= 1000
/// — p999 is the 0.1% tail, so 1000 samples gives one observation in the
/// tail bucket. We pick 1000 by default; cards can override upward.
///
/// `seed` feeds the PRNG. Pinning lets the same workload replay across
/// runs and across machines. There's no value in randomising the seed
/// — non-determinism in benchmarks is a debugging hazard.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadConfig {
    pub working_set_size: usize,
    pub n_iterations: usize,
    pub seed: u64,
}

/// The default seed every benchmark card uses unless it has a reason
/// to differ. `0xBA51_F1ED_BA51_F1ED` reads as "BASIN-FIED, BASIN-FIED"
/// — easy to spot in logs and crashes.
pub const DEFAULT_SEED: u64 = 0xBA51_F1ED_BA51_F1ED;

impl WorkloadConfig {
    /// Defaults: 1000 hot ids, 1000 iterations, [`DEFAULT_SEED`].
    pub const fn default_for_point_query() -> Self {
        Self {
            working_set_size: 1_000,
            n_iterations: 1_000,
            seed: DEFAULT_SEED,
        }
    }

    /// Same as [`default_for_point_query`] but with a caller-supplied
    /// seed. Useful when two cards in the same suite want statistically
    /// independent query streams.
    pub const fn with_seed(seed: u64) -> Self {
        Self {
            working_set_size: 1_000,
            n_iterations: 1_000,
            seed,
        }
    }
}

/// Latency percentiles computed from a workload's per-query samples.
/// All values are milliseconds.
///
/// The dashboard schema for cache-class cards expects exactly these
/// three numbers per phase (cold/warm); compute once via
/// [`LatencyDistribution::from_samples_ms`] and emit straight into the
/// `rows` array.
#[derive(Clone, Debug)]
pub struct LatencyDistribution {
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    /// Min sample, useful for sanity-checking that the pipeline does
    /// what it should under best-case conditions.
    pub min_ms: f64,
    /// Max sample. The single worst observation; complements p999 when
    /// n is small.
    pub max_ms: f64,
    /// Arithmetic mean over the samples. Reported alongside percentiles
    /// so the dashboard can surface mean-vs-median skew on long-tail
    /// distributions without forcing a re-derive in the renderer.
    pub mean_ms: f64,
    /// Total sample count.
    pub n: usize,
}

impl LatencyDistribution {
    /// Compute percentiles from a slice of millisecond samples.
    ///
    /// Uses nearest-rank percentile: the kth percentile is the smallest
    /// sample whose rank exceeds k% of the data. For p99 over 1000
    /// samples this picks sample[990] of the sorted vector. We pick
    /// nearest-rank rather than interpolated so two implementations of
    /// "p99" don't disagree on the dashboard depending on which crate
    /// emitted them.
    pub fn from_samples_ms(mut samples: Vec<f64>) -> Self {
        assert!(!samples.is_empty(), "no samples for percentile compute");
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = samples.len();
        let pick = |q: f64| -> f64 {
            // nearest-rank: idx = ceil(q * n) - 1, clamped to [0, n-1]
            let idx = ((q * n as f64).ceil() as isize - 1).max(0) as usize;
            samples[idx.min(n - 1)]
        };
        let p50_ms = pick(0.50);
        let p99_ms = pick(0.99);
        let p999_ms = pick(0.999);
        let min_ms = samples[0];
        let max_ms = samples[n - 1];
        let mean_ms = samples.iter().copied().sum::<f64>() / n as f64;
        Self {
            p50_ms,
            p99_ms,
            p999_ms,
            min_ms,
            max_ms,
            mean_ms,
            n,
        }
    }
}

/// Build the fixed working set of hot ids drawn from `[0, table_size)`.
///
/// The PRNG is seeded with `seed`. We sample with replacement (so the
/// same id can show up twice in the pool) — this matches a realistic
/// power-law skew where the actual hot set is fuzzy, and removes the
/// need for an O(n) deduplication pass when `working_set_size` is large.
pub fn pick_working_set(table_size: u64, working_set_size: usize, seed: u64) -> Vec<u64> {
    assert!(table_size > 0, "table_size must be > 0");
    let mut rng = StdRng::seed_from_u64(seed);
    (0..working_set_size).map(|_| rng.gen_range(0..table_size)).collect()
}

/// Run `n_iterations` queries against `query`, picking one id per
/// iteration uniformly from the precomputed working set, and return the
/// resulting latency distribution.
///
/// `query(id)` must return a future that resolves to `Ok(())` for a
/// successful query (any latency); `Err` panics the workload (these are
/// integration tests — surfacing a backend error as `n - failures` would
/// just hide regressions). The caller is responsible for asserting any
/// row-correctness invariants inside the closure.
///
/// This function does not own the cache state — the caller controls
/// "cold" vs "warm" by choosing what `Storage` (or equivalent) the
/// closure captures. Typical pattern:
/// ```ignore
/// let cold_dist = run_workload(&cfg, |id| async {
///     storage.point_query(id).await
/// }).await;
/// // …same closure, second invocation: cache is now warm…
/// let warm_dist = run_workload(&cfg, |id| async {
///     storage.point_query(id).await
/// }).await;
/// ```
pub async fn run_workload<F, Fut, T, E>(
    cfg: &WorkloadConfig,
    table_size: u64,
    mut query: F,
) -> LatencyDistribution
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let working_set = pick_working_set(table_size, cfg.working_set_size, cfg.seed);
    // Picker PRNG: distinct from the working-set PRNG so changing
    // `n_iterations` doesn't reshuffle the working set. Salt with a
    // small constant so seeds collide only on intent.
    let mut rng = StdRng::seed_from_u64(cfg.seed.wrapping_add(0x9E37_79B9_7F4A_7C15));
    let mut samples_ms: Vec<f64> = Vec::with_capacity(cfg.n_iterations);
    for _ in 0..cfg.n_iterations {
        let id = working_set[rng.gen_range(0..working_set.len())];
        let t0 = std::time::Instant::now();
        let res = query(id).await;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1_000.0;
        if let Err(e) = res {
            panic!("workload query failed (id={id}): {e:?}");
        }
        samples_ms.push(elapsed_ms);
    }
    LatencyDistribution::from_samples_ms(samples_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_nearest_rank() {
        // 1..=100, so p50=50, p99=99, p999=100 (with n=100).
        let samples: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let d = LatencyDistribution::from_samples_ms(samples);
        assert_eq!(d.p50_ms, 50.0);
        assert_eq!(d.p99_ms, 99.0);
        assert_eq!(d.p999_ms, 100.0);
        assert_eq!(d.min_ms, 1.0);
        assert_eq!(d.max_ms, 100.0);
        assert!((d.mean_ms - 50.5).abs() < 1e-9);
        assert_eq!(d.n, 100);
    }

    #[test]
    fn working_set_is_seed_stable() {
        let a = pick_working_set(1_000_000, 1000, 42);
        let b = pick_working_set(1_000_000, 1000, 42);
        assert_eq!(a, b);
        let c = pick_working_set(1_000_000, 1000, 43);
        assert_ne!(a, c);
        for &id in &a {
            assert!(id < 1_000_000);
        }
    }

    #[tokio::test]
    async fn run_workload_uses_random_ids() {
        let cfg = WorkloadConfig {
            working_set_size: 64,
            n_iterations: 256,
            seed: 7,
        };
        let observed = std::sync::Mutex::new(Vec::<u64>::new());
        let _ = run_workload::<_, _, (), std::convert::Infallible>(&cfg, 1_000_000, |id| {
            observed.lock().unwrap().push(id);
            async move { Ok(()) }
        })
        .await;
        let v = observed.into_inner().unwrap();
        assert_eq!(v.len(), 256);
        // With 256 picks over a working set of 64, we should observe at
        // least 16 distinct ids (very loose bound for a uniform sampler).
        let mut sorted = v.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert!(sorted.len() >= 16, "too few distinct ids: {}", sorted.len());
    }

    #[test]
    fn default_seed_is_stable() {
        // Pin the constant — changing this would invalidate every
        // existing card's published cold-p99 number.
        assert_eq!(DEFAULT_SEED, 0xBA51_F1ED_BA51_F1ED);
    }
}
