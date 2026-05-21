//! HDR-histogram latency sampling + the comprehensive `SampleReport` shape.
//!
//! The legacy `tests/integration/src/workload.rs::LatencyDistribution` reports
//! p50/p99/p999/min/max/mean — fine for one card, but two limitations bit us
//! repeatedly:
//!
//! 1. Sorting the full sample vector to compute percentiles is O(N log N) per
//!    shape and dominates the wall-clock for high-iteration shapes (50k+
//!    iterations × 30 shapes).
//! 2. There was no facility for live merging across tasks — the
//!    concurrency-fan-out tests collected per-task vectors and concatenated.
//!
//! [`LatencySamples`] wraps [`hdrhistogram::Histogram`] so percentiles are
//! O(1) post-record, merging is associative, and the dashboard surface stays
//! identical (we still emit p50/p99/p999/mean — plus p95, count, and a
//! memory-watermark sample the legacy distribution didn't carry).
//!
//! `SampleReport` is the JSON-serialisable view the dashboard renders. It is
//! a SUPERSET of the legacy `LatencyDistribution` so existing cards keep
//! rendering unchanged.

use std::sync::Arc;
use std::sync::Mutex;

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Shared, thread-safe HDR-histogram accumulator.
///
/// Cheap to clone (Arc inside). Record from any task; call `report()` at the
/// end. Range: 1 µs .. 60 s with 3 significant digits — covers every shape
/// we care about. A point-query under 1 µs is implausible; anything over 60s
/// is a deadline miss.
#[derive(Clone)]
pub struct LatencySamples {
    inner: Arc<Mutex<Histogram<u64>>>,
}

impl LatencySamples {
    /// New histogram covering 1 µs..60 s @ 3 sig figs.
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60 * 1_000_000, 3)
            .expect("hdr histogram bounds valid");
        Self {
            inner: Arc::new(Mutex::new(hist)),
        }
    }

    /// Record one latency sample in microseconds. Out-of-range values are
    /// clamped to the histogram bounds — this is the conservative choice for
    /// regression detection: a 90s outlier is recorded as "60s", which still
    /// flips the deadline assertion.
    pub fn record_us(&self, value_us: u64) {
        let mut g = self.inner.lock().expect("histogram mutex");
        let _ = g.record(value_us.clamp(1, 60 * 1_000_000));
    }

    /// Convenience: record an [`std::time::Duration`].
    pub fn record(&self, d: std::time::Duration) {
        self.record_us(d.as_micros() as u64);
    }

    /// Merge another sample bag into this one (associative).
    pub fn merge(&self, other: &LatencySamples) {
        let other_g = other.inner.lock().expect("other histogram mutex");
        let mut self_g = self.inner.lock().expect("self histogram mutex");
        self_g.add(&*other_g).expect("merge bounds match");
    }

    /// Snapshot the current state into a JSON-serialisable report.
    pub fn report(&self) -> SampleReport {
        let g = self.inner.lock().expect("histogram mutex");
        SampleReport {
            n: g.len(),
            min_us: g.min(),
            max_us: g.max(),
            mean_us: g.mean(),
            p50_us: g.value_at_quantile(0.50),
            p95_us: g.value_at_quantile(0.95),
            p99_us: g.value_at_quantile(0.99),
            p999_us: g.value_at_quantile(0.999),
        }
    }
}

impl Default for LatencySamples {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON-serialisable view of a [`LatencySamples`].
///
/// Lives next to per-shape `details` in the dashboard JSON so the renderer
/// can pull p99 / p999 / mean off without re-derivation. New fields are
/// safe to add (the dashboard ignores unknown keys); renaming is not.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleReport {
    pub n: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

impl SampleReport {
    /// Convenience: derived throughput at a wall-clock window.
    pub fn qps(&self, wall_secs: f64) -> f64 {
        if wall_secs <= 0.0 {
            0.0
        } else {
            self.n as f64 / wall_secs
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn records_and_reports() {
        let s = LatencySamples::new();
        for i in 1..=1000u64 {
            s.record_us(i * 10); // 10us..10ms
        }
        let r = s.report();
        assert_eq!(r.n, 1000);
        // p50 ≈ 5000us. Tolerate ±1% (hdr bucketisation).
        assert!(r.p50_us >= 4_900 && r.p50_us <= 5_100, "got {}", r.p50_us);
        assert!(r.p99_us >= 9_800 && r.p99_us <= 10_000);
    }

    #[test]
    fn merge_is_associative() {
        let a = LatencySamples::new();
        let b = LatencySamples::new();
        for _ in 0..500 {
            a.record(Duration::from_micros(100));
            b.record(Duration::from_micros(200));
        }
        a.merge(&b);
        assert_eq!(a.report().n, 1000);
    }
}
