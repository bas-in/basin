//! Process-global in-flight + latency telemetry feeding the cloud autoscaler.
//!
//! The cloud autoscaler polls the REST route `GET /metrics/inflight` every
//! ~15s (per region) and scales up when `p99_micros` exceeds its SLO budget
//! or `inflight` crosses a concurrency ceiling. To make that possible the
//! engine has to expose two cheap signals:
//!
//! * **`inflight`** — a concurrency gauge: how many statements are executing
//!   engine-wide right now. Maintained by a single [`AtomicI64`] that an
//!   [`InflightGuard`] increments on [`enter`] and decrements on `Drop`, so
//!   the count is correct even when a query errors or the future is dropped
//!   on an unwind.
//! * **`p50`/`p99` latency** — percentiles over a rolling `window_secs` window
//!   of per-statement execution latencies. Backed by a `Mutex<Vec<Sample>>`
//!   pruned to the window on read. One push per statement; one poll every
//!   ~15s — the lock is never hot.
//!
//! There is one process-global [`InflightMetrics`] reachable via [`global`].
//! It is OSS-appropriate and cloud-agnostic: no gating, just counters.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;

/// Default rolling window for the latency percentiles, in seconds.
pub const DEFAULT_WINDOW_SECS: i32 = 10;

/// One recorded statement latency, tagged with when it completed (monotonic).
#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    micros: i64,
}

/// Process-global in-flight + latency telemetry.
pub struct InflightMetrics {
    /// Number of statements currently executing engine-wide.
    inflight: AtomicI64,
    /// Rolling window of recent completed-statement latencies.
    samples: Mutex<Vec<Sample>>,
    /// Rolling window length in seconds.
    window_secs: i32,
    /// Monotonic anchor used to age out samples.
    start_instant: Instant,
    /// Wall-clock process start, for the `started_at` field.
    start_wall: SystemTime,
}

impl InflightMetrics {
    fn new(window_secs: i32) -> Self {
        Self {
            inflight: AtomicI64::new(0),
            samples: Mutex::new(Vec::new()),
            window_secs,
            start_instant: Instant::now(),
            start_wall: SystemTime::now(),
        }
    }

    /// Mark the start of a statement execution. The returned guard increments
    /// the in-flight gauge now and, on `Drop`, decrements it and records the
    /// elapsed latency into the rolling window — including on error/panic
    /// unwind paths.
    pub fn enter(&'static self) -> InflightGuard {
        self.inflight.fetch_add(1, Ordering::Relaxed);
        InflightGuard {
            metrics: self,
            started: Instant::now(),
        }
    }

    /// Current in-flight statement count.
    pub fn inflight(&self) -> i64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Record a completed-statement latency directly (used by the guard and by
    /// tests). Prunes the window opportunistically so the buffer stays bounded
    /// even if reads are infrequent.
    fn record(&self, micros: i64) {
        let now = Instant::now();
        let cutoff = Duration::from_secs(self.window_secs.max(0) as u64);
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        samples.retain(|s| now.duration_since(s.at) <= cutoff);
        samples.push(Sample { at: now, micros });
    }

    /// Compute a point-in-time [`MetricsSnapshot`] over the live window.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let now = Instant::now();
        let cutoff = Duration::from_secs(self.window_secs.max(0) as u64);
        let mut latencies: Vec<i64> = {
            let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
            // Prune on read so the window is honest and the buffer bounded.
            samples.retain(|s| now.duration_since(s.at) <= cutoff);
            samples.iter().map(|s| s.micros).collect()
        };
        latencies.sort_unstable();

        let (p50, p99) = if latencies.is_empty() {
            (0, 0)
        } else {
            (percentile(&latencies, 0.50), percentile(&latencies, 0.99))
        };

        let inflight = self.inflight();
        MetricsSnapshot {
            // No goroutines in Rust; report the in-flight gauge again. The
            // scaler does not gate on this field.
            goroutines: inflight.min(i32::MAX as i64) as i32,
            inflight,
            window_secs: self.window_secs,
            samples: latencies.len().min(i32::MAX as usize) as i32,
            p50_micros: p50,
            p99_micros: p99,
            started_at: rfc3339(self.start_wall),
            observed_at: rfc3339(SystemTime::now()),
        }
    }
}

/// RAII guard returned by [`InflightMetrics::enter`]. Decrements the in-flight
/// gauge and records elapsed latency on `Drop`.
pub struct InflightGuard {
    metrics: &'static InflightMetrics,
    started: Instant,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.metrics.inflight.fetch_sub(1, Ordering::Relaxed);
        let micros = self
            .started
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        self.metrics.record(micros);
    }
}

/// Serde-serializable point-in-time snapshot. Field names + casing match the
/// `MetricsSample` contract the cloud autoscaler deserializes — do not rename.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub goroutines: i32,
    pub inflight: i64,
    pub window_secs: i32,
    pub samples: i32,
    pub p50_micros: i64,
    pub p99_micros: i64,
    pub started_at: String,
    pub observed_at: String,
}

/// The one process-global metrics instance, lazily initialised on first use
/// with [`DEFAULT_WINDOW_SECS`].
pub fn global() -> &'static InflightMetrics {
    static GLOBAL: OnceLock<InflightMetrics> = OnceLock::new();
    GLOBAL.get_or_init(|| InflightMetrics::new(DEFAULT_WINDOW_SECS))
}

/// Convenience: enter the global gauge for one statement. The returned guard
/// must be held for the duration of execution.
pub fn enter() -> InflightGuard {
    global().enter()
}

/// Snapshot the global metrics.
pub fn snapshot() -> MetricsSnapshot {
    global().snapshot()
}

/// Nearest-rank percentile over a pre-sorted, non-empty slice. `q` in [0,1].
fn percentile(sorted: &[i64], q: f64) -> i64 {
    debug_assert!(!sorted.is_empty());
    let n = sorted.len();
    // Nearest-rank: rank = ceil(q * n), clamped into [1, n], 1-based.
    let rank = (q * n as f64).ceil() as usize;
    let idx = rank.clamp(1, n) - 1;
    sorted[idx]
}

/// Format a `SystemTime` as RFC3339 UTC (e.g. `2026-06-18T12:34:56.789Z`).
fn rfc3339(t: SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A dedicated metrics instance leaked to `'static` so we can exercise the
    // guard's `&'static self` API without touching the shared process global
    // (keeps the test hermetic and order-independent).
    fn fresh(window_secs: i32) -> &'static InflightMetrics {
        Box::leak(Box::new(InflightMetrics::new(window_secs)))
    }

    #[test]
    fn inflight_counts_live_guards_and_decrements_on_drop() {
        let m = fresh(DEFAULT_WINDOW_SECS);
        assert_eq!(m.inflight(), 0);

        let guards: Vec<_> = (0..5).map(|_| m.enter()).collect();
        assert_eq!(m.inflight(), 5, "five live guards => inflight == 5");

        drop(guards);
        assert_eq!(m.inflight(), 0, "all guards dropped => inflight back to 0");
    }

    #[test]
    fn percentiles_are_sane() {
        let m = fresh(DEFAULT_WINDOW_SECS);
        // 1..=100 micros.
        for v in 1..=100 {
            m.record(v);
        }
        let snap = m.snapshot();
        assert_eq!(snap.samples, 100);
        // Nearest-rank p50 of 1..=100 is the 50th value == 50.
        assert_eq!(snap.p50_micros, 50);
        // p99 is the 99th value == 99.
        assert_eq!(snap.p99_micros, 99);
        assert!(snap.p50_micros <= snap.p99_micros);
    }

    #[test]
    fn window_prunes_old_samples() {
        // Zero-second window: every prior sample ages out immediately, so a
        // fresh snapshot sees only what is pushed in the same instant.
        let m = fresh(0);
        m.record(10);
        m.record(20);
        // With a 0s window the retain on the next read drops the earlier ones.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let snap = m.snapshot();
        assert_eq!(snap.samples, 0, "0s window prunes everything older than now");
    }

    #[test]
    fn snapshot_serializes_with_all_eight_fields() {
        let m = fresh(DEFAULT_WINDOW_SECS);
        let _g = m.enter();
        m.record(123);
        let snap = m.snapshot();
        let v: serde_json::Value = serde_json::to_value(&snap).unwrap();
        let obj = v.as_object().unwrap();
        for field in [
            "goroutines",
            "inflight",
            "window_secs",
            "samples",
            "p50_micros",
            "p99_micros",
            "started_at",
            "observed_at",
        ] {
            assert!(obj.contains_key(field), "missing field: {field}");
        }
        assert_eq!(obj.len(), 8, "exactly 8 fields, no extras");
        // window_secs default + the live guard is counted.
        assert_eq!(obj["window_secs"], 10);
        assert_eq!(obj["inflight"], 1);
        // RFC3339 UTC ends in Z.
        assert!(obj["started_at"].as_str().unwrap().ends_with('Z'));
        assert!(obj["observed_at"].as_str().unwrap().ends_with('Z'));
    }
}
