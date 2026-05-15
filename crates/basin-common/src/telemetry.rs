//! Telemetry init.
//!
//! Per-project metrics from day one (see TASK.md cross-cutting). This module
//! sets up tracing-subscriber so logs are JSON in prod and pretty-printed in
//! dev. OTLP export is feature-gated so unit tests don't need a collector.
//!
//! [`ProjectCounterRegistry`] is the structured-aggregation surface: O(1) atomic
//! bumps from the engine / storage / WAL hot paths, cheap snapshots from the
//! public read API. The latency p99 is approximated from a fixed-size ring of
//! recent samples; production deployments wanting full HDR histograms can swap
//! the inner buffer without changing the public API.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::ProjectId;

/// Output format for the tracing subscriber.
#[derive(Clone, Copy, Debug, Default)]
pub enum LogFormat {
    /// Pretty, ANSI-coloured, human-readable. Default for dev.
    #[default]
    Pretty,
    /// One JSON object per line. Use in prod and CI.
    Json,
}

/// Initialise the global tracing subscriber.
///
/// Idempotent only at first call — repeated calls return Err. Tests should
/// call [`try_init_for_tests`] which silently ignores reinit errors.
pub fn init(
    default_level: Level,
    format: LogFormat,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level.to_string()));

    let registry = tracing_subscriber::registry().with(filter);

    match format {
        LogFormat::Pretty => registry
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .try_init(),
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true),
            )
            .try_init(),
    }
}

/// Test-helper. Safe to call from many tests in the same process; the second
/// and subsequent calls are no-ops.
pub fn try_init_for_tests() {
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
}

/// Number of recent latency samples retained per project for the p99 estimate.
/// Rolling-window approximation: cheap (~520 bytes/project) and stable enough
/// that p99 over the last ~128 ops is informative; swap for HDR / t-digest if
/// long-tail accuracy matters.
const LATENCY_RING_SIZE: usize = 128;

/// Per-project counters. Atomic bump points + a fixed-size ring of recent
/// latency samples. Public API is `record_*` (bumps) and `snapshot()` (read).
#[derive(Debug)]
pub struct ProjectCounters {
    ops_total: AtomicU64,
    bytes_read_total: AtomicU64,
    bytes_written_total: AtomicU64,
    errors_total: AtomicU64,
    latency: Mutex<LatencyRing>,
}

#[derive(Debug)]
struct LatencyRing {
    samples: [u32; LATENCY_RING_SIZE],
    head: usize,
    len: usize,
}

impl Default for LatencyRing {
    fn default() -> Self {
        Self {
            samples: [0; LATENCY_RING_SIZE],
            head: 0,
            len: 0,
        }
    }
}

impl Default for ProjectCounters {
    fn default() -> Self {
        Self {
            ops_total: AtomicU64::new(0),
            bytes_read_total: AtomicU64::new(0),
            bytes_written_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            latency: Mutex::new(LatencyRing::default()),
        }
    }
}

impl ProjectCounters {
    pub fn record_op(&self) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_bytes_read(&self, n: u64) {
        self.bytes_read_total.fetch_add(n, Ordering::Relaxed);
    }
    pub fn record_bytes_written(&self, n: u64) {
        self.bytes_written_total.fetch_add(n, Ordering::Relaxed);
    }
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    /// Record one latency observation in milliseconds. O(1).
    pub fn record_latency_ms(&self, ms: u32) {
        let mut g = self.latency.lock().expect("project latency ring poisoned");
        let head = g.head;
        g.samples[head] = ms;
        g.head = (head + 1) % LATENCY_RING_SIZE;
        if g.len < LATENCY_RING_SIZE {
            g.len += 1;
        }
    }

    /// Cheap copy of the current values; p99 sorts the ring (≤128 elements).
    pub fn snapshot(&self) -> ProjectCountersSnapshot {
        let g = self.latency.lock().expect("project latency ring poisoned");
        let live = &g.samples[..g.len];
        let p99 = if live.is_empty() {
            0
        } else {
            let mut buf: Vec<u32> = live.to_vec();
            buf.sort_unstable();
            let idx = ((buf.len() as f64) * 0.99).ceil() as usize;
            buf[idx.saturating_sub(1).min(buf.len() - 1)]
        };
        ProjectCountersSnapshot {
            ops_total: self.ops_total.load(Ordering::Relaxed),
            bytes_read_total: self.bytes_read_total.load(Ordering::Relaxed),
            bytes_written_total: self.bytes_written_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            latency_p99_ms_estimate: p99,
        }
    }
}

/// Plain-data snapshot of [`ProjectCounters`]. Cheap to clone / copy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProjectCountersSnapshot {
    pub ops_total: u64,
    pub bytes_read_total: u64,
    pub bytes_written_total: u64,
    pub errors_total: u64,
    pub latency_p99_ms_estimate: u32,
}

/// Process-wide registry of per-project counters. Lazy-allocates on first
/// access so per-project cost is O(bytes), not O(active_projects × buffer).
#[derive(Debug, Default)]
pub struct ProjectCounterRegistry {
    counters: RwLock<HashMap<ProjectId, Arc<ProjectCounters>>>,
}

impl ProjectCounterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-insert the per-project counters. Returns an `Arc` so callers can
    /// hold their own handle without re-locking on every bump.
    pub fn for_project(&self, project: &ProjectId) -> Arc<ProjectCounters> {
        {
            let map = self
                .counters
                .read()
                .expect("project counter registry poisoned");
            if let Some(c) = map.get(project) {
                return c.clone();
            }
        }
        let mut map = self
            .counters
            .write()
            .expect("project counter registry poisoned");
        map.entry(*project)
            .or_insert_with(|| Arc::new(ProjectCounters::default()))
            .clone()
    }

    /// `None` for projects with no recorded activity yet.
    pub fn snapshot(&self, project: &ProjectId) -> Option<ProjectCountersSnapshot> {
        let map = self
            .counters
            .read()
            .expect("project counter registry poisoned");
        map.get(project).map(|c| c.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_per_project_isolated() {
        let r = ProjectCounterRegistry::new();
        let a = ProjectId::new();
        let b = ProjectId::new();
        r.for_project(&a).record_bytes_written(1000);
        r.for_project(&b).record_bytes_written(7);
        assert_eq!(r.snapshot(&a).unwrap().bytes_written_total, 1000);
        assert_eq!(r.snapshot(&b).unwrap().bytes_written_total, 7);
        assert!(r.snapshot(&ProjectId::new()).is_none());
    }

    #[test]
    fn latency_p99_tracks_ring() {
        let c = ProjectCounters::default();
        for ms in 0..200u32 {
            c.record_latency_ms(ms);
        }
        let s = c.snapshot();
        assert!(s.latency_p99_ms_estimate >= 197);
    }
}
