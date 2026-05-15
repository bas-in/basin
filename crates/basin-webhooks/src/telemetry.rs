//! Per-project observability for the webhook fanout machinery.
//!
//! ## Counter shape
//!
//! Each project that emits at least one webhook event lazily acquires a
//! [`WebhookCounters`] handle: a small bag of `AtomicU64` (deliveries
//! succeeded / failed / retries / dead-letters), a signed `AtomicI64` for
//! the currently-pending queue depth attributable to that project, and a
//! fixed-size ring buffer of recent delivery latency samples used to
//! approximate p99. The shape mirrors `basin_common::telemetry`'s
//! [`ProjectCounters`](basin_common::telemetry::ProjectCounters); we do not
//! fork the underlying primitive — just add webhook-specific fields.
//!
//! ## Per-project cost discipline
//!
//! One process-wide [`WebhookCountersRegistry`] holding a single
//! `RwLock<HashMap<ProjectId, Arc<WebhookCounters>>>`. Per-project cost is
//! O(bytes): a `WebhookCounters` is roughly 32 bytes of atomics plus a
//! 128-sample ring (~520 bytes). Entries are created lazily on the first
//! webhook event for a project. No per-project pooled resources (worker,
//! file handle, semaphore) are allocated here — the same rule the rest
//! of `basin-webhooks` honours.
//!
//! ## How to read counters
//!
//! Production wiring holds an `Arc<WebhookSink>`. Call
//! [`WebhookSink::counters`](crate::WebhookSink::counters) to get an
//! `Arc<WebhookCountersRegistry>`, then
//! [`WebhookCountersRegistry::snapshot`] for one project or
//! [`WebhookCountersRegistry::snapshot_all`] for the whole table. The
//! plain-data [`WebhookCountersSnapshot`] is cheap to copy and serialize.
//!
//! ## OTLP propagation
//!
//! The worker delivery loop is wrapped in `#[tracing::instrument]` spans
//! whose attributes (`project`, `subscription_id`, `table`, `op`, `seq`,
//! `attempt_count`) propagate through `tracing-subscriber` to the OTLP
//! exporter wired by `basin-common::telemetry::init`. Span attributes
//! show up unchanged on the OTLP collector; counters are surfaced via
//! the snapshot API and any admin handler that wants to expose them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use basin_common::ProjectId;

/// Number of recent latency samples retained per project for the p99
/// estimate. Same constant the engine telemetry uses; bytes-cheap and
/// stable enough that p99 over the last ~128 deliveries is informative.
const LATENCY_RING_SIZE: usize = 128;

/// Per-project webhook counters.
///
/// Atomic bump points + a fixed-size ring of recent latency samples.
/// Public API is the `record_*` family (bumps) and [`Self::snapshot`]
/// (read).
#[derive(Debug)]
pub struct WebhookCounters {
    deliveries_succeeded: AtomicU64,
    deliveries_failed: AtomicU64,
    retries: AtomicU64,
    dead_lettered: AtomicU64,
    /// Signed: enqueues bump up, dequeues bump down. We keep this signed
    /// (rather than an unsigned counter pair) so a stale snapshot from a
    /// race never reads as `0` while events are clearly in flight.
    pending_queue_depth: AtomicI64,
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

impl Default for WebhookCounters {
    fn default() -> Self {
        Self {
            deliveries_succeeded: AtomicU64::new(0),
            deliveries_failed: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            pending_queue_depth: AtomicI64::new(0),
            latency: Mutex::new(LatencyRing::default()),
        }
    }
}

impl WebhookCounters {
    pub fn record_success(&self) {
        self.deliveries_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.deliveries_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dead_letter(&self) {
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_enqueue(&self) {
        self.pending_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dequeue(&self) {
        self.pending_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record one delivery latency observation in milliseconds. O(1).
    pub fn record_latency_ms(&self, ms: u32) {
        let mut g = self.latency.lock().expect("webhook latency ring poisoned");
        let head = g.head;
        g.samples[head] = ms;
        g.head = (head + 1) % LATENCY_RING_SIZE;
        if g.len < LATENCY_RING_SIZE {
            g.len += 1;
        }
    }

    /// Cheap copy of the current values; p99 sorts the ring (≤128 elements).
    pub fn snapshot(&self) -> WebhookCountersSnapshot {
        let g = self.latency.lock().expect("webhook latency ring poisoned");
        let live = &g.samples[..g.len];
        let p99 = if live.is_empty() {
            0.0
        } else {
            let mut buf: Vec<u32> = live.to_vec();
            buf.sort_unstable();
            let idx = ((buf.len() as f64) * 0.99).ceil() as usize;
            buf[idx.saturating_sub(1).min(buf.len() - 1)] as f64
        };
        WebhookCountersSnapshot {
            deliveries_succeeded: self.deliveries_succeeded.load(Ordering::Relaxed),
            deliveries_failed: self.deliveries_failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            pending_queue_depth: self.pending_queue_depth.load(Ordering::Relaxed),
            p99_latency_ms: p99,
        }
    }
}

/// Plain-data snapshot of [`WebhookCounters`]. Cheap to clone / copy.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WebhookCountersSnapshot {
    pub deliveries_succeeded: u64,
    pub deliveries_failed: u64,
    pub retries: u64,
    pub dead_lettered: u64,
    pub pending_queue_depth: i64,
    pub p99_latency_ms: f64,
}

/// Process-wide registry of per-project webhook counters. Lazy-allocates
/// on first access so per-project cost is O(bytes), not O(active_projects
/// × buffer).
#[derive(Debug, Default)]
pub struct WebhookCountersRegistry {
    per_project: RwLock<HashMap<ProjectId, Arc<WebhookCounters>>>,
}

impl WebhookCountersRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-insert the per-project counters. Returns an `Arc` so callers
    /// can hold their own handle without re-locking on every bump.
    pub fn for_project(&self, project: &ProjectId) -> Arc<WebhookCounters> {
        {
            let map = self
                .per_project
                .read()
                .expect("webhook counter registry poisoned");
            if let Some(c) = map.get(project) {
                return c.clone();
            }
        }
        let mut map = self
            .per_project
            .write()
            .expect("webhook counter registry poisoned");
        map.entry(*project)
            .or_insert_with(|| Arc::new(WebhookCounters::default()))
            .clone()
    }

    /// Snapshot for one project. Returns the zero-value snapshot if the
    /// project has not yet recorded a webhook event — saves the caller
    /// having to `Option::unwrap_or_default`.
    pub fn snapshot(&self, project: &ProjectId) -> WebhookCountersSnapshot {
        let map = self
            .per_project
            .read()
            .expect("webhook counter registry poisoned");
        map.get(project).map(|c| c.snapshot()).unwrap_or_default()
    }

    /// Snapshot every project that has ever recorded a webhook event.
    pub fn snapshot_all(&self) -> Vec<(ProjectId, WebhookCountersSnapshot)> {
        let map = self
            .per_project
            .read()
            .expect("webhook counter registry poisoned");
        map.iter().map(|(t, c)| (*t, c.snapshot())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_per_project_isolated() {
        let r = WebhookCountersRegistry::new();
        let a = ProjectId::new();
        let b = ProjectId::new();
        r.for_project(&a).record_success();
        r.for_project(&a).record_success();
        r.for_project(&b).record_failure();
        assert_eq!(r.snapshot(&a).deliveries_succeeded, 2);
        assert_eq!(r.snapshot(&a).deliveries_failed, 0);
        assert_eq!(r.snapshot(&b).deliveries_succeeded, 0);
        assert_eq!(r.snapshot(&b).deliveries_failed, 1);
        // Unseen project returns zero-value snapshot, not None.
        let unseen = WebhookCountersSnapshot::default();
        assert_eq!(r.snapshot(&ProjectId::new()), unseen);
    }

    #[test]
    fn pending_depth_signed_round_trip() {
        let c = WebhookCounters::default();
        for _ in 0..10 {
            c.record_enqueue();
        }
        for _ in 0..7 {
            c.record_dequeue();
        }
        assert_eq!(c.snapshot().pending_queue_depth, 3);
    }

    #[test]
    fn latency_p99_tracks_ring() {
        let c = WebhookCounters::default();
        for ms in 0..200u32 {
            c.record_latency_ms(ms);
        }
        let s = c.snapshot();
        // The ring keeps the last 128 samples (72..200); p99 ≈ 199.
        assert!(s.p99_latency_ms >= 197.0);
    }
}
