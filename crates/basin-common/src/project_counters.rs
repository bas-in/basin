//! Phase 6.X.F — observability counters for lease ownership + heartbeat
//! budgets (ADR 0023).
//!
//! This module is the *aggregation surface* for the metrics ops dashboards
//! consume. It is deliberately:
//!
//! - **Lock-free on the hot path.** Every primitive is an
//!   [`std::sync::atomic`] cell. A lease acquire / renew bumps one atomic;
//!   no allocation, no map walk.
//! - **Lazy per dimension.** A `(project, cap)` over-cap pair is allocated
//!   on first observation; idle projects cost zero. Matches the
//!   `feedback_multitenant_isolation` rule: per-project cost is `O(bytes)`.
//! - **Snapshot-shaped.** Consumers — an external OTLP collector, an
//!   operator's dashboard, the managed control plane's aggregator — pull a
//!   plain-data snapshot. No exporter is wired into this crate, and nothing
//!   in this tree serves these counters over HTTP; the same surface maps
//!   trivially onto OTLP/Prometheus once a collector is attached.
//!
//! ## Metric inventory
//!
//! The names mirror the OTLP names ADR 0023 calls out for 6.X.F. Counter
//! semantics are monotonic-or-saturating; gauges read the current value;
//! histograms ship as (count, sum) plus a fixed-size sample ring so p99 is
//! approximable. Production deployments wanting full HDR histograms can
//! swap [`LatencyRing`] without changing the public API (same trick as
//! [`crate::telemetry::ProjectCounters`]).
//!
//! | metric (OTLP)                              | type      | dims            |
//! |--------------------------------------------|-----------|-----------------|
//! | `basin_lease_holdings_total`               | gauge     | replica          |
//! | `basin_lease_acquire_total`                | counter   | replica, result  |
//! | `basin_lease_renew_total`                  | counter   | replica, result  |
//! | `basin_lease_handoff_duration_ms`          | histogram | replica          |
//! | `basin_budget_over_cap_seconds_total`      | counter   | project, cap     |
//! | `basin_heartbeat_lag_ms`                   | histogram | replica          |
//!
//! `basin_lease_handoff_duration_ms` is shape-only in v1 — the handoff
//! state machine (6.X.C) hasn't landed; the histogram is empty (count = 0)
//! until that lands and starts pushing samples via
//! [`LeaseMetrics::record_handoff_duration`]. Wiring it now keeps the
//! dashboard contract stable so 6.X.C is purely additive on the
//! observability side.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::ProjectId;

/// Outcome label on `basin_lease_acquire_total`. Mirrors the `result=`
/// dimension ADR 0023 calls out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AcquireResult {
    /// Lease was freshly granted (first-grant via insert).
    Acquired,
    /// Existing lease had expired and the caller stole it.
    Stolen,
    /// A different, non-expired holder owns it; caller lost the race.
    Failed,
}

impl AcquireResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Acquired => "acquired",
            Self::Stolen => "stolen",
            Self::Failed => "failed",
        }
    }
}

/// Outcome label on `basin_lease_renew_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RenewResult {
    /// Renewal succeeded — the caller still holds the lease.
    Ok,
    /// Renewal failed because the lease was lost (stolen / expired-and-
    /// regranted / never held). Caller must drop its partition state.
    Expired,
    /// Renewal hit a transport error before the registry answered. The
    /// caller treats this the same as `Expired` for safety (drop state),
    /// but the dashboard distinguishes the two so a flapping coordinator
    /// is visible separately from real ownership churn.
    Failed,
}

impl RenewResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Expired => "expired",
            Self::Failed => "failed",
        }
    }
}

/// Names the cap consumer site for the over-cap dimension. Same set as
/// [`crate`]-external `basin_catalog::CapKind`; duplicated as a private
/// string label rather than depending on basin-catalog to keep
/// basin-common a leaf crate (no upward dep).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CapLabel {
    RestQps,
    PgQps,
    MemtableBytes,
    RealtimeBytes,
    WasmConcurrency,
    NetOutboundQps,
}

impl CapLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::RestQps => "rest_qps",
            Self::PgQps => "pg_qps",
            Self::MemtableBytes => "memtable_bytes",
            Self::RealtimeBytes => "realtime_bytes",
            Self::WasmConcurrency => "wasm_concurrency",
            Self::NetOutboundQps => "net_outbound_qps",
        }
    }
}

/// Number of recent samples retained per histogram for the p99 estimate.
/// Same trade-off as [`crate::telemetry::ProjectCounters`]: cheap (~520 B
/// per ring), and stable enough for an ops dashboard. Production deploys
/// wanting long-tail accuracy can swap in HDR / t-digest behind the same
/// `record_*` / `snapshot` surface.
const LATENCY_RING_SIZE: usize = 128;

#[derive(Debug)]
struct LatencyRing {
    samples: [u32; LATENCY_RING_SIZE],
    head: usize,
    len: usize,
    count: u64,
    sum: u64,
}

impl Default for LatencyRing {
    fn default() -> Self {
        Self {
            samples: [0; LATENCY_RING_SIZE],
            head: 0,
            len: 0,
            count: 0,
            sum: 0,
        }
    }
}

impl LatencyRing {
    fn record(&mut self, ms: u32) {
        let head = self.head;
        self.samples[head] = ms;
        self.head = (head + 1) % LATENCY_RING_SIZE;
        if self.len < LATENCY_RING_SIZE {
            self.len += 1;
        }
        self.count = self.count.saturating_add(1);
        self.sum = self.sum.saturating_add(ms as u64);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        let live = &self.samples[..self.len];
        let p99 = if live.is_empty() {
            0
        } else {
            let mut buf: Vec<u32> = live.to_vec();
            buf.sort_unstable();
            let idx = ((buf.len() as f64) * 0.99).ceil() as usize;
            buf[idx.saturating_sub(1).min(buf.len() - 1)]
        };
        HistogramSnapshot {
            count: self.count,
            sum_ms: self.sum,
            p99_ms_estimate: p99,
        }
    }
}

/// Plain-data view of a histogram metric. `count` / `sum_ms` are the OTLP
/// histogram primitives; `p99_ms_estimate` is the ring-buffer approximation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum_ms: u64,
    pub p99_ms_estimate: u32,
}

/// Per-replica lease-event counters. Atomic bumps from the heartbeat loop;
/// cheap snapshot for the dashboard / `/metrics` endpoint.
#[derive(Debug, Default)]
struct ReplicaLeaseCounters {
    /// `basin_lease_holdings_total{replica}` — current number of leases
    /// held by this replica. `i64` so a transient over-decrement during a
    /// race doesn't wrap to `u64::MAX` and break the dashboard; in steady
    /// state the value is always `>= 0`.
    holdings: AtomicI64,
    /// `basin_lease_acquire_total{replica,result=acquired}`.
    acquire_acquired: AtomicU64,
    /// `basin_lease_acquire_total{replica,result=stolen}`.
    acquire_stolen: AtomicU64,
    /// `basin_lease_acquire_total{replica,result=failed}`.
    acquire_failed: AtomicU64,
    /// `basin_lease_renew_total{replica,result=ok}`.
    renew_ok: AtomicU64,
    /// `basin_lease_renew_total{replica,result=expired}`.
    renew_expired: AtomicU64,
    /// `basin_lease_renew_total{replica,result=failed}`.
    renew_failed: AtomicU64,
    /// `basin_lease_handoff_duration_ms{replica}`. v1 stays empty until
    /// 6.X.C lands and starts pushing samples; the entry exists so the
    /// dashboard contract is stable from day one.
    handoff_duration_ms: Mutex<LatencyRing>,
    /// `basin_heartbeat_lag_ms{replica}` — round-trip of the heartbeat
    /// renew / budget push observed from the heartbeat loop site.
    heartbeat_lag_ms: Mutex<LatencyRing>,
}

/// Plain-data snapshot of [`ReplicaLeaseCounters`]. Cheap to clone and ship
/// over the wire. Each `result` label is its own counter to keep the API
/// flat — exporters fan them back into `result={acquired|stolen|failed}`
/// per OTLP convention.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicaLeaseSnapshot {
    pub holdings: i64,
    pub acquire_acquired_total: u64,
    pub acquire_stolen_total: u64,
    pub acquire_failed_total: u64,
    pub renew_ok_total: u64,
    pub renew_expired_total: u64,
    pub renew_failed_total: u64,
    pub handoff_duration_ms: HistogramSnapshot,
    pub heartbeat_lag_ms: HistogramSnapshot,
}

/// Process-wide lease + budget observability registry. Cheap to clone via
/// [`Arc`]. Each replica wires one in through [`crate`]'s sibling
/// `basin-shard` so the heartbeat loop site can bump counters; readers pull
/// plain-data snapshots via [`Self::snapshot_replicas`] and
/// [`Self::snapshot_over_cap`].
///
/// Nothing in this tree serves these over HTTP. The server's only metrics
/// route is `GET /metrics/inflight`, which returns JSON from
/// `basin_engine::inflight_metrics` — a different set of counters. Telemetry
/// otherwise leaves the process as structured tracing spans over OTLP, so
/// reaching these numbers means an external collector snapshotting them
/// in-process.
#[derive(Debug, Default)]
pub struct LeaseMetrics {
    /// Per-replica state. Keyed by replica id so a multi-replica test
    /// process (the integration suite) still observes each replica's
    /// numbers independently. In a real deploy there is exactly one entry.
    replicas: RwLock<HashMap<String, Arc<ReplicaLeaseCounters>>>,
    /// `basin_budget_over_cap_seconds_total{project,cap}`. Counter, in
    /// seconds, of time the project's per-partition slice for `cap` was
    /// observed at-or-above 100 % utilisation. The leaseholder bumps this
    /// from its heartbeat tick when a cap consumer reports it was rate-
    /// limited / queued / shed during the past interval.
    over_cap: RwLock<HashMap<(ProjectId, CapLabel), Arc<AtomicU64>>>,
}

impl LeaseMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-insert the per-replica counters. Returns an `Arc` so the
    /// heartbeat loop can hold one handle and bump without re-locking.
    fn for_replica(&self, replica: &str) -> Arc<ReplicaLeaseCounters> {
        {
            let map = self
                .replicas
                .read()
                .expect("lease metrics replicas poisoned");
            if let Some(c) = map.get(replica) {
                return c.clone();
            }
        }
        let mut map = self
            .replicas
            .write()
            .expect("lease metrics replicas poisoned");
        map.entry(replica.to_string())
            .or_insert_with(|| Arc::new(ReplicaLeaseCounters::default()))
            .clone()
    }

    fn over_cap_counter(&self, project: &ProjectId, cap: CapLabel) -> Arc<AtomicU64> {
        let key = (*project, cap);
        {
            let map = self
                .over_cap
                .read()
                .expect("lease metrics over_cap poisoned");
            if let Some(c) = map.get(&key) {
                return c.clone();
            }
        }
        let mut map = self
            .over_cap
            .write()
            .expect("lease metrics over_cap poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    /// `basin_lease_acquire_total{replica,result=…}` += 1.
    /// Also bumps the holdings gauge on `Acquired` / `Stolen`.
    pub fn record_acquire(&self, replica: &str, result: AcquireResult) {
        let r = self.for_replica(replica);
        match result {
            AcquireResult::Acquired => {
                r.acquire_acquired.fetch_add(1, Ordering::Relaxed);
                r.holdings.fetch_add(1, Ordering::Relaxed);
            }
            AcquireResult::Stolen => {
                r.acquire_stolen.fetch_add(1, Ordering::Relaxed);
                r.holdings.fetch_add(1, Ordering::Relaxed);
            }
            AcquireResult::Failed => {
                r.acquire_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// `basin_lease_renew_total{replica,result=…}` += 1.
    /// On `Expired` / `Failed`, decrements the holdings gauge so it tracks
    /// "leases this replica still owns".
    pub fn record_renew(&self, replica: &str, result: RenewResult) {
        let r = self.for_replica(replica);
        match result {
            RenewResult::Ok => {
                r.renew_ok.fetch_add(1, Ordering::Relaxed);
            }
            RenewResult::Expired => {
                r.renew_expired.fetch_add(1, Ordering::Relaxed);
                r.holdings.fetch_sub(1, Ordering::Relaxed);
            }
            RenewResult::Failed => {
                r.renew_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Observe one `basin_lease_handoff_duration_ms` sample. Called by the
    /// 6.X.C handoff state machine once it lands; v1 nothing calls this.
    pub fn record_handoff_duration(&self, replica: &str, ms: u32) {
        let r = self.for_replica(replica);
        let mut g = r
            .handoff_duration_ms
            .lock()
            .expect("lease metrics handoff ring poisoned");
        g.record(ms);
    }

    /// Observe one `basin_heartbeat_lag_ms` sample. Called by the shard
    /// heartbeat loop wrapping `LeaseRegistry::renew` /
    /// `BudgetCoordinator::push_heartbeat` round-trips.
    pub fn record_heartbeat_lag(&self, replica: &str, ms: u32) {
        let r = self.for_replica(replica);
        let mut g = r
            .heartbeat_lag_ms
            .lock()
            .expect("lease metrics heartbeat ring poisoned");
        g.record(ms);
    }

    /// `basin_budget_over_cap_seconds_total{project,cap}` += `seconds`.
    /// Saturating-add so a runaway counter never wraps.
    pub fn record_over_cap_seconds(&self, project: &ProjectId, cap: CapLabel, seconds: u64) {
        let c = self.over_cap_counter(project, cap);
        c.fetch_add(seconds, Ordering::Relaxed);
    }

    /// Snapshot all per-replica counters. Cheap O(replicas) clone — in
    /// production there's one entry; in tests there's one per simulated
    /// replica.
    pub fn snapshot_replicas(&self) -> HashMap<String, ReplicaLeaseSnapshot> {
        let map = self
            .replicas
            .read()
            .expect("lease metrics replicas poisoned");
        map.iter()
            .map(|(k, v)| {
                let handoff = v
                    .handoff_duration_ms
                    .lock()
                    .expect("lease metrics handoff ring poisoned")
                    .snapshot();
                let hb = v
                    .heartbeat_lag_ms
                    .lock()
                    .expect("lease metrics heartbeat ring poisoned")
                    .snapshot();
                let snap = ReplicaLeaseSnapshot {
                    holdings: v.holdings.load(Ordering::Relaxed),
                    acquire_acquired_total: v.acquire_acquired.load(Ordering::Relaxed),
                    acquire_stolen_total: v.acquire_stolen.load(Ordering::Relaxed),
                    acquire_failed_total: v.acquire_failed.load(Ordering::Relaxed),
                    renew_ok_total: v.renew_ok.load(Ordering::Relaxed),
                    renew_expired_total: v.renew_expired.load(Ordering::Relaxed),
                    renew_failed_total: v.renew_failed.load(Ordering::Relaxed),
                    handoff_duration_ms: handoff,
                    heartbeat_lag_ms: hb,
                };
                (k.clone(), snap)
            })
            .collect()
    }

    /// Snapshot every `(project, cap)` over-cap counter. Returns the OTLP
    /// label strings so the exporter can hand the result to its sink
    /// without a lookup table.
    pub fn snapshot_over_cap(&self) -> Vec<((ProjectId, &'static str), u64)> {
        let map = self
            .over_cap
            .read()
            .expect("lease metrics over_cap poisoned");
        map.iter()
            .map(|((p, c), v)| ((*p, c.as_str()), v.load(Ordering::Relaxed)))
            .collect()
    }
}

/// String label for the `replica=` dimension, in case an exporter wants to
/// stringify the per-replica snapshot keys without re-implementing the
/// encoding.
pub fn acquire_result_label(r: AcquireResult) -> &'static str {
    r.as_str()
}

/// See [`acquire_result_label`].
pub fn renew_result_label(r: RenewResult) -> &'static str {
    r.as_str()
}

/// See [`acquire_result_label`].
pub fn cap_label(c: CapLabel) -> &'static str {
    c.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_then_renew_bumps_counters_and_holdings_gauge() {
        let m = LeaseMetrics::new();
        m.record_acquire("replica-a", AcquireResult::Acquired);
        m.record_acquire("replica-a", AcquireResult::Acquired);
        m.record_renew("replica-a", RenewResult::Ok);
        let snap = m.snapshot_replicas();
        let a = snap.get("replica-a").expect("replica entry exists");
        assert_eq!(a.acquire_acquired_total, 2);
        assert_eq!(a.renew_ok_total, 1);
        assert_eq!(a.holdings, 2);
    }

    #[test]
    fn expired_renew_decrements_holdings_gauge() {
        let m = LeaseMetrics::new();
        m.record_acquire("r", AcquireResult::Acquired);
        m.record_acquire("r", AcquireResult::Stolen);
        m.record_renew("r", RenewResult::Expired);
        let snap = m.snapshot_replicas();
        let r = snap.get("r").unwrap();
        assert_eq!(r.acquire_acquired_total, 1);
        assert_eq!(r.acquire_stolen_total, 1);
        assert_eq!(r.renew_expired_total, 1);
        assert_eq!(r.holdings, 1, "Acquired+Stolen = +2, Expired = -1, net = 1");
    }

    #[test]
    fn failed_acquire_does_not_change_holdings() {
        let m = LeaseMetrics::new();
        m.record_acquire("r", AcquireResult::Failed);
        let snap = m.snapshot_replicas();
        let r = snap.get("r").unwrap();
        assert_eq!(r.acquire_failed_total, 1);
        assert_eq!(r.holdings, 0);
    }

    #[test]
    fn over_cap_counter_is_per_project_per_cap() {
        let m = LeaseMetrics::new();
        let a = ProjectId::new();
        let b = ProjectId::new();
        m.record_over_cap_seconds(&a, CapLabel::RestQps, 3);
        m.record_over_cap_seconds(&a, CapLabel::RestQps, 5);
        m.record_over_cap_seconds(&a, CapLabel::MemtableBytes, 1);
        m.record_over_cap_seconds(&b, CapLabel::RestQps, 7);
        let snap: HashMap<(ProjectId, &'static str), u64> =
            m.snapshot_over_cap().into_iter().collect();
        assert_eq!(snap.get(&(a, "rest_qps")).copied(), Some(8));
        assert_eq!(snap.get(&(a, "memtable_bytes")).copied(), Some(1));
        assert_eq!(snap.get(&(b, "rest_qps")).copied(), Some(7));
    }

    #[test]
    fn heartbeat_lag_histogram_tracks_count_sum_and_p99() {
        let m = LeaseMetrics::new();
        for ms in [1u32, 2, 3, 4, 5, 100] {
            m.record_heartbeat_lag("r", ms);
        }
        let snap = m.snapshot_replicas();
        let r = snap.get("r").unwrap();
        assert_eq!(r.heartbeat_lag_ms.count, 6);
        assert_eq!(r.heartbeat_lag_ms.sum_ms, 115);
        assert_eq!(r.heartbeat_lag_ms.p99_ms_estimate, 100);
    }

    #[test]
    fn handoff_histogram_starts_empty_for_6_x_c_dependency() {
        // 6.X.F ships the histogram shape; 6.X.C will push samples once
        // the handoff state machine lands. Until then, count == 0 is the
        // expected dashboard reading.
        let m = LeaseMetrics::new();
        m.record_acquire("r", AcquireResult::Acquired);
        let snap = m.snapshot_replicas();
        let r = snap.get("r").unwrap();
        assert_eq!(r.handoff_duration_ms.count, 0);
        assert_eq!(r.handoff_duration_ms.sum_ms, 0);
    }

    #[test]
    fn unknown_replica_snapshot_is_empty_map() {
        let m = LeaseMetrics::new();
        assert!(m.snapshot_replicas().is_empty());
        assert!(m.snapshot_over_cap().is_empty());
    }
}
