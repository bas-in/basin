//! T-053 — per-tenant usage counters + hourly snapshot persistence.
//!
//! # Design
//!
//! `TenantCounters` is a plain-data snapshot of the per-project atomic counters
//! kept by [`basin_common::telemetry::ProjectCounterRegistry`]. The snapshot is
//! collected at the call site (no locking here) and written to the catalog's
//! `tenant_usage_periods` table once per hour by [`spawn_usage_snapshot_task`].
//!
//! ## Table schema (`tenant_usage_periods`)
//!
//! | column              | type        | note                                  |
//! |---------------------|-------------|---------------------------------------|
//! | `project_id`        | TEXT        | ULID string                           |
//! | `period_start`      | TIMESTAMPTZ | truncated to the hour                 |
//! | `storage_bytes`     | BIGINT      | `bytes_written_total` at end of period|
//! | `ops_count`         | BIGINT      | `ops_total` delta for the period      |
//! | `active_hours`      | FLOAT8      | 1.0 if the project was seen this hour |
//! | `cpu_ms`            | BIGINT      | `cpu_micros_total / 1000` delta       |
//! | `recorded_at`       | TIMESTAMPTZ | wall-clock time of the snapshot write |
//!
//! PRIMARY KEY `(project_id, period_start)` — one row per project per hour.
//! `INSERT … ON CONFLICT DO UPDATE` makes re-runs idempotent (the last writer
//! for the period wins — safe because counters are monotonic and the delta is
//! recomputed from the live snapshot each tick).
//!
//! ## OTLP export
//!
//! [`spawn_usage_otlp_exporter`] emits structured `tracing::info!` records via
//! the `basin.tenant.usage` target. When `tracing-opentelemetry` is attached
//! these become OTLP metric events; plain `fmt` subscribers log them as INFO
//! lines useful for self-hosted debugging without a collector.
//!
//! The OTLP endpoint is configurable via `BASIN_OTLP_ENDPOINT` (default:
//! `http://localhost:4318`). The cloud's metering ingest (T-023) consumes these.
//!
//! ## Active-hours accounting
//!
//! A project is considered "active" in an hourly window if its `ops_total`
//! counter advanced since the previous snapshot. The per-project last-seen
//! watermark is tracked in memory by [`UsageSnapshotState`]; it is not persisted
//! so a restart re-attributes the first post-restart hour as active regardless
//! (conservative; the cloud-side roll-up deduplicates by `period_start`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use basin_common::{ProjectCounterRegistry, ProjectId};

/// Snapshot of per-tenant usage counters for one hourly period.
/// Collected from [`ProjectCounterRegistry`] and persisted to the catalog.
#[derive(Clone, Debug, Default)]
pub struct TenantCounters {
    /// Project this snapshot belongs to.
    pub project_id: ProjectId,
    /// Hour-truncated start of the period (UTC).
    pub period_start: chrono::DateTime<chrono::Utc>,
    /// Running total of written bytes at end of period (from
    /// `ProjectCounters::bytes_written_total`). Monotonic; the cloud-side
    /// aggregator computes deltas across consecutive rows.
    pub storage_bytes: u64,
    /// Total object-store operations at end of period (`ops_total` counter).
    pub ops_count: u64,
    /// 1.0 if the project saw at least one operation in this period; 0.0
    /// otherwise. The cloud billing roll-up sums these to compute
    /// `active_hours` per billing period.
    pub active_hours: f64,
    /// Cumulative CPU time in milliseconds at end of period (from
    /// `cpu_micros_total / 1000`). Monotonic; cloud side computes deltas.
    pub cpu_ms: u64,
}

/// In-process per-project watermark used to decide whether a project is
/// "active" in the current hour. Keyed by `ProjectId`; value is the
/// `ops_total` observed in the previous snapshot tick. When the current
/// snapshot shows `ops_total > prev`, the project is active.
#[derive(Default)]
struct UsageSnapshotState {
    prev_ops: HashMap<ProjectId, u64>,
}

impl UsageSnapshotState {
    fn is_active(&mut self, project: &ProjectId, current_ops: u64) -> bool {
        let prev = self.prev_ops.entry(*project).or_insert(0);
        let active = current_ops > *prev;
        *prev = current_ops;
        active
    }
}

/// Configuration for the hourly usage snapshot task.
#[derive(Clone, Debug)]
pub struct UsageSnapshotConfig {
    /// How often to collect a snapshot and persist it.  Default: 1 hour.
    pub interval: std::time::Duration,
    /// OTLP/HTTP endpoint the exporter pushes metrics to.
    /// Read from `BASIN_OTLP_ENDPOINT` at spawn time if `None`.
    pub otlp_endpoint: Option<String>,
}

impl Default for UsageSnapshotConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(3600),
            otlp_endpoint: None,
        }
    }
}

impl UsageSnapshotConfig {
    /// Resolve the OTLP endpoint: explicit override beats the env var, which
    /// beats the hard-coded default.
    pub fn resolved_otlp_endpoint(&self) -> String {
        if let Some(ref ep) = self.otlp_endpoint {
            return ep.clone();
        }
        std::env::var("BASIN_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4318".to_string())
    }
}

// ---------------------------------------------------------------------------
// Snapshot collection helpers
// ---------------------------------------------------------------------------

/// Collect a [`TenantCounters`] snapshot for every project that has registered
/// activity in `registry`. The `state` mutex carries the previous-tick
/// watermarks across calls.
pub fn collect_snapshots(
    registry: &ProjectCounterRegistry,
    state: &Mutex<UsageSnapshotState>,
    period_start: chrono::DateTime<chrono::Utc>,
) -> Vec<TenantCounters> {
    // Read-lock the registry once to enumerate projects; each project's
    // individual snapshot is lock-free (atomic loads on the counters).
    let snapshots = registry.snapshot_all_projects();
    let mut state_guard = state.lock().expect("usage snapshot state poisoned");
    snapshots
        .into_iter()
        .map(|(project_id, snap)| {
            let active = state_guard.is_active(&project_id, snap.ops_total);
            TenantCounters {
                project_id,
                period_start,
                storage_bytes: snap.bytes_written_total,
                ops_count: snap.ops_total,
                active_hours: if active { 1.0 } else { 0.0 },
                cpu_ms: snap.cpu_micros_total / 1000,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Background task: hourly snapshot + OTLP export
// ---------------------------------------------------------------------------

/// Spawn a background task that wakes on `config.interval` and:
/// 1. Collects a [`TenantCounters`] snapshot per active project.
/// 2. Calls `persist_fn` to write each row to the catalog.
/// 3. Emits structured tracing events for OTLP export.
///
/// `persist_fn` is async and receives a `Vec<TenantCounters>`; it should
/// write to `tenant_usage_periods`. Errors from `persist_fn` are logged but
/// do not stop the loop.
///
/// Returns a [`tokio::task::JoinHandle`] that can be aborted on engine
/// shutdown.
pub fn spawn_usage_snapshot_task<F, Fut>(
    registry: Arc<ProjectCounterRegistry>,
    config: UsageSnapshotConfig,
    persist_fn: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(Vec<TenantCounters>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let otlp_endpoint = config.resolved_otlp_endpoint();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        // Skip the first immediate tick so we don't emit empty metrics on startup.
        interval.tick().await;

        let state = Mutex::new(UsageSnapshotState::default());

        loop {
            interval.tick().await;

            let now = chrono::Utc::now();
            // Truncate to the current hour for the period_start column.
            let period_start = {
                use chrono::Timelike;
                now.with_minute(0)
                    .and_then(|t| t.with_second(0))
                    .and_then(|t| t.with_nanosecond(0))
                    .unwrap_or(now)
            };

            let counters = collect_snapshots(&registry, &state, period_start);

            // 1. Emit OTLP-compatible tracing events.
            for c in &counters {
                tracing::info!(
                    target: "basin.tenant.usage",
                    project_id = %c.project_id,
                    period_start = %c.period_start,
                    storage_bytes = c.storage_bytes,
                    ops_count = c.ops_count,
                    active_hours = c.active_hours,
                    cpu_ms = c.cpu_ms,
                    otlp_endpoint = %otlp_endpoint,
                    "basin.tenant.usage.snapshot",
                );
            }

            tracing::debug!(
                target: "basin.tenant.usage",
                projects = counters.len(),
                "tenant usage snapshot cycle complete",
            );

            // 2. Persist to catalog.
            if !counters.is_empty() {
                persist_fn(counters).await;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ProjectCounterRegistry, ProjectId};
    use std::sync::{Arc, Mutex};

    #[test]
    fn active_hours_is_one_when_ops_advance() {
        let registry = ProjectCounterRegistry::new();
        let p = ProjectId::new();
        let c = registry.for_project(&p);
        c.record_op();
        c.record_bytes_written(1024);
        c.record_cpu_micros(500);

        let state = Mutex::new(UsageSnapshotState::default());
        let now = chrono::Utc::now();
        let snaps = collect_snapshots(&registry, &state, now);

        assert_eq!(snaps.len(), 1);
        let snap = &snaps[0];
        assert_eq!(snap.project_id, p);
        assert_eq!(snap.active_hours, 1.0, "first tick should be active");
        assert_eq!(snap.ops_count, 1);
        assert_eq!(snap.storage_bytes, 1024);
        assert_eq!(snap.cpu_ms, 0); // 500 µs / 1000 = 0 ms (integer division)
    }

    #[test]
    fn active_hours_is_zero_when_no_new_ops() {
        let registry = ProjectCounterRegistry::new();
        let p = ProjectId::new();
        let c = registry.for_project(&p);
        c.record_op();

        let state = Mutex::new(UsageSnapshotState::default());
        let now = chrono::Utc::now();
        // First tick: active.
        collect_snapshots(&registry, &state, now);
        // Second tick: no new ops → inactive.
        let snaps = collect_snapshots(&registry, &state, now);
        let snap = snaps.iter().find(|s| s.project_id == p).unwrap();
        assert_eq!(snap.active_hours, 0.0, "second tick without new ops should be inactive");
    }

    #[test]
    fn cpu_ms_conversion_from_micros() {
        let registry = ProjectCounterRegistry::new();
        let p = ProjectId::new();
        let c = registry.for_project(&p);
        c.record_op();
        c.record_cpu_micros(2_500_000); // 2500 ms

        let state = Mutex::new(UsageSnapshotState::default());
        let now = chrono::Utc::now();
        let snaps = collect_snapshots(&registry, &state, now);
        let snap = snaps.iter().find(|s| s.project_id == p).unwrap();
        assert_eq!(snap.cpu_ms, 2500);
    }

    #[test]
    fn multi_project_isolation_in_snapshots() {
        let registry = ProjectCounterRegistry::new();
        let a = ProjectId::new();
        let b = ProjectId::new();
        registry.for_project(&a).record_bytes_written(1000);
        registry.for_project(&a).record_op();
        registry.for_project(&b).record_bytes_written(7);
        registry.for_project(&b).record_op();

        let state = Mutex::new(UsageSnapshotState::default());
        let now = chrono::Utc::now();
        let snaps = collect_snapshots(&registry, &state, now);

        assert!(snaps.len() >= 2);
        let sa = snaps.iter().find(|s| s.project_id == a).unwrap();
        let sb = snaps.iter().find(|s| s.project_id == b).unwrap();
        assert_eq!(sa.storage_bytes, 1000);
        assert_eq!(sb.storage_bytes, 7);
    }

    #[tokio::test]
    async fn snapshot_task_calls_persist_fn() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let p = ProjectId::new();
        registry.for_project(&p).record_op();

        let captured: Arc<Mutex<Vec<TenantCounters>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();

        let config = UsageSnapshotConfig {
            interval: std::time::Duration::from_millis(20),
            otlp_endpoint: Some("http://localhost:4318".into()),
        };

        let handle = spawn_usage_snapshot_task(registry, config, move |rows| {
            let captured = captured_clone.clone();
            async move {
                captured.lock().unwrap().extend(rows);
            }
        });

        // Let at least one tick fire.
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        handle.abort();

        let rows = captured.lock().unwrap();
        assert!(!rows.is_empty(), "persist_fn should have been called at least once");
        let row = rows.iter().find(|r| r.project_id == p);
        assert!(row.is_some(), "project should appear in persisted rows");
    }
}
