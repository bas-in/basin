//! `basin-shard` — stateful shard owner.
//!
//! ## Status
//!
//! v0.1: **in-process map** of `(project_id, partition_key) → in-memory state`,
//! with lazy load from WAL + Parquet, idle eviction, and a background
//! compactor that drains WAL segments into Parquet via the storage layer
//! and commits via the catalog.
//!
//! v0.2 (deferred): a placement service that spreads projects across many
//! shard-owner processes. Today everything runs in-process; the API is
//! shaped so that swap is a backend change.
//!
//! ## Why this exists
//!
//! Two dashboard cards fail honestly today:
//! - `scaling_concurrency`: 3.4× speedup at 64 readers (bar 4×). Single
//!   process saturates one runtime.
//! - `scaling_noisy_neighbor`: 42× p99 degradation under a noisy project.
//!
//! Shard owners give each project their own slice of in-memory state plus
//! eviction; the noisy project's full scans no longer block the quiet
//! project's point queries because they live in different per-project
//! data structures and run on separate task groups.
//!
//! ## Public API
//!
//! - [`Shard`]: cheap-to-clone handle.
//! - [`ShardConfig`]: storage + catalog + WAL + eviction knobs.
//! - [`ProjectHandle`]: lazy-loaded per-project state — write/read.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_common::{PartitionKey, ProjectId, Result, TableName};

/// Knobs for [`Shard::new`].
#[derive(Clone)]
pub struct ShardConfig {
    pub storage: basin_storage::Storage,
    pub catalog: Arc<dyn basin_catalog::Catalog>,
    pub wal: Arc<dyn basin_wal::Wal>,
    /// Projects idle for at least this long are evicted by the eviction loop.
    /// Default 5 minutes.
    pub eviction_idle: Duration,
    /// How often the compactor checks for WAL segments to drain. Default 30s.
    pub compaction_interval: Duration,
    /// How often the eviction loop runs. Default 60s.
    pub eviction_interval: Duration,
    /// Process-wide HTAP hot-tier memtable registry.  When present,
    /// [`Shard::spawn_background`] also spawns the memtable → Vortex flush
    /// loop (Phase 5.14.C4).  When absent the flush loop is not started.
    pub memtable_registry: Option<Arc<basin_hottier::MemTableRegistry>>,
    /// How often the flush task's periodic trigger fires.  Default 5 s.
    pub flush_tick_interval: Duration,
    /// Phase 6.X.A — optional lease registry (ADR 0023). When present, the
    /// shard acquires a lease per `(project, partition)` it owns and the
    /// background heartbeat renews held leases on a fixed cadence; a renewal
    /// failure (lost lease) drops the partition's in-memory state. When
    /// absent, the shard runs in single-replica / no-lease mode exactly as
    /// before — `get` neither acquires nor fences.
    pub lease_registry: Option<Arc<dyn basin_catalog::LeaseRegistry>>,
    /// This replica's stable lease holder id (e.g. `host:pid` or a uuid).
    /// Defaults to a per-process random id; only meaningful when
    /// `lease_registry` is set.
    pub replica_id: String,
    /// Lease TTL handed to `acquire` / `renew`. Default 15 s
    /// (`BASIN_LEASE_TTL_SECS`).
    pub lease_ttl: Duration,
    /// Heartbeat renew cadence. Default 5 s (`BASIN_LEASE_RENEW_SECS`).
    pub lease_renew_interval: Duration,
    /// Phase 6.X.D — optional budget coordinator (ADR 0023). When attached,
    /// the lease heartbeat tick *also* pushes per-`(project, partition)`
    /// usage deltas to the coordinator and writes the returned slice budget
    /// into each registered [`basin_catalog::SliceBudgetView`]. v1 ships
    /// zero-delta heartbeats — the slice itself is the load-bearing
    /// cross-replica primitive; per-cap usage telemetry feeds the
    /// coordinator in a follow-on once each cap consumer exposes an
    /// observable counter. **Closes the multi-instance cap-bypass P0.**
    pub budget_coordinator: Option<Arc<dyn basin_catalog::BudgetCoordinator>>,
    /// Phase 6.X.D — slice views the heartbeat tick should push slices into.
    /// Same [`basin_catalog::SliceBudgetView`] handle each cap consumer was
    /// built with; the heartbeat fans the coordinator's slice answer out to
    /// every registered view (typically one per cap consumer crate).
    pub slice_views: Vec<basin_catalog::SliceBudgetView>,
    /// Phase 6.X.D — slice gates whose token counters should be refilled
    /// after every heartbeat tick. Refilling is necessary for the qps-style
    /// caps (`RestQps` / `PgQps` / `NetOutboundQps`) where the slice is
    /// "tokens per heartbeat window" — without periodic refill the gate
    /// would drain and stay drained forever. Byte-style caps
    /// (`MemtableBytes` / `RealtimeBytes`) read the slice directly via
    /// [`basin_catalog::SliceBudgetView::slice_for_sync`] and don't need
    /// the gate refill.
    pub slice_gates: Vec<basin_catalog::SliceGate>,
    /// Phase 6.X.F — optional lease observability sink (ADR 0023). When
    /// attached, the lease acquire / renew / heartbeat sites bump the
    /// corresponding counters; absent it, the metric emission sites are
    /// no-ops (zero-cost back-compat). One handle per process; consumers
    /// snapshot it via [`basin_common::LeaseMetrics::snapshot_replicas`]
    /// and [`basin_common::LeaseMetrics::snapshot_over_cap`].
    pub lease_metrics: Option<Arc<basin_common::LeaseMetrics>>,
}

impl ShardConfig {
    pub fn new(
        storage: basin_storage::Storage,
        catalog: Arc<dyn basin_catalog::Catalog>,
        wal: Arc<dyn basin_wal::Wal>,
    ) -> Self {
        Self {
            storage,
            catalog,
            wal,
            eviction_idle: Duration::from_secs(300),
            compaction_interval: Duration::from_secs(30),
            eviction_interval: Duration::from_secs(60),
            memtable_registry: None,
            flush_tick_interval: Duration::from_secs(5),
            lease_registry: None,
            replica_id: default_replica_id(),
            lease_ttl: Duration::from_secs(env_secs(
                "BASIN_LEASE_TTL_SECS",
                basin_catalog::DEFAULT_LEASE_TTL_SECS,
            )),
            lease_renew_interval: Duration::from_secs(env_secs(
                "BASIN_LEASE_RENEW_SECS",
                basin_catalog::DEFAULT_LEASE_RENEW_SECS,
            )),
            budget_coordinator: None,
            slice_views: Vec::new(),
            slice_gates: Vec::new(),
            lease_metrics: None,
        }
    }

    /// Phase 6.X.F — attach the observability sink. The lease event sites
    /// then bump counters on `lease_metrics`; consumers (basin-cloud
    /// exporter, `/metrics` endpoint) snapshot via
    /// [`basin_common::LeaseMetrics::snapshot_replicas`]. Optional —
    /// shards without a metrics handle behave identically.
    pub fn with_lease_metrics(mut self, metrics: Arc<basin_common::LeaseMetrics>) -> Self {
        self.lease_metrics = Some(metrics);
        self
    }

    /// Phase 6.X.D — attach a budget coordinator + the cap consumers'
    /// slice views/gates. The lease heartbeat then becomes the budget
    /// heartbeat too: each tick pushes a usage delta and writes the slice
    /// answer back into every registered view.
    pub fn with_budget_coordinator(
        mut self,
        coordinator: Arc<dyn basin_catalog::BudgetCoordinator>,
        views: Vec<basin_catalog::SliceBudgetView>,
        gates: Vec<basin_catalog::SliceGate>,
    ) -> Self {
        self.budget_coordinator = Some(coordinator);
        self.slice_views = views;
        self.slice_gates = gates;
        self
    }

    /// Attach a lease registry + holder id so [`Shard::spawn_background`] runs
    /// the per-partition lease heartbeat (Phase 6.X.A, ADR 0023).
    pub fn with_lease_registry(
        mut self,
        registry: Arc<dyn basin_catalog::LeaseRegistry>,
        replica_id: impl Into<String>,
    ) -> Self {
        self.lease_registry = Some(registry);
        self.replica_id = replica_id.into();
        self
    }

    /// Attach the HTAP memtable registry so [`Shard::spawn_background`] also
    /// starts the flush loop (Phase 5.14.C4).
    pub fn with_memtable_registry(
        mut self,
        registry: Arc<basin_hottier::MemTableRegistry>,
    ) -> Self {
        self.memtable_registry = Some(registry);
        self
    }
}

/// Stats. Implementations push these out via tracing for the dashboard.
#[derive(Clone, Debug, Default)]
pub struct ShardStats {
    pub resident_projects: usize,
    pub resident_partitions: usize,
    pub evictions: u64,
    pub compactions: u64,
    pub bytes_in_wal: u64,
    /// Phase 5.14.D2: count of compactions where the output file was sorted by
    /// the query-history top-K pattern rather than the declared `cluster_columns`.
    pub compactions_with_adaptive_sort: u64,
}

/// Handle to the shard map. Cheap to clone (Arc inside).
#[derive(Clone)]
pub struct Shard {
    inner: Arc<dyn ShardImpl>,
}

impl Shard {
    pub fn new(cfg: ShardConfig) -> Self {
        let inner = in_process::InProcessShard::new(cfg);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Handle to the underlying WAL. Exposed so the engine can plumb its
    /// per-project counter registry into the WAL alongside the storage layer.
    pub fn wal(&self) -> &Arc<dyn basin_wal::Wal> {
        self.inner.wal()
    }

    /// Get a handle to `(project, partition)`. Lazy-loads the state from WAL +
    /// Parquet on first access; subsequent calls return a cheap clone.
    pub async fn get(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<ProjectHandle> {
        self.inner.get(project, partition).await
    }

    /// Spawn the eviction + compaction background loops. Returns a handle
    /// that, when dropped, signals shutdown. Call this once at server boot.
    pub fn spawn_background(&self) -> ShardBackgroundHandle {
        self.inner.clone_arc().spawn_background()
    }

    pub fn stats(&self) -> ShardStats {
        self.inner.stats()
    }

    /// Synchronously drain every resident partition's in-memory tail into
    /// Parquet, committing through the catalog and truncating the WAL.
    ///
    /// Engines that share read paths with batch analytics use this to make a
    /// just-written batch visible to a downstream `SELECT` that reads through
    /// the Parquet base (no tail-merge), trading a small latency hit on the
    /// query for not having to teach the analytical pipeline about tails.
    pub async fn flush_to_parquet(&self) -> Result<()> {
        self.inner.flush_to_parquet().await
    }

    /// Task #142: returns `true` iff `(project, table)` has writes that have
    /// not yet been flushed to Parquet. The fast-select gate
    /// (`fast_select::execute_simple_select`) uses this to skip the
    /// `flush_to_parquet()` call (with its async mutex + per-partition
    /// HashMap clone) when no writes have landed since the last flush.
    ///
    /// Correctness: a `false` return is a positive assertion that
    /// every write happens-before-this-call is already durable in the
    /// catalog. The in-process shard maintains per-(project, table)
    /// pending/flushed counters with `SeqCst` ordering to make
    /// false-negatives impossible (see `DirtyCounter` in `in_process.rs`).
    pub fn has_pending_data(&self, project: &ProjectId, table: &TableName) -> bool {
        self.inner.has_pending_data(project, table)
    }

    /// Run one pass of the tiered-storage sweep. For every `(project, table)`
    /// the shard has touched (i.e. has resident state for, OR has been seen
    /// to live in the catalog under a known project), copy data files older
    /// than `cold_after_seconds` into the cold tier and atomically swap the
    /// catalog over.
    ///
    /// Exposed for tests and one-shot ops; the in-process compactor will
    /// also call this from its background tick once we surface a tier
    /// interval knob (TODO: align with `compaction_interval`).
    pub async fn run_tiering_sweep(&self) -> Result<()> {
        self.inner.run_tiering_sweep().await
    }

    /// ADR 0027 Phase 4 — rewrite every cold-tier data file for `(project,
    /// table)` that is missing the table's promoted JSONB shadow column(s).
    ///
    /// This is what a Basin background compactor does over time ("eventual
    /// backfill").  Calling this eagerly models the steady state of a live
    /// deployment where the compactor has had time to cover every file.
    ///
    /// Idempotent: files that already carry all shadow columns are skipped.
    /// Returns the count of files that were rewritten.
    ///
    /// The bench calls this after `flush_to_parquet()` in the JSONB
    /// steady-state path to ensure all seeded cold-tier files carry the
    /// shadow column before the timed measurement.
    pub async fn run_promoted_column_backfill_sweep(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<usize> {
        self.inner
            .run_promoted_column_backfill_sweep(project, table)
            .await
    }

    /// Run one CV refresh sweep across every resident project. Returns the
    /// count of CVs that were re-materialised this pass.
    ///
    /// The shard owner is the natural choice of driver because it already
    /// has the per-project residency map; reusing it avoids a parallel
    /// "list of resident projects" structure inside `basin-cv`. The actual
    /// refresh logic lives in [`basin_cv::CvRefresher`] — this method
    /// just walks the resident project set, calls the supplied
    /// [`CvRefreshDriver`] once per project, and sums the resulting
    /// `refreshed` counts.
    ///
    /// `driver` is normally a `&basin_cv::CvRefresher`, which implements
    /// [`CvRefreshDriver`] in the basin-cv crate. The trait is declared
    /// here rather than in basin-cv to keep the dependency graph one-way:
    /// `basin-engine -> basin-shard`, `basin-cv -> basin-engine ->
    /// basin-shard`, and `basin-shard` itself stays leaf-free.
    ///
    /// Mirrors [`Shard::run_tiering_sweep`]'s shape so a future production
    /// driver that ticks both at the same cadence has a uniform API.
    pub async fn run_cv_refresh<D: CvRefreshDriver>(&self, driver: &D) -> Result<usize> {
        let projects = self.inner.resident_projects().await;
        let mut total = 0usize;
        for t in &projects {
            total = total.saturating_add(driver.refresh_project(t).await?);
        }
        Ok(total)
    }

    /// Phase 5.14.D2: register the adaptive-sort query-pattern provider.
    /// The engine calls this once at startup so the compactor can detect
    /// common ORDER BY / GROUP BY access patterns and pre-sort files.
    pub fn set_top_pattern_provider(&self, provider: Arc<dyn TopPatternProvider>) {
        self.inner.set_top_pattern_provider(provider);
    }

    /// Wire the engine's GIN row-group bloom registry into the compactor.
    ///
    /// Called once from `Engine::new` so the compactor can re-index compacted
    /// files into the same registry the engine's `@>` prune reads from.  On
    /// the shard path, INSERT writes go directly to WAL + tail (bypassing the
    /// engine's `maintain_secondary_indexes_on_insert`), so compaction is the
    /// only point where GIN row-group summaries can be populated for
    /// compacted files.  Without this, the completeness guard in
    /// `apply_gin_pruning_for_query` would always see un-indexed live files
    /// and fall back to a full scan.
    pub fn set_gin_rowgroup_registry(
        &self,
        registry: Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    ) {
        self.inner.set_gin_rowgroup_registry(registry);
    }

    /// Phase 6.X.C (ADR 0023) — voluntarily hand off the lease for
    /// `(project, partition)` to `to_holder`. While the handoff is in
    /// progress new writes against this partition fail with
    /// [`basin_common::BasinError::LeaseHandoffInProgress`] so the router
    /// can retry against the new owner; reads continue. On completion the
    /// outgoing replica releases the lease, the candidate `acquire`s under
    /// a fresh epoch, and replay of the WAL on the new owner picks up any
    /// post-handoff entries.
    ///
    /// Target: **< 500 ms p99 stall** for the yielded partition
    /// (small-memtable case). Larger memtables are dominated by the
    /// cold-tier write step; operators should size partitions accordingly.
    /// See the `InProcessShard::yield_partition` docstring for the full
    /// state machine.
    ///
    /// No-op (returns `Ok`) if this replica doesn't hold the lease, or if
    /// no lease registry is configured.
    pub async fn yield_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        to_holder: &str,
    ) -> Result<()> {
        self.inner
            .yield_partition(project, partition, to_holder)
            .await
    }

    /// Drop the shard's in-memory tail (and cached schema) for
    /// `(project, table)` across every resident partition.
    ///
    /// SECURITY (sec_catalog_leakage P1): without this, rows that were
    /// INSERTed into a table and never flushed past the shard's tail
    /// survive `DROP TABLE` — the catalog row is removed but the read
    /// path still merges the tail batches over the (now-absent) cold
    /// tier, leaking data to a fresh session that re-issues
    /// `SELECT * FROM <dropped>` (or even re-creates the same table
    /// name). `exec_drop_table` calls this immediately after the
    /// catalog-side `drop_table` succeeds.
    pub async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        self.inner.drop_table(project, table).await
    }

    /// Test-only: pull out the concrete in-process implementation so the
    /// inline tests can drive its synchronous helpers. Returns `None` if a
    /// future backend swap replaces the in-process map.
    #[cfg(test)]
    pub(crate) fn impl_handle(&self) -> Option<Arc<in_process::InProcessShard>> {
        self.inner.as_in_process()
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

/// Per-project state handle. Holding one keeps the project resident.
#[derive(Clone)]
pub struct ProjectHandle {
    pub(crate) inner: Arc<dyn ProjectHandleImpl>,
}

impl ProjectHandle {
    /// Append a batch into this project's table. Durable when this returns
    /// (WAL committed). The batch is ack'd before it has reached Parquet.
    pub async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()> {
        self.inner.write_batch(table, batch).await
    }

    /// Read all rows currently visible for a table — both the in-RAM tail
    /// (WAL-resident, not yet flushed) and the Parquet base — in one stream.
    pub async fn read(
        &self,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        self.inner.read(table, opts).await
    }

    pub fn last_active(&self) -> Instant {
        self.inner.last_active()
    }

    pub fn project(&self) -> ProjectId {
        self.inner.project()
    }
}

/// Returned from [`Shard::spawn_background`]; drop to stop the loops.
pub struct ShardBackgroundHandle {
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    pub(crate) join: tokio::task::JoinHandle<()>,
}

impl ShardBackgroundHandle {
    /// Stop the eviction + compaction loops, awaiting their completion.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.join.await;
    }
}

/// Driver the shard hands each resident project to during
/// [`Shard::run_cv_refresh`]. Implementations live in `basin-cv`
/// (`CvRefresher` is the canonical one); the trait sits in `basin-shard`
/// purely so `Shard::run_cv_refresh` can call it without taking a
/// Pluggable query-pattern provider for adaptive sort (Phase 5.14.D2).
/// Declared here so `basin-shard`'s compactor can call it without a
/// direct dependency on `basin-engine`. `basin-engine::QueryHistory`
/// implements this trait and registers itself via `Shard::set_top_pattern_provider`.
pub trait TopPatternProvider: Send + Sync {
    /// Return the most-frequently-observed GROUP BY / ORDER BY column tuple
    /// for `(project, table)`, or `None` when there is insufficient history.
    fn top_pattern(&self, project: &ProjectId, table: &TableName) -> Option<Vec<String>>;
    /// Record a divergence between the observed access pattern and the
    /// table's declared `CLUSTER BY` columns.  Used for telemetry only.
    fn record_cluster_delta(
        &self,
        project: &ProjectId,
        table: &TableName,
        observed: &[String],
        declared: &[String],
    );
}

/// `basin-cv` dependency (which would otherwise close a cycle through
/// `basin-engine`).
#[async_trait]
pub trait CvRefreshDriver: Send + Sync {
    /// Refresh every CV under `project` whose interval has elapsed.
    /// Returns the count of CVs that were re-materialised on this call;
    /// `NotDue` / `Failed` outcomes do not count.
    async fn refresh_project(&self, project: &ProjectId) -> Result<usize>;
}

#[async_trait]
pub(crate) trait ShardImpl: Send + Sync {
    async fn get(&self, project: &ProjectId, partition: &PartitionKey) -> Result<ProjectHandle>;
    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle;
    fn stats(&self) -> ShardStats;
    fn clone_arc(&self) -> Arc<dyn ShardImpl>;
    fn wal(&self) -> &Arc<dyn basin_wal::Wal>;
    async fn flush_to_parquet(&self) -> Result<()>;
    /// Task #142 dirty-bit gate: does this shard have writes for
    /// `(project, table)` that have NOT yet been flushed to Parquet?
    ///
    /// `false` means it is safe for a read path to skip
    /// [`flush_to_parquet`](Self::flush_to_parquet). The implementation
    /// must guarantee that `false` is returned only when every write that
    /// happened-before this call is durable in the catalog. Default impl
    /// returns `true` (conservative) so a backend that has not wired the
    /// counter still flushes on every read — correctness first.
    fn has_pending_data(&self, _project: &ProjectId, _table: &TableName) -> bool {
        true
    }
    async fn run_tiering_sweep(&self) -> Result<()>;
    /// ADR 0027 Phase 4 — rewrite all cold-tier data files that are missing
    /// the promoted JSONB shadow column(s) for `(project, table)`.  Returns
    /// the count of files that were rewritten.  Default: returns 0 (no-op for
    /// backends that do not carry per-file cold data).
    async fn run_promoted_column_backfill_sweep(
        &self,
        _project: &ProjectId,
        _table: &TableName,
    ) -> Result<usize> {
        Ok(0)
    }
    /// Projects the shard has resident state for. Used by the CV
    /// refresher (which only refreshes CVs whose project is currently
    /// loaded) and any future per-project background driver. Default
    /// impl returns the empty set so a backend that doesn't carry
    /// per-project residency state opts out gracefully.
    async fn resident_projects(&self) -> Vec<ProjectId> {
        Vec::new()
    }
    /// Phase 5.14.D2: register the top-pattern provider. Default no-op.
    fn set_top_pattern_provider(&self, _provider: Arc<dyn TopPatternProvider>) {}
    /// Wire the GIN row-group registry (populated at INSERT time by the engine
    /// and read at query time for `@>` prune decisions) into the compactor so
    /// newly compacted files are also indexed.  Default no-op.
    fn set_gin_rowgroup_registry(
        &self,
        _registry: Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    ) {
    }
    /// Phase 6.X.C — voluntary lease handoff. Default returns Ok (a backend
    /// that doesn't model leases has nothing to hand off).
    async fn yield_partition(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _to_holder: &str,
    ) -> Result<()> {
        Ok(())
    }
    /// Drop all in-memory tail and cached schema entries for
    /// `(project, table)` across every resident partition. Called from
    /// `exec_drop_table` so a freshly-opened session can't read rows that
    /// were inserted into the dropped table but never flushed past the
    /// shard's tail (sec_catalog_leakage P1). Default no-op for backends
    /// that don't keep an in-memory tail.
    async fn drop_table(&self, _project: &ProjectId, _table: &TableName) -> Result<()> {
        Ok(())
    }
    /// Test-only downcast for the inline test suite.
    #[cfg(test)]
    fn as_in_process(&self) -> Option<Arc<in_process::InProcessShard>> {
        None
    }
}

#[async_trait]
pub(crate) trait ProjectHandleImpl: Send + Sync {
    async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()>;
    async fn read(
        &self,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>>;
    fn last_active(&self) -> Instant;
    fn project(&self) -> ProjectId;
}

/// Read a `u64` seconds value from an env var, falling back to `default` when
/// unset or unparseable. Used for the lease TTL / renew cadence knobs.
fn env_secs(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Per-process default lease holder id: `host:pid` plus a short random suffix
/// so two processes on the same host with the same pid (after recycle) don't
/// collide. Only used when a lease registry is attached.
fn default_replica_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_string());
    let pid = std::process::id();
    // Nanosecond start salt disambiguates a pid recycled on the same host.
    let salt = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{host}:{pid}:{salt}")
}

mod follower;
mod in_process;
pub mod lock_registry;
pub mod lock_wait;
pub mod split;

pub use follower::{
    promote, FollowerConfig, FollowerShard, FollowerStats, LagTier, ReplicaRole, ShardFollower,
};
pub use lock_registry::{LockEntry, LockHandle, LockRegistry};
pub use split::{CatchupReport, Epoch, LocalShardSplitter, ShardSplitter, SplitPlan};
