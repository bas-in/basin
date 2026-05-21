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
        }
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
    async fn run_tiering_sweep(&self) -> Result<()>;
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
pub mod split;

pub use follower::{
    promote, FollowerConfig, FollowerShard, FollowerStats, LagTier, ReplicaRole, ShardFollower,
};
pub use split::{CatchupReport, Epoch, LocalShardSplitter, ShardSplitter, SplitPlan};
