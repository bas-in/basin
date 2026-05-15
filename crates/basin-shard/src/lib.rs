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
use basin_common::{PartitionKey, Result, TableName, ProjectId};

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
        }
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
    pub async fn get(&self, project: &ProjectId, partition: &PartitionKey) -> Result<ProjectHandle> {
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

mod follower;
mod in_process;
pub mod split;

pub use follower::{
    promote, FollowerConfig, FollowerShard, FollowerStats, LagTier, ReplicaRole, ShardFollower,
};
pub use split::{CatchupReport, Epoch, LocalShardSplitter, ShardSplitter, SplitPlan};
