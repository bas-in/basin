//! `basin-shard` — stateful shard owner.
//!
//! ## Status
//!
//! v0.1: **in-process map** of `(tenant_id, partition_key) → in-memory state`,
//! with lazy load from WAL + Parquet, idle eviction, and a background
//! compactor that drains WAL segments into Parquet via the storage layer
//! and commits via the catalog.
//!
//! v0.2 (deferred): a placement service that spreads tenants across many
//! shard-owner processes. Today everything runs in-process; the API is
//! shaped so that swap is a backend change.
//!
//! ## Why this exists
//!
//! Two dashboard cards fail honestly today:
//! - `scaling_concurrency`: 3.4× speedup at 64 readers (bar 4×). Single
//!   process saturates one runtime.
//! - `scaling_noisy_neighbor`: 42× p99 degradation under a noisy tenant.
//!
//! Shard owners give each tenant their own slice of in-memory state plus
//! eviction; the noisy tenant's full scans no longer block the quiet
//! tenant's point queries because they live in different per-tenant
//! data structures and run on separate task groups.
//!
//! ## Public API
//!
//! - [`Shard`]: cheap-to-clone handle.
//! - [`ShardConfig`]: storage + catalog + WAL + eviction knobs.
//! - [`TenantHandle`]: lazy-loaded per-tenant state — write/read.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_common::{PartitionKey, Result, TableName, TenantId};

/// Knobs for [`Shard::new`].
#[derive(Clone)]
pub struct ShardConfig {
    pub storage: basin_storage::Storage,
    pub catalog: Arc<dyn basin_catalog::Catalog>,
    pub wal: basin_wal::Wal,
    /// Tenants idle for at least this long are evicted by the eviction loop.
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
        wal: basin_wal::Wal,
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
    pub resident_tenants: usize,
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

    /// Get a handle to `(tenant, partition)`. Lazy-loads the state from WAL +
    /// Parquet on first access; subsequent calls return a cheap clone.
    pub async fn get(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Result<TenantHandle> {
        self.inner.get(tenant, partition).await
    }

    /// Spawn the eviction + compaction background loops. Returns a handle
    /// that, when dropped, signals shutdown. Call this once at server boot.
    pub fn spawn_background(&self) -> ShardBackgroundHandle {
        self.inner.clone_arc().spawn_background()
    }

    pub fn stats(&self) -> ShardStats {
        self.inner.stats()
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

/// Per-tenant state handle. Holding one keeps the tenant resident.
#[derive(Clone)]
pub struct TenantHandle {
    pub(crate) inner: Arc<dyn TenantHandleImpl>,
}

impl TenantHandle {
    /// Append a batch into this tenant's table. Durable when this returns
    /// (WAL committed). The batch is ack'd before it has reached Parquet.
    pub async fn write_batch(
        &self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<()> {
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

    pub fn tenant(&self) -> TenantId {
        self.inner.tenant()
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

#[async_trait]
pub(crate) trait ShardImpl: Send + Sync {
    async fn get(&self, tenant: &TenantId, partition: &PartitionKey) -> Result<TenantHandle>;
    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle;
    fn stats(&self) -> ShardStats;
    fn clone_arc(&self) -> Arc<dyn ShardImpl>;
}

#[async_trait]
pub(crate) trait TenantHandleImpl: Send + Sync {
    async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()>;
    async fn read(
        &self,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>>;
    fn last_active(&self) -> Instant;
    fn tenant(&self) -> TenantId;
}

mod in_process;
