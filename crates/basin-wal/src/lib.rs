//! `basin-wal` — write-ahead log for the Basin shard owner.
//!
//! ## Status
//!
//! v0.1: **single-node, file-backed** ([`LocalWal`]). Per-tenant +
//! per-partition append-only log. Background batched flush to object storage
//! every 200 ms or 1 MB, whichever first. Recovery on startup replays from
//! object storage.
//!
//! v0.2 (scaffolded): Raft replication via [`RaftWal`]. Trait + stub ship
//! today; real raft integration is gated on a library-choice ADR. See
//! [`RAFT.md`](../RAFT.md) for the integration plan.
//!
//! ## Why this exists
//!
//! Today's engine path is synchronous: `Arrow → Parquet → ZSTD → object_store
//! → catalog commit`. That bottleneck shows up on the dashboard as the
//! `compare_postgres` insert row, where Basin is ~3.8× behind PG. Once the
//! WAL is wired in, `Engine::execute` for INSERT becomes:
//!
//! 1. Append the batch to the WAL — durable in low-millisecond range.
//! 2. Ack the client.
//! 3. Background compactor drains WAL → Parquet → catalog later.
//!
//! The WAL becomes the durability boundary; S3 is for long-term storage and
//! analytics. **This is the rule**: never put S3 on the hot path.
//!
//! ## Public API
//!
//! - [`Wal`]: object-safe trait. Hold as `Arc<dyn Wal>`.
//! - [`LocalWal`]: single-node file-backed implementation.
//! - [`RaftWal`]: multi-node Raft-backed stub (returns
//!   `BasinError::FeatureNotSupported` until the integration PR lands).
//! - [`WalConfig`]: object store + flush knobs for [`LocalWal`].
//! - [`RaftWalConfig`]: peer + timing knobs for [`RaftWal`].
//! - [`WalEntry`], [`Lsn`]: data shapes shared by all impls.

#![forbid(unsafe_code)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{PartitionKey, Result, TenantCounterRegistry, TenantId};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

/// Monotonically-increasing log sequence number, scoped to a single
/// `(tenant_id, partition)`. Stable across process restarts (recovered from
/// object storage). Used by callers to track replication progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);
    pub fn next(self) -> Lsn {
        Lsn(self.0 + 1)
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A single appended record.
///
/// `payload` is opaque to the WAL — typically an Arrow IPC-serialised
/// `RecordBatch` from the shard owner. The WAL never inspects it.
#[derive(Clone, Debug)]
pub struct WalEntry {
    pub tenant: TenantId,
    pub partition: PartitionKey,
    pub lsn: Lsn,
    pub payload: Bytes,
    pub appended_at: chrono::DateTime<chrono::Utc>,
}

/// Knobs for [`LocalWal::open`].
#[derive(Clone)]
pub struct WalConfig {
    pub object_store: Arc<dyn ObjectStore>,
    /// Optional bucket sub-prefix. WAL segments live under
    /// `{root}/wal/{tenant}/{partition}/{ulid}.seg`.
    pub root_prefix: Option<ObjectPath>,
    /// Flush after this many milliseconds of accumulated writes.
    pub flush_interval: Duration,
    /// Flush after this many bytes accumulate, even if the timer hasn't fired.
    pub flush_max_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            object_store: panic_no_default_object_store(),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn panic_no_default_object_store() -> Arc<dyn ObjectStore> {
    panic!("WalConfig::default() requires `.object_store` to be set explicitly");
}

/// Object-safe public trait. Callers (shard owner, server, integration tests)
/// hold `Arc<dyn Wal>` so the same call sites work for [`LocalWal`] today and
/// [`RaftWal`] once the raft integration lands.
///
/// Designed to accept any reasonable real raft implementation:
/// `append` returns once durability is achieved (single-node: fsync; raft:
/// quorum ack); `read_from` covers catchup / recovery; `truncate` is called
/// by the compactor after Parquet commit.
#[async_trait]
pub trait Wal: Send + Sync + std::fmt::Debug {
    /// Append `payload` for `(tenant, partition)`. Durable when this returns.
    async fn append(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn>;

    /// Force a synchronous flush of any queued segments to durable storage.
    /// For raft this is usually a no-op (already fsync'd on quorum); for
    /// the local WAL it forces an upload of buffered entries.
    async fn flush(&self) -> Result<()>;

    /// Return all entries for `(tenant, partition)` with LSN strictly greater
    /// than `since_lsn`, in append order. Used by the compactor and by shard
    /// owner cold-start replay.
    async fn read_from(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>>;

    /// Latest LSN for `(tenant, partition)`. `Lsn::ZERO` means no entries.
    async fn high_water(&self, tenant: &TenantId, partition: &PartitionKey) -> Result<Lsn>;

    /// Drop all entries for `(tenant, partition)` whose LSN is strictly
    /// less than or equal to `up_to`. Called by the compactor after the
    /// segment has been merged into Parquet and committed to the catalog.
    async fn truncate(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()>;

    /// Stop background tasks and drain. Idempotent.
    async fn close(&self) -> Result<()>;

    /// Attach a per-tenant counter registry so the WAL append path bumps the
    /// same per-tenant rollup as engine-side ops and storage-side bytes.
    /// Default impl is a no-op for backends that don't track byte counters
    /// (e.g. the [`RaftWal`] stub).
    fn attach_tenant_counters(&self, _registry: Arc<TenantCounterRegistry>) {}
}

/// Single-node, file-backed WAL. Cheap to clone (`Arc` inside); share across
/// the shard owner's tasks.
///
/// "Durable" today = local file fsync + queued for object-storage flush;
/// once Raft lands (v0.2) the [`RaftWal`] type provides quorum-ack
/// durability behind the same [`Wal`] trait.
#[derive(Clone)]
pub struct LocalWal {
    inner: Arc<dyn WalImpl>,
    /// Optional per-tenant counter registry (Phase 6 telemetry). Attached by
    /// the engine so the WAL append path bumps the same per-tenant rollup as
    /// engine-side ops and storage-side bytes.
    tenant_counters: Arc<OnceLock<Arc<TenantCounterRegistry>>>,
}

impl LocalWal {
    /// Open a WAL against the configured object store. Replays any persisted
    /// segments to recover in-memory state (per-(tenant, partition) LSN).
    pub async fn open(cfg: WalConfig) -> Result<Self> {
        let inner = file_wal::FileWal::open(cfg).await?;
        Ok(Self {
            inner: Arc::new(inner),
            tenant_counters: Arc::new(OnceLock::new()),
        })
    }
}

#[async_trait]
impl Wal for LocalWal {
    async fn append(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        let bytes = payload.len() as u64;
        let lsn = self.inner.append(tenant, partition, payload).await?;
        // Per-tenant WAL append counters (Phase 6 telemetry). One op per
        // append; bytes_written grows by payload size. No-op without registry.
        if let Some(reg) = self.tenant_counters.get() {
            let tc = reg.for_tenant(tenant);
            tc.record_op();
            tc.record_bytes_written(bytes);
        }
        Ok(lsn)
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn read_from(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        self.inner.read_from(tenant, partition, since_lsn).await
    }

    async fn high_water(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Result<Lsn> {
        self.inner.high_water(tenant, partition).await
    }

    async fn truncate(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        self.inner.truncate(tenant, partition, up_to).await
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    fn attach_tenant_counters(&self, registry: Arc<TenantCounterRegistry>) {
        // Idempotent (first attach wins).
        let _ = self.tenant_counters.set(registry);
    }
}

impl std::fmt::Debug for LocalWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalWal").finish_non_exhaustive()
    }
}

/// Backend trait. The file-backed implementation is the only one today; v0.2
/// adds a Raft-backed implementation via the public [`Wal`] trait directly.
#[async_trait]
pub(crate) trait WalImpl: Send + Sync {
    async fn append(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn>;
    async fn flush(&self) -> Result<()>;
    async fn read_from(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>>;
    async fn high_water(&self, tenant: &TenantId, partition: &PartitionKey) -> Result<Lsn>;
    async fn truncate(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()>;
    async fn close(&self) -> Result<()>;
}

mod file_wal;
mod raft_wal;
mod segment;
mod state;

pub use raft_wal::{BasinRaftRequest, BasinRaftResponse, RaftWal, RaftWalConfig, SimCluster};
