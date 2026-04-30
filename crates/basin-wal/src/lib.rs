//! `basin-wal` — write-ahead log for the Basin shard owner.
//!
//! ## Status
//!
//! v0.1: **single-node, file-backed**. Per-tenant + per-partition append-only
//! log. Background batched flush to object storage every 200 ms or 1 MB,
//! whichever first. Recovery on startup replays from object storage.
//!
//! v0.2 (deferred): Raft replication via `openraft`. The public API of this
//! crate is shaped so that swap is a *backend change*, not a rewrite of the
//! callers (the shard owner). See [ADR 0001](../../docs/decisions/0001-single-region-only.md)
//! for the deferral rationale.
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
//! - [`Wal`]: handle, cheap to clone (Arc inside).
//! - [`WalConfig`]: object store + flush knobs.
//! - [`WalEntry`], [`Lsn`]: data shapes.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{PartitionKey, Result, TenantId};
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

/// Knobs for [`Wal::open`].
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

/// The WAL handle. Cheap to clone; share across the shard owner's tasks.
#[derive(Clone)]
pub struct Wal {
    inner: Arc<dyn WalImpl>,
}

impl Wal {
    /// Open a WAL against the configured object store. Replays any persisted
    /// segments to recover in-memory state (per-(tenant, partition) LSN).
    pub async fn open(cfg: WalConfig) -> Result<Self> {
        let inner = file_wal::FileWal::open(cfg).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Append `payload` for `(tenant, partition)`. Durable when this returns.
    /// "Durable" today = local file fsync + queued for object-storage flush;
    /// once Raft lands (v0.2) it'll be quorum-ack.
    pub async fn append(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        self.inner.append(tenant, partition, payload).await
    }

    /// Force a synchronous flush of any queued segments to object storage.
    /// Tests and graceful shutdown use this; the hot path doesn't need it.
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// Return all entries for `(tenant, partition)` with LSN strictly greater
    /// than `since_lsn`, in append order. Used by the compactor and by shard
    /// owner cold-start replay.
    pub async fn read_from(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        self.inner.read_from(tenant, partition, since_lsn).await
    }

    /// Latest LSN for `(tenant, partition)`. `Lsn::ZERO` means no entries.
    pub async fn high_water(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Result<Lsn> {
        self.inner.high_water(tenant, partition).await
    }

    /// Drop all entries for `(tenant, partition)` whose LSN is strictly
    /// less than or equal to `up_to`. Called by the compactor after the
    /// segment has been merged into Parquet and committed to the catalog.
    pub async fn truncate(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        self.inner.truncate(tenant, partition, up_to).await
    }

    /// Stop the background flush task and drain. Idempotent.
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal").finish_non_exhaustive()
    }
}

/// Backend trait. The file-backed implementation is the only one today; v0.2
/// adds a Raft-backed implementation behind the same trait.
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
mod segment;
mod state;
