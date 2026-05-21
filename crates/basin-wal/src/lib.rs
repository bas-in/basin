//! `basin-wal` — write-ahead log for the Basin shard owner.
//!
//! ## Status
//!
//! v0.1: **single-node, file-backed** ([`LocalWal`]). Per-project +
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
//! - [`WalEntry`], [`WalEvent`], [`Lsn`]: data shapes shared by all impls.
//! - [`replay_wal`] + [`WalReplayConfig`]: transaction-aware replay (Phase
//!   5.14.C2 and later).

#![forbid(unsafe_code)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{PartitionKey, ProjectCounterRegistry, ProjectId, Result};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

/// Monotonically-increasing log sequence number, scoped to a single
/// `(project_id, partition)`. Stable across process restarts (recovered from
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

/// A single appended data record.
///
/// `payload` is opaque to the WAL — typically an Arrow IPC-serialised
/// `RecordBatch` from the shard owner. The WAL never inspects it.
///
/// Transaction marker records (`Begin`, `Rollback`) are represented separately
/// via [`WalEvent`] and are never exposed as `WalEntry` values through the
/// normal [`Wal::read_from`] path.
#[derive(Clone, Debug)]
pub struct WalEntry {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub lsn: Lsn,
    pub payload: Bytes,
    pub appended_at: chrono::DateTime<chrono::Utc>,
}

/// A WAL event as seen during replay. Extends the plain [`WalEntry`] data
/// record with transaction lifecycle markers.
///
/// The normal `Wal::read_from` path returns `Vec<WalEntry>` (data only).
/// Use [`Wal::read_events`] and [`replay_wal`] when transaction-aware replay
/// is required (Phase 5.14.C2 and later).
#[derive(Clone, Debug)]
pub enum WalEvent {
    /// A normal data record.
    Entry(WalEntry),
    /// Marks the start of a logical transaction with the given id.
    Begin { tx_id: u64 },
    /// Marks that all buffered entries for `tx_id` should be discarded.
    Rollback { tx_id: u64 },
}

/// Configuration for [`replay_wal`].
///
/// `suppress_rolled_back` (default `true`) activates transaction-aware replay:
/// entries enclosed by a `Begin`/`Rollback` pair are discarded. Set to `false`
/// for v0.1-format WAL files that contain no transaction markers (all entries
/// are emitted verbatim in that case anyway, but the flag makes the intent
/// explicit).
#[derive(Clone, Debug)]
pub struct WalReplayConfig {
    /// Suppress entries belonging to explicitly rolled-back transactions.
    ///
    /// When `true` (the default) a `Begin { tx_id }` / `Rollback { tx_id }`
    /// pair causes all buffered entries for that `tx_id` to be discarded.
    /// When `false` all entries are emitted regardless of transaction markers
    /// (v0.1-compat mode).
    pub suppress_rolled_back: bool,
}

impl Default for WalReplayConfig {
    fn default() -> Self {
        Self {
            suppress_rolled_back: true,
        }
    }
}

/// Replay a sequence of [`WalEvent`]s, suppressing entries that belong to
/// explicitly rolled-back transactions.
///
/// ## Behaviour
///
/// - Events with no surrounding `Begin`/`Rollback` markers are emitted
///   verbatim (back-compat: pre-marker WAL files produce identical output).
/// - A `Begin { tx_id }` starts collecting entries tagged with `tx_id`.
/// - A `Rollback { tx_id }` discards all collected entries for that `tx_id`.
/// - End-of-input with no matching `Rollback` is an **implicit commit**: all
///   buffered entries for the open transaction are emitted in LSN order.
/// - Multiple concurrent transactions (interleaved `Begin` markers) are
///   tracked independently.
///
/// ## `WalReplayConfig::suppress_rolled_back = false`
///
/// Disables the suppression logic. Every `Entry` event in `events` is emitted
/// regardless of surrounding markers. This is the v0.1-compat mode.
pub fn replay_wal(events: Vec<WalEvent>, config: &WalReplayConfig) -> Vec<WalEntry> {
    if !config.suppress_rolled_back {
        // Fast path: emit all data entries, ignore markers.
        return events
            .into_iter()
            .filter_map(|ev| match ev {
                WalEvent::Entry(e) => Some(e),
                WalEvent::Begin { .. } | WalEvent::Rollback { .. } => None,
            })
            .collect();
    }

    // tx_id -> buffered entries collected since the matching Begin.
    let mut open_txs: std::collections::HashMap<u64, Vec<WalEntry>> =
        std::collections::HashMap::new();
    // Entries not inside any open transaction (or from implicit-commit tx).
    let mut committed: Vec<WalEntry> = Vec::new();
    // Track which tx_id is "current" for the most recent Begin, so that
    // interleaved entries are routed correctly. An entry that arrives while
    // any transaction is open is attributed to the *most recently opened*
    // transaction whose Begin has not yet been resolved.
    //
    // Ordering: we track open tx_ids in insertion order.
    let mut open_tx_order: Vec<u64> = Vec::new();

    for event in events {
        match event {
            WalEvent::Begin { tx_id } => {
                open_txs.entry(tx_id).or_default();
                // Track insertion order (do not duplicate if already present).
                if !open_tx_order.contains(&tx_id) {
                    open_tx_order.push(tx_id);
                }
            }
            WalEvent::Rollback { tx_id } => {
                open_txs.remove(&tx_id);
                open_tx_order.retain(|id| *id != tx_id);
            }
            WalEvent::Entry(entry) => {
                if let Some(&current_tx) = open_tx_order.last() {
                    // Inside an open transaction — buffer against it.
                    open_txs.entry(current_tx).or_default().push(entry);
                } else {
                    // No open transaction — implicit commit immediately.
                    committed.push(entry);
                }
            }
        }
    }

    // End-of-input: implicit commit for any still-open transactions.
    // Emit in the order the transactions were opened.
    for tx_id in open_tx_order {
        if let Some(mut buffered) = open_txs.remove(&tx_id) {
            committed.append(&mut buffered);
        }
    }

    // Final LSN sort preserves append order regardless of interleaving.
    committed.sort_by_key(|e| e.lsn);
    committed
}

/// Knobs for [`LocalWal::open`].
#[derive(Clone)]
pub struct WalConfig {
    pub object_store: Arc<dyn ObjectStore>,
    /// Optional bucket sub-prefix. WAL segments live under
    /// `{root}/wal/{project}/{partition}/{ulid}.seg`.
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
    /// Append `payload` for `(project, partition)`. Durable when this returns.
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn>;

    /// Phase 6.X.A — epoch-fenced append (ADR 0023).
    ///
    /// Identical to [`Self::append`] but carries the holder's current lease
    /// `epoch`. If a stale epoch appends (a dual-leaseholder after a network
    /// partition — the loser presents a lower epoch than the winner already
    /// raised the fence to), the append is **rejected** with a
    /// [`basin_common::BasinError::Wal`] fencing error and no LSN is consumed.
    ///
    /// Back-compat: `epoch == None` (or `Some(0)`) is the no-lease /
    /// single-replica mode — it never raises the partition's fence and always
    /// appends, exactly as [`Self::append`] does today. The default
    /// implementation ignores the epoch and delegates to [`Self::append`], so
    /// non-fencing backends (e.g. the [`RaftWal`] stub) stay buildable.
    async fn append_fenced(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
    ) -> Result<Lsn> {
        let _ = epoch;
        self.append(project, partition, payload).await
    }

    /// Force a synchronous flush of any queued segments to durable storage.
    /// For raft this is usually a no-op (already fsync'd on quorum); for
    /// the local WAL it forces an upload of buffered entries.
    async fn flush(&self) -> Result<()>;

    /// Return all entries for `(project, partition)` with LSN strictly greater
    /// than `since_lsn`, in append order. Used by the compactor and by shard
    /// owner cold-start replay.
    async fn read_from(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>>;

    /// Latest LSN for `(project, partition)`. `Lsn::ZERO` means no entries.
    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn>;

    /// Drop all entries for `(project, partition)` whose LSN is strictly
    /// less than or equal to `up_to`. Called by the compactor after the
    /// segment has been merged into Parquet and committed to the catalog.
    async fn truncate(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()>;

    /// Stop background tasks and drain. Idempotent.
    async fn close(&self) -> Result<()>;

    /// Append a `BEGIN` transaction marker for `tx_id` to the log for
    /// `(project, partition)`. Returns the LSN assigned to the marker.
    ///
    /// Default implementation returns
    /// [`basin_common::BasinError::FeatureNotSupported`] — concrete backends
    /// override this (e.g. [`LocalWal`]).
    async fn append_tx_begin(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _tx_id: u64,
    ) -> Result<Lsn> {
        Err(basin_common::BasinError::feature_not_supported(
            "append_tx_begin",
        ))
    }

    /// Append a `ROLLBACK` transaction marker for `tx_id` to the log for
    /// `(project, partition)`. Returns the LSN assigned to the marker.
    ///
    /// Default implementation returns
    /// [`basin_common::BasinError::FeatureNotSupported`] — concrete backends
    /// override this (e.g. [`LocalWal`]).
    async fn append_tx_rollback(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _tx_id: u64,
    ) -> Result<Lsn> {
        Err(basin_common::BasinError::feature_not_supported(
            "append_tx_rollback",
        ))
    }

    /// Return all events (data entries **and** transaction markers) for
    /// `(project, partition)` with LSN strictly greater than `since_lsn`, in
    /// append order.
    ///
    /// Use this in combination with [`replay_wal`] when transaction-aware
    /// replay is required. For plain data reads prefer [`Self::read_from`].
    ///
    /// Default implementation returns
    /// [`basin_common::BasinError::FeatureNotSupported`] — concrete backends
    /// override this (e.g. [`LocalWal`]).
    async fn read_events(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _since_lsn: Lsn,
    ) -> Result<Vec<WalEvent>> {
        Err(basin_common::BasinError::feature_not_supported(
            "read_events",
        ))
    }

    /// Attach a per-project counter registry so the WAL append path bumps the
    /// same per-project rollup as engine-side ops and storage-side bytes.
    /// Default impl is a no-op for backends that don't track byte counters
    /// (e.g. the [`RaftWal`] stub).
    fn attach_project_counters(&self, _registry: Arc<ProjectCounterRegistry>) {}
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
    /// Optional per-project counter registry (Phase 6 telemetry). Attached by
    /// the engine so the WAL append path bumps the same per-project rollup as
    /// engine-side ops and storage-side bytes.
    project_counters: Arc<OnceLock<Arc<ProjectCounterRegistry>>>,
}

impl LocalWal {
    /// Open a WAL against the configured object store. Replays any persisted
    /// segments to recover in-memory state (per-(project, partition) LSN).
    pub async fn open(cfg: WalConfig) -> Result<Self> {
        let inner = file_wal::FileWal::open(cfg).await?;
        Ok(Self {
            inner: Arc::new(inner),
            project_counters: Arc::new(OnceLock::new()),
        })
    }
}

#[async_trait]
impl Wal for LocalWal {
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        let bytes = payload.len() as u64;
        let lsn = self.inner.append(project, partition, payload).await?;
        // Per-project WAL append counters (Phase 6 telemetry). One op per
        // append; bytes_written grows by payload size. No-op without registry.
        if let Some(reg) = self.project_counters.get() {
            let tc = reg.for_project(project);
            tc.record_op();
            tc.record_bytes_written(bytes);
        }
        Ok(lsn)
    }

    async fn append_fenced(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
    ) -> Result<Lsn> {
        let bytes = payload.len() as u64;
        let lsn = self
            .inner
            .append_fenced(project, partition, payload, epoch)
            .await?;
        // Mirror the counter bookkeeping of the unfenced path; a rejected
        // (fenced-out) append returns Err above and never reaches here, so
        // stale-epoch writes are correctly excluded from the per-project rollup.
        if let Some(reg) = self.project_counters.get() {
            let tc = reg.for_project(project);
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
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        self.inner.read_from(project, partition, since_lsn).await
    }

    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn> {
        self.inner.high_water(project, partition).await
    }

    async fn truncate(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        self.inner.truncate(project, partition, up_to).await
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn append_tx_begin(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn> {
        self.inner.append_tx_begin(project, partition, tx_id).await
    }

    async fn append_tx_rollback(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn> {
        self.inner
            .append_tx_rollback(project, partition, tx_id)
            .await
    }

    async fn read_events(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEvent>> {
        self.inner.read_events(project, partition, since_lsn).await
    }

    fn attach_project_counters(&self, registry: Arc<ProjectCounterRegistry>) {
        // Idempotent (first attach wins).
        let _ = self.project_counters.set(registry);
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
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn>;
    /// Phase 6.X.A — epoch-fenced append. Default delegates to [`Self::append`]
    /// (no fencing) so backends that don't track lease epochs stay buildable.
    async fn append_fenced(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
    ) -> Result<Lsn> {
        let _ = epoch;
        self.append(project, partition, payload).await
    }
    async fn flush(&self) -> Result<()>;
    async fn read_from(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>>;
    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn>;
    async fn truncate(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()>;
    async fn close(&self) -> Result<()>;
    async fn append_tx_begin(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn>;
    async fn append_tx_rollback(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn>;
    async fn read_events(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEvent>>;
}

mod file_wal;
mod raft_wal;
mod segment;
mod state;

pub use raft_wal::{BasinRaftRequest, BasinRaftResponse, RaftWal, RaftWalConfig, SimCluster};
