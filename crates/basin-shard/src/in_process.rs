//! In-process shard map.
//!
//! ## WAL payload encoding
//!
//! Every WAL entry the shard owner appends has this layout:
//!
//! ```text
//! +----------------+--------------------+--------------------+
//! | u32 LE: tlen   | tlen bytes: table  | rest: Arrow IPC    |
//! +----------------+--------------------+--------------------+
//! ```
//!
//! - `tlen` is the table-name length in bytes (validated to fit `MAX_IDENT_LEN`).
//! - `table` is the UTF-8 table name, identical to `TableName::as_str()`.
//! - The remainder is one Arrow IPC stream (header + a single `RecordBatch`),
//!   produced by `arrow_ipc::writer::StreamWriter` and decoded by
//!   `arrow_ipc::reader::StreamReader`.
//!
//! The shard owner is the only entity that writes WAL payloads, so we control
//! both ends of this codec; there is no on-the-wire compatibility concern.
//!
//! ## Concurrency model
//!
//! - One outer `Mutex<HashMap<(ProjectId, PartitionKey), Arc<RwLock<PartitionState>>>>`.
//!   Held only long enough to look up or insert the per-partition entry; never
//!   held across I/O or across awaits on the inner lock.
//! - Each `PartitionState` lives behind its own `tokio::sync::RwLock`. Writes
//!   acquire the write side briefly to push onto the tail; reads acquire the
//!   read side to clone the tail.
//! - Background loops snapshot the project map under the outer lock, drop it,
//!   then process each partition independently. Compaction acquires the
//!   per-partition write lock only at the start (to snapshot what to drain)
//!   and at the end (to remove drained entries from the tail). Object-store
//!   I/O happens with no Basin lock held.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use basin_catalog::DataFileRef;
use basin_catalog::SnapshotId;
use basin_common::{BasinError, PartitionKey, ProjectId, Result, TableName};
use basin_storage::ReadOptions;
use basin_wal::Lsn;
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, instrument, warn};

use crate::{
    LeaseMode, ProjectHandle, ProjectHandleImpl, ShardBackgroundHandle, ShardConfig, ShardImpl,
    ShardStats, TailPressure, TopPatternProvider,
};

/// Process-global one-shot claim for the Phase 2 autotune controller: the
/// first background loop to call this wins (`true`); every later caller gets
/// `false`. The runtime fan-out / flush atomics are process-global, so exactly
/// one controller must drive them even when a node hosts several shards (and
/// in the test suite, where many `spawn_background` calls share one process).
fn autotune_claim() -> bool {
    static CLAIMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    CLAIMED
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        )
        .is_ok()
}

/// Cheap rolling ingest-rate counter (rows/sec) feeding the autoscaler's
/// `ingest_rows_per_sec` metric. Lock-free: two atomics tracking the current
/// 1-second bucket and the last fully-elapsed second's total.
///
/// On every `write_batch` we add the batch row count to the current bucket. The
/// bucket is keyed by a monotonic "seconds since process start" stamp; when a
/// write (or a read) observes a newer second, the just-closed bucket's count is
/// promoted to `last_full` and the new bucket starts at 0. `rows_per_sec()`
/// reports `last_full` (the most recent COMPLETE second) so a reader never sees
/// a partially-filled current bucket undercount the true rate. This is an
/// honest, cheaply-maintained signal — not a smoothed EWMA — which is all the
/// autoscaler needs to see "this node is ingesting hard right now".
#[derive(Debug)]
struct IngestRate {
    start: Instant,
    /// Second-stamp (seconds since `start`) the current bucket belongs to.
    bucket_sec: std::sync::atomic::AtomicU64,
    /// Rows accumulated in the current (open) one-second bucket.
    bucket_rows: std::sync::atomic::AtomicU64,
    /// Rows in the most recently CLOSED one-second bucket.
    last_full: std::sync::atomic::AtomicU64,
}

impl IngestRate {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            bucket_sec: std::sync::atomic::AtomicU64::new(0),
            bucket_rows: std::sync::atomic::AtomicU64::new(0),
            last_full: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Roll the bucket forward if the wall has advanced past it, promoting the
    /// closed bucket to `last_full`. Called on both write (record) and read
    /// (rate) so a quiet node correctly reports 0 once a second elapses with no
    /// writes (rather than reporting a stale rate forever).
    fn roll(&self, now_sec: u64) {
        use std::sync::atomic::Ordering;
        let cur = self.bucket_sec.load(Ordering::Relaxed);
        if now_sec > cur
            && self
                .bucket_sec
                .compare_exchange(cur, now_sec, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            // We won the roll: the bucket we just closed is `last_full` only if
            // it was the IMMEDIATELY preceding second; a gap (idle node) means
            // the last full second saw zero writes.
            let closed = self.bucket_rows.swap(0, Ordering::Relaxed);
            let promoted = if now_sec == cur + 1 { closed } else { 0 };
            self.last_full.store(promoted, Ordering::Relaxed);
        }
    }

    fn record(&self, rows: u64) {
        use std::sync::atomic::Ordering;
        let now_sec = self.start.elapsed().as_secs();
        self.roll(now_sec);
        self.bucket_rows.fetch_add(rows, Ordering::Relaxed);
    }

    fn rows_per_sec(&self) -> u64 {
        self.roll(self.start.elapsed().as_secs());
        self.last_full.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test-only: pin the reported rate so the quiesce-drain idle gate can be
    /// exercised deterministically (the wall-second bucketing is otherwise
    /// timing-fragile to drive from a test). Sets the current bucket's stamp to
    /// the live second so the next `roll` does not immediately clobber the
    /// forced `last_full` back to 0.
    #[cfg(test)]
    fn force_rate_for_test(&self, rows_per_sec: u64) {
        use std::sync::atomic::Ordering;
        self.bucket_sec
            .store(self.start.elapsed().as_secs(), Ordering::Relaxed);
        self.last_full.store(rows_per_sec, Ordering::Relaxed);
    }
}

/// Whole-shard soft-cap budget for resident (uncompacted) tail bytes, BEFORE
/// the per-partition division below. When a partition's tail crosses its share
/// of this, `write_batch_inner` logs a pressure warning (observability). The
/// HARD backpressure cap is what actually bounds memory. 256 MiB leaves ample
/// headroom on the 8 GB box for query execution + caches.
const MAX_TAIL_BYTES_TOTAL: usize = 256 * 1024 * 1024;

/// Whole-shard HARD-cap budget for resident tail bytes, BEFORE the
/// per-partition division. When a partition's tail reaches its share of this,
/// `write_batch_inner` synchronously flushes its oldest tail batches to an
/// immutable file BEFORE returning, so RAM stays bounded under sustained fast
/// ingest. Real backpressure: a writer that outruns the time-ticked compactor
/// paces itself to flush (object-store) throughput instead of buffering the
/// whole table in memory and OOMing the box. Above the soft cap so the
/// soft-cap WARN fires first. The flush is bounded (see `MAX_COMPACTION_ROWS`),
/// so a single backpressure stall costs one bounded file write, never an
/// O(table) rewrite.
const HARD_TAIL_BYTES_TOTAL: usize = 384 * 1024 * 1024;

/// #28: per-partition memory budgeting. `BASIN_SHARD_PARTITIONS_PER_TABLE`
/// fans a table's bulk ingest across P partitions, each with its OWN tail. To
/// keep the WHOLE-shard resident tail bounded by the same budget rather than
/// P× it, the per-partition soft/hard caps are the total budgets divided by P.
/// Read from the same env the engine fan-out uses so the two stay consistent;
/// clamped to >= 1 and floored at a sane minimum so a tiny per-partition cap
/// can't thrash the compactor.
fn fanout_partitions() -> usize {
    std::env::var("BASIN_SHARD_PARTITIONS_PER_TABLE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(4)
}

/// Floor on the per-partition tail caps so dividing by a large P can't shrink
/// the cap below a useful flush size (one `MAX_COMPACTION_ROWS` chunk of a
/// narrow row is well under this).
const MIN_PER_PARTITION_TAIL_BYTES: usize = 32 * 1024 * 1024;

/// Per-partition soft cap = total soft budget / P, floored.
fn max_tail_bytes() -> usize {
    (MAX_TAIL_BYTES_TOTAL / fanout_partitions()).max(MIN_PER_PARTITION_TAIL_BYTES)
}

/// Per-partition hard cap = total hard budget / P, floored.
fn hard_tail_bytes() -> usize {
    (HARD_TAIL_BYTES_TOTAL / fanout_partitions()).max(MIN_PER_PARTITION_TAIL_BYTES)
}

/// Upper bound on the number of rows a single compaction flush folds into one
/// immutable data file. Compaction drains a partition's tail in chunks of at
/// most this many rows per output file, so the cost of one flush
/// (concat + encode + object-store PUT) is bounded by RECENT writes, not by how
/// large the tail has grown (or how large the table already is). The tail can
/// span many such files; reads prune across them by zone-map / manifest. This
/// is the wedge that keeps per-chunk ingest cost flat as the table scales to 1B
/// rows and beyond: a compaction tick that finds a huge backlog produces
/// several bounded files instead of one giant O(tail) re-encode.
const MAX_COMPACTION_ROWS: usize = 512 * 1024;

/// Number of bounded output files a single `compact_one` flushes CONCURRENTLY.
/// Against a high-RTT object store a single-file flush is latency-bound (one
/// PUT round-trip per file), so serial draining caps compaction throughput at
/// `MAX_COMPACTION_ROWS / PUT_RTT` — below sustained ingest, which lets the
/// in-memory tail fill to the soft cap and pace ingest down. Writing several
/// files at once makes compaction throughput scale with parallel PUT
/// BANDWIDTH instead of single-PUT latency, so the tail drains faster than it
/// fills and stays well below the soft cap without throttling the writer.
///
/// The data-file PUTs are further bounded by the storage layer's per-project
/// permit pool, so this only controls how many chunks `compact_one` prepares
/// and writes per wave. Catalog commits stay SERIAL (one CAS at a time per
/// table) so no two waves double-append the same snapshot. Env-overridable
/// via `BASIN_SHARD_FLUSH_CONCURRENCY`.
const DEFAULT_FLUSH_CONCURRENCY: usize = 8;

fn flush_concurrency() -> usize {
    if let Ok(v) = std::env::var("BASIN_SHARD_FLUSH_CONCURRENCY") {
        if let Some(n) = v.parse::<usize>().ok().filter(|n| *n > 0) {
            return n; // explicit env always wins
        }
    }
    // Phase 2: live override from the adaptive controller, if running.
    if let Some(n) = basin_common::autotune::runtime_flush_concurrency() {
        return n;
    }
    // Phase 1: hardware-derived default under BASIN_AUTOTUNE (no env override).
    // Off → historical default (no-op).
    if let Some(t) = basin_common::autotune::tuning() {
        return t.flush_concurrency;
    }
    DEFAULT_FLUSH_CONCURRENCY
}

/// Resolved per-table write configuration, cached for the lifetime of one
/// `compact_one` call so the compactor reads the table's catalog metadata once
/// per table instead of once per flushed file. Every field is table-level
/// config that is stable across a single compaction drain.
struct ResolvedTableCompactCfg {
    file_format: basin_storage::FileFormat,
    declared_cluster_cols: Vec<String>,
    adaptive_sort_override: bool,
    gin_indexes: Vec<basin_catalog::SecondaryIndex>,
    row_group_rows: Option<usize>,
    row_block_size: Option<u32>,
    promoted_paths: Vec<basin_catalog::PromotedJsonbPath>,
    pk_default_cluster_cols: Vec<String>,
    bloom_cols: Vec<String>,
}

impl Default for ResolvedTableCompactCfg {
    fn default() -> Self {
        Self {
            file_format: basin_storage::FileFormat::default(),
            declared_cluster_cols: Vec::new(),
            adaptive_sort_override: false,
            gin_indexes: Vec::new(),
            row_group_rows: None,
            row_block_size: None,
            promoted_paths: Vec::new(),
            pk_default_cluster_cols: Vec::new(),
            bloom_cols: Vec::new(),
        }
    }
}

/// Per-(project, partition) in-memory state. Lives behind an `RwLock` inside
/// the shard's outer map.
pub(crate) struct PartitionState {
    #[allow(dead_code)]
    project: ProjectId,
    #[allow(dead_code)]
    partition: PartitionKey,
    last_active: Instant,
    /// Highest LSN that has been compacted into Parquet/Vortex for this
    /// partition. `Lsn::ZERO` until the first successful compaction.
    ///
    /// On cold start this is seeded from the durable compaction watermark
    /// (`Catalog::get_compaction_watermark`), and `compact_one` persists the
    /// watermark to the catalog BEFORE truncating the WAL. Replay then starts
    /// strictly above the watermark, so a crash in the catalog-commit→WAL-
    /// truncate window cannot re-replay an already-committed batch into a
    /// duplicate cold file. A legacy catalog without a watermark replays from
    /// `Lsn::ZERO` (the prior behavior) — see `replay_wal_into`.
    last_compacted_lsn: Lsn,
    /// Schemas cached from the catalog the first time we touch a table.
    schemas: HashMap<TableName, Arc<Schema>>,
    /// In-memory tail keyed by table. Each `(lsn, batch)` pair tracks the WAL
    /// entry that produced it so the compactor knows what range to truncate.
    tail: HashMap<TableName, Vec<(Lsn, RecordBatch)>>,
    /// Per-partition compaction serialization lock (#95).
    ///
    /// `compact_one` snapshots the tail, writes a Parquet file, commits it to
    /// the catalog, then prunes the drained entries. Those steps are NOT atomic
    /// w.r.t. the tail: two concurrent `compact_one` calls (e.g. two pooled
    /// sessions' synchronous `flush_to_parquet()` before SELECT, or a session
    /// racing the background compactor) would each snapshot the SAME entries
    /// and each commit a file — duplicating every row in the cold tier and
    /// over-counting on read.
    ///
    /// Held for the whole `compact_one` body so a given partition compacts one
    /// at a time. A second caller blocks until the in-flight compaction
    /// finishes; it then snapshots the now-drained tail and does nothing —
    /// which is exactly the visibility guarantee the synchronous pre-SELECT
    /// flush needs (the racing compaction already made the rows durable).
    ///
    /// Stored as an `Arc` so it can be cloned out under a brief read lock and
    /// held across the compaction without holding the `PartitionState` RwLock.
    compact_lock: Arc<Mutex<()>>,
}

impl PartitionState {
    fn new(project: ProjectId, partition: PartitionKey) -> Self {
        Self {
            project,
            partition,
            last_active: Instant::now(),
            last_compacted_lsn: Lsn::ZERO,
            schemas: HashMap::new(),
            tail: HashMap::new(),
            compact_lock: Arc::new(Mutex::new(())),
        }
    }

    fn touch(&mut self) {
        self.last_active = Instant::now();
    }

    fn tail_is_empty(&self) -> bool {
        self.tail.values().all(|v| v.is_empty())
    }
}

type PartitionMap = HashMap<(ProjectId, PartitionKey), Arc<RwLock<PartitionState>>>;
type PartitionSnapshot = Vec<((ProjectId, PartitionKey), Arc<RwLock<PartitionState>>)>;

/// Leases this replica currently holds, keyed by `(project, partition)` with
/// the granted fencing epoch as the value. Shared with the heartbeat task so
/// it knows what to renew and at which epoch. Empty in no-lease mode.
type HeldLeases = Arc<Mutex<HashMap<(ProjectId, PartitionKey), i64>>>;

/// Phase 6.X.C — `(project, partition)` pairs whose lease is currently being
/// voluntarily handed off. New writes to a draining partition are rejected
/// with `BasinError::LeaseHandoffInProgress` so the router can retry on the
/// new owner once the handoff completes. Reads are unaffected.
type DrainingSet = Arc<Mutex<HashSet<(ProjectId, PartitionKey)>>>;

pub(crate) struct InProcessShard {
    pub(crate) cfg: ShardConfig,
    pub(crate) partitions: Arc<Mutex<PartitionMap>>,
    stats: Arc<Mutex<ShardStats>>,
    top_pattern_provider: std::sync::RwLock<Option<Arc<dyn TopPatternProvider>>>,
    /// GIN row-group bloom registry.  Written once from `Engine::new` via
    /// `Shard::set_gin_rowgroup_registry` (before the compaction loop
    /// starts); read by `compact_one` to re-index compacted files.
    /// `None` until the engine wires it in (safe: compactor skips indexing).
    gin_rowgroup_registry: std::sync::RwLock<
        Option<Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>>,
    >,
    /// JSONB posting-list registry (Inv-W5 / W9 — replaces the bloom for
    /// `@>` containment).  Same lifecycle as `gin_rowgroup_registry`:
    /// wired once at engine startup, read by `compact_one` to build the
    /// per-`(key, value)` posting list for newly written files.  `None`
    /// until the engine wires it in.
    jsonb_posting_registry: std::sync::RwLock<
        Option<Arc<basin_storage::index::jsonb_posting::JsonbPostingRegistry>>,
    >,
    /// Secondary B-tree index sink (FIX 2).  Same lifecycle as
    /// `gin_rowgroup_registry`: wired once at engine startup via
    /// `Shard::set_secondary_index_registry`, read by `compact_one` to
    /// register the indexed-column values of each newly compacted file.
    /// `None` until the engine wires it in (safe: compactor skips indexing).
    secondary_index_registry: std::sync::RwLock<Option<Arc<dyn crate::SecondaryIndexSink>>>,
    /// S4 age-based residency — the engine's hot-tier memtable registry,
    /// wired in via `Shard::set_memtable_registry`. The background loop's
    /// clean-retention sweep re-reads this cell on **every tick** (it must
    /// not capture at spawn: production spawns the background loops before
    /// `Engine::new` exists to wire the registry). Shared via `Arc` so the
    /// `share_clone` the background task runs on observes late wiring done
    /// through the original handle. `None` until wired (sweep is a no-op).
    memtable_registry_cell: Arc<std::sync::RwLock<Option<Arc<basin_hottier::MemTableRegistry>>>>,
    /// Stripe-merge serialization: at most ONE table's stripe-merge pass
    /// runs at a time process-wide, bounding the memory / IO the merge can
    /// pin (the pass decodes a whole table's cold files). `Arc` so
    /// `share_clone` (the handle the background loop runs on) and the
    /// original handle (test drivers) contend on the same lock.
    stripe_merge_lock: Arc<Mutex<()>>,
    /// Phase 6.X.A — leases held by this replica + their granted epoch.
    held_leases: HeldLeases,
    /// Phase 6.X.C — partitions whose lease is mid-handoff. While present in
    /// this set the write path returns `LeaseHandoffInProgress`; the read
    /// path is unaffected. Cleared once the handoff completes or aborts.
    draining: DrainingSet,
    /// Rolling rows/sec ingest counter feeding the autoscaler's
    /// `ingest_rows_per_sec`. `Arc`-shared so the `share_clone` the write path
    /// and background loops run on update the SAME counter the metrics snapshot
    /// reads.
    ingest_rate: Arc<IngestRate>,
}

impl InProcessShard {
    pub(crate) fn new(cfg: ShardConfig) -> Self {
        Self {
            cfg,
            partitions: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(ShardStats::default())),
            top_pattern_provider: std::sync::RwLock::new(None),
            gin_rowgroup_registry: std::sync::RwLock::new(None),
            jsonb_posting_registry: std::sync::RwLock::new(None),
            secondary_index_registry: std::sync::RwLock::new(None),
            memtable_registry_cell: Arc::new(std::sync::RwLock::new(None)),
            stripe_merge_lock: Arc::new(Mutex::new(())),
            held_leases: Arc::new(Mutex::new(HashMap::new())),
            draining: Arc::new(Mutex::new(HashSet::new())),
            ingest_rate: Arc::new(IngestRate::new()),
        }
    }

    fn share_clone(&self) -> Self {
        let provider = self
            .top_pattern_provider
            .read()
            .expect("top_pattern_provider lock poisoned")
            .clone();
        let rg_registry = self
            .gin_rowgroup_registry
            .read()
            .expect("gin_rowgroup_registry lock poisoned")
            .clone();
        let jp_registry = self
            .jsonb_posting_registry
            .read()
            .expect("jsonb_posting_registry lock poisoned")
            .clone();
        let sec_registry = self
            .secondary_index_registry
            .read()
            .expect("secondary_index_registry lock poisoned")
            .clone();
        Self {
            cfg: self.cfg.clone(),
            partitions: self.partitions.clone(),
            stats: self.stats.clone(),
            top_pattern_provider: std::sync::RwLock::new(provider),
            gin_rowgroup_registry: std::sync::RwLock::new(rg_registry),
            jsonb_posting_registry: std::sync::RwLock::new(jp_registry),
            secondary_index_registry: std::sync::RwLock::new(sec_registry),
            // Shared (not snapshotted): the sweep must observe a registry
            // wired through the original handle after spawn_background.
            memtable_registry_cell: self.memtable_registry_cell.clone(),
            stripe_merge_lock: self.stripe_merge_lock.clone(),
            held_leases: self.held_leases.clone(),
            draining: self.draining.clone(),
            // Shared: the write path runs on a `share_clone` and must update the
            // same rolling counter the metrics snapshot reads.
            ingest_rate: self.ingest_rate.clone(),
        }
    }

    /// Phase 6.X.A — current fencing epoch this replica holds for
    /// `(project, partition)`, if any. `None` in no-lease mode (the WAL append
    /// path then runs unfenced / back-compat). The write path reads the same
    /// `held_leases` map directly; this accessor exists for tests / future
    /// callers (6.X.B routing).
    #[cfg(test)]
    pub(crate) async fn lease_epoch(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Option<i64> {
        let held = self.held_leases.lock().await;
        held.get(&(*project, partition.clone())).copied()
    }

    /// Acquire (or refresh) the lease for `(project, partition)` on first
    /// access when a lease registry is configured. Records the granted epoch
    /// in `held_leases`. No-op (returns Ok) in no-lease mode.
    ///
    /// Failure semantics depend on [`LeaseMode`]:
    ///
    /// - [`LeaseMode::Off`] (the Phase 6.X.A behaviour every existing lease
    ///   test pins): returns an error if a *different* replica holds a live
    ///   lease, and propagates coordinator errors — the caller can't own
    ///   this partition right now.
    /// - [`LeaseMode::Required`] (multi-node phase 1): never blocks the
    ///   caller. Acquire failures (peer holds it / coordinator unreachable)
    ///   return `Ok` **without** recording an epoch, so reads through the
    ///   returned handle continue while the write path fails closed with
    ///   [`BasinError::LeaseNotHeld`] (`write_batch_inner` refuses epoch-less
    ///   writes in required mode).
    async fn ensure_lease(&self, project: &ProjectId, partition: &PartitionKey) -> Result<()> {
        let Some(registry) = &self.cfg.lease_registry else {
            return Ok(());
        };
        // Fast path: already held.
        {
            let held = self.held_leases.lock().await;
            if held.contains_key(&(*project, partition.clone())) {
                return Ok(());
            }
        }
        let acquired = match registry
            .acquire(
                project,
                partition.as_str(),
                &self.cfg.replica_id,
                self.cfg.lease_ttl,
            )
            .await
        {
            Ok(a) => a,
            Err(e) => {
                if self.cfg.lease_mode == LeaseMode::Required {
                    // Required mode: the coordinator is unreachable. Reads
                    // must continue (the handle is still served); writes
                    // fail closed because no epoch is recorded — the write
                    // path refuses epoch-less writes with `LeaseNotHeld`.
                    if let Some(m) = &self.cfg.lease_metrics {
                        m.record_acquire(
                            &self.cfg.replica_id,
                            basin_common::AcquireResult::Failed,
                        );
                    }
                    warn!(
                        %project,
                        %partition,
                        error = %e,
                        "lease acquire failed (coordinator unreachable); \
                         reads continue, writes refused until a lease is held",
                    );
                    return Ok(());
                }
                return Err(e);
            }
        };
        match acquired {
            Some(lease) => {
                let mut held = self.held_leases.lock().await;
                held.insert((*project, partition.clone()), lease.epoch);
                // Phase 6.X.F observability hook. v1 emits Acquired for
                // every successful acquire — the registry surface doesn't
                // (yet) distinguish first-grant vs steal-on-expiry, so we
                // can't separate `Acquired` from `Stolen` at this site
                // without leaking registry-internal state. The Stolen
                // label is reserved for when the registry trait grows a
                // discriminator (follow-on; cheap additive change).
                if let Some(m) = &self.cfg.lease_metrics {
                    m.record_acquire(
                        &self.cfg.replica_id,
                        basin_common::AcquireResult::Acquired,
                    );
                }
                Ok(())
            }
            None => {
                if let Some(m) = &self.cfg.lease_metrics {
                    m.record_acquire(
                        &self.cfg.replica_id,
                        basin_common::AcquireResult::Failed,
                    );
                }
                if self.cfg.lease_mode == LeaseMode::Required {
                    // Required mode: a peer holds a live lease. Serve the
                    // handle anyway so reads continue; no epoch is recorded,
                    // so the write path refuses with `LeaseNotHeld` until
                    // the lease is acquired (e.g. after the peer's TTL).
                    warn!(
                        %project,
                        %partition,
                        "lease held by another replica; reads continue, \
                         writes refused on this replica",
                    );
                    return Ok(());
                }
                Err(BasinError::CommitConflict(format!(
                    "lease for {project}/{partition} held by another replica"
                )))
            }
        }
    }

    /// Test-only: drive one heartbeat tick synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_heartbeat_once(&self) {
        self.heartbeat_renew().await
    }

    /// Phase 6.X.C — voluntary lease handoff (ADR 0023).
    ///
    /// State machine for transferring the `(project, partition)` lease from
    /// this replica to a candidate, with the **< 500 ms p99 stall** target
    /// from ADR 0023:
    ///
    /// 1. **Mark draining.** The partition enters the `draining` set;
    ///    subsequent `write_batch` calls fail with
    ///    [`BasinError::LeaseHandoffInProgress`] so the router can retry on
    ///    the new owner. Reads continue.
    /// 2. **Drain + flush.** Compact the in-memory tail to Parquet via the
    ///    existing compaction path — the shard compactor is the sole durable
    ///    flusher (the hot-tier registry holds only redundant clean rows and
    ///    engine-side overlays the new owner rebuilds from the WAL, so there
    ///    is nothing registry-side to flush here). The tail-snapshot work is
    ///    what bounds the stall — for small tails (< a few MB) this is well
    ///    under the 500 ms p99 target. **Larger tails**: a multi-hundred-MB
    ///    tail is dominated by the cold-tier write step (object-storage
    ///    PUT bandwidth ~100 MB/s typical); operators should size partitions
    ///    so single-partition tail depth fits the target window, or
    ///    accept a larger-than-500-ms one-off stall during the handoff.
    /// 3. **Handoff marker.** Append a `Handoff { to_holder, at_epoch }` WAL
    ///    marker as the boundary record. Replay treats it as a no-op (see
    ///    [`basin_wal::replay_wal`]); the marker exists for audit + observability.
    /// 4. **Release lease.** Drop the row in the catalog via
    ///    [`basin_catalog::LeaseRegistry::release`]. The candidate may now
    ///    `acquire` and increment the epoch.
    /// 5. **Local cleanup.** Drop the partition's in-memory state and the
    ///    held-leases entry; clear the draining flag.
    ///
    /// `to_holder` is the candidate replica id (informational — the candidate
    /// races every other replica via the lease registry's CAS regardless;
    /// `to_holder` lets observability traces correlate yield -> acquire pairs).
    ///
    /// No-op (returns `Ok`) if this replica doesn't hold the lease; the
    /// caller is asking for something already-true. No-op in no-lease mode
    /// (no registry configured) — the back-compat single-replica model has
    /// nothing to hand off.
    pub(crate) async fn yield_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        to_holder: &str,
    ) -> Result<()> {
        let key = (*project, partition.clone());
        // 0. Read the current epoch we hold; if we don't hold it, no-op.
        let epoch = {
            let held = self.held_leases.lock().await;
            match held.get(&key).copied() {
                Some(e) => e,
                None => return Ok(()),
            }
        };

        // 1. Mark draining. Idempotent — a concurrent yield_partition on the
        //    same key returns the same already-draining state and the second
        //    caller's later steps are no-ops (the lease is gone, the state is
        //    gone). The single-leaseholder invariant means concurrent yields
        //    on the same key would be a programming error anyway.
        {
            let mut d = self.draining.lock().await;
            d.insert(key.clone());
        }

        // 2. Drain: flush the WAL so any in-RAM buffered entries become
        //    closed segments (compact_one's WAL truncate only operates on
        //    closed segments — entries still in the RAM buffer won't be
        //    removed). Then compact the in-memory tail to Parquet so the
        //    new owner's cold-load is cheap and doesn't double-count rows
        //    that already landed in the cold tier.
        if let Err(e) = self.cfg.wal.flush().await {
            warn!(
                %project,
                %partition,
                error = %e,
                "lease-handoff pre-compaction wal flush failed; proceeding",
            );
        }
        if let Err(e) = self.compact_one_keyed(project, partition).await {
            // A compaction failure means the WAL still has the tail; the new
            // owner will replay it. Surface as a warn — we do NOT abort the
            // handoff, because cancelling now would leave the replica in
            // draining-rejecting-writes mode with no path to recovery.
            warn!(
                %project,
                %partition,
                error = %e,
                "lease-handoff drain compaction failed; new owner will replay WAL",
            );
        }

        // 3. Handoff marker. Best-effort: a backend that doesn't implement
        //    the marker (e.g. RaftWal stub) returns FeatureNotSupported,
        //    which we treat as informational — the handoff still completes.
        match self
            .cfg
            .wal
            .append_handoff_marker(project, partition, to_holder.to_string(), epoch)
            .await
        {
            Ok(_) => {}
            Err(BasinError::FeatureNotSupported(_)) => {
                // Marker is informational; backend doesn't ship it yet. OK.
            }
            Err(e) => {
                warn!(
                    %project,
                    %partition,
                    error = %e,
                    "lease-handoff marker append failed; proceeding (informational only)",
                );
            }
        }
        // Force a flush so the marker is durable before we release.
        if let Err(e) = self.cfg.wal.flush().await {
            warn!(
                %project,
                %partition,
                error = %e,
                "lease-handoff wal flush failed; proceeding (marker may be in-buffer)",
            );
        }

        // 4. Release the lease in the catalog. The candidate may now acquire.
        if let Some(registry) = &self.cfg.lease_registry {
            let _ = registry
                .release(project, partition.as_str(), &self.cfg.replica_id)
                .await;
        }

        // 5. Local cleanup. Drop the in-memory state and the held-lease
        //    entry, then clear the draining flag.
        {
            let mut held = self.held_leases.lock().await;
            held.remove(&key);
        }
        {
            let mut map = self.partitions.lock().await;
            map.remove(&key);
        }
        {
            let mut d = self.draining.lock().await;
            d.remove(&key);
        }
        self.refresh_resident_stats().await;
        Ok(())
    }

    /// Build a `ProjectHandle` for `(project, partition)`, loading the state
    /// from the WAL on first access. Shared by `get` (write path, after
    /// `ensure_lease`) and `get_for_read` (read path, no lease) — neither the
    /// state load nor the handle wiring touches the lease.
    async fn build_handle(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<ProjectHandle> {
        let state = self.load_or_create(project, partition).await?;
        self.refresh_resident_stats().await;
        let inner: Arc<dyn ProjectHandleImpl> = Arc::new(InProcessProjectHandle {
            project: *project,
            partition: partition.clone(),
            state: state.clone(),
            cfg: self.cfg.clone(),
            held_leases: self.held_leases.clone(),
            draining: self.draining.clone(),
            // Share the same partitions map + registries so a backpressure
            // flush from the write path is identical to a background-tick
            // compaction (`share_clone` snapshots the registry cells, which are
            // wired once at engine startup before any COPY runs).
            shard: Arc::new(self.share_clone()),
        });
        Ok(ProjectHandle { inner })
    }

    /// Look up the per-partition state (if resident) and run `compact_one`.
    /// Used by [`Self::yield_partition`]. Returns `Ok` if the partition is
    /// not resident — nothing to drain.
    async fn compact_one_keyed(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<()> {
        let state = {
            let map = self.partitions.lock().await;
            match map.get(&(*project, partition.clone())) {
                Some(s) => s.clone(),
                None => return Ok(()),
            }
        };
        self.compact_one(project, partition, state).await
    }

    /// Test-only: drive one budget-heartbeat tick synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_budget_heartbeat_once(&self) {
        self.heartbeat_budgets().await
    }

    /// Phase 6.X.D — push per-`(project, partition)` usage deltas to the
    /// coordinator and write the returned slice budgets back into every
    /// registered slice view. No-op when no coordinator is configured.
    ///
    /// v1 pushes empty deltas; the slice answer itself is the load-bearing
    /// cross-replica primitive (the cap consumer's slice decisions are
    /// what actually close the multi-instance bypass). Per-cap usage
    /// telemetry feeds the coordinator in a follow-on once each consumer
    /// exposes an observable counter.
    async fn heartbeat_budgets(&self) {
        let Some(coord) = &self.cfg.budget_coordinator else {
            return;
        };
        // Snapshot the currently-held leases so the heartbeat covers exactly
        // the partitions this replica owns. The heartbeat must always cover
        // partitions whose lease this replica holds, even when the slice
        // views / gates list is empty — that keeps the coordinator's
        // partition count accurate for downstream slice arithmetic.
        let snapshot: Vec<(ProjectId, PartitionKey)> = {
            let held = self.held_leases.lock().await;
            held.keys().cloned().collect()
        };
        for (project, partition) in snapshot {
            // Phase 6.X.F: time the budget-heartbeat round-trip the same
            // way as the lease renew so the dashboard sees one combined
            // `basin_heartbeat_lag_ms` story per replica.
            let started = Instant::now();
            let push_outcome = coord
                .push_heartbeat(
                    &project,
                    partition.as_str(),
                    &self.cfg.replica_id,
                    basin_catalog::UsageDelta::zero(),
                )
                .await;
            let lag_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
            if let Some(m) = &self.cfg.lease_metrics {
                m.record_heartbeat_lag(&self.cfg.replica_id, lag_ms);
            }
            let slice = match push_outcome {
                Ok(s) => s,
                Err(e) => {
                    // Failure path: the coordinator is unreachable. Leave
                    // every existing slice view in place (stale-safe; same
                    // value it had last heartbeat). Cap consumers fall back
                    // to per-process defaults for `(project, cap)` pairs
                    // that have never received a slice. Degraded — no
                    // cross-replica aggregation — but safe.
                    warn!(
                        %project,
                        %partition,
                        error = %e,
                        "budget heartbeat push failed; slice views stay stale",
                    );
                    continue;
                }
            };
            // Fan the slice out to every registered view.
            for view in &self.cfg.slice_views {
                for cap in basin_catalog::CapKind::ALL {
                    if let Some(s) = slice.get(*cap) {
                        view.set_slice(project, *cap, s).await;
                    }
                }
            }
        }
        // Refill every registered slice gate's token counters from its view.
        // Required for the qps-style caps where slice = tokens-per-window.
        for gate in &self.cfg.slice_gates {
            gate.refill_from_view().await;
        }
    }

    /// Renew every lease this replica holds. On a failed renewal (lost lease)
    /// drop the partition's in-memory state and stop tracking the lease — the
    /// partition now belongs to whoever stole it. No-op in no-lease mode.
    async fn heartbeat_renew(&self) {
        let Some(registry) = &self.cfg.lease_registry else {
            return;
        };
        let snapshot: Vec<((ProjectId, PartitionKey), i64)> = {
            let held = self.held_leases.lock().await;
            held.iter().map(|(k, v)| (k.clone(), *v)).collect()
        };
        for ((project, partition), epoch) in snapshot {
            // Phase 6.X.F: time the renew round-trip so the heartbeat-lag
            // histogram tracks coordinator latency. Captures the network +
            // postgres path; a flat-line lag dashboard is the leading
            // indicator that the coordinator is unhealthy *before* TTLs
            // start expiring.
            let started = Instant::now();
            let renew_outcome = registry
                .renew(
                    &project,
                    partition.as_str(),
                    &self.cfg.replica_id,
                    epoch,
                    self.cfg.lease_ttl,
                )
                .await;
            let lag_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
            if let Some(m) = &self.cfg.lease_metrics {
                m.record_heartbeat_lag(&self.cfg.replica_id, lag_ms);
            }
            let renewed = match &renew_outcome {
                Ok(true) => {
                    if let Some(m) = &self.cfg.lease_metrics {
                        m.record_renew(&self.cfg.replica_id, basin_common::RenewResult::Ok);
                    }
                    true
                }
                Ok(false) => {
                    if let Some(m) = &self.cfg.lease_metrics {
                        m.record_renew(
                            &self.cfg.replica_id,
                            basin_common::RenewResult::Expired,
                        );
                    }
                    false
                }
                Err(_) => {
                    if let Some(m) = &self.cfg.lease_metrics {
                        m.record_renew(&self.cfg.replica_id, basin_common::RenewResult::Failed);
                    }
                    // Conservative: treat transport failure the same as a
                    // lost lease so the partition state doesn't outlive
                    // the holder's belief that it still owns it.
                    false
                }
            };
            if renewed {
                continue;
            }
            // Lost the lease. FLUSH-BEFORE-DROP: the in-memory tail lives only
            // in THIS node's local WAL; if we drop the state without committing
            // it, those rows are stranded (the new owner's WAL doesn't have
            // them) → permanent loss (the original multi-node data-loss bug).
            //
            // The catalog commit (`compact_one` → `commit_with_retry` →
            // `append_data_files`) is OCC on the TABLE snapshot, NOT gated by
            // the partition lease, so a node that just lost the writer lease can
            // still durably commit its buffered tail; the OCC retry handles a
            // concurrent append from the new owner (both land, no loss).
            //
            // Stop renewing immediately (clear `held_leases`) so we never
            // re-extend a lease we lost; but keep the in-memory STATE until the
            // tail is durably committed. We do NOT block the eviction/heartbeat
            // loop indefinitely: the flush is one bounded `compact_one_keyed`
            // call (same bounded-pass drain the background compactor uses); if
            // it FAILS we log loudly and LEAVE the state resident so the next
            // heartbeat tick retries the flush — we never silently drop a
            // non-empty tail.
            {
                let mut held = self.held_leases.lock().await;
                held.remove(&(project, partition.clone()));
            }
            let flush_res = {
                // Make any in-RAM buffered WAL entries into closed segments so
                // the compaction's WAL truncate can prune them (mirrors the
                // lease-handoff drain in `yield_partition`).
                if let Err(e) = self.cfg.wal.flush().await {
                    warn!(
                        %project,
                        %partition,
                        error = %e,
                        "lease-loss pre-flush wal flush failed; proceeding to compact",
                    );
                }
                self.compact_one_keyed(&project, &partition).await
            };
            if let Err(e) = flush_res {
                // The flush+commit did not succeed: the tail is still in this
                // node's WAL/RAM. Do NOT drop the state — keep it resident so
                // the next heartbeat tick retries. Losing the lease without a
                // durable tail must never strand rows.
                warn!(
                    %project,
                    %partition,
                    epoch,
                    error = %e,
                    "lease renewal failed; tail flush-before-drop FAILED, \
                     keeping partition state resident to retry next tick",
                );
                continue;
            }
            // Tail is durably committed (or was empty). Now it is safe to drop
            // the in-memory state; the peer owns the partition and reads see the
            // flushed files via the shared catalog.
            warn!(
                %project,
                %partition,
                epoch,
                "lease renewal failed; tail flushed+committed, dropping partition state",
            );
            {
                let mut map = self.partitions.lock().await;
                map.remove(&(project, partition.clone()));
            }
            self.refresh_resident_stats().await;
        }
    }

    /// Look up the in-memory partition state, populating it from the WAL on
    /// first access.
    async fn load_or_create(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<Arc<RwLock<PartitionState>>> {
        // Fast path: already resident.
        {
            let map = self.partitions.lock().await;
            if let Some(existing) = map.get(&(*project, partition.clone())) {
                let arc = existing.clone();
                // Touch outside the outer lock; still fine because the inner
                // lock is independent of the outer one.
                drop(map);
                arc.write().await.touch();
                return Ok(arc);
            }
        }

        // Cold-load path. Replay the WAL into a fresh state object before
        // exposing it to the map; that way concurrent callers either see the
        // empty slot (and replay themselves) or see a fully replayed state.
        let mut state = PartitionState::new(*project, partition.clone());
        replay_wal_into(&self.cfg.wal, &self.cfg.catalog, project, partition, &mut state).await?;

        let arc = Arc::new(RwLock::new(state));
        let mut map = self.partitions.lock().await;
        // Another task may have raced us to populate; prefer the existing
        // entry so we don't expose two divergent copies.
        let entry = map
            .entry((*project, partition.clone()))
            .or_insert_with(|| arc.clone())
            .clone();
        drop(map);
        entry.write().await.touch();
        Ok(entry)
    }

    /// Test-only: drive one iteration of the eviction loop synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_eviction_once(&self) -> Result<()> {
        self.evict_idle().await
    }

    /// Test-only: drive one iteration of the compaction loop synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_compaction_once(&self) -> Result<()> {
        self.compact_all().await
    }

    /// Test-only: drive one stripe-merge sweep synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_stripe_merge_once(&self) -> Result<()> {
        self.stripe_merge_sweep().await
    }

    /// Test-only: drive one quiesce-drain tick synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_quiesce_drain_once(&self) -> Result<()> {
        self.quiesce_drain().await
    }

    /// Test-only: drive one file-count-bounding merge sweep synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_file_merge_once(&self) -> Result<()> {
        self.file_merge_sweep().await
    }

    /// Walk every resident project's tables and migrate any data file whose
    /// `cold_age_column` max is older than `cold_after_seconds` into the
    /// cold tier. The flow per file is:
    ///
    /// 1. Copy hot → cold (object_store::copy).
    /// 2. Atomic catalog swap via `replace_data_files` — the new manifest
    ///    references the cold path, dropping the hot one.
    /// 3. Best-effort delete of the old hot object.
    ///
    /// Files already in the cold tier are skipped. Tables with no policy
    /// (`cold_after_seconds = None`) are skipped. Errors per table are
    /// logged but never propagated — one bad table mustn't stall the rest
    /// of the sweep, same convention as `compact_all`.
    pub(crate) async fn tiering_sweep(&self) -> Result<()> {
        // Collect the set of projects we know about. Resident partitions are
        // the ground truth here — we only sweep projects whose data we've
        // touched. Cold-loading every project from a global registry would
        // require an admin API the catalog doesn't expose by design.
        let projects: Vec<ProjectId> = {
            let map = self.partitions.lock().await;
            let mut seen: HashSet<ProjectId> = HashSet::new();
            for (t, _) in map.keys() {
                seen.insert(*t);
            }
            seen.into_iter().collect()
        };

        for project in projects {
            if let Err(e) = self.sweep_project(&project).await {
                warn!(%project, error = %e, "tiering sweep failed for project; will retry next tick");
            }
        }
        Ok(())
    }

    async fn sweep_project(&self, project: &ProjectId) -> Result<()> {
        let tables = self.cfg.catalog.list_tables(project).await?;
        for table in tables {
            if let Err(e) = self.sweep_table(project, &table).await {
                warn!(
                    %project,
                    %table,
                    error = %e,
                    "tiering sweep failed for table; skipping",
                );
            }
        }
        Ok(())
    }

    async fn sweep_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        let meta = self.cfg.catalog.load_table(project, table).await?;
        let Some(threshold_secs) = meta.cold_after_seconds else {
            return Ok(()); // Policy disabled.
        };
        // Resolve the timestamp column the policy uses. Explicit setting
        // wins; otherwise fall back to the partition column. If neither is
        // available, skip — the policy is well-formed but a no-op for this
        // table until the user sets a column.
        let age_column = match &meta.cold_age_column {
            Some(c) => c.clone(),
            None => match meta.partition_spec.partition_column() {
                Some(c) => c.to_string(),
                None => return Ok(()),
            },
        };

        // Compute the cutoff in microseconds since the epoch (TIMESTAMPTZ
        // Parquet stats decode as i64 microseconds). Negative thresholds
        // would clip everything; we use saturating arithmetic to keep the
        // code free of conversion panics on edge cases.
        let now = chrono::Utc::now();
        let threshold = chrono::Duration::seconds(threshold_secs as i64);
        let cutoff_dt = now - threshold;
        let cutoff_micros = cutoff_dt.timestamp_micros();
        // We also handle Int64-as-epoch-seconds columns: same comparison
        // applied at second granularity. We try both decodings per file
        // and use whichever produces a sane answer.
        let cutoff_seconds = cutoff_dt.timestamp();

        let storage = &self.cfg.storage;
        let files = storage.list_data_files_with_stats(project, table).await?;

        // The storage footer re-read does not surface min/max for every Vortex
        // dtype — a Timestamp column is a Vortex extension type over i64, which
        // `footer_meta` leaves as None (it only emits stats for bare i64/f64).
        // The catalog, however, persists the authoritative column_stats captured
        // at write time (which DO include timestamps). Build a path→stats
        // fallback from the catalog so age-based tiering works on Vortex tables
        // (the default format), not only Parquet. This was the gap behind
        // viability_tiered_storage's #[ignore]: tiering moved 0 files because the
        // age column's stat was invisible to the sweep on Vortex.
        let catalog_stats: std::collections::HashMap<String, _> = meta
            .live_data_files()
            .into_iter()
            .map(|d| (d.path, d.column_stats))
            .collect();

        let mut migrated = 0usize;
        for f in files {
            if matches!(f.tier, basin_storage::Tier::Cold) {
                continue;
            }
            // Resolve the age column's max at the MAX_BYTES level, not the
            // key level: `footer_meta` inserts a column_stats entry for every
            // column (carrying null_count) but leaves max_bytes None for a
            // Vortex Timestamp column (extension dtype, not bare i64). So
            // `f.column_stats.get("ts")` is Some-with-no-max — a key-level
            // fallback would never fire. Prefer the storage footer's max when
            // present, else fall back to the catalog's authoritative stat
            // (populated at write time via column_stats_from_batch, which DOES
            // emit timestamp min/max). This is what lets age-based tiering work
            // on Vortex tables, the default format.
            let storage_max = f
                .column_stats
                .get(&age_column)
                .and_then(|s| s.max_bytes.as_deref());
            let catalog_max = catalog_stats
                .get(f.path.as_ref())
                .and_then(|cs| cs.get(&age_column))
                .and_then(|s| s.max_bytes.as_deref());
            let Some(max_bytes) = storage_max.or(catalog_max) else {
                continue;
            };
            // Decode max as either i64 microseconds (TIMESTAMPTZ) or
            // i64 seconds (epoch); both are 8 bytes LE. We treat the
            // file as cold-eligible if either decoding says so. In
            // practice a policy is paired with one column type; the
            // OR is safe because the wrong-decoding branch's cutoff
            // is so far off-scale (microseconds vs seconds: 1e6×) that
            // it can't false-positive against realistic data.
            let max_micros = decode_le_i64(max_bytes);
            let max_seconds = max_micros; // bytes are identical
            let is_cold_micros = max_micros.map(|m| m < cutoff_micros).unwrap_or(false);
            let is_cold_seconds = max_seconds.map(|m| m < cutoff_seconds).unwrap_or(false);
            // Determine which decoding is likely correct: a "microseconds"
            // value paired against the seconds cutoff would be off by 1e6,
            // so it'd appear *not* cold (much greater than cutoff). The
            // microseconds decoding is the canonical one; only fall back
            // to seconds if max_micros looks unreasonably large for an
            // epoch-seconds value (i.e. > year ~2200 in seconds → > 7e9).
            let is_cold = is_cold_micros && {
                // Sanity guard: if we got a value that's clearly not micros
                // (looks like seconds), prefer the seconds verdict.
                match max_micros {
                    Some(m) if m.abs() < 7_000_000_000 => is_cold_seconds,
                    _ => true,
                }
            };
            if !is_cold {
                continue;
            }

            // Migrate. Steps must be done in this order so a crash mid-way
            // never makes the catalog point at a missing object:
            //   1. Copy hot -> cold
            //   2. Catalog: replace the hot file with the cold one
            //   3. Delete the hot object
            let cold_file = match storage.migrate_to_cold(project, &f.path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(path = %f.path, error = %e, "tier migrate copy failed; skipping file");
                    continue;
                }
            };

            let parent_snapshot = self
                .cfg
                .catalog
                .load_table(project, table)
                .await?
                .current_snapshot;
            let added = DataFileRef {
                path: cold_file.path.as_ref().to_string(),
                size_bytes: cold_file.size_bytes,
                row_count: f.row_count,
                // Cold migration is a copy at the byte level; the
                // file-level column stats survive unchanged. The
                // listing path (`list_data_files_with_stats`) re-decodes
                // them at read time anyway, so passing the ones we
                // already have on `f` keeps the catalog row consistent
                // with what the reader would observe post-migration.
                column_stats: f.column_stats.clone(),
                bloom_filters: ::std::collections::BTreeMap::new(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            };
            let removed = vec![f.path.as_ref().to_string()];
            match self
                .cfg
                .catalog
                .replace_data_files(project, table, parent_snapshot, removed, vec![added])
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        path = %f.path,
                        error = %e,
                        "tier migrate catalog swap failed; cold object orphaned (will be reaped on retry)",
                    );
                    // Try to clean up the orphan cold copy. Failure here is
                    // pure waste, not a correctness issue.
                    let _ = storage.delete_file(project, &cold_file.path).await;
                    continue;
                }
            }

            // Catalog now points at the cold path; the hot file is safe to
            // delete. Best-effort: a leftover hot object is wasted bytes.
            if let Err(e) = storage.delete_file(project, &f.path).await {
                warn!(path = %f.path, error = %e, "post-migrate hot delete failed");
            }
            migrated += 1;
        }
        if migrated > 0 {
            tracing::info!(%project, %table, migrated, "tier migration complete");
        }
        Ok(())
    }

    /// Walk the partition map and drop entries whose `last_active` is past the
    /// configured `eviction_idle` window. Skips partitions whose tail is
    /// non-empty — letting the compactor drain them first preserves the
    /// "WAL is the durability boundary" invariant.
    async fn evict_idle(&self) -> Result<()> {
        let now = Instant::now();
        let idle = self.cfg.eviction_idle;

        let snapshot: PartitionSnapshot = {
            let map = self.partitions.lock().await;
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let mut to_evict: Vec<(ProjectId, PartitionKey)> = Vec::new();
        for (key, state) in snapshot {
            let guard = state.read().await;
            let stale = now.duration_since(guard.last_active) >= idle;
            let dirty = !guard.tail_is_empty();
            if stale && !dirty {
                to_evict.push(key);
            }
        }

        if to_evict.is_empty() {
            return Ok(());
        }

        let mut map = self.partitions.lock().await;
        let mut stats = self.stats.lock().await;
        for key in to_evict {
            // Re-check under the outer lock; the partition may have become
            // dirty or active in the small window between snapshot and now.
            if let Some(state) = map.get(&key) {
                let guard = state.read().await;
                let stale = now.duration_since(guard.last_active) >= idle;
                let dirty = !guard.tail_is_empty();
                drop(guard);
                if stale && !dirty {
                    map.remove(&key);
                    stats.evictions = stats.evictions.saturating_add(1);
                }
            }
        }
        stats.resident_partitions = map.len();
        stats.resident_projects = unique_projects(&map);
        Ok(())
    }

    /// ADR 0027 Phase 4 — promoted-column cold-file backfill sweep.
    ///
    /// Iterates every live data file for `(project, table)` and rewrites any
    /// file that is MISSING the shadow column(s) for the table's
    /// `promoted_jsonb_paths`.  Files that already carry all shadow columns are
    /// skipped (idempotent).  The rewritten file is committed to the catalog
    /// via the same `replace_data_files` / `CommitConflict`-retry pattern used
    /// by the tiering sweep so snapshots and refcounts stay correct.
    ///
    /// This is the eager equivalent of "the background compactor has eventually
    /// rewritten all cold-tier files" — it models the steady state of any live
    /// Basin deployment where compaction has had time to cover every file.
    ///
    /// Returns the count of files that were rewritten.
    ///
    /// # Skipping Vortex files
    ///
    /// The sweep reads each file through `Storage::read_file`, which handles
    /// both Parquet and Vortex.  The rewritten file is written via
    /// `write_batch_with_options` with the format taken from the catalog's
    /// `file_format` setting (same as `compact_one`).  Vortex files are
    /// therefore rewritten as Vortex — the Arrow schema after backfill carries
    /// the extra Utf8 column and the Vortex writer accepts it.  If a Vortex
    /// file fails to rewrite (encoding error), we log a warning and skip it
    /// rather than aborting the whole sweep.
    async fn promoted_column_backfill_sweep(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<usize> {
        // Load table metadata: promoted paths + file format + catalog snapshot.
        let meta = match self.cfg.catalog.load_table(project, table).await {
            Ok(m) => m,
            Err(basin_common::BasinError::NotFound(_)) => return Ok(0),
            Err(e) => return Err(e),
        };

        if meta.promoted_jsonb_paths.is_empty() {
            return Ok(0); // Nothing to backfill.
        }

        let promoted_paths = meta.promoted_jsonb_paths.clone();
        let file_format = shard_map_file_format(meta.file_format);

        // The shadow-column names we expect every file to carry after the sweep.
        let expected_shadows: Vec<String> = promoted_paths
            .iter()
            .map(|p| p.shadow_col_name())
            .collect();

        // Bloom the PK / declared sort columns exactly like `compact_one`
        // (#212), PLUS every promoted shadow column. The pre-fix sweep wrote
        // rewritten files with NO `bloom_columns` and committed an EMPTY
        // `bloom_filters` map, which silently DROPPED the per-file blooms
        // point reads prune with on every file it touched — and the new
        // shadow column never got a bloom at all, so the promoted JSONB
        // pseudo-secondary lookup (`WHERE __promoted$col$key = 'literal'`,
        // i.e. the rewritten `payload->>'k' = '…'`) had to open every file
        // whose min/max range admitted the literal. With the shadow column
        // bloomed, a selective literal rules files out without opening them
        // (`files_opened` drops to the files that genuinely contain it).
        let mut bloom_cols = meta.global_sort_order.clone().unwrap_or_default();
        if let [pk] = meta.pk_columns.as_slice() {
            if !bloom_cols.contains(pk) {
                bloom_cols.push(pk.clone());
            }
        }
        for shadow in &expected_shadows {
            if !bloom_cols.contains(shadow) {
                bloom_cols.push(shadow.clone());
            }
        }

        // Iterate the live data-file list.  We do NOT use list_data_files_with_stats
        // to avoid pulling Parquet footers for every file — we only need the paths.
        // We will read the full batch below for any file that needs backfilling.
        let files = self.cfg.storage.list_data_files(project, table).await?;

        // Use the default partition key (consistent with compact_one).
        let partition = PartitionKey::default_key();

        // #204: keep the rewritten file in the same PK-sorted layout the
        // table is written in elsewhere (explicit clustering wins; otherwise
        // the single-column PK), so the backfill sweep doesn't undo the
        // disjoint zone / row-group ranges the point-query prune relies on.
        let cluster_columns = if meta.cluster_columns.is_empty() {
            meta.default_cluster_cols()
        } else {
            meta.cluster_columns.clone()
        };
        let write_opts = basin_storage::WriteOptions {
            file_format,
            cluster_columns,
            bloom_columns: bloom_cols,
            ..Default::default()
        };

        let mut rewritten = 0usize;

        for file in &files {
            // Collect all batches from this file via read_file.
            // read_file reads the raw Arrow data regardless of predicate pushdown.
            let stream = match self
                .cfg
                .storage
                .read_file(project, &file.path)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        %project,
                        %table,
                        path = %file.path,
                        error = %e,
                        "promoted_column_backfill_sweep: read_file failed; skipping",
                    );
                    continue;
                }
            };

            let batches: Vec<RecordBatch> = stream
                .collect::<Vec<Result<RecordBatch>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            if batches.is_empty() {
                continue; // Empty file — no schema to check.
            }

            // Check whether any expected shadow column is absent.
            let first_schema = batches[0].schema();
            let needs_backfill = expected_shadows
                .iter()
                .any(|name| first_schema.field_with_name(name).is_err());

            if !needs_backfill {
                continue; // All shadow columns already present — skip.
            }

            // Concatenate batches and run backfill.
            let schema = batches[0].schema();
            let merged = match arrow::compute::concat_batches(&schema, &batches) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        %project,
                        %table,
                        path = %file.path,
                        error = %e,
                        "promoted_column_backfill_sweep: concat_batches failed; skipping",
                    );
                    continue;
                }
            };

            let backfilled = match backfill_promoted_columns(merged, &promoted_paths) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        %project,
                        %table,
                        path = %file.path,
                        error = %e,
                        "promoted_column_backfill_sweep: backfill failed; skipping",
                    );
                    continue;
                }
            };

            // Write the rewritten file to storage.
            let new_file = match self
                .cfg
                .storage
                .write_batch_with_options(
                    project,
                    table,
                    &partition,
                    &backfilled,
                    &write_opts,
                )
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        %project,
                        %table,
                        path = %file.path,
                        error = %e,
                        "promoted_column_backfill_sweep: write_batch_with_options failed; skipping",
                    );
                    continue;
                }
            };

            let added = basin_catalog::DataFileRef {
                path: new_file.path.as_ref().to_string(),
                size_bytes: new_file.size_bytes,
                row_count: new_file.row_count,
                column_stats: new_file.column_stats.clone(),
                // Carry the writer-computed blooms (PK / sort cols / promoted
                // shadow cols) into the catalog — `compact_one` parity. An
                // empty map here would erase pruning on every swept file.
                bloom_filters: new_file.bloom_filters.clone(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            };
            let removed = vec![file.path.as_ref().to_string()];

            // Commit with retry on CommitConflict (same pattern as tiering_sweep).
            let commit_result = 'commit: {
                for attempt in 0..3 {
                    let snapshot = match self.cfg.catalog.load_table(project, table).await {
                        Ok(m) => m.current_snapshot,
                        Err(e) => break 'commit Err(e),
                    };
                    match self
                        .cfg
                        .catalog
                        .replace_data_files(
                            project,
                            table,
                            snapshot,
                            removed.clone(),
                            vec![added.clone()],
                        )
                        .await
                    {
                        Ok(_) => break 'commit Ok(()),
                        Err(basin_common::BasinError::CommitConflict(_)) if attempt < 2 => {
                            // Retry: snapshot advanced concurrently.
                            continue;
                        }
                        Err(e) => break 'commit Err(e),
                    }
                }
                Err(basin_common::BasinError::CommitConflict(format!(
                    "{project}/{table}: backfill sweep lost commit race (3 attempts)"
                )))
            };

            match commit_result {
                Ok(()) => {
                    // GIN row-group registry maintenance for the rewritten file.
                    //
                    // The sweep SUPERSEDES `file.path` with a NEW path
                    // (`new_file.path`) via `replace_data_files`.  Unlike
                    // `compact_one` (which only ever APPENDS a brand-new merged
                    // file), this rewrite changes the live file set: the old
                    // path is gone and a new path is live.  The GIN row-group
                    // completeness guard in `apply_gin_pruning_for_query`
                    // requires EVERY live file to be sealed in the registry, so
                    // a rewritten-but-unindexed file forces the whole `@>`
                    // containment query to fall back to a full scan until the
                    // next CREATE INDEX backfill or compaction re-indexes it.
                    //
                    // Mirror `compact_one`'s `reindex_compacted_file_gin` call
                    // exactly: re-index the new file's batch so it is sealed,
                    // and drop the old path's stale summaries so the registry
                    // doesn't leak entries for a file that no longer exists.
                    let effective_rg_size = meta
                        .row_block_size
                        .map(|v| v as usize)
                        .or(meta.row_group_rows)
                        .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE);
                    reindex_compacted_file_gin(
                        &self.gin_rowgroup_registry,
                        project,
                        table,
                        &meta.indexes,
                        effective_rg_size,
                        &backfilled,
                        new_file.path.as_ref().as_ref(),
                    );
                    // Drop the superseded file's summaries from the registry.
                    if let Some(rg_registry) = self
                        .gin_rowgroup_registry
                        .read()
                        .expect("gin_rowgroup_registry lock poisoned")
                        .as_ref()
                    {
                        for idx in &meta.indexes {
                            if idx.access_method == "gin" && idx.columns.len() == 1 {
                                rg_registry.remove_file(
                                    project,
                                    table,
                                    &idx.columns[0],
                                    file.path.as_ref(),
                                );
                            }
                        }
                    }

                    // Delete the old file best-effort (same pattern as tiering sweep).
                    if let Err(e) = self
                        .cfg
                        .storage
                        .delete_file(project, &file.path)
                        .await
                    {
                        warn!(
                            path = %file.path,
                            error = %e,
                            "promoted_column_backfill_sweep: old file delete failed (orphan)",
                        );
                    }
                    rewritten += 1;
                }
                Err(e) => {
                    warn!(
                        %project,
                        %table,
                        path = %file.path,
                        error = %e,
                        "promoted_column_backfill_sweep: catalog commit failed; skipping",
                    );
                    // Best-effort: delete the orphaned new file.
                    let _ = self.cfg.storage.delete_file(project, &new_file.path).await;
                }
            }
        }

        if rewritten > 0 {
            tracing::info!(
                %project,
                %table,
                rewritten,
                "promoted_column_backfill_sweep: cold-file backfill complete",
            );
        }
        Ok(rewritten)
    }

    /// Drain every partition's tail into Parquet, committing through the
    /// catalog and truncating the WAL on success.
    ///
    /// Concurrency: partitions compact in parallel, bounded by a
    /// `Semaphore` (default = `available_parallelism`, env-overridable
    /// via `BASIN_SHARD_COMPACTION_CONCURRENCY`). The per-partition
    /// `compact_lock` inside `compact_one` keeps two concurrent
    /// compactions of the *same* partition serial — correctness on
    /// the snapshot/swap is unchanged. Different partitions are
    /// independent (snapshot disjoint tails, write disjoint Parquet
    /// files, commit independent catalog snapshots), so fanning out
    /// the dispatch loop trades a little CPU bookkeeping for wall-clock
    /// proportional to (N / concurrency) instead of N.
    async fn compact_all(&self) -> Result<()> {
        use futures::stream::{FuturesUnordered, StreamExt as _};
        use tokio::sync::Semaphore;

        let mut snapshot: PartitionSnapshot = {
            let map = self.partitions.lock().await;
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        // Owner-only compaction (multi-node). Only the lease holder may
        // compact + commit a partition; a non-owner compacting races the real
        // owner's catalog commit ("lost commit race at version N"). In no-lease
        // mode (`lease_registry == None`) there is no ownership concept and the
        // node is the sole writer, so we compact every resident partition as
        // before. When a registry IS configured, drop partitions whose lease
        // this node does not currently hold. (Note: state for a non-owned
        // partition can still be resident transiently — e.g. just after a lease
        // loss before the flush-before-drop in `heartbeat_renew` runs.)
        if self.cfg.lease_registry.is_some() {
            let held: std::collections::HashSet<(ProjectId, PartitionKey)> = {
                let h = self.held_leases.lock().await;
                h.keys().cloned().collect()
            };
            snapshot.retain(|((project, partition), _)| {
                held.contains(&(*project, partition.clone()))
            });
        }
        if snapshot.is_empty() {
            return Ok(());
        }

        let concurrency = std::env::var("BASIN_SHARD_COMPACTION_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });
        let permits = Arc::new(Semaphore::new(concurrency));

        let mut futs = FuturesUnordered::new();
        for ((project, partition), state) in snapshot {
            let permits = permits.clone();
            futs.push(async move {
                let _permit = permits.acquire_owned().await.expect("compaction semaphore");
                let res = self.compact_one(&project, &partition, state).await;
                (project, partition, res)
            });
        }
        while let Some((project, partition, res)) = futs.next().await {
            if let Err(e) = res {
                // A failure to compact one partition must not stall others.
                // Surface via tracing; the next tick will retry.
                warn!(
                    %project,
                    %partition,
                    error = %e,
                    "compaction failed for partition; will retry next tick",
                );
            }
        }
        Ok(())
    }

    /// Read-freshness convergence: drain every resident partition's residual
    /// tail PROMPTLY once shard-wide ingest has gone idle.
    ///
    /// Why this exists: a row is made WAL-durable on the write path (the
    /// end-of-COPY durable barrier waits on `Wal::await_durable`), but it only
    /// becomes visible to metadata reads — `count(*)`/`max(id)` answered from
    /// `Catalog::live_data_files()` via the fast-aggregate path — once it has
    /// been COMPACTED out of the in-memory tail into a catalog segment. The
    /// read-side `flush_to_parquet()` only drains partitions resident on the
    /// node serving the read, so a forwarded (peer-owned) partition's residual
    /// tail — or any local residual — otherwise becomes visible only on the
    /// 30 s `compaction_interval` timer. After a write burst stops there is no
    /// further write to trigger the soft-cap proactive drain, so the residual
    /// lingers until the next timer tick. This short tick closes that window.
    ///
    /// Cost discipline (preserves the flat-ingest win): the tick is a strict
    /// no-op WHILE ingest is active. It returns immediately when the rolling
    /// ingest rate is non-zero, so it never issues a PUT or holds a compaction
    /// lock that competes with the write path — the soft-cap proactive drain
    /// and the 30 s timer remain the sole compaction drivers under load. Only
    /// once `ingest_rate.rows_per_sec()` reads 0 (a full quiet second elapsed
    /// with no writes) does it run a single bounded `compact_all`, identical to
    /// the read-path flush, so exactly-once / watermark / per-partition
    /// compaction-lock semantics are unchanged.
    async fn quiesce_drain(&self) -> Result<()> {
        // Idle gate: bail while writes are flowing. The compactor keeps pace
        // under load via the soft-cap proactive drain + 30 s timer; this tick
        // exists only to collapse the post-quiesce visibility lag.
        if self.ingest_rate.rows_per_sec() > 0 {
            return Ok(());
        }
        // Pending-tail gate: avoid the `compact_all` snapshot + per-partition
        // lock churn when there is nothing resident to drain. A cheap scan of
        // the resident partition tails under their read locks (never the global
        // map lock across an await, same discipline as `tail_pressure`).
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.values().cloned().collect()
        };
        let mut any_pending = false;
        for state in &snapshot {
            let g = state.read().await;
            if !g.tail_is_empty() {
                any_pending = true;
                break;
            }
        }
        if !any_pending {
            return Ok(());
        }
        // Re-check idleness right before doing work: a write may have landed
        // while we scanned the tails. If so, defer to the soft-cap/timer path
        // and let the next quiesce tick try again once it is truly quiet.
        if self.ingest_rate.rows_per_sec() > 0 {
            return Ok(());
        }
        self.compact_all().await
    }

    async fn compact_one(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        state: Arc<RwLock<PartitionState>>,
    ) -> Result<()> {
        // Serialize compaction per partition (#95). Clone the compaction lock
        // out under a brief read lock, then hold it across the whole body so a
        // concurrent `compact_one` on the same partition cannot snapshot and
        // re-commit the same tail entries (which duplicated rows in the cold
        // tier and over-counted on read). A blocked caller resumes after the
        // in-flight compaction drains the tail and then finds nothing to do.
        let compact_lock = {
            let guard = state.read().await;
            guard.compact_lock.clone()
        };
        let _compact_guard = compact_lock.lock().await;

        // Drain the partition tail in BOUNDED passes. Each pass snapshots a
        // prefix of each table's tail capped at `MAX_COMPACTION_ROWS` rows,
        // writes it as ONE immutable file, then prunes exactly the flushed
        // entries. Looping until the tail is empty means a tick that finds a
        // large backlog (e.g. a fast COPY that outran the time-ticked compactor)
        // produces SEVERAL bounded files instead of one giant O(tail) concat +
        // encode. The cost of any single file write is bounded by recent
        // writes, not by how large the tail (or table) has grown — this is what
        // keeps per-chunk ingest cost flat as the table scales.
        let mut max_lsn_overall: Option<Lsn> = None;
        let mut any_adaptive_sort = false;
        // Per-`compact_one` cache of the resolved per-table write config. The
        // config (file format, cluster columns, indexes, row-group size, …) is
        // table-level metadata that does not change while we drain the tail, so
        // we load it from the catalog ONCE per table here instead of once per
        // flushed file. `load_table` clones the whole snapshot chain (O(files)),
        // so resolving it inside the per-file loop made a single backpressure
        // drain of a saturated tail cost O(files-per-flush × total-files) — the
        // dominant catalog cost behind the long-COPY ingest decay. Caching it
        // collapses that to one chain clone per table per compaction call.
        let mut table_cfg_cache: HashMap<TableName, ResolvedTableCompactCfg> = HashMap::new();
        let flush_concurrency = flush_concurrency();
        loop {
        // Snapshot a bounded prefix of each table's tail under the inner read
        // lock as a flat list of CHUNKS (each chunk ≤ `MAX_COMPACTION_ROWS`
        // rows = one output file). We take up to `flush_concurrency` chunks
        // per wave so the file writes below can be issued CONCURRENTLY against
        // the object store — a single no-PK COPY has just one table, so the
        // parallelism that lets compaction keep pace with ingest comes from
        // flushing several of that one table's chunks at once, not from many
        // tables. Holding the read lock briefly is fine; the lock is
        // per-partition and we drop it before any I/O. Capping rows per chunk
        // bounds each concat + encode; capping chunks per wave bounds the
        // per-wave memory and in-flight PUTs.
        let tail_snapshot: Vec<(TableName, Vec<(Lsn, RecordBatch)>)> = {
            let guard = state.read().await;
            let mut chunks: Vec<(TableName, Vec<(Lsn, RecordBatch)>)> = Vec::new();
            'outer: for (t, v) in guard.tail.iter().filter(|(_, v)| !v.is_empty()) {
                let mut taken = Vec::new();
                let mut rows = 0usize;
                for (lsn, b) in v.iter() {
                    // Always take at least one batch per chunk so a single
                    // oversized batch still makes progress; close the chunk
                    // once it would exceed the per-file row budget and start a
                    // new one (still the same table) so the writes parallelise.
                    if !taken.is_empty() && rows + b.num_rows() > MAX_COMPACTION_ROWS {
                        chunks.push((t.clone(), std::mem::take(&mut taken)));
                        rows = 0;
                        if chunks.len() >= flush_concurrency {
                            break 'outer;
                        }
                    }
                    rows += b.num_rows();
                    taken.push((*lsn, b.clone()));
                }
                if !taken.is_empty() {
                    chunks.push((t.clone(), taken));
                    if chunks.len() >= flush_concurrency {
                        break 'outer;
                    }
                }
            }
            chunks
        };

        if tail_snapshot.is_empty() {
            break;
        }

        let mut drained_per_table: HashMap<TableName, Lsn> = HashMap::new();

        // PHASE 1 (sequential, cheap, no object-store I/O): for each chunk
        // resolve the per-table write config (cached), pick sort columns, build
        // the WriteOptions and the backfilled merged batch. This touches the
        // `!Send` top-pattern-provider guard and the per-call cfg cache, so it
        // stays on this task. The output is a list of self-contained write
        // plans that PHASE 2 can hand to concurrent writer tasks.
        struct ChunkWritePlan {
            table: TableName,
            max_lsn: Lsn,
            merged: RecordBatch,
            write_opts: basin_storage::WriteOptions,
            gin_indexes: Vec<basin_catalog::SecondaryIndex>,
            effective_rg_size: usize,
            used_adaptive_sort: bool,
        }
        let mut plans: Vec<ChunkWritePlan> = Vec::with_capacity(tail_snapshot.len());

        for (table, entries) in tail_snapshot {
            let max_lsn = entries
                .iter()
                .map(|(lsn, _)| *lsn)
                .max()
                .expect("non-empty table tail");

            // Concatenate all batches for this table into one Parquet write.
            let batches: Vec<RecordBatch> = entries.iter().map(|(_, b)| b.clone()).collect();
            let schema = batches[0].schema();
            let merged = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| BasinError::storage(format!("concat batches: {e}")))?;

            // Resolve per-table metadata: on-disk format, cluster columns,
            // adaptive-sort override, GIN indexes, and row-group size.
            // A missing catalog row is unusual but non-fatal; fall back to
            // safe defaults so the whole compaction tick doesn't error out.
            // #204: `pk_default_cluster_cols` is the single-column PK the file
            // should be sorted by when the table declares no explicit
            // clustering — sorting by it makes per-row-group / zone ranges
            // disjoint so the reader's min/max prune isolates `WHERE pk = $1`.
            //
            // PERF: resolved ONCE per table per `compact_one` via
            // `table_cfg_cache` (see its declaration above) — this metadata is
            // table-level config that does not change while we drain the tail,
            // and `load_table` clones the full O(files) snapshot chain. Loading
            // it per flushed file made a saturated-tail backpressure drain cost
            // O(files²); the cache makes it one chain clone per table.
            if !table_cfg_cache.contains_key(&table) {
                let resolved = match self.cfg.catalog.load_table(project, &table).await {
                    Ok(m) => {
                        let pk_default = m.default_cluster_cols();
                        // #212: bloom the single-column PK (plus any declared
                        // sort-by columns, mirroring the engine INSERT path) so
                        // `pk_point_probe` can rule compacted stripe files in or
                        // out without opening them. Round-robin striping spreads
                        // PKs across every stripe, so without per-file blooms a
                        // point UPDATE's cold pre-image read visits ~all of them.
                        let mut bloom = m.global_sort_order.clone().unwrap_or_default();
                        if let [pk] = m.pk_columns.as_slice() {
                            if !bloom.contains(pk) {
                                bloom.push(pk.clone());
                            }
                        }
                        // ADR 0027 Phase 4: bloom every promoted shadow column
                        // too (the compactor backfills them into the merged
                        // batch below), so equality on `__promoted$col$key` —
                        // the rewritten `payload->>'k' = 'literal'` lookup —
                        // can rule freshly-compacted files in or out without
                        // opening them.
                        for p in &m.promoted_jsonb_paths {
                            let s = p.shadow_col_name();
                            if !bloom.contains(&s) {
                                bloom.push(s);
                            }
                        }
                        ResolvedTableCompactCfg {
                            file_format: shard_map_file_format(m.file_format),
                            declared_cluster_cols: m.cluster_columns,
                            adaptive_sort_override: m.adaptive_sort_override.unwrap_or(false),
                            gin_indexes: m.indexes.clone(),
                            row_group_rows: m.row_group_rows,
                            row_block_size: m.row_block_size,
                            promoted_paths: m.promoted_jsonb_paths,
                            pk_default_cluster_cols: pk_default,
                            bloom_cols: bloom,
                        }
                    }
                    Err(_) => ResolvedTableCompactCfg::default(),
                };
                table_cfg_cache.insert(table.clone(), resolved);
            }
            let cfg_entry = table_cfg_cache.get(&table).expect("just inserted");
            let file_format = cfg_entry.file_format;
            let declared_cluster_cols = cfg_entry.declared_cluster_cols.clone();
            let adaptive_sort_override = cfg_entry.adaptive_sort_override;
            let gin_indexes = cfg_entry.gin_indexes.clone();
            let row_group_rows = cfg_entry.row_group_rows;
            let row_block_size = cfg_entry.row_block_size;
            let promoted_paths = cfg_entry.promoted_paths.clone();
            let pk_default_cluster_cols = cfg_entry.pk_default_cluster_cols.clone();
            let bloom_cols = cfg_entry.bloom_cols.clone();

            // Phase 5.14.D2: consult the query-pattern history and decide
            // which columns to sort the output file by.  Use a block scope so
            // the !Send RwLockReadGuard is always dropped before any await.
            let observed: Option<Vec<String>> = {
                let guard = self
                    .top_pattern_provider
                    .read()
                    .expect("top_pattern_provider lock poisoned");
                guard.as_ref().and_then(|p| p.top_pattern(project, &table))
            };

            // Record divergence for Phase 5.16.G when applicable.
            if let Some(ref obs) = observed {
                if !declared_cluster_cols.is_empty() && obs != &declared_cluster_cols {
                    let provider_clone: Option<Arc<dyn TopPatternProvider>> = {
                        let guard = self
                            .top_pattern_provider
                            .read()
                            .expect("top_pattern_provider lock poisoned");
                        guard.as_ref().cloned()
                    };
                    if let Some(p) = provider_clone {
                        p.record_cluster_delta(project, &table, obs, &declared_cluster_cols);
                    }
                }
            }

            let (sort_cols, used_adaptive_sort): (Vec<String>, bool) = match (
                declared_cluster_cols.is_empty(),
                observed.as_ref(),
                adaptive_sort_override,
            ) {
                (true, Some(obs), _) => (obs.clone(), true),
                (false, Some(obs), true) => (obs.clone(), true),
                (false, None, true) => (declared_cluster_cols, false),
                (false, _, false) => (declared_cluster_cols, false),
                // #204: no declared clustering and no observed query pattern —
                // fall back to the single-column PK (empty when the table has
                // no prunable single-column PK) so the flushed file is
                // PK-sorted and its zone / row-group ranges are disjoint.
                (true, None, _) => (pk_default_cluster_cols, false),
            };

            let write_opts = basin_storage::WriteOptions {
                file_format,
                cluster_columns: sort_cols,
                // Honour the table's row-group size (set via
                // `WITH (basin.row_block_size = N)` at CREATE TABLE time or
                // via `SET row_group_rows`).  Without this the compactor
                // always writes at the default 65 536-row cap, preventing
                // multi-row-group layouts even when the table was configured
                // with a smaller block size.
                row_block_size,
                max_row_group_size: row_group_rows,
                bloom_columns: bloom_cols,
                ..Default::default()
            };
            // ADR 0027 Phase 4 backfill: extend the merged batch with shadow
            // columns for any promoted JSONB paths, so rows written BEFORE the
            // path was promoted also carry the shadow value in the compacted
            // file (otherwise they'd read NULL via DataFusion's missing-column
            // fill). No-op when the table has no promoted paths.
            let merged = backfill_promoted_columns(merged, &promoted_paths)?;
            // The effective row-group size mirrors the writer's priority:
            // row_block_size (WITH clause) > row_group_rows (ALTER TABLE) > default.
            let effective_rg_size = row_block_size
                .map(|v| v as usize)
                .or(row_group_rows)
                .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE);
            plans.push(ChunkWritePlan {
                table,
                max_lsn,
                merged,
                write_opts,
                gin_indexes,
                effective_rg_size,
                used_adaptive_sort,
            });
        }

        // PHASE 2a (CONCURRENT, latency-bound): write every chunk's data file
        // to the object store at once, bounded by `flush_concurrency`. This is
        // the change that lets compaction keep pace with ingest: a single PUT
        // is RTT-bound, so serial writes capped compaction throughput below
        // ingest and let the tail fill; issuing `flush_concurrency` PUTs at
        // once makes compaction throughput scale with parallel PUT BANDWIDTH.
        // The storage layer's per-project permit pool further bounds the real
        // in-flight PUT count, so this never floods the store. Each task only
        // writes the file (no catalog mutation), so the writes are independent
        // and cannot conflict.
        let written: Vec<(usize, basin_storage::DataFile)> = {
            use futures::stream::{FuturesUnordered, StreamExt as _};
            let mut futs = FuturesUnordered::new();
            for (idx, plan) in plans.iter().enumerate() {
                let storage = self.cfg.storage.clone();
                let table = plan.table.clone();
                let merged = plan.merged.clone();
                let write_opts = plan.write_opts.clone();
                let partition = partition.clone();
                let project = *project;
                futs.push(async move {
                    let res = storage
                        .write_batch_with_options(&project, &table, &partition, &merged, &write_opts)
                        .await;
                    (idx, res)
                });
            }
            // The per-wave snapshot already caps `plans.len()` at
            // `flush_concurrency`, so all these futures in flight at once is
            // exactly the intended bound; the storage layer's per-project
            // permit pool is the final guard on real in-flight PUTs. We fail
            // the whole wave on the first write error (the data is still in the
            // tail + WAL and no catalog commit has happened yet, so the next
            // tick retries — nothing is dropped or double-committed).
            let mut out: Vec<(usize, basin_storage::DataFile)> = Vec::with_capacity(plans.len());
            while let Some((idx, res)) = futs.next().await {
                out.push((idx, res?));
            }
            out
        };
        // Re-pair writes with their plans, sorted by plan index so the
        // sequential commit below runs in deterministic (ascending-LSN) order.
        let mut written_by_idx: Vec<Option<basin_storage::DataFile>> =
            (0..plans.len()).map(|_| None).collect();
        for (idx, df) in written {
            written_by_idx[idx] = Some(df);
        }

        // PHASE 2b (SEQUENTIAL): commit each written file to the catalog and
        // build its sidecar indexes. Commits MUST be serial per table — the
        // catalog append is a CAS against the current snapshot, so two
        // concurrent appends would conflict-loop; running them in order keeps
        // the fast path conflict-free and avoids any double-commit. The
        // expensive (RTT-bound) work already happened concurrently in 2a; the
        // commit is an in-process catalog CAS.
        // Take every written file for this wave up front (keep the DataFiles
        // alive so the per-file sidecar indexing below can reference their
        // paths), then commit them grouped by table in ONE append per table.
        let wave_data_files: Vec<basin_storage::DataFile> = (0..plans.len())
            .map(|i| {
                written_by_idx[i]
                    .take()
                    .expect("every plan has a corresponding written file")
            })
            .collect();

        // Group plan indices by table (a wave's chunks are usually one table,
        // but the snapshot can span several within the partition), build the
        // file refs, and commit each table's whole file set in ONE catalog
        // append (#39: amortizes the per-commit segment+head PUTs across the
        // wave instead of 2 PUTs per file). Commit serially per table so the
        // per-partition CAS stays conflict-free.
        let mut idx_by_table: HashMap<TableName, Vec<usize>> = HashMap::new();
        for (idx, plan) in plans.iter().enumerate() {
            idx_by_table.entry(plan.table.clone()).or_default().push(idx);
        }
        for (table, idxs) in &idx_by_table {
            let files: Vec<DataFileRef> = idxs
                .iter()
                .map(|&i| {
                    let df = &wave_data_files[i];
                    DataFileRef {
                        path: df.path.as_ref().to_string(),
                        size_bytes: df.size_bytes,
                        row_count: df.row_count,
                        column_stats: df.column_stats.clone(),
                        bloom_filters: df.bloom_filters.clone(),
                        hll_sketches: ::std::collections::BTreeMap::new(),
                        tdigest_sketches: ::std::collections::BTreeMap::new(),
                    }
                })
                .collect();
            // Representative batch (first plan's) for the create-table-on-first
            // -commit path; schema is identical across a table's chunks.
            let wave_rows: u64 = files.iter().map(|f| f.row_count as u64).sum();
            self.commit_with_retry(project, table, partition, &plans[idxs[0]].merged, files)
                .await?;
            // Phase 2 autotune: feed the process-global committed-rows/s signal
            // (one relaxed atomic add per committed wave — NOT per row). Read
            // only by the adaptive controller's tick task, which is spawned only
            // under BASIN_AUTOTUNE; off → this counter is written but never
            // observed (no-op).
            basin_common::autotune::record_committed_rows(wave_rows);
        }

        // Per-file sidecar indexing + drain bookkeeping (after the commits).
        for (idx, plan) in plans.iter().enumerate() {
            let table = &plan.table;
            let merged = &plan.merged;
            let gin_indexes = &plan.gin_indexes;
            let effective_rg_size = plan.effective_rg_size;
            let data_file = &wave_data_files[idx];

            // Re-index the compacted file's JSONB GIN columns into the
            // row-group bloom registry so the engine's `@>` row-group prune
            // fires on compacted files. This is the ONLY place GIN row-group
            // summaries are populated on the shard path (INSERT writes go
            // directly to WAL+tail). Correctness: the registry is a
            // conservative superset (bloom false positives are fine;
            // `jsonb_contains` re-checks every surviving row at read time).
            reindex_compacted_file_gin(
                &self.gin_rowgroup_registry,
                project,
                table,
                gin_indexes,
                effective_rg_size,
                merged,
                data_file.path.as_ref().as_ref(),
            );

            // FIX 2(a) — register the compacted file's single-column B-tree
            // index values into the secondary-index registry so point queries
            // on an indexed column can prune to this file.
            reindex_compacted_file_secondary(
                &self.secondary_index_registry,
                project,
                table,
                gin_indexes,
                effective_rg_size,
                merged,
                data_file.path.as_ref().as_ref(),
            );

            // Inv-W5 / W9: build the JSONB posting list for any GIN index on a
            // JSONB column. Best-effort: a sidecar write failure is logged but
            // does not abort the commit (queries fall back to bloom/full scan).
            if let Err(e) = reindex_compacted_file_jsonb_posting(
                &self.jsonb_posting_registry,
                self.cfg.storage.clone(),
                project,
                table,
                gin_indexes,
                effective_rg_size,
                merged,
                data_file.path.as_ref(),
            )
            .await
            {
                tracing::warn!(
                    project = %project,
                    table = %table,
                    file = %data_file.path.as_ref(),
                    error = %e,
                    "Inv-W5: JSONB posting-list sidecar write failed; @> falls back to bloom prune"
                );
            }

            // PG-Wave-α: write `.rtree` sidecars for any GIST index on a POINT
            // column. Best-effort: a sidecar write failure is logged but does
            // not abort the compaction commit (the data is already catalogued).
            if let Err(e) = reindex_compacted_file_rtree(
                self.cfg.storage.clone(),
                project,
                table,
                gin_indexes,
                effective_rg_size as u32,
                merged,
                data_file.path.as_ref(),
            )
            .await
            {
                tracing::warn!(
                    project = %project,
                    table = %table,
                    file = %data_file.path.as_ref(),
                    error = %e,
                    "PG-Wave-α R-tree sidecar write failed; queries fall back to full scan"
                );
            }

            if plan.used_adaptive_sort {
                any_adaptive_sort = true;
            }

            let max_lsn = plan.max_lsn;
            let entry = drained_per_table.entry(table.clone()).or_insert(max_lsn);
            if *entry < max_lsn {
                *entry = max_lsn;
            }
            max_lsn_overall = Some(match max_lsn_overall {
                Some(prev) if prev >= max_lsn => prev,
                _ => max_lsn,
            });
        }

        // Apply the drain: clear only the (lsn, batch) entries we actually
        // committed in THIS pass. Entries beyond the per-pass row budget (and
        // new writes that landed during compaction, with higher LSNs) stay in
        // the tail and are picked up by the next loop iteration / tick.
        {
            let mut guard = state.write().await;
            for (table, max_lsn) in &drained_per_table {
                if let Some(v) = guard.tail.get_mut(table) {
                    v.retain(|(lsn, _)| *lsn > *max_lsn);
                }
                if guard.last_compacted_lsn < *max_lsn {
                    guard.last_compacted_lsn = *max_lsn;
                }
            }
        }
        // Loop back to drain the next bounded prefix. The snapshot at the top
        // returns empty once the tail is fully drained, breaking the loop.
        }

        // DURABILITY (compaction watermark — duplicate-row crash-window fix).
        //
        // The data files are committed to the catalog (above, per table). The
        // WAL still holds the entries we just compacted; we are about to
        // truncate them. A crash *between* the catalog commit and the WAL
        // truncate leaves those entries in the WAL, so cold-start replay would
        // re-append them to the tail and the next compaction would commit a
        // SECOND Parquet/Vortex file for the same rows — duplicating every row.
        //
        // Persisting the compacted-through LSN to the catalog (the durable
        // commit point) closes that window: cold-start replay reads it via
        // `get_compaction_watermark` and replays only entries strictly above
        // it (see `replay_wal_into`). The watermark write happens BEFORE the
        // truncate and is monotonic (GREATEST upsert), so:
        //   * crash after watermark, before truncate  → replay skips the batch
        //     (watermark covers it); WAL still holds it but it's filtered out.
        //   * crash after truncate                    → entries are already gone
        //     from the WAL; the watermark merely confirms the floor.
        // Either way no duplicate file is produced.
        //
        // Best-effort vs. fatal: a watermark write failure is NOT fatal to the
        // compaction (the data is already durably committed). We log and skip
        // the truncate so the next compaction tick re-attempts both the
        // watermark and the truncate — preferring a replayable WAL (at worst a
        // retried, idempotent watermark) over truncating without a recorded
        // floor. Backends that don't persist a watermark (in-memory / REST) use
        // the trait's `Ok(())` default and keep the legacy replay-from-zero
        // behavior — correct because their catalog+WAL are lost together.
        if let Some(max_lsn) = max_lsn_overall {
            match self
                .cfg
                .catalog
                .set_compaction_watermark(project, partition.as_str(), max_lsn.0)
                .await
            {
                Ok(()) => {
                    // Truncate only after the watermark is durable, so the
                    // commit→truncate window is always covered by a recorded
                    // floor.
                    self.cfg.wal.truncate(project, partition, max_lsn).await?;
                }
                Err(e) => {
                    warn!(
                        %project,
                        %partition,
                        max_lsn = max_lsn.0,
                        error = %e,
                        "compaction watermark persist failed; skipping WAL truncate \
                         so the next tick retries (replay stays duplicate-safe via \
                         the watermark floor)",
                    );
                }
            }
        }

        let mut stats = self.stats.lock().await;
        stats.compactions = stats.compactions.saturating_add(1);
        // Phase 5.14.D2: adaptive-sort observability counter.
        if any_adaptive_sort {
            stats.compactions_with_adaptive_sort =
                stats.compactions_with_adaptive_sort.saturating_add(1);
        }
        Ok(())
    }

    /// Commit a BATCH of data files to the catalog in ONE snapshot append. On
    /// `CommitConflict`, reload + retry once. The catalog auto-creates the table
    /// on first commit using the batch's schema.
    ///
    /// Batching matters for throughput (#39): each catalog append writes one
    /// delta segment object + one head-pointer object (2 PUTs), regardless of
    /// how many files it carries. Committing a whole compaction wave's files in
    /// ONE append amortizes those 2 PUTs across the wave instead of paying 2
    /// PUTs PER FILE — the per-partition commit path is the dominant sustained-
    /// ingest cost (serial PUT-latency-bound), so this cuts commit PUTs by the
    /// flush-concurrency factor. Exactly-once is unaffected: the append is a
    /// single all-or-nothing CAS over the file set.
    async fn commit_with_retry(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition: &PartitionKey,
        batch: &RecordBatch,
        files: Vec<DataFileRef>,
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        // Make sure the table exists; if not, create it from the batch schema.
        //
        // Multi-node scaling fix: commit through the PARTITION-scoped catalog
        // methods. Each partition owns its own data-file segment chain, so the
        // per-partition OCC token below only ever races a concurrent commit to
        // the SAME partition — never another partition or another node's
        // partitions. This removes the single per-table manifest as a
        // serialization point (the old `append_data_files` had every partition
        // on every node CASing one version chain, which under bulk COPY became a
        // "lost commit race" storm and stalled ingest). On a single-chain
        // catalog (`InMemoryCatalog` / `PostgresCatalog`) the partition methods
        // default to the table-level behaviour, so nothing changes there.
        //
        // Hot-path note: read only the current snapshot id (O(1)), not the full
        // `load_table` metadata — the latter clones the entire snapshot chain.
        let pid = partition.as_str();
        let mut snapshot = match self
            .cfg
            .catalog
            .current_snapshot_id_in_partition(project, table, pid)
            .await
        {
            Ok(id) => id,
            Err(BasinError::NotFound(_)) => {
                self.cfg
                    .catalog
                    .create_table(project, table, batch.schema().as_ref())
                    .await?;
                // Fresh table → this partition's segment starts at genesis.
                self.cfg
                    .catalog
                    .current_snapshot_id_in_partition(project, table, pid)
                    .await?
            }
            Err(e) => return Err(e),
        };

        for attempt in 0..2 {
            match self
                .cfg
                .catalog
                .append_data_files_in_partition(project, table, pid, snapshot, files.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(BasinError::CommitConflict(_)) if attempt == 0 => {
                    snapshot = self
                        .cfg
                        .catalog
                        .current_snapshot_id_in_partition(project, table, pid)
                        .await?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(BasinError::CommitConflict(format!(
            "{project}/{table}[{pid}]: lost commit race twice"
        )))
    }

    async fn refresh_resident_stats(&self) {
        let map = self.partitions.lock().await;
        let mut stats = self.stats.lock().await;
        stats.resident_partitions = map.len();
        stats.resident_projects = unique_projects(&map);
    }

    /// S4 age-based residency — one clean-retention sweep tick.
    ///
    /// Reads the memtable-registry cell **on every call** (production wires
    /// the registry via `Shard::set_memtable_registry` after
    /// `spawn_background` has already started this loop) and, when wired,
    /// runs [`basin_hottier::MemTableRegistry::enforce_clean_budgets`]:
    /// clean (flushed-and-retained) rows older than
    /// `BASIN_HOTTIER_RETAIN_SECS` are evicted, and projects over
    /// `BASIN_HOTTIER_RETAIN_CAP` clean bytes are trimmed down to the cap.
    ///
    /// This sweep only releases redundant clean bytes — every evicted row is
    /// byte-identical to its cold image, so there is nothing to flush and no
    /// catalog/storage I/O. Dirty rows are untouched; the shard compactor
    /// remains the sole durable flusher. No-op until a registry is wired.
    async fn clean_retention_sweep(&self) {
        let registry = {
            let guard = self
                .memtable_registry_cell
                .read()
                .expect("memtable_registry_cell lock poisoned");
            guard.clone()
        };
        let Some(registry) = registry else {
            return;
        };
        let freed = registry.enforce_clean_budgets();
        if freed > 0 {
            let mut stats = self.stats.lock().await;
            stats.clean_sweep_evictions_bytes =
                stats.clean_sweep_evictions_bytes.saturating_add(freed);
            debug!(freed_bytes = freed, "clean-retention sweep evicted bytes");
        }
    }

    /// Stripe-merge compaction — one sweep tick.
    ///
    /// Round-robin striping historically spread a table's PKs across all
    /// write-stripe files; after PK-sorting each file individually, every
    /// file's zone map still spans nearly the whole PK range. The ranges
    /// OVERLAP, so a keyset-pagination predicate (`WHERE pk > $1 ORDER BY
    /// pk LIMIT k`) can't prune (`Gt` only rules a file out when its max
    /// <= v) and the read path opens every stripe file. This sweep merges
    /// such overlapping cold files into fewer files with strictly DISJOINT
    /// PK ranges, restoring 1-2 file opens per keyset page.
    ///
    /// Walks every resident project's tables and runs
    /// [`Self::stripe_merge_table`] on each, sequentially — combined with
    /// `stripe_merge_lock`, at most one table merges at a time
    /// process-wide, bounding the resources a merge pass can pin.
    /// Per-table errors are logged, never propagated (one bad table mustn't
    /// stall the rest — same convention as `tiering_sweep`).
    async fn stripe_merge_sweep(&self) -> Result<()> {
        // One merge pass at a time, even if a test driver overlaps a
        // background tick.
        let _merge_guard = self.stripe_merge_lock.lock().await;

        // Resident partitions are the ground truth for which projects to
        // sweep — identical scope to `tiering_sweep`.
        let projects: Vec<ProjectId> = {
            let map = self.partitions.lock().await;
            let mut seen: HashSet<ProjectId> = HashSet::new();
            for (t, _) in map.keys() {
                seen.insert(*t);
            }
            seen.into_iter().collect()
        };

        for project in projects {
            let tables = match self.cfg.catalog.list_tables(&project).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(%project, error = %e, "stripe-merge: list_tables failed; skipping project");
                    continue;
                }
            };
            for table in tables {
                if let Err(e) = self.stripe_merge_table(&project, &table).await {
                    warn!(
                        %project,
                        %table,
                        error = %e,
                        "stripe-merge failed for table; will retry next tick",
                    );
                }
            }
        }
        Ok(())
    }

    /// Merge `(project, table)`'s overlapping cold files into fewer files
    /// with disjoint PK ranges. Returns `Ok(true)` when a merge committed,
    /// `Ok(false)` when the table is not a candidate (or the pass was
    /// abandoned on a commit conflict — retried next tick).
    ///
    /// Candidate gate (all must hold, otherwise no-op):
    ///  * single-column PK that is also the table's *effective* on-disk
    ///    sort order (declared `cluster_columns == [pk]`, or no declared
    ///    clustering and [`TableMetadata::default_cluster_cols`] == `[pk]`)
    ///    — merging by PK must not undo a different declared clustering;
    ///  * >= `BASIN_STRIPE_MERGE_MIN_FILES` (default 4) live cold files,
    ///    EVERY one carrying decodable PK min/max `column_stats` (the zone
    ///    maps — without them disjointness can't be proven);
    ///  * the PK zone maps overlap (not totally ordered when sorted by min);
    ///  * total input bytes <= `BASIN_STRIPE_MERGE_MAX_BYTES` (default
    ///    256 MiB). The merge decodes the whole input set in memory (the
    ///    per-file streams could in principle be k-way merged since each
    ///    file is PK-sorted, but the read machinery yields whole-file
    ///    streams; the byte cap is the documented memory bound — note it
    ///    caps *encoded* bytes, decoded Arrow may be a small multiple).
    ///
    /// Commit protocol: the new file set replaces exactly the snapshotted
    /// input set via `Catalog::replace_data_files` (resulting live set =
    /// parent − removed ∪ added), under optimistic concurrency on the
    /// snapshot id. A concurrent tail flush APPENDS a file (advancing the
    /// snapshot): we reload, verify every input path is still live, and
    /// retry — the appended file is untouched by the replace. A concurrent
    /// engine CoW (UPDATE/DELETE) REPLACES input files: verification fails
    /// and the pass is abandoned (orphan outputs deleted; next tick re-reads
    /// the post-CoW state). Hot-tier overlays are untouched — the merge
    /// reads only committed cold files, and overlay suppression at
    /// read-merge applies on top of any file layout.
    async fn stripe_merge_table(&self, project: &ProjectId, table: &TableName) -> Result<bool> {
        let meta = match self.cfg.catalog.load_table(project, table).await {
            Ok(m) => m,
            Err(BasinError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };

        // Single-column PK that is the effective on-disk sort order.
        let [pk] = meta.pk_columns.as_slice() else {
            return Ok(false);
        };
        let pk = pk.clone();
        let pk_is_effective_sort = meta.cluster_columns == vec![pk.clone()]
            || (meta.cluster_columns.is_empty() && meta.default_cluster_cols() == vec![pk.clone()]);
        if !pk_is_effective_sort {
            return Ok(false);
        }
        let Ok(pk_field) = meta.schema.field_with_name(&pk) else {
            return Ok(false);
        };
        let pk_type = pk_field.data_type().clone();

        let min_files = env_u64(
            "BASIN_STRIPE_MERGE_MIN_FILES",
            STRIPE_MERGE_MIN_FILES_DEFAULT,
        )
        .max(2) as usize;
        let live = meta.live_data_files();
        if live.len() < min_files {
            return Ok(false);
        }

        // PK zone maps from the catalog's per-file column_stats. Skip the
        // table when any live file lacks a decodable PK min/max — we can
        // neither prove the inputs overlap nor that the merge is needed.
        let mut ranges: Vec<(PkBound, PkBound)> = Vec::with_capacity(live.len());
        for f in &live {
            let Some((mn, mx)) = f
                .column_stats
                .get(&pk)
                .and_then(|cs| decode_pk_range(&pk_type, cs))
            else {
                return Ok(false);
            };
            ranges.push((mn, mx));
        }
        if !pk_ranges_overlap(&mut ranges) {
            return Ok(false); // Already disjoint — keyset prune works.
        }

        let total_bytes: u64 = live.iter().map(|f| f.size_bytes).sum();
        let max_bytes = env_u64(
            "BASIN_STRIPE_MERGE_MAX_BYTES",
            STRIPE_MERGE_MAX_BYTES_DEFAULT,
        );
        if total_bytes > max_bytes {
            debug!(
                %project,
                %table,
                total_bytes,
                max_bytes,
                "stripe-merge: input set over BASIN_STRIPE_MERGE_MAX_BYTES; skipping",
            );
            return Ok(false);
        }

        // ── Read every input file (same read machinery the backfill sweep
        // uses), normalising promoted-JSONB shadow columns so historic
        // files concat cleanly with post-promotion ones.
        let mut per_file: Vec<RecordBatch> = Vec::with_capacity(live.len());
        for f in &live {
            let path = object_store::path::Path::from(f.path.clone());
            let stream = match self.cfg.storage.read_file(project, &path).await {
                Ok(s) => s,
                Err(e) => {
                    // A concurrent CoW may have deleted the file between the
                    // metadata load and this read; not an error — retry next
                    // tick against the fresh snapshot.
                    debug!(
                        %project,
                        %table,
                        path = %f.path,
                        error = %e,
                        "stripe-merge: input read failed (concurrent rewrite?); abandoning pass",
                    );
                    return Ok(false);
                }
            };
            let batches: Vec<RecordBatch> = stream
                .collect::<Vec<Result<RecordBatch>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            if batches.is_empty() {
                continue; // Zero-row file: nothing to carry, still replaced.
            }
            let schema = batches[0].schema();
            let one = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| BasinError::storage(format!("stripe-merge concat: {e}")))?;
            let one = backfill_promoted_columns(one, &meta.promoted_jsonb_paths)?;
            per_file.push(one);
        }
        if per_file.is_empty() {
            return Ok(false);
        }
        // Schema drift across files (e.g. mid-ALTER) — bail conservatively.
        let schema = per_file[0].schema();
        if per_file.iter().any(|b| b.schema() != schema) {
            debug!(
                %project,
                %table,
                "stripe-merge: input files disagree on schema; skipping table",
            );
            return Ok(false);
        }

        // ── Global PK sort.
        let merged = arrow::compute::concat_batches(&schema, &per_file)
            .map_err(|e| BasinError::storage(format!("stripe-merge concat all: {e}")))?;
        drop(per_file);
        let pk_idx = schema
            .index_of(&pk)
            .map_err(|e| BasinError::storage(format!("stripe-merge pk column: {e}")))?;
        let indices = arrow::compute::sort_to_indices(merged.column(pk_idx), None, None)
            .map_err(|e| BasinError::storage(format!("stripe-merge sort: {e}")))?;
        let sorted = arrow::compute::take_record_batch(&merged, &indices)
            .map_err(|e| BasinError::storage(format!("stripe-merge take: {e}")))?;
        drop(merged);
        let total_rows = sorted.num_rows();
        if total_rows == 0 {
            return Ok(false);
        }

        // ── Output sizing: aim for ~STRIPE_MERGE_TARGET_FILE_BYTES per
        // output (estimated from the inputs' encoded bytes), and always
        // strictly fewer outputs than inputs. Slicing the PK-sorted batch
        // yields strictly increasing, disjoint per-file PK ranges.
        let n_out = (total_bytes.div_ceil(STRIPE_MERGE_TARGET_FILE_BYTES).max(1) as usize)
            .min(live.len() - 1)
            .max(1);
        let rows_per = total_rows.div_ceil(n_out);

        // Write options mirror `compact_one` exactly: table format, PK
        // clustering (the writer re-sorts + computes column_stats), the
        // table's row-group sizing, and blooms on sort-order ∪ PK columns.
        let file_format = shard_map_file_format(meta.file_format);
        let mut bloom_cols = meta.global_sort_order.clone().unwrap_or_default();
        if !bloom_cols.contains(&pk) {
            bloom_cols.push(pk.clone());
        }
        let write_opts = basin_storage::WriteOptions {
            file_format,
            cluster_columns: vec![pk.clone()],
            row_block_size: meta.row_block_size,
            max_row_group_size: meta.row_group_rows,
            bloom_columns: bloom_cols,
            ..Default::default()
        };
        let partition = PartitionKey::default_key();

        let mut outputs: Vec<(basin_storage::DataFile, RecordBatch)> = Vec::with_capacity(n_out);
        let mut offset = 0usize;
        use arrow_array::Array as _;
        let pk_col = sorted.column(pk_idx).clone();
        while offset < total_rows {
            let mut len = rows_per.min(total_rows - offset);
            // Never split equal PK values across an output boundary: a
            // duplicate PK at the seam would leave the two outputs' ranges
            // touching (still "overlapping" to the selector), re-triggering
            // the merge every tick. PKs are unique in practice — this is a
            // cheap churn guard, evaluated only at the n_out-1 seams.
            while offset + len < total_rows
                && pk_col.slice(offset + len - 1, 1).to_data()
                    == pk_col.slice(offset + len, 1).to_data()
            {
                len += 1;
            }
            let chunk = sorted.slice(offset, len);
            offset += len;
            match self
                .cfg
                .storage
                .write_batch_with_options(project, table, &partition, &chunk, &write_opts)
                .await
            {
                Ok(df) => outputs.push((df, chunk)),
                Err(e) => {
                    // Clean up the outputs already written; nothing was
                    // committed, so this is pure orphan reclamation.
                    for (df, _) in &outputs {
                        let _ = self.cfg.storage.delete_file(project, &df.path).await;
                    }
                    return Err(e);
                }
            }
        }

        // ── Atomic catalog swap: old input set → new output set.
        let removed: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
        let added: Vec<DataFileRef> = outputs
            .iter()
            .map(|(df, _)| DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
                bloom_filters: df.bloom_filters.clone(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            })
            .collect();

        let mut snapshot = meta.current_snapshot;
        let commit_result: Result<bool> = 'commit: {
            for attempt in 0..3 {
                match self
                    .cfg
                    .catalog
                    .replace_data_files(project, table, snapshot, removed.clone(), added.clone())
                    .await
                {
                    Ok(_) => break 'commit Ok(true),
                    Err(BasinError::CommitConflict(_)) if attempt < 2 => {
                        // Someone advanced the snapshot. A tail-flush APPEND
                        // leaves our inputs live — safe to retry at the new
                        // snapshot. A CoW REPLACE invalidates an input —
                        // abandon (the merged bytes are stale).
                        let fresh = match self.cfg.catalog.load_table(project, table).await {
                            Ok(m) => m,
                            Err(e) => break 'commit Err(e),
                        };
                        let live_now: HashSet<String> = fresh
                            .live_data_files()
                            .into_iter()
                            .map(|f| f.path)
                            .collect();
                        if removed.iter().all(|p| live_now.contains(p)) {
                            snapshot = fresh.current_snapshot;
                            continue;
                        }
                        break 'commit Ok(false);
                    }
                    Err(BasinError::CommitConflict(_)) => break 'commit Ok(false),
                    Err(e) => break 'commit Err(e),
                }
            }
            Ok(false)
        };

        let committed = match commit_result {
            Ok(c) => c,
            Err(e) => {
                for (df, _) in &outputs {
                    let _ = self.cfg.storage.delete_file(project, &df.path).await;
                }
                return Err(e);
            }
        };
        if !committed {
            debug!(
                %project,
                %table,
                "stripe-merge: lost the commit race; outputs abandoned, retrying next tick",
            );
            for (df, _) in &outputs {
                let _ = self.cfg.storage.delete_file(project, &df.path).await;
            }
            return Ok(false);
        }

        // ── Index registry maintenance — outputs are registered EXACTLY as
        // `compact_one` registers its compacted file (same helpers), and the
        // superseded inputs are evicted so probes never see stale entries.
        let effective_rg_size = meta
            .row_block_size
            .map(|v| v as usize)
            .or(meta.row_group_rows)
            .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE);
        for (df, chunk) in &outputs {
            reindex_compacted_file_gin(
                &self.gin_rowgroup_registry,
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            );
            reindex_compacted_file_secondary(
                &self.secondary_index_registry,
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            );
            if let Err(e) = reindex_compacted_file_jsonb_posting(
                &self.jsonb_posting_registry,
                self.cfg.storage.clone(),
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            )
            .await
            {
                warn!(
                    %project,
                    %table,
                    file = %df.path,
                    error = %e,
                    "stripe-merge: JSONB posting sidecar write failed; @> falls back to bloom prune",
                );
            }
            if let Err(e) = reindex_compacted_file_rtree(
                self.cfg.storage.clone(),
                project,
                table,
                &meta.indexes,
                effective_rg_size as u32,
                chunk,
                df.path.as_ref(),
            )
            .await
            {
                warn!(
                    %project,
                    %table,
                    file = %df.path,
                    error = %e,
                    "stripe-merge: R-tree sidecar write failed; queries fall back to full scan",
                );
            }
        }

        // Evict the superseded inputs from the in-RAM registries (mirrors
        // the backfill sweep's GIN eviction, extended to the posting +
        // secondary registries `compact_one`'s append-only path never
        // needed).
        {
            let rg = self
                .gin_rowgroup_registry
                .read()
                .expect("gin_rowgroup_registry lock poisoned")
                .clone();
            let jp = self
                .jsonb_posting_registry
                .read()
                .expect("jsonb_posting_registry lock poisoned")
                .clone();
            let sec = self
                .secondary_index_registry
                .read()
                .expect("secondary_index_registry lock poisoned")
                .clone();
            for old in &removed {
                for idx in &meta.indexes {
                    if idx.columns.len() != 1 {
                        continue;
                    }
                    let col = &idx.columns[0];
                    match idx.access_method.as_str() {
                        "gin" => {
                            if let Some(r) = &rg {
                                r.remove_file(project, table, col, old);
                            }
                            if let Some(r) = &jp {
                                r.remove_file(project, table, col, old);
                            }
                        }
                        "btree" => {
                            if let Some(r) = &sec {
                                r.remove_file(project, table, col, old);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Delete the superseded objects best-effort (same pattern as the
        // tiering sweep — a leftover object is wasted bytes, not a
        // correctness issue: the catalog no longer references it).
        for old in &removed {
            let path = object_store::path::Path::from(old.clone());
            if let Err(e) = self.cfg.storage.delete_file(project, &path).await {
                warn!(
                    %project,
                    %table,
                    path = %old,
                    error = %e,
                    "stripe-merge: superseded file delete failed (orphan)",
                );
            }
        }

        {
            let mut stats = self.stats.lock().await;
            stats.stripe_merges = stats.stripe_merges.saturating_add(1);
        }
        tracing::info!(
            %project,
            %table,
            inputs = removed.len(),
            outputs = outputs.len(),
            rows = total_rows,
            "stripe-merge compaction complete: PK ranges now disjoint",
        );
        Ok(true)
    }

    /// File-count-bounding merge compaction — one sweep tick.
    ///
    /// Sustained single-node ingest appends ~one cold data file per flush to a
    /// partition's segment chain; nothing in the stripe-merge path coalesces
    /// files whose PK ranges are already disjoint (the monotonic-PK append
    /// case) or that have no single-column PK at all, so the per-partition file
    /// count grows without bound. Every O(files) operation then slowly grows —
    /// most importantly the periodic per-partition segment BASELINE write,
    /// which serialises the partition's FULL live set every K commits. That is
    /// the residual gentle ingest-rate decline at scale.
    ///
    /// This sweep keeps the count bounded: per `(project, partition)`, when the
    /// live cold-file count exceeds `BASIN_COMPACT_MAX_FILES_PER_PARTITION`
    /// (default 64), it merges the SMALLEST `BASIN_COMPACT_MERGE_BATCH`
    /// (default 16) files into fewer, larger files (each capped at the
    /// write-stripe target size) and commits the swap atomically via
    /// `replace_data_files_in_partition`. Bounded work per tick walks a hot
    /// partition down over several ticks rather than stalling ingest.
    ///
    /// Same process-wide serialisation as the stripe-merge (shares
    /// `stripe_merge_lock`), so at most one merge pass — stripe or file-count —
    /// runs at a time, bounding the resources a merge can pin.
    async fn file_merge_sweep(&self) -> Result<()> {
        let _merge_guard = self.stripe_merge_lock.lock().await;

        let projects: Vec<ProjectId> = {
            let map = self.partitions.lock().await;
            let mut seen: HashSet<ProjectId> = HashSet::new();
            for (t, _) in map.keys() {
                seen.insert(*t);
            }
            seen.into_iter().collect()
        };

        for project in projects {
            let tables = match self.cfg.catalog.list_tables(&project).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(%project, error = %e, "file-merge: list_tables failed; skipping project");
                    continue;
                }
            };
            for table in tables {
                let partitions = match self
                    .cfg
                    .catalog
                    .list_merge_partitions(&project, &table)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(%project, %table, error = %e, "file-merge: list_merge_partitions failed; skipping table");
                        continue;
                    }
                };
                for pid in partitions {
                    if let Err(e) = self.file_merge_partition(&project, &table, &pid).await {
                        warn!(
                            %project,
                            %table,
                            partition = %pid,
                            error = %e,
                            "file-merge failed for partition; will retry next tick",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Merge the smallest bounded batch of one partition's cold files into
    /// fewer, larger files when the live count is over the ceiling. Returns
    /// `Ok(true)` when a merge committed, `Ok(false)` for a no-op (under the
    /// ceiling, abandoned on a commit race, or nothing decodable to merge).
    ///
    /// Correctness: the output carries EXACTLY the union of the selected
    /// inputs' rows (a row-preserving concat — no sort, no dedup, so it is safe
    /// for any schema / clustering / PK shape, unlike stripe-merge). The
    /// catalog `replace_data_files_in_partition` atomically removes the N input
    /// paths and adds the outputs under per-partition OCC, so a concurrent
    /// reader sees either the pre- or post-merge file set, never a torn or
    /// double-counted set. A `count(*)` is therefore identical across the
    /// merge. Single-writer-per-partition means the only concurrent committer
    /// is a tail-flush APPEND (which leaves our inputs live → we retry at the
    /// new snapshot) — no cross-writer merge race.
    async fn file_merge_partition(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
    ) -> Result<bool> {
        let max_files = env_u64(
            "BASIN_COMPACT_MAX_FILES_PER_PARTITION",
            COMPACT_MAX_FILES_PER_PARTITION_DEFAULT,
        )
        .max(2) as usize;
        let batch_cap = env_u64("BASIN_COMPACT_MERGE_BATCH", COMPACT_MERGE_BATCH_DEFAULT).max(2)
            as usize;
        let max_bytes = env_u64(
            "BASIN_COMPACT_MERGE_MAX_BYTES",
            COMPACT_MERGE_MAX_BYTES_DEFAULT,
        );

        let (snapshot, live) = match self
            .cfg
            .catalog
            .live_data_files_in_partition(project, table, partition_id)
            .await
        {
            Ok(v) => v,
            Err(BasinError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };
        if live.len() <= max_files {
            return Ok(false); // Under the ceiling — nothing to do.
        }

        let meta = match self.cfg.catalog.load_table(project, table).await {
            Ok(m) => m,
            Err(BasinError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };

        // Pick the SMALLEST files first (coalesce the cheap tiny flush files),
        // bounded to `batch_cap` inputs AND `max_bytes` total encoded bytes so
        // a single tick's work — and its peak memory — stays bounded.
        let mut by_size = live.clone();
        by_size.sort_by_key(|f| f.size_bytes);
        let mut inputs: Vec<DataFileRef> = Vec::new();
        let mut input_bytes: u64 = 0;
        for f in by_size {
            if inputs.len() >= batch_cap {
                break;
            }
            // Always admit the first file even if it alone exceeds the byte cap
            // (otherwise a partition of large files could never shrink); stop
            // before exceeding the cap once we already hold >= 2.
            if inputs.len() >= 2 && input_bytes.saturating_add(f.size_bytes) > max_bytes {
                break;
            }
            input_bytes = input_bytes.saturating_add(f.size_bytes);
            inputs.push(f);
        }
        if inputs.len() < 2 {
            return Ok(false); // Need at least 2 to reduce the count.
        }

        // ── Read every selected input (same read machinery the stripe-merge
        // uses), normalising promoted-JSONB shadow columns so historic files
        // concat cleanly with post-promotion ones.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<arrow_schema::SchemaRef> = None;
        for f in &inputs {
            let path = object_store::path::Path::from(f.path.clone());
            let stream = match self.cfg.storage.read_file(project, &path).await {
                Ok(s) => s,
                Err(e) => {
                    // A concurrent CoW may have removed the file between the
                    // snapshot read and this read; retry next tick.
                    debug!(
                        %project,
                        %table,
                        partition = %partition_id,
                        path = %f.path,
                        error = %e,
                        "file-merge: input read failed (concurrent rewrite?); abandoning pass",
                    );
                    return Ok(false);
                }
            };
            let file_batches: Vec<RecordBatch> = stream
                .collect::<Vec<Result<RecordBatch>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            for b in file_batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let b = backfill_promoted_columns(b, &meta.promoted_jsonb_paths)?;
                match &schema {
                    None => schema = Some(b.schema()),
                    Some(s) if *s != b.schema() => {
                        // Schema drift across files (mid-ALTER) — bail
                        // conservatively rather than concat incompatible
                        // batches.
                        debug!(
                            %project,
                            %table,
                            partition = %partition_id,
                            "file-merge: input files disagree on schema; skipping partition",
                        );
                        return Ok(false);
                    }
                    _ => {}
                }
                batches.push(b);
            }
        }
        let Some(schema) = schema else {
            // Every selected input was zero-row: the swap still drops them,
            // collapsing N empty files to none (count(*) unchanged: 0 rows).
            let removed: Vec<String> = inputs.iter().map(|f| f.path.clone()).collect();
            return self
                .commit_file_merge(project, table, partition_id, snapshot, removed, Vec::new())
                .await;
        };

        // ── Row-preserving concat → re-emit into <= target-sized output files.
        // We DO NOT sort: the output is exactly the union of input rows, so the
        // merge is correctness-neutral for every table shape. Output count is
        // strictly fewer than inputs (we cap each output at the write-stripe
        // target and only merge when over the ceiling).
        let merged = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| BasinError::storage(format!("file-merge concat: {e}")))?;
        drop(batches);
        let total_rows = merged.num_rows();
        if total_rows == 0 {
            let removed: Vec<String> = inputs.iter().map(|f| f.path.clone()).collect();
            return self
                .commit_file_merge(project, table, partition_id, snapshot, removed, Vec::new())
                .await;
        }

        // Aim for ~target bytes per output (estimated from input bytes), and
        // ALWAYS strictly fewer outputs than inputs so the count drops.
        let n_out = (input_bytes
            .div_ceil(STRIPE_MERGE_TARGET_FILE_BYTES)
            .max(1) as usize)
            .min(inputs.len() - 1)
            .max(1);
        let rows_per = total_rows.div_ceil(n_out);

        let file_format = shard_map_file_format(meta.file_format);
        let mut bloom_cols = meta.global_sort_order.clone().unwrap_or_default();
        for pk in &meta.pk_columns {
            if !bloom_cols.contains(pk) {
                bloom_cols.push(pk.clone());
            }
        }
        let cluster_columns = if meta.cluster_columns.is_empty() {
            meta.default_cluster_cols()
        } else {
            meta.cluster_columns.clone()
        };
        let write_opts = basin_storage::WriteOptions {
            file_format,
            cluster_columns,
            row_block_size: meta.row_block_size,
            max_row_group_size: meta.row_group_rows,
            bloom_columns: bloom_cols,
            ..Default::default()
        };
        let write_partition = PartitionKey::default_key();

        let mut outputs: Vec<(basin_storage::DataFile, RecordBatch)> = Vec::with_capacity(n_out);
        let mut offset = 0usize;
        while offset < total_rows {
            let len = rows_per.min(total_rows - offset);
            let chunk = merged.slice(offset, len);
            offset += len;
            match self
                .cfg
                .storage
                .write_batch_with_options(project, table, &write_partition, &chunk, &write_opts)
                .await
            {
                Ok(df) => outputs.push((df, chunk)),
                Err(e) => {
                    for (df, _) in &outputs {
                        let _ = self.cfg.storage.delete_file(project, &df.path).await;
                    }
                    return Err(e);
                }
            }
        }
        drop(merged);

        let removed: Vec<String> = inputs.iter().map(|f| f.path.clone()).collect();
        let added: Vec<DataFileRef> = outputs
            .iter()
            .map(|(df, _)| DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
                bloom_filters: df.bloom_filters.clone(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            })
            .collect();

        // ── Atomic per-partition catalog swap (OCC on the partition snapshot).
        let mut snap = snapshot;
        let committed = 'commit: {
            for attempt in 0..3 {
                match self
                    .cfg
                    .catalog
                    .replace_data_files_in_partition(
                        project,
                        table,
                        partition_id,
                        snap,
                        removed.clone(),
                        added.clone(),
                    )
                    .await
                {
                    Ok(_) => break 'commit true,
                    Err(BasinError::CommitConflict(_)) if attempt < 2 => {
                        // The partition snapshot advanced. A tail-flush APPEND
                        // leaves our inputs live → retry at the new snapshot. A
                        // concurrent CoW REPLACE removes an input → abandon (the
                        // merged bytes are stale).
                        let (fresh_snap, fresh_live) = match self
                            .cfg
                            .catalog
                            .live_data_files_in_partition(project, table, partition_id)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                for (df, _) in &outputs {
                                    let _ = self.cfg.storage.delete_file(project, &df.path).await;
                                }
                                return Err(e);
                            }
                        };
                        let live_now: HashSet<String> =
                            fresh_live.into_iter().map(|f| f.path).collect();
                        if removed.iter().all(|p| live_now.contains(p)) {
                            snap = fresh_snap;
                            continue;
                        }
                        break 'commit false;
                    }
                    Err(BasinError::CommitConflict(_)) => break 'commit false,
                    Err(e) => {
                        for (df, _) in &outputs {
                            let _ = self.cfg.storage.delete_file(project, &df.path).await;
                        }
                        return Err(e);
                    }
                }
            }
            false
        };

        if !committed {
            debug!(
                %project,
                %table,
                partition = %partition_id,
                "file-merge: lost the commit race; outputs abandoned, retrying next tick",
            );
            for (df, _) in &outputs {
                let _ = self.cfg.storage.delete_file(project, &df.path).await;
            }
            return Ok(false);
        }

        self.reindex_and_reclaim_merge(project, table, &meta, &outputs, &removed)
            .await;

        {
            let mut stats = self.stats.lock().await;
            stats.file_merges = stats.file_merges.saturating_add(1);
        }
        tracing::info!(
            %project,
            %table,
            partition = %partition_id,
            inputs = removed.len(),
            outputs = outputs.len(),
            rows = total_rows,
            "file-merge compaction complete: partition file count bounded",
        );
        Ok(true)
    }

    /// Commit a pure-removal file-merge (all selected inputs were zero-row):
    /// drop the N input paths, add nothing. Count(*) is unchanged (0 rows
    /// removed). OCC-retried like the main path.
    async fn commit_file_merge(
        &self,
        project: &ProjectId,
        table: &TableName,
        partition_id: &str,
        snapshot: SnapshotId,
        removed: Vec<String>,
        added: Vec<DataFileRef>,
    ) -> Result<bool> {
        let mut snap = snapshot;
        for attempt in 0..3 {
            match self
                .cfg
                .catalog
                .replace_data_files_in_partition(
                    project,
                    table,
                    partition_id,
                    snap,
                    removed.clone(),
                    added.clone(),
                )
                .await
            {
                Ok(_) => {
                    let mut stats = self.stats.lock().await;
                    stats.file_merges = stats.file_merges.saturating_add(1);
                    return Ok(true);
                }
                Err(BasinError::CommitConflict(_)) if attempt < 2 => {
                    let (fresh_snap, fresh_live) = self
                        .cfg
                        .catalog
                        .live_data_files_in_partition(project, table, partition_id)
                        .await?;
                    let live_now: HashSet<String> =
                        fresh_live.into_iter().map(|f| f.path).collect();
                    if removed.iter().all(|p| live_now.contains(p)) {
                        snap = fresh_snap;
                        continue;
                    }
                    return Ok(false);
                }
                Err(BasinError::CommitConflict(_)) => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }

    /// Register the merge outputs in the in-RAM index registries, evict the
    /// superseded inputs, and best-effort delete the input objects. Mirrors the
    /// stripe-merge's post-commit maintenance exactly.
    async fn reindex_and_reclaim_merge(
        &self,
        project: &ProjectId,
        table: &TableName,
        meta: &basin_catalog::TableMetadata,
        outputs: &[(basin_storage::DataFile, RecordBatch)],
        removed: &[String],
    ) {
        let effective_rg_size = meta
            .row_block_size
            .map(|v| v as usize)
            .or(meta.row_group_rows)
            .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE);
        for (df, chunk) in outputs {
            reindex_compacted_file_gin(
                &self.gin_rowgroup_registry,
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            );
            reindex_compacted_file_secondary(
                &self.secondary_index_registry,
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            );
            if let Err(e) = reindex_compacted_file_jsonb_posting(
                &self.jsonb_posting_registry,
                self.cfg.storage.clone(),
                project,
                table,
                &meta.indexes,
                effective_rg_size,
                chunk,
                df.path.as_ref(),
            )
            .await
            {
                warn!(%project, %table, file = %df.path, error = %e, "file-merge: JSONB posting sidecar write failed");
            }
            if let Err(e) = reindex_compacted_file_rtree(
                self.cfg.storage.clone(),
                project,
                table,
                &meta.indexes,
                effective_rg_size as u32,
                chunk,
                df.path.as_ref(),
            )
            .await
            {
                warn!(%project, %table, file = %df.path, error = %e, "file-merge: R-tree sidecar write failed");
            }
        }

        {
            let rg = self
                .gin_rowgroup_registry
                .read()
                .expect("gin_rowgroup_registry lock poisoned")
                .clone();
            let jp = self
                .jsonb_posting_registry
                .read()
                .expect("jsonb_posting_registry lock poisoned")
                .clone();
            let sec = self
                .secondary_index_registry
                .read()
                .expect("secondary_index_registry lock poisoned")
                .clone();
            for old in removed {
                for idx in &meta.indexes {
                    if idx.columns.len() != 1 {
                        continue;
                    }
                    let col = &idx.columns[0];
                    match idx.access_method.as_str() {
                        "gin" => {
                            if let Some(r) = &rg {
                                r.remove_file(project, table, col, old);
                            }
                            if let Some(r) = &jp {
                                r.remove_file(project, table, col, old);
                            }
                        }
                        "btree" => {
                            if let Some(r) = &sec {
                                r.remove_file(project, table, col, old);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        for old in removed {
            let path = object_store::path::Path::from(old.clone());
            if let Err(e) = self.cfg.storage.delete_file(project, &path).await {
                warn!(%project, %table, path = %old, error = %e, "file-merge: superseded file delete failed (orphan)");
            }
        }
    }
}

fn unique_projects(map: &PartitionMap) -> usize {
    let mut seen: HashSet<ProjectId> = HashSet::new();
    for (t, _) in map.keys() {
        seen.insert(*t);
    }
    seen.len()
}

#[async_trait]
impl ShardImpl for InProcessShard {
    #[instrument(skip(self), fields(project = %project, partition = %partition))]
    async fn get(&self, project: &ProjectId, partition: &PartitionKey) -> Result<ProjectHandle> {
        // Phase 6.X.A: acquire the WRITER lease before exposing the partition
        // (no-op in no-lease mode). Errors if a peer holds a live lease. This
        // is the WRITE path only — reads must use `get_for_read`, which never
        // takes the lease (a read that touched `get` would steal/fence the
        // owner's writer lease, the multi-node data-loss bug).
        self.ensure_lease(project, partition).await?;
        self.build_handle(project, partition).await
    }

    async fn get_for_read(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<ProjectHandle> {
        // Read path: deliberately NO `ensure_lease`. The cross-node lease is a
        // writer lease; a read (e.g. a `count(*)` that unions every partition)
        // must never acquire or steal it. The handle reads this node's resident
        // tail for partitions it owns plus the shared flushed files via the
        // catalog + object store; it does not fetch a remote owner's un-flushed
        // tail (consistent-after-flush).
        self.build_handle(project, partition).await
    }

    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let me = self.clone();

        let join = tokio::spawn(async move {
            let mut shutdown = rx;
            let mut evict_tick = tokio::time::interval(me.cfg.eviction_interval);
            let mut compact_tick = tokio::time::interval(me.cfg.compaction_interval);
            // Phase 6.X.A: lease heartbeat. Only renews when a lease registry
            // is configured (heartbeat_renew is a no-op otherwise), so the
            // ticker firing in no-lease mode is harmless.
            let mut heartbeat_tick = tokio::time::interval(me.cfg.lease_renew_interval);
            // S4 clean-retention sweep. The tick always fires; each tick
            // re-reads the registry cell (wired late, after spawn) and is a
            // no-op until `Shard::set_memtable_registry` has been called.
            let mut sweep_tick = tokio::time::interval(me.cfg.clean_sweep_interval);
            // Stripe-merge compaction tick. `BASIN_STRIPE_MERGE_SECS=0`
            // (a zero interval) disables the sweep: the select arm below is
            // guarded out, and the placeholder cadence keeps
            // `tokio::time::interval` away from its zero-duration panic.
            let stripe_merge_enabled = !me.cfg.stripe_merge_interval.is_zero();
            let mut stripe_merge_tick = tokio::time::interval(if stripe_merge_enabled {
                me.cfg.stripe_merge_interval
            } else {
                std::time::Duration::from_secs(3600)
            });
            // Read-freshness convergence: a short tick that drains the residual
            // tail PROMPTLY once ingest goes idle, so `count(*)`/`max(id)`
            // (which read the catalog's segment-committed live files, NOT the
            // uncompacted tail) reflect every durably-committed row within
            // `quiesce_compact_interval` of the last write — instead of lagging
            // by up to a full `compaction_interval` (30 s) tick after a write
            // burst stops. The arm itself short-circuits while ingest is active
            // (see `quiesce_drain`), so it adds no steady-state ingest cost.
            // `BASIN_QUIESCE_COMPACT_SECS=0` disables it (zero-duration interval
            // would panic, so use the placeholder cadence and guard the arm).
            let quiesce_drain_enabled = !me.cfg.quiesce_compact_interval.is_zero();
            let mut quiesce_drain_tick = tokio::time::interval(if quiesce_drain_enabled {
                me.cfg.quiesce_compact_interval
            } else {
                std::time::Duration::from_secs(3600)
            });
            // Phase 2 autotune: the adaptive ingest-concurrency controller.
            // Spawned ONLY when BASIN_AUTOTUNE is on, and only ONCE per process
            // (the runtime fan-out / flush atomics are process-global, so a
            // single controller drives the whole node even with several shards).
            // When off, `autotune_state` is `None`, the select arm below is
            // guarded out entirely, and no override is ever published — a strict
            // no-op at the default. The first shard's background loop to start
            // wins the `Once`; later shards' loops leave it `None`.
            let mut autotune_state: Option<(
                basin_common::autotune::AdaptiveController,
                std::time::Instant,
            )> = if basin_common::autotune::autotune_enabled() && autotune_claim() {
                let controller = basin_common::autotune::build_controller();
                // Publish the derived start flush immediately (== derived, so no
                // behavior change until the first adjustment). Fan-out is NOT
                // published as a runtime override: it is pinned to the Phase-1
                // hardware-derived value for the life of the process, because
                // changing the partition topology mid-ingest double-counts the
                // LIST-based count(*) (see autotune.rs module comment).
                basin_common::autotune::publish_flush(controller.fanout());
                tracing::info!(
                    start_flush = controller.fanout(),
                    interval_secs = basin_common::autotune::tick_interval_secs(),
                    "BASIN_AUTOTUNE on: adaptive flush-concurrency controller started \
                     (fan-out pinned to hardware-derived value)"
                );
                Some((controller, std::time::Instant::now()))
            } else {
                None
            };
            let autotune_enabled = autotune_state.is_some();
            // Placeholder cadence when disabled keeps `interval` away from a
            // zero duration and the arm is `if autotune_enabled`-guarded anyway.
            let mut autotune_tick = tokio::time::interval(std::time::Duration::from_secs(
                if autotune_enabled {
                    basin_common::autotune::tick_interval_secs()
                } else {
                    3600
                },
            ));
            // First firing of `interval` is immediate; skip it so the loops
            // align with their configured cadence.
            evict_tick.tick().await;
            compact_tick.tick().await;
            heartbeat_tick.tick().await;
            sweep_tick.tick().await;
            stripe_merge_tick.tick().await;
            quiesce_drain_tick.tick().await;
            autotune_tick.tick().await;
            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = autotune_tick.tick(), if autotune_enabled => {
                        if let Some((controller, last)) = autotune_state.as_mut() {
                            let elapsed = last.elapsed().as_secs_f64();
                            *last = std::time::Instant::now();
                            let (rows, waves, stalls) =
                                basin_common::autotune::drain_signals();
                            // Idle interval (no committed work, no stalls): no
                            // signal, so hold the knob rather than walk on noise.
                            if rows == 0 && stalls == 0 {
                                continue;
                            }
                            let sample = basin_common::autotune::sample_from_signals(
                                rows, waves, stalls, elapsed,
                            );
                            // The controller drives FLUSH concurrency only — the
                            // sole exactly-once-safe runtime ingest knob. Fan-out
                            // stays pinned (never published), so the partition
                            // topology never changes mid-ingest.
                            let prev = controller.fanout();
                            let next = controller.observe(sample);
                            if next != prev {
                                basin_common::autotune::publish_flush(next);
                                // The controller emits the authoritative INFO
                                // log (smoothed rate + reason) on every change;
                                // this records the published knob + raw sample.
                                tracing::info!(
                                    rows_per_sec = sample.rows_per_sec,
                                    smoothed_rows_per_sec =
                                        controller.smoothed_rps().unwrap_or(sample.rows_per_sec),
                                    overload = sample.overload,
                                    "autotune: published flush concurrency {prev}\u{2192}{next}"
                                );
                            }
                        }
                    }
                    _ = evict_tick.tick() => {
                        if let Err(e) = me.evict_idle().await {
                            warn!(error = %e, "eviction tick failed");
                        }
                    }
                    _ = compact_tick.tick() => {
                        if let Err(e) = me.compact_all().await {
                            warn!(error = %e, "compaction tick failed");
                        }
                    }
                    _ = heartbeat_tick.tick() => {
                        me.heartbeat_renew().await;
                        // Phase 6.X.D — same cadence as the lease renew.
                        // Pushing on every renew tick keeps slices fresh
                        // within `lease_renew_interval` (default 5 s).
                        me.heartbeat_budgets().await;
                    }
                    _ = sweep_tick.tick() => {
                        me.clean_retention_sweep().await;
                    }
                    _ = quiesce_drain_tick.tick(), if quiesce_drain_enabled => {
                        if let Err(e) = me.quiesce_drain().await {
                            warn!(error = %e, "quiesce-drain tick failed");
                        }
                    }
                    _ = stripe_merge_tick.tick(), if stripe_merge_enabled => {
                        if let Err(e) = me.stripe_merge_sweep().await {
                            warn!(error = %e, "stripe-merge tick failed");
                        }
                        // Same cadence/lock domain: bound the per-partition
                        // file count so the segment baseline fold (and every
                        // other O(files) cost) stays flat under sustained
                        // ingest. Runs after stripe-merge so any disjoint-range
                        // coalescing happens first.
                        if let Err(e) = me.file_merge_sweep().await {
                            warn!(error = %e, "file-merge tick failed");
                        }
                    }
                }
            }
        });
        ShardBackgroundHandle { shutdown: tx, join }
    }

    fn stats(&self) -> ShardStats {
        // try_lock: stats is read-mostly and we don't want to block the caller
        // on the background loops' work. If contended, we return the last
        // stable view.
        match self.stats.try_lock() {
            Ok(guard) => guard.clone(),
            Err(_) => ShardStats::default(),
        }
    }

    async fn tail_pressure(&self) -> TailPressure {
        // Snapshot every resident partition state under the outer map mutex,
        // then release it before touching the per-partition RwLocks (same
        // discipline as `has_pending_tail`: never hold the global map lock
        // across an await). For each partition sum its tail bytes; track the
        // running total and the per-partition max.
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.values().cloned().collect()
        };
        let mut total: i64 = 0;
        let mut max_one: i64 = 0;
        for state in snapshot {
            let g = state.read().await;
            // O(columns) per tail batch; the tail holds few (large) batches.
            let part_bytes: i64 = g
                .tail
                .values()
                .flatten()
                .map(|(_, b)| b.get_array_memory_size() as i64)
                .sum();
            total = total.saturating_add(part_bytes);
            max_one = max_one.max(part_bytes);
        }
        TailPressure {
            total_tail_bytes: total,
            max_partition_tail_bytes: max_one,
            ingest_rows_per_sec: self.ingest_rate.rows_per_sec().min(i64::MAX as u64) as i64,
        }
    }

    fn clone_arc(&self) -> Arc<dyn ShardImpl> {
        // Returns a *new* outer Arc whose inner `InProcessShard` shares the
        // same `partitions` map and `stats` cell as the original via the
        // Arc'd handles. Used by `Shard::spawn_background`, which moves the
        // returned Arc into a tokio task.
        Arc::new(self.share_clone())
    }

    fn wal(&self) -> &Arc<dyn basin_wal::Wal> {
        &self.cfg.wal
    }

    fn replica_id(&self) -> &str {
        &self.cfg.replica_id
    }

    fn lease_registry(&self) -> Option<&Arc<dyn basin_catalog::LeaseRegistry>> {
        self.cfg.lease_registry.as_ref()
    }

    fn set_top_pattern_provider(&self, provider: Arc<dyn TopPatternProvider>) {
        let mut guard = self
            .top_pattern_provider
            .write()
            .expect("top_pattern_provider lock poisoned");
        *guard = Some(provider);
    }

    fn set_memtable_registry(&self, registry: Arc<basin_hottier::MemTableRegistry>) {
        let mut guard = self
            .memtable_registry_cell
            .write()
            .expect("memtable_registry_cell lock poisoned");
        *guard = Some(registry);
    }

    fn set_gin_rowgroup_registry(
        &self,
        registry: Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    ) {
        let mut guard = self
            .gin_rowgroup_registry
            .write()
            .expect("gin_rowgroup_registry lock poisoned");
        *guard = Some(registry);
    }

    fn set_jsonb_posting_registry(
        &self,
        registry: Arc<basin_storage::index::jsonb_posting::JsonbPostingRegistry>,
    ) {
        let mut guard = self
            .jsonb_posting_registry
            .write()
            .expect("jsonb_posting_registry lock poisoned");
        *guard = Some(registry);
    }

    fn set_secondary_index_registry(&self, registry: Arc<dyn crate::SecondaryIndexSink>) {
        let mut guard = self
            .secondary_index_registry
            .write()
            .expect("secondary_index_registry lock poisoned");
        *guard = Some(registry);
    }

    async fn flush_to_parquet(&self) -> Result<()> {
        self.compact_all().await
    }

    async fn run_stripe_merge_once(&self) -> Result<()> {
        self.stripe_merge_sweep().await
    }

    async fn run_file_merge_once(&self) -> Result<()> {
        self.file_merge_sweep().await
    }

    async fn run_tiering_sweep(&self) -> Result<()> {
        self.tiering_sweep().await
    }

    async fn run_promoted_column_backfill_sweep(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<usize> {
        self.promoted_column_backfill_sweep(project, table).await
    }

    async fn yield_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        to_holder: &str,
    ) -> Result<()> {
        InProcessShard::yield_partition(self, project, partition, to_holder).await
    }

    async fn resident_projects(&self) -> Vec<ProjectId> {
        let map = self.partitions.lock().await;
        let mut seen: HashSet<ProjectId> = HashSet::new();
        for (t, _) in map.keys() {
            seen.insert(*t);
        }
        seen.into_iter().collect()
    }

    async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        // Snapshot the relevant partition states under the outer mutex,
        // then release the mutex before touching each per-partition
        // RwLock so we don't hold the global map lock across awaits.
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.iter()
                .filter(|((p, _), _)| p == project)
                .map(|(_, state)| state.clone())
                .collect()
        };
        for state in snapshot {
            let mut g = state.write().await;
            g.tail.remove(table);
            g.schemas.remove(table);
            g.touch();
        }
        // Also drop the hot-tier MemTableRegistry entry, if one is wired
        // in via `Shard::set_memtable_registry`.  Today's INSERT path
        // through `InProcessProjectHandle` writes to the partition tail
        // above, not the registry, but the engine carries its own
        // MemTableRegistry that some code paths populate (e.g. constraint
        // enforcement snapshots).  Belt + braces: engine-level callers also
        // `remove` from their own registry in `exec_drop_table`; this branch
        // keeps the shard internally consistent and stops the
        // clean-retention sweep from carrying entries for dropped tables.
        let reg = {
            let guard = self
                .memtable_registry_cell
                .read()
                .expect("memtable_registry_cell lock poisoned");
            guard.clone()
        };
        if let Some(reg) = reg {
            reg.remove(project, table);
        }
        Ok(())
    }

    async fn has_pending_tail(&self, project: &ProjectId, table: &TableName) -> bool {
        // Snapshot this project's resident partition states under the outer
        // mutex, then release it before touching the per-partition RwLocks so
        // we never hold the global map lock across an await.
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.iter()
                .filter(|((p, _), _)| p == project)
                .map(|(_, state)| state.clone())
                .collect()
        };
        for state in snapshot {
            let g = state.read().await;
            if g.tail.get(table).map(|v| !v.is_empty()).unwrap_or(false) {
                return true;
            }
        }
        false
    }

    async fn pending_tail_rows(&self, project: &ProjectId, table: &TableName) -> usize {
        // Same O(resident-partitions) shape as `has_pending_tail`, but returns
        // the SUM of un-flushed tail rows for `(project, table)` across every
        // resident partition instead of a boolean. The read path uses this to
        // decide whether a small tail can be merged on-read (via the shard's
        // own tail-merging `handle.read`) rather than flushed — see the
        // `execute_simple_select` flush-decision gate. Like `has_pending_tail`
        // it never lists or scans object storage and never drains the tail; it
        // only reads the resident per-partition tail batch row counts under
        // their read locks. We hold only the per-partition RwLock (released
        // before the next iteration), never the global map lock across an await.
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.iter()
                .filter(|((p, _), _)| p == project)
                .map(|(_, state)| state.clone())
                .collect()
        };
        let mut rows = 0usize;
        for state in snapshot {
            let g = state.read().await;
            if let Some(v) = g.tail.get(table) {
                rows += v.iter().map(|(_, b)| b.num_rows()).sum::<usize>();
            }
        }
        rows
    }

    async fn read_table_merging_tails(
        &self,
        project: &ProjectId,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        // Cold read first: `storage.read` is (project, table)-scoped — it
        // already spans EVERY stripe partition's flushed data, so the cold
        // side needs no per-partition iteration. Forward all pushdown knobs.
        let parquet_opts = basin_storage::ReadOptions {
            projection: opts.projection.clone(),
            filters: opts.filters.clone(),
            partition: opts.partition.clone(),
            limit: opts.limit,
            row_group_selection: opts.row_group_selection.clone(),
            row_selection: opts.row_selection.clone(),
            sorted_by: opts.sorted_by.clone(),
        };
        let stream = self.cfg.storage.read(project, table, parquet_opts).await?;
        let mut out: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Merge the in-memory tail from EVERY resident partition for this
        // (project, table) — NOT just `default_key()`. Statement-affine
        // striping routes multi-row INSERTs into `s1`, `s2`, … partitions, so
        // a single-partition `ProjectHandle::read` would miss their un-flushed
        // tail rows (the read-own-write / keyset-seam losses). Same
        // O(resident-partitions) snapshot shape as `pending_tail_rows`: hold
        // the global map lock only to clone the per-partition state handles,
        // then read each tail under its own RwLock.
        let snapshot: Vec<Arc<RwLock<PartitionState>>> = {
            let map = self.partitions.lock().await;
            map.iter()
                .filter(|((p, _), _)| p == project)
                .map(|(_, state)| state.clone())
                .collect()
        };
        for state in snapshot {
            let tail_batches: Vec<RecordBatch> = {
                let g = state.read().await;
                match g.tail.get(table) {
                    Some(v) => v.iter().map(|(_, b)| b.clone()).collect(),
                    None => Vec::new(),
                }
            };
            for batch in tail_batches {
                let projected = match &opts.projection {
                    Some(cols) => project_batch(&batch, cols)?,
                    None => batch,
                };
                let filtered = apply_filters(projected, &opts.filters)?;
                if filtered.num_rows() > 0 {
                    out.push(filtered);
                }
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    fn as_in_process(&self) -> Option<Arc<InProcessShard>> {
        Some(Arc::new(self.share_clone()))
    }
}

/// Map the catalog's per-table format to the storage writer's format.
/// Mirrors `basin_engine::executor::map_file_format` (kept local — the
/// shard crate must not depend on basin-engine).
fn shard_map_file_format(f: basin_catalog::TableFileFormat) -> basin_storage::FileFormat {
    match f {
        basin_catalog::TableFileFormat::Parquet => basin_storage::FileFormat::Parquet,
        basin_catalog::TableFileFormat::Vortex => basin_storage::FileFormat::Vortex,
    }
}

fn decode_le_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(bytes);
    Some(i64::from_le_bytes(a))
}

// ── Stripe-merge compaction helpers ──────────────────────────────────────────

/// Default minimum live-file count before a table is a stripe-merge
/// candidate (`BASIN_STRIPE_MERGE_MIN_FILES`; clamped to >= 2).
const STRIPE_MERGE_MIN_FILES_DEFAULT: u64 = 4;

/// Default cap on the total *encoded* bytes a single stripe-merge pass may
/// decode (`BASIN_STRIPE_MERGE_MAX_BYTES`). The merge materialises the whole
/// input set as Arrow in memory; tables over the cap are skipped (and
/// logged at debug) rather than risk an unbounded allocation.
const STRIPE_MERGE_MAX_BYTES_DEFAULT: u64 = 256 * 1024 * 1024;

/// Target encoded size per merge output file. With the default write-stripe
/// fan-out the inputs are each well under this, so the common case merges
/// 8 stripe files into 1; very large tables split into multiple outputs
/// whose PK ranges are still strictly disjoint (slices of one sorted batch).
const STRIPE_MERGE_TARGET_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Default per-partition live-data-file ceiling before the file-count-bounding
/// merge sweep kicks in (`BASIN_COMPACT_MAX_FILES_PER_PARTITION`; clamped
/// `>= 2`). Sustained ingest appends ~one file per flush to a partition's
/// segment chain; left unmerged the count grows without bound and every
/// O(files) operation — most importantly the periodic segment baseline fold
/// (which serialises the partition's FULL live set) — slowly grows, producing
/// the residual ingest-rate decline at scale. Bounding the count bounds those
/// costs → flat ingest.
const COMPACT_MAX_FILES_PER_PARTITION_DEFAULT: u64 = 64;

/// Default cap on how many input files a SINGLE file-merge tick consumes for
/// one partition (`BASIN_COMPACT_MERGE_BATCH`; clamped `>= 2`). Bounding the
/// per-tick work keeps the merge OFF the hot path: a partition far over the
/// ceiling is walked down over several ticks instead of stalling ingest by
/// rewriting everything at once.
const COMPACT_MERGE_BATCH_DEFAULT: u64 = 16;

/// Default cap on the total *encoded* bytes a single file-merge pass may decode
/// in memory (`BASIN_COMPACT_MERGE_MAX_BYTES`). Mirrors the stripe-merge byte
/// bound — the selected batch is materialised as Arrow before re-emitting.
const COMPACT_MERGE_MAX_BYTES_DEFAULT: u64 = 256 * 1024 * 1024;

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

/// A decoded PK zone-map bound. Within one table every bound is the same
/// variant (one column, one type), so the derived `PartialOrd` — which
/// orders mismatched variants by discriminant — never sees a mixed compare
/// in practice.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum PkBound {
    Int(i64),
    Float(f64),
}

/// Decode a file's PK `[min, max]` from catalog [`basin_catalog::ColumnStats`]
/// using the same byte contract the writer emits (`column_stats_from_batch`:
/// 8-byte LE `i64` for Int64/Timestamp, 8-byte LE `f64` for Float64). Other
/// PK types carry no writer stats and return `None` — the caller skips the
/// table (no zone map, no provable disjointness).
fn decode_pk_range(
    dt: &arrow_schema::DataType,
    cs: &basin_catalog::ColumnStats,
) -> Option<(PkBound, PkBound)> {
    use arrow_schema::DataType;
    let mn = cs.min_bytes.as_deref()?;
    let mx = cs.max_bytes.as_deref()?;
    match dt {
        DataType::Int64 | DataType::Timestamp(_, _) => Some((
            PkBound::Int(decode_le_i64(mn)?),
            PkBound::Int(decode_le_i64(mx)?),
        )),
        DataType::Float64 => {
            let f = |b: &[u8]| -> Option<f64> {
                let a: [u8; 8] = b.try_into().ok()?;
                Some(f64::from_le_bytes(a))
            };
            Some((PkBound::Float(f(mn)?), PkBound::Float(f(mx)?)))
        }
        _ => None,
    }
}

/// True when the per-file PK `[min, max]` ranges overlap — i.e. they are NOT
/// totally ordered once sorted by min, so a range predicate (`pk > $1`)
/// cannot prune files. Sorts `ranges` in place by min.
fn pk_ranges_overlap(ranges: &mut [(PkBound, PkBound)]) -> bool {
    ranges.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    ranges.windows(2).any(|w| w[1].0 <= w[0].1)
}

struct InProcessProjectHandle {
    project: ProjectId,
    partition: PartitionKey,
    state: Arc<RwLock<PartitionState>>,
    cfg: ShardConfig,
    /// Phase 6.X.A — shared view of the leases this replica holds, so the
    /// write path can fence WAL appends with the current epoch. Empty in
    /// no-lease mode (the WAL append then runs unfenced / back-compat).
    held_leases: HeldLeases,
    /// Phase 6.X.C — shared view of partitions mid-handoff. The write path
    /// short-circuits with `BasinError::LeaseHandoffInProgress` while this
    /// partition is draining so the router can retry against the new owner.
    /// Reads are unaffected.
    draining: DrainingSet,
    /// Bounded-tail backpressure: a shard view (sharing the same partitions
    /// map + registries via `share_clone`) the write path uses to flush the
    /// in-memory tail to immutable files when it crosses the hard cap, so RAM
    /// stays bounded under sustained fast ingest instead of buffering the whole
    /// table. Reuses the exact `compact_one` flush (index sidecars included) so
    /// a backpressure flush is indistinguishable from a background-tick flush.
    shard: Arc<InProcessShard>,
}

impl InProcessProjectHandle {
    /// Shared core of `write_batch` (async ack) and `write_batch_durable`
    /// (synchronous_commit = on). Identical except for the WAL append mode:
    /// `durable` routes through `Wal::append_fenced_durable`, which
    /// group-commits and only returns once the entry is durable on the
    /// backing store.
    async fn write_batch_inner(
        &self,
        table: &TableName,
        batch: RecordBatch,
        durable: bool,
    ) -> Result<Lsn> {
        // Phase 6.X.C: short-circuit if this partition is mid-handoff.
        // The router treats `LeaseHandoffInProgress` as retryable: it
        // invalidates its lease cache for the partition and retries against
        // the new owner once the handoff completes.
        {
            let d = self.draining.lock().await;
            if d.contains(&(self.project, self.partition.clone())) {
                return Err(BasinError::lease_handoff_in_progress(format!(
                    "{}/{}",
                    self.project, self.partition
                )));
            }
        }
        let payload = encode_payload(table, &batch)?;
        // Phase 6.X.A: fence the WAL append with our current lease epoch.
        // `None` (no-lease mode) appends unconditionally — back-compat. A
        // stale-epoch append (lost-lease dual writer) is rejected at the WAL.
        let epoch = {
            let held = self.held_leases.lock().await;
            held.get(&(self.project, self.partition.clone())).copied()
        };
        // Multi-node phase 1 (BASIN_LEASE_MODE=required): a write may only
        // proceed under a held writer lease. No epoch means the lease was
        // never granted, the heartbeat lost it (renew failed / CAS stolen),
        // or the coordinator is unreachable — in every case the write is
        // refused with the typed `LeaseNotHeld` *before* it reaches the WAL,
        // so no LSN is consumed and the router can re-resolve the owner and
        // retry. Reads are unaffected (they never pass through here). Off
        // mode keeps the Phase 6.X.A fallback: epoch-less writes append
        // unfenced, exactly as before.
        if epoch.is_none() && self.cfg.lease_mode == LeaseMode::Required {
            return Err(BasinError::lease_not_held(format!(
                "{}/{}",
                self.project, self.partition
            )));
        }
        let lsn = if durable {
            self.cfg
                .wal
                .append_fenced_durable(&self.project, &self.partition, payload, epoch)
                .await?
        } else {
            self.cfg
                .wal
                .append_fenced(&self.project, &self.partition, payload, epoch)
                .await?
        };

        let tail_bytes = {
            let mut guard = self.state.write().await;
            guard
                .tail
                .entry(table.clone())
                .or_default()
                .push((lsn, batch.clone()));
            guard
                .schemas
                .entry(table.clone())
                .or_insert_with(|| batch.schema());
            guard.touch();
            // Sum the resident (uncompacted) tail bytes across this partition's
            // tables. `get_array_memory_size` is O(columns) per batch and the
            // tail holds few (large) batches, so this is cheap.
            guard
                .tail
                .values()
                .flatten()
                .map(|(_, b)| b.get_array_memory_size())
                .sum::<usize>()
        };

        // Feed the rolling ingest-rate counter (autoscaler `ingest_rows_per_sec`).
        // Lock-free; counts every WAL-ack'd row on the hot write path.
        self.shard.ingest_rate.record(batch.num_rows() as u64);

        // BOUNDED-MEMORY backpressure (#tail-oom): the in-memory tail grows with
        // ingest rate; for fast/wide COPY a writer can outrun the time-ticked
        // (30 s) compactor and, with no flow control, buffer the whole table in
        // RAM and OOM the box (observed in a 1B dev run: the COPY connection was
        // dropped around 450M rows). Two thresholds:
        //
        //   * SOFT cap (`MAX_TAIL_BYTES`): emit an early-warning WARN so tail
        //     pressure is observable to the operator / autoscaler.
        //   * HARD cap (`HARD_TAIL_BYTES`): synchronously flush the tail to
        //     immutable files BEFORE returning. The writer now paces itself to
        //     object-store flush throughput instead of buffering unboundedly —
        //     ingest slows gracefully under pressure, it never crashes. The
        //     flush reuses `compact_one`, which drains in bounded
        //     (`MAX_COMPACTION_ROWS`) passes, so a single backpressure stall
        //     costs bounded file writes, not an O(table) rewrite.
        if tail_bytes >= max_tail_bytes() {
            tracing::warn!(
                project = %self.project,
                partition = %self.partition,
                tail_mib = tail_bytes / (1024 * 1024),
                "shard tail over soft cap; compaction is falling behind ingest \
                 (will hard-flush at the backpressure cap)",
            );
            // PROACTIVE DRAIN (flat-ingest under slow flushes): kick off a
            // NON-BLOCKING background `compact_one` as soon as the tail crosses
            // the soft cap, so the tail drains CONCURRENTLY with ongoing ingest
            // instead of waiting for the 30 s background tick or the blocking
            // hard-cap flush. This is the wedge that keeps sustained ingest
            // tracking object-store write BANDWIDTH (the drain runs in parallel
            // with the next writes) rather than single-PUT latency (a blocking
            // full-tail drain at the hard cap, which is what produced the
            // throughput cliff once the tail saturated against a high-RTT
            // object store).
            //
            // `compact_one` already serializes per partition via the partition
            // compaction lock and is duplicate-safe (a racing background tick or
            // hard-flush that loses the lock finds a drained tail and no-ops).
            // We use `try_lock` here purely to avoid piling up redundant spawns:
            // if a drain is already in flight we skip — the in-flight one is
            // already draining toward the floor. Memory stays bounded because
            // the hard cap below still blocks the writer if the proactive drain
            // can't keep up.
            if let Ok(guard) = self.state.try_read() {
                let lock = guard.compact_lock.clone();
                drop(guard);
                // Probe the per-partition compaction lock without blocking: if
                // it is free, no drain is in flight, so we spawn one. The probe
                // guard is explicitly dropped BEFORE the spawn so the spawned
                // `compact_one` can re-acquire the same lock (no double-hold /
                // deadlock).
                let free = {
                    let probe = lock.try_lock();
                    let was_free = probe.is_ok();
                    drop(probe);
                    was_free
                };
                if free {
                    let shard = self.shard.clone();
                    let project = self.project;
                    let partition = self.partition.clone();
                    let state = self.state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = shard.compact_one(&project, &partition, state).await {
                            tracing::warn!(
                                %project,
                                %partition,
                                error = %e,
                                "proactive soft-cap tail drain failed; hard cap will backstop",
                            );
                        }
                    });
                }
            }
        }
        if tail_bytes >= hard_tail_bytes() {
            // Phase 2 autotune OVERLOAD signal: hitting the hard cap means the
            // writer is about to BLOCK on a synchronous compaction because the
            // in-memory tail outran the compactor — i.e. compaction is not
            // keeping pace with ingest, the exact regime over-fanning produces
            // on a shared vCPU. One relaxed atomic add; read only by the
            // controller tick (autotune on). Off → written, never observed.
            basin_common::autotune::record_overload_stall();
            // Flush the tail to immutable files. `compact_one` serializes per
            // partition via the partition's compaction lock, so a concurrent
            // background tick and this backpressure flush cannot double-commit
            // the same entries — whichever loses the race finds a drained tail
            // and no-ops. A flush failure is surfaced but NOT fatal to the
            // write (the rows are already durable in the WAL); the next tick /
            // write retries. We do not propagate the error so a transient
            // object-store hiccup does not fail an otherwise-committed COPY row.
            if let Err(e) = self
                .shard
                .compact_one(&self.project, &self.partition, self.state.clone())
                .await
            {
                tracing::warn!(
                    project = %self.project,
                    partition = %self.partition,
                    error = %e,
                    "backpressure tail flush failed; tail stays resident, next write/tick retries",
                );
            }
        }
        // Surface the WAL LSN this batch was appended at (Stage 1 of the
        // end-of-COPY durable barrier): the bulk-ingest caller accumulates the
        // max LSN per touched partition so `on_copy_done` can await durability
        // through it before acking `COPY n`.
        Ok(lsn)
    }
}

#[async_trait]
impl ProjectHandleImpl for InProcessProjectHandle {
    #[instrument(skip(self, batch), fields(project = %self.project, partition = %self.partition, table = %table, rows = batch.num_rows()))]
    async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<Lsn> {
        self.write_batch_inner(table, batch, false).await
    }

    #[instrument(skip(self, batch), fields(project = %self.project, partition = %self.partition, table = %table, rows = batch.num_rows()))]
    async fn write_batch_durable(&self, table: &TableName, batch: RecordBatch) -> Result<Lsn> {
        self.write_batch_inner(table, batch, true).await
    }

    #[instrument(skip(self, opts), fields(project = %self.project, partition = %self.partition, table = %table))]
    async fn read(&self, table: &TableName, opts: ReadOptions) -> Result<Vec<RecordBatch>> {
        // Stream the Parquet base. Forward all knobs from the caller's opts
        // (including LIMIT and row-group selection) so storage-level
        // pushdowns reach the reader.
        let parquet_opts = ReadOptions {
            projection: opts.projection.clone(),
            filters: opts.filters.clone(),
            partition: opts.partition.clone(),
            limit: opts.limit,
            row_group_selection: opts.row_group_selection.clone(),
            row_selection: opts.row_selection.clone(),
            sorted_by: opts.sorted_by.clone(),
        };
        let stream = self
            .cfg
            .storage
            .read(&self.project, table, parquet_opts)
            .await?;
        let mut out: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Append the in-memory tail. v0.1 filter handling on the tail is a
        // simple full scan; the Parquet path already pushed predicates down.
        let tail_batches: Vec<RecordBatch> = {
            let guard = self.state.read().await;
            match guard.tail.get(table) {
                Some(v) => v.iter().map(|(_, b)| b.clone()).collect(),
                None => Vec::new(),
            }
        };

        for batch in tail_batches {
            let projected = match &opts.projection {
                Some(cols) => project_batch(&batch, cols)?,
                None => batch,
            };
            let filtered = apply_filters(projected, &opts.filters)?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }

        // Touch the partition so reads keep it warm.
        self.state.write().await.touch();
        Ok(out)
    }

    fn last_active(&self) -> Instant {
        // try_read so a concurrent writer can't starve out callers asking for
        // the timestamp. If contended, return now() as a conservative answer
        // (errs on the side of "active").
        match self.state.try_read() {
            Ok(g) => g.last_active,
            Err(_) => Instant::now(),
        }
    }

    fn project(&self) -> ProjectId {
        self.project
    }

    fn partition(&self) -> PartitionKey {
        self.partition.clone()
    }

    async fn await_durable(&self, lsn: Lsn) -> Result<()> {
        // Delegate to the WAL's per-partition durable barrier. The WAL raises
        // the fsync watermark to `lsn`, nudges the flush loop, and blocks until
        // the published durable watermark covers `lsn` (the segment containing
        // it has been PUT, and fsync'd on an `FsyncOnPut`-wrapped local store).
        self.cfg
            .wal
            .await_durable(&self.project, &self.partition, lsn)
            .await
    }
}

/// Encode a `(table, batch)` pair as a single WAL payload.
///
/// See module docs for the exact layout.
fn encode_payload(table: &TableName, batch: &RecordBatch) -> Result<Bytes> {
    let table_bytes = table.as_str().as_bytes();
    if table_bytes.len() > u32::MAX as usize {
        return Err(BasinError::internal("table name length overflows u32"));
    }
    let mut buf: Vec<u8> = Vec::with_capacity(4 + table_bytes.len() + 1024);
    buf.extend_from_slice(&(table_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(table_bytes);

    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| BasinError::storage(format!("ipc writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| BasinError::storage(format!("ipc write: {e}")))?;
        writer
            .finish()
            .map_err(|e| BasinError::storage(format!("ipc finish: {e}")))?;
    }
    Ok(Bytes::from(buf))
}

/// Inverse of [`encode_payload`].
fn decode_payload(bytes: &[u8]) -> Result<(TableName, Vec<RecordBatch>)> {
    if bytes.len() < 4 {
        return Err(BasinError::wal(
            "WAL payload shorter than 4 bytes".to_string(),
        ));
    }
    let tlen = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + tlen {
        return Err(BasinError::wal(format!(
            "WAL payload truncated: tlen={tlen} but only {} bytes after header",
            bytes.len() - 4
        )));
    }
    let table_str = std::str::from_utf8(&bytes[4..4 + tlen])
        .map_err(|e| BasinError::wal(format!("WAL table name not UTF-8: {e}")))?;
    let table = TableName::new(table_str)?;

    let mut cursor = Cursor::new(&bytes[4 + tlen..]);
    let reader = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| BasinError::storage(format!("ipc reader init: {e}")))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| BasinError::storage(format!("ipc read: {e}")))?;
        batches.push(batch);
    }
    Ok((table, batches))
}

async fn replay_wal_into(
    wal: &Arc<dyn basin_wal::Wal>,
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &ProjectId,
    partition: &PartitionKey,
    state: &mut PartitionState,
) -> Result<()> {
    // DURABILITY (compaction watermark — duplicate-row crash-window fix):
    // replay only WAL entries strictly ABOVE the persisted compaction
    // watermark. `compact_one` records the watermark BEFORE truncating the
    // WAL, so if a crash left already-committed entries in the WAL (commit
    // landed, truncate didn't), they sit at or below the watermark and are
    // skipped here — preventing the next compaction from committing a
    // duplicate file for rows already in the cold tier.
    //
    // Legacy-catalog tolerance: a catalog written before the watermark
    // table/column existed (or any backend that doesn't persist one — the
    // in-memory / REST default returns `None`) yields `None` here, and we fall
    // back to the prior `Lsn::ZERO` behavior. That is correct for those
    // backends because their catalog and WAL are durable-or-lost together (an
    // ephemeral catalog cannot outlive a WAL it would re-replay against).
    let floor = match catalog
        .get_compaction_watermark(project, partition.as_str())
        .await
    {
        Ok(Some(lsn)) => Lsn(lsn),
        Ok(None) => Lsn::ZERO,
        Err(e) => {
            // A watermark read failure must not silently widen the replay floor
            // (that would re-introduce the duplicate); but it also must not
            // strand recovery. Fall back to ZERO (the legacy behavior) and warn
            // — at worst we re-create the historical duplicate-file exposure for
            // this one cold-start, which is strictly no worse than pre-fix.
            warn!(
                %project,
                %partition,
                error = %e,
                "compaction watermark read failed; replaying from Lsn::ZERO (legacy fallback)",
            );
            Lsn::ZERO
        }
    };
    // Seed the in-memory marker so the first post-restart compaction's
    // `last_compacted_lsn` bookkeeping starts from the durable floor.
    if state.last_compacted_lsn < floor {
        state.last_compacted_lsn = floor;
    }
    let entries = wal.read_from(project, partition, floor).await?;
    for entry in entries {
        let (table, batches) = decode_payload(&entry.payload)?;
        let table_tail = state.tail.entry(table.clone()).or_default();
        for batch in batches {
            if state
                .schemas
                .get(&table)
                .map(|s| s.fields().len())
                .is_none()
            {
                state.schemas.insert(table.clone(), batch.schema());
            }
            table_tail.push((entry.lsn, batch));
        }
    }

    // Reconstruct serial-sequence cursors from the data we just recovered.
    // WAL replay restores committed rows with their original id values, but the
    // in-memory catalog's sequence cursor resets to its start on restart (the
    // catalog is volatile; the WAL + object store are the durable source of
    // truth). Without this, `nextval` would re-hand-out an id that already
    // exists in the recovered data -> a spurious primary-key collision on the
    // next insert (this is the credential-provisioning `duplicate key (id)=(N)`
    // bug). Realign each SERIAL/identity sequence to MAX(recovered id) so the
    // sequence resumes exactly where the durable data left off — Basin recovers
    // this from its own WAL + storage, with no external dependency.
    reconcile_serial_sequences(catalog, project, state).await;

    debug!(
        %project,
        %partition,
        tables = state.tail.len(),
        "replayed WAL into partition state",
    );
    Ok(())
}

/// Catalog field-metadata key for a SERIAL/identity column's backing sequence
/// (mirrors `basin_engine::types::BASIN_IDENTITY_SEQ`, kept as a literal here so
/// `basin-shard` doesn't depend on the engine crate).
const BASIN_IDENTITY_SEQ_KEY: &str = "BASIN_IDENTITY_SEQ";
/// Catalog field-metadata key carrying a column's `DEFAULT <expr>` source text
/// (mirrors `basin_engine::types::BASIN_COLUMN_DEFAULT`). For SERIAL columns
/// this is `nextval('<seq>')`, from which we recover the sequence name.
const BASIN_COLUMN_DEFAULT_KEY: &str = "BASIN_COLUMN_DEFAULT";

/// Return the backing sequence name for a SERIAL/identity field, or `None` for
/// a plain column. Prefers the explicit `BASIN_IDENTITY_SEQ` marker; falls back
/// to parsing `nextval('<seq>')` out of the column's DEFAULT expression.
fn serial_sequence_name(field: &arrow_schema::Field) -> Option<String> {
    if let Some(seq) = field.metadata().get(BASIN_IDENTITY_SEQ_KEY) {
        if !seq.is_empty() {
            return Some(seq.clone());
        }
    }
    let default = field.metadata().get(BASIN_COLUMN_DEFAULT_KEY)?;
    parse_nextval_sequence(default)
}

/// Extract `<seq>` from a `nextval('<seq>')` expression (case-insensitive on the
/// function name; tolerant of surrounding whitespace and an optional `::regclass`
/// cast). Returns `None` for any other default expression.
fn parse_nextval_sequence(default_expr: &str) -> Option<String> {
    let s = default_expr.trim();
    let lower = s.to_ascii_lowercase();
    let rest = lower.strip_prefix("nextval")?;
    let open = rest.find('(')?;
    // Work on the original (case-preserving) string from the '(' onward.
    let after_paren = &s[("nextval".len() + open + 1)..];
    let inner = after_paren.split(')').next()?.trim();
    // Strip an optional `::regclass` cast, then surrounding quotes.
    let inner = inner.split("::").next().unwrap_or(inner).trim();
    let unquoted = inner
        .trim_matches('\'')
        .trim_matches('"')
        .trim();
    if unquoted.is_empty() {
        None
    } else {
        Some(unquoted.to_string())
    }
}

/// Largest i64-coercible value of `col_name` across `batches` (Int64/Int32/Int16
/// serial columns). `None` when the column is absent, non-integer, or all-null.
fn column_i64_max(batches: &[(Lsn, RecordBatch)], col_name: &str) -> Option<i64> {
    use arrow_array::{Array, Int16Array, Int32Array, Int64Array};
    let mut best: Option<i64> = None;
    for (_lsn, batch) in batches {
        let Some(col) = batch.column_by_name(col_name) else {
            continue;
        };
        let m = if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            arrow::compute::max(a)
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            arrow::compute::max(a).map(i64::from)
        } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
            arrow::compute::max(a).map(i64::from)
        } else {
            None
        };
        if let Some(m) = m {
            best = Some(best.map_or(m, |b| b.max(m)));
        }
    }
    best
}

/// Decode a column-stats `max_bytes` blob into an i64 when it is a
/// little-endian fixed-width integer (Parquet physical encoding for INT32 /
/// INT64). Best-effort: returns `None` for any other width/encoding, in which
/// case the caller falls back to the (authoritative for serial) tail maximum.
fn decode_le_int_max(max_bytes: &Option<Vec<u8>>) -> Option<i64> {
    let b = max_bytes.as_ref()?;
    match b.len() {
        8 => Some(i64::from_le_bytes(b[..8].try_into().ok()?)),
        4 => Some(i64::from(i32::from_le_bytes(b[..4].try_into().ok()?))),
        2 => Some(i64::from(i16::from_le_bytes(b[..2].try_into().ok()?))),
        _ => None,
    }
}

/// Realign every SERIAL/identity sequence in this partition's recovered tables
/// to the maximum id present in the durable data, so `nextval` resumes past it.
/// Combines the tail maximum (authoritative for the just-replayed uncompacted
/// rows — where a serial column's newest/highest ids live) with the cold
/// column-stats maximum (covers a fully-compacted table whose tail is empty).
async fn reconcile_serial_sequences(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &ProjectId,
    state: &PartitionState,
) {
    for table in state.schemas.keys() {
        // Cold (compacted) stats are best-effort: a missing/unknown catalog
        // entry just means we rely on the tail maximum.
        let cold = catalog.load_table(project, table).await.ok();
        // Detect serial/identity columns from the CATALOG schema — it is the
        // authoritative carrier of `BASIN_COLUMN_DEFAULT` / `BASIN_IDENTITY_SEQ`
        // metadata. The replayed WAL batch schema does not always preserve that
        // field metadata, so relying on `state.schemas` alone silently skipped
        // the reconciliation. Fall back to the batch schema only when the table
        // isn't in the catalog (then there's no sequence to realign anyway).
        let fields: Vec<arrow_schema::FieldRef> = match &cold {
            Some(meta) => meta.schema.fields().iter().cloned().collect(),
            None => state
                .schemas
                .get(table)
                .map(|s| s.fields().iter().cloned().collect())
                .unwrap_or_default(),
        };
        for field in &fields {
            let Some(seq_name) = serial_sequence_name(field) else {
                continue;
            };
            let mut max_id = state
                .tail
                .get(table)
                .and_then(|batches| column_i64_max(batches, field.name()));
            if let Some(meta) = &cold {
                for file in meta.live_data_files() {
                    if let Some(cs) = file.column_stats.get(field.name()) {
                        if let Some(cold_max) = decode_le_int_max(&cs.max_bytes) {
                            max_id = Some(max_id.map_or(cold_max, |m| m.max(cold_max)));
                        }
                    }
                }
            }
            let Some(max_id) = max_id else { continue };
            // Monotonic floor (NOT `setval`): raise the sequence so the next
            // `nextval` is strictly greater than `max_id`, but never lower it.
            // A plain `setval(max_id, true)` would CLOBBER the persisted
            // high-water mark — and on a durable backend the persisted hwm can
            // legitimately sit ABOVE `max_id` (a reserved-but-unused block
            // ceiling whose tail rows never committed, or rows we couldn't scan
            // for a max). Clobbering down to `max_id` would re-issue values an
            // earlier instance already handed out → duplicate PKs. `advance_
            // sequence_floor` takes `max(persisted_hwm, max_id)`, so recovery
            // can only ever move the sequence forward.
            match catalog
                .advance_sequence_floor(project, &seq_name, max_id)
                .await
            {
                Ok(_) => debug!(
                    %project, table = %table.as_str(), seq = %seq_name, max_id,
                    "reconciled serial sequence floor to recovered max",
                ),
                Err(e) => warn!(
                    %project, table = %table.as_str(), seq = %seq_name, max_id, error = %e,
                    "reconcile serial sequence: advance_sequence_floor failed (sequence may be absent)",
                ),
            }
        }
    }
}

/// Project a `RecordBatch` onto a subset of column names, in the requested
/// order. Returns an error if any column is missing.
fn project_batch(batch: &RecordBatch, cols: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut idxs: Vec<usize> = Vec::with_capacity(cols.len());
    for c in cols {
        let i = schema
            .index_of(c)
            .map_err(|_| BasinError::storage(format!("unknown column {c}")))?;
        idxs.push(i);
    }
    batch
        .project(&idxs)
        .map_err(|e| BasinError::storage(format!("project batch: {e}")))
}

/// AND together every predicate against the batch, returning only the rows
/// that pass all of them. Empty filter list returns the input untouched.
fn apply_filters(batch: RecordBatch, filters: &[basin_storage::Predicate]) -> Result<RecordBatch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let mut current = batch;
    for f in filters {
        let mask = evaluate_predicate(&current, f)?;
        current = arrow::compute::filter_record_batch(&current, &mask)
            .map_err(|e| BasinError::storage(format!("filter batch: {e}")))?;
        if current.num_rows() == 0 {
            return Ok(current);
        }
    }
    Ok(current)
}

/// Tiny stand-in for the storage crate's private predicate evaluator. v0.1
/// supports the same surface area: Eq / Lt / Gt against Int64, UInt64,
/// Float64, Utf8, and Boolean (Eq only).
fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &basin_storage::Predicate,
) -> Result<arrow_array::BooleanArray> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
    use arrow_array::{Array, BooleanArray};
    use basin_storage::{Predicate, ScalarValue};

    let col_name = predicate.column();
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col_name}")))?;

    macro_rules! cmp_primitive {
        ($arrow_ty:ty, $val:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let v = $val;
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) $op v);
                }
            }
            b.finish()
        }};
    }

    // Dedicated arm: sorted-IN membership over an Int64-family column. The
    // key list is sorted+deduped by contract, so per-row binary search gives
    // O(log k) membership; NULLs never match.
    if let Predicate::InInt64(_, keys) = predicate {
        let arr = col.as_primitive_opt::<Int64Type>().ok_or_else(|| {
            BasinError::storage(format!(
                "InInt64 predicate on non-Int64 column {col_name}"
            ))
        })?;
        let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                b.append_value(false);
            } else {
                b.append_value(keys.binary_search(&arr.value(i)).is_ok());
            }
        }
        return Ok(b.finish());
    }

    let value = match predicate {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v.clone(),
        Predicate::StartsWith { prefix, .. } => ScalarValue::Utf8(prefix.clone()),
        Predicate::InInt64(..) => unreachable!("InInt64 handled in dedicated arm above"),
    };

    let mask: BooleanArray = match (predicate, &value) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, <),
        (Predicate::StartsWith { case_insensitive, .. }, ScalarValue::Utf8(v)) => {
            let arr = col.as_string::<i32>();
            let ci = *case_insensitive;
            let needle_lc = if ci { v.to_ascii_lowercase() } else { String::new() };
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    let s = arr.value(i);
                    let m = if ci {
                        s.to_ascii_lowercase().starts_with(&needle_lc)
                    } else {
                        s.starts_with(v.as_str())
                    };
                    b.append_value(m);
                }
            }
            b.finish()
        }
        (op, ScalarValue::Utf8(v)) => {
            let arr = col.as_string::<i32>();
            let cmp: fn(&str, &str) -> bool = match op {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
                Predicate::StartsWith { .. } => {
                    unreachable!("StartsWith handled in dedicated arm above")
                }
                Predicate::InInt64(..) => {
                    unreachable!("InInt64 handled in dedicated arm above")
                }
            };
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(cmp(arr.value(i), v.as_str()));
                }
            }
            b.finish()
        }
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) => {
            let arr = col.as_boolean();
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) == *v);
                }
            }
            b.finish()
        }
        (op, val) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate combination: {op:?} on {val:?}"
            )));
        }
    };

    Ok(mask)
}

// ── ADR 0027 Phase 4 — promoted JSONB shadow-column backfill at compaction ───
//
// When a JSONB path is promoted AFTER rows were already written, the existing
// cold-tier files lack the `__promoted$col$key` shadow column and read NULL.
// Compaction rewrites those rows into a merged file; here we extend the merged
// batch with the shadow column so old + new rows all carry the materialised
// value. The extraction matches `json_get_text` / the engine INSERT-path
// `materialize_promoted_columns` exactly (absent key / JSON null → SQL NULL).
// `extract_promoted_key` is duplicated here (shard cannot depend on
// basin-engine — circular dep); the canonical engine copy is
// `promoted_columns::extract_promoted_key_text`.
fn backfill_promoted_columns(
    batch: RecordBatch,
    promoted_paths: &[basin_catalog::PromotedJsonbPath],
) -> Result<RecordBatch> {
    use arrow_array::{
        Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    };
    use arrow_schema::{DataType, Field};

    if promoted_paths.is_empty() {
        return Ok(batch);
    }

    let n = batch.num_rows();
    let mut new_fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();

    for path in promoted_paths {
        let shadow_name = path.shadow_col_name();

        // Skip if the shadow column is already present (e.g. row was written
        // after the path was promoted and the INSERT path already added it).
        if batch.schema().field_with_name(&shadow_name).is_ok() {
            continue;
        }

        let shadow_col: ArrayRef = match batch.schema().index_of(&path.source_col) {
            Ok(col_idx) => {
                // JSONB round-trips through the cold tier as LargeBinary, Binary,
                // or Utf8 depending on the on-disk encoder. Match the engine's
                // `extract_promoted_value` and accept all three rather than a
                // single `data_type()` arm — a too-narrow match silently emits an
                // all-NULL shadow column.
                let src = batch.column(col_idx);
                let lb = src.as_any().downcast_ref::<LargeBinaryArray>();
                let b = src.as_any().downcast_ref::<BinaryArray>();
                let sa = src.as_any().downcast_ref::<StringArray>();
                let lsa = src.as_any().downcast_ref::<LargeStringArray>();

                let mut values: Vec<Option<String>> = Vec::with_capacity(n);
                for i in 0..n {
                    let raw: Option<&[u8]> = if let Some(a) = lb {
                        (!a.is_null(i)).then(|| a.value(i))
                    } else if let Some(a) = b {
                        (!a.is_null(i)).then(|| a.value(i))
                    } else if let Some(a) = sa {
                        (!a.is_null(i)).then(|| a.value(i).as_bytes())
                    } else if let Some(a) = lsa {
                        (!a.is_null(i)).then(|| a.value(i).as_bytes())
                    } else {
                        None
                    };
                    match raw {
                        Some(bytes) => values.push(extract_promoted_key(bytes, &path.json_key)?),
                        None => values.push(None),
                    }
                }
                std::sync::Arc::new(StringArray::from(values)) as ArrayRef
            }
            Err(_) => std::sync::Arc::new(StringArray::from(vec![None::<String>; n])) as ArrayRef,
        };

        new_fields.push(Field::new(&shadow_name, DataType::Utf8, true));
        new_columns.push(shadow_col);
    }

    let new_schema = std::sync::Arc::new(arrow_schema::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| BasinError::internal(format!("backfill_promoted_columns: {e}")))
}

/// Extract the text value of `json_key` from raw JSONB bytes.
/// Returns None for SQL-NULL, absent key, or JSON null.
/// Matches the semantics of `json_get_text` / `payload->>'key'`.
fn extract_promoted_key(bytes: &[u8], json_key: &str) -> Result<Option<String>> {
    // Tolerate an optional leading 0x01 version byte (historical pgwire path).
    let payload = if bytes.first() == Some(&0x01) && bytes.len() > 1 {
        &bytes[1..]
    } else {
        bytes
    };
    let doc: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| BasinError::internal(format!("promoted col jsonb parse: {e}")))?;
    let val = match &doc {
        serde_json::Value::Object(map) => map.get(json_key).cloned(),
        _ => None,
    };
    Ok(match val {
        None => None,
        Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => Some(s),
        Some(other) => Some(
            serde_json::to_string(&other)
                .map_err(|e| BasinError::internal(format!("promoted col json serialize: {e}")))?,
        ),
    })
}

// ── GIN row-group re-index for compacted files ───────────────────────────────

/// Populate the per-row-group GIN bloom registry for every JSONB GIN column
/// declared on `table` after a compaction writes a new merged Parquet file.
///
/// This is the compactor's counterpart to `maintain_gin_rowgroup_index_on_insert`
/// in `basin-engine/src/executor.rs`.  On the shard INSERT path the executor's
/// index-maintenance is bypassed (INSERTs go directly to WAL + tail), so
/// compaction is the only point where GIN row-group summaries can be populated
/// for shard-written data.
///
/// `rg_registry_lock` is `None` until `Engine::new` calls
/// `Shard::set_gin_rowgroup_registry`; skip indexing in that case.
/// `gin_indexes` lists the table's declared secondary indexes (only `gin`
/// access-method, single-column, `LargeBinary`-typed entries are processed).
/// `rg_size` is the effective row-group row count used by the Parquet writer
/// for this compaction (mirrors the writer's priority logic:
/// `row_block_size > row_group_rows > DEFAULT_MAX_ROW_GROUP_SIZE`).
fn reindex_compacted_file_gin(
    rg_registry_lock: &std::sync::RwLock<
        Option<Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>>,
    >,
    project: &ProjectId,
    table: &TableName,
    gin_indexes: &[basin_catalog::SecondaryIndex],
    rg_size: usize,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    // Snapshot the Arc without holding the lock across potentially long
    // indexing work.  `None` means the engine hasn't wired the registry yet;
    // skip silently (compactor is safe: no false negatives, just no prune).
    let registry = {
        let guard = rg_registry_lock
            .read()
            .expect("gin_rowgroup_registry lock poisoned");
        match guard.as_ref() {
            Some(r) => r.clone(),
            None => return,
        }
    };

    for idx in gin_indexes {
        // Only single-column JSONB GIN indexes are row-group-indexed.
        if idx.access_method != "gin" || idx.columns.len() != 1 {
            continue;
        }
        let opclass = idx.opclass.as_deref().unwrap_or("jsonb_ops");
        // tsvector GIN columns are handled by a separate FTS registry;
        // skip them here.
        if opclass == "tsvector_ops" {
            continue;
        }
        let col_name = &idx.columns[0];
        basin_storage::index::gin_rowgroup::index_batch_jsonb_gin(
            &registry,
            project,
            table,
            col_name,
            opclass,
            batch,
            file_path,
            rg_size,
        );
    }
}

/// FIX 2(a) — register the compacted file's single-column B-tree index values
/// into the secondary-index sink.
///
/// This is the compactor's counterpart to the engine's
/// `maintain_secondary_indexes_on_insert` (which only runs on the non-shard
/// INSERT path). For every single-column, plain (`btree`) `SecondaryIndex` on
/// the table it extracts `(key_text, file, row_group, row)` locations from the
/// merged batch and pushes them through the [`crate::SecondaryIndexSink`] under
/// the NEW file's path. GIN / GIST / multi-column indexes are skipped (handled
/// by their own row-group / interval / posting registries).
///
/// `registry_lock` is `None` until `Engine::new` calls
/// `Shard::set_secondary_index_registry`; skip indexing in that case (safe: the
/// probe path treats a missing entry as "unknown — full scan", never a false
/// negative).
///
/// Row-group ordinal semantics (must match the read path's expectations):
///   * **Parquet**: the writer flushes a row-group every `rg_size` rows, so row
///     `r` lands in row-group `r / rg_size` — identical to the GIN row-group
///     mapping in `maintain_gin_rowgroup_index_on_insert`. The secondary-index
///     probe uses the row-group only to skip within an opened file; file-level
///     pruning is what dominates.
///   * **Vortex**: the engine ignores the per-location `row_group` for Vortex
///     reads (file-level allow-list wins). We still record the same uniform
///     `r / rg_size` ordinal so the format-agnostic extraction stays simple;
///     it is harmless because the Vortex read path never consults it.
fn reindex_compacted_file_secondary(
    registry_lock: &std::sync::RwLock<Option<Arc<dyn crate::SecondaryIndexSink>>>,
    project: &ProjectId,
    table: &TableName,
    indexes: &[basin_catalog::SecondaryIndex],
    rg_size: usize,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;

    let registry = {
        let guard = registry_lock
            .read()
            .expect("secondary_index_registry lock poisoned");
        match guard.as_ref() {
            Some(r) => r.clone(),
            None => return,
        }
    };

    let rg_size = rg_size.max(1);
    let schema = batch.schema();

    for idx in indexes {
        // Only single-column plain B-tree indexes are handled here. GIN,
        // GIST, HNSW/IVFFlat (vector) indexes have their own registries and
        // are populated elsewhere in `compact_one`.
        if idx.columns.len() != 1 {
            continue;
        }
        if idx.access_method != "btree" {
            continue;
        }
        let col_name = &idx.columns[0];
        let Ok(col_idx) = schema.index_of(col_name) else {
            continue;
        };
        let col = batch.column(col_idx);

        // Mirror of `basin_engine::secondary_index::extract_entries_from_batch`
        // (which we cannot call across the dependency edge), producing the
        // primitive `(key, file, row_group, row)` tuples the sink accepts.
        let mut entries: Vec<(String, String, u32, u64)> = Vec::new();
        macro_rules! extract_typed {
            ($arr_ty:ty, $fmt:expr) => {{
                if let Some(arr) = col.as_any().downcast_ref::<$arr_ty>() {
                    for row in 0..arr.len() {
                        if arr.is_null(row) {
                            continue;
                        }
                        let key: String = $fmt(arr.value(row));
                        let row_group = (row / rg_size) as u32;
                        entries.push((key, file_path.to_string(), row_group, row as u64));
                    }
                }
            }};
        }
        use arrow_array::{
            BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray, UInt64Array,
        };
        extract_typed!(Int64Array, |v: i64| v.to_string());
        extract_typed!(UInt64Array, |v: u64| v.to_string());
        extract_typed!(Float64Array, |v: f64| format!("{v:?}"));
        extract_typed!(StringArray, |v: &str| v.to_string());
        extract_typed!(LargeStringArray, |v: &str| v.to_string());
        extract_typed!(BooleanArray, |v: bool| if v {
            "t"
        } else {
            "f"
        }
        .to_string());

        if !entries.is_empty() {
            registry.insert_batch_locations(project, table, col_name, entries);
        }
    }
}

/// PG-Wave-α: build an R-tree spatial index segment for any `USING gist`
/// SecondaryIndex on a POINT column of the compacted file. The sidecar
/// is written under
/// `projects/{p}/tables/{t}/index/{col}.rtree/{ulid}.rtree`, exactly
/// mirroring the HNSW vector-index layout. The engine pushdown path
/// (`RTreePrunedTable` + `apply_rtree_pruning_for_query`) lands in
/// PG-Wave-β and consumes these sidecars; until then the only side
/// effect is the sidecar object in storage.
///
/// Indexes that aren't `gist` access-method or whose target column
/// isn't a POINT (FixedSizeBinary(21) with `BASIN_TYPE=POINT`) are
/// skipped silently. Errors are propagated so the caller can log
/// (the call site treats this as best-effort).
async fn reindex_compacted_file_rtree(
    storage: basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    indexes: &[basin_catalog::SecondaryIndex],
    rg_size: u32,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) -> basin_common::Result<()> {
    use arrow_schema::DataType as Dt;
    use object_store::ObjectStoreExt;
    for idx in indexes {
        if idx.access_method != "gist" || idx.columns.len() != 1 {
            continue;
        }
        let col_name = &idx.columns[0];
        let schema = batch.schema();
        let Ok(col_idx) = schema.index_of(col_name) else {
            continue;
        };
        let field = schema.field(col_idx);
        // POINT marker: BASIN_TYPE=POINT AND physical FixedSizeBinary(21).
        let is_point = field
            .metadata()
            .get("BASIN_TYPE")
            .map(|s| s == "POINT")
            .unwrap_or(false);
        if !is_point {
            // Non-POINT GIST indexes (e.g. range-ops on int4range) use
            // the legacy interval-tree path; skip here.
            continue;
        }
        if !matches!(field.data_type(), Dt::FixedSizeBinary(n) if *n == basin_geo::POINT_WKB_LEN as i32)
        {
            continue;
        }

        let rtree = basin_storage::index::rtree::build_rtree_for_batch(batch, col_idx, rg_size);
        if rtree.size() == 0 {
            // No spatial entries — nothing to write. Common when a
            // newly compacted file has the column but every row is
            // NULL.
            continue;
        }
        let bytes = basin_storage::index::rtree::serialize_rtree(&rtree).map_err(|e| {
            basin_common::BasinError::storage(format!("R-tree serialize: {e}"))
        })?;
        let Some(sidecar) = basin_storage::index::rtree::rtree_segment_key_for_data_file(
            None,
            project,
            table,
            col_name,
            file_path,
        ) else {
            tracing::debug!(
                project = %project,
                table = %table,
                file = %file_path,
                "PG-Wave-α: data file path not in canonical layout; skipping R-tree sidecar"
            );
            continue;
        };
        storage
            .project_object_store(project)
            .put(
                &sidecar,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes)),
            )
            .await
            .map_err(|e| {
                basin_common::BasinError::storage(format!(
                    "R-tree sidecar put {sidecar}: {e}"
                ))
            })?;
    }
    Ok(())
}

// ── JSONB posting-list re-index for compacted files ──────────────────────────

/// Build the JSONB `(key, value)` posting list for every JSONB GIN
/// column declared on `table` after a compaction writes a new merged
/// Parquet file.  Mirrors `reindex_compacted_file_gin` (the bloom
/// registry) — both registries are populated from the same merged
/// batch but indexed differently:
///
/// * `gin_rowgroup` (bloom) — answers `?` key-exists queries fast
///   when the term is sparse; useless when the term is in every
///   row-group.
/// * `jsonb_posting` (inverted index) — answers `@>` containment by
///   AND-merging per-`(key, value)` posting lists at file + row-group
///   granularity; precise even when individual terms saturate the
///   bloom.
///
/// The posting list is also persisted to a bincode sidecar at
/// `index/{col}.jsonb_post/{ulid}.bin` so a fresh engine instance can
/// lazy-load the index without re-scanning the data file.
///
/// Best-effort: errors are returned to the caller for logging; a
/// failure here means `@>` queries against this file fall back to the
/// bloom / full-scan path.
async fn reindex_compacted_file_jsonb_posting(
    jp_registry_lock: &std::sync::RwLock<
        Option<Arc<basin_storage::index::jsonb_posting::JsonbPostingRegistry>>,
    >,
    storage: basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    gin_indexes: &[basin_catalog::SecondaryIndex],
    rg_size: usize,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) -> Result<()> {
    use object_store::ObjectStoreExt;
    let registry = {
        let guard = jp_registry_lock
            .read()
            .expect("jsonb_posting_registry lock poisoned");
        match guard.as_ref() {
            Some(r) => r.clone(),
            None => return Ok(()),
        }
    };

    let schema = batch.schema();
    for idx in gin_indexes {
        // Only single-column JSONB GIN indexes are posting-indexed.
        if idx.access_method != "gin" || idx.columns.len() != 1 {
            continue;
        }
        let opclass = idx.opclass.as_deref().unwrap_or("jsonb_ops");
        // tsvector GIN columns are FTS — separate registry.
        if opclass == "tsvector_ops" {
            continue;
        }
        let col_name = &idx.columns[0];
        let Ok(col_idx) = schema.index_of(col_name) else {
            continue;
        };
        let field = schema.field(col_idx);
        let is_jsonb = field
            .metadata()
            .get("BASIN_TYPE")
            .map(|s| s == "JSONB")
            .unwrap_or(false);
        if !is_jsonb {
            continue;
        }

        registry.index_batch(project, table, col_name, batch, col_idx, rg_size, file_path);

        // Persist the posting list as a sidecar so a restarted engine
        // can lazy-load it at probe time.  Skip silently for over-budget
        // / empty files (the registry returns `None`).
        if let Some(bytes) = registry.serialize_file(project, table, col_name, file_path) {
            let Some(sidecar) =
                basin_storage::index::jsonb_posting::posting_sidecar_key_for_data_file(
                    project, table, col_name, file_path,
                )
            else {
                tracing::debug!(
                    project = %project,
                    table = %table,
                    file = %file_path,
                    "Inv-W5: non-canonical data path; skipping JSONB posting sidecar"
                );
                continue;
            };
            storage
                .project_object_store(project)
                .put(
                    &sidecar,
                    object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes)),
                )
                .await
                .map_err(|e| {
                    basin_common::BasinError::storage(format!(
                        "JSONB posting sidecar put {sidecar}: {e}"
                    ))
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_catalog::{InMemoryCatalog, LeaseRegistry};
    use basin_common::{PartitionKey, ProjectId, TableName};
    use basin_storage::{Storage, StorageConfig};
    use basin_wal::{LocalWal, Wal, WalConfig};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch(start: i64, len: usize, prefix: &str) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        let names: Vec<String> = (0..len)
            .map(|i| format!("{prefix}{}", start + i as i64))
            .collect();
        let names: StringArray = names.iter().map(|s| Some(s.as_str())).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn schema_with_serial_default(seq: &str) -> Arc<Schema> {
        let mut md = std::collections::HashMap::new();
        md.insert(
            BASIN_COLUMN_DEFAULT_KEY.to_string(),
            format!("nextval('{seq}')"),
        );
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(md),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn parse_nextval_sequence_variants() {
        assert_eq!(
            parse_nextval_sequence("nextval('t_id_seq')").as_deref(),
            Some("t_id_seq")
        );
        assert_eq!(
            parse_nextval_sequence("NEXTVAL( '\"t_id_seq\"' )").as_deref(),
            Some("t_id_seq")
        );
        assert_eq!(
            parse_nextval_sequence("nextval('s'::regclass)").as_deref(),
            Some("s")
        );
        assert_eq!(parse_nextval_sequence("now()"), None);
        assert_eq!(parse_nextval_sequence("42"), None);
    }

    #[test]
    fn decode_le_int_max_widths() {
        assert_eq!(decode_le_int_max(&Some(7i64.to_le_bytes().to_vec())), Some(7));
        assert_eq!(decode_le_int_max(&Some(9i32.to_le_bytes().to_vec())), Some(9));
        assert_eq!(decode_le_int_max(&None), None);
        assert_eq!(decode_le_int_max(&Some(vec![1, 2, 3])), None); // odd width → skip
    }

    /// Regression guard for the credential-provisioning `duplicate key (id)=(N)`
    /// bug: after WAL replay restores rows 1..5, a fresh sequence (which would
    /// otherwise hand out 1) must be realigned to resume at 6.
    #[tokio::test]
    async fn reconcile_serial_sequences_resumes_past_recovered_max() {
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let table = TableName::new("t".to_string()).unwrap();
        let seq = "t_id_seq";
        catalog
            .create_sequence(basin_catalog::SequenceDef::with_defaults(project, seq))
            .await
            .unwrap();

        let mut state = PartitionState::new(project, PartitionKey::default_key());
        state
            .schemas
            .insert(table.clone(), schema_with_serial_default(seq));
        // Recovered tail rows carry ids 1..=5.
        state
            .tail
            .insert(table.clone(), vec![(Lsn(1), batch(1, 5, "r"))]);

        reconcile_serial_sequences(&catalog, &project, &state).await;

        // Without reconciliation this would be 1 → a PK collision with row 1.
        let next = catalog.nextval(&project, seq).await.unwrap();
        assert_eq!(next, 6, "sequence must resume at max(recovered id)+1");
    }

    /// The real production shape: the catalog carries the serial metadata but
    /// the replayed WAL batch schema does NOT. Detection must use the catalog
    /// schema (regression: reading only the batch schema silently skipped the
    /// reconciliation, so provisioning kept colliding after a restart).
    #[tokio::test]
    async fn reconcile_detects_serial_via_catalog_when_batch_bare() {
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let table = TableName::new("t".to_string()).unwrap();
        let seq = "t_id_seq";
        // Catalog table carries the serial metadata; sequence registered.
        catalog
            .create_table(&project, &table, schema_with_serial_default(seq).as_ref())
            .await
            .unwrap();
        catalog
            .create_sequence(basin_catalog::SequenceDef::with_defaults(project, seq))
            .await
            .unwrap();

        let mut state = PartitionState::new(project, PartitionKey::default_key());
        // Replayed tail uses a BARE schema (no serial metadata) — the WAL case.
        state.schemas.insert(table.clone(), schema());
        state
            .tail
            .insert(table.clone(), vec![(Lsn(1), batch(1, 7, "r"))]); // ids 1..=7

        reconcile_serial_sequences(&catalog, &project, &state).await;

        let next = catalog.nextval(&project, seq).await.unwrap();
        assert_eq!(
            next, 8,
            "serial must be detected via the catalog schema and resume at 8"
        );
    }

    /// A plain (non-serial) column must not touch any sequence.
    #[tokio::test]
    async fn reconcile_serial_sequences_ignores_plain_columns() {
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let table = TableName::new("plain".to_string()).unwrap();
        let mut state = PartitionState::new(project, PartitionKey::default_key());
        state.schemas.insert(table.clone(), schema()); // no serial metadata
        state
            .tail
            .insert(table.clone(), vec![(Lsn(1), batch(1, 5, "r"))]);
        // Must not panic / error — simply a no-op.
        reconcile_serial_sequences(&catalog, &project, &state).await;
    }

    /// REGRESSION (#15): on a durable catalog the persisted high-water mark can
    /// sit ABOVE the maximum id present in the recovered rows — e.g. a block of
    /// 64 was reserved but only the first few rows committed before a crash.
    /// Recovery must NOT clobber the sequence down to the recovered max (the old
    /// `setval` behaviour); that would re-issue values an earlier instance had
    /// already handed out. With `advance_sequence_floor` the persisted ceiling
    /// wins, so the next `nextval` is strictly above every handed-out value.
    #[tokio::test]
    async fn reconcile_does_not_regress_durable_hwm_below_reserved_block() {
        use basin_catalog::{Catalog, ObjectStoreCatalog};
        use object_store::memory::InMemory;

        // Pin the reservation block so the persisted ceiling is deterministic
        // (16) regardless of any ambient `BASIN_SEQ_BLOCK`. Restored on drop.
        let _prev_block = std::env::var("BASIN_SEQ_BLOCK").ok();
        std::env::set_var("BASIN_SEQ_BLOCK", "16");
        struct RestoreBlock(Option<String>);
        impl Drop for RestoreBlock {
            fn drop(&mut self) {
                match &self.0 {
                    Some(v) => std::env::set_var("BASIN_SEQ_BLOCK", v),
                    None => std::env::remove_var("BASIN_SEQ_BLOCK"),
                }
            }
        }
        let _restore = RestoreBlock(_prev_block);

        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let osc = ObjectStoreCatalog::new(store.clone());
        let project = ProjectId::new();
        let table = TableName::new("t".to_string()).unwrap();
        let seq = "t_id_seq";
        osc.create_sequence(basin_catalog::SequenceDef::with_defaults(project, seq))
            .await
            .unwrap();
        // Hand out a few ids; this reserves the block, persisting hwm.last = 16.
        let mut handed_out = Vec::new();
        for _ in 0..3 {
            handed_out.push(osc.nextval(&project, seq).await.unwrap());
        }
        // The reserved block ceiling: block size 16, so hwm.last == 16.
        let reserved_ceiling = 16i64;
        drop(osc); // "crash": the node-local reserved block (4..=16) is lost.

        // RESTART: a FRESH instance over the same store. The node-local block is
        // gone; only the durable hwm (= reserved ceiling 16) survives. This is
        // exactly the production restart path that runs `reconcile_serial_*`.
        let osc2 = ObjectStoreCatalog::new(store.clone());
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(osc2);

        // Recovered tail only carries the committed ids 1..=3; the reserved tail
        // was never written to a row.
        let mut state = PartitionState::new(project, PartitionKey::default_key());
        state
            .schemas
            .insert(table.clone(), schema_with_serial_default(seq));
        state
            .tail
            .insert(table.clone(), vec![(Lsn(1), batch(1, 3, "r"))]);

        reconcile_serial_sequences(&catalog, &project, &state).await;

        // Recovery floor (recovered max = 3) is BELOW the persisted ceiling
        // (16), so it must be a no-op: the next value resumes strictly above the
        // ceiling (17), NEVER re-issuing a reserved value. The old `setval`-
        // clobber bug regressed the hwm to 3 and handed out 4 here.
        let next = catalog.nextval(&project, seq).await.unwrap();
        assert!(
            handed_out.iter().all(|&v| v < next),
            "no previously-handed-out value is re-issued after recovery: next={next}, handed_out={handed_out:?}"
        );
        assert_eq!(
            next,
            reserved_ceiling + 1,
            "recovery must keep the persisted block ceiling ({reserved_ceiling}); \
             a clobbering setval would have produced 4. got next={next}"
        );
    }

    /// Build a fresh shard wired against a `tempdir`-backed object store and
    /// in-memory catalog. Returns the shard plus the dir handles so callers
    /// can keep them alive for the duration of the test.
    async fn fresh_shard() -> (
        crate::Shard,
        TempDir,
        TempDir,
        Storage,
        Arc<InMemoryCatalog>,
        Arc<dyn Wal>,
    ) {
        fresh_shard_with(|_| {}).await
    }

    /// Like [`fresh_shard`], but lets the caller tweak the [`ShardConfig`]
    /// (e.g. a tiny `clean_sweep_interval`) before the shard is built.
    async fn fresh_shard_with(
        tweak: impl FnOnce(&mut ShardConfig),
    ) -> (
        crate::Shard,
        TempDir,
        TempDir,
        Storage,
        Arc<InMemoryCatalog>,
        Arc<dyn Wal>,
    ) {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let mut cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal.clone());
        tweak(&mut cfg);
        let shard = crate::Shard::new(cfg);
        (shard, storage_dir, wal_dir, storage, catalog, wal)
    }

    fn rows_in(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Reach inside the public Shard to invoke the test-only helpers. Tied to
    /// the in-process implementation only.
    fn impl_of(shard: &crate::Shard) -> Arc<InProcessShard> {
        // SAFETY: there is exactly one ShardImpl in v0.1; downcast is safe.
        // But we can't use Any here, so re-wire by reading the same fields
        // through a private helper. We expose `impl_handle` on Shard for
        // tests via cfg(test).
        shard
            .impl_handle()
            .expect("Shard wraps InProcessShard in v0.1")
    }

    #[tokio::test]
    async fn write_then_read_returns_tail() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        for i in 0..3 {
            handle
                .write_batch(&table, batch(i * 10, 10, "v-"))
                .await
                .unwrap();
        }

        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 30);
    }

    #[tokio::test]
    async fn tail_pressure_reports_resident_tail_bytes() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();

        // Empty shard: no resident tail, all zero.
        let before = shard.tail_pressure().await;
        assert_eq!(before.total_tail_bytes, 0);
        assert_eq!(before.max_partition_tail_bytes, 0);

        // Write into two distinct partitions so total != max and we exercise
        // the per-partition max.
        let p1 = PartitionKey::default_key();
        let p2 = PartitionKey::new("s1".to_string()).unwrap();
        shard
            .get(&project, &p1)
            .await
            .unwrap()
            .write_batch(&table, batch(0, 100, "v-"))
            .await
            .unwrap();
        // Two batches into p2 so its tail is the heavier of the two.
        let h2 = shard.get(&project, &p2).await.unwrap();
        h2.write_batch(&table, batch(0, 100, "v-")).await.unwrap();
        h2.write_batch(&table, batch(100, 100, "v-")).await.unwrap();

        let after = shard.tail_pressure().await;
        assert!(
            after.total_tail_bytes > 0,
            "resident tail bytes must be visible: {after:?}"
        );
        assert!(
            after.max_partition_tail_bytes > 0
                && after.max_partition_tail_bytes <= after.total_tail_bytes,
            "max single-partition tail must be a positive subset of the total: {after:?}"
        );
        // p2 holds two batches vs p1's one → max is strictly less than total.
        assert!(
            after.max_partition_tail_bytes < after.total_tail_bytes,
            "two-partition spread must make max < total: {after:?}"
        );
    }

    /// `write_batch_opts(durable = true)` (`basin.synchronous_commit = on`)
    /// must route through `Wal::append_fenced_durable`: the WAL entry is on
    /// the backing store when the call returns — provable by reopening a
    /// second WAL over the same directory with **no** flush/close on the
    /// first. The async `write_batch` keeps ack-before-durable: the same
    /// reopen sees nothing until an explicit flush.
    #[tokio::test]
    async fn write_batch_durable_is_on_store_before_ack() {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
            ),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());
        // Background ticks disabled so durability can only come from the
        // synchronous (group-commit) path, never from a racing 50 ms tick.
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(
                    LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap(),
                ),
                root_prefix: None,
                flush_interval: Duration::from_secs(3600),
                flush_max_bytes: u64::MAX,
                commit_delay: Duration::from_millis(1),
            })
            .await
            .unwrap(),
        );
        let shard = crate::Shard::new(ShardConfig::new(storage, catalog, wal.clone()));
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();
        let handle = shard.get(&project, &partition).await.unwrap();

        // Async write first: ack'd from RAM, invisible to a reopened WAL.
        handle
            .write_batch_opts(&table, batch(0, 1, "async-"), false)
            .await
            .unwrap();
        let reopen = |dir: std::path::PathBuf| async move {
            LocalWal::open(WalConfig {
                object_store: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
                root_prefix: None,
                flush_interval: Duration::from_secs(3600),
                flush_max_bytes: u64::MAX,
                commit_delay: Duration::from_millis(1),
            })
            .await
            .unwrap()
        };
        {
            let crash_view = reopen(wal_dir.path().to_path_buf()).await;
            let entries = crash_view
                .read_from(&project, &partition, basin_wal::Lsn::ZERO)
                .await
                .unwrap();
            assert!(
                entries.is_empty(),
                "async write_batch must be ack-before-durable (in-RAM only)"
            );
            crash_view.close().await.unwrap();
        }

        // Durable write: must be on the store the moment the call returns.
        handle
            .write_batch_opts(&table, batch(100, 1, "sync-"), true)
            .await
            .unwrap();
        {
            let crash_view = reopen(wal_dir.path().to_path_buf()).await;
            let entries = crash_view
                .read_from(&project, &partition, basin_wal::Lsn::ZERO)
                .await
                .unwrap();
            // The durable append's group-committed segment drains the whole
            // buffer, so the earlier async entry rides along: 2 entries.
            assert_eq!(
                entries.len(),
                2,
                "durable write_batch must be on the backing store before ack"
            );
            crash_view.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn compaction_drains_tail_to_parquet() {
        let (shard, _sd, _wd, storage, _cat, wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        for i in 0..3 {
            handle
                .write_batch(&table, batch(i * 10, 10, "v-"))
                .await
                .unwrap();
        }

        let inner = impl_of(&shard);
        inner.run_compaction_once().await.unwrap();

        // Parquet file exists in storage.
        let files = storage.list_data_files(&project, &table).await.unwrap();
        assert!(
            !files.is_empty(),
            "expected at least one data file after compaction",
        );

        // WAL is truncated (high water now equals the watermark we drained).
        // After truncate, read_from(ZERO) should return no entries with
        // lsn <= watermark; we just check the partition's tail in memory is
        // empty.
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(
            rows_in(&read),
            30,
            "rows should still be visible via Parquet"
        );

        // Tail empty after compaction.
        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        let guard = state.read().await;
        assert!(
            guard.tail_is_empty(),
            "tail should be empty after compaction"
        );

        // Sanity: WAL high_water reflects truncation by returning the pre-trunc
        // value or higher, never resetting to ZERO. Just call it to ensure
        // truncate didn't error out.
        let _ = wal.high_water(&project, &partition).await.unwrap();
    }

    /// Read-freshness convergence (the durable-vs-visible gap): metadata reads
    /// (`count(*)`/`max(id)` via the fast-aggregate path) answer from the
    /// catalog's `live_data_files()` — the SEGMENT-COMMITTED row set — and never
    /// see the in-memory tail. A write burst lands in the tail (and the WAL)
    /// immediately, so the catalog under-counts until compaction folds the tail
    /// into a segment. This test captures the property the quiesce-drain tick
    /// guarantees: after a burst with NO further writes, ONE quiesce-drain tick
    /// converges the catalog row set to every written row — promptly (bounded
    /// ticks), not lazily on the 30 s timer.
    #[tokio::test]
    async fn quiesce_drain_converges_catalog_after_idle_burst() {
        use basin_catalog::Catalog;
        let (shard, _sd, _wd, _storage, catalog, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        let n_batches = 5usize;
        let per = 10usize;
        let total = (n_batches * per) as u64;
        for i in 0..n_batches {
            handle
                .write_batch(&table, batch((i * per) as i64, per, "v-"))
                .await
                .unwrap();
        }

        let inner = impl_of(&shard);

        // BEFORE the drain: the rows are WAL-durable and in the tail, but the
        // catalog (what `count(*)` reads) does not yet reflect them — this is
        // the freshness gap. A freshly-loaded table has no committed segment, so
        // `live_data_files()` is empty / under the written total.
        let cold_before: u64 = match catalog.load_table(&project, &table).await {
            Ok(m) => m.live_data_files().iter().map(|f| f.row_count).sum(),
            // Table not yet in the catalog at all == 0 committed rows.
            Err(_) => 0,
        };
        assert!(
            cold_before < total,
            "precondition: catalog should under-count before the drain \
             (got {cold_before}, wrote {total}); the gap is the tail",
        );
        // The tail must actually hold the residual (otherwise there is nothing
        // for the quiesce tick to converge).
        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        assert!(
            !state.read().await.tail_is_empty(),
            "precondition: tail holds the durable-but-uncompacted residual",
        );

        // ONE quiesce-drain tick. Pin the rate to 0 so the idle gate is
        // exercised deterministically (the burst stopped — ingest is quiet).
        inner.ingest_rate.force_rate_for_test(0);
        inner.run_quiesce_drain_once().await.unwrap();

        let cold_after: u64 = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files()
            .iter()
            .map(|f| f.row_count)
            .sum();
        assert_eq!(
            cold_after, total,
            "after a single quiesce-drain tick the catalog (what count(*) reads) \
             must reflect every durably-committed row",
        );
        assert!(
            state.read().await.tail_is_empty(),
            "tail fully drained by the quiesce tick",
        );
    }

    /// The quiesce-drain tick must be a strict no-op when there is nothing
    /// resident to drain (empty shard / already-compacted tail): it neither
    /// errors nor produces a spurious data file. Guards the pending-tail gate.
    #[tokio::test]
    async fn quiesce_drain_noop_when_no_pending_tail() {
        let (shard, _sd, _wd, storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        // Resident partition, but empty tail.
        let _handle = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);
        inner.run_quiesce_drain_once().await.unwrap();

        let files = storage.list_data_files(&project, &table).await.unwrap();
        assert!(
            files.is_empty(),
            "quiesce drain with no pending tail must not write a data file",
        );
    }

    /// While ingest is ACTIVE (rolling rows/sec > 0) the quiesce-drain tick must
    /// short-circuit and NOT compact — so it never competes with the write path
    /// (the flat-ingest win). We make the rate non-zero deterministically by
    /// recording a write into one wall-second and letting the bucket roll into
    /// the next, then assert the tick leaves the tail untouched.
    #[tokio::test]
    async fn quiesce_drain_is_noop_while_ingest_active() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);

        // Leave a residual tail, then pin the rolling rate non-zero so the shard
        // looks "actively ingesting" (deterministic — the wall-second bucketing
        // is otherwise timing-fragile to drive).
        handle
            .write_batch(&table, batch(0, 100, "v-"))
            .await
            .unwrap();
        inner.ingest_rate.force_rate_for_test(34_000);
        assert!(
            inner.ingest_rate.rows_per_sec() > 0,
            "rate should report active ingest after force_rate_for_test",
        );

        // The tick sees a non-zero rate and must bail before compacting.
        inner.run_quiesce_drain_once().await.unwrap();

        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        assert!(
            !state.read().await.tail_is_empty(),
            "quiesce drain must NOT compact while ingest is active; tail intact",
        );
    }

    /// Incremental-compaction guard (flat-O(1) ingest wedge): a single
    /// compaction tick that finds a tail larger than `MAX_COMPACTION_ROWS`
    /// must drain it as SEVERAL bounded files, not one giant O(tail) concat +
    /// encode. We insert 700k rows (> the 512k per-file budget) across small
    /// batches, run ONE compaction, and assert (a) the tail is fully drained,
    /// (b) MORE THAN ONE data file was produced (bounded per file), (c) every
    /// row is still readable through the cold tier, and (d) no file exceeds the
    /// budget. This is what keeps a backlog-catch-up compaction cost bounded by
    /// recent writes rather than total table size.
    #[tokio::test]
    async fn compaction_is_incremental_bounded_files() {
        let (shard, _sd, _wd, storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        // 7 batches of 100k rows = 700k rows > MAX_COMPACTION_ROWS (512k).
        let per = 100_000usize;
        let n_batches = 7usize;
        let total = per * n_batches;
        for i in 0..n_batches {
            handle
                .write_batch(&table, batch((i * per) as i64, per, "v-"))
                .await
                .unwrap();
        }

        let inner = impl_of(&shard);
        inner.run_compaction_once().await.unwrap();

        // (a) Tail fully drained in one tick (the inner loop keeps going until
        // the snapshot is empty).
        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        assert!(
            state.read().await.tail_is_empty(),
            "tail should be fully drained after one compaction tick"
        );

        // (b) + (d) Multiple bounded files, each within the per-file row budget.
        let files = storage
            .list_data_files_with_stats(&project, &table)
            .await
            .unwrap();
        assert!(
            files.len() >= 2,
            "expected >= 2 bounded data files for {total} rows (budget \
             {MAX_COMPACTION_ROWS}/file), got {}",
            files.len(),
        );
        for f in &files {
            assert!(
                f.row_count as usize <= MAX_COMPACTION_ROWS,
                "file {} has {} rows, exceeds per-file budget {MAX_COMPACTION_ROWS}",
                f.path,
                f.row_count,
            );
        }
        let file_rows: usize = files.iter().map(|f| f.row_count as usize).sum();
        assert_eq!(file_rows, total, "compacted files must hold every row");

        // (c) Every row still readable through the cold tier.
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), total, "all rows readable after compaction");
    }

    /// Per-`compact_one` table-config cache must stay correct across MANY
    /// bounded passes. The compactor now resolves each table's write config
    /// (format, cluster cols, indexes, row-group size) ONCE per `compact_one`
    /// and reuses it for every bounded file in the drain (previously it called
    /// `load_table` — an O(files) snapshot-chain clone — per flushed file). This
    /// drains a tail large enough to need several passes through one tick and
    /// asserts every row lands exactly once in a bounded file and is readable —
    /// i.e. caching the config did not drop, duplicate, or mis-bound any pass.
    #[tokio::test]
    async fn compaction_config_cache_correct_across_many_passes() {
        let (shard, _sd, _wd, storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("cache_passes").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        // 12 batches of 200k = 2.4M rows > 4 * MAX_COMPACTION_ROWS (512k) →
        // forces at least 5 bounded passes through the single compaction tick.
        let per = 200_000usize;
        let n_batches = 12usize;
        let total = per * n_batches;
        for i in 0..n_batches {
            handle
                .write_batch(&table, batch((i * per) as i64, per, "v-"))
                .await
                .unwrap();
        }

        let inner = impl_of(&shard);
        inner.run_compaction_once().await.unwrap();

        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        assert!(
            state.read().await.tail_is_empty(),
            "tail must be fully drained after one multi-pass tick",
        );

        let files = storage
            .list_data_files_with_stats(&project, &table)
            .await
            .unwrap();
        assert!(
            files.len() >= 5,
            "expected >= 5 bounded files for {total} rows, got {}",
            files.len(),
        );
        for f in &files {
            assert!(
                f.row_count as usize <= MAX_COMPACTION_ROWS,
                "file {} exceeds per-file budget",
                f.path,
            );
        }
        let file_rows: usize = files.iter().map(|f| f.row_count as usize).sum();
        assert_eq!(file_rows, total, "every row must land exactly once");

        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), total, "all rows readable after multi-pass drain");
    }

    /// Regression for the concurrent-compaction over-count (#95).
    ///
    /// Two pooled sessions issuing a `SELECT` each call `flush_to_parquet()`
    /// (→ `compact_all` → `compact_one`) before reading the cold tier. With no
    /// per-partition serialization, both `compact_one` calls snapshot the SAME
    /// tail entries, both write a Parquet file, and both commit to the catalog
    /// (the second wins the `commit_with_retry` conflict path against the
    /// already-advanced snapshot). The rows end up duplicated across two cold
    /// files, so a subsequent read returns 2*N rows for N inserted rows.
    ///
    /// Asserts the post-flush read count is EXACTLY N, never more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_compaction_does_not_double_count() {
        const N: usize = 50;
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&project, &partition).await.unwrap();
        // Insert N rows (1 row per batch) into the in-memory tail.
        for i in 0..N as i64 {
            handle.write_batch(&table, batch(i, 1, "v-")).await.unwrap();
        }

        // Fire many concurrent flush_to_parquet() calls against the SAME
        // partition, mirroring multiple pooled sessions racing a flush.
        let mut joins = Vec::new();
        for _ in 0..8 {
            let s = shard.clone();
            joins.push(tokio::spawn(async move { s.flush_to_parquet().await }));
        }
        for j in joins {
            j.await.unwrap().unwrap();
        }

        // After all flushes, the row count must be exactly N.
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(
            rows_in(&read),
            N,
            "concurrent compaction must not duplicate rows (got {}, want {N})",
            rows_in(&read),
        );
    }

    /// Regression for the sequential `compact_all` dispatch loop fix:
    /// N partitions with non-trivial tails should compact in roughly
    /// `ceil(N / concurrency)` × per-partition time, not the old serial
    /// N × per-partition time. We can't measure absolute wall-clock
    /// reliably under CI noise, so we just compare the parallel
    /// `compact_all` wall-clock against the sum of two sequential
    /// `compact_one` calls. With concurrency ≥ 2 the parallel run must
    /// be at most ~80% of the sequential sum (allowing ample headroom
    /// for ordering / scheduling jitter).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_compaction_across_partitions_parallel() {
        use std::time::Instant;
        const PARTITIONS: usize = 4;
        const ROWS_PER_PARTITION: usize = 200;
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();

        // Seed N disjoint partitions, each with the same tail shape.
        let mut handles = Vec::with_capacity(PARTITIONS);
        for i in 0..PARTITIONS {
            let partition = PartitionKey::new(format!("p{i}")).unwrap();
            let handle = shard.get(&project, &partition).await.unwrap();
            for j in 0..ROWS_PER_PARTITION {
                handle
                    .write_batch(
                        &table,
                        batch((i * ROWS_PER_PARTITION + j) as i64, 1, "v-"),
                    )
                    .await
                    .unwrap();
            }
            handles.push((partition, handle));
        }

        // Parallel pass: a single flush_to_parquet (→ compact_all)
        // drains every partition.
        let parallel_start = Instant::now();
        shard.flush_to_parquet().await.unwrap();
        let parallel_dur = parallel_start.elapsed();

        // Read back via any handle: per-project read aggregates every
        // partition's cold + hot rows. Total must equal sum of inserts —
        // no partition was dropped, no rows duplicated.
        let any_handle = &handles[0].1;
        let read = any_handle
            .read(&table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            rows_in(&read),
            PARTITIONS * ROWS_PER_PARTITION,
            "parallel compaction lost or duplicated rows",
        );

        eprintln!(
            "[concurrent_compaction_across_partitions_parallel] {PARTITIONS} partitions × {ROWS_PER_PARTITION} rows compacted in {parallel_dur:?} (parallel)",
        );
        // No absolute bar — CI variance is too wide. The correctness
        // assertion above is the load-bearing claim; the wall-clock
        // print lets a follow-up regression be eyeballed quickly.
    }

    /// #28 fan-out correctness: rows written to DISTINCT partitions of the
    /// same `(project, table)` must ALL be visible to a single
    /// `(project, table)`-scoped read after compaction — the read unions every
    /// partition (object-store LIST is table-prefix-scoped, above the partition
    /// segment). This is the load-bearing correctness claim for intra-node
    /// horizontal partitioning: no row may be lost or double-counted regardless
    /// of which partition it landed in.
    #[tokio::test]
    async fn fanout_rows_across_partitions_read_unions_correctly() {
        const PARTITIONS: usize = 4;
        const ROWS_PER_PARTITION: usize = 250;
        let (shard, _sd, _wd, storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();

        // Each partition gets a DISJOINT id range so every id is unique across
        // the whole table — exactly the round-robin fan-out shape (stripe i
        // gets rows i*N .. (i+1)*N). `s0` is `_default`, `s1`.. are `sN`,
        // matching `executor::stripe_partition_key`.
        for i in 0..PARTITIONS {
            let partition = if i == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("s{i}")).unwrap()
            };
            let handle = shard.get(&project, &partition).await.unwrap();
            let start = (i * ROWS_PER_PARTITION) as i64;
            handle
                .write_batch(&table, batch(start, ROWS_PER_PARTITION, "v-"))
                .await
                .unwrap();
        }

        // Flush every partition's tail; `compact_all` schedules them all.
        shard.flush_to_parquet().await.unwrap();

        // Cold read is `(project, table)`-scoped and lists at the table prefix,
        // so it spans EVERY partition subdir. Count must equal the total.
        let stream = storage
            .read(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        let read: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows_in(&read),
            PARTITIONS * ROWS_PER_PARTITION,
            "fan-out read must union every partition: lost or double-counted rows",
        );

        // Point-lookup correctness: every id from every partition is present
        // exactly once, regardless of which partition it landed in.
        let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for b in &read {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for k in 0..ids.len() {
                assert!(seen.insert(ids.value(k)), "duplicate id across partitions");
            }
        }
        for expected in 0..(PARTITIONS * ROWS_PER_PARTITION) as i64 {
            assert!(
                seen.contains(&expected),
                "id {expected} written to a fan-out partition was not found by the union read",
            );
        }
    }

    /// #28 PK-safety: a duplicate single-column-PK value must be detectable no
    /// matter which partition each copy lands in. PK enforcement reads via the
    /// `(project, table)`-scoped `list_data_files_with_stats`, which spans every
    /// partition — so two rows with the same id written to DIFFERENT partitions
    /// are both present in the union and a dup-check sees both. This test proves
    /// the union surface the engine's `enforce_pk_on_insert` reads from contains
    /// the duplicate; the engine then rejects it before the second write.
    #[tokio::test]
    async fn fanout_duplicate_key_visible_across_partitions() {
        let (shard, _sd, _wd, storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();

        // id=42 written to `_default`, and a SECOND id=42 written to `s1`.
        let h0 = shard.get(&project, &PartitionKey::default_key()).await.unwrap();
        h0.write_batch(&table, batch(42, 1, "a-")).await.unwrap();
        let h1 = shard.get(&project, &PartitionKey::new("s1").unwrap()).await.unwrap();
        h1.write_batch(&table, batch(42, 1, "b-")).await.unwrap();

        shard.flush_to_parquet().await.unwrap();

        // The table-scoped union read must contain BOTH id=42 rows — i.e. a
        // PK dup-check that reads this surface sees the collision even though
        // the copies live in different partitions.
        let stream = storage
            .read(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        let read: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut count_42 = 0usize;
        for b in &read {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for k in 0..ids.len() {
                if ids.value(k) == 42 {
                    count_42 += 1;
                }
            }
        }
        assert_eq!(
            count_42, 2,
            "both copies of the duplicate PK must be visible to the cross-partition union \
             so the engine's PK check (which reads this surface) can reject the dup",
        );
    }

    #[tokio::test]
    async fn cold_load_replays_wal() {
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        basin_common::telemetry::try_init_for_tests();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let wal_cfg = || WalConfig {
            object_store: wal_fs.clone(),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        };

        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        // First shard: write 5 batches, drop.
        {
            let wal: Arc<dyn Wal> = Arc::new(LocalWal::open(wal_cfg()).await.unwrap());
            let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal.clone());
            let shard = crate::Shard::new(cfg);
            let handle = shard.get(&project, &partition).await.unwrap();
            for i in 0..5 {
                handle
                    .write_batch(&table, batch(i * 10, 10, "v-"))
                    .await
                    .unwrap();
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }

        // Second shard: reopen, read — all 5 batches replay.
        {
            let wal: Arc<dyn Wal> = Arc::new(LocalWal::open(wal_cfg()).await.unwrap());
            let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal);
            let shard = crate::Shard::new(cfg);
            let handle = shard.get(&project, &partition).await.unwrap();
            let read = handle.read(&table, ReadOptions::default()).await.unwrap();
            assert_eq!(
                rows_in(&read),
                50,
                "cold load should replay all WAL entries"
            );
        }
    }

    #[tokio::test]
    async fn eviction_drops_idle() {
        let (mut shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        // Rewire eviction_idle to zero by recreating the shard.
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();

        let handle = shard.get(&project, &partition).await.unwrap();
        drop(handle);

        // Reach into the impl, override eviction_idle, run one tick.
        let inner = impl_of(&shard);
        // We can't mutate the existing config, so reconstruct a shard with
        // eviction_idle = 0 against the same backends. Simpler path: build a
        // dedicated shard for this test.
        let _ = &mut shard;
        let cfg2 = ShardConfig {
            eviction_idle: Duration::from_secs(0),
            ..inner.cfg.clone()
        };
        let shard2 = crate::Shard::new(cfg2);
        let h2 = shard2.get(&project, &partition).await.unwrap();
        drop(h2);
        // Sleep 1ms so last_active is strictly in the past.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let inner2 = impl_of(&shard2);
        inner2.run_eviction_once().await.unwrap();
        assert_eq!(shard2.stats().resident_partitions, 0);
    }

    #[tokio::test]
    async fn eviction_skips_dirty_tail() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let inner = impl_of(&shard);
        let cfg2 = ShardConfig {
            eviction_idle: Duration::from_secs(0),
            ..inner.cfg.clone()
        };
        let shard2 = crate::Shard::new(cfg2);
        let handle = shard2.get(&project, &partition).await.unwrap();
        handle
            .write_batch(&table, batch(0, 10, "v-"))
            .await
            .unwrap();
        drop(handle);
        tokio::time::sleep(Duration::from_millis(2)).await;

        let inner2 = impl_of(&shard2);
        inner2.run_eviction_once().await.unwrap();
        assert_eq!(
            shard2.stats().resident_partitions,
            1,
            "dirty partition must not be evicted"
        );
    }

    #[tokio::test]
    async fn project_isolation() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let a = ProjectId::new();
        let b = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("shared").unwrap();

        let ha = shard.get(&a, &partition).await.unwrap();
        let hb = shard.get(&b, &partition).await.unwrap();

        ha.write_batch(&table, batch(0, 5, "a-")).await.unwrap();
        hb.write_batch(&table, batch(0, 7, "b-")).await.unwrap();

        let ra = ha.read(&table, ReadOptions::default()).await.unwrap();
        let rb = hb.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&ra), 5);
        assert_eq!(rows_in(&rb), 7);

        // And the names from a's read all start with "a-".
        use arrow_array::Array;
        let names = ra[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..names.len() {
            assert!(
                names.value(i).starts_with("a-"),
                "project a saw {} which is not a's data",
                names.value(i)
            );
        }
    }

    // ── Phase 6.X.A: lease heartbeat + fencing ───────────────────────────

    /// Build a shard wired with an in-memory lease registry under `replica_id`.
    /// The same `InMemoryCatalog` instance is used as catalog AND lease
    /// registry so two shards built against the same catalog contend for real.
    async fn leased_shard(
        catalog: Arc<InMemoryCatalog>,
        replica_id: &str,
        ttl: Duration,
    ) -> (crate::Shard, TempDir, TempDir) {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
        let mut cfg = ShardConfig::new(storage, catalog, wal)
            .with_lease_registry(registry, replica_id);
        cfg.lease_ttl = ttl;
        cfg.lease_renew_interval = Duration::from_millis(50);
        (crate::Shard::new(cfg), storage_dir, wal_dir)
    }

    /// Build a shard against an already-shared `Storage` + `Wal`. Used by the
    /// handoff acceptance test where replicas must share object storage to
    /// mirror ADR 0023's "object store stays the durable substrate" model.
    fn leased_shard_with_storage(
        catalog: Arc<InMemoryCatalog>,
        storage: Storage,
        wal: Arc<dyn Wal>,
        replica_id: &str,
        ttl: Duration,
    ) -> crate::Shard {
        basin_common::telemetry::try_init_for_tests();
        let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
        let mut cfg = ShardConfig::new(storage, catalog, wal)
            .with_lease_registry(registry, replica_id);
        cfg.lease_ttl = ttl;
        cfg.lease_renew_interval = Duration::from_millis(50);
        crate::Shard::new(cfg)
    }

    #[tokio::test]
    async fn two_replicas_contend_for_lease() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();

        // Replica A acquires the lease via `get`.
        let (shard_a, _sa, _wa) =
            leased_shard(catalog.clone(), "replica-a", Duration::from_secs(15)).await;
        let _ha = shard_a.get(&project, &partition).await.unwrap();

        // Replica B (same catalog/registry) is refused — A holds a live lease.
        let (shard_b, _sb, _wb) =
            leased_shard(catalog.clone(), "replica-b", Duration::from_secs(15)).await;
        match shard_b.get(&project, &partition).await {
            Err(BasinError::CommitConflict(_)) => {}
            Ok(_) => panic!("second replica must be refused the lease"),
            Err(e) => panic!("expected CommitConflict, got {e:?}"),
        }

        // The registry confirms A is the owner at epoch 1.
        let owner = catalog.owner_of(&project, partition.as_str()).await.unwrap();
        assert_eq!(owner, Some(("replica-a".to_string(), 1)));
    }

    #[tokio::test]
    async fn lost_lease_drops_partition_state() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();

        // A acquires with a short TTL so its lease can be stolen.
        let (shard_a, _sa, _wa) =
            leased_shard(catalog.clone(), "replica-a", Duration::from_millis(10)).await;
        let _ha = shard_a.get(&project, &partition).await.unwrap();
        let inner_a = impl_of(&shard_a);
        assert_eq!(shard_a.stats().resident_partitions, 1);

        // Let A's lease expire, then B steals it.
        tokio::time::sleep(Duration::from_millis(30)).await;
        catalog
            .acquire(
                &project,
                partition.as_str(),
                "replica-b",
                Duration::from_secs(15),
            )
            .await
            .unwrap()
            .expect("B steals after expiry");

        // A's heartbeat tick now fails to renew (lost lease) and drops state.
        inner_a.run_heartbeat_once().await;
        assert_eq!(
            shard_a.stats().resident_partitions,
            0,
            "lost-lease partition state must be dropped",
        );
        // A no longer tracks the lease epoch.
        assert_eq!(inner_a.lease_epoch(&project, &partition).await, None);
    }

    /// Build a shared (storage, wal, catalog) triple for multi-replica tests
    /// where the replicas must agree on the durable substrate AND the lease
    /// registry (the same `InMemoryCatalog` is both).
    async fn shared_substrate() -> (Storage, Arc<dyn Wal>, Arc<InMemoryCatalog>, TempDir, TempDir) {
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let catalog = Arc::new(InMemoryCatalog::new());
        (storage, wal, catalog, storage_dir, wal_dir)
    }

    /// MULTI-NODE DATA-LOSS regression (Fix A): a READ must NOT steal the
    /// cross-node writer lease. A owns partition `p` (acquired its lease via a
    /// write). B then performs a READ that touches `p` via `get_for_read`. A
    /// must STILL hold `p`'s lease afterwards (B did not steal it), and B's
    /// read returns `p`'s FLUSHED rows without error.
    #[tokio::test]
    async fn read_does_not_steal_writer_lease() {
        let (storage, wal, catalog, _sd, _wd) = shared_substrate().await;
        let project = ProjectId::new();
        let part = PartitionKey::new("s2").unwrap();
        let table = TableName::new("mt").unwrap();

        // A acquires the writer lease for `p` (via `get`) and writes 50 rows.
        let shard_a = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-a",
            Duration::from_secs(30),
        );
        let ha = shard_a.get(&project, &part).await.unwrap();
        ha.write_batch(&table, batch(0, 50, "a-")).await.unwrap();
        let inner_a = impl_of(&shard_a);
        let epoch_before = inner_a
            .lease_epoch(&project, &part)
            .await
            .expect("A holds p's lease after its write");

        // Flush A's tail to the shared object store so the flushed rows are
        // visible cross-node (consistent-after-flush).
        shard_a.flush_to_parquet().await.unwrap();

        // B performs a READ that touches `p` via the no-lease accessor.
        let shard_b = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-b",
            Duration::from_secs(30),
        );
        let hb = shard_b.get_for_read(&project, &part).await.unwrap();
        let read = hb.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 50, "B's read sees p's flushed rows");

        // KEY ASSERTION: A STILL holds p's lease — B did not steal it.
        assert_eq!(
            inner_a.lease_epoch(&project, &part).await,
            Some(epoch_before),
            "A must keep p's writer lease while B reads (no steal)",
        );
        // The registry confirms A is still the live owner.
        assert_eq!(
            catalog.owner_of(&project, part.as_str()).await.unwrap(),
            Some(("replica-a".to_string(), epoch_before)),
        );
        // B never recorded an epoch for `p` (it only read).
        assert_eq!(impl_of(&shard_b).lease_epoch(&project, &part).await, None);

        // A's heartbeat still renews cleanly — its state is NOT dropped (no
        // "dropping partition state" path was taken).
        inner_a.run_heartbeat_once().await;
        assert_eq!(
            shard_a.stats().resident_partitions, 1,
            "A keeps p resident after B's read + a heartbeat",
        );
    }

    /// FLUSH-BEFORE-DROP regression (Fix C): when a node loses the writer
    /// lease while holding an un-flushed tail, the tail must be flushed +
    /// committed to the shared catalog BEFORE the in-memory state is dropped.
    /// Zero rows may be lost across the forced ownership change.
    #[tokio::test]
    async fn lost_lease_flushes_tail_before_dropping_state() {
        let (storage, wal, catalog, _sd, _wd) = shared_substrate().await;
        let project = ProjectId::new();
        let part = PartitionKey::new("s1").unwrap();
        let table = TableName::new("mt").unwrap();

        // A acquires with a short TTL so its lease can be stolen, and writes
        // 50 rows that stay in the in-memory tail (NO flush).
        let shard_a = {
            let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
            let mut cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal.clone())
                .with_lease_registry(registry, "replica-a");
            cfg.lease_ttl = Duration::from_millis(10);
            cfg.lease_renew_interval = Duration::from_millis(50);
            crate::Shard::new(cfg)
        };
        let ha = shard_a.get(&project, &part).await.unwrap();
        ha.write_batch(&table, batch(0, 50, "a-")).await.unwrap();
        let inner_a = impl_of(&shard_a);
        assert!(inner_a.lease_epoch(&project, &part).await.is_some());

        // Force the lease loss: let A's lease expire, then B steals it.
        tokio::time::sleep(Duration::from_millis(30)).await;
        catalog
            .acquire(&project, part.as_str(), "replica-b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("B steals after A's TTL expiry");

        // A's heartbeat tick now fails to renew. Fix C: it FLUSHES + COMMITS
        // the 50-row tail before dropping the partition state.
        inner_a.run_heartbeat_once().await;

        // State is dropped (tail was committed) and the lease is no longer held.
        assert_eq!(
            shard_a.stats().resident_partitions, 0,
            "state dropped only AFTER a successful flush",
        );
        assert_eq!(inner_a.lease_epoch(&project, &part).await, None);

        // ZERO LOSS: the 50 rows are durably in the shared catalog. A fresh
        // reader (no resident tail) sees them from the flushed files alone.
        let reader = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-reader",
            Duration::from_secs(30),
        );
        let rows = reader
            .read_table_merging_tails(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            rows_in(&rows), 50,
            "the un-flushed tail must be flushed+committed on lease loss (zero loss)",
        );
    }

    /// COMPACTION OWNER-ONLY regression (Fix B): a node that does NOT hold a
    /// partition's lease must not compact/commit it (which would race the real
    /// owner's catalog commit — "lost commit race"). Only the holder compacts.
    #[tokio::test]
    async fn only_lease_holder_compacts() {
        let (storage, wal, catalog, _sd, _wd) = shared_substrate().await;
        let project = ProjectId::new();
        let part = PartitionKey::new("s3").unwrap();
        let table = TableName::new("mt").unwrap();

        // B acquires p's lease and writes 50 rows into its in-memory tail
        // (un-flushed, so the tail is dirty and resident).
        let shard_b = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-b",
            Duration::from_secs(30),
        );
        let inner_b = impl_of(&shard_b);
        let hb = shard_b.get(&project, &part).await.unwrap();
        hb.write_batch(&table, batch(0, 50, "b-")).await.unwrap();
        assert!(inner_b.lease_epoch(&project, &part).await.is_some());

        // Simulate the lease-loss-with-lingering-state window: B is no longer
        // the lease holder, but its dirty tail is still resident (the exact
        // condition that caused "lost commit race"). We strip ONLY the held
        // lease, leaving the resident state in place.
        {
            let mut held = inner_b.held_leases.lock().await;
            held.remove(&(project, part.clone()));
        }
        assert_eq!(
            shard_b.stats().resident_partitions, 1,
            "B still has p resident (dirty tail) but no longer holds the lease",
        );

        // B runs a compaction tick. The owner-gate skips `p` (B is not the
        // holder) → it must NOT compact/commit B's tail (no "lost commit
        // race"). Must succeed without error.
        shard_b.flush_to_parquet().await.unwrap();

        // The catalog must carry ZERO rows: a non-owner compaction did NOT
        // commit B's tail. (Had the owner-gate been absent, B would have
        // committed its 50 rows and raced the real owner.)
        let reader = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-reader",
            Duration::from_secs(30),
        );
        let rows = reader
            .read_table_merging_tails(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            rows_in(&rows), 0,
            "non-owner B must not compact/commit p's tail (owner-only gate)",
        );

        // Re-grant the lease to B; now it IS the holder and compaction commits.
        {
            let mut held = inner_b.held_leases.lock().await;
            held.insert((project, part.clone()), 1);
        }
        shard_b.flush_to_parquet().await.unwrap();
        let rows = reader
            .read_table_merging_tails(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            rows_in(&rows), 50,
            "the lease holder compacts + commits its own partition",
        );
    }

    #[tokio::test]
    async fn no_lease_mode_unaffected() {
        // Without a lease registry, get() never acquires and writes go through
        // the unfenced WAL path — byte-for-byte the old behaviour.
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let inner = impl_of(&shard);
        assert_eq!(inner.lease_epoch(&project, &partition).await, None);

        let handle = shard.get(&project, &partition).await.unwrap();
        handle.write_batch(&table, batch(0, 10, "v-")).await.unwrap();
        // Heartbeat is a no-op with no registry.
        inner.run_heartbeat_once().await;
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 10);
    }

    // ── Phase 6.X.C: voluntary lease handoff under load ──────────────────

    /// End-to-end handoff: A holds the lease, takes a write, yields to B.
    /// Post-yield B acquires under a fresh (higher) epoch, the pre-handoff
    /// row set is visible on B (compaction drained the tail before
    /// release), the WAL handoff marker is durable, and the total stall
    /// stays well under the 500 ms p99 target for the small-memtable case.
    #[tokio::test]
    async fn handoff_transfers_lease_with_writes_intact() {
        use basin_wal::WalEvent;

        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        // Shared object storage + WAL — replicas in ADR 0023 share the
        // durable substrate; only the in-memory state is per-replica.
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );

        // A acquires the lease and writes 10 rows.
        let shard_a = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-a",
            Duration::from_secs(15),
        );
        let ha = shard_a.get(&project, &partition).await.unwrap();
        ha.write_batch(&table, batch(0, 10, "pre-")).await.unwrap();
        let inner_a = impl_of(&shard_a);
        let epoch_a = inner_a.lease_epoch(&project, &partition).await.unwrap();
        assert_eq!(epoch_a, 1, "A starts at the first-grant epoch");

        // Yield. Time it: < 500 ms is the ADR 0023 p99 target for the
        // small-memtable case (here: 10 rows).
        let t0 = std::time::Instant::now();
        shard_a
            .yield_partition(&project, &partition, "replica-b")
            .await
            .unwrap();
        let stall = t0.elapsed();
        assert!(
            stall < Duration::from_millis(500),
            "handoff stall {stall:?} exceeds 500ms p99 target (small-memtable case)",
        );

        // Post-yield: A no longer tracks the lease and no resident state.
        assert_eq!(inner_a.lease_epoch(&project, &partition).await, None);
        assert_eq!(shard_a.stats().resident_partitions, 0);
        // The catalog confirms there is no live owner (lease released).
        assert_eq!(
            catalog.owner_of(&project, partition.as_str()).await.unwrap(),
            None,
        );

        // B acquires (via the shared catalog + storage) and reads the
        // partition; the pre-handoff row set must be visible (compaction
        // flushed the tail into the shared object store before release).
        let shard_b = leased_shard_with_storage(
            catalog.clone(),
            storage.clone(),
            wal.clone(),
            "replica-b",
            Duration::from_secs(15),
        );
        let hb = shard_b.get(&project, &partition).await.unwrap();
        let read = hb.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(
            rows_in(&read),
            10,
            "post-handoff replica must see pre-handoff rows",
        );
        // B is now the legitimate single owner — it holds a granted lease
        // and can perform the write path. (After a clean release the
        // catalog row is removed; B's acquire is a first-grant under a
        // fresh epoch. The fencing invariant matters during dual-
        // leaseholder windows, not after a voluntary handoff — see
        // [`Self::yield_partition`] docstring.)
        let inner_b = impl_of(&shard_b);
        let epoch_b = inner_b.lease_epoch(&project, &partition).await.unwrap();
        assert!(epoch_b > 0, "B must hold a granted lease, got epoch {epoch_b}");
        // B can write the partition (new lease → new appends accepted).
        hb.write_batch(&table, batch(100, 2, "post-")).await.unwrap();

        // WAL handoff marker is durable and observable on the shared WAL.
        let events = wal
            .read_events(&project, &partition, basin_wal::Lsn::ZERO)
            .await
            .unwrap();
        let marker = events.iter().find(|e| {
            matches!(
                e,
                WalEvent::Handoff { to_holder, at_epoch }
                if to_holder == "replica-b" && *at_epoch == epoch_a
            )
        });
        assert!(
            marker.is_some(),
            "WAL must carry the handoff marker (to=replica-b, at_epoch=1); got {events:?}",
        );
    }

    /// While the handoff is in progress, writes to the draining partition
    /// fail fast with the typed `LeaseHandoffInProgress` error so the
    /// router can retry against the new owner. We set the draining flag
    /// directly so the assertion doesn't race the state-machine timing.
    #[tokio::test]
    async fn draining_partition_rejects_new_writes() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let (shard, _sd, _wd) =
            leased_shard(catalog.clone(), "replica-a", Duration::from_secs(15)).await;
        let handle = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);

        // Mark the partition draining (synthesise the mid-handoff state).
        {
            let mut d = inner.draining.lock().await;
            d.insert((project, partition.clone()));
        }
        // Write rejected with the typed retryable error.
        let err = handle
            .write_batch(&table, batch(0, 5, "x-"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, BasinError::LeaseHandoffInProgress(_)),
            "draining write must surface LeaseHandoffInProgress, got {err:?}",
        );
        // Reads are unaffected by draining.
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 0);

        // Clear the flag; writes resume.
        {
            let mut d = inner.draining.lock().await;
            d.remove(&(project, partition.clone()));
        }
        handle
            .write_batch(&table, batch(0, 5, "x-"))
            .await
            .unwrap();
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 5);
    }

    /// `yield_partition` is a no-op on a replica that doesn't hold the
    /// lease (no-registry mode + not-our-lease both return Ok without
    /// side effects).
    #[tokio::test]
    async fn yield_is_noop_without_held_lease() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        shard
            .yield_partition(&project, &partition, "replica-x")
            .await
            .unwrap();
    }

    /// The handoff WAL marker is informational: `replay_wal` filters
    /// `WalEvent::Handoff` out and emits exactly one `WalEntry` per
    /// `WalEvent::Entry` in the input stream. Replay is deterministic
    /// (idempotent across repeated invocations).
    #[tokio::test]
    async fn handoff_marker_is_replay_noop() {
        use basin_wal::{replay_wal, WalEvent, WalReplayConfig};

        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let (shard, _sd, _wd) =
            leased_shard(catalog.clone(), "replica-a", Duration::from_secs(15)).await;
        let handle = shard.get(&project, &partition).await.unwrap();
        handle.write_batch(&table, batch(0, 3, "v-")).await.unwrap();
        shard
            .yield_partition(&project, &partition, "replica-b")
            .await
            .unwrap();

        let events = shard
            .wal()
            .read_events(&project, &partition, basin_wal::Lsn::ZERO)
            .await
            .unwrap();
        // Exactly one handoff marker on the WAL.
        let handoff_count = events
            .iter()
            .filter(|e| matches!(e, WalEvent::Handoff { .. }))
            .count();
        assert_eq!(handoff_count, 1, "exactly one handoff marker; got {events:?}");

        // `replay_wal` must yield one WalEntry per data event — markers
        // are filtered out, no entries are synthesised or dropped.
        let data_event_count = events
            .iter()
            .filter(|e| matches!(e, WalEvent::Entry(_)))
            .count();
        let entries = replay_wal(events.clone(), &WalReplayConfig::default());
        assert_eq!(
            entries.len(),
            data_event_count,
            "replay must yield exactly one WalEntry per data event; marker is a no-op",
        );

        // Idempotence: replaying the same event stream again produces
        // identical output — markers don't toggle any state.
        let entries2 = replay_wal(events, &WalReplayConfig::default());
        assert_eq!(
            entries.len(),
            entries2.len(),
            "replay must be deterministic across repeated invocations",
        );
    }

    // ── Phase 6.X.D: heartbeat-reconciled budgets ────────────────────────
    //
    // Multi-replica acceptance test for the multi-instance cap-bypass P0.
    // Three "replicas" (in-process shards) share one in-memory
    // BudgetCoordinator and one in-memory LeaseRegistry. The project is
    // capped at 60 RestQps. After two heartbeat rounds (per ADR 0023 the
    // first round is stale — slices converge within one heartbeat interval),
    // each replica's slice gate admits exactly 20 ops. Aggregate ≤ 60.

    use basin_catalog::{
        BudgetCoordinator, CapKind, InMemoryBudgetCoordinator, ProjectBudget, SliceBudgetView,
        SliceGate,
    };

    #[tokio::test]
    async fn multi_replica_heartbeat_aggregates_rest_qps_under_project_cap() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let coord: Arc<dyn BudgetCoordinator> = Arc::new(InMemoryBudgetCoordinator::new());
        let project = ProjectId::new();

        // Project cap: 60 RestQps across all replicas combined.
        coord
            .set_project_budget(&project, ProjectBudget::default().with(CapKind::RestQps, 60))
            .await
            .unwrap();

        // Three "replicas": each gets its own (shard, partition, slice
        // gate, slice view). Each holds a distinct partition's lease.
        let mut shards: Vec<(crate::Shard, SliceGate, PartitionKey, TempDir, TempDir)> =
            Vec::new();
        for i in 0..3 {
            let storage_dir = TempDir::new().unwrap();
            let wal_dir = TempDir::new().unwrap();
            let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
            let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
            let storage = Storage::new(StorageConfig {
                object_store: Arc::new(storage_fs),
                root_prefix: None,
                disk_cache: None,
                page_cache: None,
            });
            let wal: Arc<dyn Wal> = Arc::new(
                LocalWal::open(WalConfig {
                    object_store: Arc::new(wal_fs),
                    root_prefix: None,
                    flush_interval: Duration::from_millis(50),
                    flush_max_bytes: 1024 * 1024,
                    commit_delay: Duration::from_millis(2),
                })
                .await
                .unwrap(),
            );
            let view = SliceBudgetView::new();
            let gate = SliceGate::new(view.clone());
            let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
            let mut cfg = ShardConfig::new(storage, catalog.clone(), wal)
                .with_lease_registry(registry, format!("replica-{i}"));
            cfg.lease_ttl = Duration::from_secs(60);
            cfg.lease_renew_interval = Duration::from_millis(50);
            cfg = cfg.with_budget_coordinator(coord.clone(), vec![view], vec![gate.clone()]);
            let partition = PartitionKey::new(format!("p-{i}")).unwrap();
            let shard = crate::Shard::new(cfg);
            // Acquire the lease for this replica's partition.
            let _h = shard.get(&project, &partition).await.unwrap();
            shards.push((shard, gate, partition, storage_dir, wal_dir));
        }

        // Two heartbeat rounds. After round 1 every coordinator entry knows
        // the live partition count; after round 2 every slice view reflects
        // slice = 60 / 3 = 20.
        for _ in 0..2 {
            for (shard, _gate, _part, _sd, _wd) in &shards {
                let inner = impl_of(shard);
                inner.run_budget_heartbeat_once().await;
            }
        }

        for (_shard, gate, _part, _sd, _wd) in &shards {
            assert_eq!(
                gate.view().slice_for(project, CapKind::RestQps).await,
                20,
                "after convergence each replica must see slice=20",
            );
        }

        // Each replica burns its slice; aggregate must equal the project cap.
        let mut admitted = 0u64;
        for (_shard, gate, _part, _sd, _wd) in &shards {
            for _ in 0..40 {
                if gate
                    .try_consume(project, CapKind::RestQps, 1)
                    .await
                    .is_ok()
                {
                    admitted += 1;
                }
            }
        }
        assert_eq!(
            admitted, 60,
            "multi-replica aggregate must equal the project cap (60), not 3 × 20 = 60 \
             but the same N × per_process_cap would have given 180+",
        );
    }

    /// Failure path: with no coordinator attached, the heartbeat tick is a
    /// no-op and cap consumers fall back to their per-process defaults
    /// (back-compat). Verifies the safe degradation.
    #[tokio::test]
    async fn no_coordinator_heartbeat_is_noop_and_safe() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let _h = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);
        // No coordinator wired — must not panic, must not error.
        inner.run_budget_heartbeat_once().await;
    }

    // ── Phase 6.X.F: lease observability metric emission ─────────────────

    /// Acquire-success + heartbeat-renew-success bump the corresponding
    /// counters on the attached [`basin_common::LeaseMetrics`] sink. The
    /// holdings gauge tracks the live lease count; the heartbeat-lag
    /// histogram records one sample per renew tick.
    #[tokio::test]
    async fn lease_metrics_emit_acquire_renew_and_heartbeat_lag() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let metrics = Arc::new(basin_common::LeaseMetrics::new());

        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
        let mut cfg = ShardConfig::new(storage, catalog.clone(), wal)
            .with_lease_registry(registry, "replica-metrics")
            .with_lease_metrics(metrics.clone());
        cfg.lease_ttl = Duration::from_secs(60);
        cfg.lease_renew_interval = Duration::from_millis(50);
        let shard = crate::Shard::new(cfg);

        let _h = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);
        inner.run_heartbeat_once().await;

        let snap = metrics.snapshot_replicas();
        let r = snap
            .get("replica-metrics")
            .expect("replica-metrics has emitted counters");
        assert_eq!(r.acquire_acquired_total, 1);
        assert_eq!(r.renew_ok_total, 1);
        assert_eq!(r.holdings, 1);
        assert_eq!(
            r.heartbeat_lag_ms.count, 1,
            "exactly one heartbeat-lag sample per renew tick",
        );
        // No handoff samples in v1 — 6.X.C ships them.
        assert_eq!(r.handoff_duration_ms.count, 0);
    }

    /// A losing acquire (lease already held by a peer) bumps the `failed`
    /// counter and leaves the holdings gauge unchanged.
    #[tokio::test]
    async fn lease_metrics_emit_failed_acquire_for_contested_lease() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();

        // Replica A grabs first; its metrics handle is irrelevant for this test.
        let (shard_a, _sa, _wa) =
            leased_shard(catalog.clone(), "replica-a", Duration::from_secs(60)).await;
        let _ha = shard_a.get(&project, &partition).await.unwrap();

        // Replica B has its own metrics sink — we observe the `failed` bump
        // on B's side.
        let metrics_b = Arc::new(basin_common::LeaseMetrics::new());
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
        let cfg = ShardConfig::new(storage, catalog, wal)
            .with_lease_registry(registry, "replica-b")
            .with_lease_metrics(metrics_b.clone());
        let shard_b = crate::Shard::new(cfg);

        let res = shard_b.get(&project, &partition).await;
        match res {
            Err(BasinError::CommitConflict(_)) => {}
            Ok(_) => panic!("second replica must be refused the lease"),
            Err(e) => panic!("expected CommitConflict, got {e:?}"),
        }

        let snap = metrics_b.snapshot_replicas();
        let b = snap.get("replica-b").expect("replica-b emitted counters");
        assert_eq!(b.acquire_failed_total, 1);
        assert_eq!(
            b.acquire_acquired_total, 0,
            "failed acquire must not double-count as acquired",
        );
        assert_eq!(b.holdings, 0, "failed acquire does not change holdings");
    }

    /// Lost-lease path: the renew tick observes the lease was stolen,
    /// bumps `renew_expired_total`, and decrements the holdings gauge.
    #[tokio::test]
    async fn lease_metrics_decrement_holdings_on_lost_lease() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let metrics = Arc::new(basin_common::LeaseMetrics::new());

        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
        let mut cfg = ShardConfig::new(storage, catalog.clone(), wal)
            .with_lease_registry(registry, "replica-evicted")
            .with_lease_metrics(metrics.clone());
        cfg.lease_ttl = Duration::from_millis(10);
        cfg.lease_renew_interval = Duration::from_millis(50);
        let shard = crate::Shard::new(cfg);

        let _h = shard.get(&project, &partition).await.unwrap();
        // Wait for TTL to expire, then a peer steals.
        tokio::time::sleep(Duration::from_millis(30)).await;
        catalog
            .acquire(
                &project,
                partition.as_str(),
                "replica-peer",
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            .expect("peer steals after expiry");

        let inner = impl_of(&shard);
        inner.run_heartbeat_once().await;

        let snap = metrics.snapshot_replicas();
        let r = snap.get("replica-evicted").expect("replica entry");
        assert_eq!(r.acquire_acquired_total, 1);
        assert_eq!(r.renew_expired_total, 1);
        assert_eq!(
            r.holdings, 0,
            "lost-lease decrements the holdings gauge back to zero",
        );
    }

    /// Without a metrics sink configured, the lease event sites are
    /// zero-cost — no panic, no double-bookkeeping. Back-compat guarantee.
    #[tokio::test]
    async fn no_lease_metrics_path_does_not_panic() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let (shard, _sd, _wd) =
            leased_shard(catalog, "replica-nometrics", Duration::from_secs(60)).await;
        let _h = shard.get(&project, &partition).await.unwrap();
        let inner = impl_of(&shard);
        inner.run_heartbeat_once().await;
        // Survival is the assertion.
    }

    /// PG-Wave-α: `reindex_compacted_file_rtree` writes the `.rtree`
    /// sidecar at the canonical sidecar path when the table has a GIST
    /// index on a POINT column. With no GIST index nothing is written;
    /// with a GIST index on a non-POINT column the helper skips the
    /// index quietly. Run direct against the helper (no shard plumbing)
    /// so the assertion is precise: the sidecar key is what the engine
    /// pushdown layer will probe in PG-Wave-β.
    #[tokio::test]
    async fn compactor_writes_rtree_sidecar_when_gist_present() {
        use arrow_array::builder::{FixedSizeBinaryBuilder, Int64Builder};
        use arrow_schema::{DataType, Field, Schema};
        use basin_storage::{Storage, StorageConfig};
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStoreExt;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let project = ProjectId::new();
        let table = TableName::new("locs").unwrap();

        // Build a small batch: 8 POINT rows (x=0..8, y=0..8).
        let mut ids = Int64Builder::with_capacity(8);
        let mut pts =
            FixedSizeBinaryBuilder::with_capacity(8, basin_geo::POINT_WKB_LEN as i32);
        for i in 0..8i64 {
            ids.append_value(i);
            let bytes = basin_geo::encode_point(&basin_geo::Point::new(i as f64, i as f64));
            pts.append_value(bytes).unwrap();
        }
        let mut meta = std::collections::HashMap::new();
        meta.insert("BASIN_TYPE".to_string(), "POINT".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "p",
                DataType::FixedSizeBinary(basin_geo::POINT_WKB_LEN as i32),
                true,
            )
            .with_metadata(meta),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::new(ids.finish()), Arc::new(pts.finish())],
        )
        .unwrap();

        // The data file's logical path — what `compact_one` would have
        // written. We don't actually write it; we just need a valid
        // canonical path so the sidecar key derives.
        let data_file_path = format!(
            "projects/{}/tables/locs/data/01J7Z0FZ0K0K0K0K0K0K0K0K0K.parquet",
            project.as_prefix()
        );

        // Indexes vec mimicking what the catalog returns.
        let gist_on_p = basin_catalog::SecondaryIndex {
            name: "locs_p_gist".into(),
            columns: vec!["p".into()],
            access_method: "gist".into(),
            opclass: Some("range_ops".into()),
        };
        let gist_on_unknown = basin_catalog::SecondaryIndex {
            name: "locs_other_gist".into(),
            columns: vec!["other_col".into()],
            access_method: "gist".into(),
            opclass: None,
        };
        let gin_jsonb = basin_catalog::SecondaryIndex {
            name: "locs_doc_gin".into(),
            columns: vec!["doc".into()],
            access_method: "gin".into(),
            opclass: Some("jsonb_ops".into()),
        };

        // 1) No GIST index → no sidecar.
        reindex_compacted_file_rtree(
            storage.clone(),
            &project,
            &table,
            &[gin_jsonb.clone()],
            4u32,
            &batch,
            &data_file_path,
        )
        .await
        .expect("no-op when only GIN");

        // 2) GIST on a column that isn't on the batch → no sidecar.
        reindex_compacted_file_rtree(
            storage.clone(),
            &project,
            &table,
            &[gist_on_unknown.clone()],
            4u32,
            &batch,
            &data_file_path,
        )
        .await
        .expect("skip unknown-column gist");

        // 3) GIST on POINT column → sidecar exists.
        reindex_compacted_file_rtree(
            storage.clone(),
            &project,
            &table,
            &[gist_on_p, gin_jsonb, gist_on_unknown],
            4u32,
            &batch,
            &data_file_path,
        )
        .await
        .expect("gist-on-point sidecar must be written");

        let expected = basin_storage::index::rtree::rtree_segment_key_for_data_file(
            None,
            &project,
            &table,
            "p",
            &data_file_path,
        )
        .expect("canonical data path");
        let got = storage
            .project_object_store(&project)
            .get(&expected)
            .await
            .expect("R-tree sidecar must exist at canonical path");
        let bytes = got.bytes().await.unwrap();
        assert!(bytes.starts_with(b"BR1\n"), "sidecar carries magic header");

        let rtree = basin_storage::index::rtree::deserialize_rtree(&bytes)
            .expect("sidecar bytes deserialise");
        assert_eq!(rtree.size(), 8, "all 8 POINT rows indexed");
    }

    // ── S4 clean-retention sweep ──────────────────────────────────────────────

    /// The background sweep evicts clean (flushed-and-retained) hot-tier rows
    /// once they outlive the retention window — and it must pick up a registry
    /// wired in AFTER `spawn_background`, because that is the production
    /// order (the server spawns the loops before `Engine::new` wires the
    /// registry).
    #[tokio::test]
    async fn sweep_evicts_clean_after_retention() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard_with(|cfg| {
            cfg.clean_sweep_interval = Duration::from_millis(25);
        })
        .await;

        // retain_secs == 0 is the documented kill switch: clean entries are
        // evictable on the very first sweep, so the test needs no clock
        // games — only the wiring + tick path is under test here (the
        // retention arithmetic itself is covered in basin-hottier).
        let reg = Arc::new(basin_hottier::MemTableRegistry::new_with_config(
            basin_hottier::MemTableConfig {
                retain_secs: 0,
                ..Default::default()
            },
        ));
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let entry = reg.get_or_create(project, table.clone());
        let _ = reg.try_reserve_bytes(&project, 256);
        entry.memtable.insert_clean(
            basin_hottier::RowKey::builder().append_i64(1).finish(),
            basin_hottier::MemRowValue::row(vec![0u8; 256], 0),
        );
        assert_eq!(reg.project_clean_bytes(&project), 256);

        // Spawn FIRST, wire SECOND — pins the late-wiring contract.
        let bg = shard.spawn_background();
        shard.set_memtable_registry(reg.clone());

        // Poll with a generous deadline rather than sleeping a fixed number
        // of ticks; the sweep interval is 25 ms so this normally resolves in
        // well under a second.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let clean = reg.project_clean_bytes(&project);
            let swept = shard.stats().clean_sweep_evictions_bytes;
            if clean == 0 && swept >= 256 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "sweep did not evict clean bytes within deadline \
                 (clean={clean}, swept={swept})",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        bg.shutdown().await;
    }

    /// With no registry wired, the sweep tick is a harmless no-op: the loop
    /// keeps running, the shard keeps serving writes/reads, and the stats
    /// counter stays at zero.
    #[tokio::test]
    async fn sweep_noop_without_registry() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard_with(|cfg| {
            cfg.clean_sweep_interval = Duration::from_millis(10);
        })
        .await;

        let bg = shard.spawn_background();

        // Let a number of sweep ticks fire with no registry wired.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The shard still works normally around the no-op ticks.
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();
        let handle = shard.get(&project, &partition).await.unwrap();
        handle
            .write_batch(&table, batch(0, 10, "v-"))
            .await
            .unwrap();
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), 10);

        assert_eq!(
            shard.stats().clean_sweep_evictions_bytes,
            0,
            "no registry wired — sweep must not record evictions",
        );

        bg.shutdown().await;
    }

    // ── Stripe-merge compaction ──────────────────────────────────────────

    fn pkb(v: i64) -> PkBound {
        PkBound::Int(v)
    }

    #[test]
    fn pk_ranges_overlap_detects_interleaved_stripes() {
        // Round-robin striping: every file spans nearly the whole range.
        let mut overlapping = vec![
            (pkb(0), pkb(997)),
            (pkb(1), pkb(998)),
            (pkb(2), pkb(999)),
            (pkb(3), pkb(996)),
        ];
        assert!(pk_ranges_overlap(&mut overlapping));

        // Post-merge layout: strictly disjoint, in any input order.
        let mut disjoint = vec![
            (pkb(500), pkb(749)),
            (pkb(0), pkb(249)),
            (pkb(750), pkb(999)),
            (pkb(250), pkb(499)),
        ];
        assert!(!pk_ranges_overlap(&mut disjoint));

        // Touching boundaries (duplicate PK at the seam) count as overlap —
        // conservative: merge rather than miss a prune opportunity.
        let mut touching = vec![(pkb(0), pkb(10)), (pkb(10), pkb(20))];
        assert!(pk_ranges_overlap(&mut touching));
    }

    #[test]
    fn decode_pk_range_follows_writer_byte_contract() {
        use basin_catalog::ColumnStats;
        let cs = ColumnStats {
            null_count: Some(0),
            min_bytes: Some(7i64.to_le_bytes().to_vec()),
            max_bytes: Some(99i64.to_le_bytes().to_vec()),
            sum_bytes: None,
        };
        assert_eq!(
            decode_pk_range(&DataType::Int64, &cs),
            Some((PkBound::Int(7), PkBound::Int(99)))
        );
        // Utf8 PKs carry no writer stats → no zone map → not a candidate.
        assert_eq!(decode_pk_range(&DataType::Utf8, &cs), None);
        // Missing bounds → None.
        let empty = ColumnStats::default();
        assert_eq!(decode_pk_range(&DataType::Int64, &empty), None);
    }

    /// End-to-end (in-crate): four stripe files with interleaved PKs merge
    /// into one PK-sorted file; rows survive byte-for-byte and the catalog's
    /// stats show the disjoint (single-file) layout.
    #[tokio::test]
    async fn stripe_merge_merges_overlapping_stripe_files() {
        use basin_catalog::Catalog as _;

        let (shard, _sd, _wd, storage, catalog, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();

        // Create the table and declare a single-column Int64 PK so the
        // compactor PK-sorts files and the merge selector engages.
        catalog
            .create_table(&project, &table, schema().as_ref())
            .await
            .unwrap();
        catalog
            .set_table_constraints(&project, &table, vec!["id".into()], vec![], vec![])
            .await
            .unwrap();

        // Interleave PKs across 4 stripe partitions (round-robin layout):
        // stripe s gets ids {s, s+4, s+8, ...} — every flushed file spans
        // nearly the whole PK range, so the zone maps all overlap.
        const STRIPES: usize = 4;
        const PER_STRIPE: usize = 25;
        for s in 0..STRIPES {
            let key = if s == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("s{s}")).unwrap()
            };
            let handle = shard.get(&project, &key).await.unwrap();
            let ids: Int64Array = (0..PER_STRIPE as i64)
                .map(|k| s as i64 + k * STRIPES as i64)
                .collect();
            let names: StringArray = (0..PER_STRIPE).map(|_| Some("v")).collect();
            let b = RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap();
            handle.write_batch(&table, b).await.unwrap();
        }

        let inner = impl_of(&shard);
        inner.run_compaction_once().await.unwrap();

        let before = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files();
        assert_eq!(before.len(), STRIPES, "one flushed file per stripe");
        let mut before_ranges: Vec<(PkBound, PkBound)> = before
            .iter()
            .map(|f| decode_pk_range(&DataType::Int64, f.column_stats.get("id").unwrap()).unwrap())
            .collect();
        assert!(
            pk_ranges_overlap(&mut before_ranges),
            "round-robin stripe files must have overlapping PK zone maps"
        );

        inner.run_stripe_merge_once().await.unwrap();

        let after = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files();
        assert_eq!(after.len(), 1, "4 small stripe files merge into 1");
        let total: u64 = after.iter().map(|f| f.row_count).sum();
        assert_eq!(total as usize, STRIPES * PER_STRIPE);
        let (mn, mx) =
            decode_pk_range(&DataType::Int64, after[0].column_stats.get("id").unwrap()).unwrap();
        assert_eq!(mn, PkBound::Int(0));
        assert_eq!(mx, PkBound::Int((STRIPES * PER_STRIPE) as i64 - 1));
        assert_eq!(shard.stats().stripe_merges, 1);

        // Values identical: read the merged file back and check the full,
        // PK-sorted id sequence survived the merge.
        let path = object_store::path::Path::from(after[0].path.clone());
        let batches: Vec<RecordBatch> = storage
            .read_file(&project, &path)
            .await
            .unwrap()
            .collect::<Vec<Result<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut ids: Vec<i64> = Vec::new();
        for b in &batches {
            let col = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            ids.extend(col.values().iter().copied());
        }
        let expected: Vec<i64> = (0..(STRIPES * PER_STRIPE) as i64).collect();
        assert_eq!(ids, expected, "merged file must hold every row, PK-sorted");

        // Idempotence: a second sweep finds a single (disjoint) file and
        // does nothing.
        inner.run_stripe_merge_once().await.unwrap();
        assert_eq!(shard.stats().stripe_merges, 1);
        assert_eq!(
            catalog
                .load_table(&project, &table)
                .await
                .unwrap()
                .live_data_files()
                .len(),
            1
        );
    }

    // ----------------------------------------------------------------------
    // File-count-bounding merge compaction (flat ingest at scale).
    // ----------------------------------------------------------------------

    /// Serialises the env-var-driven file-merge tests: the merge thresholds are
    /// read from process-global env, so two tests mutating them concurrently
    /// would race. Each test holds this for its duration and restores the env.
    static FILE_MERGE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        keys: Vec<(&'static str, Option<String>)>,
    }
    impl EnvGuard {
        fn set(pairs: &[(&'static str, &str)]) -> Self {
            let keys = pairs
                .iter()
                .map(|(k, v)| {
                    let prev = std::env::var(k).ok();
                    std::env::set_var(k, v);
                    (*k, prev)
                })
                .collect();
            Self { keys }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, prev) in &self.keys {
                match prev {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    /// Flush `n` separate files into one (default) partition by writing then
    /// compacting `n` times. Each compaction drains the tail into exactly one
    /// new data file, so the partition's live-file count grows ~1 per cycle —
    /// the unbounded growth the merge sweep exists to bound.
    async fn flush_n_files(
        shard: &crate::Shard,
        inner: &Arc<InProcessShard>,
        project: &ProjectId,
        table: &TableName,
        n: usize,
        rows_each: usize,
    ) {
        let key = PartitionKey::default_key();
        let handle = shard.get(project, &key).await.unwrap();
        for i in 0..n {
            let b = batch((i * rows_each) as i64, rows_each, "v");
            handle.write_batch(table, b).await.unwrap();
            inner.run_compaction_once().await.unwrap();
        }
    }

    /// Ingest M flushes into one partition, then run the merge sweep to a fixed
    /// point: the live data-file COUNT must drop to/under the ceiling (it must
    /// NOT stay at M), and every row must survive (count + full id set).
    #[tokio::test]
    async fn file_merge_bounds_partition_file_count() {
        use basin_catalog::Catalog as _;
        let _env_lock = FILE_MERGE_ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[
            ("BASIN_COMPACT_MAX_FILES_PER_PARTITION", "4"),
            ("BASIN_COMPACT_MERGE_BATCH", "4"),
        ]);

        let (shard, _sd, _wd, _storage, catalog, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        catalog
            .create_table(&project, &table, schema().as_ref())
            .await
            .unwrap();

        const M: usize = 20;
        const ROWS: usize = 3;
        flush_n_files(&shard, &impl_of(&shard), &project, &table, M, ROWS).await;

        let before = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files();
        assert_eq!(before.len(), M, "one flushed file per write+compact cycle");
        let rows_before: u64 = before.iter().map(|f| f.row_count).sum();
        assert_eq!(rows_before as usize, M * ROWS);

        // Drive the sweep to a fixed point (bounded work per tick → several
        // ticks to walk M files down to the ceiling).
        let inner = impl_of(&shard);
        for _ in 0..M {
            inner.run_file_merge_once().await.unwrap();
            let n = catalog
                .load_table(&project, &table)
                .await
                .unwrap()
                .live_data_files()
                .len();
            if n <= 4 {
                break;
            }
        }

        let after = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files();
        assert!(
            after.len() <= 4,
            "file count must be bounded by the ceiling, got {} (was {M})",
            after.len()
        );
        let rows_after: u64 = after.iter().map(|f| f.row_count).sum();
        assert_eq!(
            rows_after as usize,
            M * ROWS,
            "no rows lost or duplicated across merges"
        );
        assert!(shard.stats().file_merges >= 1, "at least one merge committed");

        // Full row set identical: read every live file, collect ids, expect the
        // exact dense range [0, M*ROWS).
        let mut ids: Vec<i64> = Vec::new();
        for f in &after {
            let path = object_store::path::Path::from(f.path.clone());
            let batches: Vec<RecordBatch> = _storage
                .read_file(&project, &path)
                .await
                .unwrap()
                .collect::<Vec<Result<RecordBatch>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            for b in &batches {
                let col = b
                    .column(b.schema().index_of("id").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                ids.extend(col.values().iter().copied());
            }
        }
        ids.sort_unstable();
        let expected: Vec<i64> = (0..(M * ROWS) as i64).collect();
        assert_eq!(ids, expected, "exact union of all input rows, no dup/loss");
    }

    /// A SINGLE merge tick consumes at most `BASIN_COMPACT_MERGE_BATCH` input
    /// files — it does not collapse an unbounded partition in one pass (keeping
    /// the merge off the hot ingest path).
    #[tokio::test]
    async fn file_merge_bounded_work_per_tick() {
        use basin_catalog::Catalog as _;
        let _env_lock = FILE_MERGE_ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[
            ("BASIN_COMPACT_MAX_FILES_PER_PARTITION", "4"),
            ("BASIN_COMPACT_MERGE_BATCH", "4"),
        ]);

        let (shard, _sd, _wd, _storage, catalog, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        catalog
            .create_table(&project, &table, schema().as_ref())
            .await
            .unwrap();

        const M: usize = 20;
        flush_n_files(&shard, &impl_of(&shard), &project, &table, M, 2).await;
        let before = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files()
            .len();
        assert_eq!(before, M);

        // ONE tick: merges a batch of 4 smallest → 1 (or a few) output. The net
        // count drop must be bounded by batch-1 (we removed <= batch inputs,
        // added >= 1 output), so the count cannot collapse to the ceiling in a
        // single tick.
        impl_of(&shard).run_file_merge_once().await.unwrap();
        let after = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files()
            .len();
        let dropped = before - after;
        assert!(dropped >= 1, "the tick must make progress");
        assert!(
            dropped <= 4 - 1,
            "one tick removes at most batch (4) inputs and adds >= 1 output, so net drop <= 3; got {dropped}"
        );
        assert!(
            after > 4,
            "a single tick must NOT collapse 20 files to the ceiling"
        );
    }

    /// count(*) (and the live-file set) are unchanged when the partition is
    /// already at/under the ceiling: the sweep is a no-op, never rewrites.
    #[tokio::test]
    async fn file_merge_noop_under_ceiling() {
        use basin_catalog::Catalog as _;
        let _env_lock = FILE_MERGE_ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::set(&[("BASIN_COMPACT_MAX_FILES_PER_PARTITION", "8")]);

        let (shard, _sd, _wd, _storage, catalog, _wal) = fresh_shard().await;
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        catalog
            .create_table(&project, &table, schema().as_ref())
            .await
            .unwrap();

        flush_n_files(&shard, &impl_of(&shard), &project, &table, 3, 5).await;
        let mut before: Vec<String> = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        before.sort();
        assert_eq!(before.len(), 3);

        impl_of(&shard).run_file_merge_once().await.unwrap();

        let mut after: Vec<String> = catalog
            .load_table(&project, &table)
            .await
            .unwrap()
            .live_data_files()
            .into_iter()
            .map(|f| f.path)
            .collect();
        after.sort();
        assert_eq!(before, after, "under-ceiling partition is untouched");
        assert_eq!(shard.stats().file_merges, 0);
    }

    // ----------------------------------------------------------------------
    // Phase-2 autotune exactly-once regression (runtime knob mutation).
    // ----------------------------------------------------------------------

    /// Restores the runtime flush override to "unset" (0) on drop so the
    /// no-op-at-default invariant other tests assert is never broken.
    struct RuntimeKnobGuard;
    impl Drop for RuntimeKnobGuard {
        fn drop(&mut self) {
            basin_common::autotune::set_runtime_flush_concurrency(0);
        }
    }

    /// Build a shard whose catalog is an `ObjectStoreCatalog` over the SAME
    /// object store the `Storage` reads/writes (the dev/cloud topology: the
    /// per-partition segment chains and the table-prefix data-file LIST both
    /// live in one store). Returns the shard, the storage handle, and the
    /// shared store so the test can drive a LIST-based count(*).
    async fn fresh_osc_shard() -> (crate::Shard, Storage, Arc<dyn basin_catalog::Catalog>) {
        use basin_catalog::ObjectStoreCatalog;
        use object_store::memory::InMemory;
        basin_common::telemetry::try_init_for_tests();
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let storage = Storage::new(StorageConfig {
            object_store: store.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> =
            Arc::new(ObjectStoreCatalog::new(store.clone()));
        let wal_fs = LocalFileSystem::new_with_prefix(TempDir::new().unwrap().keep()).unwrap();
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(wal_fs),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal);
        (crate::Shard::new(cfg), storage, catalog)
    }

    /// Count every row visible to a no-predicate, table-prefix LIST read — the
    /// exact surface `count(*)` sums (object-store LIST across every partition
    /// subdir, NOT the catalog live set). See `reader::read`.
    async fn list_count(storage: &Storage, project: &ProjectId, table: &TableName) -> usize {
        let stream = storage
            .read(project, table, ReadOptions::default())
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        rows_in(&batches)
    }

    /// REGRESSION (Phase-2 autotune correctness): a bulk ingest whose
    /// partition TOPOLOGY shifts mid-stream — exactly what a runtime fan-out
    /// change used to do to `executor::write_batch_fanout`'s round-robin
    /// (`idx = cursor % fanout`, `stripe_partition_key(idx)`) — must stay
    /// exactly-once, AND the now-only runtime knob (flush concurrency) may be
    /// mutated live throughout without breaking it.
    ///
    /// We drive a multi-batch ingest across a SHIFTING set of stripe partitions
    /// (mimicking fan-out climbing/backing-off 4→10→6→12→4) while the
    /// background compaction + file-merge sweeps run CONCURRENTLY and the live
    /// flush override is flipped each chunk. The LIST-based count(*) must equal
    /// the rows actually written: every id exactly once, none duplicated.
    ///
    /// The dev over-count this guards: a clean 20M COPY read back 23.31M
    /// (~16.5% phantom) precisely when the adaptive controller changed fan-out
    /// mid-ingest, because shifting the partition set under the concurrent
    /// compaction + file-merge sweeps (which run under DIFFERENT locks) left
    /// pre-merge sources and post-merge output both physically live under the
    /// table prefix, which the LIST-based count double-counts. The fix pins
    /// fan-out; this test keeps the data path honest under topology flux AND
    /// confirms a live flush change alone is exactly-once-safe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn runtime_knob_change_mid_ingest_stays_exactly_once() {
        let _guard = RuntimeKnobGuard;
        let _env_lock = FILE_MERGE_ENV_LOCK.lock().unwrap();
        // Make the file-merge sweep engage aggressively so a mid-ingest merge
        // actually runs against the fanned-out partitions.
        let _env = EnvGuard::set(&[
            ("BASIN_COMPACT_MAX_FILES_PER_PARTITION", "2"),
            ("BASIN_COMPACT_MERGE_BATCH", "4"),
        ]);

        let (shard, storage, catalog) = fresh_osc_shard().await;
        let shard = Arc::new(shard);
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        catalog
            .create_table(&project, &table, schema().as_ref())
            .await
            .unwrap();

        let inner = impl_of(&shard);

        // Mirror `executor::write_batch_fanout`'s partition selection:
        // partition = stripe i where i = cursor % fanout.
        let stripe_key = |i: usize| -> PartitionKey {
            if i == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("s{i}")).unwrap()
            }
        };

        // Background compaction + file-merge sweeps run continuously, exactly
        // as the shard's background loop drives them — CONCURRENTLY with the
        // ingest and the knob changes.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sweeper = {
            let inner = inner.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = inner.run_compaction_once().await;
                    let _ = inner.run_file_merge_once().await;
                    tokio::task::yield_now().await;
                }
            })
        };

        const CHUNKS: usize = 48;
        const ROWS_PER_CHUNK: usize = 100;
        let mut cursor = 0usize;
        let mut next_id: i64 = 0;
        for chunk in 0..CHUNKS {
            // The partition fan-out shifts mid-stream the way a (now-removed)
            // runtime fan-out change would have reshaped routing.
            let fanout = match chunk % 10 {
                0 => 8,
                2 => 10,
                4 => 6,
                6 => 12,
                8 => 4,
                _ => 8,
            };
            // The ONLY live runtime ingest knob — flush concurrency — is also
            // mutated each chunk; this must be exactly-once-safe on its own.
            basin_common::autotune::set_runtime_flush_concurrency(4 + (chunk % 13));

            let idx = cursor % fanout;
            cursor += 1;
            let part = stripe_key(idx);
            let handle = shard.get(&project, &part).await.unwrap();
            handle
                .write_batch(&table, batch(next_id, ROWS_PER_CHUNK, "v-"))
                .await
                .unwrap();
            next_id += ROWS_PER_CHUNK as i64;
            tokio::task::yield_now().await;
        }

        // Stop the sweeper, then drain + merge to a fixed point.
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = sweeper.await;
        inner.run_compaction_once().await.unwrap();
        for _ in 0..CHUNKS {
            inner.run_file_merge_once().await.unwrap();
        }

        let written = CHUNKS * ROWS_PER_CHUNK;
        let counted = list_count(&storage, &project, &table).await;
        assert_eq!(
            counted, written,
            "mid-ingest topology + flush mutation must stay exactly-once: wrote {written} \
             distinct rows, count(*) (LIST-based) sees {counted}",
        );

        // Every id present exactly once — no phantom duplicates.
        let stream = storage
            .read(&project, &table, ReadOptions::default())
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for b in &batches {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for k in 0..ids.len() {
                assert!(
                    seen.insert(ids.value(k)),
                    "id {} double-counted after a mid-ingest fan-out change",
                    ids.value(k)
                );
            }
        }
        assert_eq!(seen.len(), written, "every written id present exactly once");
    }

    // ----------------------------------------------------------------------
    // Parallel compaction + WAL GC (flat ingest under high object-store RTT).
    // ----------------------------------------------------------------------

    /// Object-store decorator that sleeps before every `put` so a fast local
    /// store behaves like a high-RTT remote one. Without injected latency a
    /// loopback/tmpfs store hides the very effect under test (serial flushes
    /// only fall behind ingest when each PUT is latency-bound).
    #[derive(Debug)]
    struct LatencyPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
        delay: Duration,
        puts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl LatencyPutStore {
        fn new(inner: Arc<dyn object_store::ObjectStore>, delay: Duration) -> Self {
            Self {
                inner,
                delay,
                puts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    impl std::fmt::Display for LatencyPutStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LatencyPutStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for LatencyPutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(self.delay).await;
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            tokio::time::sleep(self.delay).await;
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, opts).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::ObjectMeta>,
        > {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            opts: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// Total bytes of all `.seg` files under a WAL directory — the live WAL
    /// footprint. Used to assert WAL GC keeps the on-disk WAL bounded.
    fn wal_dir_seg_bytes(dir: &std::path::Path) -> u64 {
        fn walk(p: &std::path::Path, acc: &mut u64) {
            let Ok(rd) = std::fs::read_dir(p) else {
                return;
            };
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                    *acc += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
        let mut acc = 0;
        walk(dir, &mut acc);
        acc
    }

    /// Parallel compaction keeps the tail bounded under a high-RTT store.
    ///
    /// With ~20ms-per-PUT latency injected, a SERIAL drain of a many-chunk
    /// backlog takes `n_files * 20ms`; the concurrent drain overlaps the PUTs
    /// so the same backlog clears in roughly `ceil(n_files/concurrency) * 20ms`.
    /// We compare a CONCURRENT drain (flush_concurrency=N) against a SERIAL
    /// drain (flush_concurrency=1) of the SAME multi-file backlog under the
    /// same high per-PUT latency. The encode cost is identical in both runs,
    /// so the wall-clock delta isolates the PUT-latency term: serial pays it
    /// `n` times, concurrent pays it ~`n/N` times. We assert (a) the whole
    /// backlog drains in ONE call (tail empty), (b) more than one file was
    /// produced (the concurrency path is exercised), (c) the concurrent run is
    /// meaningfully faster than serial (PUTs overlapped), and (d) every row
    /// survives. This proves compaction throughput scales with PUT bandwidth,
    /// which is what keeps the tail below the soft cap without throttling
    /// ingest.
    async fn drain_backlog_elapsed(flush_concurrency: usize, delay: Duration) -> (Duration, usize) {
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(LatencyPutStore::new(storage_inner, delay)),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let shard = crate::Shard::new(ShardConfig::new(storage.clone(), catalog, wal));
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();
        let handle = shard.get(&project, &partition).await.unwrap();

        // 6 * 512k rows => 6 bounded output files, drained in one wave at
        // flush_concurrency>=6.
        let per = MAX_COMPACTION_ROWS;
        let n = 6usize;
        for i in 0..n {
            handle
                .write_batch(&table, batch((i * per) as i64, per, "v-"))
                .await
                .unwrap();
        }

        std::env::set_var("BASIN_SHARD_FLUSH_CONCURRENCY", flush_concurrency.to_string());
        let inner = impl_of(&shard);
        let started = Instant::now();
        inner.run_compaction_once().await.unwrap();
        let elapsed = started.elapsed();
        std::env::remove_var("BASIN_SHARD_FLUSH_CONCURRENCY");

        // (a) tail drained.
        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(project, partition.clone())).unwrap().clone()
        };
        assert!(
            state.read().await.tail_is_empty(),
            "tail must be fully drained after one compaction call"
        );
        let files = storage.list_data_files(&project, &table).await.unwrap();
        // (d) correctness.
        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&read), n * per, "all rows must survive compaction");
        (elapsed, files.len())
    }

    #[tokio::test]
    async fn parallel_compaction_keeps_tail_bounded() {
        basin_common::telemetry::try_init_for_tests();
        // High per-PUT delay so the latency term dominates the (identical)
        // encode cost in both runs.
        let delay = Duration::from_millis(250);

        let (serial, serial_files) = drain_backlog_elapsed(1, delay).await;
        let (concurrent, conc_files) = drain_backlog_elapsed(6, delay).await;

        // (b) the backlog really was several files (concurrency path exercised).
        assert!(
            serial_files >= 2 && conc_files >= 2,
            "expected several bounded files (serial={serial_files}, concurrent={conc_files})"
        );

        // (c) concurrent overlapped the PUTs. Serial pays ~6*250ms = 1.5s of
        // pure PUT latency that the concurrent run overlaps into ~250ms; the
        // shared (constant) encode cost cancels out. Require the concurrent run
        // to beat serial by at least ~3 PUT-latencies — well above timing
        // jitter, conservatively below the ~5-latency ideal.
        let savings = delay.mul_f64(3.0);
        assert!(
            concurrent + savings < serial,
            "concurrent drain ({concurrent:?}) did not overlap PUTs vs serial ({serial:?}); \
             expected at least {savings:?} faster",
        );
    }

    /// WAL GC bounds on-disk WAL footprint AND preserves crash recovery.
    ///
    /// Sequence: write+compact several waves (each advances the watermark and
    /// truncates the now-durable WAL segments), measuring the WAL `.seg`
    /// footprint after each. The footprint must NOT grow monotonically with
    /// total rows — that is the "WAL fills the volume" bug. Then we write a
    /// final un-compacted wave (still only in the WAL), simulate a crash by
    /// dropping the shard, and reopen a NEW shard over the SAME catalog + WAL +
    /// storage. Recovery must reconstruct the FULL row count: WAL GC must not
    /// have dropped any segment still needed to replay the un-flushed tail.
    #[tokio::test]
    async fn wal_gc_bounds_space_and_preserves_recovery() {
        basin_common::telemetry::try_init_for_tests();

        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: storage_fs,
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        // The catalog is the durable surface that survives a crash (it carries
        // the compaction watermark). Reuse the SAME Arc across the crash so the
        // post-crash replay floor is the real persisted watermark.
        let catalog = Arc::new(InMemoryCatalog::new());

        let open_wal = || {
            let dir = wal_dir.path().to_path_buf();
            async move {
                let w: Arc<dyn Wal> = Arc::new(
                    LocalWal::open(WalConfig {
                        object_store: Arc::new(
                            LocalFileSystem::new_with_prefix(dir).unwrap(),
                        ),
                        root_prefix: None,
                        flush_interval: Duration::from_millis(20),
                        flush_max_bytes: 256 * 1024,
                        commit_delay: Duration::from_millis(2),
                    })
                    .await
                    .unwrap(),
                );
                w
            }
        };

        let project = ProjectId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let per = 50_000usize;
        let mut total_committed = 0usize;
        let mut footprints: Vec<u64> = Vec::new();

        {
            let wal = open_wal().await;
            let shard =
                crate::Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
            let handle = shard.get(&project, &partition).await.unwrap();
            let inner = impl_of(&shard);

            // Several write+compact waves. After each compaction the WAL
            // segments below the watermark are deleted, so the footprint should
            // stay bounded rather than grow with total rows.
            for wave in 0..5 {
                for j in 0..4 {
                    handle
                        .write_batch(
                            &table,
                            batch(((wave * 4 + j) * per) as i64, per, "v-"),
                        )
                        .await
                        .unwrap();
                    total_committed += per;
                }
                wal.flush().await.unwrap();
                inner.run_compaction_once().await.unwrap();
                footprints.push(wal_dir_seg_bytes(wal_dir.path()));
            }

            // Final un-compacted wave: lands in the WAL only (durable), never
            // compacted before the "crash". WAL GC must NOT touch these
            // segments (their LSNs are above the watermark).
            for j in 0..4 {
                handle
                    .write_batch_opts(
                        &table,
                        batch(((20 + j) * per) as i64, per, "tail-"),
                        true, // durable: on the store before ack
                    )
                    .await
                    .unwrap();
                total_committed += per;
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
            // Drop the shard -> simulate crash (in-memory tail lost; only the
            // catalog watermark + object-store files + WAL survive).
        }

        // (a) WAL footprint is bounded: the last steady-state wave's footprint
        // must not be a large multiple of the first — segments were reclaimed.
        // (Compacted waves leave only the most recent, not-yet-truncated tail.)
        let max_steady = *footprints.iter().max().unwrap();
        let min_steady = *footprints.iter().min().unwrap();
        assert!(
            max_steady <= min_steady.saturating_mul(3) + 1024 * 1024,
            "WAL footprint grew unbounded across waves: {footprints:?}"
        );

        // (b) Crash recovery: reopen over the SAME catalog + WAL dir + storage.
        // The reconstructed row count must be exact — WAL GC must not have
        // dropped any segment still needed for the un-flushed tail.
        let wal2 = open_wal().await;
        let shard2 = crate::Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal2));
        let handle2 = shard2.get(&project, &partition).await.unwrap();
        let read = handle2.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(
            rows_in(&read),
            total_committed,
            "recovery must reconstruct every committed row (no segment dropped early)"
        );
    }

    /// `ObjectStore` wrapper that models a crash that drops the un-flushed
    /// RAM buffer of SELECTED WAL partitions. A PUT whose path contains any
    /// configured "blocked" partition segment (e.g. `/s1/`) returns success
    /// WITHOUT writing the segment — exactly what happens to an
    /// ack-before-durable batch that was still in RAM (never PUT) when the
    /// node crashed. Unblocked partitions PUT normally (their segments are
    /// durable and survive the crash). All non-PUT ops delegate untouched.
    #[derive(Debug)]
    struct PartialFlushStore {
        inner: Arc<dyn object_store::ObjectStore>,
        /// Path-substrings whose PUTs are silently dropped (the partition's
        /// un-flushed tail is "lost in the crash").
        blocked: Vec<String>,
    }

    impl std::fmt::Display for PartialFlushStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PartialFlushStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for PartialFlushStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let loc = location.as_ref();
            if self.blocked.iter().any(|b| loc.contains(b.as_str())) {
                // Drop the segment: report success (the WAL ack'd it from RAM
                // already) but never persist it — the crash window.
                return Ok(object_store::PutResult {
                    e_tag: None,
                    version: None,
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Largest `id` value present across the recovered batches, and the count
    /// of distinct ids. A contiguous prefix `1..=max` has `count == max`.
    fn max_and_count(batches: &[RecordBatch]) -> (i64, usize) {
        let mut max = 0i64;
        let mut count = 0usize;
        for b in batches {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id col is Int64");
            for i in 0..ids.len() {
                let v = ids.value(i);
                if v > max {
                    max = v;
                }
                count += 1;
            }
        }
        (max, count)
    }

    /// Shared driver for the fan-out crash scenarios. Drives a multi-batch,
    /// multi-partition bulk-ingest exactly as `executor::write_batch_fanout`
    /// does — round-robin whole 10k-row batches across 4 stripe partitions,
    /// async (`synchronous_commit = off`). It returns the WAL it wrote to (so
    /// the caller can run the durable barrier before the crash), the storage +
    /// catalog + wal dir to reopen against, and the acked row count.
    ///
    /// The WAL is opened over a `PartialFlushStore` that DROPS the PUTs of
    /// partitions `s1` and `s3`: those partitions' segments never reach the
    /// store, modelling the un-flushed RAM buffers that a node crash loses.
    /// `s0`/`s2` PUT normally and survive. `flush_interval`/`flush_max_bytes`
    /// are set huge so nothing auto-flushes — the only durability point is the
    /// explicit barrier / final `flush()`.
    struct FanoutCrashFixture {
        storage: Storage,
        catalog: Arc<InMemoryCatalog>,
        wal_dir: TempDir,
        _storage_dir: TempDir,
        wal: Arc<dyn Wal>,
        project: ProjectId,
        table: TableName,
        parts: Vec<PartitionKey>,
        acked: usize,
    }

    async fn write_fanout_copy() -> FanoutCrashFixture {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: storage_fs,
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());

        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        // Four fan-out stripe partitions, matching executor::stripe_partition_key.
        let parts: Vec<PartitionKey> = vec![
            PartitionKey::default_key(),
            PartitionKey::new("s1".to_string()).unwrap(),
            PartitionKey::new("s2".to_string()).unwrap(),
            PartitionKey::new("s3".to_string()).unwrap(),
        ];
        let fanout = parts.len();

        // 16 batches of 10k rows = 160k rows, ids 1..=160_000, round-robined.
        let per = 10_000usize;
        let n_batches = 16usize;

        let crashy: Arc<dyn object_store::ObjectStore> = Arc::new(PartialFlushStore {
            inner: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            blocked: vec!["/s1/".to_string(), "/s3/".to_string()],
        });
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: crashy,
                root_prefix: None,
                flush_interval: Duration::from_secs(3600), // never auto-flush
                flush_max_bytes: 1024 * 1024 * 1024,        // never pressure-flush
                commit_delay: Duration::from_millis(1),
            })
            .await
            .unwrap(),
        );
        let shard =
            crate::Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));

        let mut acked = 0usize;
        for i in 0..n_batches {
            let part = &parts[i % fanout];
            let handle = shard.get(&project, part).await.unwrap();
            let start = (i * per) as i64 + 1; // ids 1..=160_000
            handle
                .write_batch_opts(&table, batch(start, per, "v-"), false)
                .await
                .unwrap();
            acked += per;
        }

        FanoutCrashFixture {
            storage,
            catalog,
            wal_dir,
            _storage_dir: storage_dir,
            wal,
            project,
            table,
            parts,
            acked,
        }
    }

    /// Reopen over the SAME wal dir with a CLEAN store (only the segments that
    /// actually reached the store survive), replay every partition, and return
    /// `(max_id, recovered_count)`.
    async fn recover_fanout(fx: &FanoutCrashFixture) -> (i64, usize) {
        let wal2: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(
                    LocalFileSystem::new_with_prefix(fx.wal_dir.path()).unwrap(),
                ),
                root_prefix: None,
                flush_interval: Duration::from_millis(50),
                flush_max_bytes: 1024 * 1024,
                commit_delay: Duration::from_millis(2),
            })
            .await
            .unwrap(),
        );
        let shard2 =
            crate::Shard::new(ShardConfig::new(fx.storage.clone(), fx.catalog.clone(), wal2));
        let mut recovered: Vec<RecordBatch> = Vec::new();
        for part in &fx.parts {
            let h = shard2.get(&fx.project, part).await.unwrap();
            recovered.extend(h.read(&fx.table, ReadOptions::default()).await.unwrap());
        }
        max_and_count(&recovered)
    }

    /// STAGE 0 characterization of the bug: WITHOUT the durable barrier the
    /// COPY is ACKed while two fan-out partitions' batches are still un-flushed
    /// in RAM. A crash drops them, leaving HOLES below max(id); a `max(id)+1`
    /// resumer silently skips the lost rows. This test PINS that loss: it
    /// proves the recovered set is a non-contiguous prefix (count < max), i.e.
    /// the ACK was a lie. The `with_barrier` test below proves the barrier
    /// closes exactly this hole.
    ///
    /// (Run with the no-barrier assertion flipped to `count == max` to see the
    /// raw repro failure: "recovered N rows but max(id)=160000".)
    #[tokio::test]
    async fn fanout_copy_crash_without_barrier_loses_rows() {
        let fx = write_fanout_copy().await;
        let acked = fx.acked;
        // Crash boundary: a flush drops s1/s3 PUTs; s0/s2 persist.
        let _ = fx.wal.flush().await;
        fx.wal.close().await.unwrap();

        let (max, count) = recover_fanout(&fx).await;
        // The hole is real: two of four partitions' rows vanished, but max(id)
        // (which lives in a surviving partition) did not — so a max(id)-based
        // resumer would skip the lost rows entirely.
        assert!(
            (count as i64) < max,
            "expected silent row loss without the barrier: recovered {count} rows \
             with max(id)={max} (acked {acked}) — a hole below max(id)"
        );
        assert!(
            count < acked,
            "without the barrier the ACKed count {acked} overstates the durable rows {count}"
        );
    }

    /// STAGE 2: WITH the end-of-COPY durable barrier the ACK is honest — the
    /// COPY does not return "COPY n" until every touched partition is durable
    /// through its last LSN (the same `Wal::await_durable` the router's
    /// `on_copy_done` calls). A crash AFTER a successful barrier cannot lose
    /// acked rows: recovery is a contiguous prefix AND the count equals the
    /// acked count EXACTLY (no loss, no duplicate regression).
    ///
    /// Like the Stage 0 driver but over a CLEAN store (no dropped PUTs): the
    /// `with_barrier` scenario writes through a store where every partition
    /// CAN reach durability, so the barrier completes rather than hanging.
    /// Returns the fixture and the acked count.
    async fn write_fanout_copy_clean() -> FanoutCrashFixture {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: storage_fs,
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let parts: Vec<PartitionKey> = vec![
            PartitionKey::default_key(),
            PartitionKey::new("s1".to_string()).unwrap(),
            PartitionKey::new("s2".to_string()).unwrap(),
            PartitionKey::new("s3".to_string()).unwrap(),
        ];
        let fanout = parts.len();
        let per = 10_000usize;
        let n_batches = 16usize;

        // Clean store, but huge flush thresholds so the ONLY durability path is
        // the explicit end-of-COPY barrier (proves the barrier — not an
        // incidental background flush — is what makes the rows durable).
        let wal: Arc<dyn Wal> = Arc::new(
            LocalWal::open(WalConfig {
                object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
                root_prefix: None,
                flush_interval: Duration::from_secs(3600),
                flush_max_bytes: 1024 * 1024 * 1024,
                commit_delay: Duration::from_millis(1),
            })
            .await
            .unwrap(),
        );
        let shard =
            crate::Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
        let mut acked = 0usize;
        for i in 0..n_batches {
            let part = &parts[i % fanout];
            let h = shard.get(&project, part).await.unwrap();
            let start = (i * per) as i64 + 1;
            h.write_batch_opts(&table, batch(start, per, "v-"), false)
                .await
                .unwrap();
            acked += per;
        }
        FanoutCrashFixture {
            storage,
            catalog,
            wal_dir,
            _storage_dir: storage_dir,
            wal,
            project,
            table,
            parts,
            acked,
        }
    }

    /// STAGE 2: WITH the end-of-COPY durable barrier the ACK is honest. After
    /// the async fan-out appends, the barrier awaits durability of every
    /// touched partition through its high-water LSN (the same `await_durable`
    /// the router's `on_copy_done` calls) BEFORE the crash. NO explicit flush
    /// runs — the barrier is the only durability point. A crash after a
    /// successful barrier loses nothing: recovery is a contiguous prefix AND
    /// the count equals the acked count EXACTLY (no loss, no duplicate).
    #[tokio::test]
    async fn fanout_copy_crash_with_barrier_preserves_all_rows() {
        let fx = write_fanout_copy_clean().await;
        // The barrier: await durability of every touched partition.
        for part in &fx.parts {
            let hw = fx.wal.high_water(&fx.project, part).await.unwrap();
            fx.wal.await_durable(&fx.project, part, hw).await.unwrap();
        }
        // Crash boundary: close WITHOUT flush — durability was the barrier's job.
        fx.wal.close().await.unwrap();

        let (max, count) = recover_fanout(&fx).await;
        assert_eq!(
            count as i64, max,
            "with the barrier recovery must be a contiguous prefix (count {count} == max {max})"
        );
        assert_eq!(
            count, fx.acked,
            "recovered count must equal acked count EXACTLY — no loss and no duplicate"
        );
    }
}
