//! File-backed (single-node) WAL implementation.
//!
//! ## Storage layout
//!
//! ```text
//! {root_prefix}/wal/{project_id}/{partition_key}/
//!     {ulid}.seg              # closed segment, immutable
//! ```
//!
//! Each `.seg` is a sequence of length-prefixed bincode records. The first
//! record is a [`SegmentHeader`]; subsequent records are payload entries. See
//! [`crate::segment`] for the wire format.
//!
//! ## Durability contract
//!
//! Two append modes share the same buffer, LSN space, and flusher:
//!
//! **Async** (`append` / `append_fenced`, the `synchronous_commit = off`
//! default): returns once the entry is in the in-RAM buffer for its
//! `(project, partition)` — ack-before-durable. The buffer is uploaded to
//! the configured [`ObjectStore`] either:
//!
//! 1. when the background flush task wakes (every `flush_interval`,
//!    default 200 ms), or
//! 2. when the buffer's accumulated size exceeds `flush_max_bytes`
//!    (default 1 MiB), or
//! 3. on an explicit [`crate::Wal::flush`] call.
//!
//! The crash-loss window for acked-but-unflushed async appends is therefore
//! `min(flush_interval, time-to-flush_max_bytes)`.
//!
//! **Synchronous** (`append_fenced_durable`, `synchronous_commit = on`):
//! buffers the entry exactly like the async path, nudges the flusher, and
//! only returns once the partition's published `durable_lsn` covers the
//! entry — ack-after-durable. The flusher coalesces every synchronous
//! append that lands within `commit_delay` (default 2 ms) into one segment,
//! so N concurrent synchronous appends cost one PUT (+ one fsync): group
//! commit.
//!
//! Backend durability nuance: an S3-style PUT is durable on success, but
//! [`object_store::local::LocalFileSystem`]'s PUT is write-temp + rename
//! with **no fsync** — it survives a process crash, not necessarily an OS
//! crash or power loss. When a flushed segment carries at least one
//! synchronous waiter, the flusher marks the PUT with [`crate::DurablePut`]
//! so a [`crate::FsyncOnPut`]-wrapped local store fsyncs the segment file
//! and its parent directory before the waiters are released. Segments with
//! no synchronous waiters are never fsync'd — the async path's cost profile
//! is unchanged. v0.2 (Raft) replaces all of this with quorum-ack.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, ProjectId, Result};
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutOptions, PutPayload};
use tokio::sync::{watch, Mutex, Notify, RwLock};
use ulid::Ulid;

use crate::fsync::DurablePut;
use crate::segment::{
    decode_segment, decode_segment_full, entry_record, frame_into, handoff_record,
    tx_begin_record, tx_commit_record, tx_rollback_record, DecodedRecord, EntryRecord,
    SegmentHeader, SegmentRecord, FORMAT_VERSION,
};
use crate::state::{BufferRecord, ClosedSegment, PartitionState};
use crate::{Lsn, WalConfig, WalEntry, WalEvent, WalImpl};

/// Top-level segment of the WAL key namespace. Lives directly under
/// `{root_prefix}` (or the bucket root) so listing can scope a recovery scan
/// to just the WAL.
const WAL_SEGMENT: &str = "wal";

type PartitionMap = HashMap<(ProjectId, PartitionKey), Arc<Mutex<PartitionState>>>;

/// Shared state between the public `FileWal` handle and the background flush
/// task. Holding it behind one `Arc` avoids the awkward "split a partitions
/// map across two owners" dance.
struct Inner {
    object_store: Arc<dyn ObjectStore>,
    root_prefix: Option<ObjectPath>,
    flush_max_bytes: u64,
    /// Nudged by synchronous (`append_fenced_durable`) appends so the flush
    /// loop can group-commit after `commit_delay` instead of waiting out the
    /// full `flush_interval` tick.
    flush_notify: Notify,
    /// Outer lock is brief — only held for hashmap lookup/insert. Per-partition
    /// work serialises on the inner mutex so two partitions can flush in
    /// parallel.
    partitions: RwLock<PartitionMap>,
}

pub(crate) struct FileWal {
    inner: Arc<Inner>,
    /// Signal to the background flush loop. `true` means shut down.
    shutdown_tx: watch::Sender<bool>,
    /// JoinHandle for the flush task. Held in a mutex so [`WalImpl::close`]
    /// can take it (idempotently).
    flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FileWal {
    pub(crate) async fn open(cfg: WalConfig) -> Result<Self> {
        let WalConfig {
            object_store,
            root_prefix,
            flush_interval,
            flush_max_bytes,
            commit_delay,
        } = cfg;

        let partitions = recover_partitions(&object_store, root_prefix.as_ref()).await?;

        let inner = Arc::new(Inner {
            object_store,
            root_prefix,
            flush_max_bytes,
            flush_notify: Notify::new(),
            partitions: RwLock::new(partitions),
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_handle =
            spawn_flush_loop(inner.clone(), flush_interval, commit_delay, shutdown_rx);

        Ok(Self {
            inner,
            shutdown_tx,
            flush_task: Mutex::new(Some(flush_handle)),
        })
    }

    /// Shared core of `append_fenced` (async ack) and
    /// `append_fenced_durable` (synchronous-commit ack). Both buffer the
    /// entry identically; `durable` additionally registers a group-commit
    /// waiter and blocks until the partition's published `durable_lsn`
    /// covers the new entry.
    async fn append_entry(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
        durable: bool,
    ) -> Result<Lsn> {
        // Pre-compute the timestamp and build the EntryRecord *before*
        // acquiring the per-partition mutex. The timestamp is a syscall
        // (clock_gettime); the record itself is copy-free — the payload is a
        // refcounted `Bytes` that is moved, not memcopied. The only payload
        // copy left on the write path is the single serialize into the
        // segment buffer at flush time, which also runs outside this mutex.
        //
        // We construct the record with a placeholder LSN (Lsn(0)); the real LSN
        // is stamped inside the lock where it is guaranteed monotonic.
        let now = Utc::now();
        // Approximate framed size: 4-byte length prefix + bincode body ≈ payload
        // + ~64 bytes for project/partition/lsn/timestamp. Used only for the
        // buffer-pressure heuristic; the real size is computed at flush time.
        let approx = (payload.len() as u64) + 96;
        let mut record = entry_record(project, partition, Lsn(0), payload, now);

        let state = self.inner.get_or_create_partition(project, partition).await;
        let (lsn, should_flush, durable_rx) = {
            let mut guard = state.lock().await;
            // Epoch fence (ADR 0023). `None` / `Some(0)` is the no-lease
            // back-compat path: it never raises the fence and is always
            // accepted. A real lease epoch must be >= the highest epoch this
            // partition has seen; a strictly lower epoch is a stale
            // dual-leaseholder write and is rejected before consuming an LSN.
            if let Some(e) = epoch {
                if e != 0 {
                    if e < guard.fence_epoch {
                        return Err(BasinError::wal(format!(
                            "stale lease epoch {e} for {project}/{partition}: \
                             fence is at {} (dual-leaseholder write rejected)",
                            guard.fence_epoch
                        )));
                    }
                    // Monotonically raise the fence to the winning epoch.
                    guard.fence_epoch = e;
                }
            }
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            // Stamp the real LSN — this is the only field that must be assigned
            // under the lock to preserve the monotonic guarantee.
            record.lsn = lsn;
            guard.buffer.push(BufferRecord::Entry(record));
            guard.buffer_bytes += approx;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            // Synchronous commit: raise the fsync watermark and subscribe to
            // durable-LSN publications under the same lock that assigned the
            // LSN, so the flusher's needs_fsync decision can never miss us.
            let rx = if durable {
                if lsn > guard.sync_requested_up_to {
                    guard.sync_requested_up_to = lsn;
                }
                Some(guard.durable_tx.subscribe())
            } else {
                None
            };
            (lsn, pressure, rx)
        };
        if should_flush {
            // Buffer-pressure flush errors are logged but not surfaced: the
            // entries remain in RAM and the next tick or explicit flush will
            // retry. Tests and graceful shutdown call `flush()` to surface
            // errors.
            if let Err(e) = self.inner.flush_one(&state).await {
                tracing::warn!(error = %e, "buffer-pressure flush failed");
            }
        }
        if let Some(mut rx) = durable_rx {
            // Nudge the flush loop (group-commit window starts now), then
            // wait until the published durable watermark covers our LSN.
            // One segment PUT (+fsync) wakes every waiter it covers. If a
            // flush attempt fails, the buffer is restored and the next tick
            // retries — we keep waiting: "durable" means durable.
            self.inner.flush_notify.notify_one();
            rx.wait_for(|durable| *durable >= lsn).await.map_err(|_| {
                BasinError::wal("wal closed while a synchronous append awaited durability")
            })?;
        }
        Ok(lsn)
    }
}

impl Inner {
    async fn get_or_create_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Arc<Mutex<PartitionState>> {
        let key = (*project, partition.clone());
        {
            let map = self.partitions.read().await;
            if let Some(p) = map.get(&key) {
                return p.clone();
            }
        }
        let mut map = self.partitions.write().await;
        map.entry(key)
            .or_insert_with(|| {
                Arc::new(Mutex::new(PartitionState::new(*project, partition.clone())))
            })
            .clone()
    }

    /// Flush every partition that has buffered entries.
    async fn flush_all(&self) -> Result<()> {
        let states: Vec<Arc<Mutex<PartitionState>>> = {
            let map = self.partitions.read().await;
            map.values().cloned().collect()
        };
        for state in states {
            self.flush_one(&state).await?;
        }
        Ok(())
    }

    /// Drain one partition's in-RAM buffer to a new closed segment in object
    /// storage. On error (framing or PUT) the buffered entries are restored
    /// to the front of the buffer so the next flush attempt picks them up.
    /// No data loss as long as the process is alive.
    ///
    /// Locking: the partition mutex is held only to swap the buffer out —
    /// cheap pointer moves, since entry payloads are refcounted `Bytes`.
    /// Framing (the bincode serialization of every record, including the one
    /// unavoidable payload copy into the segment buffer) and the network/disk
    /// PUT both run with the lock released, so concurrent appenders to this
    /// partition never serialise behind a flush. The swapped-out batch keeps
    /// its already-assigned LSNs; appends that land during the flush get
    /// strictly higher LSNs, so restoring the batch to the *front* of the
    /// buffer on failure preserves LSN order.
    async fn flush_one(&self, state: &Arc<Mutex<PartitionState>>) -> Result<()> {
        let (header, drained, drained_bytes, needs_fsync) = {
            let mut guard = state.lock().await;
            if guard.buffer.is_empty() {
                return Ok(());
            }
            let header = SegmentHeader {
                format_version: FORMAT_VERSION,
                project: guard.project,
                partition: guard.partition.clone(),
                first_lsn: guard.buffer.first().expect("non-empty").lsn(),
                segment_id: Ulid::new(),
            };
            let drained: Vec<BufferRecord> = std::mem::take(&mut guard.buffer);
            let drained_bytes = guard.buffer_bytes;
            guard.buffer_bytes = 0;
            // A synchronous-commit waiter's LSN can only be inside this
            // segment if it is >= the segment's first LSN (every requested
            // LSN below first_lsn was drained — and published durable — by
            // an earlier flush). Computed under the same lock that assigns
            // LSNs and raises `sync_requested_up_to`, so no waiter can slip
            // between the drain and this decision.
            let needs_fsync = guard.sync_requested_up_to >= header.first_lsn;
            (header, drained, drained_bytes, needs_fsync)
        };
        // Lock released. While the flush is in flight the drained records are
        // visible neither in `buffer` nor in `closed` — identical to the
        // pre-restructure behaviour, which also drained before dropping the
        // guard for the PUT.
        let first_lsn = header.first_lsn;
        let last_lsn = drained.last().expect("non-empty").lsn();
        let segment_id = header.segment_id;
        let path = closed_segment_path(
            self.root_prefix.as_ref(),
            &header.project,
            &header.partition,
            segment_id,
        );

        let framed: Result<Vec<u8>> = (|| {
            let mut buf: Vec<u8> = Vec::with_capacity(drained_bytes as usize + 256);
            frame_into(&mut buf, &SegmentRecord::Header(header))?;
            for record in &drained {
                match record {
                    BufferRecord::Entry(entry) => {
                        // Cheap clone: refcount bump on the `Bytes` payload
                        // plus small fixed-size fields — no payload memcpy.
                        frame_into(&mut buf, &SegmentRecord::Entry(entry.clone()))?;
                    }
                    BufferRecord::TxBegin { tx_id, .. } => {
                        frame_into(&mut buf, &tx_begin_record(*tx_id))?;
                    }
                    BufferRecord::TxRollback { tx_id, .. } => {
                        frame_into(&mut buf, &tx_rollback_record(*tx_id))?;
                    }
                    BufferRecord::TxCommit { tx_id, .. } => {
                        frame_into(&mut buf, &tx_commit_record(*tx_id))?;
                    }
                    BufferRecord::Handoff {
                        to_holder,
                        at_epoch,
                        ..
                    } => {
                        frame_into(&mut buf, &handoff_record(to_holder.clone(), *at_epoch))?;
                    }
                }
            }
            Ok(buf)
        })();
        let buf = match framed {
            Ok(buf) => buf,
            Err(e) => {
                // Framing failed (encode error / record over 4 GiB). Restore
                // the drained records and surface the error — same contract
                // as before the restructure, where framing ran before the
                // drain and a failure left the entries buffered.
                restore_buffer(state, drained, drained_bytes).await;
                return Err(e);
            }
        };

        // Segments that carry at least one synchronous-commit waiter are
        // marked with `DurablePut` so an `FsyncOnPut`-wrapped local store
        // fsyncs the segment file (+ parent dir) before returning. Stores
        // that are durable on PUT (S3) or unwrapped ignore the extension —
        // `object_store` backends ignore extensions by contract.
        let mut put_opts = PutOptions::default();
        if needs_fsync {
            put_opts.extensions.insert(DurablePut);
        }
        let put_result = self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(buf)), put_opts)
            .await;
        match put_result {
            Ok(_) => {
                let mut guard = state.lock().await;
                guard.closed.insert(
                    first_lsn,
                    ClosedSegment {
                        path: path.clone(),
                        first_lsn,
                        last_lsn,
                        segment_id,
                    },
                );
                // Publish durable progress (group commit): one completed PUT
                // wakes every synchronous waiter whose LSN it covers.
                // Contiguity-gated so an out-of-order completion of a higher
                // segment can't ack waiters in a lower, still-in-flight one.
                guard.mark_range_durable(first_lsn, last_lsn);
                tracing::debug!(
                    project = %guard.project,
                    partition = %guard.partition,
                    %first_lsn,
                    %last_lsn,
                    records = drained.len(),
                    fsynced = needs_fsync,
                    "wal segment flushed",
                );
                Ok(())
            }
            Err(e) => {
                restore_buffer(state, drained, drained_bytes).await;
                Err(BasinError::storage(format!("wal segment put {path}: {e}")))
            }
        }
    }
}

/// Restore drained-but-unflushed records to the *front* of the partition
/// buffer after a failed flush (framing or PUT error). Any records appended
/// while the flush was in flight carry strictly higher LSNs, so prepending
/// preserves LSN order. The byte accounting is added back so buffer-pressure
/// flushes keep firing.
async fn restore_buffer(
    state: &Arc<Mutex<PartitionState>>,
    drained: Vec<BufferRecord>,
    drained_bytes: u64,
) {
    let mut guard = state.lock().await;
    let mut combined = drained;
    combined.append(&mut guard.buffer);
    guard.buffer = combined;
    guard.buffer_bytes += drained_bytes;
}

/// Background flush loop. Wakes on tick, on a synchronous-commit nudge
/// (`Inner::flush_notify`), or on shutdown; flushes every partition that has
/// buffered entries. Errors are logged but don't kill the loop — the next
/// tick retries (synchronous waiters keep waiting until a retry succeeds),
/// and explicit `flush()` callers see errors directly.
///
/// On a nudge the loop sleeps `commit_delay` before draining so concurrent
/// synchronous appends coalesce into one segment PUT — group commit.
/// `Notify` stores a permit when nobody is parked on `notified()`, so a
/// nudge that races an in-progress flush is never lost; it just triggers one
/// extra (possibly empty, hence free) flush pass.
fn spawn_flush_loop(
    inner: Arc<Inner>,
    flush_interval: Duration,
    commit_delay: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Burn the immediate first tick so we don't flush a freshly-opened
        // WAL before the caller has a chance to append.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = inner.flush_all().await {
                        tracing::warn!(error = %e, "wal background flush failed");
                    }
                }
                _ = inner.flush_notify.notified() => {
                    // Group-commit window: let concurrent synchronous
                    // appends pile into the buffer before draining once.
                    if commit_delay > Duration::ZERO {
                        tokio::time::sleep(commit_delay).await;
                    }
                    if let Err(e) = inner.flush_all().await {
                        tracing::warn!(error = %e, "wal group-commit flush failed");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        if let Err(e) = inner.flush_all().await {
                            tracing::warn!(error = %e, "wal shutdown flush failed");
                        }
                        break;
                    }
                }
            }
        }
    })
}

/// Build `{root}/wal/{project}/{partition}/{ulid}.seg`.
fn closed_segment_path(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    partition: &PartitionKey,
    segment_id: Ulid,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(WAL_SEGMENT);
    p = p.child(project.as_prefix());
    let part = if partition.as_str().is_empty() {
        PartitionKey::DEFAULT
    } else {
        partition.as_str()
    };
    // Mirror storage's path layout: a `/` inside a PartitionKey is a path
    // separator, so we expand each segment into its own directory level.
    for segment in part.split('/') {
        p = p.child(segment);
    }
    p.child(format!("{segment_id}.seg"))
}

/// Top-level WAL prefix, used by listing during recovery.
fn wal_root_prefix(root: Option<&ObjectPath>) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(WAL_SEGMENT);
    p
}

/// Walk every segment under the WAL prefix on open, parse its header + last
/// LSN, and rebuild the per-partition state. We read each segment in full to
/// pick up the last LSN; segments are small (1 MB by default) and recovery
/// runs once per process so the read cost is fine.
async fn recover_partitions(
    object_store: &Arc<dyn ObjectStore>,
    root_prefix: Option<&ObjectPath>,
) -> Result<PartitionMap> {
    let prefix = wal_root_prefix(root_prefix);
    let mut stream = object_store.list(Some(&prefix));
    let mut closed: HashMap<(ProjectId, PartitionKey), Vec<ClosedSegment>> = HashMap::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| BasinError::storage(format!("wal recovery list: {e}")))?;
        if !meta.location.as_ref().ends_with(".seg") {
            continue;
        }
        let body = object_store
            .get(&meta.location)
            .await
            .map_err(|e| BasinError::storage(format!("wal recovery get {}: {e}", meta.location)))?
            .bytes()
            .await
            .map_err(|e| {
                BasinError::storage(format!("wal recovery body {}: {e}", meta.location))
            })?;
        // RECOVERY ROBUSTNESS: an abrupt kill (OOM, power loss, SIGKILL after a
        // drain that couldn't flush in time) can leave the segment that was
        // being written with no header yet ("segment: missing header") or a
        // partial trailing frame ("truncated …"). That segment holds only
        // un-flushed, ack-before-durable entries — never committed/durable state
        // (durable state lives in the object-store catalog). Skipping it with a
        // loud warning lets the engine BOOT and recover every complete segment,
        // instead of crash-looping forever on an unreadable trailing segment. A
        // client resuming from the authoritative count(*) re-ingests any lost
        // tail, so this preserves exactly-once. (Closed/complete segments must
        // still decode cleanly — a genuine error there is surfaced via the warn,
        // not silently masked; the engine simply does not replay that segment.)
        let (header, entries) = match decode_segment(&body) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    segment = %meta.location,
                    error = %e,
                    "WAL recovery: skipping unreadable segment (interrupted write / partial \
                     trailing frame). Its un-flushed entries are not replayed; durable state is \
                     in the catalog and a resuming client re-ingests any lost tail."
                );
                continue;
            }
        };
        let last_lsn = entries.last().map(|e| e.lsn).unwrap_or(header.first_lsn);
        let key = (header.project, header.partition.clone());
        closed.entry(key).or_default().push(ClosedSegment {
            path: meta.location,
            first_lsn: header.first_lsn,
            last_lsn,
            segment_id: header.segment_id,
        });
    }

    let mut out: PartitionMap = HashMap::new();
    for ((project, partition), mut segs) in closed {
        segs.sort_by_key(|s| s.first_lsn);
        let next_lsn = segs.last().map(|s| Lsn(s.last_lsn.0 + 1)).unwrap_or(Lsn(1));
        let mut state = PartitionState::new(project, partition.clone());
        state.next_lsn = next_lsn;
        // Everything in a closed segment is durable by definition (it was
        // read back from the store). Seed the durable watermark so a
        // synchronous append issued right after recovery doesn't wait on
        // LSNs that were already persisted by the previous process.
        state.durable_lsn = Lsn(next_lsn.0 - 1);
        state.durable_tx.send_replace(state.durable_lsn);
        for s in segs {
            state.closed.insert(s.first_lsn, s);
        }
        out.insert((project, partition), Arc::new(Mutex::new(state)));
    }
    Ok(out)
}

#[async_trait]
impl WalImpl for FileWal {
    #[tracing::instrument(skip(self, payload), fields(project=%project, partition=%partition, bytes=payload.len()))]
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        // Unfenced append == no-lease mode (epoch None): never raises the
        // fence, always accepted. Identical behaviour to pre-6.X.A.
        self.append_fenced(project, partition, payload, None).await
    }

    #[tracing::instrument(skip(self, payload), fields(project=%project, partition=%partition, bytes=payload.len(), ?epoch))]
    async fn append_fenced(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
    ) -> Result<Lsn> {
        self.append_entry(project, partition, payload, epoch, false)
            .await
    }

    #[tracing::instrument(skip(self, payload), fields(project=%project, partition=%partition, bytes=payload.len(), ?epoch))]
    async fn append_fenced_durable(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
        epoch: Option<i64>,
    ) -> Result<Lsn> {
        self.append_entry(project, partition, payload, epoch, true)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn flush(&self) -> Result<()> {
        self.inner.flush_all().await
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, %since_lsn))]
    async fn read_from(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        // Snapshot the closed-segment list and the in-RAM buffer under the
        // lock; do the (potentially slow) GETs without holding it.
        let (closed_paths, buffered): (Vec<ClosedSegment>, Vec<BufferRecord>) = {
            let guard = state.lock().await;
            let segs: Vec<ClosedSegment> = guard
                .closed
                .values()
                .filter(|s| s.last_lsn > since_lsn)
                .cloned()
                .collect();
            let buf: Vec<BufferRecord> = guard
                .buffer
                .iter()
                .filter(|r| r.lsn() > since_lsn)
                .cloned()
                .collect();
            (segs, buf)
        };

        let mut out: Vec<WalEntry> = Vec::new();
        for seg in closed_paths {
            let body = self
                .inner
                .object_store
                .get(&seg.path)
                .await
                .map_err(|e| BasinError::storage(format!("wal read get {}: {e}", seg.path)))?
                .bytes()
                .await
                .map_err(|e| BasinError::storage(format!("wal read body {}: {e}", seg.path)))?;
            let (_header, entries) = decode_segment(&body)?;
            for e in entries {
                if e.lsn > since_lsn {
                    out.push(into_public_entry(e));
                }
            }
        }
        for record in buffered {
            if let BufferRecord::Entry(e) = record {
                out.push(into_public_entry(e));
            }
        }
        // Closed segments were sorted by first_lsn and entries within a
        // segment are append-ordered, but if a buffer flush raced with a
        // read, ordering could be off. Final sort guarantees append order
        // regardless of source.
        out.sort_by_key(|e| e.lsn);
        Ok(out)
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition))]
    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let guard = state.lock().await;
        Ok(guard.high_water())
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, %up_to))]
    async fn truncate(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let to_delete: Vec<ObjectPath> = {
            let guard = state.lock().await;
            guard
                .closed
                .values()
                .filter(|s| s.last_lsn <= up_to)
                .map(|s| s.path.clone())
                .collect()
        };
        for p in &to_delete {
            self.inner
                .object_store
                .delete(p)
                .await
                .map_err(|e| BasinError::storage(format!("wal truncate delete {p}: {e}")))?;
        }
        if !to_delete.is_empty() {
            let mut guard = state.lock().await;
            guard.closed.retain(|_, seg| seg.last_lsn > up_to);
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<()> {
        // Idempotent: if we've already shut down the watch send is a no-op.
        let _ = self.shutdown_tx.send(true);
        let task = {
            let mut guard = self.flush_task.lock().await;
            guard.take()
        };
        if let Some(handle) = task {
            handle
                .await
                .map_err(|e| BasinError::wal(format!("flush task join: {e}")))?;
        }
        // Final synchronous drain in case any appends raced the shutdown
        // signal and landed after the task's last flush.
        self.inner.flush_all().await
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, tx_id))]
    async fn append_tx_begin(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let (lsn, should_flush) = {
            let mut guard = state.lock().await;
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            guard.buffer.push(BufferRecord::TxBegin { lsn, tx_id });
            // Markers are tiny; use a fixed overhead for the pressure heuristic.
            guard.buffer_bytes += 32;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            (lsn, pressure)
        };
        if should_flush {
            if let Err(e) = self.inner.flush_one(&state).await {
                tracing::warn!(error = %e, "buffer-pressure flush failed (tx_begin)");
            }
        }
        Ok(lsn)
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, tx_id))]
    async fn append_tx_rollback(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let (lsn, should_flush) = {
            let mut guard = state.lock().await;
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            guard.buffer.push(BufferRecord::TxRollback { lsn, tx_id });
            guard.buffer_bytes += 32;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            (lsn, pressure)
        };
        if should_flush {
            if let Err(e) = self.inner.flush_one(&state).await {
                tracing::warn!(error = %e, "buffer-pressure flush failed (tx_rollback)");
            }
        }
        Ok(lsn)
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, tx_id))]
    async fn append_tx_commit(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        tx_id: u64,
    ) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let (lsn, should_flush) = {
            let mut guard = state.lock().await;
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            guard.buffer.push(BufferRecord::TxCommit { lsn, tx_id });
            // Markers are tiny; use a fixed overhead for the pressure heuristic.
            guard.buffer_bytes += 32;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            (lsn, pressure)
        };
        if should_flush {
            if let Err(e) = self.inner.flush_one(&state).await {
                tracing::warn!(error = %e, "buffer-pressure flush failed (tx_commit)");
            }
        }
        Ok(lsn)
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, %to_holder, at_epoch))]
    async fn append_handoff_marker(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        to_holder: String,
        at_epoch: i64,
    ) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let (lsn, should_flush) = {
            let mut guard = state.lock().await;
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            guard.buffer.push(BufferRecord::Handoff {
                lsn,
                to_holder,
                at_epoch,
            });
            // Markers are tiny; same fixed overhead as the tx markers.
            guard.buffer_bytes += 64;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            (lsn, pressure)
        };
        if should_flush {
            if let Err(e) = self.inner.flush_one(&state).await {
                tracing::warn!(error = %e, "buffer-pressure flush failed (handoff)");
            }
        }
        Ok(lsn)
    }

    #[tracing::instrument(skip(self), fields(project=%project, partition=%partition, %since_lsn))]
    async fn read_events(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEvent>> {
        let state = self.inner.get_or_create_partition(project, partition).await;
        let (closed_paths, buffered): (Vec<ClosedSegment>, Vec<BufferRecord>) = {
            let guard = state.lock().await;
            let segs: Vec<ClosedSegment> = guard
                .closed
                .values()
                .filter(|s| s.last_lsn > since_lsn)
                .cloned()
                .collect();
            let buf: Vec<BufferRecord> = guard
                .buffer
                .iter()
                .filter(|r| r.lsn() > since_lsn)
                .cloned()
                .collect();
            (segs, buf)
        };

        // Build the event list in segment-append order. Closed segments are
        // indexed by first_lsn (BTreeMap), so iterating values() is already
        // LSN-ordered. Within each segment, decode_segment_full preserves
        // on-disk order.
        let mut events: Vec<WalEvent> = Vec::new();
        for seg in closed_paths {
            let body = self
                .inner
                .object_store
                .get(&seg.path)
                .await
                .map_err(|e| {
                    BasinError::storage(format!("wal read_events get {}: {e}", seg.path))
                })?
                .bytes()
                .await
                .map_err(|e| {
                    BasinError::storage(format!("wal read_events body {}: {e}", seg.path))
                })?;
            let (_header, records) = decode_segment_full(&body)?;
            for r in records {
                match r {
                    DecodedRecord::Entry(e) if e.lsn > since_lsn => {
                        events.push(WalEvent::Entry(into_public_entry(e)));
                    }
                    DecodedRecord::TxBegin { tx_id } => {
                        events.push(WalEvent::Begin { tx_id });
                    }
                    DecodedRecord::TxRollback { tx_id } => {
                        events.push(WalEvent::Rollback { tx_id });
                    }
                    DecodedRecord::TxCommit { tx_id } => {
                        events.push(WalEvent::Commit { tx_id });
                    }
                    DecodedRecord::Handoff {
                        to_holder,
                        at_epoch,
                    } => {
                        events.push(WalEvent::Handoff {
                            to_holder,
                            at_epoch,
                        });
                    }
                    DecodedRecord::Entry(_) => {} // lsn <= since_lsn, skip
                }
            }
        }

        for record in buffered {
            match record {
                BufferRecord::Entry(e) => events.push(WalEvent::Entry(into_public_entry(e))),
                BufferRecord::TxBegin { tx_id, .. } => events.push(WalEvent::Begin { tx_id }),
                BufferRecord::TxRollback { tx_id, .. } => {
                    events.push(WalEvent::Rollback { tx_id })
                }
                BufferRecord::TxCommit { tx_id, .. } => {
                    events.push(WalEvent::Commit { tx_id })
                }
                BufferRecord::Handoff {
                    to_holder,
                    at_epoch,
                    ..
                } => events.push(WalEvent::Handoff {
                    to_holder,
                    at_epoch,
                }),
            }
        }

        Ok(events)
    }
}

fn into_public_entry(e: EntryRecord) -> WalEntry {
    WalEntry {
        project: e.project,
        partition: e.partition,
        lsn: e.lsn,
        // Already a refcounted `Bytes` — moved, never copied.
        payload: e.payload,
        appended_at: e.appended_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Instant;

    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{LocalWal, Wal, WalConfig};

    fn cfg_in(dir: &TempDir) -> WalConfig {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        }
    }

    fn payload(i: u64) -> Bytes {
        Bytes::from(format!("payload-{i:08}"))
    }

    #[tokio::test]
    async fn append_assigns_monotonic_lsn() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let mut prev = Lsn::ZERO;
        for i in 1..=100u64 {
            let lsn = wal.append(&project, &part, payload(i)).await.unwrap();
            assert!(lsn > prev);
            assert_eq!(lsn, Lsn(i));
            prev = lsn;
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn read_from_returns_appended() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        for i in 1..=50u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 50);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }

        let tail = wal.read_from(&project, &part, Lsn(25)).await.unwrap();
        assert_eq!(tail.len(), 25);
        for e in &tail {
            assert!(e.lsn > Lsn(25));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn high_water_tracks_appends() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        assert_eq!(wal.high_water(&project, &part).await.unwrap(), Lsn::ZERO);
        for i in 1..=20u64 {
            let lsn = wal.append(&project, &part, payload(i)).await.unwrap();
            assert_eq!(wal.high_water(&project, &part).await.unwrap(), lsn);
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn project_isolation() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let a = ProjectId::new();
        let b = ProjectId::new();
        let part = PartitionKey::default_key();

        for i in 1..=10u64 {
            wal.append(&a, &part, payload(i)).await.unwrap();
        }
        for i in 1..=20u64 {
            wal.append(&b, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        let a_all = wal.read_from(&a, &part, Lsn::ZERO).await.unwrap();
        let b_all = wal.read_from(&b, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(a_all.len(), 10);
        assert_eq!(b_all.len(), 20);
        for e in &a_all {
            assert_eq!(e.project, a);
        }
        for e in &b_all {
            assert_eq!(e.project, b);
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn partition_isolation() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let p1 = PartitionKey::new("p1").unwrap();
        let p2 = PartitionKey::new("p2").unwrap();

        for i in 1..=15u64 {
            wal.append(&project, &p1, payload(i)).await.unwrap();
        }
        for i in 1..=25u64 {
            wal.append(&project, &p2, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        let p1_all = wal.read_from(&project, &p1, Lsn::ZERO).await.unwrap();
        let p2_all = wal.read_from(&project, &p2, Lsn::ZERO).await.unwrap();
        assert_eq!(p1_all.len(), 15);
        assert_eq!(p2_all.len(), 25);
        for e in &p1_all {
            assert_eq!(e.partition, p1);
        }
        for e in &p2_all {
            assert_eq!(e.partition, p2);
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_after_reopen() {
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        {
            let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
            for i in 1..=30u64 {
                wal.append(&project, &part, payload(i)).await.unwrap();
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }

        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        assert_eq!(wal.high_water(&project, &part).await.unwrap(), Lsn(30));
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 30);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_skips_corrupt_trailing_segment() {
        // An abrupt kill (OOM / power loss / SIGKILL after a drain that couldn't
        // flush) can leave the actively-written segment headerless or with a
        // partial trailing frame. Recovery must BOOT (skip that segment) rather
        // than crash-loop on "segment: missing header" / "truncated …".
        use std::fs;
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();
        {
            let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
            for i in 1..=20u64 {
                wal.append(&project, &part, payload(i)).await.unwrap();
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }
        // Inject a corrupt segment into the partition dir, named to sort LAST
        // (the trailing/newest segment). A 0xFFFFFFFF length prefix claims a body
        // that isn't there -> undecodable -> must be skipped, not fatal.
        fn find_seg_dir(p: &std::path::Path) -> Option<std::path::PathBuf> {
            for e in fs::read_dir(p).ok()?.flatten() {
                let pp = e.path();
                if pp.is_dir() {
                    if let Some(d) = find_seg_dir(&pp) {
                        return Some(d);
                    }
                } else if pp.extension().map_or(false, |x| x == "seg") {
                    return pp.parent().map(|x| x.to_path_buf());
                }
            }
            None
        }
        let seg_dir = find_seg_dir(dir.path()).expect("a directory holding .seg files");
        fs::write(seg_dir.join("zzzz_crash.seg"), [0xFFu8, 0xFF, 0xFF, 0xFF]).unwrap();

        // Reopen MUST NOT crash and MUST recover all 20 committed entries.
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        assert_eq!(wal.high_water(&project, &part).await.unwrap(), Lsn(20));
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(
            all.len(),
            20,
            "complete segments recover despite a corrupt trailing segment"
        );
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn truncate_removes_segments() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        // Two segments: flush after 25 to force a segment boundary.
        for i in 1..=25u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        for i in 26..=50u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        wal.truncate(&project, &part, Lsn(25)).await.unwrap();
        let remaining = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(remaining.len(), 25);
        for e in &remaining {
            assert!(e.lsn > Lsn(25));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn flush_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        for i in 1..=10u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        wal.flush().await.unwrap();

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 10);
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_append_serialised() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let wal = wal.clone();
            let part = part.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..100u64 {
                    wal.append(&project, &part, payload(i)).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        wal.flush().await.unwrap();
        let elapsed = start.elapsed();
        eprintln!("concurrent_append_serialised: 1000 appends in {elapsed:?}");

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 1000);
        let mut seen: std::collections::HashSet<Lsn> = std::collections::HashSet::new();
        for e in &all {
            assert!(seen.insert(e.lsn), "duplicate lsn {}", e.lsn);
        }
        for i in 1..=1000u64 {
            assert!(seen.contains(&Lsn(i)), "missing lsn {i}");
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn fence_rejects_stale_epoch() {
        // Phase 6.X.A epoch fence: a higher epoch raises the fence; a later
        // append with a strictly lower epoch is rejected; the current (or a
        // higher) epoch succeeds.
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        // Epoch 1 winner raises the fence.
        wal.append_fenced(&project, &part, payload(1), Some(1))
            .await
            .unwrap();
        // Epoch 2 winner raises it further.
        wal.append_fenced(&project, &part, payload(2), Some(2))
            .await
            .unwrap();

        // A stale epoch-1 holder (dual-leaseholder loser) is rejected.
        let err = wal
            .append_fenced(&project, &part, payload(3), Some(1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, BasinError::Wal(_)),
            "stale-epoch append must be a Wal fencing error, got {err:?}"
        );

        // The current epoch still succeeds, as does a higher one.
        wal.append_fenced(&project, &part, payload(4), Some(2))
            .await
            .unwrap();
        wal.append_fenced(&project, &part, payload(5), Some(3))
            .await
            .unwrap();

        // Exactly the four accepted appends are durable; the rejected one
        // consumed no LSN.
        wal.flush().await.unwrap();
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 4, "rejected append must not consume an LSN");
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn fence_no_lease_mode_always_appends() {
        // Back-compat: epoch None and Some(0) never raise the fence and are
        // always accepted, even interleaved with fenced appends.
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        // Raise the fence to epoch 5 with a fenced append.
        wal.append_fenced(&project, &part, payload(1), Some(5))
            .await
            .unwrap();
        // No-lease appends (None and 0) still succeed despite the raised fence.
        wal.append_fenced(&project, &part, payload(2), None)
            .await
            .unwrap();
        wal.append_fenced(&project, &part, payload(3), Some(0))
            .await
            .unwrap();
        // And the plain append() (no-lease) path is unaffected.
        wal.append(&project, &part, payload(4)).await.unwrap();

        wal.flush().await.unwrap();
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 4, "no-lease appends must all be accepted");
        wal.close().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Tests added to cover the pre-lock record construction refactor and to
    // verify the durability contract: appends are in-RAM-only until flush,
    // after which every acked LSN is readable on replay.
    // -----------------------------------------------------------------------

    /// After N concurrent appenders complete and `flush()` is called, every
    /// acked LSN must be visible and the sequence must be gapless. This
    /// exercises the pre-lock record-construction path under real concurrency:
    /// multiple tasks build their `EntryRecord` concurrently before serialising
    /// on the partition mutex to stamp the LSN.
    #[tokio::test]
    async fn concurrent_appends_all_durable_after_flush() {
        const WRITERS: usize = 8;
        const PER_WRITER: u64 = 200;
        const TOTAL: u64 = (WRITERS as u64) * PER_WRITER;

        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let wal2 = wal.clone();
            let part2 = part.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..PER_WRITER {
                    // Vary payload size (16-256 bytes) so that the pre-lock
                    // record-construction path sees different payload sizes.
                    let body = vec![(w as u8).wrapping_add(i as u8); 16 + (i % 240) as usize];
                    wal2.append(&project, &part2, Bytes::from(body))
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // flush() is the explicit durability gate; after it returns every acked
        // entry must be readable (they are all now in closed segments).
        wal.flush().await.unwrap();

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(
            all.len() as u64,
            TOTAL,
            "all acked records must be readable after flush"
        );

        // LSNs must be a contiguous 1..=TOTAL sequence (no gaps, no duplicates).
        let mut lsns: Vec<u64> = all.iter().map(|e| e.lsn.0).collect();
        lsns.sort_unstable();
        for (idx, &lsn) in lsns.iter().enumerate() {
            assert_eq!(
                lsn,
                (idx as u64) + 1,
                "LSN gap or duplicate at position {idx}: got {lsn}"
            );
        }

        wal.close().await.unwrap();
    }

    /// Ordering invariant: after concurrent appends, reading back with
    /// `read_from` returns entries in strictly ascending LSN order.
    #[tokio::test]
    async fn concurrent_appends_ordering_preserved() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let mut handles = Vec::new();
        for _ in 0..6 {
            let wal2 = wal.clone();
            let part2 = part.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..50u64 {
                    wal2.append(&project, &part2, payload(i)).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        wal.flush().await.unwrap();

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        for window in all.windows(2) {
            assert!(
                window[0].lsn < window[1].lsn,
                "out-of-order LSNs: {} >= {}",
                window[0].lsn,
                window[1].lsn
            );
        }
        wal.close().await.unwrap();
    }

    /// Replay after reopen sees every acked record. Durability contract: records
    /// are in-RAM until flush; once flushed they survive reopen.
    #[tokio::test]
    async fn replay_sees_all_acked_records_after_flush_reopen() {
        const N: u64 = 60;
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        {
            let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
            for i in 1..=N {
                wal.append(&project, &part, payload(i)).await.unwrap();
            }
            // flush() is required for durability — records not yet flushed are
            // in-RAM only and would be lost on process restart.
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }

        // Reopen simulates a process restart / recovery.
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len() as u64, N, "all flushed records survive reopen");
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i as u64) + 1));
        }
        wal.close().await.unwrap();
    }

    /// close() drains the in-RAM buffer to storage before returning. Records
    /// appended without an explicit flush() must survive reopen as long as
    /// close() completes successfully.
    #[tokio::test]
    async fn close_flushes_buffered_entries() {
        const N: u64 = 40;
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        {
            let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
            for i in 1..=N {
                wal.append(&project, &part, payload(i)).await.unwrap();
            }
            // No explicit flush — close() must flush the buffer.
            wal.close().await.unwrap();
        }

        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(
            all.len() as u64,
            N,
            "close() must drain the buffer; all records must survive reopen"
        );
        wal.close().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Buffer-swap flush coverage: (1) appenders make progress while a segment
    // PUT is in flight, (2) a failed PUT restores the drained records so no
    // acked entry is lost.
    // -----------------------------------------------------------------------

    /// Object-store double for flush tests. `put_opts` first signals the test
    /// on `put_started`, then either fails (when `fail_puts` is set) or parks
    /// until the `gate` semaphore grants a permit, then delegates to the inner
    /// store. Everything else delegates directly.
    ///
    /// Each PUT also records whether it carried the [`DurablePut`] extension
    /// marker into `put_durability`, so tests can assert which flushes
    /// requested an fsync (sync path) and which did not (async path).
    #[derive(Debug)]
    struct GatedStore {
        inner: Arc<dyn ObjectStore>,
        put_started: tokio::sync::mpsc::UnboundedSender<()>,
        gate: Arc<tokio::sync::Semaphore>,
        fail_puts: Arc<std::sync::atomic::AtomicBool>,
        put_durability: Arc<std::sync::Mutex<Vec<bool>>>,
    }

    impl std::fmt::Display for GatedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GatedStore")
        }
    }

    #[async_trait]
    impl ObjectStore for GatedStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.put_durability
                .lock()
                .unwrap()
                .push(opts.extensions.get::<DurablePut>().is_some());
            let _ = self.put_started.send(());
            if self.fail_puts.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "GatedStore",
                    source: "injected put failure".into(),
                });
            }
            // Park until the test releases the gate; one permit per PUT.
            let permit = self
                .gate
                .clone()
                .acquire_owned()
                .await
                .expect("gate semaphore closed");
            permit.forget();
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct GatedHarness {
        wal: LocalWal,
        started_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        gate: Arc<tokio::sync::Semaphore>,
        fail_puts: Arc<std::sync::atomic::AtomicBool>,
        put_durability: Arc<std::sync::Mutex<Vec<bool>>>,
    }

    /// Open a WAL over a [`GatedStore`] with `initial_permits` on the PUT
    /// gate. Background ticks are effectively disabled so the test fully
    /// controls when PUTs happen; the group-commit nudge path
    /// (`commit_delay` 1 ms) stays live so synchronous appends can flush.
    async fn gated_wal_in(dir: &TempDir, initial_permits: usize) -> GatedHarness {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let (started_tx, started_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(tokio::sync::Semaphore::new(initial_permits));
        let fail_puts = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let put_durability = Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = GatedStore {
            inner: Arc::new(fs),
            put_started: started_tx,
            gate: gate.clone(),
            fail_puts: fail_puts.clone(),
            put_durability: put_durability.clone(),
        };
        let cfg = WalConfig {
            object_store: Arc::new(store),
            root_prefix: None,
            flush_interval: Duration::from_secs(3600),
            flush_max_bytes: u64::MAX,
            commit_delay: Duration::from_millis(1),
        };
        let wal = LocalWal::open(cfg).await.unwrap();
        GatedHarness {
            wal,
            started_rx,
            gate,
            fail_puts,
            put_durability,
        }
    }

    /// While a segment PUT is parked inside the object store, appends to the
    /// same partition must complete without waiting for the upload: flush_one
    /// swaps the buffer out under the partition mutex and releases the lock
    /// before framing and PUT. Deterministic — the PUT is gated, not slow.
    #[tokio::test]
    async fn appends_progress_while_flush_put_in_flight() {
        let dir = TempDir::new().unwrap();
        let mut h = gated_wal_in(&dir, 0).await;
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        h.wal.append(&project, &part, payload(1)).await.unwrap();

        // Kick off an explicit flush; its PUT parks on the gate.
        let flush_wal = h.wal.clone();
        let flush_task = tokio::spawn(async move { flush_wal.flush().await });

        // Wait until the PUT is provably in flight (and therefore the
        // partition mutex has been released by flush_one).
        tokio::time::timeout(Duration::from_secs(5), h.started_rx.recv())
            .await
            .expect("flush PUT never started")
            .expect("put_started channel closed");

        // The PUT is parked. Appends must still make progress.
        let wal2 = h.wal.clone();
        let part2 = part.clone();
        let appends = async move {
            for i in 2..=50u64 {
                let lsn = wal2.append(&project, &part2, payload(i)).await.unwrap();
                assert_eq!(lsn, Lsn(i));
            }
        };
        tokio::time::timeout(Duration::from_secs(5), appends)
            .await
            .expect("appends blocked behind an in-flight segment PUT");

        // Release the parked PUT (with headroom for the flushes in close()).
        h.gate.add_permits(64);
        flush_task.await.unwrap().unwrap();

        // The flushed first entry plus the 49 appended mid-PUT are all
        // visible, in order.
        let all = h.wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 50);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }
        h.wal.close().await.unwrap();
    }

    /// PUT-failure contract: a failed segment upload restores the drained
    /// records to the buffer (no acked entry is lost), surfaces the error to
    /// explicit flush() callers, and the next flush retries successfully.
    #[tokio::test]
    async fn flush_put_failure_keeps_entries_buffered() {
        let dir = TempDir::new().unwrap();
        let mut h = gated_wal_in(&dir, 1024).await;
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        for i in 1..=5u64 {
            h.wal.append(&project, &part, payload(i)).await.unwrap();
        }

        h.fail_puts.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = h.wal.flush().await.unwrap_err();
        assert!(
            matches!(err, BasinError::Storage(_)),
            "failed segment PUT must surface as a storage error, got {err:?}"
        );
        // One PUT attempt happened and failed.
        assert!(h.started_rx.recv().await.is_some());

        // Entries survive the failed flush in the in-RAM buffer.
        let all = h.wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 5, "no acked entry may be lost on PUT failure");
        assert_eq!(h.wal.high_water(&project, &part).await.unwrap(), Lsn(5));

        // New appends interleave correctly behind the restored batch.
        h.wal.append(&project, &part, payload(6)).await.unwrap();

        // Heal the store; the retry flush must persist everything in order.
        h.fail_puts.store(false, std::sync::atomic::Ordering::SeqCst);
        h.wal.flush().await.unwrap();
        let all = h.wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 6);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }
        h.wal.close().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Synchronous-commit (group-commit) coverage: durable appends block until
    // their segment is durable, concurrent durable appends coalesce into one
    // PUT, the async path is byte-for-byte unchanged (no fsync marker, no
    // waiting), and the FsyncOnPut wrapper fsyncs once per group.
    // -----------------------------------------------------------------------

    /// Count `.seg` files under `root`, recursively. Segments are the unit of
    /// group commit: N coalesced durable appends share one segment.
    fn count_seg_files(root: &std::path::Path) -> usize {
        let mut n = 0;
        let entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                n += count_seg_files(&path);
            } else if path.extension().is_some_and(|e| e == "seg") {
                n += 1;
            }
        }
        n
    }

    /// A synchronous append must not ack until its segment PUT completes.
    /// Deterministic: the PUT parks on the gate, so we can assert the append
    /// is still blocked, then release the gate and assert it completes.
    #[tokio::test]
    async fn durable_append_blocks_until_segment_durable() {
        let dir = TempDir::new().unwrap();
        let mut h = gated_wal_in(&dir, 0).await;
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let wal2 = h.wal.clone();
        let part2 = part.clone();
        let append_task = tokio::spawn(async move {
            wal2.append_fenced_durable(&project, &part2, payload(1), None)
                .await
        });

        // The group-commit nudge wakes the flusher; its PUT parks on the gate.
        tokio::time::timeout(Duration::from_secs(5), h.started_rx.recv())
            .await
            .expect("group-commit PUT never started")
            .expect("put_started channel closed");

        // The PUT is provably in flight but not complete — the durable
        // append must still be blocked (ack-after-durable).
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !append_task.is_finished(),
            "durable append must not ack before its segment PUT completes"
        );

        // Release the PUT; the waiter must now be released with its LSN.
        h.gate.add_permits(64);
        let lsn = tokio::time::timeout(Duration::from_secs(5), append_task)
            .await
            .expect("durable append never completed after PUT was released")
            .unwrap()
            .unwrap();
        assert_eq!(lsn, Lsn(1));

        // The waiter-bearing segment requested the fsync pass.
        assert!(
            h.put_durability.lock().unwrap().iter().any(|d| *d),
            "a segment carrying a synchronous waiter must request DurablePut"
        );
        h.wal.close().await.unwrap();
    }

    /// N concurrent synchronous appends inside one `commit_delay` window
    /// must share segments: segment count << N, and every append is durable
    /// (on disk) by the time it acks — no flush()/close() needed.
    #[tokio::test]
    async fn group_commit_coalesces_concurrent_durable_appends() {
        const WRITERS: usize = 16;
        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let cfg = WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            // Ticks disabled: only the group-commit nudge can flush.
            flush_interval: Duration::from_secs(3600),
            flush_max_bytes: u64::MAX,
            commit_delay: Duration::from_millis(25),
        };
        let wal = LocalWal::open(cfg).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let wal2 = wal.clone();
            let part2 = part.clone();
            handles.push(tokio::spawn(async move {
                wal2.append_fenced_durable(&project, &part2, payload(i as u64), None)
                    .await
                    .unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Every append acked durable, so the segments are already on disk —
        // count them BEFORE any flush/close.
        let segs = count_seg_files(dir.path());
        assert!(segs >= 1, "at least one segment must exist");
        assert!(
            segs <= WRITERS / 4,
            "group commit must coalesce: {WRITERS} concurrent durable appends \
             produced {segs} segments (expected <= {})",
            WRITERS / 4
        );

        let all = wal.read_from(&project, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), WRITERS, "every durable append must be readable");
        wal.close().await.unwrap();
    }

    /// Async-path regression guard: plain appends never trigger a PUT by
    /// themselves (ack-before-durable, loss window = flush tick / pressure),
    /// and the segments they produce never carry the DurablePut fsync marker.
    #[tokio::test]
    async fn async_appends_never_request_fsync_and_never_block() {
        let dir = TempDir::new().unwrap();
        let h = gated_wal_in(&dir, 1024).await;
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        for i in 1..=20u64 {
            h.wal.append(&project, &part, payload(i)).await.unwrap();
        }
        // Acked from RAM: with ticks disabled and no pressure, zero PUTs so
        // far — appends did not wait on storage.
        assert!(
            h.put_durability.lock().unwrap().is_empty(),
            "async appends must not PUT (or block on storage) by themselves"
        );

        h.wal.flush().await.unwrap();
        {
            let puts = h.put_durability.lock().unwrap();
            assert_eq!(puts.len(), 1, "one explicit flush => one segment PUT");
            assert!(
                puts.iter().all(|d| !*d),
                "async-path segments must never request DurablePut/fsync"
            );
        }
        h.wal.close().await.unwrap();
    }

    /// End-to-end over the real [`FsyncOnPut`] wrapper: one group of
    /// concurrent durable appends costs ~one fsync, and later async-path
    /// flushes leave the fsync counter untouched.
    #[tokio::test]
    async fn durable_group_over_fsync_wrapper_fsyncs_once() {
        const WRITERS: usize = 8;
        let dir = TempDir::new().unwrap();
        let wrapper = Arc::new(crate::FsyncOnPut::new(Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        )));
        let cfg = WalConfig {
            object_store: wrapper.clone(),
            root_prefix: None,
            flush_interval: Duration::from_secs(3600),
            flush_max_bytes: u64::MAX,
            commit_delay: Duration::from_millis(25),
        };
        let wal = LocalWal::open(cfg).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();

        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let wal2 = wal.clone();
            let part2 = part.clone();
            handles.push(tokio::spawn(async move {
                wal2.append_fenced_durable(&project, &part2, payload(i as u64), None)
                    .await
                    .unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let group_fsyncs = wrapper.fsync_count();
        assert!(group_fsyncs >= 1, "the durable group must fsync");
        // Typically exactly 1; a straggler that misses the commit_delay
        // window legitimately costs a second. Half the writer count is the
        // "coalescing definitely happened" line that stays robust on a
        // loaded CI box.
        assert!(
            group_fsyncs <= (WRITERS as u64) / 2,
            "group commit must share the fsync: {WRITERS} durable appends \
             cost {group_fsyncs} fsyncs"
        );

        // Async appends + explicit flush afterwards: no new fsyncs.
        for i in 100..120u64 {
            wal.append(&project, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        assert_eq!(
            wrapper.fsync_count(),
            group_fsyncs,
            "async-path flushes must not fsync"
        );
        wal.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn bench_10k_writes() {
        let dir = TempDir::new().unwrap();
        let wal = LocalWal::open(cfg_in(&dir)).await.unwrap();
        let project = ProjectId::new();
        let part = PartitionKey::default_key();
        let body = vec![0u8; 256];

        let start = Instant::now();
        for _ in 0..10_000 {
            wal.append(&project, &part, Bytes::from(body.clone()))
                .await
                .unwrap();
        }
        wal.flush().await.unwrap();
        let elapsed = start.elapsed();
        let rate = 10_000.0 / elapsed.as_secs_f64();
        eprintln!("wal bench: 10k writes in {elapsed:?} ({rate:.0} writes/sec)");
        // Lenient bound so the test is meaningful but not flaky on slow CI.
        assert!(
            rate >= 1_000.0,
            "wal append throughput {rate:.0} writes/sec is below the 1k/sec floor",
        );
        wal.close().await.unwrap();
    }
}
