//! File-backed (single-node) WAL implementation.
//!
//! ## Storage layout
//!
//! ```text
//! {root_prefix}/wal/{tenant_id}/{partition_key}/
//!     {ulid}.seg              # closed segment, immutable
//! ```
//!
//! Each `.seg` is a sequence of length-prefixed bincode records. The first
//! record is a [`SegmentHeader`]; subsequent records are payload entries. See
//! [`crate::segment`] for the wire format.
//!
//! ## Durability guarantee
//!
//! `append` returns once the entry is in the in-RAM buffer for its
//! `(tenant, partition)`. The buffer is uploaded to the configured
//! [`ObjectStore`] either:
//!
//! 1. when the background flush task wakes (every `flush_interval`), or
//! 2. when the buffer's accumulated size exceeds `flush_max_bytes`, or
//! 3. on an explicit [`crate::Wal::flush`] call.
//!
//! With [`object_store::local::LocalFileSystem`], a successful PUT is an
//! `fsync`'d file on disk; with the S3 backend it is a durable PUT. Either
//! way the entry only survives a process crash *after* it has been uploaded
//! — v0.1 does not stage writes to a separate local-disk log first. The
//! shard owner that wraps this knows the trade-off; v0.2 (Raft) closes the
//! gap.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, Result, TenantId};
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use tokio::sync::{watch, Mutex, RwLock};
use ulid::Ulid;

use crate::segment::{
    decode_segment, entry_record, frame_into, EntryRecord, SegmentHeader, SegmentRecord,
    FORMAT_VERSION,
};
use crate::state::{ClosedSegment, PartitionState};
use crate::{Lsn, WalConfig, WalEntry, WalImpl};

/// Top-level segment of the WAL key namespace. Lives directly under
/// `{root_prefix}` (or the bucket root) so listing can scope a recovery scan
/// to just the WAL.
const WAL_SEGMENT: &str = "wal";

type PartitionMap = HashMap<(TenantId, PartitionKey), Arc<Mutex<PartitionState>>>;

/// Shared state between the public `FileWal` handle and the background flush
/// task. Holding it behind one `Arc` avoids the awkward "split a partitions
/// map across two owners" dance.
struct Inner {
    object_store: Arc<dyn ObjectStore>,
    root_prefix: Option<ObjectPath>,
    flush_max_bytes: u64,
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
        } = cfg;

        let partitions = recover_partitions(&object_store, root_prefix.as_ref()).await?;

        let inner = Arc::new(Inner {
            object_store,
            root_prefix,
            flush_max_bytes,
            partitions: RwLock::new(partitions),
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush_handle = spawn_flush_loop(inner.clone(), flush_interval, shutdown_rx);

        Ok(Self {
            inner,
            shutdown_tx,
            flush_task: Mutex::new(Some(flush_handle)),
        })
    }
}

impl Inner {
    async fn get_or_create_partition(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Arc<Mutex<PartitionState>> {
        let key = (*tenant, partition.clone());
        {
            let map = self.partitions.read().await;
            if let Some(p) = map.get(&key) {
                return p.clone();
            }
        }
        let mut map = self.partitions.write().await;
        map.entry(key)
            .or_insert_with(|| {
                Arc::new(Mutex::new(PartitionState::new(*tenant, partition.clone())))
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
    /// storage. On error the buffered entries are restored to the front of
    /// the buffer so the next flush attempt picks them up. No data loss as
    /// long as the process is alive.
    async fn flush_one(&self, state: &Arc<Mutex<PartitionState>>) -> Result<()> {
        let mut guard = state.lock().await;
        if guard.buffer.is_empty() {
            return Ok(());
        }
        let segment_id = Ulid::new();
        let first_lsn = guard.buffer.first().expect("non-empty").lsn;
        let last_lsn = guard.buffer.last().expect("non-empty").lsn;
        let header = SegmentHeader {
            format_version: FORMAT_VERSION,
            tenant: guard.tenant,
            partition: guard.partition.clone(),
            first_lsn,
            segment_id,
        };
        let mut buf: Vec<u8> = Vec::with_capacity(guard.buffer_bytes as usize + 256);
        frame_into(&mut buf, &SegmentRecord::Header(header))?;
        for entry in guard.buffer.iter() {
            frame_into(&mut buf, &SegmentRecord::Entry(entry.clone()))?;
        }

        let path = closed_segment_path(
            self.root_prefix.as_ref(),
            &guard.tenant,
            &guard.partition,
            segment_id,
        );

        let drained: Vec<EntryRecord> = guard.buffer.drain(..).collect();
        let drained_bytes = guard.buffer_bytes;
        guard.buffer_bytes = 0;
        // Drop the mutex across the network/disk round-trip so other appends
        // for this partition don't block on the upload.
        drop(guard);

        let put_result = self
            .object_store
            .put(&path, PutPayload::from_bytes(Bytes::from(buf)))
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
                tracing::debug!(
                    tenant = %guard.tenant,
                    partition = %guard.partition,
                    %first_lsn,
                    %last_lsn,
                    entries = drained.len(),
                    "wal segment flushed",
                );
                Ok(())
            }
            Err(e) => {
                let mut guard = state.lock().await;
                let mut combined = drained;
                combined.append(&mut guard.buffer);
                guard.buffer = combined;
                guard.buffer_bytes += drained_bytes;
                Err(BasinError::storage(format!("wal segment put {path}: {e}")))
            }
        }
    }
}

/// Background flush loop. Wakes on tick or shutdown; flushes every partition
/// that has buffered entries. Errors are logged but don't kill the loop —
/// the next tick retries, and explicit `flush()` callers see errors directly.
fn spawn_flush_loop(
    inner: Arc<Inner>,
    flush_interval: Duration,
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

/// Build `{root}/wal/{tenant}/{partition}/{ulid}.seg`.
fn closed_segment_path(
    root: Option<&ObjectPath>,
    tenant: &TenantId,
    partition: &PartitionKey,
    segment_id: Ulid,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(WAL_SEGMENT);
    p = p.child(tenant.as_prefix());
    let part = if partition.as_str().is_empty() {
        PartitionKey::DEFAULT
    } else {
        partition.as_str()
    };
    p = p.child(part);
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
    let mut closed: HashMap<(TenantId, PartitionKey), Vec<ClosedSegment>> = HashMap::new();
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
        let (header, entries) = decode_segment(&body)?;
        let last_lsn = entries
            .last()
            .map(|e| e.lsn)
            .unwrap_or(header.first_lsn);
        let key = (header.tenant, header.partition.clone());
        closed.entry(key).or_default().push(ClosedSegment {
            path: meta.location,
            first_lsn: header.first_lsn,
            last_lsn,
            segment_id: header.segment_id,
        });
    }

    let mut out: PartitionMap = HashMap::new();
    for ((tenant, partition), mut segs) in closed {
        segs.sort_by_key(|s| s.first_lsn);
        let next_lsn = segs
            .last()
            .map(|s| Lsn(s.last_lsn.0 + 1))
            .unwrap_or(Lsn(1));
        let mut state = PartitionState::new(tenant, partition.clone());
        state.next_lsn = next_lsn;
        for s in segs {
            state.closed.insert(s.first_lsn, s);
        }
        out.insert((tenant, partition), Arc::new(Mutex::new(state)));
    }
    Ok(out)
}

#[async_trait]
impl WalImpl for FileWal {
    #[tracing::instrument(skip(self, payload), fields(tenant=%tenant, partition=%partition, bytes=payload.len()))]
    async fn append(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(tenant, partition).await;
        let (lsn, should_flush) = {
            let mut guard = state.lock().await;
            let lsn = guard.next_lsn;
            guard.next_lsn = lsn.next();
            let now = Utc::now();
            let record = entry_record(tenant, partition, lsn, &payload, now);
            // Approximate framed size: 4 (length prefix) + bincode body. The
            // bincode body is roughly the payload + ~64 bytes for ids and
            // timestamp. The actual flush serialises again; this counter is
            // only for the buffer-pressure heuristic.
            let approx = (payload.len() as u64) + 96;
            guard.buffer.push(record);
            guard.buffer_bytes += approx;
            let pressure = guard.buffer_bytes >= self.inner.flush_max_bytes;
            (lsn, pressure)
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
        Ok(lsn)
    }

    #[tracing::instrument(skip(self))]
    async fn flush(&self) -> Result<()> {
        self.inner.flush_all().await
    }

    #[tracing::instrument(skip(self), fields(tenant=%tenant, partition=%partition, %since_lsn))]
    async fn read_from(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        let state = self.inner.get_or_create_partition(tenant, partition).await;
        // Snapshot the closed-segment list and the in-RAM buffer under the
        // lock; do the (potentially slow) GETs without holding it.
        let (closed_paths, buffered): (Vec<ClosedSegment>, Vec<EntryRecord>) = {
            let guard = state.lock().await;
            let segs: Vec<ClosedSegment> = guard
                .closed
                .values()
                .filter(|s| s.last_lsn > since_lsn)
                .cloned()
                .collect();
            let buf: Vec<EntryRecord> = guard
                .buffer
                .iter()
                .filter(|e| e.lsn > since_lsn)
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
        for e in buffered {
            out.push(into_public_entry(e));
        }
        // Closed segments were sorted by first_lsn and entries within a
        // segment are append-ordered, but if a buffer flush raced with a
        // read, ordering could be off. Final sort guarantees append order
        // regardless of source.
        out.sort_by_key(|e| e.lsn);
        Ok(out)
    }

    #[tracing::instrument(skip(self), fields(tenant=%tenant, partition=%partition))]
    async fn high_water(&self, tenant: &TenantId, partition: &PartitionKey) -> Result<Lsn> {
        let state = self.inner.get_or_create_partition(tenant, partition).await;
        let guard = state.lock().await;
        Ok(guard.high_water())
    }

    #[tracing::instrument(skip(self), fields(tenant=%tenant, partition=%partition, %up_to))]
    async fn truncate(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) -> Result<()> {
        let state = self.inner.get_or_create_partition(tenant, partition).await;
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
}

fn into_public_entry(e: EntryRecord) -> WalEntry {
    WalEntry {
        tenant: e.tenant,
        partition: e.partition,
        lsn: e.lsn,
        payload: Bytes::from(e.payload),
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

    use crate::{Wal, WalConfig};

    fn cfg_in(dir: &TempDir) -> WalConfig {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        }
    }

    fn payload(i: u64) -> Bytes {
        Bytes::from(format!("payload-{i:08}"))
    }

    #[tokio::test]
    async fn append_assigns_monotonic_lsn() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        let mut prev = Lsn::ZERO;
        for i in 1..=100u64 {
            let lsn = wal.append(&tenant, &part, payload(i)).await.unwrap();
            assert!(lsn > prev);
            assert_eq!(lsn, Lsn(i));
            prev = lsn;
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn read_from_returns_appended() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        for i in 1..=50u64 {
            wal.append(&tenant, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        let all = wal.read_from(&tenant, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 50);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }

        let tail = wal.read_from(&tenant, &part, Lsn(25)).await.unwrap();
        assert_eq!(tail.len(), 25);
        for e in &tail {
            assert!(e.lsn > Lsn(25));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn high_water_tracks_appends() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        assert_eq!(wal.high_water(&tenant, &part).await.unwrap(), Lsn::ZERO);
        for i in 1..=20u64 {
            let lsn = wal.append(&tenant, &part, payload(i)).await.unwrap();
            assert_eq!(wal.high_water(&tenant, &part).await.unwrap(), lsn);
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let a = TenantId::new();
        let b = TenantId::new();
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
            assert_eq!(e.tenant, a);
        }
        for e in &b_all {
            assert_eq!(e.tenant, b);
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn partition_isolation() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let p1 = PartitionKey::new("p1").unwrap();
        let p2 = PartitionKey::new("p2").unwrap();

        for i in 1..=15u64 {
            wal.append(&tenant, &p1, payload(i)).await.unwrap();
        }
        for i in 1..=25u64 {
            wal.append(&tenant, &p2, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        let p1_all = wal.read_from(&tenant, &p1, Lsn::ZERO).await.unwrap();
        let p2_all = wal.read_from(&tenant, &p2, Lsn::ZERO).await.unwrap();
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
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        {
            let wal = Wal::open(cfg_in(&dir)).await.unwrap();
            for i in 1..=30u64 {
                wal.append(&tenant, &part, payload(i)).await.unwrap();
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }

        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        assert_eq!(wal.high_water(&tenant, &part).await.unwrap(), Lsn(30));
        let all = wal.read_from(&tenant, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 30);
        for (i, e) in all.iter().enumerate() {
            assert_eq!(e.lsn, Lsn((i + 1) as u64));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn truncate_removes_segments() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        // Two segments: flush after 25 to force a segment boundary.
        for i in 1..=25u64 {
            wal.append(&tenant, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        for i in 26..=50u64 {
            wal.append(&tenant, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();

        wal.truncate(&tenant, &part, Lsn(25)).await.unwrap();
        let remaining = wal.read_from(&tenant, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(remaining.len(), 25);
        for e in &remaining {
            assert!(e.lsn > Lsn(25));
        }
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn flush_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        for i in 1..=10u64 {
            wal.append(&tenant, &part, payload(i)).await.unwrap();
        }
        wal.flush().await.unwrap();
        wal.flush().await.unwrap();

        let all = wal.read_from(&tenant, &part, Lsn::ZERO).await.unwrap();
        assert_eq!(all.len(), 10);
        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_append_serialised() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();

        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let wal = wal.clone();
            let part = part.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..100u64 {
                    wal.append(&tenant, &part, payload(i)).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        wal.flush().await.unwrap();
        let elapsed = start.elapsed();
        eprintln!("concurrent_append_serialised: 1000 appends in {elapsed:?}");

        let all = wal.read_from(&tenant, &part, Lsn::ZERO).await.unwrap();
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
    #[ignore]
    async fn bench_10k_writes() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(cfg_in(&dir)).await.unwrap();
        let tenant = TenantId::new();
        let part = PartitionKey::default_key();
        let body = vec![0u8; 256];

        let start = Instant::now();
        for _ in 0..10_000 {
            wal.append(&tenant, &part, Bytes::from(body.clone()))
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
