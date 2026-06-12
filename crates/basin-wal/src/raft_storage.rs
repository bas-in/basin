//! Disk-backed openraft storage for [`crate::RaftWal`] (multi-node commit 2).
//!
//! Replaces the v0.1 RAM-only `BasinRaftStorage`: the raft **log** and
//! **vote** (hard state) now survive process restarts, which is the
//! prerequisite for running raft outside the single-process simulation.
//!
//! ## Layout (under `RaftWalConfig::data_dir`)
//!
//! ```text
//! <data_dir>/
//!   raft.log       append-only log segment (shared WAL framing, see below)
//!   raft.meta      vote + last-purged log id (small JSON, atomic rewrite)
//!   raft.snapshot  latest state-machine snapshot (JSON, atomic rewrite)
//! ```
//!
//! ## Log segment format
//!
//! `raft.log` reuses the file WAL's segment framing primitive byte-for-byte
//! ([`crate::segment::frame_record_into`] / [`crate::segment::iter_frames`]):
//! a sequence of `[u32 LE length][bincode record]` frames. The record
//! vocabulary is raft-specific ([`RaftLogRecord`]) but the framing format is
//! the same one `file_wal.rs` writes — one framing format in this crate, two
//! record vocabularies.
//!
//! - **Append** (`append_to_log`): one `Entry` frame per raft entry,
//!   `write_all` + `sync_data` before returning (openraft requires entries
//!   durable on return). The entry body is serde_json — the codec the
//!   snapshot path already uses; openraft's `Entry` types are JSON-friendly.
//! - **Truncate-from** (`delete_conflict_logs_since`): a `TruncateSince`
//!   marker frame is appended (+fsync). Replay drops every entry at or above
//!   the marker; a later re-append at the same index overwrites on replay
//!   (BTreeMap insert), which is exactly openraft's conflict-resolution
//!   behaviour.
//! - **Purge-to** (`purge_logs_upto`): the purge floor is persisted to
//!   `raft.meta` first (atomic rewrite + fsync), then the log file is
//!   compacted in place (atomic rewrite of the surviving suffix). A crash
//!   between the two steps leaves purged entries in the file; reopen drops
//!   everything at or below the persisted floor.
//!
//! ## What is and is not persisted
//!
//! Log, vote, and the latest snapshot are durable. The applied **state
//! machine** stays in RAM and is rebuilt on open from the snapshot file —
//! the "implementation with persistent snapshot" model from openraft's
//! `RaftStorage::apply_to_state_machine` docs. Entries applied after the
//! latest snapshot are re-applied from the (durable) log by the raft
//! protocol itself after restart. Snapshots move to S3-anchored manifests in
//! a follow-up commit; the local snapshot file is the stopgap that keeps
//! `purge` + restart loss-free until then.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use basin_common::{BasinError, PartitionKey, ProjectId, Result};
use chrono::Utc;
use openraft::storage::{LogState, RaftSnapshotBuilder, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftLogReader, RaftStorage, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft_wal::{BasinRaftResponse, BasinRaftTypeConfig};
use crate::segment::{frame_record_into, iter_frames};
use crate::Lsn;

type C = BasinRaftTypeConfig;
type NodeId = u64;

const LOG_FILE: &str = "raft.log";
const META_FILE: &str = "raft.meta";
const SNAPSHOT_FILE: &str = "raft.snapshot";

// ---------------------------------------------------------------------------
// On-disk record shapes
// ---------------------------------------------------------------------------

/// One framed record in `raft.log`. Framed via the shared WAL segment
/// primitive (`[u32 LE length][bincode bytes]` — see module docs); the entry
/// body inside is serde_json because openraft's `Entry` types are
/// JSON-friendly (same codec choice as the snapshot path).
#[derive(Debug, Serialize, Deserialize)]
enum RaftLogRecord {
    /// A raft log entry at `index`. On replay a later record at the same
    /// index overwrites an earlier one — the append-after-conflict shape.
    Entry { index: u64, entry_json: Vec<u8> },
    /// Conflict-truncation marker (`delete_conflict_logs_since`): replay
    /// drops every entry with `index >= since`.
    TruncateSince { since: u64 },
}

/// `raft.meta` — the small fsync'd hard-state file: vote plus the purge
/// floor. Rewritten atomically (tmp + fsync + rename + dir fsync, the same
/// idiom as [`crate::FsyncOnPut`]) on every vote change and purge.
#[derive(Debug, Default, Serialize, Deserialize)]
struct RaftMeta {
    vote: Option<Vote<NodeId>>,
    last_purged_log_id: Option<LogId<NodeId>>,
}

/// `raft.snapshot` — latest snapshot blob + its raft metadata.
#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// State machine (in-RAM; rebuilt from the snapshot file on open)
// ---------------------------------------------------------------------------

/// Per-`(project, partition)` log entries the state machine has applied.
/// One `Vec` per partition, sorted by LSN.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct PartitionLog {
    /// LSN to assign to the next applied entry. Monotonic per partition.
    pub(crate) next_lsn: u64,
    /// Entries that survive `truncate` (i.e. LSN > the most recent truncate
    /// floor). Sorted by LSN.
    pub(crate) entries: Vec<StoredEntry>,
    /// Truncation floor: any entry with LSN <= `truncated_up_to` has been
    /// pruned. `read_from` / `high_water` honour it.
    pub(crate) truncated_up_to: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredEntry {
    pub(crate) lsn: u64,
    pub(crate) payload: Vec<u8>,
    pub(crate) appended_at: chrono::DateTime<chrono::Utc>,
}

/// The state machine's applied state. Everything in here is what a snapshot
/// serialises and what `apply` mutates.
///
/// `partitions` lives in a `HashMap` for O(1) lookup but is serialised as a
/// `Vec<PartitionEntry>` because `serde_json` can't encode non-string map
/// keys (the natural `(ProjectId, PartitionKey)` tuple). See the manual
/// serde impl below.
#[derive(Clone, Debug, Default)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    partitions: HashMap<(ProjectId, PartitionKey), PartitionLog>,
}

#[derive(Serialize, Deserialize)]
struct PartitionEntry {
    project: ProjectId,
    partition: PartitionKey,
    log: PartitionLog,
}

#[derive(Serialize, Deserialize)]
struct StateMachineDataWire {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    partitions: Vec<PartitionEntry>,
}

impl Serialize for StateMachineData {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        let wire = StateMachineDataWire {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|((t, p), log)| PartitionEntry {
                    project: *t,
                    partition: p.clone(),
                    log: log.clone(),
                })
                .collect(),
        };
        wire.serialize(ser)
    }
}

impl<'de> Deserialize<'de> for StateMachineData {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        let wire = StateMachineDataWire::deserialize(de)?;
        let mut partitions = HashMap::new();
        for e in wire.partitions {
            partitions.insert((e.project, e.partition), e.log);
        }
        Ok(Self {
            last_applied_log: wire.last_applied_log,
            last_membership: wire.last_membership,
            partitions,
        })
    }
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Disk plumbing helpers (sync I/O — small, bounded writes; the same
// reasoning as openraft's reference rocks store, which also does its disk
// work inline in the storage methods)
// ---------------------------------------------------------------------------

/// Write `bytes` to `path` atomically: tmp file + `sync_data`, rename into
/// place, then `sync_all` the parent directory so the rename itself is
/// durable. Same idiom as [`crate::FsyncOnPut`].
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    // Tmp name appends ".tmp" to the FULL file name (`raft.meta` →
    // `raft.meta.tmp`) — `with_extension` would collapse `raft.log` /
    // `raft.meta` / `raft.snapshot` onto one `raft.tmp` and let concurrent
    // meta + snapshot writes clobber each other's staging file.
    let mut tmp_name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    tmp_name.push(".tmp");
    let tmp = path.with_file_name(tmp_name);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_data()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Open (or create) the log file for appending.
fn open_log_for_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// Log-side state: the in-RAM index of the on-disk log, plus the open
/// append handle. One lock; every log/vote method serialises through it.
struct LogInner {
    entries: BTreeMap<u64, Entry<C>>,
    last_purged_log_id: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    /// Open append handle on `raft.log`. Replaced after a purge compaction
    /// rewrites the file (the old handle would point at the unlinked inode).
    file: File,
}

/// Disk-backed combined storage for log + state machine. Wrapped in
/// `Adaptor` by [`crate::RaftWal`] to satisfy `Raft::new` — same shape as
/// the RAM store it replaces, so the swap is invisible to the rest of the
/// raft wiring (including every `SimCluster` test).
pub(crate) struct DiskRaftStorage {
    log_path: PathBuf,
    meta_path: PathBuf,
    snapshot_path: PathBuf,
    log: RwLock<LogInner>,
    /// State machine data. In-RAM; rebuilt from `raft.snapshot` on open.
    sm: RwLock<StateMachineData>,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_idx: RwLock<u64>,
}

impl DiskRaftStorage {
    /// Open (or create) the storage rooted at `dir`, replaying `raft.meta`,
    /// `raft.snapshot`, and `raft.log` into RAM.
    pub(crate) fn open(dir: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(dir)
            .map_err(|e| BasinError::wal(format!("raft storage: create dir {dir:?}: {e}")))?;
        let log_path = dir.join(LOG_FILE);
        let meta_path = dir.join(META_FILE);
        let snapshot_path = dir.join(SNAPSHOT_FILE);

        // Meta: vote + purge floor.
        let meta: RaftMeta = match std::fs::read(&meta_path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| BasinError::wal(format!("raft storage: decode meta: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RaftMeta::default(),
            Err(e) => return Err(BasinError::wal(format!("raft storage: read meta: {e}"))),
        };

        // Snapshot: latest state machine image.
        let (sm, current_snapshot) = match std::fs::read(&snapshot_path) {
            Ok(bytes) => {
                let file: SnapshotFile = serde_json::from_slice(&bytes)
                    .map_err(|e| BasinError::wal(format!("raft storage: decode snapshot: {e}")))?;
                let sm: StateMachineData = serde_json::from_slice(&file.data).map_err(|e| {
                    BasinError::wal(format!("raft storage: decode snapshot state: {e}"))
                })?;
                let stored = StoredSnapshot {
                    meta: file.meta,
                    data: file.data,
                };
                (sm, Some(stored))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                (StateMachineData::default(), None)
            }
            Err(e) => return Err(BasinError::wal(format!("raft storage: read snapshot: {e}"))),
        };

        // Log: replay frames in order. `Entry` inserts (later frames at the
        // same index overwrite — append-after-conflict), `TruncateSince`
        // drops the suffix.
        //
        // Torn tail: a crash mid-append can leave a partial final frame.
        // That is the expected crash window of an append-only log, not
        // corruption — the partial frame was never acked (the fsync happens
        // before `append_to_log` returns), so we truncate the file back to
        // the last whole frame and continue. A bincode/serde_json failure on
        // a COMPLETE frame, by contrast, is real corruption and stays a
        // hard error.
        let mut entries: BTreeMap<u64, Entry<C>> = BTreeMap::new();
        match std::fs::read(&log_path) {
            Ok(bytes) => {
                let mut frames = iter_frames(&bytes);
                let mut good_up_to = 0usize;
                loop {
                    match frames.next() {
                        None => break,
                        Some(Ok(body)) => {
                            let record: RaftLogRecord = bincode::deserialize(body)
                                .map_err(|e| BasinError::wal(format!("raft log decode: {e}")))?;
                            match record {
                                RaftLogRecord::Entry { index, entry_json } => {
                                    let entry: Entry<C> = serde_json::from_slice(&entry_json)
                                        .map_err(|e| {
                                            BasinError::wal(format!(
                                                "raft log entry decode: {e}"
                                            ))
                                        })?;
                                    entries.insert(index, entry);
                                }
                                RaftLogRecord::TruncateSince { since } => {
                                    entries.split_off(&since);
                                }
                            }
                            good_up_to = frames.offset();
                        }
                        Some(Err(e)) => {
                            tracing::warn!(
                                error = %e,
                                good_up_to,
                                file_len = bytes.len(),
                                "raft log: torn tail frame (crash mid-append); \
                                 truncating back to the last whole frame",
                            );
                            let f = OpenOptions::new()
                                .write(true)
                                .open(&log_path)
                                .map_err(|e| {
                                    BasinError::wal(format!("raft log open for repair: {e}"))
                                })?;
                            f.set_len(good_up_to as u64).map_err(|e| {
                                BasinError::wal(format!("raft log tail truncate: {e}"))
                            })?;
                            f.sync_data().map_err(|e| {
                                BasinError::wal(format!("raft log tail truncate sync: {e}"))
                            })?;
                            break;
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BasinError::wal(format!("raft storage: read log: {e}"))),
        }
        // Drop anything at or below the persisted purge floor — covers the
        // crash window where the meta write landed but the log compaction
        // rewrite did not.
        if let Some(purged) = meta.last_purged_log_id {
            let keep = entries.split_off(&(purged.index + 1));
            entries = keep;
        }

        let file = open_log_for_append(&log_path)
            .map_err(|e| BasinError::wal(format!("raft storage: open log: {e}")))?;

        Ok(Arc::new(Self {
            log_path,
            meta_path,
            snapshot_path,
            log: RwLock::new(LogInner {
                entries,
                last_purged_log_id: meta.last_purged_log_id,
                vote: meta.vote,
                file,
            }),
            sm: RwLock::new(sm),
            current_snapshot: RwLock::new(current_snapshot),
            snapshot_idx: RwLock::new(0),
        }))
    }

    /// Serialise + atomically persist `raft.meta` from the locked log state.
    fn persist_meta(&self, inner: &LogInner) -> std::result::Result<(), std::io::Error> {
        let meta = RaftMeta {
            vote: inner.vote,
            last_purged_log_id: inner.last_purged_log_id,
        };
        let bytes = serde_json::to_vec(&meta)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_atomic(&self.meta_path, &bytes)
    }

    /// Compact `raft.log` to exactly the surviving entries (atomic rewrite)
    /// and reopen the append handle. Called after a purge so the on-disk log
    /// stays bounded by the un-purged window.
    fn rewrite_log(&self, inner: &mut LogInner) -> Result<()> {
        let mut buf = Vec::new();
        for entry in inner.entries.values() {
            let entry_json = serde_json::to_vec(entry)
                .map_err(|e| BasinError::wal(format!("raft log encode: {e}")))?;
            frame_record_into(
                &mut buf,
                &RaftLogRecord::Entry {
                    index: entry.log_id.index,
                    entry_json,
                },
            )?;
        }
        write_atomic(&self.log_path, &buf)
            .map_err(|e| BasinError::wal(format!("raft log rewrite: {e}")))?;
        inner.file = open_log_for_append(&self.log_path)
            .map_err(|e| BasinError::wal(format!("raft log reopen: {e}")))?;
        Ok(())
    }

    /// Append pre-framed bytes to `raft.log` and fsync. The caller frames
    /// with [`frame_record_into`] before taking this hop.
    fn append_and_sync(inner: &mut LogInner, buf: &[u8]) -> std::io::Result<()> {
        inner.file.write_all(buf)?;
        inner.file.sync_data()
    }

    // -- state-machine accessors used by `RaftWal` ---------------------------

    /// Clone the applied view of one `(project, partition)` for `read_from`
    /// / `high_water`.
    pub(crate) async fn partition_view(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> PartitionLog {
        let sm = self.sm.read().await;
        sm.partitions
            .get(&(*project, partition.clone()))
            .cloned()
            .unwrap_or_default()
    }

    /// Apply a `Wal::truncate` to the applied view: drop entries with
    /// LSN <= `up_to` and advance the truncation floor. Local-node-only by
    /// design (see `RaftWal::truncate`'s docs).
    pub(crate) async fn truncate_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        up_to: Lsn,
    ) {
        let mut sm = self.sm.write().await;
        let key = (*project, partition.clone());
        if let Some(part) = sm.partitions.get_mut(&key) {
            part.entries.retain(|e| e.lsn > up_to.0);
            if up_to.0 > part.truncated_up_to {
                part.truncated_up_to = up_to.0;
            }
        }
    }

    /// Persist the current snapshot blob to `raft.snapshot` (atomic rewrite).
    fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        data: &[u8],
    ) -> std::result::Result<(), std::io::Error> {
        let file = SnapshotFile {
            meta: meta.clone(),
            data: data.to_vec(),
        };
        let bytes = serde_json::to_vec(&file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_atomic(&self.snapshot_path, &bytes)
    }
}

impl RaftLogReader<C> for Arc<DiskRaftStorage> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<C>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries: Vec<Entry<C>> = log.entries.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<C> for Arc<DiskRaftStorage> {
    async fn build_snapshot(&mut self) -> std::result::Result<Snapshot<C>, StorageError<NodeId>> {
        let (data, last_applied_log, last_membership) = {
            let sm = self.sm.read().await;
            let bytes =
                serde_json::to_vec(&*sm).map_err(|e| StorageIOError::read_state_machine(&e))?;
            (bytes, sm.last_applied_log, sm.last_membership.clone())
        };

        let snapshot_idx = {
            let mut g = self.snapshot_idx.write().await;
            *g += 1;
            *g
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        // Durable before visible: persist the snapshot file, then publish it
        // as the current snapshot.
        self.persist_snapshot(&meta, &data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        {
            let mut current = self.current_snapshot.write().await;
            *current = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<C> for Arc<DiskRaftStorage> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<C>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last = log.entries.iter().next_back().map(|(_, e)| *e.get_log_id());
        let last_purged = log.last_purged_log_id;
        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        log.vote = Some(*vote);
        // The vote must be on disk before this returns — raft's "never vote
        // twice in one term" safety rule rides on it.
        self.persist_meta(&log)
            .map_err(|e| StorageIOError::write_vote(&e))?;
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.log.read().await.vote)
    }

    async fn append_to_log<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
    {
        let mut log = self.log.write().await;
        let mut buf = Vec::new();
        for entry in entries {
            let entry_json =
                serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
            frame_record_into(
                &mut buf,
                &RaftLogRecord::Entry {
                    index: entry.log_id.index,
                    entry_json,
                },
            )
            .map_err(|e| StorageIOError::write_logs(&e))?;
            log.entries.insert(entry.log_id.index, entry);
        }
        if !buf.is_empty() {
            // Entries must be durable before returning (openraft contract).
            DiskRaftStorage::append_and_sync(&mut log, &buf)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        log.entries.split_off(&log_id.index);
        // Logical truncation marker: replay drops `index >= since`. The
        // truncated bytes stay in the file until the next purge compaction;
        // any re-append at these indexes overwrites on replay anyway.
        let mut buf = Vec::new();
        frame_record_into(
            &mut buf,
            &RaftLogRecord::TruncateSince {
                since: log_id.index,
            },
        )
        .map_err(|e| StorageIOError::write_logs(&e))?;
        DiskRaftStorage::append_and_sync(&mut log, &buf)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        assert!(log.last_purged_log_id <= Some(log_id));
        log.last_purged_log_id = Some(log_id);
        let keep = log.entries.split_off(&(log_id.index + 1));
        log.entries = keep;
        // Order matters for crash-safety: persist the floor first, then
        // compact the file. A crash in between leaves purged entries on
        // disk; reopen drops everything at or below the persisted floor.
        self.persist_meta(&log)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        self.rewrite_log(&mut log)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
        StorageError<NodeId>,
    > {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<C>],
    ) -> std::result::Result<Vec<BasinRaftResponse>, StorageError<NodeId>> {
        let mut sm = self.sm.write().await;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {
                    out.push(BasinRaftResponse { lsn: Lsn::ZERO });
                }
                EntryPayload::Normal(req) => {
                    let key = (req.project, req.partition.clone());
                    let part = sm.partitions.entry(key).or_default();
                    if part.next_lsn == 0 {
                        part.next_lsn = 1;
                    }
                    let lsn = part.next_lsn;
                    part.next_lsn += 1;
                    part.entries.push(StoredEntry {
                        lsn,
                        payload: req.payload.clone(),
                        appended_at: Utc::now(),
                    });
                    out.push(BasinRaftResponse { lsn: Lsn(lsn) });
                }
                EntryPayload::Membership(m) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    out.push(BasinRaftResponse { lsn: Lsn::ZERO });
                }
            }
        }
        Ok(out)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let new_sm: StateMachineData = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // Durable before visible, same as build_snapshot.
        self.persist_snapshot(meta, &data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        {
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }
        let mut current = self.current_snapshot.write().await;
        *current = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<C>>, StorageError<NodeId>> {
        let snap = self.current_snapshot.read().await;
        Ok(snap.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests — crash-safety is "write, drop, reopen, verify"
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_wal::BasinRaftRequest;
    use openraft::CommittedLeaderId;
    use tempfile::TempDir;

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn entry(term: u64, index: u64, payload: &str) -> Entry<C> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(BasinRaftRequest {
                project: ProjectId::new(),
                partition: PartitionKey::default_key(),
                payload: payload.as_bytes().to_vec(),
            }),
        }
    }

    fn payload_of(e: &Entry<C>) -> &[u8] {
        match &e.payload {
            EntryPayload::Normal(req) => &req.payload,
            other => panic!("expected Normal payload, got {other:?}"),
        }
    }

    async fn append(store: &mut Arc<DiskRaftStorage>, entries: Vec<Entry<C>>) {
        store.append_to_log(entries).await.unwrap();
    }

    async fn all_entries(store: &mut Arc<DiskRaftStorage>) -> Vec<Entry<C>> {
        store.try_get_log_entries(..).await.unwrap()
    }

    /// Crash-safety: appended entries must read back identically after the
    /// process "dies" (store dropped) and the storage is reopened from disk.
    #[tokio::test]
    async fn log_append_read_back_across_reopen() {
        let dir = TempDir::new().unwrap();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        let entries: Vec<Entry<C>> = (1..=5).map(|i| entry(1, i, &format!("payload-{i}"))).collect();
        append(&mut store, entries).await;

        // Read back pre-drop.
        let got = all_entries(&mut store).await;
        assert_eq!(got.len(), 5);
        drop(store);

        // Reopen — the disk is the only carrier of state now.
        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        let got = all_entries(&mut reopened).await;
        assert_eq!(got.len(), 5);
        for (i, e) in got.iter().enumerate() {
            let index = (i + 1) as u64;
            assert_eq!(e.log_id, log_id(1, index));
            assert_eq!(payload_of(e), format!("payload-{index}").as_bytes());
        }

        // Range reads honour bounds.
        let mid = reopened.try_get_log_entries(2..=4).await.unwrap();
        assert_eq!(mid.len(), 3);
        assert_eq!(mid[0].log_id.index, 2);
        assert_eq!(mid[2].log_id.index, 4);

        // Log state survives too.
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 5)));
        assert_eq!(state.last_purged_log_id, None);
    }

    /// The vote (hard state) must be on disk before `save_vote` returns and
    /// must read back after reopen; a later vote overwrites an earlier one.
    #[tokio::test]
    async fn vote_persists_across_reopen() {
        let dir = TempDir::new().unwrap();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), None);
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        drop(store);

        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(3, 1)));

        // Overwrite with a later term; the latest write wins after reopen.
        reopened.save_vote(&Vote::new(5, 2)).await.unwrap();
        drop(reopened);
        let mut again = DiskRaftStorage::open(dir.path()).unwrap();
        assert_eq!(again.read_vote().await.unwrap(), Some(Vote::new(5, 2)));
    }

    /// Conflict case: a follower truncates from index 6, then a new leader's
    /// entries land at the same indexes with a higher term. Both the
    /// truncation and the overwrite must survive reopen.
    #[tokio::test]
    async fn truncation_conflict_survives_reopen() {
        let dir = TempDir::new().unwrap();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        let entries: Vec<Entry<C>> = (1..=10).map(|i| entry(1, i, &format!("t1-{i}"))).collect();
        append(&mut store, entries).await;

        // Leader conflict: drop everything from index 6 (inclusive).
        store.delete_conflict_logs_since(log_id(1, 6)).await.unwrap();
        let got = all_entries(&mut store).await;
        assert_eq!(got.len(), 5, "indexes 6..=10 deleted");
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 5)));

        // Truncation alone must survive a crash — reopen BEFORE re-appending
        // (this is the window where a marker-less implementation would
        // resurrect the deleted suffix).
        drop(store);
        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        let got = all_entries(&mut reopened).await;
        assert_eq!(got.len(), 5, "truncation persisted across reopen");
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_log_id,
            Some(log_id(1, 5))
        );

        // New leader's entries land at 6..=8 with term 2.
        let replacement: Vec<Entry<C>> = (6..=8).map(|i| entry(2, i, &format!("t2-{i}"))).collect();
        append(&mut reopened, replacement).await;
        drop(reopened);

        let mut final_store = DiskRaftStorage::open(dir.path()).unwrap();
        let got = all_entries(&mut final_store).await;
        assert_eq!(got.len(), 8);
        for e in &got[..5] {
            assert_eq!(e.log_id.leader_id, CommittedLeaderId::new(1, 1));
        }
        for (off, e) in got[5..].iter().enumerate() {
            let index = (6 + off) as u64;
            assert_eq!(e.log_id, log_id(2, index), "term-2 overwrite survived");
            assert_eq!(payload_of(e), format!("t2-{index}").as_bytes());
        }
        assert_eq!(
            final_store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(2, 8))
        );
    }

    /// Purge: entries at or below the floor disappear, the floor itself is
    /// reported by `get_log_state` (including the purged-everything case
    /// where the log is empty), and both survive reopen. The on-disk file is
    /// compacted, so purged bytes don't accumulate.
    #[tokio::test]
    async fn purge_survives_reopen_and_compacts() {
        let dir = TempDir::new().unwrap();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        let entries: Vec<Entry<C>> = (1..=10).map(|i| entry(1, i, &format!("p-{i}"))).collect();
        append(&mut store, entries).await;
        let bytes_before = std::fs::metadata(dir.path().join(LOG_FILE)).unwrap().len();

        store.purge_logs_upto(log_id(1, 4)).await.unwrap();
        let got = all_entries(&mut store).await;
        assert_eq!(got.len(), 6);
        assert_eq!(got[0].log_id.index, 5);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_log_id, Some(log_id(1, 10)));

        // Purge compaction must shrink the on-disk segment.
        let bytes_after = std::fs::metadata(dir.path().join(LOG_FILE)).unwrap().len();
        assert!(
            bytes_after < bytes_before,
            "purge must compact the log file ({bytes_before} -> {bytes_after})"
        );

        drop(store);
        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        let got = all_entries(&mut reopened).await;
        assert_eq!(got.len(), 6, "purge floor persisted across reopen");
        assert_eq!(got[0].log_id.index, 5);
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_log_id, Some(log_id(1, 10)));

        // Purge everything: the log is empty and last_log_id collapses to
        // the purge floor — the exact contract get_log_state documents.
        reopened.purge_logs_upto(log_id(1, 10)).await.unwrap();
        drop(reopened);
        let mut empty = DiskRaftStorage::open(dir.path()).unwrap();
        assert!(all_entries(&mut empty).await.is_empty());
        let state = empty.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 10)));
        assert_eq!(state.last_log_id, Some(log_id(1, 10)));

        // Appends keep working after a full purge + reopen.
        append(&mut empty, vec![entry(2, 11, "post-purge")]).await;
        assert_eq!(
            empty.get_log_state().await.unwrap().last_log_id,
            Some(log_id(2, 11))
        );
    }

    /// Crash mid-append: a torn (partial) final frame must not brick the
    /// store. Reopen truncates back to the last whole frame; the acked
    /// prefix survives and new appends keep working.
    #[tokio::test]
    async fn torn_tail_frame_is_truncated_on_reopen() {
        let dir = TempDir::new().unwrap();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        let entries: Vec<Entry<C>> = (1..=3).map(|i| entry(1, i, &format!("ok-{i}"))).collect();
        append(&mut store, entries).await;
        drop(store);

        // Simulate the crash window: a length prefix promising more bytes
        // than the file holds (the partial write of a fourth frame).
        let log_path = dir.path().join(LOG_FILE);
        let good_len = std::fs::metadata(&log_path).unwrap().len();
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_path)
                .unwrap();
            f.write_all(&1024u32.to_le_bytes()).unwrap();
            f.write_all(b"partial").unwrap();
        }
        assert!(std::fs::metadata(&log_path).unwrap().len() > good_len);

        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        let got = all_entries(&mut reopened).await;
        assert_eq!(got.len(), 3, "acked prefix survives the torn tail");
        assert_eq!(
            std::fs::metadata(&log_path).unwrap().len(),
            good_len,
            "torn bytes truncated away so future appends replay cleanly"
        );

        // Appends after the repair land and replay on the next open.
        append(&mut reopened, vec![entry(1, 4, "post-repair")]).await;
        drop(reopened);
        let mut again = DiskRaftStorage::open(dir.path()).unwrap();
        assert_eq!(all_entries(&mut again).await.len(), 4);
    }

    /// Snapshot file: applied state survives reopen via `raft.snapshot`, so
    /// a purge below the snapshot point is loss-free across restarts.
    #[tokio::test]
    async fn snapshot_and_applied_state_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let partition = PartitionKey::default_key();

        let mut store = DiskRaftStorage::open(dir.path()).unwrap();
        let entries: Vec<Entry<C>> = (1..=3)
            .map(|i| Entry {
                log_id: log_id(1, i),
                payload: EntryPayload::Normal(BasinRaftRequest {
                    project,
                    partition: partition.clone(),
                    payload: format!("sm-{i}").into_bytes(),
                }),
            })
            .collect();
        append(&mut store, entries.clone()).await;
        let responses = store.apply_to_state_machine(&entries).await.unwrap();
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[2].lsn, Lsn(3), "state machine assigns LSNs 1..=3");

        // Snapshot, then purge the whole applied prefix.
        let snap = store.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id, Some(log_id(1, 3)));
        store.purge_logs_upto(log_id(1, 3)).await.unwrap();
        drop(store);

        // Reopen: the log is empty but the snapshot file restores the
        // applied state — nothing was lost to the purge.
        let mut reopened = DiskRaftStorage::open(dir.path()).unwrap();
        assert!(all_entries(&mut reopened).await.is_empty());
        let (applied, _membership) = reopened.last_applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 3)));
        let part = reopened.partition_view(&project, &partition).await;
        assert_eq!(part.entries.len(), 3);
        assert_eq!(part.entries[2].payload, b"sm-3");
        let current = reopened.get_current_snapshot().await.unwrap();
        assert_eq!(
            current.expect("snapshot restored").meta.last_log_id,
            Some(log_id(1, 3))
        );
    }
}
