//! Per-`(project, partition)` mutable state for the file-backed WAL.

use std::collections::BTreeMap;

use basin_common::{PartitionKey, ProjectId};
use object_store::path::Path as ObjectPath;
use ulid::Ulid;

use crate::segment::EntryRecord;
use crate::Lsn;

/// LSN range covered by one closed segment, plus its object key. Sorted by
/// `first_lsn` for ordered replay.
#[derive(Debug, Clone)]
pub(crate) struct ClosedSegment {
    pub path: ObjectPath,
    pub first_lsn: Lsn,
    pub last_lsn: Lsn,
    /// ULID embedded in the segment file name. Carried through recovery for
    /// debugging and future compaction joins; not consulted by the hot path.
    #[allow(dead_code)]
    pub segment_id: Ulid,
}

/// An in-RAM buffer record. Data entries and transaction markers are kept in
/// the same ordered buffer so they are serialised to disk in the correct
/// interleaved order.
#[derive(Debug, Clone)]
pub(crate) enum BufferRecord {
    Entry(EntryRecord),
    TxBegin { lsn: Lsn, tx_id: u64 },
    TxRollback { lsn: Lsn, tx_id: u64 },
    /// ADR 0020 §6 — explicit commit marker. See [`crate::segment::SegmentRecord::TxCommit`].
    TxCommit { lsn: Lsn, tx_id: u64 },
    /// Phase 6.X.C — voluntary lease-handoff marker. Replay treats it as
    /// informational; the marker exists so the new owner / audit logs see
    /// the boundary.
    Handoff {
        lsn: Lsn,
        to_holder: String,
        at_epoch: i64,
    },
}

impl BufferRecord {
    pub fn lsn(&self) -> Lsn {
        match self {
            BufferRecord::Entry(e) => e.lsn,
            BufferRecord::TxBegin { lsn, .. }
            | BufferRecord::TxRollback { lsn, .. }
            | BufferRecord::TxCommit { lsn, .. }
            | BufferRecord::Handoff { lsn, .. } => *lsn,
        }
    }
}

/// All mutable state for a single `(project, partition)`.
///
/// The buffer holds entries (and transaction markers) that have been assigned
/// an LSN but may not yet have been flushed to object storage. The flush task
/// drains it.
#[derive(Debug)]
pub(crate) struct PartitionState {
    pub project: ProjectId,
    pub partition: PartitionKey,
    /// LSN to assign to the next [`crate::Wal::append`] call. Recovered on
    /// open from the maximum `last_lsn` across closed segments.
    pub next_lsn: Lsn,
    /// In-RAM records that have been ack'd to callers but not yet uploaded
    /// to object storage. Kept in append order. Holds both data entries and
    /// transaction markers.
    pub buffer: Vec<BufferRecord>,
    /// Approximate byte size of `buffer` once framed; used to decide whether
    /// to trigger an early flush.
    pub buffer_bytes: u64,
    /// Closed segments in object storage, indexed by `first_lsn` for fast
    /// range scan. Two segments cannot share a `first_lsn` so a `BTreeMap`
    /// is sufficient.
    pub closed: BTreeMap<Lsn, ClosedSegment>,
    /// Phase 6.X.A — highest lease epoch ever observed on a fenced append for
    /// this partition (ADR 0023). Monotonic: once a higher-epoch holder
    /// appends, any later append carrying a strictly lower epoch is rejected
    /// (the loser of a dual-leaseholder window). `0` is the back-compat
    /// no-lease sentinel — single-replica / no-lease appends pass `0`/`None`,
    /// never raise the fence, and are accepted unconditionally.
    pub fence_epoch: i64,
}

impl PartitionState {
    pub fn new(project: ProjectId, partition: PartitionKey) -> Self {
        Self {
            project,
            partition,
            next_lsn: Lsn(1),
            buffer: Vec::new(),
            buffer_bytes: 0,
            closed: BTreeMap::new(),
            fence_epoch: 0,
        }
    }

    /// Maximum LSN known to this partition: the last buffered record's LSN, or
    /// the last closed segment's `last_lsn`, or `Lsn::ZERO` if neither exists.
    pub fn high_water(&self) -> Lsn {
        if let Some(last) = self.buffer.last() {
            return last.lsn();
        }
        self.closed
            .values()
            .next_back()
            .map(|c| c.last_lsn)
            .unwrap_or(Lsn::ZERO)
    }
}
