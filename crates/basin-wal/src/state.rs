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
    /// Highest LSN known durable on the backing store: every record with
    /// `lsn <= durable_lsn` has been PUT to object storage (and, when a
    /// synchronous-commit waiter requested it on the local backend, fsync'd
    /// via the [`crate::FsyncOnPut`] wrapper). Advanced only contiguously —
    /// see `pending_durable`.
    pub durable_lsn: Lsn,
    /// Watch channel that publishes `durable_lsn` advances. Synchronous
    /// (group-commit) appenders subscribe and wait until the published value
    /// reaches their own LSN; one segment PUT wakes every waiter whose LSN
    /// it covers.
    pub durable_tx: tokio::sync::watch::Sender<Lsn>,
    /// Highest LSN any synchronous-commit appender has asked to be made
    /// durable. The flusher fsyncs a drained segment iff this is >= the
    /// segment's `first_lsn` — i.e. some waiter's record may be inside it.
    /// Plain async appends never raise it, so their segments are never
    /// fsync'd (async-path behaviour unchanged).
    pub sync_requested_up_to: Lsn,
    /// Segment LSN ranges (`first_lsn -> last_lsn`) whose PUT (+fsync, if
    /// requested) completed but whose range is not yet contiguous with
    /// `durable_lsn`. Guards against two in-flight flushes completing out of
    /// order: a higher segment finishing first must not publish a
    /// `durable_lsn` that covers a lower, still-in-flight (and possibly
    /// failing) segment.
    pub pending_durable: BTreeMap<Lsn, Lsn>,
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
        let (durable_tx, _durable_rx) = tokio::sync::watch::channel(Lsn::ZERO);
        Self {
            project,
            partition,
            next_lsn: Lsn(1),
            buffer: Vec::new(),
            buffer_bytes: 0,
            closed: BTreeMap::new(),
            durable_lsn: Lsn::ZERO,
            durable_tx,
            sync_requested_up_to: Lsn::ZERO,
            pending_durable: BTreeMap::new(),
            fence_epoch: 0,
        }
    }

    /// Record that the LSN range `[first, last]` is now durable on the
    /// backing store and advance `durable_lsn` over every contiguous
    /// completed range, publishing the new value to waiters. Ranges that
    /// complete out of order park in `pending_durable` until the gap below
    /// them closes.
    pub fn mark_range_durable(&mut self, first: Lsn, last: Lsn) {
        self.pending_durable.insert(first, last);
        while let Some((&f, &l)) = self.pending_durable.first_key_value() {
            if f.0 <= self.durable_lsn.0 + 1 {
                if l > self.durable_lsn {
                    self.durable_lsn = l;
                }
                self.pending_durable.remove(&f);
            } else {
                break;
            }
        }
        // `send_replace` never fails (works with zero receivers), unlike
        // `send`, which errors once the last receiver drops.
        self.durable_tx.send_replace(self.durable_lsn);
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
