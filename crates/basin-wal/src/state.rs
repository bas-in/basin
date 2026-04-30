//! Per-`(tenant, partition)` mutable state for the file-backed WAL.

use std::collections::BTreeMap;

use basin_common::{PartitionKey, TenantId};
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

/// All mutable state for a single `(tenant, partition)`.
///
/// The buffer holds entries that have been assigned an LSN but may not yet
/// have been flushed to object storage. The flush task drains it.
#[derive(Debug)]
pub(crate) struct PartitionState {
    pub tenant: TenantId,
    pub partition: PartitionKey,
    /// LSN to assign to the next [`crate::Wal::append`] call. Recovered on
    /// open from the maximum `last_lsn` across closed segments.
    pub next_lsn: Lsn,
    /// In-RAM entries that have been ack'd to callers but not yet uploaded
    /// to object storage. Kept in append order.
    pub buffer: Vec<EntryRecord>,
    /// Approximate byte size of `buffer` once framed; used to decide whether
    /// to trigger an early flush.
    pub buffer_bytes: u64,
    /// Closed segments in object storage, indexed by `first_lsn` for fast
    /// range scan. Two segments cannot share a `first_lsn` so a `BTreeMap`
    /// is sufficient.
    pub closed: BTreeMap<Lsn, ClosedSegment>,
}

impl PartitionState {
    pub fn new(tenant: TenantId, partition: PartitionKey) -> Self {
        Self {
            tenant,
            partition,
            next_lsn: Lsn(1),
            buffer: Vec::new(),
            buffer_bytes: 0,
            closed: BTreeMap::new(),
        }
    }

    /// Maximum LSN known to this partition: the last buffered entry, or the
    /// last closed segment's `last_lsn`, or `Lsn::ZERO` if neither exists.
    pub fn high_water(&self) -> Lsn {
        if let Some(last) = self.buffer.last() {
            return last.lsn;
        }
        self.closed
            .values()
            .next_back()
            .map(|c| c.last_lsn)
            .unwrap_or(Lsn::ZERO)
    }
}
