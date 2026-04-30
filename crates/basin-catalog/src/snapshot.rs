//! Snapshot types — the unit of atomic table change.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::metadata::DataFileRef;

/// Monotonic snapshot identifier, scoped to one `(tenant, table)`.
///
/// We use a `u64` rather than the random 64-bit ints Iceberg uses on the wire
/// because in-memory we want strict ordering for `list_snapshots` and the
/// optimistic-concurrency check. When we wire this to a real REST catalog in
/// Phase 2/3 we'll translate to/from the catalog's id space at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub u64);

impl SnapshotId {
    pub const GENESIS: SnapshotId = SnapshotId(0);

    pub fn next(self) -> SnapshotId {
        SnapshotId(self.0 + 1)
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Iceberg-flavored summary of what a single snapshot did.
///
/// Phase 1 only emits `Append` (no deletes, no overwrites, no compaction
/// commits); the enum is shaped this way so additional ops slot in without
/// breaking the public type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub operation: SnapshotOperation,
    pub added_files: u64,
    pub added_rows: u64,
    pub added_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotOperation {
    /// Initial empty snapshot at table creation. Iceberg uses `append` with
    /// zero files for this; we keep it distinct so the integration test can
    /// assert on it.
    Genesis,
    Append,
}

/// One snapshot in a table's history. Snapshots are append-only; older
/// snapshots are retained until an explicit retention policy expires them
/// (not in scope for Phase 1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub committed_at: DateTime<Utc>,
    pub data_files: Vec<DataFileRef>,
    pub summary: SnapshotSummary,
}
