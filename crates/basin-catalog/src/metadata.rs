//! Table metadata — what a `load_table` call returns.

use std::sync::Arc;

use arrow_schema::Schema;
use basin_common::{TableName, TenantId};
use serde::{Deserialize, Serialize};

use crate::snapshot::{Snapshot, SnapshotId};

/// Reference to a single Parquet data file already written by `basin-storage`.
///
/// Phase 1 keeps this minimal. The Iceberg `DataFile` struct also carries
/// per-column min/max stats, partition tuples, and split offsets; we'll grow
/// into those as the storage layer learns to emit them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileRef {
    /// Object key, full path under the bucket (no scheme, no leading slash).
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

/// Materialized view of a table at one point in time.
///
/// `current_snapshot` is the head of the history; `snapshots` is the full
/// chain (oldest first). The `Arc<Schema>` lets callers cheaply pass the
/// schema to Arrow / Parquet readers without re-cloning column metadata.
#[derive(Clone, Debug)]
pub struct TableMetadata {
    pub tenant: TenantId,
    pub table: TableName,
    pub schema: Arc<Schema>,
    pub current_snapshot: SnapshotId,
    pub snapshots: Vec<Snapshot>,
    /// Iceberg format version. We commit to v2 from day one because v1 has no
    /// row-level deletes and we'll need them for the SQL path in Phase 4.
    pub format_version: u8,
}

impl TableMetadata {
    /// Convenience: the snapshot record matching `current_snapshot`. The
    /// in-memory implementation always keeps these consistent; this helper
    /// is for callers who don't want to re-scan `snapshots` themselves.
    pub fn current(&self) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.id == self.current_snapshot)
    }
}
