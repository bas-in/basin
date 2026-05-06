//! Table metadata — what a `load_table` call returns.

use std::sync::Arc;

use arrow_schema::Schema;
use basin_common::{TableName, TenantId};
use serde::{Deserialize, Serialize};

use crate::snapshot::{Snapshot, SnapshotId};

/// Which DML command a [`Policy`] applies to. Mirrors Postgres'
/// `CREATE POLICY ... FOR { ALL | SELECT | INSERT | UPDATE | DELETE }`.
///
/// `Default` is `All` so older deserialised payloads (which never had this
/// field) come back with the broadest applicability — matching Postgres'
/// default when `FOR` is omitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PolicyCommand {
    #[default]
    All,
    Select,
    Insert,
    Update,
    Delete,
}

/// One row-level-security policy attached to a table. The policy's
/// `using_expr` is stored as raw SQL text — we reparse it at plan-rewrite time
/// rather than serialise an AST node, because the AST type isn't `Serialize`-
/// stable across `sqlparser` versions and round-tripping through text is
/// equivalent for our use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    /// PostgreSQL roles the policy applies to. Empty list = PUBLIC (every
    /// authenticated principal). Role matching is by exact-string equality
    /// against `current_user` for v0.1 — group membership / inheritance is
    /// out of scope.
    #[serde(default)]
    pub applies_to_roles: Vec<String>,
    #[serde(default)]
    pub command: PolicyCommand,
    /// USING expression: filters rows visible to SELECT / UPDATE / DELETE.
    pub using_expr: String,
    /// WITH CHECK expression: enforced on INSERT and on UPDATE's *new* row
    /// values. Defaults to `using_expr` semantically when absent (Postgres
    /// behaviour); we materialise that fallback at evaluation time, not
    /// here, so the catalog round-trip is byte-stable.
    #[serde(default)]
    pub with_check_expr: Option<String>,
}

/// Within-tenant partitioning declared via `CREATE TABLE … PARTITION BY …`.
///
/// Catalog-side this is metadata only; the engine consults it at INSERT to
/// pick a per-row [`basin_common::PartitionKey`] and at SELECT to prune
/// data files. New variants are intentionally additive — every existing
/// catalog payload deserialises to [`PartitionSpec::Unpartitioned`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionSpec {
    /// No partitioning. Files land under `…/data/_default/yyyy/mm/dd/`.
    /// This is the default when a `CREATE TABLE` omits `PARTITION BY`.
    #[default]
    Unpartitioned,
    /// `PARTITION BY RANGE (col)` on a `TIMESTAMPTZ` (or `Int64`-as-epoch)
    /// column. Each row's partition key is `year=YYYY/month=MM` derived
    /// from the column's value.
    RangeMonthly { column: String },
}

impl PartitionSpec {
    pub fn partition_column(&self) -> Option<&str> {
        match self {
            PartitionSpec::Unpartitioned => None,
            PartitionSpec::RangeMonthly { column } => Some(column.as_str()),
        }
    }

    pub fn is_partitioned(&self) -> bool {
        !matches!(self, PartitionSpec::Unpartitioned)
    }
}

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
    /// Partitioning declared by `CREATE TABLE … PARTITION BY …`. Defaults
    /// to [`PartitionSpec::Unpartitioned`] for backwards compatibility.
    pub partition_spec: PartitionSpec,
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` flips this to true. When
    /// false, the engine takes the no-RLS fast path: zero plan rewriting,
    /// zero per-query overhead. Defaults to false for back-compat.
    pub rls_enabled: bool,
    /// Policies declared by `CREATE POLICY ... ON <this table>`. Empty when
    /// no policy has been declared. RLS still requires `rls_enabled = true`
    /// for the policies here to take effect — Postgres-equivalent behaviour.
    pub policies: Vec<Policy>,
    /// Files older than this (measured against `cold_age_column`) are
    /// migrated to cold tier by the compactor's tiering sweep. `None`
    /// disables the policy — the default for back-compat. Stored as
    /// seconds rather than `Duration` so the catalog row is a single
    /// `BIGINT` column.
    pub cold_after_seconds: Option<u64>,
    /// Column the tiering compactor consults to decide whether a file is
    /// cold. Must be a `TIMESTAMPTZ` (or `Int64`-as-epoch-seconds)
    /// column whose Parquet column statistics carry a `max` value. When
    /// `None` the policy falls back to the partition column (if any);
    /// if no partition column exists either, the policy is a no-op.
    pub cold_age_column: Option<String>,
    /// Columns for which the writer should emit native Parquet bloom
    /// filters in each row group. Empty (the default) preserves the
    /// pre-bloom behaviour exactly: no bloom filter sections are written
    /// to the Parquet footer and the reader's pruning remains driven by
    /// min/max statistics alone. Configured per-table via
    /// [`crate::Catalog::set_bloom_filter_columns`]; see
    /// `tests/integration/tests/viability_bloom_filter_pruning.rs` for
    /// the point-query speedup this targets.
    pub bloom_filter_columns: Vec<String>,
    /// Override for the writer's `max_row_group_size` on this table.
    /// `None` (the default) means use the writer's global default
    /// (currently 65,536 rows). Smaller values trade scan throughput for
    /// finer-grained pruning — the typical use case is point-query-heavy
    /// tables paired with bloom filters, where ≤4k rows per group lets a
    /// single point query land on at most one row group instead of
    /// scanning the whole table. Configured per-table via
    /// [`crate::Catalog::set_row_group_rows`]. See
    /// `tests/integration/tests/viability_row_group_sizing.rs`.
    pub row_group_rows: Option<usize>,
}

impl TableMetadata {
    /// Convenience: the snapshot record matching `current_snapshot`. The
    /// in-memory implementation always keeps these consistent; this helper
    /// is for callers who don't want to re-scan `snapshots` themselves.
    pub fn current(&self) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.id == self.current_snapshot)
    }
}
