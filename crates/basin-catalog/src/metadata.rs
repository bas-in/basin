//! Table metadata — what a `load_table` call returns.

use std::collections::BTreeMap;
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

/// Phase 5.7 B1: secondary index declaration. Records `(name, column)` for a
/// per-tenant per-table B-tree-shaped index. The physical map (value-bytes →
/// (file, row_group, row)) is materialised lazily on first SELECT after a
/// write; this struct is purely the catalog declaration.
///
/// v0.1 supports single-column indexes only. v0.2 adds persistence to a
/// per-tenant index file in object storage and (optionally) multi-column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndex {
    pub name: String,
    pub column: String,
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

/// Per-column file-level statistics persisted in the catalog alongside a
/// [`DataFileRef`]. Mirrors the shape `basin-storage` extracts from a
/// freshly-written Parquet footer (and the same shape used by the
/// row-group pruning helper) so a catalog-stats prune is byte-equivalent
/// to a footer-stats prune at file granularity.
///
/// Phase 5.7 A4: file-level coalesced stats only. Row-group-level stats
/// stay in the Parquet footer for the deeper prune; B1 secondary indexes
/// will subsume the row-group case via a separate per-tenant index file.
///
/// All fields default-deserialise so historic catalog rows that predate
/// this struct come back as an empty `BTreeMap` on
/// [`DataFileRef::column_stats`] — preserving the pre-A4 read path
/// (which always fetched the footer).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_count: Option<u64>,
    /// Best-effort min/max as a bytes-or-string blob, encoded by Parquet's
    /// `Statistics::min_bytes` / `max_bytes`. Higher layers can decode
    /// according to the Arrow schema.
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
}

/// Reference to a single Parquet data file already written by `basin-storage`.
///
/// Phase 5.7 A4 lifts file-level [`ColumnStats`] up into the catalog so the
/// reader's planning path can prune whole files without fetching their
/// Parquet footers. The Iceberg `DataFile` struct also carries partition
/// tuples and split offsets; we'll grow into those as the engine needs them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileRef {
    /// Object key, full path under the bucket (no scheme, no leading slash).
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    /// File-level coalesced column stats (min / max / null-count per
    /// column, merged across row groups). Empty (the back-compat default)
    /// means "unknown — must fetch footer to prune". Populated at write
    /// commit time by the engine from the writer-emitted
    /// `basin_storage::DataFile::column_stats`.
    #[serde(default)]
    pub column_stats: BTreeMap<String, ColumnStats>,
}

/// Definition of a TimescaleDB-style continuous aggregate ("CV").
///
/// When `TableMetadata::continuous_aggregate` is `Some(_)`, the table is
/// the *materialised* result of running `query_sql` against `source_table`.
/// The CV refresher (`basin_cv::CvRefresher`) periodically re-executes
/// the query and atomically swaps in a fresh Parquet file via
/// [`crate::Catalog::replace_data_files`]. Reads of the CV go through the
/// engine's normal `SELECT` path — the CV is structurally a regular table
/// from the storage layer's point of view.
///
/// All fields default-deserialise so older rows that predate this struct
/// come back as `None` on `TableMetadata::continuous_aggregate`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CvDef {
    /// The base table the aggregate reads from. The CV refresher does not
    /// (yet) cross-check that the SQL only references this table; it's
    /// metadata for v0.2's incremental refresh planner.
    pub source_table: String,
    /// Full SQL text the refresher re-executes on each tick. Stored
    /// verbatim — the engine reparses at refresh time.
    pub query_sql: String,
    /// Refresh cadence. Ticks at intervals shorter than this are no-ops.
    pub refresh_interval_secs: u64,
    /// Time of the last successful refresh tick. `None` until the first
    /// refresh completes; used by [`crate::Catalog::set_continuous_aggregate`]
    /// to compute "is this CV due?". Stored as Unix-epoch milliseconds so
    /// the catalog row is a single `BIGINT`.
    #[serde(default)]
    pub last_refreshed_at_unix_ms: Option<i64>,
    /// Watermark for the latest source-table bucket the materialised data
    /// covers. `None` after registration, before the first refresh. v0.2's
    /// incremental refresh will scan only `source_table` rows above this
    /// watermark; the v0.1 refresher rebuilds the full materialisation
    /// every tick and therefore ignores this value, but it still updates
    /// it so the v0.2 upgrade is a backwards-compatible read of the same
    /// catalog payload.
    #[serde(default)]
    pub last_bucket_max_unix_ms: Option<i64>,
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
    /// When `Some(_)`, this table is a TimescaleDB-style continuous
    /// aggregate (CV) materialised from another table. The `basin-cv`
    /// crate's refresher consults this on each tick to decide which CVs
    /// are due. `None` (the default) is a regular table. Configured per-
    /// table via [`crate::Catalog::set_continuous_aggregate`]. See the
    /// `basin-cv` crate docs for the refresh / read-path semantics.
    pub continuous_aggregate: Option<CvDef>,
    /// Phase 5.7 B2: physically sort each newly-written batch by these
    /// columns before flushing to Parquet, so related rows live in the
    /// same row group / file. Combined with A3 bloom filters and A4
    /// catalog stats, point queries on the cluster columns prune to one
    /// file in the common case. Empty (the default) preserves the
    /// pre-B2 write path exactly. Configured per-table via
    /// [`crate::Catalog::set_cluster_columns`] or `CLUSTER BY (...)` on
    /// `CREATE TABLE`.
    pub cluster_columns: Vec<String>,
    /// Phase 6 multi-region scaffolding (ADR 0009). The region a tenant's
    /// writes for this table are *pinned* to — i.e. the home region for
    /// all writes; reads can come from any region (with the freshness
    /// bound from ADR 0004). `None` (the default) means "no pinning";
    /// every existing catalog row deserialises to `None` so back-compat
    /// is preserved. v0.1 *records* the value but does not yet route on
    /// it — the actual cross-region replication / forwarding is Phase 6
    /// work, see ADR 0009 for the locked-in shape.
    pub home_region: Option<String>,
    /// Phase 5.7 B1: secondary indexes declared on this table. Each entry
    /// records (index_name, column_name). The index is physically
    /// materialised lazily by the storage reader on first query; this
    /// catalog row is the authoritative declaration.
    pub indexes: Vec<SecondaryIndex>,
}

impl TableMetadata {
    /// Convenience: the snapshot record matching `current_snapshot`. The
    /// in-memory implementation always keeps these consistent; this helper
    /// is for callers who don't want to re-scan `snapshots` themselves.
    pub fn current(&self) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.id == self.current_snapshot)
    }
}
