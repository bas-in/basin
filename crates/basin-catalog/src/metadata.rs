//! Table metadata — what a `load_table` call returns.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::Schema;
use basin_common::{ProjectId, TableName};
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

/// Phase 5.7 B1: secondary index declaration. Records `(name, columns)` for a
/// per-project per-table B-tree-shaped index. The physical map (value-bytes →
/// (file, row_group, row)) is materialised lazily on first SELECT after a
/// write; this struct is purely the catalog declaration.
///
/// Multi-column indexes are accepted at the SQL surface but materialised
/// as metadata-only in v0.1 — queries still take a full scan. v0.2 wires
/// the declaration through to basin-storage's secondary-index file format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndex {
    pub name: String,
    /// Columns the index keys on, in declaration order. Single-column indexes
    /// land here as a one-element vec.
    pub columns: Vec<String>,
    /// Access method (index type). The default is `"btree"` (standard B-tree).
    /// Phase 5.19.B adds `"gin"` for JSONB GIN indexes.
    #[serde(default = "default_access_method")]
    pub access_method: String,
    /// Operator class for GIN indexes. For `USING gin`, this is either
    /// `"jsonb_ops"` (default, key+value containment) or `"jsonb_path_ops"`
    /// (path-hash only, more selective for `@>`). `None` for B-tree indexes
    /// (where the operator class is implicit / unrecorded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opclass: Option<String>,
}

fn default_access_method() -> String {
    "btree".to_string()
}

/// One UNIQUE constraint attached to a table. Records the column list
/// the engine enforces uniqueness over. Column-level `UNIQUE` derives a
/// synthetic name (`<table>_<col>_key`, PG convention); table-level
/// `UNIQUE (a, b)` uses the user-supplied `CONSTRAINT <name>` if given,
/// otherwise `<table>_<col1>_<col2>_key`.
///
/// v0.1 enforces via a full-table scan on every INSERT / UPDATE — same
/// cost shape as PRIMARY KEY enforcement. v0.2 will use the secondary
/// index materialised from [`SecondaryIndex`] once that machinery lands
/// in basin-storage.
///
/// NULL handling: PG's default `UNIQUE` treats NULL values as
/// distinct — any number of rows may have NULL in a UNIQUE column.
/// v0.1 matches this: rows with NULL in *any* UNIQUE column are
/// excluded from the uniqueness check. `NULLS NOT DISTINCT` (PG 15+)
/// is out of scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueConstraint {
    /// Auto-named `<table>_<col>_key` (column-level) or
    /// `<table>_<col1>_<col2>_..._key` (table-level), unless the user
    /// wrote `CONSTRAINT <name> UNIQUE (...)`.
    pub name: String,
    /// Columns the constraint enforces uniqueness across, in
    /// declaration order.
    pub columns: Vec<String>,
}

/// One CHECK constraint attached to a table. The predicate is stored as
/// raw SQL text — the engine reparses it at write time and evaluates it
/// against the row's RecordBatch via DataFusion (same trick used by
/// `GENERATED ALWAYS AS` columns and `CREATE DOMAIN ... CHECK`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    /// Auto-named `<table>_<col>_check` (column-level) or
    /// `<table>_check_<n>` (table-level), unless the user wrote
    /// `CONSTRAINT <name> CHECK (...)`.
    pub name: String,
    /// Predicate text; e.g. `"price > 0"` (no surrounding parens).
    pub predicate: String,
}

/// Referential action for a foreign key. v0.1 supports `NoAction` (the
/// default — reject DELETE / UPDATE of a referenced row when referring
/// rows exist) and `Cascade` (delete / update the referencing rows).
/// `SetNull`, `SetDefault`, `Restrict` are out of scope for v0.1 and
/// rejected at CREATE time as `feature_not_supported`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RefAction {
    #[default]
    NoAction,
    Cascade,
}

/// One FOREIGN KEY constraint attached to a table. v0.1 enforces
/// referential integrity on INSERT / UPDATE of referencing columns
/// (referenced row must exist) and on DELETE / UPDATE of referenced
/// columns (NO ACTION rejects, CASCADE propagates). Single-shard,
/// single-project only — cross-project FKs are rejected at CREATE.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    /// Auto-named `<table>_<col>_fkey` unless user wrote
    /// `CONSTRAINT <name> FOREIGN KEY (...) REFERENCES ...`.
    pub name: String,
    /// Local columns (in order) that participate in the FK.
    pub columns: Vec<String>,
    /// Bare table name of the referenced table (same project only).
    pub ref_table: String,
    /// Referenced columns on `ref_table`. Must be the PK columns of that
    /// table in v0.1 (UNIQUE-constraint-only references are deferred).
    pub ref_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: RefAction,
    #[serde(default)]
    pub on_update: RefAction,
}

/// Within-project partitioning declared via `CREATE TABLE … PARTITION BY …`.
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
/// will subsume the row-group case via a separate per-project index file.
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
    /// Best-effort exact column SUM, encoded little-endian by the writer
    /// (8-byte LE `i64` for `Int64`, 8-byte LE `f64` for `Float64`). `None`
    /// — the back-compat default for every catalog row written before this
    /// field existed (and until the writer/Vortex side starts populating
    /// it) — means "unknown". `#[serde(default)]` so historic rows
    /// deserialise to `None`. The metadata-only aggregate fast path only
    /// answers `SUM(col)` from the catalog when EVERY live file carries
    /// `Some(_)` here; any `None` falls the whole query back to a full
    /// scan, so the result stays correct while population lands later.
    #[serde(default)]
    pub sum_bytes: Option<Vec<u8>>,
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
    /// Phase 5.14.A — per-column bloom filters for `point_eq` miss-path
    /// pruning.  Keyed by column name; the value is the column's bloom
    /// serialised as raw bytes (format owned by `basin-storage`'s bloom
    /// helper).  Empty (the back-compat default) means "no bloom recorded
    /// — fall through to existing min/max pruning".  Populated at write
    /// commit time for columns named in `TableMetadata::global_sort_order`
    /// only; other tables and other columns carry an empty map.
    ///
    /// Probed in `fast_select.rs::execute_simple_select` before the Vortex
    /// file is opened: a bloom that says "definitely not present" lets us
    /// skip the file entirely.  False-positives fall through to the
    /// existing open + decode path (no correctness risk).
    #[serde(default)]
    pub bloom_filters: BTreeMap<String, Vec<u8>>,
    /// Phase 5.14.B1 — per-column HyperLogLog sketches.
    #[serde(default)]
    pub hll_sketches: BTreeMap<String, Vec<u8>>,
    /// Phase 5.14.B1 — per-column t-digest sketches.
    #[serde(default)]
    pub tdigest_sketches: BTreeMap<String, Vec<u8>>,
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

/// Per-table on-disk data-file format (#161). `Parquet` is the default and
/// the value every pre-existing catalog row deserialises to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TableFileFormat {
    Parquet,
    #[default]
    Vortex,
}

/// ADR 0027 Phase 4 — one promoted JSONB top-level path.
///
/// `source_col` is the JSONB column name (e.g. `"payload"`), `json_key` is
/// the top-level key to promote (e.g. `"category"`).  The physical shadow
/// column name is `__promoted$<source_col>$<json_key>` (a `Utf8` column).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotedJsonbPath {
    /// The JSONB column that owns the document (e.g. `"payload"`).
    pub source_col: String,
    /// Top-level JSON object key to extract (e.g. `"category"`).
    pub json_key: String,
}

impl PromotedJsonbPath {
    /// Canonical shadow-column name: `__promoted$<source_col>$<json_key>`.
    ///
    /// The `__promoted$` prefix is long enough to be collision-free with any
    /// user-defined column (Postgres disallows leading `$` in unquoted idents;
    /// the underscore prefix also escapes accidental join on `*`).
    pub fn shadow_col_name(&self) -> String {
        format!("__promoted${}${}", self.source_col, self.json_key)
    }
}

/// Materialized view of a table at one point in time.
///
/// `current_snapshot` is the head of the history; `snapshots` is the full
/// chain (oldest first). The `Arc<Schema>` lets callers cheaply pass the
/// schema to Arrow / Parquet readers without re-cloning column metadata.
#[derive(Clone, Debug)]
pub struct TableMetadata {
    pub project: ProjectId,
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
    /// On-disk data-file format for this table (#161). `Parquet` (the
    /// default) preserves Iceberg / Athena / Spark / DuckDB read-compat;
    /// `Vortex` is the opt-in columnar format selected at CREATE TABLE via
    /// `WITH (basin.file_format = 'vortex')`. `#[serde(default)]` means
    /// every pre-existing catalog row deserialises to `Parquet`, so this
    /// is fully back-compatible. A table is single-format (mixed
    /// Parquet+Vortex within one table is a deferred feature); the write
    /// path picks the format from here, the read path from each file's
    /// extension.
    pub file_format: TableFileFormat,
    /// Per-table Vortex chunk / Parquet row-group size in rows.
    ///
    /// For Vortex tables this maps to `WriteStrategyBuilder::with_row_block_size`.
    /// For Parquet tables this maps to `WriterProperties::max_row_group_size` —
    /// the same as `row_group_rows` but controlled via the `WITH` clause at
    /// CREATE TABLE time rather than a separate `ALTER TABLE` command.
    ///
    /// `None` (the default) keeps the writer's built-in default for each
    /// format. Valid values are powers of two in [256, 65536]; the engine
    /// rejects values outside this range at DDL time. Pre-existing catalog
    /// rows without this column deserialise to `None` via the Postgres
    /// `ADD COLUMN IF NOT EXISTS` migration and the `Option` default.
    pub row_block_size: Option<u32>,
    /// Phase 6 multi-region scaffolding (ADR 0009). The region a project's
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
    /// PRIMARY KEY column list (empty when no PK declared). Order is
    /// declaration order. Single-column or composite. PK columns are
    /// always NOT NULL — enforced at CREATE TABLE.
    ///
    /// Enforcement: on INSERT (and on UPDATE that touches a PK column)
    /// the engine scans the existing table for a row whose PK tuple
    /// matches and rejects the write with SQLSTATE 23505 if found.
    /// v0.1 uses a full-table scan; Phase 5.7 B1 secondary indexes
    /// will speed this up later.
    pub pk_columns: Vec<String>,
    /// CHECK constraints attached to this table. Evaluated on every
    /// INSERT row and on every UPDATE row that touches any column the
    /// predicate references. Predicate violations surface as SQLSTATE
    /// 23514.
    pub check_constraints: Vec<CheckConstraint>,
    /// FOREIGN KEY constraints declared on this table. Enforced on
    /// INSERT / UPDATE (referenced row must exist) and on DELETE /
    /// UPDATE of the *referenced* table (NO ACTION rejects when
    /// referring rows exist; CASCADE propagates).
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// UNIQUE constraints declared on this table (column-level and
    /// table-level both land here). Enforced via full-table scan on
    /// every INSERT / UPDATE — same cost shape as PRIMARY KEY in v0.1.
    /// Empty when no UNIQUE constraint has been declared. v0.2 will
    /// use the secondary index file format once basin-storage ships
    /// it; today the scan is honest about its O(N) cost.
    pub unique_constraints: Vec<UniqueConstraint>,
    /// User-asserted global sort order declared via `WITH (basin.sort_by
    /// = 'col1,col2')` at CREATE TABLE time.
    ///
    /// **Semantics (user assertion, not auto-detected):** when `Some(cols)`,
    /// every data file is sorted ascending on `cols` AND the column ranges
    /// across files are non-overlapping (i.e. `SortPreservingMergeExec`
    /// across files is correct without a full re-sort). This invariant
    /// holds only if writes are strictly monotone-append on those columns.
    /// An UPDATE that touches the sort key or an INSERT with out-of-order
    /// keys invalidates the invariant — the engine does *not* enforce this
    /// at write time in v0.1; the user accepts responsibility by setting
    /// `basin.sort_by`.
    ///
    /// When `Some`, the session wires `ListingOptions::with_file_sort_order`
    /// so DataFusion can propagate the ordering through the plan and avoid
    /// inserting a `SortExec` before `WindowAggExec` / `SortMergeJoin`.
    /// `None` is the default for every table that predates this feature.
    pub global_sort_order: Option<Vec<String>>,
    /// Phase 5.14.D2: when `Some(true)`, the compactor may override
    /// `cluster_columns` with the observed `QueryHistory::top_pattern` when
    /// the threshold is met.  When `None` (the default, back-compat) or
    /// `Some(false)`, user-declared `CLUSTER BY` always wins.
    pub adaptive_sort_override: Option<bool>,
    /// Phase 6 refcount GC: data-file paths collected at `rollback_to_snapshot`
    /// time that were written by now-discarded snapshots.  These paths no longer
    /// appear in any snapshot record (those records were pruned by the rollback)
    /// but they are still candidates for physical deletion — they become orphaned
    /// if no other table (e.g. a fork) holds a live reference to them.
    ///
    /// The `gc_orphaned_files` sweep includes this list in the "universe" it
    /// checks against the cross-table live set.  Empty by default (back-compat:
    /// pre-GC catalog rows simply have no accumulated orphan list).
    pub gc_orphan_paths: Vec<String>,
    /// ADR 0027 Phase 4 — promoted JSONB shadow columns.
    ///
    /// Each entry `(source_col, json_key)` declares that the top-level path
    /// `<source_col>->>'<json_key>'` is materialised at write time as a real
    /// `Utf8` column named `__promoted$<source_col>$<json_key>`.  The planner
    /// rewrites matching `json_get_text(source_col, 'json_key')` calls to a
    /// direct column projection, bypassing the per-row UDF dispatch entirely.
    ///
    /// Populated via `Catalog::promote_jsonb_path` (test API; production
    /// auto-promotion driven by `QueryHistory` is a follow-up task).
    /// Empty (the default) preserves byte-identical behaviour for every
    /// existing table.
    pub promoted_jsonb_paths: Vec<PromotedJsonbPath>,
}

// ---------------------------------------------------------------------------
// T-048 — per-project BYO-bucket config
// ---------------------------------------------------------------------------

/// Stored inside [`ProjectMetadata::byo_bucket`] when a project has configured
/// its own object-storage bucket. The `secret_access_key_enc` field carries
/// the secret key envelope-encrypted by the cloud layer's KMS; the OSS engine
/// persists the bytes verbatim and never inspects them. The `force_path_style`
/// flag enables path-style request routing required by MinIO and some S3-
/// compatible stores that don't support virtual-hosted-style.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    /// Full HTTPS endpoint URL (e.g. `"https://s3.amazonaws.com"` or
    /// `"https://fly.storage.tigris.dev"`).
    pub endpoint: String,
    /// Customer's bucket name.
    pub bucket: String,
    /// Region string. `"auto"` for Tigris/R2; a real AWS region for S3.
    pub region: String,
    /// AWS-style access key identifier (not secret).
    pub access_key_id: String,
    /// Secret access key, envelope-encrypted by the cloud layer's KMS.
    /// The OSS engine persists the ciphertext verbatim and never decrypts it.
    pub secret_access_key_enc: Vec<u8>,
    /// When `true`, use path-style URLs. Required for MinIO and some
    /// custom S3-compatible providers.
    pub force_path_style: bool,
}

/// Per-project settings shared across all tables owned by the project.
///
/// Every field is optional with `#[serde(default)]` + `skip_serializing_if`,
/// so a catalog row that predates a given field deserialises with that field
/// at its default — preserving the prior behaviour. `byo_bucket` gates
/// customer-bucket routing (T-048); `home_region` pins the project to a
/// single region for multi-region deployments (ADR 0009).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectMetadata {
    /// When `Some`, reads and writes for this project are routed to the
    /// customer's own S3-compatible bucket. `None` (the default) uses
    /// Basin's shared bucket — back-compat with all existing projects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byo_bucket: Option<S3Config>,

    /// Multi-region (ADR 0009): the region this project is pinned to. All
    /// WRITES, and all strongly-consistent (`basin.read_tier = 'primary'`)
    /// READS, must execute in this region; submitted elsewhere they raise
    /// `BasinError::WrongRegion` (unless a `WriteForwarder` is registered).
    /// `'lagging'` reads serve flushed, S3-CRR-replicated data from any region.
    ///
    /// `None` (the default, and the value any pre-multi-region catalog row
    /// deserialises to) means **unpinned**: the project is treated as homed
    /// in whatever region the serving process runs in, so single-region
    /// deployments and legacy projects never reject. An operator opts a
    /// project into pinning by setting this to a concrete region string that
    /// matches some deployment's `BASIN_REGION`. Stored verbatim; comparison
    /// against `local_region()` is exact-string (see
    /// `basin_common::is_home_region`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_region: Option<String>,
}

impl TableMetadata {
    /// Convenience: the snapshot record matching `current_snapshot`. The
    /// in-memory implementation always keeps these consistent; this helper
    /// is for callers who don't want to re-scan `snapshots` themselves.
    pub fn current(&self) -> Option<&Snapshot> {
        self.snapshots
            .iter()
            .find(|s| s.id == self.current_snapshot)
    }

    /// Compute the **complete live data-file set** at `current_snapshot` by
    /// replaying the snapshot chain from the genesis snapshot forward.
    ///
    /// Each [`SnapshotOperation::Append`] adds its `data_files` to the live
    /// set; each [`SnapshotOperation::Replace`] removes the paths recorded in
    /// `removed_paths` and adds the replacement `data_files`. Genesis snapshots
    /// contribute nothing (they have no files by construction).
    ///
    /// This is the correct input for the engine's read path: after a
    /// `rollback_to_snapshot` call the catalog prunes every snapshot with
    /// `id > snapshot_id`, so `live_data_files()` on the returned
    /// [`TableMetadata`] yields exactly the files that were live at the
    /// rollback target — post-rollback files never appear.
    ///
    /// Callers that scan the object-store directory directly (e.g.
    /// DataFusion's `ListingTable` with a directory URL) see *all* physical
    /// files, including those logically removed by rollback. Using this
    /// method instead makes the read path catalog-driven and correct.
    pub fn live_data_files(&self) -> Vec<DataFileRef> {
        self.live_data_files_at(self.current_snapshot)
    }

    /// Like [`live_data_files`](Self::live_data_files) but reconstructs the
    /// live file set as it was at an arbitrary historical `snapshot_id`,
    /// rather than at `current_snapshot`.
    ///
    /// This is the read-side primitive for transaction snapshot-stable reads
    /// (REPEATABLE READ): a transaction pins the snapshot id it observed at
    /// `BEGIN` (or at first touch of a table) and every in-transaction read
    /// reconstructs the file set at that pinned id, so writes committed by
    /// other sessions after `BEGIN` — which advance the catalog head to a
    /// *higher* snapshot id — are invisible to this transaction.
    ///
    /// Correctness relies on the catalog retaining the full snapshot chain
    /// (snapshots are append-only; only `rollback_to_snapshot` prunes ids
    /// `> target`). Both `InMemoryCatalog` and `PostgresCatalog` load the
    /// complete chain into [`TableMetadata::snapshots`], so this method
    /// reconstructs any retained historical snapshot exactly.
    ///
    /// Snapshots with `id > snapshot_id` are ignored — identical to how
    /// [`live_data_files`](Self::live_data_files) ignores ids beyond
    /// `current_snapshot`.
    pub fn live_data_files_at(&self, snapshot_id: SnapshotId) -> Vec<DataFileRef> {
        use std::collections::HashMap;
        // Walk snapshots in id order (genesis → target) and maintain the
        // live set as a path → DataFileRef map so we can apply removes in O(1).
        let mut live: HashMap<String, DataFileRef> = HashMap::new();
        let mut ordered: Vec<&Snapshot> = self.snapshots.iter().collect();
        ordered.sort_by_key(|s| s.id);
        for snap in ordered {
            if snap.id > snapshot_id {
                // Snapshots beyond the requested point-in-time are not part of
                // the live set. For the `live_data_files()` (current head)
                // caller this also defends against metadata loaded mid-rollback;
                // for the snapshot-pinned caller it is the load-bearing cut that
                // hides writes committed after the transaction's pinned id.
                break;
            }
            // Remove any paths this snapshot replaced (Replace operation).
            for path in &snap.removed_paths {
                live.remove(path);
            }
            // Add the files written by this snapshot.
            for f in &snap.data_files {
                live.insert(f.path.clone(), f.clone());
            }
        }
        live.into_values().collect()
    }

    #[cfg(test)]
    fn empty_for_test() -> TableMetadata {
        TableMetadata {
            project: ProjectId::new(),
            table: TableName::new("t").unwrap(),
            schema: Arc::new(Schema::empty()),
            current_snapshot: SnapshotId::GENESIS,
            snapshots: Vec::new(),
            format_version: 2,
            partition_spec: PartitionSpec::Unpartitioned,
            rls_enabled: false,
            policies: Vec::new(),
            cold_after_seconds: None,
            cold_age_column: None,
            bloom_filter_columns: Vec::new(),
            row_group_rows: None,
            continuous_aggregate: None,
            cluster_columns: Vec::new(),
            file_format: TableFileFormat::default(),
            row_block_size: None,
            home_region: None,
            indexes: Vec::new(),
            pk_columns: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            global_sort_order: None,
            adaptive_sort_override: None,
            gc_orphan_paths: Vec::new(),
            promoted_jsonb_paths: Vec::new(),
        }
    }

    /// Point-query scaling (#204): the cluster/sort columns a writer should
    /// physically sort cold files by, defaulting to the single-column primary
    /// key when the table declares no explicit clustering.
    ///
    /// When a cold file is sorted by the PK, the per-row-group / per-zone
    /// `[min, max]` ranges become disjoint, so the reader's existing
    /// min/max pruning (Parquet row-group stats + page index; Vortex native
    /// zone-maps) isolates the ~1 group/zone holding `WHERE pk = $1` instead
    /// of scanning the whole file as the table grows.
    ///
    /// Policy (deliberately conservative):
    ///  * Only kicks in when BOTH `cluster_columns` AND `global_sort_order`
    ///    are empty — an explicit `WITH (basin.cluster_by=…)` /
    ///    `basin.sort_by=…` always wins and is returned by the caller instead.
    ///  * Only single-column primary keys (a composite PK doesn't give a
    ///    single prunable order for `pk = $1`).
    ///  * Only PK column types that the read path actually prunes on. The
    ///    Vortex pushdown gate (`vortex_filter_expr`) accepts
    ///    Int64/Float64/Boolean/UInt64; Parquet stats cover the full integer
    ///    and float width family. We type-gate to integer / float / bool and
    ///    skip text/timestamp/decimal/uuid (a later task can widen).
    ///
    /// Returns an empty vec when the policy does not apply, so callers can
    /// pass the result straight through to `WriteOptions.cluster_columns`.
    pub fn default_cluster_cols(&self) -> Vec<String> {
        use arrow_schema::DataType;

        // Explicit clustering / sort declarations always take precedence.
        if !self.cluster_columns.is_empty() {
            return Vec::new();
        }
        if self
            .global_sort_order
            .as_ref()
            .is_some_and(|s| !s.is_empty())
        {
            return Vec::new();
        }
        // Single-column PK only.
        let [pk] = self.pk_columns.as_slice() else {
            return Vec::new();
        };
        // Type-gate to what the read path prunes on.
        let prunable = self
            .schema
            .field_with_name(pk)
            .ok()
            .map(|f| {
                matches!(
                    f.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Boolean
                )
            })
            .unwrap_or(false);
        if prunable {
            vec![pk.clone()]
        } else {
            Vec::new()
        }
    }
}


#[cfg(test)]
mod default_cluster_cols_tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    fn with_pk(pk: &[&str], fields: &[(&str, DataType)]) -> TableMetadata {
        let mut m = TableMetadata::empty_for_test();
        m.schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, dt)| Field::new(*n, dt.clone(), false))
                .collect::<Vec<_>>(),
        ));
        m.pk_columns = pk.iter().map(|s| s.to_string()).collect();
        m
    }

    #[test]
    fn single_int_pk_drives_default() {
        let m = with_pk(&["id"], &[("id", DataType::Int64)]);
        assert_eq!(m.default_cluster_cols(), vec!["id".to_string()]);
    }

    #[test]
    fn float_and_bool_pk_are_prunable() {
        let mf = with_pk(&["k"], &[("k", DataType::Float64)]);
        assert_eq!(mf.default_cluster_cols(), vec!["k".to_string()]);
        let mb = with_pk(&["k"], &[("k", DataType::Boolean)]);
        assert_eq!(mb.default_cluster_cols(), vec!["k".to_string()]);
    }

    #[test]
    fn text_pk_is_skipped() {
        let m = with_pk(&["id"], &[("id", DataType::Utf8)]);
        assert!(m.default_cluster_cols().is_empty());
    }

    #[test]
    fn composite_pk_is_skipped() {
        let m = with_pk(&["a", "b"], &[("a", DataType::Int64), ("b", DataType::Int64)]);
        assert!(m.default_cluster_cols().is_empty());
    }

    #[test]
    fn no_pk_is_empty() {
        let m = with_pk(&[], &[("id", DataType::Int64)]);
        assert!(m.default_cluster_cols().is_empty());
    }

    #[test]
    fn explicit_cluster_cols_take_precedence() {
        let mut m = with_pk(&["id"], &[("id", DataType::Int64)]);
        m.cluster_columns = vec!["id".to_string()];
        // Explicit clustering means the caller uses cluster_columns directly;
        // the PK default must NOT also fire (would be a redundant override).
        assert!(m.default_cluster_cols().is_empty());
    }

    #[test]
    fn explicit_sort_order_take_precedence() {
        let mut m = with_pk(&["id"], &[("id", DataType::Int64)]);
        m.global_sort_order = Some(vec!["id".to_string()]);
        assert!(m.default_cluster_cols().is_empty());
    }
}

#[cfg(test)]
mod project_metadata_region_tests {
    use super::*;

    /// `home_region` survives a JSON round-trip — the exact path the Postgres
    /// backend uses (JSONB column = `serde_json::to_value(&meta)`).
    #[test]
    fn home_region_json_round_trip() {
        let meta = ProjectMetadata {
            byo_bucket: None,
            home_region: Some("eu-west-1".to_string()),
        };
        let v = serde_json::to_value(&meta).unwrap();
        let back: ProjectMetadata = serde_json::from_value(v).unwrap();
        assert_eq!(back.home_region.as_deref(), Some("eu-west-1"));
        assert_eq!(back, meta);
    }

    /// A legacy JSONB blob written BEFORE `home_region` existed (only
    /// `byo_bucket`, or `{}`) deserialises with `home_region == None` — the
    /// additive-field back-compat contract.
    #[test]
    fn legacy_blob_without_home_region_defaults_none() {
        // Empty object: every field defaults.
        let back: ProjectMetadata = serde_json::from_str("{}").unwrap();
        assert_eq!(back.home_region, None);
        assert_eq!(back.byo_bucket, None);
        assert_eq!(back, ProjectMetadata::default());
    }

    /// `None` home_region is skipped from the serialized form (so a pinned
    /// project's blob is minimal and an unpinned project's blob carries no
    /// region key at all).
    #[test]
    fn none_home_region_is_skipped_in_serialization() {
        let meta = ProjectMetadata::default();
        let s = serde_json::to_string(&meta).unwrap();
        assert!(!s.contains("home_region"), "unset field is skipped: {s}");
    }
}
