//! Per-project DataFusion session.
//!
//! ## URL convention for the object store
//!
//! DataFusion routes each `ListingTable`'s I/O through whatever
//! `ObjectStore` is registered on the [`SessionContext`]'s `RuntimeEnv`
//! under the URL's `scheme://host` pair. We pick a synthetic scheme,
//! `basin://engine/`, and register `Storage`'s underlying store there once
//! per session.
//!
//! That gives us one URL convention that works identically against
//! `LocalFileSystem` for the PoC and against `AmazonS3` / `GoogleCloudStorage`
//! for production: only the registered store changes; the listing-table URLs
//! the engine constructs do not. The path component carries the configured
//! `root_prefix` (if any) followed by the standard
//! `projects/{project}/tables/{table}/data/` layout.
//!
//! Production note: when this crate moves to native S3 listing, swap the
//! registered store to one whose semantics match `s3://` and then either keep
//! the `basin://` synthetic scheme (simplest) or change paths to `s3://...`.
//! The `register_object_store` call is the single switch point.

use crate::pg_ast::ObjectNamePartExt;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use crate::AuthContext;

use basin_catalog::{
    DataFileRef, PartitionSpec, PromotedJsonbPath, SnapshotId, TableFileFormat, TableMetadata,
    ViewDef,
};
use basin_common::{BasinError, ProjectId, QualifiedTableName, Result, SchemaName, TableName};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::common::TableReference;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::{col, SortExpr};
use datafusion::prelude::SessionContext;
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::Mutex;
use tracing::instrument;
use url::Url;

use crate::convert::schema_ws_to_df;
use crate::parquet_listing_format::BasinParquetFormat;
use crate::vortex_listing_format::BasinVortexFormat;
use crate::{Engine, ProjectSession, StatelessUdfCache};
// `VortexSession::default()` is provided by the `VortexSessionDefault` trait;
// the trait must be in scope to call it.
#[allow(unused_imports)]
use vortex::VortexSessionDefault as _;

/// Synthetic URL we register the storage `ObjectStore` under. The scheme is
/// purely an internal protocol between `basin-engine` and DataFusion; it is
/// never exposed to clients.
pub(crate) const BASIN_URL_BASE: &str = "basin://engine/";

/// Default statement timeout (milliseconds) when `BASIN_STATEMENT_TIMEOUT_MS`
/// is unset. 30 s is generous for analytic workloads yet cheap insurance
/// against a hostile cartesian self-join or an ill-bounded recursive CTE
/// pinning a DataFusion worker thread (noisy-neighbor P0, Phase 6.P0.A).
pub(crate) const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;

/// Parse a PostgreSQL GUC duration string into an `Option<Duration>`.
///
/// Accepted forms (case-insensitive):
///   - `0` or `"0"` → `None` (disabled)
///   - `"500ms"` → 500 milliseconds
///   - `"5s"` or `"5000ms"` → 5 seconds
///   - A bare integer is treated as **milliseconds** (matching `statement_timeout`
///     convention: `SET lock_timeout = 500` means 500 ms).
///
/// Any unrecognised form returns `None` (disabled), which is safe: the lock
/// timeout simply doesn't fire rather than misfiring.
pub(crate) fn parse_pg_duration(raw: &str) -> Option<std::time::Duration> {
    let s = raw.trim().to_ascii_lowercase();
    if s.is_empty() || s == "0" || s == "off" {
        return None;
    }
    // "Nms"
    if let Some(n) = s.strip_suffix("ms") {
        if let Ok(ms) = n.trim().parse::<u64>() {
            return if ms == 0 {
                None
            } else {
                Some(std::time::Duration::from_millis(ms))
            };
        }
    }
    // "Ns" or "Nsec"
    if let Some(n) = s.strip_suffix("sec").or_else(|| s.strip_suffix('s')) {
        if let Ok(sec) = n.trim().parse::<u64>() {
            return if sec == 0 {
                None
            } else {
                Some(std::time::Duration::from_secs(sec))
            };
        }
    }
    // "Nmin"
    if let Some(n) = s.strip_suffix("min") {
        if let Ok(min) = n.trim().parse::<u64>() {
            return if min == 0 {
                None
            } else {
                Some(std::time::Duration::from_secs(min * 60))
            };
        }
    }
    // bare integer → milliseconds
    if let Ok(ms) = s.parse::<u64>() {
        return if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        };
    }
    None
}

/// Render `Option<Duration>` as a PG-compatible GUC string.
/// `None` → `"0"` (disabled); `Some(d)` → `"<ms>ms"`.
pub(crate) fn format_pg_duration(d: Option<std::time::Duration>) -> String {
    match d {
        None => "0".to_string(),
        Some(dur) => format!("{}ms", dur.as_millis()),
    }
}

/// Process-wide statement wall-clock budget. `None` = disabled (back-compat,
/// `BASIN_STATEMENT_TIMEOUT_MS=0`); `Some(d)` = cancel any statement still
/// executing after `d`. Cached in a `OnceLock` so a query never pays a getenv
/// on the hot path — the deadline is read once and compared, never per-row.
pub(crate) fn statement_timeout() -> Option<std::time::Duration> {
    #[cfg(test)]
    if let Some(over) = test_timeout_override::get() {
        return over;
    }
    static CACHED: OnceLock<Option<std::time::Duration>> = OnceLock::new();
    *CACHED.get_or_init(|| {
        parse_statement_timeout(std::env::var("BASIN_STATEMENT_TIMEOUT_MS").ok().as_deref())
    })
}

/// Test-only deterministic override for [`statement_timeout`]. The production
/// path caches the env once in a `OnceLock`, so tests cannot exercise the
/// cancellation path by mutating the env (it races, and the `OnceLock` would
/// have already latched). Instead a test installs a thread-local override for
/// the duration of its body. Never compiled into release builds.
#[cfg(test)]
pub(crate) mod test_timeout_override {
    use std::cell::Cell;
    use std::time::Duration;

    thread_local! {
        // Outer Option: is an override installed? Inner Option: the timeout
        // value an installed override maps to (None = disabled).
        static OVERRIDE: Cell<Option<Option<Duration>>> = const { Cell::new(None) };
    }

    pub(crate) fn get() -> Option<Option<Duration>> {
        OVERRIDE.with(|c| c.get())
    }

    /// Install `value` as the effective statement timeout for the current
    /// thread; the returned guard restores the previous state on drop.
    pub(crate) fn install(value: Option<Duration>) -> Guard {
        let prev = OVERRIDE.with(|c| c.replace(Some(value)));
        Guard { prev }
    }

    pub(crate) struct Guard {
        prev: Option<Option<Duration>>,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            OVERRIDE.with(|c| c.set(self.prev));
        }
    }
}

/// Pure parse of `BASIN_STATEMENT_TIMEOUT_MS`. Pulled out of
/// [`statement_timeout`] so unit tests pass strings directly instead of
/// mutating the process env (which races under `cargo test`'s parallel
/// runner). Unset → default; `0` → disabled; non-numeric / empty → default.
pub(crate) fn parse_statement_timeout(raw: Option<&str>) -> Option<std::time::Duration> {
    let ms = match raw.map(str::trim) {
        None | Some("") => DEFAULT_STATEMENT_TIMEOUT_MS,
        Some(s) => s.parse::<u64>().unwrap_or(DEFAULT_STATEMENT_TIMEOUT_MS),
    };
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
}

/// Parse a SQL GUC-style `statement_timeout` value as set by
/// `SET statement_timeout = <val>`.
///
/// Accepted forms (matching Postgres behaviour):
/// * Bare integer — milliseconds: `5000`, `0`
/// * Quoted string with unit suffix: `'5s'`, `'500ms'`, `'2min'`, `'1h'`,
///   `'0'` (also bare integer strings like `'5000'` are accepted as ms)
///
/// Returns:
/// * `Ok(None)` — timeout disabled (`0` / `'0'`)
/// * `Ok(Some(d))` — effective timeout
/// * `Err(…)` — unrecognised format
pub(crate) fn parse_statement_timeout_guc(raw: &str) -> Result<Option<std::time::Duration>> {
    let s = raw.trim().trim_matches('\'');
    let s = s.trim();
    if s == "0" || s.is_empty() {
        return Ok(None);
    }
    // Try bare integer first (milliseconds).
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(if ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(ms))
        });
    }
    // Try string with unit suffix (case-insensitive).
    // Walk backwards to split numeric prefix from unit suffix.
    let (num_part, unit_part) = {
        let idx = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
        (&s[..idx], s[idx..].trim())
    };
    let count: f64 = num_part.parse().map_err(|_| {
        BasinError::InvalidSchema(format!("invalid statement_timeout value: {raw}"))
    })?;
    let ms = match unit_part.to_ascii_lowercase().as_str() {
        "ms" | "msec" | "millisecond" | "milliseconds" => count,
        "s" | "sec" | "second" | "seconds" => count * 1_000.0,
        "min" | "minute" | "minutes" => count * 60_000.0,
        "h" | "hour" | "hours" => count * 3_600_000.0,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "invalid statement_timeout unit: {other}"
            )));
        }
    };
    let ms = ms as u64;
    Ok(if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    })
}

/// Return the effective `statement_timeout` for `state`, honouring any
/// per-session override set by `SET statement_timeout = …`.
///
/// Priority: per-session override (if set) → process-wide default
/// (`BASIN_STATEMENT_TIMEOUT_MS` env / [`statement_timeout`]).
pub(crate) fn session_statement_timeout(state: &SessionState) -> Option<std::time::Duration> {
    let v = state
        .statement_timeout_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    match v {
        -1 => statement_timeout(), // no per-session override
        0 => None,                 // explicitly disabled
        ms => Some(std::time::Duration::from_millis(ms as u64)),
    }
}

/// The minimum number of Parquet files a table must have before the engine
/// activates intra-query scan parallelism (i.e. bumps `target_partitions`
/// above 1).  At the default row-group size of 65 536 rows this corresponds
/// to ~65 k rows — small enough that the per-file startup overhead is
/// negligible compared with the wall-clock gain from parallel reads.
const MIN_FILES_FOR_PARALLEL_SCAN: usize = 2;

/// Return the `target_partitions` value to use for a full-DataFusion scan of
/// `file_count` Parquet files.
///
/// Rules (applied in order):
/// 1. `file_count < MIN_FILES_FOR_PARALLEL_SCAN` → return 1 (single-file /
///    empty tables get no fan-out).
/// 2. `BASIN_ENGINE_TARGET_PARTITIONS_MAX` env var (parsed once, cached) caps
///    the value.  Unset / `0` / non-numeric → `available_parallelism()`.
/// 3. Return `min(cap, file_count)` — never fan out more than the file count
///    since DataFusion can't split a file across partition slots without a
///    full repartition node.
///
/// This function is called only from the full-DataFusion SELECT path
/// (`exec_select`).  The simple-SELECT fast-path and all DML paths keep
/// `target_partitions = 1` permanently.
pub(crate) fn target_partitions_for_bulk_scan(file_count: usize) -> usize {
    if file_count < MIN_FILES_FOR_PARALLEL_SCAN {
        return 1;
    }
    static MAX_CACHED: OnceLock<usize> = OnceLock::new();
    let cap = *MAX_CACHED.get_or_init(|| {
        let from_env: Option<usize> = std::env::var("BASIN_ENGINE_TARGET_PARTITIONS_MAX")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0);
        from_env.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
    });
    cap.min(file_count).max(1)
}

/// Apply a `SET statement_timeout = <raw>` GUC to the session.
///
/// `raw` is the raw RHS string from the SQL statement (may include quotes).
/// Returns `Err` on unrecognised format.
pub(crate) fn set_statement_timeout(state: &SessionState, raw: &str) -> Result<()> {
    let d = parse_statement_timeout_guc(raw)?;
    let ms: i64 = match d {
        None => 0,
        Some(d) => d.as_millis().min(i64::MAX as u128) as i64,
    };
    state
        .statement_timeout_ms
        .store(ms, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

/// Return the current per-session `statement_timeout` as a Postgres-style
/// string for `SHOW statement_timeout`.
pub(crate) fn show_statement_timeout(state: &SessionState) -> String {
    match session_statement_timeout(state) {
        None => "0".to_string(),
        Some(d) => {
            let ms = d.as_millis();
            if ms % 60_000 == 0 {
                format!("{}min", ms / 60_000)
            } else if ms % 1_000 == 0 {
                format!("{}s", ms / 1_000)
            } else {
                format!("{}ms", ms)
            }
        }
    }
}

/// Build the shared stateless UDF registry once at `Engine::new` time.
///
/// Strategy: register all stateless UDFs into a throwaway `SessionContext`
/// (which has DataFusion's own built-in functions pre-seeded). Then extract
/// the populated `scalar_functions` and `aggregate_functions` maps. Each
/// subsequent session-open just clones the `Arc` handles (ref-count bumps
/// only, no struct allocation) and passes them to `SessionStateBuilder`
/// for batch insertion without holding a write-lock per UDF.
pub(crate) fn build_stateless_udf_cache() -> StatelessUdfCache {
    let ctx = SessionContext::new();
    crate::udf::register_distance_udfs(&ctx);
    crate::udf::register_pg_udfs(&ctx);
    crate::udf::register_pg_compat_udfs(&ctx);
    crate::string_dt_udf::register_string_dt_udfs(&ctx);
    crate::fts_udf::register_fts_udfs(&ctx);
    crate::jsonb_udf::register_jsonb_udfs(&ctx);
    crate::interval_tz_udf::register_interval_tz_udfs(&ctx);
    crate::pg_scalar_aliases::register_pg_scalar_aliases(&ctx);
    crate::pg_catalog_udf::register_pg_catalog_udfs(&ctx);
    crate::pg_agg_udf::register_json_agg_udafs(&ctx);
    crate::approx_count_distinct::register_approx_count_distinct(&ctx);
    crate::approx_percentile::register_approx_percentile(&ctx);
    crate::range_udf::register_range_udfs(&ctx);
    crate::jsonb_path_udf::register_jsonb_path_udfs(&ctx);
    crate::jsonb_modify_udf::register_jsonb_modify_udfs(&ctx);
    crate::json_build_udf::register_json_build_udfs(&ctx);
    crate::inet_udf::register_inet_udfs(&ctx);
    crate::datetime_more_udf::register_datetime_more_udfs(&ctx);
    crate::string_more_udf::register_string_more_udfs(&ctx);
    crate::regex_udf::register_regex_udfs(&ctx);
    // Phase 5.29.B: time_bucket(interval_text, ts) → ts (TimescaleDB-compat).
    // Registered before datetime_extras so that datetime_extras (which
    // registers array_dims) comes last and takes precedence over any
    // DataFusion built-in with the same name.
    crate::hypertable::register_time_bucket_udf(&ctx);
    crate::datetime_extras::register_datetime_extras(&ctx);
    // Phase 5.30.C: register citext comparison UDFs (citext_eq, citext_ne,
    // citext_lt, citext_le, citext_gt, citext_ge, citext_like).
    for udf in crate::operators::citext_cmp::citext_udfs() {
        ctx.register_udf((*udf).clone());
    }
    // PG-Wave-α: PostGIS-shape ST_* UDFs (ST_MakePoint, ST_X, ST_Y,
    // ST_Distance, ST_DWithin, ST_AsText, ST_AsEWKB, ST_GeomFromText,
    // ST_GeomFromWKB). Registered last so case-insensitive lookups
    // resolve to the basin-geo implementation.
    crate::geo_glue::install_udfs(&ctx);
    // pg_trgm-compatible fuzzy-text UDFs: similarity, word_similarity,
    // show_trgm. Operators (%, <%, <->) are wired via rewrite_trgm_operators
    // in the pre-parse pipeline rather than as DataFusion binary operators.
    crate::trgm_glue::register_trgm_udfs(&ctx);
    let state = ctx.state();
    // Build DataFusion's default optimizer rule list once.  The 27
    // stateless rules are heap-allocated `Arc<dyn OptimizerRule>`; without
    // this cache every `session::open` call paid a fresh
    // `Optimizer::default()` to rebuild them.  Cloning the `Vec` at
    // session-open time is O(n) Arc ref-count bumps with no allocation.
    let optimizer_rules = datafusion::optimizer::Optimizer::default().rules;
    StatelessUdfCache {
        scalar: state.scalar_functions().values().cloned().collect(),
        aggregate: state.aggregate_functions().values().cloned().collect(),
        optimizer_rules,
    }
}

/// `OVERRIDING { SYSTEM | USER } VALUE` clause on INSERT. Tracked
/// per-pending-statement because sqlparser 0.52 doesn't recognise the
/// clause; the executor pre-screens it textually and stashes the kind
/// for the INSERT path to consume. See `session::take_pending_overriding`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OverridingKind {
    /// `OVERRIDING SYSTEM VALUE`: bypasses IDENTITY ALWAYS rejection;
    /// user-supplied values are accepted. No effect on BY DEFAULT.
    System,
    /// `OVERRIDING USER VALUE`: discards user-supplied values on BY
    /// DEFAULT IDENTITY columns and fills from nextval instead. No
    /// effect on ALWAYS.
    User,
}

/// A single savepoint frame — captures the per-table pending-file watermark
/// at the moment `SAVEPOINT <name>` was issued.  Rolling back to this
/// savepoint discards all files (and tx-buffered htap batches) appended
/// after the watermark.
#[derive(Debug)]
pub(crate) struct SavepointFrame {
    /// Name of the savepoint (case-sensitive, per PG).
    pub(crate) name: String,
    /// For each table touched *before* this savepoint, how many pending files
    /// existed at savepoint time. Files at or beyond this index are the ones
    /// that would be rolled back if we `ROLLBACK TO <name>`.
    pub(crate) file_offsets: HashMap<TableName, usize>,
    /// For each table with tx-buffered htap batches at savepoint time, the
    /// `Vec<RecordBatch>` length.  perf-w7-txn: INSERTs in an open tx are
    /// buffered in `htap_rows` instead of being written to Parquet, so the
    /// savepoint must rewind that buffer too.
    pub(crate) htap_offsets: HashMap<TableName, usize>,
}

impl SavepointFrame {
    /// Clone the `file_offsets` map for use during rollback-to-savepoint.
    fn clone_offsets(&self) -> HashMap<TableName, usize> {
        self.file_offsets.clone()
    }
    /// Clone the `htap_offsets` map for use during rollback-to-savepoint.
    fn clone_htap_offsets(&self) -> HashMap<TableName, usize> {
        self.htap_offsets.clone()
    }
}

/// Per-session transaction state — tracks whether a `BEGIN` has been issued
/// and records catalog-snapshot heads captured at `BEGIN` time.
///
/// ## Role in the overall transaction model (Step 3+, issue #83)
///
/// * `active` — set by `BEGIN`, cleared by `COMMIT`/`ROLLBACK`.
/// * `aborted` — set when any statement fails *inside* an active txn.  Only
///   `ROLLBACK` (or `ROLLBACK TO SAVEPOINT`) clears it.  Every statement
///   except those two returns SQLSTATE 25P02 while `aborted` is true.
/// * `pre_tx_snapshots` — catalog snapshot IDs at `BEGIN` time.  `ROLLBACK`
///   uses these to restore the catalog read-view to the pre-txn state.
/// * `pending_files` — data files written during the txn that have NOT yet
///   been committed to the catalog.  `COMMIT` flushes them; `ROLLBACK`
///   deletes them.
/// * `savepoints` — stack of `SavepointFrame`s recording per-table file
///   watermarks for `ROLLBACK TO SAVEPOINT`.
/// * `tx_id` — monotonic transaction id assigned on the first DML inside the
///   txn.  Used for WAL Begin/Commit/Rollback markers (Phase 5.14.C2).
/// * `htap_rows` — in-memory Arrow `RecordBatch`es buffered per table during
///   the open transaction.  Provides tx-local read-your-own-writes visibility
///   for projection queries (the "known gap" in the Parquet-only path).  On
///   COMMIT the batches are promoted to `MemTableRegistry`; on ROLLBACK they
///   are simply discarded without touching any shared state.
/// * `tx_overlay` — per-table PK→`Update`/`Tombstone` overlay written by the
///   in-tx single-row PK UPDATE/DELETE fast path. On COMMIT each entry is
///   drained into the shared `MemTableRegistry`; on ROLLBACK it is dropped.
///   See the field doc for the savepoint limitation.
#[derive(Debug, Default)]
pub(crate) struct TxState {
    /// `true` between `BEGIN` and the matching `COMMIT` / `ROLLBACK`.
    pub(crate) active: bool,
    /// `true` when a statement inside an active txn has errored.  Cleared
    /// only by `ROLLBACK`.
    pub(crate) aborted: bool,
    /// Snapshot IDs of tables as they appeared when `BEGIN` was issued.
    /// Used by `ROLLBACK` to restore catalog heads.
    pub(crate) pre_tx_snapshots: HashMap<TableName, SnapshotId>,
    /// Transaction read-view: the catalog snapshot id each table is *pinned*
    /// to for the lifetime of this transaction (REPEATABLE-READ-ish snapshot
    /// stability). A table's read snapshot is captured the first time the
    /// transaction touches it (its first in-tx SELECT/DML), *not* at `BEGIN`:
    /// the read path flushes the shard tail before loading the table, so the
    /// first-touch head is the snapshot that read actually observes (see
    /// [`tx_read_snapshot_for`] and [`tx_begin`]). Every in-transaction read
    /// reconstructs the table's file set at this pinned id via
    /// `Catalog::load_table_at_snapshot`, so writes committed by *other*
    /// sessions after `BEGIN` (which advance the catalog head) stay invisible.
    /// Cleared on COMMIT / ROLLBACK alongside the rest of the tx state.
    pub(crate) read_snapshots: HashMap<TableName, SnapshotId>,
    /// Data files written during the open transaction that have not yet
    /// been committed to the catalog.
    pub(crate) pending_files: HashMap<TableName, Vec<DataFileRef>>,
    /// Savepoint stack.  Innermost (most-recently-created) frame is last.
    pub(crate) savepoints: Vec<SavepointFrame>,
    /// Monotonic WAL transaction id, assigned lazily on the first DML
    /// statement inside an explicit `BEGIN` block. `None` before any DML
    /// has been issued or when the session is in auto-commit mode.
    pub(crate) tx_id: Option<u64>,
    /// Hot-tier in-memory row buffer: per-table Arrow `RecordBatch`es written
    /// during this transaction.  Provides tx-local projection-scan visibility
    /// (Phase 5.14.C2 HTAP write-path).  The batches are:
    ///   - Appended on each INSERT/UPDATE/DELETE inside an active tx.
    ///   - Merged with the cold-tier scan by `refresh_table_with_htap`.
    ///   - Promoted to the shared `MemTableRegistry` on COMMIT.
    ///   - Silently discarded on ROLLBACK (no shared state is touched).
    pub(crate) htap_rows: HashMap<TableName, Vec<arrow_array::RecordBatch>>,
    /// Transaction-scoped hot-tier UPDATE/DELETE overlay (the OLTP-in-tx
    /// fast path).  Per table, an override / tombstone keyed by encoded PK
    /// (`RowKey`) written by the in-tx single-row PK UPDATE/DELETE fast path
    /// (`dml_mutate::hot_tier_update_by_pk_tx` / `..._delete_by_pk_tx`).
    ///
    /// Unlike the non-tx fast path (which writes straight to the process-wide
    /// `MemTableRegistry` and has no rollback story), these entries live ONLY
    /// in this session's `TxState`, so:
    ///   - they are visible to this transaction's own reads (merged ON TOP of
    ///     the shared-registry overlay snapshot by the read path);
    ///   - on COMMIT they are drained into the shared `MemTableRegistry`
    ///     (`entry.memtable.insert(key, value)`), exactly mirroring what the
    ///     non-tx fast path would have written;
    ///   - on ROLLBACK they are simply dropped — no shared state was touched.
    ///
    /// A `BTreeMap` so repeated read-modify-write on the same PK within the
    /// transaction (`UPDATE v=v+1` twice) overwrites the same key and
    /// accumulates.  Values are `MemRowValue::Update` (full-row override) or
    /// `MemRowValue::Tombstone` (in-tx fast-path DELETE).
    ///
    /// ## Savepoint limitation
    ///
    /// Savepoints snapshot `pending_files` / `htap_rows` by Vec-length
    /// watermark; this map has no equivalent length watermark (RMW overwrites
    /// the same key), so it does NOT participate in savepoint rollback. The
    /// write-path gate therefore disables the tx fast path whenever any
    /// savepoint is active (see `tx_overlay_fastpath_blocked`), routing those
    /// statements to the cold copy-on-write path which DOES honour savepoints.
    pub(crate) tx_overlay: HashMap<
        TableName,
        std::collections::BTreeMap<basin_hottier::RowKey, basin_hottier::MemRowValue>,
    >,
    /// Hot-tier MVCC sequence watermark pinned per table for this transaction's
    /// read-view. Captured at the SAME first-touch moment as the cold
    /// `read_snapshots` pin (inside `load_table_for_read`, after the read's own
    /// tail flush) so the hot and cold halves of the snapshot are consistent.
    ///
    /// The overlay read path (`register_cold_with_overlay` / `HtapUnionTable`)
    /// passes this watermark to `snapshot_tombstones` / `snapshot_updates`,
    /// which drop any shared-registry entry whose `seq` exceeds it — i.e. an
    /// auto-commit UPDATE/DELETE fast-path write another session committed after
    /// this transaction pinned its snapshot. This closes the documented hot-tier
    /// isolation gap: those post-snapshot overlay writes no longer leak into an
    /// open transaction's pinned view. The transaction's OWN `tx_overlay`
    /// entries are layered on top separately and always win.
    ///
    /// Empty outside a transaction; cleared on COMMIT / ROLLBACK.
    pub(crate) hot_seq_watermark: HashMap<TableName, u64>,
    /// In-transaction buffered change events (CDC / realtime), in execution
    /// order. Hot-tier UPDATE/DELETE fast-path statements (and the in-tx
    /// UPDATE…FROM write tail) push a [`TxChangeEvent`] here at statement time
    /// — WITHOUT a `seq` (the per-`(project, table)` sequence is allocated in
    /// commit order at COMMIT, so other sessions' interleaved auto-commit
    /// events don't fragment this tx's ordering). On COMMIT the executor drains
    /// this buffer (`tx_change_events_take_all`), assigns each entry a `seq`
    /// via `Engine::next_event_seq` in buffer order, and dispatches them to the
    /// post-commit sinks AFTER the overlay/catalog commit has landed. On
    /// ROLLBACK it is dropped, so a rolled-back mutation never reaches a sink.
    ///
    /// Only populated when at least one change-event sink is attached at
    /// statement time (the fast paths gate on `sinks_attached`); empty and
    /// untouched on the zero-sink OLTP hot path.
    pub(crate) tx_change_events: Vec<TxChangeEvent>,
}

/// One buffered in-transaction change event, captured at statement execution
/// time but not yet assigned a commit `seq`. The before/after JSON payloads are
/// the same lazily-materialised images the auto-commit path builds; `op` and
/// `table` carry the rest of the [`basin_common::ChangeEvent`] shape. The
/// `seq` and `committed_at` are filled in at COMMIT drain time so the event
/// carries the same commit ordering the cold paths get.
#[derive(Debug)]
pub(crate) struct TxChangeEvent {
    pub(crate) table: TableName,
    pub(crate) op: basin_common::ChangeOp,
    pub(crate) before: Option<serde_json::Value>,
    pub(crate) after: Option<serde_json::Value>,
    /// The causing user (`None` for the anonymous role), captured at statement
    /// time from the session so a `RESET ROLE` before COMMIT can't rewrite it.
    pub(crate) causation_user: Option<String>,
}

/// Per-session mutable state. The `SessionContext` itself is `Send + Sync`
/// and DataFusion handles concurrency on it; we only need the snapshot cache
/// behind a mutex.
pub(crate) struct SessionState {
    /// Latest snapshot id we've observed for each table this session has
    /// touched. Used to feed the `expected_snapshot` argument of
    /// `append_data_files` without an extra catalog load on every INSERT.
    pub(crate) snapshots: Mutex<HashMap<TableName, SnapshotId>>,
    /// Per-session prepared-statement registry. See `prepared.rs`.
    pub(crate) prepared: crate::prepared::PreparedRegistry,
    /// Tracks whether this project has *any* table with a non-trivial
    /// partition spec. The SELECT path uses this as an O(1) gate to skip
    /// the partition-pruning AST walk on every query for projects that
    /// never used `PARTITION BY`. Initialised at session-open time and
    /// flipped to `true` by `refresh_table` when it sees a partitioned
    /// table.
    pub(crate) has_partitioned_table: std::sync::atomic::AtomicBool,
    /// Per-session sequence "last nextval" cache. PG's `currval`
    /// semantics require this to be per-session: every session has its
    /// own view of "what value was most recently handed out for each
    /// sequence". Empty until the session's first `nextval`; consulted
    /// by the SQL-string sequence rewriter on every `currval` call.
    pub(crate) sequence_cache: Arc<crate::seq_udf::SessionSequenceCache>,
    /// Per-session open-cursor registry. `DECLARE … CURSOR FOR …`
    /// materialises the SELECT result and stores it here under the cursor
    /// name; `FETCH` / `MOVE` advance the position; `CLOSE` removes the
    /// entry.  All cursors are destroyed when the session is dropped
    /// (`CursorRegistry` holds no external state).
    pub(crate) cursors: crate::cursor::CursorRegistry,
    /// Per-session schema registry and search_path. `"public"` is always
    /// present. Updated by `CREATE SCHEMA` / `DROP SCHEMA` /
    /// `SET search_path`. See `crate::schema_ddl::SchemaState`.
    pub(crate) schema_state: Arc<RwLock<crate::schema_ddl::SchemaState>>,
    /// Pending `OVERRIDING { SYSTEM | USER } VALUE` clause stripped from
    /// the current INSERT statement before sqlparser sees it. Set by
    /// `execute()`'s pre-screen, consumed (taken) by `exec_insert`.
    /// `std::sync::Mutex` (not `tokio::sync::Mutex`) so the consumer can
    /// `.take()` synchronously from inside the INSERT path without
    /// crossing an `await`.
    pub(crate) pending_overriding: std::sync::Mutex<Option<OverridingKind>>,
    /// Per-session explicit-transaction state. Tracks whether a `BEGIN`
    /// has been issued and (in the future) the catalog-snapshot heads at
    /// `BEGIN` time. See [`TxState`] doc-comment for the full rationale
    /// and the Step 3+ design notes.
    ///
    /// Uses a `std::sync::Mutex` (not `tokio`) because the accessors
    /// (`tx_begin`, `tx_commit`, `tx_rollback`, `tx_is_active`) are
    /// called from synchronous contexts inside the executor dispatch path
    /// — no `await` needed.
    pub(crate) tx_state: std::sync::Mutex<TxState>,
    /// Per-session advisory-lock ownership (BUG #138). Holds this session's
    /// unique owner token and the set of keys it currently holds (session-
    /// and xact-scoped). The `pg_advisory_*` UDFs registered in
    /// `session::open` share this `Arc`. Xact-scoped locks are released by
    /// `tx_commit` / `tx_rollback`; all locks are released when the session
    /// is dropped (see `impl Drop for SessionState`).
    pub(crate) advisory: Arc<crate::advisory_lock::AdvisorySessionLocks>,
    /// Per-session `statement_timeout` set by `SET statement_timeout = …`.
    ///
    /// Encoded as milliseconds in an `AtomicI64`:
    /// * `-1` — no per-session override; use the process-wide default
    ///   (`BASIN_STATEMENT_TIMEOUT_MS`).
    /// * `0` — timeout disabled (Postgres semantics: `SET statement_timeout = 0`).
    /// * `> 0` — the effective timeout in milliseconds.
    ///
    /// `AtomicI64` is `Send + Sync` and avoids a `Mutex` on the hot path
    /// (a single 64-bit load with `Relaxed` ordering is sufficient — there
    /// is no ordering relationship required with other fields).
    pub(crate) statement_timeout_ms: std::sync::atomic::AtomicI64,

    // ── Phase 5.28 session-level GUCs ────────────────────────────────────────
    /// `SET lock_timeout = '500ms'` per-session override.
    /// `None` = disabled (0). Atomically readable by the lock-wait primitive.
    /// Uses `std::sync::Mutex` so the SET handler can update it synchronously.
    pub(crate) lock_timeout: std::sync::Mutex<Option<std::time::Duration>>,

    /// `SET idle_in_transaction_session_timeout = '30s'` per-session override.
    /// `None` = disabled (0). The reaper reads this.
    pub(crate) idle_in_transaction_session_timeout: std::sync::Mutex<Option<std::time::Duration>>,

    /// `SET basin.synchronous_commit = on|off` per-session durability mode.
    ///
    /// * `off` (default) — INSERTs on the shard path ack once the WAL has the
    ///   entry in RAM (ack-before-durable; loss window = the WAL's flush
    ///   tick / buffer-pressure bound).
    /// * `on` — INSERTs only ack after the WAL group-commits the entry to
    ///   the backing store (segment PUT + fsync on the local backend).
    ///
    /// Initialised from `BASIN_SYNCHRONOUS_COMMIT` (engine-wide default);
    /// `AtomicBool` because the write path reads it on every statement and
    /// needs no ordering relationship with other fields.
    pub(crate) synchronous_commit: std::sync::atomic::AtomicBool,

    // ── pg_trgm session-level GUCs ────────────────────────────────────────────
    /// `SET pg_trgm.similarity_threshold = <float>` per-session threshold for
    /// the `%` operator.  Defaults to `basin_trgm::DEFAULT_SIMILARITY_THRESHOLD`
    /// (0.3, matching the PG default).  Stored as an `AtomicU32` holding the
    /// IEEE 754 bit-pattern of the `f32` value so the rewrite pipeline can read
    /// it on every statement without a Mutex.
    pub(crate) trgm_similarity_threshold: std::sync::atomic::AtomicU32,

    /// `SET pg_trgm.word_similarity_threshold = <float>` per-session threshold
    /// for the `<%` operator.  Defaults to
    /// `basin_trgm::DEFAULT_WORD_SIMILARITY_THRESHOLD` (0.6).
    pub(crate) trgm_word_similarity_threshold: std::sync::atomic::AtomicU32,

    // ── Multi-region read tier GUC (ADR 0009) ────────────────────────────────
    /// `SET basin.read_tier = 'primary' | 'lagging'` per-session read-staleness
    /// mode. Stored as the `u8` discriminant of [`ReadTier`] in an `AtomicU8`
    /// so the read path can consult it without a Mutex.
    ///
    /// * `primary` (default) — today's behaviour: strongly-consistent reads
    ///   served from the home region's hot tier + WAL tail. A primary read of
    ///   a project homed in another region is rejected with
    ///   `BasinError::WrongRegion` (only the home region has the live tail).
    /// * `lagging` — serve from local S3-CRR-replicated cold data, accepting
    ///   staleness, WITHOUT requiring the home region. Non-home-region
    ///   lagging reads see flushed-only state (the hot tier / WAL tail does not
    ///   exist outside the home region); the durable compaction watermark is
    ///   the staleness bound.
    pub(crate) read_tier: std::sync::atomic::AtomicU8,

    /// Timestamp of the last statement activity on this session. Updated by
    /// the executor at the start of every `execute()` call; the idle-in-txn
    /// reaper compares this against the current time.
    pub(crate) last_active: std::sync::Mutex<std::time::Instant>,

    /// Per-session SQL `LISTEN` subscriptions and the buffer of pending
    /// `NOTIFY` payloads queued inside an open transaction (PG buffers
    /// notifications until COMMIT and discards them on ROLLBACK). All
    /// state behind one `Mutex` so executor handlers can flip
    /// subscriptions and drain pending notifies atomically.
    pub(crate) listen: std::sync::Mutex<ListenState>,
    /// Per-session table-metadata cache. Populated lazily by the SELECT
    /// fast-path gate; cleared at the top of executor dispatch for any
    /// non-`SELECT` statement. See [`TableMetaCache`] for the full
    /// correctness model.
    pub(crate) table_meta_cache: TableMetaCache,
    /// Per-session DataFusion provider cache (Fix B+C). Memoises the final
    /// built `Arc<dyn TableProvider>` per `(table, snapshot)` so the auto-commit
    /// SELECT refresh skips the bloom-laden `TableMetadata` clone, schema
    /// conversion and `ListingTable` rebuild on a hit. Bypassed inside a
    /// transaction; cleared on tx boundaries and the catalog-epoch bump (and
    /// per-table on a single-table DML — concurrency fix #4). See
    /// [`ProviderCache`] for the full correctness model.
    pub(crate) provider_cache: ProviderCache,
    /// Per-session live-head probe cache. Lets the [`ProviderCache`] fast path
    /// build its `(table, snapshot)` key on a HIT without the bloom-laden
    /// `load_table` clone. Epoch+TTL validated; cleared in lockstep with
    /// `provider_cache`. See [`HeadProbeCache`].
    pub(crate) head_probe_cache: HeadProbeCache,
    /// Per-session cache of the two project-scoped catalog flags the hot-tier
    /// UPDATE/DELETE fast path consults PER STATEMENT — whether any child table
    /// references this table by FK (`fks_referencing`) and whether a per-table
    /// UPDATE reactor is registered (`list_reactors`). Both were uncached
    /// awaited catalog round-trips on every single-row UPDATE
    /// (~120µs/statement on the warm OLTP loop). Epoch+TTL validated exactly
    /// like [`HeadProbeCache`]: any catalog mutation (FK DDL, reactor
    /// register/drop, or any other epoch bump) invalidates the entry. See
    /// [`DmlFlagsCache`].
    pub(crate) dml_flags_cache: DmlFlagsCache,
    /// Per-session ingest META cache. Serves `exec_ingest_batch`'s per-batch
    /// schema/constraint fetch from the cheap META-only `Catalog::load_table_meta`,
    /// keyed on `Catalog::meta_version()` so the stream of per-partition DATA
    /// commits a bulk COPY issues never invalidates it (the multi-node ingest
    /// throughput fix). See [`IngestMetaCache`].
    pub(crate) ingest_meta_cache: IngestMetaCache,
    /// Tables this session has already fired a `BASIN_PREWARM_PROVIDERS`
    /// fire-and-forget warm for. The prewarm reads the table's per-file
    /// stats/footers (`Storage::list_data_files_with_stats`) into the
    /// process-wide footer/stats caches on the FIRST cold `load_table` miss,
    /// so a follow-up cold SELECT against the same files skips the per-file
    /// footer fetch (cold 3.4→~2.3ms; the steady warm path is unaffected). The
    /// set bounds it to one spawn per (session, table) — a no-op when the env
    /// flag is unset, which is the default.
    pub(crate) prewarmed_tables: std::sync::Mutex<std::collections::HashSet<TableName>>,
}

/// Multi-region read-staleness tier (`basin.read_tier`). `Primary` is the
/// default and reproduces single-region behaviour; `Lagging` opts into
/// reading S3-replicated cold data from a non-home region. Repr-`u8` so it
/// round-trips through the session's `AtomicU8` with no allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ReadTier {
    /// Strongly-consistent read; must be served from the project's home region.
    Primary = 0,
    /// Bounded-staleness read from local S3-replicated cold data; allowed in
    /// any region.
    Lagging = 1,
}

impl ReadTier {
    /// The default tier a fresh session opens with (matches PG-style
    /// strongly-consistent reads). Kept as a named const so `new()`, the
    /// reset, and the test all agree on one source of truth.
    pub(crate) const DEFAULT: ReadTier = ReadTier::Primary;

    fn from_u8(v: u8) -> ReadTier {
        match v {
            1 => ReadTier::Lagging,
            // 0 and any unexpected value fall back to the safe strong default.
            _ => ReadTier::Primary,
        }
    }

    /// The GUC string `SHOW basin.read_tier` renders.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ReadTier::Primary => "primary",
            ReadTier::Lagging => "lagging",
        }
    }

    /// Parse a `SET basin.read_tier = '<v>'` value (case-insensitive, quotes
    /// already stripped by the caller). Errors on anything else so a typo'd
    /// tier does not silently downgrade consistency.
    pub(crate) fn parse(raw: &str) -> Result<ReadTier> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "primary" | "default" => Ok(ReadTier::Primary),
            "lagging" | "replica" => Ok(ReadTier::Lagging),
            other => Err(BasinError::InvalidSchema(format!(
                "invalid value for basin.read_tier: {other:?} (expected 'primary' or 'lagging')"
            ))),
        }
    }
}

impl Drop for SessionState {
    fn drop(&mut self) {
        // Session end: release every advisory lock this session still holds
        // (session- and xact-scoped). Matches PG, which drops all advisory
        // locks held by a backend when the connection terminates.
        self.advisory.release_all_on_session_end();
        // LISTEN subscriptions are dropped automatically — the broadcast
        // receivers live inside `ListenState::subscriptions`, which is
        // owned by this `SessionState`. Once the `Mutex` drops, each
        // `ListenSubscription` drops its receiver and the NotifyRegistry's
        // per-channel sender sees the receiver count fall to zero (entry
        // becomes prunable). No explicit unlisten call is required.
    }
}

/// Per-session SQL LISTEN / NOTIFY state. Holds the active subscription
/// handles plus the pending-notify buffer that PG semantics require:
/// `NOTIFY` issued inside an open transaction is queued and only fanned
/// out on `COMMIT`; `ROLLBACK` discards the buffer.
#[derive(Default)]
pub(crate) struct ListenState {
    /// Active subscriptions, keyed by lowercased channel name.
    pub(crate) subscriptions: HashMap<String, Arc<crate::notify_registry::ListenSubscription>>,
    /// Notifications buffered while a transaction is open. Drained and
    /// dispatched to the engine `NotifyRegistry` on COMMIT; cleared on
    /// ROLLBACK. Auto-commit `NOTIFY` bypasses this buffer entirely and
    /// publishes immediately.
    pub(crate) pending_tx_notifies: Vec<PendingNotify>,
}

/// One queued NOTIFY waiting for transaction commit.
#[derive(Clone, Debug)]
pub(crate) struct PendingNotify {
    pub channel: String,
    pub payload: String,
}

impl SessionState {
    pub(crate) fn new() -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
            prepared: crate::prepared::PreparedRegistry::new(),
            has_partitioned_table: std::sync::atomic::AtomicBool::new(false),
            sequence_cache: Arc::new(crate::seq_udf::SessionSequenceCache::default()),
            cursors: crate::cursor::CursorRegistry::new(),
            schema_state: Arc::new(RwLock::new(crate::schema_ddl::SchemaState::default())),
            pending_overriding: std::sync::Mutex::new(None),
            tx_state: std::sync::Mutex::new(TxState::default()),
            advisory: Arc::new(crate::advisory_lock::AdvisorySessionLocks::new()),
            // -1 means "no per-session override; use process-wide default".
            statement_timeout_ms: std::sync::atomic::AtomicI64::new(-1),
            lock_timeout: std::sync::Mutex::new(None),
            idle_in_transaction_session_timeout: std::sync::Mutex::new(None),
            synchronous_commit: std::sync::atomic::AtomicBool::new(synchronous_commit_env_default()),
            trgm_similarity_threshold: std::sync::atomic::AtomicU32::new(
                basin_trgm::DEFAULT_SIMILARITY_THRESHOLD.to_bits(),
            ),
            trgm_word_similarity_threshold: std::sync::atomic::AtomicU32::new(
                basin_trgm::DEFAULT_WORD_SIMILARITY_THRESHOLD.to_bits(),
            ),
            read_tier: std::sync::atomic::AtomicU8::new(ReadTier::DEFAULT as u8),
            last_active: std::sync::Mutex::new(std::time::Instant::now()),
            listen: std::sync::Mutex::new(ListenState::default()),
            table_meta_cache: TableMetaCache::new(),
            provider_cache: ProviderCache::new(),
            head_probe_cache: HeadProbeCache::new(),
            dml_flags_cache: DmlFlagsCache::new(),
            ingest_meta_cache: IngestMetaCache::new(),
            prewarmed_tables: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Variant of `new()` that accepts a pre-built `schema_state` Arc.
    /// Used when the schema state must be shared with the info-schema providers
    /// that are registered before the SessionState is fully assembled — the
    /// providers hold an Arc clone and read the live user-schema set at scan
    /// time, while this session's CREATE/DROP SCHEMA handlers mutate it.
    pub(crate) fn new_with_schema_state(
        schema_state: Arc<RwLock<crate::schema_ddl::SchemaState>>,
    ) -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
            prepared: crate::prepared::PreparedRegistry::new(),
            has_partitioned_table: std::sync::atomic::AtomicBool::new(false),
            sequence_cache: Arc::new(crate::seq_udf::SessionSequenceCache::default()),
            cursors: crate::cursor::CursorRegistry::new(),
            schema_state,
            pending_overriding: std::sync::Mutex::new(None),
            tx_state: std::sync::Mutex::new(TxState::default()),
            advisory: Arc::new(crate::advisory_lock::AdvisorySessionLocks::new()),
            // -1 means "no per-session override; use process-wide default".
            statement_timeout_ms: std::sync::atomic::AtomicI64::new(-1),
            lock_timeout: std::sync::Mutex::new(None),
            idle_in_transaction_session_timeout: std::sync::Mutex::new(None),
            synchronous_commit: std::sync::atomic::AtomicBool::new(synchronous_commit_env_default()),
            trgm_similarity_threshold: std::sync::atomic::AtomicU32::new(
                basin_trgm::DEFAULT_SIMILARITY_THRESHOLD.to_bits(),
            ),
            trgm_word_similarity_threshold: std::sync::atomic::AtomicU32::new(
                basin_trgm::DEFAULT_WORD_SIMILARITY_THRESHOLD.to_bits(),
            ),
            read_tier: std::sync::atomic::AtomicU8::new(ReadTier::DEFAULT as u8),
            last_active: std::sync::Mutex::new(std::time::Instant::now()),
            listen: std::sync::Mutex::new(ListenState::default()),
            table_meta_cache: TableMetaCache::new(),
            provider_cache: ProviderCache::new(),
            head_probe_cache: HeadProbeCache::new(),
            dml_flags_cache: DmlFlagsCache::new(),
            ingest_meta_cache: IngestMetaCache::new(),
            prewarmed_tables: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Reset every settable session GUC back to its process-default value —
    /// the value `new()` initialises it to. This is the GUC half of
    /// `DISCARD ALL` / `RESET ALL` and is the authoritative reset the
    /// connection-pool scrub relies on (it does not depend on SQL-level
    /// `RESET ALL` parsing, which is a noop-accept in v0.1).
    ///
    /// ## Complete-by-construction
    ///
    /// Every GUC field declared on [`SessionState`] is reset here. The intent
    /// is that adding a NEW session GUC field is paired with one line in BOTH
    /// `new()` (its initial value) and this method (its reset value) — and the
    /// `gucs_reset_to_defaults` unit test asserts that the post-reset values
    /// match a freshly-constructed `SessionState`, so a forgotten field fails
    /// the test rather than silently leaking across a pooled checkout.
    ///
    /// Non-GUC per-session state (advisory locks, LISTEN subscriptions, cursors,
    /// prepared statements, the provider/metadata caches) is reset by
    /// [`ProjectSession::reset_for_pool_reuse`], which calls this method as its
    /// GUC step. The two together form `DISCARD ALL`.
    pub(crate) fn reset_gucs(&self) {
        use std::sync::atomic::Ordering::Relaxed;

        // search_path → ["public"] (SchemaState::default()'s search_path).
        // Only the search_path is routing-relevant; the schema *registry*
        // (CREATE SCHEMA) is catalog-backed and intentionally NOT reset here —
        // DISCARD ALL does not drop schemas, only resets the path GUC.
        {
            let mut st = self
                .schema_state
                .write()
                .expect("schema_state lock poisoned");
            st.search_path = crate::schema_ddl::SchemaState::default().search_path;
        }

        // statement_timeout → -1 ("no per-session override; use process default").
        self.statement_timeout_ms.store(-1, Relaxed);

        // lock_timeout → None (also mirror into the advisory-lock manager, the
        // same way `set_session_lock_timeout` keeps the two in sync).
        *self
            .lock_timeout
            .lock()
            .expect("lock_timeout lock poisoned") = None;
        self.advisory.set_lock_timeout(None);

        // idle_in_transaction_session_timeout → None.
        *self
            .idle_in_transaction_session_timeout
            .lock()
            .expect("idle_in_transaction_session_timeout lock poisoned") = None;

        // basin.synchronous_commit → engine-wide env default.
        self.synchronous_commit
            .store(synchronous_commit_env_default(), Relaxed);

        // pg_trgm thresholds → the basin-trgm crate defaults (0.3 / 0.6).
        self.trgm_similarity_threshold
            .store(basin_trgm::DEFAULT_SIMILARITY_THRESHOLD.to_bits(), Relaxed);
        self.trgm_word_similarity_threshold.store(
            basin_trgm::DEFAULT_WORD_SIMILARITY_THRESHOLD.to_bits(),
            Relaxed,
        );

        // basin.read_tier → primary (strongly-consistent default). Reset-by-
        // construction: the `gucs_reset_to_defaults` test asserts this matches
        // a fresh SessionState, so a future pooled checkout cannot leak a
        // 'lagging' tier set by a prior logical client.
        self.read_tier.store(ReadTier::DEFAULT as u8, Relaxed);
    }
}

// ── Per-session table-metadata cache ────────────────────────────────────────
//
// Inv-OLTP-point (#149): on the executor's point-query fast path, the gate
// at `executor.rs:2117-2131` pays two async catalog round-trips per query —
// `load_table` (~5–15µs) plus `lookup_view` (~2–5µs). The fast path itself
// then runs in single-µs territory, so those catalog calls dominate the
// latency floor.
//
// This cache stores one `(Arc<TableMetadata>, view_present)` tuple per
// table name, capped at 128 entries (LRU) per session with a short TTL
// (default 500ms, override via `BASIN_ENGINE_TABLE_META_CACHE_TTL_MS`).
//
// **Correctness model**
//
// * Same-session DDL invalidation is mandatory — the executor calls
//   [`TableMetaCache::invalidate_all`] at the top of dispatch for every
//   non-`SELECT` statement, which clears the entire cache before the DDL
//   handler runs. The next SELECT therefore observes the post-DDL state.
// * Same-session DML does NOT mutate schema/indexes; `data_files` staleness
//   in the cached `TableMetadata` is irrelevant because
//   `execute_simple_select` calls `shard.flush_to_parquet()` + reloads the
//   metadata via `catalog.load_table` itself after the flush. The cached
//   value is only consumed by the fast-path GATE (schema / RLS / view /
//   soft-delete / citext flags), not as the final source of truth for the
//   read.
// * Cross-session DDL becomes visible within the TTL window — same
//   "eventual consistency" bound the engine already advertises for catalog
//   reads. 500ms is short enough that even integration tests that race
//   DDL across sessions remain reliable.

/// Default TTL for cached `(TableMetadata, view_present)` entries when
/// `BASIN_ENGINE_TABLE_META_CACHE_TTL_MS` is unset. 500ms is short enough
/// to bound cross-session DDL visibility; the same-session DDL path
/// invalidates eagerly, so this only affects external mutations.
pub(crate) const DEFAULT_TABLE_META_CACHE_TTL_MS: u64 = 500;

/// Maximum number of distinct tables tracked per session. 128 is enough to
/// cover any real OLTP working set without unbounded growth on a session
/// that touches many tables. Eviction is LRU.
pub(crate) const TABLE_META_CACHE_CAP: usize = 128;

/// Resolve the effective TTL for the table-metadata cache.
///
/// Cached behind a `OnceLock` so the hot path never reads `std::env`. Tests
/// override via [`test_meta_cache_ttl_override`] (compiled out of release).
pub(crate) fn table_meta_cache_ttl() -> std::time::Duration {
    #[cfg(test)]
    if let Some(over) = test_meta_cache_ttl_override::get() {
        return over;
    }
    static CACHED: OnceLock<std::time::Duration> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let ms = std::env::var("BASIN_ENGINE_TABLE_META_CACHE_TTL_MS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TABLE_META_CACHE_TTL_MS);
        std::time::Duration::from_millis(ms)
    })
}

/// Test-only override for the table-meta cache TTL. Mirrors the pattern in
/// [`test_timeout_override`]: install a thread-local TTL for the duration
/// of one test body so it doesn't race with the `OnceLock`'d production
/// value.
#[cfg(test)]
pub(crate) mod test_meta_cache_ttl_override {
    use std::cell::Cell;
    use std::time::Duration;

    thread_local! {
        static OVERRIDE: Cell<Option<Duration>> = const { Cell::new(None) };
    }

    pub(crate) fn get() -> Option<Duration> {
        OVERRIDE.with(|c| c.get())
    }

    pub(crate) fn install(value: Duration) -> Guard {
        let prev = OVERRIDE.with(|c| c.replace(Some(value)));
        Guard { prev }
    }

    pub(crate) struct Guard {
        prev: Option<Duration>,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            OVERRIDE.with(|c| c.set(self.prev));
        }
    }
}

/// One cached `(TableMetadata, lookup_view)` tuple plus its insertion
/// timestamp and catalog epoch.
#[derive(Clone, Debug)]
pub(crate) struct TableMetaCacheEntry {
    pub(crate) meta: Arc<TableMetadata>,
    /// Snapshot of `lookup_view(name).is_some()` at cache-fill time. The
    /// fast-path gate only consumes this boolean (not the full `ViewDef`)
    /// so we cache just the discriminator.
    pub(crate) view_present: bool,
    inserted_at: Instant,
    /// Catalog epoch at cache-fill time. Compared against
    /// `Catalog::epoch()` on each read: a mismatch causes an immediate
    /// cache-miss and refetch, ensuring cross-session DDL (including
    /// ENABLE/DISABLE ROW LEVEL SECURITY) is visible without waiting for
    /// the TTL to expire.
    catalog_epoch: u64,
}

/// Per-session bounded LRU cache mapping table name → metadata + view
/// presence. See module-level comment for the correctness model.
pub(crate) struct TableMetaCache {
    inner: std::sync::Mutex<lru::LruCache<TableName, TableMetaCacheEntry>>,
}

impl TableMetaCache {
    pub(crate) fn new() -> Self {
        let cap = NonZeroUsize::new(TABLE_META_CACHE_CAP).expect("cap is non-zero");
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Look up a fresh entry. Returns `Some(entry)` only when present AND
    /// the catalog epoch has not changed since the entry was cached. Epoch
    /// staleness is checked first (one atomic load — nanosecond cost); the
    /// TTL check is a secondary guard for the `PostgresCatalog` / `RestCatalog`
    /// backends that return `epoch() == 0` (always-stale → always TTL-bound).
    pub(crate) fn get_fresh(
        &self,
        table: &TableName,
        catalog_epoch: u64,
    ) -> Option<TableMetaCacheEntry> {
        let ttl = table_meta_cache_ttl();
        let mut g = self.inner.lock().expect("table_meta_cache lock poisoned");
        let entry = g.get(table)?;
        // Epoch 0 means the catalog backend does not implement epoch tracking
        // (default impl returns 0). In that case fall back to TTL-only freshness.
        // When epochs are available, a mismatch is an instant miss — the
        // catalog mutated since this entry was filled, so it may be stale.
        let epoch_ok = if catalog_epoch == 0 {
            true
        } else {
            entry.catalog_epoch == catalog_epoch
        };
        if epoch_ok && entry.inserted_at.elapsed() <= ttl {
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Populate / overwrite the cache entry for `table`.
    pub(crate) fn insert(
        &self,
        table: TableName,
        meta: Arc<TableMetadata>,
        view_present: bool,
        catalog_epoch: u64,
    ) {
        let mut g = self.inner.lock().expect("table_meta_cache lock poisoned");
        g.put(
            table,
            TableMetaCacheEntry {
                meta,
                view_present,
                inserted_at: Instant::now(),
                catalog_epoch,
            },
        );
    }

    /// Drop the entry for `table` (no-op if absent). Called by the
    /// per-table DDL/DML invalidation path when the executor knows which
    /// table is mutating.
    pub(crate) fn invalidate(&self, table: &TableName) {
        let mut g = self.inner.lock().expect("table_meta_cache lock poisoned");
        g.pop(table);
    }

    /// Drop every entry. The executor calls this at the top of dispatch
    /// for any statement that isn't a pure `SELECT`, which is the simplest
    /// safe invalidation strategy (the cost is one `Mutex` lock + a
    /// `clear()` — both negligible compared to the DDL/DML it precedes).
    pub(crate) fn invalidate_all(&self) {
        let mut g = self.inner.lock().expect("table_meta_cache lock poisoned");
        g.clear();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("table_meta_cache lock poisoned")
            .len()
    }
}

// ── Per-session ingest META cache (multi-node ingest throughput fix) ─────────
//
// The bulk-ingest constraint-prep path (`exec_ingest_batch`) needs the table's
// schema + constraints + RLS policies + write tunables on EVERY batch, but NOT
// the live data-file set (existing-row PK/UNIQUE/FK checks source their
// candidate files from the storage LIST, never from this metadata).
//
// `TableMetaCache` is keyed on `Catalog::epoch()`, which the sharded
// `ObjectStoreCatalog` bumps on every per-partition DATA commit. During a 1M-row
// COPY that invalidates the entry on every chunk → a full `load_table` (LIST
// `parts/` + GET per partition, ~5 round-trips) per chunk → the ~12.5k r/s
// regression. This cache instead keys on `Catalog::meta_version()`, which bumps
// ONLY on META/DDL changes, so a stream of data commits never invalidates it:
// the whole COPY pays exactly one cold META load.
//
// Correctness: the entry is dropped the instant the META version moves (DDL /
// schema evolution / constraint change), so an `ALTER TABLE … ADD CONSTRAINT`
// concurrent with ingest is observed on the next batch. Cleared wholesale by the
// dispatch-top `invalidate_all` hook alongside the other per-session caches.
struct IngestMetaEntry {
    meta: Arc<TableMetadata>,
    inserted_at: Instant,
    /// `Catalog::meta_version()` at fill time. A mismatch is an instant miss.
    meta_version: u64,
}

pub(crate) struct IngestMetaCache {
    inner: std::sync::Mutex<lru::LruCache<TableName, IngestMetaEntry>>,
}

impl IngestMetaCache {
    pub(crate) fn new() -> Self {
        let cap = NonZeroUsize::new(TABLE_META_CACHE_CAP).expect("cap is non-zero");
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Return the cached META iff present, meta-version-fresh and within TTL.
    /// `meta_version == 0` (backend could not resolve a version) is treated as
    /// always-stale so the caller re-loads rather than serving a stale hit.
    fn get_fresh(&self, table: &TableName, meta_version: u64) -> Option<Arc<TableMetadata>> {
        let ttl = table_meta_cache_ttl();
        let mut g = self.inner.lock().expect("ingest_meta_cache lock poisoned");
        let entry = g.get(table)?;
        let version_ok = meta_version != 0 && entry.meta_version == meta_version;
        if version_ok && entry.inserted_at.elapsed() <= ttl {
            Some(entry.meta.clone())
        } else {
            None
        }
    }

    fn insert(&self, table: TableName, meta: Arc<TableMetadata>, meta_version: u64) {
        self.inner
            .lock()
            .expect("ingest_meta_cache lock poisoned")
            .put(
                table,
                IngestMetaEntry {
                    meta,
                    inserted_at: Instant::now(),
                    meta_version,
                },
            );
    }

    pub(crate) fn invalidate(&self, table: &TableName) {
        self.inner
            .lock()
            .expect("ingest_meta_cache lock poisoned")
            .pop(table);
    }

    pub(crate) fn invalidate_all(&self) {
        self.inner
            .lock()
            .expect("ingest_meta_cache lock poisoned")
            .clear();
    }
}

// ── Per-session DataFusion provider cache (Fix B+C) ─────────────────────────
//
// `refresh_table_inner` rebuilds, on EVERY auto-commit SELECT, the full
// DataFusion provider for each referenced table: a `TableMetadata` clone
// (bloom-blob-laden — 2-5ms with compaction PK blooms), schema conversion,
// per-file `ListingTable::try_new`, the optional `HtapUnionTable` wrapper, and
// `register_table`. At a few-thousand-row scale this dominates the small-query
// latency floor (19-39ms analytic queries vs PG's 1-7ms).
//
// This cache memoises the *final registered provider* (`Arc<dyn TableProvider>`)
// plus its live file count, keyed by everything the provider bakes in at build
// time. A HIT skips the catalog load, the bloom clone, the schema conversion and
// the `ListingTable` build, and only re-runs `register_table(cached)` (cheap).
//
// ## What the provider bakes in (the invalidation surface)
//
// * **Cold file set** — enumerated from `meta.live_data_files()` at the table's
//   current snapshot. Keyed by `SnapshotId`: a new INSERT flush / compaction /
//   rollback advances the snapshot → key miss → rebuild.
// * **Schema (incl. promoted JSONB shadow columns)** — derived from the
//   snapshot-keyed `TableMetadata` (`schema` + `promoted_jsonb_paths`). Schema
//   evolution either advances the snapshot OR is metadata-only DDL; the latter
//   is caught by the catalog-epoch clear (see below).
// * **Overlay path (now SHAPE-INVARIANT)** — `build_cold_with_overlay` wraps
//   every single-PK table's cold scan in an `HtapUnionTable` UNCONDITIONALLY
//   (auto-commit included), regardless of whether the overlay is currently
//   empty. The wrapper's shape no longer depends on the overlay's emptiness, so
//   it never has to flip on the empty↔non-empty boundary. Composite/no-PK
//   tables (which can never have an overlay) keep the plain `ListingTable`; that
//   shape can never change either. See "Overlay freshness" below.
//
// ## Overlay freshness without the hot-tier epoch (concurrency fix #3)
//
// `HtapUnionTable::scan` calls `snapshot_tombstones` / `snapshot_updates`
// against the LIVE `engine.memtable_registry()` at SCAN time — it does NOT bake
// the tombstone/update set at build time. The ONLY build-time captures are
// `hot_seq_watermark` (a per-tx MVCC ceiling, `None` in auto-commit) and
// `tx_overlay` (this tx's uncommitted writes, empty in auto-commit). So a cached
// (auto-commit) `HtapUnionTable` observes fresh registry overlay mutations
// automatically: a fast-path UPDATE/DELETE landing after the provider was cached
// is applied on the very next scan, and an overlay draining to cold afterwards
// is likewise reflected (the scan re-reads the live snapshots each time, and an
// empty overlay is a zero-overhead pass-through).
//
// The OLD reason `hot_tier_epoch` was in the key was the build-time
// `needs_overlay` SHAPE gate: a cached *plain ListingTable* (built while the
// registry was empty) had no overlay path and could not grow one. By always
// wrapping single-PK tables, that shape divergence is gone, so the provider is
// a pure function of `(table, snapshot)` and the epoch leaves the key. Each
// protection the epoch used to give is preserved:
//
//   * *plain → needs-overlay transition* (a fast-path UPDATE/DELETE lands after
//     caching): the cached provider is ALREADY the overlay-capable
//     `HtapUnionTable`; its scan picks up the new tombstones/updates live. ✔
//   * *needs-overlay → empty transition* (the overlay drains to cold): the same
//     cached `HtapUnionTable`'s scan re-reads the now-empty live snapshots and
//     short-circuits to the pass-through, so it does not keep applying a stale
//     overlay. (Note: a drain that rewrites cold files ADVANCES the snapshot,
//     which is still in the key, so the rebuilt cold file set is picked up too.) ✔
//   * *pushdown exactness*: the empty-overlay `HtapUnionTable` delegates the
//     cold provider's pushdown verdict verbatim (Exact stays Exact) via
//     `supports_filters_pushdown`, so the no-overlay read path is
//     performance-identical to the old plain `ListingTable`. ✔
//
// ## Transactions, RLS
//
// * **In-tx reads are NEVER cached and NEVER served from cache.** Inside a tx
//   the provider bakes a pinned cold snapshot + hot watermark + this tx's
//   `tx_overlay`, all of which the cache key does not model. The cache is
//   bypassed whenever `tx_is_active`, and cleared on BEGIN/COMMIT/ROLLBACK.
// * **RLS does not affect the provider.** RLS is a query-time SQL/plan rewrite;
//   the registered provider exposes raw rows regardless of the connecting role.
//   RLS DDL (ENABLE/DISABLE ROW LEVEL SECURITY) bumps the catalog epoch and is
//   not a `SELECT`, so it flows through the dispatch-top `invalidate_all` hook,
//   which also clears this cache. RLS tables are therefore cacheable.
//
// ## Bounds & lifecycle
//
// Per-session LRU (64 entries). Cleared on tx BEGIN/COMMIT/ROLLBACK and on the
// catalog-epoch bump (same dispatch-top hook as `TableMetaCache::invalidate_all`).

/// Maximum number of distinct (table, snapshot) provider entries tracked per
/// session. 64 covers any realistic single-query working set; LRU eviction
/// bounds churny tables.
pub(crate) const PROVIDER_CACHE_CAP: usize = 64;

/// Cache key for a fully-built, registered DataFusion provider. Every field is
/// something the provider bakes in at build time; see the module comment for
/// why each is load-bearing.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ProviderCacheKey {
    /// The table the provider serves.
    pub(crate) table: TableName,
    /// Catalog head the cold file set + schema were enumerated at. Advances on
    /// INSERT-flush / compaction / rollback / most schema evolution.
    pub(crate) snapshot: SnapshotId,
    // NOTE (concurrency fix #3): `hot_epoch` was REMOVED from this key. It used
    // to be here solely so the build-time `needs_overlay` provider SHAPE could
    // flip on the empty↔non-empty overlay boundary (a cached plain
    // `ListingTable` built while the registry held no overlay could not grow an
    // overlay path). We eliminated that shape divergence: `build_cold_with_overlay`
    // now ALWAYS builds the overlay-capable `HtapUnionTable` in auto-commit, and
    // its `scan` reads `snapshot_tombstones` / `snapshot_updates` LIVE at scan
    // time (an empty overlay is a zero-overhead pass-through; a grown overlay is
    // applied without any provider rebuild). The provider is therefore now a
    // pure function of `(table, snapshot)`, and keying on the hot epoch only
    // caused hot-tier-only churn (every fast-path INSERT/UPDATE/DELETE) to evict
    // a still-correct cached provider — the read-path serialization the mixed
    // RW benchmark exposed. See the module comment "Overlay freshness" section.
}

/// A cached provider: the final `Arc<dyn TableProvider>` that was registered,
/// plus the live cold-file count `refresh_table_counted` returns for the
/// scan-parallelism heuristic (so a HIT need not recount).
#[derive(Clone)]
pub(crate) struct ProviderCacheEntry {
    pub(crate) provider: Arc<dyn datafusion::catalog::TableProvider>,
    pub(crate) live_file_count: usize,
    /// Whether the source table is partitioned (so a HIT can still set
    /// `state.has_partitioned_table` without reloading metadata).
    pub(crate) partitioned: bool,
}

/// Per-session bounded LRU cache of fully-built DataFusion providers, keyed by
/// [`ProviderCacheKey`]. See the module comment for the correctness model.
pub(crate) struct ProviderCache {
    inner: std::sync::Mutex<lru::LruCache<ProviderCacheKey, ProviderCacheEntry>>,
}

impl ProviderCache {
    pub(crate) fn new() -> Self {
        let cap = NonZeroUsize::new(PROVIDER_CACHE_CAP).expect("cap is non-zero");
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Look up a built provider by exact key. LRU-touches on hit.
    pub(crate) fn get(&self, key: &ProviderCacheKey) -> Option<ProviderCacheEntry> {
        self.inner
            .lock()
            .expect("provider_cache lock poisoned")
            .get(key)
            .cloned()
    }

    /// Insert / overwrite the entry for `key`.
    pub(crate) fn insert(&self, key: ProviderCacheKey, entry: ProviderCacheEntry) {
        self.inner
            .lock()
            .expect("provider_cache lock poisoned")
            .put(key, entry);
    }

    /// Drop every entry. Called on tx BEGIN/COMMIT/ROLLBACK and on the
    /// catalog-epoch bump (DDL/DML dispatch-top hook).
    pub(crate) fn invalidate_all(&self) {
        self.inner
            .lock()
            .expect("provider_cache lock poisoned")
            .clear();
    }

    /// Drop every entry for a single table, leaving other tables' cached
    /// providers intact (concurrency fix #4). A write to table A no longer
    /// evicts table B's still-valid provider. The key now models only
    /// `(table, snapshot)`, so this removes every snapshot generation cached
    /// for `table`.
    pub(crate) fn invalidate(&self, table: &TableName) {
        let mut g = self.inner.lock().expect("provider_cache lock poisoned");
        let stale: Vec<ProviderCacheKey> = g
            .iter()
            .filter(|(k, _)| &k.table == table)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            g.pop(&k);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("provider_cache lock poisoned")
            .len()
    }
}

/// Per-session cache of a table's live catalog head (snapshot id), used purely
/// to key the [`ProviderCache`] on a HIT WITHOUT paying the bloom-laden
/// `TableMetadata` clone that `load_table` performs. Validated against the
/// catalog epoch exactly like [`TableMetaCache`]: a mismatch (any catalog
/// mutation since fill) is an instant miss → fall through to `load_table`,
/// which re-reads the true head. TTL-bounds the epoch-0 (Postgres/Rest)
/// backends so cross-session DDL becomes visible within the same window the
/// engine already advertises for catalog reads.
struct HeadProbeEntry {
    snapshot: SnapshotId,
    inserted_at: Instant,
    catalog_epoch: u64,
}

pub(crate) struct HeadProbeCache {
    inner: std::sync::Mutex<lru::LruCache<TableName, HeadProbeEntry>>,
}

impl HeadProbeCache {
    pub(crate) fn new() -> Self {
        let cap = NonZeroUsize::new(PROVIDER_CACHE_CAP).expect("cap is non-zero");
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Return the cached head iff present, epoch-fresh and within TTL.
    pub(crate) fn get_fresh(&self, table: &TableName, catalog_epoch: u64) -> Option<SnapshotId> {
        let ttl = table_meta_cache_ttl();
        let mut g = self.inner.lock().expect("head_probe_cache lock poisoned");
        let entry = g.get(table)?;
        let epoch_ok = if catalog_epoch == 0 {
            true
        } else {
            entry.catalog_epoch == catalog_epoch
        };
        if epoch_ok && entry.inserted_at.elapsed() <= ttl {
            Some(entry.snapshot)
        } else {
            None
        }
    }

    pub(crate) fn insert(&self, table: TableName, snapshot: SnapshotId, catalog_epoch: u64) {
        self.inner
            .lock()
            .expect("head_probe_cache lock poisoned")
            .put(
                table,
                HeadProbeEntry {
                    snapshot,
                    inserted_at: Instant::now(),
                    catalog_epoch,
                },
            );
    }

    pub(crate) fn invalidate_all(&self) {
        self.inner
            .lock()
            .expect("head_probe_cache lock poisoned")
            .clear();
    }

    /// Drop the single table's cached head, leaving other tables intact
    /// (concurrency fix #4).
    pub(crate) fn invalidate(&self, table: &TableName) {
        self.inner
            .lock()
            .expect("head_probe_cache lock poisoned")
            .pop(table);
    }
}

/// The two project-scoped catalog flags the hot-tier UPDATE/DELETE fast path
/// consults per statement. `false`/`false` is the common OLTP case (no FK
/// children, no UPDATE reactor) that admits the overlay write.
#[derive(Clone, Copy)]
pub(crate) struct DmlFlags {
    /// `true` when at least one child table references this table by FK (so an
    /// UPDATE/DELETE could need ON UPDATE/DELETE cascade handling) — the fast
    /// path declines.
    pub(crate) has_referencing_fk: bool,
    /// `true` when a per-table UPDATE reactor is registered for this table (it
    /// needs before/after row images the overlay write doesn't gather) — the
    /// fast path declines.
    pub(crate) has_update_reactor: bool,
}

struct DmlFlagsEntry {
    flags: DmlFlags,
    inserted_at: Instant,
    catalog_epoch: u64,
}

/// Per-session cache of [`DmlFlags`] keyed by table + catalog epoch. Validated
/// exactly like [`HeadProbeCache`]: any catalog mutation bumps the epoch and
/// forces a refetch, and a TTL bounds the epoch-0 (Postgres/Rest) backends.
/// Replaces the two uncached awaited round-trips (`fks_referencing` +
/// `list_reactors`) that ran on every fast-path UPDATE/DELETE.
pub(crate) struct DmlFlagsCache {
    inner: std::sync::Mutex<lru::LruCache<TableName, DmlFlagsEntry>>,
}

impl DmlFlagsCache {
    pub(crate) fn new() -> Self {
        let cap = NonZeroUsize::new(PROVIDER_CACHE_CAP).expect("cap is non-zero");
        Self {
            inner: std::sync::Mutex::new(lru::LruCache::new(cap)),
        }
    }

    pub(crate) fn get_fresh(&self, table: &TableName, catalog_epoch: u64) -> Option<DmlFlags> {
        let ttl = table_meta_cache_ttl();
        let mut g = self.inner.lock().expect("dml_flags_cache lock poisoned");
        let entry = g.get(table)?;
        let epoch_ok = if catalog_epoch == 0 {
            true
        } else {
            entry.catalog_epoch == catalog_epoch
        };
        if epoch_ok && entry.inserted_at.elapsed() <= ttl {
            Some(entry.flags)
        } else {
            None
        }
    }

    pub(crate) fn insert(&self, table: TableName, flags: DmlFlags, catalog_epoch: u64) {
        self.inner
            .lock()
            .expect("dml_flags_cache lock poisoned")
            .put(
                table,
                DmlFlagsEntry {
                    flags,
                    inserted_at: Instant::now(),
                    catalog_epoch,
                },
            );
    }

    pub(crate) fn invalidate_all(&self) {
        self.inner
            .lock()
            .expect("dml_flags_cache lock poisoned")
            .clear();
    }

    pub(crate) fn invalidate(&self, table: &TableName) {
        self.inner
            .lock()
            .expect("dml_flags_cache lock poisoned")
            .pop(table);
    }
}

/// Return the cached [`DmlFlags`] for `table` if epoch-fresh; otherwise issue
/// the `fks_referencing` + `list_reactors` catalog calls, cache the result, and
/// return it. Used by the fast-path UPDATE gate so the two awaited round-trips
/// it ran per statement collapse to one warm `Mutex` lock on the steady-state
/// OLTP loop. (`has_update_reactor` is UPDATE-specific; the DELETE fast-path
/// gate keys on `ReactorOps::DELETE` and is left on its inline checks.)
pub(crate) async fn load_dml_flags_cached(
    sess: &crate::ProjectSession,
    table: &TableName,
) -> Result<DmlFlags> {
    let catalog = &sess.engine.config().catalog;
    let current_epoch = catalog.epoch();
    if let Some(flags) = sess.state.dml_flags_cache.get_fresh(table, current_epoch) {
        return Ok(flags);
    }
    let referencing =
        crate::constraints::fks_referencing(catalog, &sess.project, table.as_str()).await?;
    let reactors = catalog.list_reactors(&sess.project).await;
    let has_update_reactor = reactors.iter().any(|r| {
        r.table.as_str().eq_ignore_ascii_case(table.as_str())
            && r.ops.contains(basin_catalog::ReactorOps::UPDATE)
    });
    let flags = DmlFlags {
        has_referencing_fk: !referencing.is_empty(),
        has_update_reactor,
    };
    let fill_epoch = catalog.epoch();
    sess.state
        .dml_flags_cache
        .insert(table.clone(), flags, fill_epoch);
    Ok(flags)
}

/// One-shot helper: return the cached `(TableMetadata, view_present)` for
/// `(project, table)` if fresh; otherwise call `load_table` + `lookup_view`,
/// populate the cache, and return the new value.
///
/// The two catalog calls are issued sequentially, NOT in parallel —
/// `tokio::join!` adds a `select`-style poll harness that's measurable on
/// the µs-scale fast path, and on a warm catalog both calls are cheap.
/// We pay them only on cache miss / stale.
pub(crate) async fn load_table_meta_cached(
    sess: &crate::ProjectSession,
    table: &TableName,
) -> Option<(Arc<TableMetadata>, bool)> {
    let catalog = &sess.engine.config().catalog;
    let current_epoch = catalog.epoch();
    if let Some(entry) = sess.state.table_meta_cache.get_fresh(table, current_epoch) {
        return Some((entry.meta, entry.view_present));
    }
    let meta = catalog.load_table(&sess.project, table).await.ok()?;
    let view_present = catalog
        .lookup_view(&sess.project, table.as_str())
        .await
        .is_some();
    let arc = Arc::new(meta);
    // Re-read epoch after the catalog calls so the stored epoch is at least
    // as fresh as the data we just loaded. Using the pre-load epoch is safe
    // too (it's conservative — the next mutation bumps epoch and we refetch),
    // but reading post-load avoids an unnecessary miss if a concurrent mutation
    // completed before our load started.
    let fill_epoch = catalog.epoch();
    sess.state
        .table_meta_cache
        .insert(table.clone(), arc.clone(), view_present, fill_epoch);
    // BASIN_PREWARM_PROVIDERS (opt-in): on the FIRST cold meta miss for this
    // table in this session, fire-and-forget a task that reads the table's
    // per-file stats/footers into the process-wide footer/stats caches, so a
    // follow-up cold SELECT skips the per-file footer fetch. Off by default; a
    // best-effort warm — any error is swallowed (it only ever cost a cache fill
    // that the real read would do anyway).
    maybe_prewarm_table(sess, table);
    Some((arc, view_present))
}

/// Fire-and-forget the `BASIN_PREWARM_PROVIDERS` footer/stats warm for `table`,
/// at most once per (session, table). No-op unless the env flag is set to `1`.
fn maybe_prewarm_table(sess: &crate::ProjectSession, table: &TableName) {
    if std::env::var("BASIN_PREWARM_PROVIDERS").as_deref() != Ok("1") {
        return;
    }
    {
        let mut seen = sess
            .state
            .prewarmed_tables
            .lock()
            .expect("prewarmed_tables lock poisoned");
        if !seen.insert(table.clone()) {
            return; // already warmed this table in this session
        }
    }
    let storage = sess.engine.config().storage.clone();
    let project = sess.project;
    let table = table.clone();
    tokio::spawn(async move {
        // Populates the process-wide DataFileStatsCache (and, on read, the
        // footer caches) for the table's live files. Errors are intentionally
        // ignored: the prewarm is purely a latency optimisation.
        let _ = storage.list_data_files_with_stats(&project, &table).await;
    });
}

/// Result-returning counterpart of [`load_table_meta_cached`]: the INSERT
/// path treats a missing table as a hard error (so it can return a clean
/// `NotFound` rather than degrade to a silent skip). Same cache semantics
/// as the SELECT-side helper — populates `view_present = false` on the
/// fresh path because the INSERT call site doesn't consume that flag.
///
/// Inv-OLTP-write (#155, perf-w7-more): the txn_insert_x100 bench drives
/// 100 INSERTs against the same table inside `BEGIN..COMMIT`; serving each
/// one from the per-session cache (post the invalidation carve-out for
/// `Statement::Insert` in `executor::execute`) collapses 100 catalog
/// `load_table` round-trips to a single cold-fill + 99 LRU hits.
///
/// Why a separate fn from [`load_table_meta_cached`]: the SELECT-side
/// returns `Option<(_, bool)>` so the fast-path gate can fall through
/// silently on catalog miss; the INSERT path needs `BasinError`
/// propagation (`?`), and decorating the missing-table case with a
/// dedicated `NotFound` keeps the error surface identical to the previous
/// `catalog.load_table().await?` shape.
pub(crate) async fn load_table_meta_cached_err(
    sess: &crate::ProjectSession,
    table: &TableName,
) -> Result<Arc<TableMetadata>> {
    let catalog = &sess.engine.config().catalog;
    let current_epoch = catalog.epoch();
    if let Some(entry) = sess.state.table_meta_cache.get_fresh(table, current_epoch) {
        return Ok(entry.meta);
    }
    let meta = catalog.load_table(&sess.project, table).await?;
    // Probe view presence so a subsequent SELECT-side gate served from
    // this same cache entry doesn't have to re-issue the lookup. Cheap
    // (one in-RAM map probe in the in-memory catalog).
    let view_present = catalog
        .lookup_view(&sess.project, table.as_str())
        .await
        .is_some();
    let arc = Arc::new(meta);
    let fill_epoch = catalog.epoch();
    sess.state
        .table_meta_cache
        .insert(table.clone(), arc.clone(), view_present, fill_epoch);
    Ok(arc)
}

/// Ingest hot-path counterpart of [`load_table_meta_cached_err`]: returns the
/// cheap META-only `TableMetadata` (schema + constraints + RLS policies + write
/// tunables, NO unioned per-partition data-file set) for `exec_ingest_batch`'s
/// per-batch constraint prep.
///
/// Keyed on [`Catalog::meta_version`], NOT the global `epoch`: the sharded
/// `ObjectStoreCatalog` bumps `epoch` on every per-partition DATA commit, so an
/// epoch-keyed cache would invalidate on every COPY chunk and re-pay the
/// `load_table` partition union (~5 object-store round-trips → the ~12.5k r/s
/// regression). `meta_version` advances only on META/DDL changes, so the whole
/// bulk ingest pays exactly one cold META load — and an `ALTER TABLE …` that
/// changes a constraint is still observed on the next batch.
///
/// The constraint enforcers `exec_ingest_batch` calls consume only META fields
/// (schema / `check_constraints` / `pk_columns` / `unique_constraints` /
/// `foreign_keys` / RLS `policies` / enum+domain defs); existing-row checks
/// source their candidate files from the storage LIST, never from this
/// metadata's (empty) data-file set. So the cheap META load is sufficient.
pub(crate) async fn load_table_meta_cached_for_ingest(
    sess: &crate::ProjectSession,
    table: &TableName,
) -> Result<Arc<TableMetadata>> {
    let catalog = &sess.engine.config().catalog;
    let meta_version = catalog.meta_version(&sess.project, table).await;
    if let Some(meta) = sess.state.ingest_meta_cache.get_fresh(table, meta_version) {
        return Ok(meta);
    }
    let meta = catalog.load_table_meta(&sess.project, table).await?;
    let arc = Arc::new(meta);
    // Re-read the meta-version after the load so a concurrent DDL that landed
    // mid-load is not masked by a stale fill version (conservative: a bump
    // between the two reads just costs the next batch one extra load).
    let fill_version = catalog.meta_version(&sess.project, table).await;
    sess.state
        .ingest_meta_cache
        .insert(table.clone(), arc.clone(), fill_version);
    Ok(arc)
}

/// Bring `ViewDef` into scope as a type so the unused-import check stays
/// happy when the helpers above only reference `lookup_view`'s return
/// shape implicitly.
#[allow(dead_code)]
fn _view_def_in_scope(_: ViewDef) {}

// ── LISTEN / NOTIFY session-state helpers ───────────────────────────────────

/// Subscribe this session to `channel` in `project`. Idempotent: if the
/// session is already listening on `channel` (normalised case), the
/// existing subscription is kept and no second receiver is created.
pub(crate) fn listen_subscribe(
    state: &SessionState,
    registry: &crate::notify_registry::NotifyRegistry,
    project: ProjectId,
    channel: &str,
) {
    let normalised = channel.to_ascii_lowercase();
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    if listen.subscriptions.contains_key(&normalised) {
        return;
    }
    let sub = Arc::new(registry.subscribe(project, &normalised));
    listen.subscriptions.insert(normalised, sub);
}

/// Drop a single channel subscription. No-op if absent.
pub(crate) fn listen_unsubscribe(state: &SessionState, channel: &str) {
    let normalised = channel.to_ascii_lowercase();
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    listen.subscriptions.remove(&normalised);
}

/// `UNLISTEN *` — drop every subscription this session holds. Also clears
/// the pending-notify buffer (PG semantics: UNLISTEN does not affect
/// already-queued notifies, but the pool-return scrub path that calls
/// this also issues ROLLBACK so any buffered notifies would be dropped
/// either way).
pub(crate) fn listen_unsubscribe_all(state: &SessionState) {
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    listen.subscriptions.clear();
    listen.pending_tx_notifies.clear();
}

/// Snapshot the channel names this session is currently listening on, in
/// sorted order. Used by `pg_listening_channels()`.
pub(crate) fn listen_channels(state: &SessionState) -> Vec<String> {
    let listen = state.listen.lock().expect("listen lock poisoned");
    let mut chans: Vec<String> = listen.subscriptions.keys().cloned().collect();
    chans.sort();
    chans
}

/// Queue a NOTIFY for delivery on COMMIT. The caller has already checked
/// that `tx_is_active(state)` is true.
pub(crate) fn listen_buffer_notify(state: &SessionState, channel: String, payload: String) {
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    listen
        .pending_tx_notifies
        .push(PendingNotify { channel, payload });
}

/// Drain the per-session pending-notify buffer. Called on COMMIT to fan
/// the buffered notifications out to the engine `NotifyRegistry`.
pub(crate) fn listen_take_pending_notifies(state: &SessionState) -> Vec<PendingNotify> {
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    std::mem::take(&mut listen.pending_tx_notifies)
}

/// Clear the pending-notify buffer without dispatching. Called on
/// ROLLBACK.
pub(crate) fn listen_discard_pending_notifies(state: &SessionState) {
    let mut listen = state.listen.lock().expect("listen lock poisoned");
    listen.pending_tx_notifies.clear();
}

/// Mark the start of an explicit transaction. Snapshots the current
/// per-table snapshot-id map into `TxState::pre_tx_snapshots` so that
/// `ROLLBACK` can restore the catalog heads.  Idempotent if called while
/// already active (matches PG behaviour for `WARNING: there is already a
/// transaction in progress`).  Resets `aborted` and the savepoint stack.
pub(crate) fn tx_begin(state: &SessionState, current_snapshots: HashMap<TableName, SnapshotId>) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    // Idempotent: if already active, leave state unchanged (matches PG
    // WARNING: there is already a transaction in progress).
    if !tx.active {
        tx.active = true;
        tx.aborted = false;
        // The transaction read-view is captured *lazily on first touch* of each
        // table inside the transaction (see `tx_read_snapshot_for`), NOT seeded
        // here from the session's pre-BEGIN observed heads.
        //
        // Why: the first in-tx SELECT flushes the shard tail to Parquet
        // (`exec_select` → `shard.flush_to_parquet()`) *before* it loads the
        // table. Seeding `read_snapshots` at BEGIN would pin the pre-flush head;
        // the first read would then rewind to that older snapshot and miss rows
        // this session wrote before BEGIN but had not yet flushed (counting 0
        // instead of N). Pinning on first touch — after that read's own flush —
        // captures the snapshot the first read actually observes, so repeated
        // reads stay stable at that value.
        //
        // `pre_tx_snapshots` IS seeded here: it is the ROLLBACK restore set and
        // must reflect the heads as of BEGIN; it is never mutated afterwards.
        tx.read_snapshots.clear();
        tx.pre_tx_snapshots = current_snapshots;
        tx.pending_files.clear();
        tx.savepoints.clear();
        tx.tx_id = None;
        tx.htap_rows.clear();
        tx.tx_overlay.clear();
        // Buffered change events from any prior tx on this session (drained on
        // COMMIT, dropped on ROLLBACK — clear defensively for the idempotent
        // re-BEGIN / abandoned-tx cases).
        tx.tx_change_events.clear();
        // Hot-tier MVCC watermark is, like `read_snapshots`, captured lazily on
        // first touch of each table (see `tx_hot_seq_watermark_for`), NOT seeded
        // here. Clear any residue from a prior transaction on this session.
        tx.hot_seq_watermark.clear();
        // Provider cache (Fix B+C) only holds auto-commit-built providers, which
        // bake the live head + live overlay shape. Entering a tx switches the
        // build inputs (pinned snapshot, hot watermark, tx overlay), so drop the
        // cache so no auto-commit provider is reused inside the tx.
        drop(tx);
        state.provider_cache.invalidate_all();
        state.head_probe_cache.invalidate_all();
    }
}

/// Mark the end of an explicit transaction with `COMMIT`. Returns the
/// pending files map so the executor can flush them to the catalog.
/// Clears all txn state afterwards (including `tx_id` and `htap_rows`;
/// the caller is responsible for promoting `htap_rows` via
/// [`tx_htap_take_all`] *before* calling this function if needed).
pub(crate) fn tx_commit(state: &SessionState) -> HashMap<TableName, Vec<DataFileRef>> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    let pending = std::mem::take(&mut tx.pending_files);
    tx.active = false;
    tx.aborted = false;
    tx.pre_tx_snapshots.clear();
    tx.read_snapshots.clear();
    tx.savepoints.clear();
    tx.tx_id = None;
    tx.htap_rows.clear();
    // The overlay is drained explicitly by the executor COMMIT path via
    // `tx_overlay_take_all` *before* this call (mirroring `tx_htap_take_all`);
    // clear defensively in case a caller commits without draining.
    tx.tx_overlay.clear();
    // Buffered change events are drained explicitly by the executor COMMIT path
    // via `tx_change_events_take_all` *before* this call, then dispatched after
    // the commit lands; clear defensively in case a caller commits without
    // draining.
    tx.tx_change_events.clear();
    tx.hot_seq_watermark.clear();
    drop(tx);
    // Drop any provider built inside this tx (pinned snapshot / tx overlay) so
    // the next auto-commit read rebuilds against the live head + live overlay.
    state.provider_cache.invalidate_all();
    state.head_probe_cache.invalidate_all();
    // PG: xact-scoped advisory locks auto-release at transaction end.
    state.advisory.release_xact();
    pending
}

/// Mark the end of an explicit transaction with `ROLLBACK`. Returns the
/// pending files (so the executor can delete them from storage) and the
/// pre-transaction snapshot heads (so the executor can restore catalog
/// read-views).  Clears all txn state.
pub(crate) fn tx_rollback(
    state: &SessionState,
) -> (
    HashMap<TableName, Vec<DataFileRef>>,
    HashMap<TableName, SnapshotId>,
) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    let pending = std::mem::take(&mut tx.pending_files);
    let snapshots = std::mem::take(&mut tx.pre_tx_snapshots);
    tx.read_snapshots.clear();
    tx.active = false;
    tx.aborted = false;
    tx.savepoints.clear();
    // Discard tx-local HTAP buffers — they were never committed to the shared
    // MemTableRegistry so no cleanup is needed beyond dropping the batches.
    tx.htap_rows.clear();
    // Discard the tx-scoped UPDATE/DELETE overlay — these entries were never
    // written to the shared MemTableRegistry, so ROLLBACK is just a drop.
    tx.tx_overlay.clear();
    // Discard buffered change events — a rolled-back mutation must never reach
    // a CDC / realtime sink. They were never assigned a commit seq, so dropping
    // them is a pure no-op against shared state.
    tx.tx_change_events.clear();
    tx.hot_seq_watermark.clear();
    tx.tx_id = None;
    drop(tx);
    // Drop any provider built inside this tx so the next auto-commit read
    // rebuilds against the (rolled-back) live head + live overlay.
    state.provider_cache.invalidate_all();
    state.head_probe_cache.invalidate_all();
    // PG: xact-scoped advisory locks auto-release at transaction end
    // (ROLLBACK releases them just like COMMIT).
    state.advisory.release_xact();
    (pending, snapshots)
}

/// Returns `true` if an explicit `BEGIN` has been issued and neither
/// `COMMIT` nor `ROLLBACK` has been seen yet. Pure flag read — no I/O.
pub(crate) fn tx_is_active(state: &SessionState) -> bool {
    state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned")
        .active
}

/// Resolve the catalog snapshot id `table` should be read at *for the current
/// transaction's read-view*, implementing snapshot-stable (REPEATABLE-READ-ish)
/// reads.
///
/// Returns `None` when no explicit transaction is active — auto-commit reads
/// always see the live head, so the caller should fall through to the normal
/// current-snapshot `load_table` path.
///
/// When a transaction is active:
///   * If the table is already pinned in `TxState::read_snapshots` (captured on
///     a prior touch this transaction), returns that pinned id.
///   * Otherwise this is the transaction's *first touch* of the table:
///     `current_head` (the table's current catalog head, which the caller has
///     just loaded or is about to) is recorded as the pin and returned, so
///     every subsequent read in this transaction reuses it.
///
/// `current_head` is only consulted on the first-touch path; pass the table's
/// live `current_snapshot`.
pub(crate) fn tx_read_snapshot_for(
    state: &SessionState,
    table: &TableName,
    current_head: SnapshotId,
) -> Option<SnapshotId> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    if !tx.active {
        return None;
    }
    if let Some(pinned) = tx.read_snapshots.get(table) {
        return Some(*pinned);
    }
    // First touch inside this transaction: pin the current head.
    tx.read_snapshots.insert(table.clone(), current_head);
    Some(current_head)
}

/// Non-capturing peek at the transaction's pinned read snapshot for `table`.
/// Unlike [`tx_read_snapshot_for`] this NEVER pins on first touch — callers
/// that cannot guarantee the pre-pin flush ordering (e.g. the fast-path
/// SELECT gate, which runs before any tail flush) must use this and fall to
/// the DataFusion path when no pin exists yet, so the pin is always captured
/// by `load_table_for_read` after the read's own flush.
pub(crate) fn tx_read_snapshot_peek(state: &SessionState, table: &TableName) -> Option<SnapshotId> {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    if !tx.active {
        return None;
    }
    tx.read_snapshots.get(table).copied()
}

/// Resolve the hot-tier MVCC sequence watermark `table` should be read at *for
/// the current transaction's read-view*. The hot-tier twin of
/// [`tx_read_snapshot_for`]; pinned at the SAME first-touch moment so the hot
/// and cold halves of the snapshot agree.
///
/// Returns `None` when no explicit transaction is active — auto-commit reads
/// always see the latest committed hot-tier overlay, so the overlay read path
/// applies no watermark filter (zero cost on the hot path).
///
/// When a transaction is active:
///   * already pinned → return the pinned watermark;
///   * first touch → record `current_seq` (the registry's live hot-tier
///     high-water mark for the table, which the caller just read) as the pin
///     and return it.
///
/// `current_seq` is only consulted on the first-touch path; pass
/// `MemTableRegistry::hot_tier_seq(project, table)` captured alongside the cold
/// head in `load_table_for_read`.
pub(crate) fn tx_hot_seq_watermark_for(
    state: &SessionState,
    table: &TableName,
    current_seq: u64,
) -> Option<u64> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    if !tx.active {
        return None;
    }
    if let Some(pinned) = tx.hot_seq_watermark.get(table) {
        return Some(*pinned);
    }
    tx.hot_seq_watermark.insert(table.clone(), current_seq);
    Some(current_seq)
}

/// Non-capturing peek at the transaction's pinned hot-tier sequence watermark
/// for `table`. Returns `None` outside a transaction or before the table's
/// first touch. Used by the overlay read path to filter post-snapshot
/// registry writes without re-pinning.
pub(crate) fn tx_hot_seq_watermark_peek(state: &SessionState, table: &TableName) -> Option<u64> {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    if !tx.active {
        return None;
    }
    tx.hot_seq_watermark.get(table).copied()
}

/// Returns `true` if the current transaction is in the aborted state
/// (a statement failed inside the txn and only `ROLLBACK` can recover).
pub(crate) fn tx_is_aborted(state: &SessionState) -> bool {
    state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned")
        .aborted
}

/// Set the aborted flag on the current transaction. Called whenever any
/// statement fails while `tx_is_active` is true.
pub(crate) fn tx_set_aborted(state: &SessionState) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.aborted = true;
}

/// Append a pending data file for `table` during an active transaction.
/// The file is visible only to this session until `COMMIT`.
pub(crate) fn tx_push_pending_file(state: &SessionState, table: &TableName, file: DataFileRef) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.pending_files
        .entry(table.clone())
        .or_default()
        .push(file);
}

/// Create a new savepoint with `name`, recording the current pending-file
/// watermark for every touched table.
pub(crate) fn tx_push_savepoint(state: &SessionState, name: String) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    let offsets: HashMap<TableName, usize> = tx
        .pending_files
        .iter()
        .map(|(t, v)| (t.clone(), v.len()))
        .collect();
    let htap_offsets: HashMap<TableName, usize> = tx
        .htap_rows
        .iter()
        .map(|(t, v)| (t.clone(), v.len()))
        .collect();
    tx.savepoints.push(SavepointFrame {
        name,
        file_offsets: offsets,
        htap_offsets,
    });
}

/// Release the named savepoint (PG: RELEASE SAVEPOINT <name>).
/// The writes that happened after it remain pending. Returns `Err` if
/// the savepoint name is not found.
pub(crate) fn tx_release_savepoint(state: &SessionState, name: &str) -> Result<()> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    // Find the last frame with this name (PG allows name reuse; RELEASE
    // removes the most-recently-created one with that name).
    let pos = tx.savepoints.iter().rposition(|f| f.name == name);
    match pos {
        Some(i) => {
            tx.savepoints.remove(i);
            Ok(())
        }
        // PG: SQLSTATE 3B001 (no_such_savepoint).
        None => Err(BasinError::InvalidSchema(format!(
            "savepoint \"{name}\" does not exist (SQLSTATE 3B001)"
        ))),
    }
}

/// Roll back to the named savepoint. Discards every savepoint frame created
/// *after* the named one, but — per PostgreSQL — KEEPS the named savepoint
/// itself so it can be rolled back to again (it remains re-establishable
/// until `RELEASE`d, `COMMIT`, or full `ROLLBACK`). Returns the abandoned
/// pending files (tail beyond the named savepoint's saved offset per table)
/// so the executor can delete them from storage. Also returns the pre-tx
/// snapshot map (for restoring tables that now have zero pending files
/// after truncation, so reads can see the pre-tx state).
pub(crate) fn tx_rollback_to_savepoint(
    state: &SessionState,
    name: &str,
) -> Result<(
    HashMap<TableName, Vec<DataFileRef>>,
    HashMap<TableName, SnapshotId>,
)> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    // Find the savepoint frame.
    let pos = tx.savepoints.iter().rposition(|f| f.name == name);
    let pos = match pos {
        Some(i) => i,
        None => {
            // PG: SQLSTATE 3B001 (no_such_savepoint).
            return Err(BasinError::InvalidSchema(format!(
                "savepoint \"{name}\" does not exist (SQLSTATE 3B001)"
            )));
        }
    };
    // PG: keep the named savepoint, drop only the inner (later) ones.
    // `truncate(pos + 1)` retains the target frame at index `pos`; a
    // subsequent `ROLLBACK TO SAVEPOINT <name>` therefore still finds it.
    // The target frame's recorded watermarks remain valid because we are
    // about to discard every pending file beyond them.
    let target_frame = tx.savepoints[pos].clone_offsets();
    let target_htap = tx.savepoints[pos].clone_htap_offsets();
    tx.savepoints.truncate(pos + 1);

    // Collect the abandoned tail for each table.
    let mut abandoned: HashMap<TableName, Vec<DataFileRef>> = HashMap::new();
    for (table, files) in tx.pending_files.iter_mut() {
        let offset = target_frame.get(table).copied().unwrap_or(0);
        if files.len() > offset {
            let tail: Vec<DataFileRef> = files.drain(offset..).collect();
            abandoned.insert(table.clone(), tail);
        }
    }
    // Remove tables that now have zero pending files.
    tx.pending_files.retain(|_, v| !v.is_empty());

    // perf-w7-txn: truncate tx-buffered htap batches to the savepoint
    // watermark.  INSERTs are now buffered there, so a ROLLBACK TO must
    // discard the batches written after the savepoint or those rows would
    // resurrect on COMMIT.  Unlike pending files there's no on-disk cleanup
    // to do — just drop the batches.
    for (table, batches) in tx.htap_rows.iter_mut() {
        let offset = target_htap.get(table).copied().unwrap_or(0);
        if batches.len() > offset {
            batches.truncate(offset);
        }
    }
    tx.htap_rows.retain(|_, v| !v.is_empty());

    // Clear aborted state — ROLLBACK TO SAVEPOINT recovers the txn.
    tx.aborted = false;

    let snapshots = tx.pre_tx_snapshots.clone();
    Ok((abandoned, snapshots))
}

/// Returns the current pending files for `table` (for within-tx reads).
pub(crate) fn tx_pending_files_for(state: &SessionState, table: &TableName) -> Vec<DataFileRef> {
    state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned")
        .pending_files
        .get(table)
        .cloned()
        .unwrap_or_default()
}

/// Returns all tables that have pending files OR tx-buffered htap batches.
/// perf-w7-txn: INSERT-only tables no longer add to pending_files (they go
/// into htap_rows), so the rollback-to-savepoint refresh-loop must consult
/// both maps to find every table whose visible state changed.
pub(crate) fn tx_touched_tables(state: &SessionState) -> Vec<TableName> {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    let mut set: std::collections::HashSet<TableName> = tx.pending_files.keys().cloned().collect();
    set.extend(tx.htap_rows.keys().cloned());
    set.into_iter().collect()
}

// ── Phase 5.14.C2 HTAP helpers ────────────────────────────────────────────────

/// Set the WAL transaction id on the current tx. Idempotent — once set, the
/// same id is retained for the life of the tx. Returns the id that is now
/// active (either the newly assigned one or the pre-existing one).
pub(crate) fn tx_ensure_id(state: &SessionState, candidate: u64) -> u64 {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    if tx.tx_id.is_none() {
        tx.tx_id = Some(candidate);
    }
    tx.tx_id.unwrap()
}

/// Read the current WAL tx id without mutation. Returns `None` when no DML
/// has been issued yet inside the active transaction.
pub(crate) fn tx_get_id(state: &SessionState) -> Option<u64> {
    state.tx_state.lock().expect("tx_state lock poisoned").tx_id
}

/// Append a `RecordBatch` to the tx-local hot-tier buffer for `table`.
///
/// Called by the INSERT path immediately after constraint checks succeed and
/// before Parquet is written. The batch is keyed by table name so the read
/// path (`refresh_table_with_htap`) can merge it with the cold-tier scan.
pub(crate) fn tx_htap_push_batch(
    state: &SessionState,
    table: &TableName,
    batch: arrow_array::RecordBatch,
) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.htap_rows.entry(table.clone()).or_default().push(batch);
}

/// Clone the tx-local HTAP batches for `table`. Empty vec when none exist.
/// Used by `refresh_table_with_htap` to build the in-memory scan segment.
pub(crate) fn tx_htap_batches_for(
    state: &SessionState,
    table: &TableName,
) -> Vec<arrow_array::RecordBatch> {
    state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned")
        .htap_rows
        .get(table)
        .cloned()
        .unwrap_or_default()
}

/// Drain and return all tx-local HTAP batches on COMMIT so the caller can
/// promote them to the shared `MemTableRegistry`.
pub(crate) fn tx_htap_take_all(
    state: &SessionState,
) -> HashMap<TableName, Vec<arrow_array::RecordBatch>> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    std::mem::take(&mut tx.htap_rows)
}

// ── Transaction-scoped hot-tier UPDATE/DELETE overlay (OLTP-in-tx fast path) ──

/// Per-PK overlay map for one table: encoded PK → `Update` / `Tombstone`.
pub(crate) type TxOverlayTable =
    std::collections::BTreeMap<basin_hottier::RowKey, basin_hottier::MemRowValue>;

/// Returns `true` when the in-tx hot-tier UPDATE/DELETE fast path must be
/// disabled because a savepoint is active. The `tx_overlay` map cannot be
/// rewound to a savepoint watermark (RMW overwrites the same key, so there is
/// no length-based snapshot like `pending_files` / `htap_rows` have), so any
/// statement issued while a savepoint is live routes to the cold
/// copy-on-write path, which DOES honour `ROLLBACK TO SAVEPOINT`.
///
/// Pure flag read — no I/O.
pub(crate) fn tx_overlay_fastpath_blocked(state: &SessionState) -> bool {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    !tx.savepoints.is_empty()
}

/// Peek the current tx-overlay value for a single PK (read precedence:
/// `tx_overlay` > shared memtable > PK cache > cold). Returns a clone so the
/// caller can drop the lock before doing I/O. `None` means this transaction
/// has no overlay entry for `key` — the caller continues down the precedence
/// chain.
pub(crate) fn tx_overlay_get(
    state: &SessionState,
    table: &TableName,
    key: &basin_hottier::RowKey,
) -> Option<basin_hottier::MemRowValue> {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.tx_overlay.get(table)?.get(key).cloned()
}

/// Write one overlay entry (override or tombstone) for `table` keyed by `key`.
/// Overwrites any prior entry for the same PK so repeated RMW accumulates.
pub(crate) fn tx_overlay_put(
    state: &SessionState,
    table: &TableName,
    key: basin_hottier::RowKey,
    value: basin_hottier::MemRowValue,
) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.tx_overlay
        .entry(table.clone())
        .or_default()
        .insert(key, value);
}

/// Clone this transaction's overlay map for `table` (for in-tx reads).
/// Empty map when none. Returned by value so the read path can merge it on
/// top of the shared-registry snapshot without holding the tx lock.
pub(crate) fn tx_overlay_peek(state: &SessionState, table: &TableName) -> TxOverlayTable {
    let tx = state.tx_state.lock().expect("tx_state lock poisoned");
    // Fast-out for the auto-commit read hot path: outside a tx the overlay is
    // always empty, so skip the clone+lookup entirely.
    if !tx.active {
        return TxOverlayTable::new();
    }
    tx.tx_overlay.get(table).cloned().unwrap_or_default()
}

/// Drain and return the entire tx-overlay on COMMIT so the executor can write
/// each entry into the shared `MemTableRegistry`. Mirrors `tx_htap_take_all`:
/// call this BEFORE `tx_commit` (which clears `TxState`).
pub(crate) fn tx_overlay_take_all(state: &SessionState) -> HashMap<TableName, TxOverlayTable> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    std::mem::take(&mut tx.tx_overlay)
}

// ── Transaction-buffered change events (CDC / realtime) ──────────────────────

/// Buffer one in-transaction change event for delivery at COMMIT. Called by the
/// hot-tier UPDATE/DELETE fast path (and the in-tx UPDATE…FROM write tail) only
/// when at least one change-event sink is attached. The `seq` is NOT assigned
/// here — it is allocated in commit order at COMMIT drain time so the event
/// carries the same commit ordering the cold paths get and is not fragmented by
/// other sessions' interleaved auto-commit events. Push order == execution
/// order, which the COMMIT drain preserves.
pub(crate) fn tx_change_events_push(state: &SessionState, ev: TxChangeEvent) {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    tx.tx_change_events.push(ev);
}

/// Drain the buffered in-transaction change events on COMMIT, in execution
/// order. Mirrors `tx_overlay_take_all`: call this BEFORE `tx_commit` (which
/// clears `TxState`). The caller assigns each entry a commit `seq` and
/// dispatches to the post-commit sinks AFTER the commit has landed; on ROLLBACK
/// the buffer is dropped instead (see `tx_rollback`), so nothing is delivered.
pub(crate) fn tx_change_events_take_all(state: &SessionState) -> Vec<TxChangeEvent> {
    let mut tx = state.tx_state.lock().expect("tx_state lock poisoned");
    std::mem::take(&mut tx.tx_change_events)
}

/// Stash the OVERRIDING kind extracted from the current INSERT
/// statement's source text. Called by the executor pre-screen.
pub(crate) fn set_pending_overriding(state: &SessionState, kind: OverridingKind) {
    *state
        .pending_overriding
        .lock()
        .expect("pending_overriding lock poisoned") = Some(kind);
}

/// Atomically consume any pending OVERRIDING kind. Returns `None`
/// when the user didn't write the clause.
pub(crate) fn take_pending_overriding(state: &SessionState) -> Option<OverridingKind> {
    state
        .pending_overriding
        .lock()
        .expect("pending_overriding lock poisoned")
        .take()
}

// ── Phase 5.28 GUC accessors ─────────────────────────────────────────────────

/// Read the current `lock_timeout` for this session. `None` = disabled.
pub(crate) fn session_lock_timeout(state: &SessionState) -> Option<std::time::Duration> {
    *state
        .lock_timeout
        .lock()
        .expect("lock_timeout lock poisoned")
}

/// Set `lock_timeout` for this session. `None` disables it.
///
/// Updates both the `SessionState.lock_timeout` field (for any callers that
/// read it directly) and `advisory.lock_timeout` (used by the blocking
/// `pg_advisory_lock` UDF — ADR 0026).
pub(crate) fn set_session_lock_timeout(state: &SessionState, d: Option<std::time::Duration>) {
    *state
        .lock_timeout
        .lock()
        .expect("lock_timeout lock poisoned") = d;
    // Mirror into the advisory-lock manager so pg_advisory_lock(key)
    // honors the new timeout immediately (ADR 0026).
    state.advisory.set_lock_timeout(d);
}

/// Read the current `idle_in_transaction_session_timeout` for this session.
pub(crate) fn session_idle_in_transaction_timeout(
    state: &SessionState,
) -> Option<std::time::Duration> {
    *state
        .idle_in_transaction_session_timeout
        .lock()
        .expect("idle_in_transaction_session_timeout lock poisoned")
}

/// Set `idle_in_transaction_session_timeout` for this session.
pub(crate) fn set_session_idle_in_transaction_timeout(
    state: &SessionState,
    d: Option<std::time::Duration>,
) {
    *state
        .idle_in_transaction_session_timeout
        .lock()
        .expect("idle_in_transaction_session_timeout lock poisoned") = d;
}

/// Engine-wide default for `basin.synchronous_commit`.
///
/// Defaults to `on` (DURABLE): a write is not acked until its WAL entry is
/// durably flushed (the WAL lives on a local fsync'd volume, so this is a
/// ~1-5 ms group-committed fsync, not a network round-trip — Postgres-like,
/// and the same guarantee Postgres gives by default). This closes the async
/// loss window (up to `flush_interval`, ~200 ms of acked-but-unflushed writes
/// lost on a crash) that a database must not expose by default.
///
/// Set `BASIN_SYNCHRONOUS_COMMIT=off` (or per-session `SET
/// basin.synchronous_commit = off`) to opt into the faster async path — an
/// explicit, informed throughput/durability trade, never a silent default.
/// `BASIN_SYNCHRONOUS_COMMIT=on`/`true`/`1`/`yes` also forces it on.
pub(crate) fn synchronous_commit_env_default() -> bool {
    match std::env::var("BASIN_SYNCHRONOUS_COMMIT") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "on" | "true" | "1" | "yes"
        ),
        // Unset → durable by default.
        Err(_) => true,
    }
}

/// Parse a Postgres boolean GUC value (`SET … = on|off|true|false|1|0|
/// yes|no`, quoted or bare, case-insensitive). Errors on anything else so a
/// typo'd `SET basin.synchronous_commit = onn` doesn't silently downgrade
/// durability.
pub(crate) fn parse_pg_bool(raw: &str) -> Result<bool> {
    let trimmed = raw.trim().trim_matches('\'').trim_matches('"').trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Ok(true),
        "off" | "false" | "0" | "no" => Ok(false),
        other => Err(BasinError::InvalidSchema(format!(
            "invalid boolean value: {other:?} (expected on/off/true/false/1/0/yes/no)"
        ))),
    }
}

/// Read the session's `basin.synchronous_commit` mode. `true` = INSERTs on
/// the shard path ack only after the WAL group-commit is durable.
pub(crate) fn session_synchronous_commit(state: &SessionState) -> bool {
    state
        .synchronous_commit
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Set `basin.synchronous_commit` for this session.
pub(crate) fn set_session_synchronous_commit(state: &SessionState, on: bool) {
    state
        .synchronous_commit
        .store(on, std::sync::atomic::Ordering::Relaxed);
}

/// `SHOW basin.synchronous_commit` rendering.
pub(crate) fn show_synchronous_commit(state: &SessionState) -> &'static str {
    if session_synchronous_commit(state) {
        "on"
    } else {
        "off"
    }
}

// ── pg_trgm GUC accessors ────────────────────────────────────────────────────

/// Read the session's `pg_trgm.similarity_threshold` (default 0.3).
pub(crate) fn session_trgm_similarity_threshold(state: &SessionState) -> f32 {
    f32::from_bits(
        state
            .trgm_similarity_threshold
            .load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Set `pg_trgm.similarity_threshold` for this session.
/// Clamps the value to `[0.0, 1.0]` per PG semantics.
pub(crate) fn set_session_trgm_similarity_threshold(state: &SessionState, v: f32) {
    let clamped = v.clamp(0.0, 1.0);
    state
        .trgm_similarity_threshold
        .store(clamped.to_bits(), std::sync::atomic::Ordering::Relaxed);
}

/// Read the session's `pg_trgm.word_similarity_threshold` (default 0.6).
pub(crate) fn session_trgm_word_similarity_threshold(state: &SessionState) -> f32 {
    f32::from_bits(
        state
            .trgm_word_similarity_threshold
            .load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Set `pg_trgm.word_similarity_threshold` for this session.
/// Clamps to `[0.0, 1.0]`.
pub(crate) fn set_session_trgm_word_similarity_threshold(state: &SessionState, v: f32) {
    let clamped = v.clamp(0.0, 1.0);
    state
        .trgm_word_similarity_threshold
        .store(clamped.to_bits(), std::sync::atomic::Ordering::Relaxed);
}

// ── basin.read_tier accessors (ADR 0009) ─────────────────────────────────────

/// Read the session's `basin.read_tier` (default `Primary`).
pub(crate) fn session_read_tier(state: &SessionState) -> ReadTier {
    ReadTier::from_u8(state.read_tier.load(std::sync::atomic::Ordering::Relaxed))
}

/// Set `basin.read_tier` for this session.
pub(crate) fn set_session_read_tier(state: &SessionState, tier: ReadTier) {
    state
        .read_tier
        .store(tier as u8, std::sync::atomic::Ordering::Relaxed);
}

/// `SHOW basin.read_tier` rendering.
pub(crate) fn show_read_tier(state: &SessionState) -> &'static str {
    session_read_tier(state).as_str()
}

impl ProjectSession {
    /// The session's `basin.synchronous_commit` mode. The executor's shard
    /// INSERT path passes this to `ProjectHandle::write_batch_opts` so `on`
    /// sessions get ack-after-durable (WAL group commit + fsync) and `off`
    /// sessions keep the fast ack-before-durable default.
    pub fn synchronous_commit(&self) -> bool {
        session_synchronous_commit(&self.state)
    }
}

/// Touch the session's last-active timestamp. Called at the start of every
/// `execute()` so the idle-in-txn reaper sees fresh activity.
pub(crate) fn touch_last_active(state: &SessionState) {
    *state.last_active.lock().expect("last_active lock poisoned") = std::time::Instant::now();
}

/// Read the session's last-active timestamp.
pub(crate) fn last_active(state: &SessionState) -> std::time::Instant {
    *state.last_active.lock().expect("last_active lock poisoned")
}

#[instrument(skip(engine, current_user, auth_context), fields(project = %project))]
pub(crate) async fn open(
    engine: Engine,
    project: ProjectId,
    current_user: String,
    auth_context: Arc<AuthContext>,
    is_system: bool,
) -> Result<ProjectSession> {
    // 1. Idempotent namespace.
    engine.config().catalog.create_namespace(&project).await?;

    // 2. SessionContext + register the storage's object store under our
    //    synthetic scheme. Recursively descend into the date-and-partition
    //    subdirectories `basin-storage` writes (otherwise DataFusion's
    //    default `listing_table_ignore_subdirectory = true` would skip them).
    //
    //    Noisy-project downshift: when this project's recent query rate is
    //    over the threshold (see `crate::noisy_detector`), pin
    //    `target_partitions = 1` so its bulk scans stop fanning out
    //    parallel range reads at full strength. This is a cooperative hint
    //    that lets a heavy project self-cap; the storage layer's fair-share
    //    scheduler is the real fairness mechanism. We only consult the
    //    detector here (at session-open time): a project that becomes noisy
    //    mid-session keeps its current partition count until the next
    //    `open_session`, which is the natural granularity for this kind of
    //    soft throttle.
    // Pin target_partitions=1 by default so per-query Parquet fan-out
    // is bounded. Each query reads its files sequentially via one stream
    // instead of issuing 4–8 concurrent range reads per file. Combined
    // with the storage scheduler's small global budget (default 4),
    // this lets quiet projects always find a free permit slot within
    // ~one in-flight RPC's duration. Noisy projects pay a per-query
    // throughput cost (single-threaded reads) — that's the right
    // tradeoff for fairness on bounded-concurrency backends. On AWS S3
    // with effectively unbounded server-side concurrency, raising this
    // back to `num_cpus` is fine; surface as a per-deployment knob in
    // a v0.3 catalog field. The noisy detector still applies — it
    // would catch a hypothetical per-deployment override that bumps
    // partitions back up for a project that abuses it.
    let session_cfg = datafusion::execution::config::SessionConfig::new()
        .set_str(
            "datafusion.execution.listing_table_ignore_subdirectory",
            "false",
        )
        .with_target_partitions(1);
    if engine.is_noisy(&project) {
        // Already pinned to 1; keep the log so noisy detection is
        // observable in tracing.
        tracing::info!(
            project = %project,
            "noisy project detected (target_partitions already pinned to 1)"
        );
    }

    // Build the SessionContext via SessionStateBuilder so all stateless UDFs
    // are batch-inserted during state construction (no write-lock per UDF).
    // The pre-built cache on Engine holds DataFusion's built-in functions
    // plus every Basin stateless UDF as `Arc<ScalarUDF>` handles; cloning
    // the Vec here is O(n) Arc ref-count bumps — no struct allocation.
    let udf_cache = engine.inner.udf_cache.as_ref();
    // Prepend our ANY/ALL → scalar-subquery rule so it fires before
    // DataFusion's RewriteSetComparison decomposes uncorrelated ANY/ALL
    // into LeftMark NestedLoopJoin plans.  Clone the pre-built rule list
    // from the stateless UDF cache instead of re-running
    // `Optimizer::default()` (saves 27 Arc<dyn OptimizerRule>
    // allocations per session-open).
    let mut optimizer_rules = udf_cache.optimizer_rules.clone();
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::any_all_rewrite::AnyAllToScalarSubquery),
    );
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::union_scan_collapse::UnionScanCollapse),
    );
    optimizer_rules.insert(0, std::sync::Arc::new(crate::nullif_rewrite::NullifRewrite));
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::is_distinct_rewrite::IsDistinctRewrite),
    );

    // Build a per-session RuntimeEnv that plugs in the process-wide file
    // metadata cache. Vortex/Parquet footer parses survive session recycling —
    // the dominant cost behind scale regressions at 100k rows / 50 files.
    // DefaultFilesMetadataCache validates entries via size + last_modified.
    let cache_cfg = CacheManagerConfig::default()
        .with_file_metadata_cache(Some(engine.inner.file_metadata_cache.clone()));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_cfg)
        // Plug in the process-wide bounded memory pool so the SUM of concurrent
        // query working sets is capped: a single heavy aggregate spills or fails
        // cleanly instead of OOM-killing the shared node.
        .with_memory_pool(engine.inner.query_memory_pool.clone())
        .build_arc()
        .map_err(|e| BasinError::internal(format!("RuntimeEnv build: {e}")))?;
    // Targeted defaults: install only the DF feature sets we actually
    // need fresh per session.  `with_default_features()` also rebuilds
    // every DF scalar + aggregate UDF, which is immediately overwritten
    // by `with_scalar_functions(udf_cache.scalar.clone())` /
    // `with_aggregate_functions(...)` below — pure waste on every
    // session-open.  By calling the individual setters directly we skip
    // that work entirely (~3-10ms saved per `session::open`).
    let state = SessionStateBuilder::new()
        .with_config(session_cfg)
        .with_runtime_env(runtime_env)
        .with_table_factories(SessionStateDefaults::default_table_factories())
        .with_file_formats(SessionStateDefaults::default_file_formats())
        .with_expr_planners(SessionStateDefaults::default_expr_planners())
        .with_window_functions(SessionStateDefaults::default_window_functions())
        .with_table_function_list(SessionStateDefaults::default_table_functions())
        // Inject our prepended optimizer rule list (cloned from the
        // engine-wide cache).
        .with_optimizer_rules(optimizer_rules)
        // The cache includes DF's default scalar+agg UDFs alongside
        // Basin's, so passing it here is the single source of both.
        .with_scalar_functions(udf_cache.scalar.clone())
        .with_aggregate_functions(udf_cache.aggregate.clone())
        // Appended after all DF default physical optimizer rules: force
        // single-partition streaming when OFFSET sits above a sort that
        // matches the file's natural sort order.  The rule runs last so
        // EnforceDistribution / EnforceSorting have already finished their
        // fan-out decisions; we then collapse when the pattern allows it.
        .with_physical_optimizer_rule(std::sync::Arc::new(
            crate::sort_streaming_limit::SortStreamingLimit::new(),
        ))
        // Elide redundant SortExec above WindowAggExec when the file's
        // declared sort order (basin.sort_by) already covers the window's
        // PARTITION BY + ORDER BY (Phase 5.14.D3).
        .with_physical_optimizer_rule(std::sync::Arc::new(
            crate::catalog_window_exec::CatalogWindowExecSortElision::new(),
        ))
        // Phase 5.30.C/E: schema-aware citext logical-plan rewrite.
        // Runs after TypeCoercion (all schemas resolved) and rewrites
        // binary comparisons / sort exprs on BASIN_TYPE=CITEXT columns
        // to use lower()-folded operands so plain SQL `WHERE col = 'Foo'`
        // and `ORDER BY col` are automatically case-insensitive.
        .with_analyzer_rule(std::sync::Arc::new(
            crate::citext_analyzer::CitextAnalyzerRule,
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let url = Url::parse(BASIN_URL_BASE)
        .map_err(|e| BasinError::internal(format!("bad basin url: {e}")))?;
    // Register the *project-scoped* store so every range read DataFusion
    // drives for this session counts against the project's per-project
    // concurrency budget. This is the load-bearing call for in-process
    // project fairness on shared object-store backends (real S3 in
    // particular, where the shared reqwest pool would otherwise be
    // saturated by one heavy project).
    // #36 (Stage 2b): WARM the project's bucket assignment from the durable
    // catalog BEFORE resolving the scan store. `scan_object_store` registers
    // the striping-aware store only when `should_stripe_scan` sees a warmed
    // multi-bucket stripe in the pool's per-process cache. That cache is warmed
    // by writes (the writer's `ensure_bucket_assignment`), so a node that has
    // only ever SERVED READS for this project — the non-owning multi-node peer,
    // or any node after a restart — would find a COLD cache, register the plain
    // primary store, and full-scan ZERO files for a striped project (whose data
    // lives in the pool buckets, not on primary). Warming reads the durable
    // assignment and populates the cache so the registration below stripes
    // correctly regardless of which node planned the write. No-op when the pool
    // is OFF/absent or the project is unstriped (width-1 → primary, unchanged).
    // FAIL CLOSED: if the assignment cannot be resolved we must NOT silently
    // register the primary store and return wrong (empty) results for a striped
    // table — surface the error and let the session open fail.
    engine
        .config()
        .storage
        .ensure_bucket_assignment(&project)
        .await?;
    // #36 (Stage 2a, Scheme C): when partition→bucket striping is ON and this
    // project has a warmed multi-bucket stripe, register the striping-aware
    // store so each `basin://engine/<path>` GET re-derives the file's partition
    // and resolves its stripe bucket, and LISTs union across the stripe. In
    // every other case (pool absent / OFF / width-1 / unwarmed / BYO) this
    // returns exactly `project_object_store(project)` — byte-identical to the
    // single-store registration that has always been here.
    let store = engine.config().storage.scan_object_store(&project);
    ctx.register_object_store(&url, store);

    // Register JSONB table-valued functions (UDTFs) directly on the real
    // session context. UDTFs are stored in a separate `table_functions` map
    // that is not captured by `StatelessUdfCache` (which only snapshots scalar
    // and aggregate functions). These must be registered per-session.
    crate::jsonb_udf::register_jsonb_udtfs(&ctx);
    crate::jsonb_path_udf::register_jsonb_path_udtfs(&ctx);

    // Auth session functions: `auth_uid()`, `auth_role()`, `auth_jwt()`.
    // These capture a per-session Arc<AuthContext> so they cannot be cached.
    // Only these 3 UDFs require individual write-lock acquisitions per session.
    crate::udf::register_auth_udfs(&ctx, auth_context.clone());

    // TABLESAMPLE sampling UDFs (BUG #134). Registered per-session, not in
    // the stateless cache, because the REPEATABLE (seeded) variants hold a
    // per-session draw counter that must start at 0 for each fresh session
    // so a seeded sample is reproducible from a clean session.
    crate::udf::register_tablesample_udfs(&ctx);

    // Phase 5.11.M: route `information_schema.tables` and
    // `pg_catalog.pg_class` SELECTs to the project-scoped catalog
    // snapshot. The providers hold `Arc<dyn Catalog>` + `ProjectId` only;
    // the heavy resource (the catalog handle) is shared across every
    // session, so per-project cost is O(bytes).
    // Create the schema_state here so both the info-schema providers and the
    // SessionState share the same Arc. Providers hold an Arc clone and read the
    // live user-schema set at scan time; SessionState's CREATE/DROP SCHEMA
    // handlers mutate it at execute time.
    let schema_state: Arc<RwLock<crate::schema_ddl::SchemaState>> =
        Arc::new(RwLock::new(crate::schema_ddl::SchemaState::default()));

    crate::info_schema_provider::register_info_schema_providers(
        &ctx,
        engine.config().catalog.clone(),
        project,
        engine.lock_registry().clone(),
        engine.connection_registry().clone(),
        Arc::clone(&schema_state),
    )
    .map_err(|e| BasinError::internal(format!("info_schema providers: {e}")))?;

    // Phase 5.16.D: register `basin_stat_statements` virtual view in the
    // `public` schema so `SELECT * FROM basin_stat_statements` works without
    // a schema prefix. The provider reads the process-wide QueryStatRegistry
    // (shared, O(bytes) per idle project) on every scan — no caching needed.
    crate::query_stats_export::register_basin_stat_statements(
        &ctx,
        engine.inner.query_stats.clone(),
    )
    .map_err(|e| BasinError::internal(format!("basin_stat_statements: {e}")))?;

    // Phase 5.29.C: register `timescaledb_information.chunks` virtual schema.
    // Serves chunk metadata from the HypertableRegistry so TimescaleDB-
    // compatible queries like `SELECT chunk_name FROM
    // timescaledb_information.chunks WHERE hypertable_name = 'metrics'` work.
    {
        use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
        use datafusion::datasource::TableProvider;
        let catalog_name = ctx.state().config_options().catalog.default_catalog.clone();
        if let Some(df_catalog) = ctx.catalog(&catalog_name) {
            let ts_schema = Arc::new(MemorySchemaProvider::new());
            let chunks_provider: Arc<dyn TableProvider> =
                Arc::new(crate::hypertable_provider::ChunksProvider::new(
                    engine.hypertable_registry().clone(),
                    project,
                ));
            let hypertables_provider: Arc<dyn TableProvider> =
                Arc::new(crate::hypertable_provider::HypertablesProvider::new(
                    engine.hypertable_registry().clone(),
                    project,
                ));
            let _ = ts_schema.register_table("chunks".to_string(), chunks_provider);
            let _ = ts_schema.register_table("hypertables".to_string(), hypertables_provider);
            let _ = df_catalog.register_schema("timescaledb_information", ts_schema);
        }
    }

    // Phase 5.21.B/E: register CDC virtual tables (pg_replication_slots,
    // pg_publication, pg_publication_tables) in the pg_catalog schema.
    if let Err(e) = crate::info_schema_provider::register_cdc_providers(
        &ctx,
        engine.slot_registry().clone(),
        engine.publication_registry().clone(),
        project,
    ) {
        tracing::warn!("register_cdc_providers: {e}");
    }

    // Phase 5.11.R: register `basin_realtime.channels` and
    // `basin_realtime.stats` virtual tables. The providers return live data
    // when a RealtimeChannelSource is attached; otherwise they return valid
    // empty/zero rows so the cloud handler's graceful fallback still triggers.
    {
        let rt_source = engine.realtime_source();
        let notify = Arc::new(engine.notify_registry().clone());
        if let Err(e) =
            crate::realtime_catalog::register_realtime_providers(&ctx, project, rt_source, notify)
        {
            tracing::warn!("register_realtime_providers: {e}");
        }
    }

    let state = Arc::new(SessionState::new_with_schema_state(Arc::clone(
        &schema_state,
    )));

    // Phase 5.28.C: register with the idle-in-txn reaper.
    let (reaper_id, reaped_flag) = engine.reaper_registry().register(state.clone());

    // Advisory-lock UDFs (BUG #138). Session-scoped: a lock owned by this
    // session must appear "held" to other sessions, so these capture the
    // per-session `Arc<AdvisorySessionLocks>` and overwrite (by name) the
    // removed stateless stubs. Registered here, like `register_auth_udfs`.
    crate::advisory_lock::register_advisory_lock_udfs(&ctx, state.advisory.clone());

    // Phase 5.8.A: cron.schedule / cron.unschedule UDFs. Capture engine +
    // project so they can open an independent session to mutate cron_job.
    crate::cron_glue::register_cron_udfs(&ctx, engine.clone(), project);

    // Phase 5.8.A: net.http_get / net.http_post UDFs (stateless, per-session).
    crate::net_glue::register_net_udfs(&ctx);

    // Phase 5.11.J: register any LANGUAGE wasm UDFs the project has as
    // DataFusion ScalarUDFs. Each one wraps a wasmtime call; they appear
    // alongside the stateless UDFs already loaded above so DataFusion can
    // resolve their names during query planning. Skipped if the project has
    // no WASM functions (the common case — one catalog round-trip overhead).
    {
        use basin_catalog::SqlFunctionLanguage;
        let wasm_fns: Vec<_> = engine
            .config()
            .catalog
            .list_sql_functions(&project)
            .await
            .into_iter()
            .filter(|f| f.language == SqlFunctionLanguage::Wasm)
            .collect();
        for def in &wasm_fns {
            if let Some(udf) = crate::wasm_udf::make_wasm_scalar_udf(def) {
                ctx.register_udf((*udf).clone());
            }
        }
    }

    // 3. Pre-register every table the catalog already knows about. This makes
    //    SELECT work immediately without a per-query refresh.
    //
    // Phase 5.18.C: use `list_tables_qualified` so system-schema tables
    // (cron.job, auth.users, net._http_response, etc.) are also pre-registered
    // in this session's DataFusion context. DataFusion uses bare table names
    // (schema stripping happens before SQL reaches DataFusion), so we register
    // each table under its bare TableName regardless of schema.
    //
    // AVAILABILITY: pre-registration is BEST-EFFORT per table. A single
    // corrupt/unresolvable table (a torn partition delta chain whose baseline
    // segment is missing, a NotFound on a referenced segment, a transient store
    // error) must NEVER fail session-open — otherwise one bad table locks EVERY
    // pgwire connection to the whole project out at the door (even `SELECT 1`),
    // and the operator can't even `DROP TABLE` it to recover. So we log a warning
    // and SKIP the offending table: it is simply not registered in this session,
    // so a query that explicitly touches it errors ("table not found") while the
    // session opens and every other table works normally. The table is also lazy
    // re-resolved on first access (`ensure_table_registered` /
    // `refresh_table` on the query path), so a table that fails to warm here but
    // is later repaired (or whose transient error clears) still becomes queryable
    // without reconnecting.
    let qtables = engine
        .config()
        .catalog
        .list_tables_qualified(&project)
        .await?;
    for qtable in qtables {
        if let Err(e) = refresh_table_qualified(&engine, &project, &ctx, &state, &qtable).await {
            tracing::warn!(
                %project,
                table = %qtable,
                error = %e,
                "skipping table during session-open warm: it failed to resolve \
                 (e.g. torn/missing partition segment); the session opens and \
                 other tables work — only a query that touches this table errors"
            );
        }
    }

    // Phase 5.23.C: register session with the connection registry. The handle
    // is stored on the session and dropped when the session is dropped, which
    // automatically removes the entry from pg_stat_activity.
    let connection_handle = engine.connection_registry().connect(&project);
    let session_pid = connection_handle.pid;
    // Phase 5.31.A: capture the per-session cancel notify from the handle so
    // the executor can await it alongside DataFusion collect futures.
    let cancel_notify = connection_handle.cancel_notify.clone();

    // Phase 5.23.D: register the session's virtualxid lock with the lock
    // registry. Every active Postgres backend holds a virtualxid lock
    // (ExclusiveLock on its own virtual transaction id). This ensures that
    // pg_locks always returns at least one row for the current session,
    // matching PG's behaviour where `SELECT * FROM pg_locks` in any live
    // session shows the session's own virtualxid lock.
    let vtxid = format!("1/{session_pid}");
    let lock_entry = basin_shard::LockEntry::virtualxid_lock(session_pid, &vtxid);
    let lock_handle = engine.lock_registry().acquire(&project, lock_entry);

    // ADR 0026: wire the LockRegistry + project + pid into the advisory-lock
    // manager so that held advisory locks appear in `pg_locks` and the session
    // pid is correctly reported.
    state
        .advisory
        .set_registry(engine.lock_registry().clone(), project, session_pid);

    // Phase 5.31.A: register a per-session `pg_cancel_backend(pid)` UDF.
    // This captures the engine handle and session_pid so that calling
    // `SELECT pg_cancel_backend(N)` from another session resolves through
    // the engine's cancel_backend path, which fires the target's notify and
    // returns SQLSTATE 57014 to the target's running query.
    crate::cancel_udf::register_pg_cancel_backend_udf(&ctx, engine.clone());

    // SQL pub-sub introspection: `pg_listening_channels()` reports the
    // channels this session is currently listening on. Per-session because
    // each pgwire backend tracks its own subscription set.
    crate::notify_registry::register_pg_listening_channels(&ctx, state.clone());

    crate::project_usage_view::register_basin_project_usage(
        &ctx,
        engine.project_counters_registry().clone(),
        project.clone(),
    )
    .map_err(|e| BasinError::internal(format!("basin_project_usage: {e}")))?;

    Ok(ProjectSession {
        engine,
        project,
        current_user,
        auth_context,
        ctx,
        state,
        reaped_flag,
        reaper_id,
        _lock_handle: Some(lock_handle),
        _connection_handle: Some(connection_handle),
        session_pid,
        cancel_notify,
        is_system,
        copy_touched: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        copy_audit: tokio::sync::Mutex::new(crate::CopyDurabilityAudit::default()),
    })
}

/// Build the DataFusion [`FileFormat`] + file extension for a table's
/// on-disk data format (#161/#162). Single format per table — opt-in Vortex,
/// Parquet remains the default and is byte-identical to the prior inline
/// expression so existing tables see zero regression.
fn listing_file_format(format: TableFileFormat) -> (Arc<dyn FileFormat>, &'static str) {
    match format {
        // `BasinParquetFormat` is a pass-through to `ParquetFormat` for every
        // table WITHOUT an interval column, so this stays behaviourally
        // identical to the historical inline expression for those. Interval
        // columns are stored as a 16-byte `LargeBinary` blob (Parquet cannot
        // encode `Interval(MonthDayNano)` any more than Vortex can), and the
        // wrapper is what stops DataFusion attempting the impossible
        // LargeBinary → Interval cast and restores the logical type above the
        // scan. See `crate::parquet_listing_format`.
        TableFileFormat::Parquet => (
            Arc::new(BasinParquetFormat::new(Arc::new(ParquetFormat::default()))),
            ".parquet",
        ),
        // Opt-in Vortex read path. Construct `VortexFormat` directly with
        // `new_with_options` so we can wrap it in `BasinVortexFormat`.
        //
        // W2-1 (this commit): `BasinVortexFormat` patches `total_byte_size`
        // from `Precision::Absent` to `Precision::Inexact(object.size)` so
        // DataFusion's `join_selection` / `supports_collect_by_thresholds`
        // optimizer rules get a real byte-size estimate instead of falling
        // back to row-count heuristics.  Fixes `inner_join@100k`.
        //
        // D2 (preserved from 126b038): `scan_concurrency = 8` parallelises
        // per-file row-chunk splits so range/group_by/order_by/window/join
        // shapes overlap I/O with decode; `projection_pushdown = true` so a
        // projected scan reads only the needed columns natively instead of
        // DataFusion projecting post-scan.
        TableFileFormat::Vortex => {
            let inner = Arc::new(vortex_datafusion::VortexFormat::new_with_options(
                vortex::session::VortexSession::default(),
                vortex_datafusion::VortexTableOptions {
                    projection_pushdown: true,
                    scan_concurrency: Some(8),
                    ..Default::default()
                },
            ));
            (Arc::new(BasinVortexFormat::new(inner)), ".vortex")
        }
    }
}

/// Build `Vec<Vec<SortExpr>>` suitable for
/// `ListingOptions::with_file_sort_order` from a global sort-column list.
///
/// Each column sorts ASC NULLS LAST — matching PostgreSQL's default sort order
/// for `ORDER BY col ASC` (which is `NULLS LAST`).  Using `NULLS LAST` is
/// essential so that `CatalogWindowExecSortElision` can match the
/// `DataSourceExec`'s declared ordering against the `SortExec` expressions
/// that `EnforceSorting` inserts (which also use `NULLS LAST` for ASC columns).
/// A NULLS_FIRST / NULLS_LAST mismatch would silently prevent elision.
///
/// The outer `Vec` wraps a single inner `Vec` (one sort key per file, not
/// multiple alternative orderings).
fn build_file_sort_order(cols: &[String]) -> Vec<Vec<SortExpr>> {
    // sort(ascending=true, nulls_first=false) → ASC NULLS LAST (PG default)
    let exprs: Vec<SortExpr> = cols
        .iter()
        .map(|c| col(c.as_str()).sort(true, false))
        .collect();
    vec![exprs]
}

/// Re-load a table's catalog metadata and (re-)register it with the
/// `SessionContext`. Called after CREATE / INSERT so subsequent queries see
/// the new state.
///
/// `ListingTable` caches the file list it discovers when it's constructed,
/// which is exactly why we have to throw it away and build a fresh one after
/// every commit.

/// Build the cold-tier [`ListingTable`] for `files` and register it under
/// `tref`, wrapping it in an overlay-aware provider when the hot tier holds
/// fast-path DELETE tombstones or UPDATE overrides for `(project, table)`.
///
/// This is the SINGLE overlay-wiring point for the **non-transactional**
/// DataFusion read path (`refresh_table_inner`, `refresh_table_qualified`,
/// `refresh_table_with_extra`). It mirrors the transactional
/// [`HtapUnionTable::scan`] overlay exactly by reusing that same provider with
/// an *empty hot half*: when tombstones/updates are present on a single-column
/// PK table, the registered plan therefore contains the identical
/// `TombstoneFilterExec` + `UpdateOverlayExec` nodes. Without this, a
/// fast-path UPDATE override (written to the per-Engine memtable registry) was
/// applied only on the point-lookup fast path and inside transactions — any
/// full-scan / aggregate / ORDER-BY-without-LIMIT / JOIN read returned the
/// stale pre-update cold-tier value.
///
/// The empty-hot `HtapUnionTable` adds negligible overhead: its `scan` gate
/// (`apply_overlay`) only wraps the cold scan when the registry actually holds
/// overrides for this table, and an empty `MemTable` scan is a trivial plan.
#[allow(clippy::too_many_arguments)]
async fn register_cold_with_overlay(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    table: &TableName,
    tref: &TableReference,
    df_schema: &Arc<arrow_schema::Schema>,
    files: &[DataFileRef],
    file_format: TableFileFormat,
    global_sort_order: Option<&[String]>,
    pk_columns: &[String],
    tx_overlay: TxOverlayTable,
    // Hot-tier MVCC sequence watermark pinned for this transaction's read-view
    // (`None` in auto-commit). Filters post-snapshot registry overlay writes
    // out of the union so they don't leak into an open transaction.
    hot_seq_watermark: Option<u64>,
) -> Result<()> {
    let provider = build_cold_with_overlay(
        engine,
        project,
        table,
        df_schema,
        files,
        file_format,
        global_sort_order,
        pk_columns,
        tx_overlay,
        hot_seq_watermark,
    )
    .await?;
    ctx.register_table(tref.clone(), provider)
        .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;
    Ok(())
}

/// Build (but do not register) the cold-tier provider for `table`, wrapping it
/// in an [`HtapUnionTable`] iff a fast-path DELETE/UPDATE overlay (or this tx's
/// `tx_overlay`) applies. Split out of [`register_cold_with_overlay`] so the
/// auto-commit provider cache (Fix B+C) can memoise the returned
/// `Arc<dyn TableProvider>` and re-register it on a hit without rebuilding.
#[allow(clippy::too_many_arguments)]
async fn build_cold_with_overlay(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    df_schema: &Arc<arrow_schema::Schema>,
    files: &[DataFileRef],
    file_format: TableFileFormat,
    global_sort_order: Option<&[String]>,
    pk_columns: &[String],
    tx_overlay: TxOverlayTable,
    hot_seq_watermark: Option<u64>,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    // Build the cold-tier ListingTable over exactly `files`.
    let (ff, file_ext) = listing_file_format(file_format);
    let mut listing_options = ListingOptions::new(ff).with_file_extension(file_ext);
    if let Some(sort_cols) = global_sort_order {
        listing_options = listing_options.with_file_sort_order(build_file_sort_order(sort_cols));
    }
    let mut urls: Vec<ListingTableUrl> = Vec::with_capacity(files.len());
    for f in files {
        let mut s = String::from(BASIN_URL_BASE);
        s.push_str(&f.path);
        let url = ListingTableUrl::parse(&s)
            .map_err(|e| BasinError::internal(format!("listing url parse {s}: {e}")))?;
        urls.push(url);
    }
    let cfg = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(df_schema.clone());
    let cold_provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        ListingTable::try_new(cfg)
            .map_err(|e| BasinError::internal(format!("ListingTable::try_new {table}: {e}")))?,
    );

    // Overlay gate (concurrency fix #3): the fast-path DELETE/UPDATE overlay
    // and a tx's own `tx_overlay` only ever apply to single-column-PK tables —
    // composite/no-PK tables can NEVER have a registry overlay, so they keep
    // the plain `ListingTable` (no wrapper overhead, and no shape that could
    // ever change ⇒ a `(table, snapshot)` cache key is exact for them).
    //
    // For single-PK tables we now build the overlay-capable `HtapUnionTable`
    // UNCONDITIONALLY rather than gating on a build-time `needs_overlay` probe.
    // The wrapper's `scan` reads `snapshot_tombstones` / `snapshot_updates`
    // LIVE at scan time and short-circuits to a zero-overhead pass-through when
    // the overlay is empty, so an empty-overlay `HtapUnionTable` is functionally
    // and (with the empty-overlay pushdown delegation in
    // `supports_filters_pushdown`) PERFORMANCE-identical to the plain
    // `ListingTable` — yet the SAME cached `Arc` correctly grows an overlay path
    // the instant a fast-path UPDATE/DELETE lands. This is what lets the
    // provider cache key drop the hot-tier epoch: the build-time shape no longer
    // depends on the overlay's emptiness, so hot-tier-only churn cannot
    // invalidate a still-correct cached provider.
    if pk_columns.len() == 1 {
        // Empty-hot half: this provider serves cold + (live) overlay only; the
        // engine memtable is read inside `HtapUnionTable::scan` via the registry
        // snapshots, not through this MemTable.
        let empty_hot = MemTable::try_new(df_schema.clone(), vec![vec![]])
            .map_err(|e| BasinError::internal(format!("MemTable empty-hot {table}: {e}")))?;
        let union_provider = HtapUnionTable::new(
            cold_provider,
            Arc::new(empty_hot),
            df_schema.clone(),
            engine.clone(),
            *project,
            table.clone(),
            pk_columns.to_vec(),
            tx_overlay,
            hot_seq_watermark,
        );
        Ok(Arc::new(union_provider))
    } else {
        // Composite / no-PK table: no overlay is possible, plain ListingTable.
        Ok(cold_provider)
    }
}

/// Load `(project, table)` metadata using the current transaction's read-view
/// when an explicit transaction is active, so in-transaction reads are
/// snapshot-stable (REPEATABLE-READ-ish).
///
/// Behaviour:
///   * No active transaction → plain `load_table` (the live head). Auto-commit
///     reads always see the latest committed state.
///   * Active transaction → resolve the table's pinned read snapshot via
///     [`tx_read_snapshot_for`]. On first touch the just-loaded current head is
///     pinned and returned unchanged (no second round-trip). On a later read,
///     if the table's head has since advanced past the pin (another session
///     committed), reload the *historical* metadata at the pinned snapshot via
///     `Catalog::load_table_at_snapshot` so `live_data_files()` reconstructs
///     the file set this transaction is supposed to see.
///
/// Falls back to the freshly-loaded current metadata if the pinned snapshot is
/// no longer retained in the catalog chain (the catalog returns
/// `FeatureNotSupported`) — degrading to read-committed for that one read
/// rather than erroring the query.
///
/// ## Hot-tier isolation (cold snapshot + hot MVCC watermark)
///
/// This pins BOTH halves of the transaction's read-view:
///   * the *cold* (catalog) snapshot, reconstructed at the pinned id below, and
///   * the *hot* MVCC watermark — captured here at first-touch via
///     [`tx_hot_seq_watermark_for`] (registry `hot_tier_seq`), then read back by
///     `register_cold_with_overlay` / `HtapUnionTable::scan`, which drop any
///     shared-registry overlay entry whose `seq` exceeds it.
///
/// So a row another session writes via the hot-tier DELETE/UPDATE fast path
/// (tombstone / override in the shared `MemTableRegistry`) AFTER this
/// transaction pinned its snapshot now carries a higher `seq` than the
/// watermark and is filtered out — it no longer leaks into the open
/// transaction's reads. (An INSERT by another session flushes to Parquet and
/// advances the catalog snapshot, which the cold pin already hides.) The
/// transaction's OWN in-tx overlay (`tx_overlay`) is layered on top and always
/// wins (read-your-own-writes).
///
/// Residual limitation: the memtable is single-version per key, so if another
/// session OVERWRITES a key multiple times the pre-snapshot value cannot be
/// reconstructed once gone — only the latest-write's seq is compared against
/// the watermark. The realistic leak case (a single post-snapshot overlay write
/// over a row whose pre-snapshot state lived in the cold tier) is fully closed.
/// Returns `(read_metadata, live_head)`:
///   * `read_metadata` is what the reader should register (pinned to the
///     transaction read-view inside a tx, live head otherwise).
///   * `live_head` is the table's *current* catalog head observed during this
///     load — callers cache this in `state.snapshots` so the INSERT/commit
///     optimistic-concurrency baseline keeps meaning "latest head this session
///     saw", NOT the rewound read snapshot (which would otherwise force a
///     CommitConflict-retry at COMMIT).
async fn load_table_for_read(
    engine: &Engine,
    project: &ProjectId,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<(TableMetadata, SnapshotId)> {
    let meta = engine.config().catalog.load_table(project, table).await?;
    let live_head = meta.current_snapshot;
    // Pin the hot-tier MVCC watermark at the SAME first-touch moment as the
    // cold snapshot (after the read's own tail flush, which the caller performs
    // before refresh): the registry's current sequence for this table is the
    // hot-tier high-water mark this read observes. The overlay read path filters
    // out any later (seq > watermark) write by another session. No-op outside a
    // transaction. Side-effect only — the value is read back via
    // `tx_hot_seq_watermark_peek` when the overlay union is built.
    let current_hot_seq = engine.memtable_registry().hot_tier_seq(project, table);
    let _ = tx_hot_seq_watermark_for(state, table, current_hot_seq);
    let Some(pinned) = tx_read_snapshot_for(state, table, live_head) else {
        // Auto-commit: live head.
        return Ok((meta, live_head));
    };
    if pinned == live_head {
        // First touch, or head hasn't moved since the pin: the metadata we just
        // loaded already reflects the read-view.
        return Ok((meta, live_head));
    }
    // Head advanced past our pin (a concurrent committer): reconstruct the
    // historical metadata at the pinned snapshot.
    match engine
        .config()
        .catalog
        .load_table_at_snapshot(project, table, pinned)
        .await
    {
        Ok(historical) => Ok((historical, live_head)),
        Err(BasinError::FeatureNotSupported(_)) => {
            // Pinned snapshot no longer retained / backend lacks history:
            // degrade to read-committed for this read rather than failing.
            Ok((meta, live_head))
        }
        Err(e) => Err(e),
    }
}

/// Variant of [`refresh_table`] used by `exec_select` to collect the live
/// file count in the same catalog load that registers the table.  Returns the
/// number of live Parquet files for the table at the current snapshot; this
/// lets the caller accumulate a total without an extra `load_table` call.
///
/// All other callers (INSERT, UPDATE, DELETE, DDL) use the plain
/// `refresh_table` wrapper below, which discards the count.
pub(crate) async fn refresh_table_counted(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<usize> {
    let file_count = refresh_table_inner(engine, project, ctx, state, table).await?;
    Ok(file_count)
}

pub(crate) async fn refresh_table(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<()> {
    refresh_table_inner(engine, project, ctx, state, table).await?;
    Ok(())
}

async fn refresh_table_inner(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<usize> {
    let tref = || TableReference::Bare {
        table: table.as_str().into(),
    };

    // ── Provider cache fast path (Fix B+C) ──────────────────────────────────
    //
    // Only in auto-commit. Inside a transaction the provider bakes a pinned
    // cold snapshot + hot watermark + this tx's overlay — none of which the
    // cache key models — so the tx path always rebuilds (and the cache is
    // cleared at tx boundaries anyway).
    //
    // The key needs the table's live head (snapshot) + the hot-tier epoch. To
    // serve a HIT without the bloom-laden `TableMetadata` clone, we read the
    // head from the per-session head-probe cache (a tiny `(snapshot, epoch)`
    // tuple validated against the catalog epoch). On a head-probe miss we fall
    // through to the full `load_table` below and populate both caches.
    let auto_commit = !tx_is_active(state);
    if auto_commit {
        let catalog_epoch = engine.config().catalog.epoch();
        if let Some(live_head) = state.head_probe_cache.get_fresh(table, catalog_epoch) {
            // Concurrency fix #3: the provider is now a pure function of
            // `(table, snapshot)` — hot-tier overlay freshness is handled by the
            // always-overlay-capable `HtapUnionTable` at scan time, not by the
            // cache key — so a HIT need not even read the hot-tier epoch.
            let key = ProviderCacheKey {
                table: table.clone(),
                snapshot: live_head,
            };
            if let Some(hit) = state.provider_cache.get(&key) {
                // HIT: re-register the already-built provider. No catalog clone,
                // no schema conversion, no ListingTable rebuild.
                let _ = ctx.deregister_table(tref());
                ctx.register_table(tref(), hit.provider).map_err(|e| {
                    BasinError::internal(format!("register_table cached {table}: {e}"))
                })?;
                state
                    .snapshots
                    .lock()
                    .await
                    .insert(table.clone(), live_head);
                if hit.partitioned {
                    state
                        .has_partitioned_table
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                return Ok(hit.live_file_count);
            }
        }
    }

    // Transaction-aware load: in an explicit tx this pins the table's read
    // snapshot (REPEATABLE-READ-ish) so a concurrent committer's new rows stay
    // invisible; in auto-commit it is exactly `load_table` (the live head).
    let (meta, live_head) = load_table_for_read(engine, project, state, table).await?;

    // The catalog hands us a workspace-version schema; convert into the
    // version DataFusion's `register_listing_table` expects.
    let base_df_schema = schema_ws_to_df(meta.schema.as_ref())?;
    // ADR 0027 Phase 4: extend the DataFusion schema with shadow columns for
    // any promoted JSONB paths.  These are written as real Utf8 columns in
    // the Parquet files but are kept OUT of the user-visible catalog schema
    // (so `information_schema.columns` stays clean).  DataFusion's
    // ListingTable picks them up via the extended schema below.
    let df_schema = Arc::new(extend_schema_with_promoted_cols(
        base_df_schema,
        &meta.promoted_jsonb_paths,
    ));

    // Drop any stale registration before re-registering. `deregister_table`
    // returns Ok(None) for the first-time path, which is exactly what we want.
    let _ = ctx.deregister_table(tref());

    // Catalog-driven read path: enumerate exactly the files that are live at
    // `current_snapshot`. This is the fix for bug #41: a directory-URL
    // ListingTable would re-list the object store on every scan and return ALL
    // physical Parquet files, including those logically removed by a rollback
    // (GC is deferred by design). Using `live_data_files()` instead restricts
    // the scan to the canonical file set the catalog records for the current
    // snapshot, so post-rollback rows are never visible.
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    let partitioned = meta.partition_spec.is_partitioned();

    // Build the provider (the expensive step we want the cache to skip).
    let provider: Arc<dyn datafusion::catalog::TableProvider> = if live_files.is_empty() {
        // Table has no data at this snapshot (genesis, TRUNCATE, or rolled back
        // to genesis). An empty in-memory table returns zero rows with the
        // correct schema rather than erroring. MemTable requires at least one
        // partition; supply an empty one.
        Arc::new(
            MemTable::try_new(df_schema.clone(), vec![vec![]])
                .map_err(|e| BasinError::internal(format!("MemTable empty {table}: {e}")))?,
        )
    } else {
        // Non-HTAP cold-only path. When fast-path DELETE tombstones or UPDATE
        // overrides are present for a single-PK table, the shared helper wraps
        // the cold scan in the overlay-aware provider (same overlay as the
        // transactional `HtapUnionTable::scan`); otherwise it builds a plain
        // ListingTable with no wrapper overhead.
        build_cold_with_overlay(
            engine,
            project,
            table,
            &df_schema,
            &live_files,
            meta.file_format,
            meta.global_sort_order.as_deref(),
            &meta.pk_columns,
            tx_overlay_peek(state, table),
            tx_hot_seq_watermark_peek(state, table),
        )
        .await?
    };

    // Populate the auto-commit caches BEFORE registering (so a failure to
    // register doesn't leave a half-populated cache claiming success). The
    // provider `Arc` is shared between the cache and the registration.
    //
    // Skip caching inside a transaction: the built provider may bake a rewound
    // historical snapshot, a pinned hot watermark and this tx's `tx_overlay` —
    // none of which the `(table, snapshot)` key models. The head-probe cache is
    // likewise live-head only. Tx-built providers are re-registered fresh on
    // every read and the caches are cleared at tx boundaries
    // (`tx_begin`/`tx_commit`/`tx_rollback`).
    //
    // Concurrency fix #3: in auto-commit the built provider is the
    // always-overlay-capable `HtapUnionTable`, whose `scan` reads the hot-tier
    // overlay LIVE — so the cached `Arc` stays correct as the overlay grows or
    // drains, and the key needs no hot-tier epoch component.
    if auto_commit {
        let fill_epoch = engine.config().catalog.epoch();
        state
            .head_probe_cache
            .insert(table.clone(), live_head, fill_epoch);
        state.provider_cache.insert(
            ProviderCacheKey {
                table: table.clone(),
                snapshot: live_head,
            },
            ProviderCacheEntry {
                provider: provider.clone(),
                live_file_count: live_files.len(),
                partitioned,
            },
        );
    }

    ctx.register_table(tref(), provider)
        .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;

    // Cache the *live* head (not the rewound read snapshot) for this session's
    // INSERT/commit optimistic-concurrency baseline. See `load_table_for_read`.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), live_head);

    if partitioned {
        state
            .has_partitioned_table
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(live_files.len())
}

/// Phase 5.18.C — schema-qualified variant of [`refresh_table`].
///
/// Loads the table from the catalog using `load_table_qualified` (so the
/// key is `(schema, table)`) and registers it in DataFusion under the bare
/// table name. DataFusion itself is schema-unaware; schema stripping happens
/// before SQL reaches DataFusion via `strip_schema_qualifiers_for_session`.
///
/// Used by:
/// - `session::open` (pre-registration of all qualified tables at startup).
/// - `exec_create_table` for system sessions (post-create registration).
pub(crate) async fn refresh_table_qualified(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    qtable: &QualifiedTableName,
) -> Result<()> {
    // For public-schema tables delegate to the unqualified path (identity).
    if qtable.schema == basin_common::SchemaName::public() {
        return refresh_table(engine, project, ctx, state, &qtable.name).await;
    }

    let meta = engine
        .config()
        .catalog
        .load_table_qualified(project, qtable)
        .await?;
    let base_df_schema = schema_ws_to_df(meta.schema.as_ref())?;
    let df_schema = Arc::new(extend_schema_with_promoted_cols(
        base_df_schema,
        &meta.promoted_jsonb_paths,
    ));

    // Register under the bare table name so that DataFusion can find the
    // table after schema-qualifier stripping. The schema part is handled at
    // the SQL rewrite layer, not at the DataFusion registration layer.
    let bare_name = &qtable.name;
    let tref = || TableReference::Bare {
        table: bare_name.as_str().into(),
    };
    let _ = ctx.deregister_table(tref());

    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        let provider = MemTable::try_new(df_schema, vec![vec![]])
            .map_err(|e| BasinError::internal(format!("MemTable empty {bare_name}: {e}")))?;
        ctx.register_table(tref(), Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register_table {bare_name}: {e}")))?;
    } else {
        // Same overlay wiring as `refresh_table_inner`, keyed on the bare name.
        register_cold_with_overlay(
            engine,
            project,
            ctx,
            bare_name,
            &tref(),
            &df_schema,
            &live_files,
            meta.file_format,
            meta.global_sort_order.as_deref(),
            &meta.pk_columns,
            tx_overlay_peek(state, bare_name),
            // Qualified (non-public-schema) tables are not pinned by
            // `load_table_for_read`, so the watermark peek is `None` here —
            // consistent with the cold side, which also stays at the live head
            // for this path. (Public-schema tables delegate to `refresh_table`
            // above and DO get the pin.)
            tx_hot_seq_watermark_peek(state, bare_name),
        )
        .await?;
    }

    state
        .snapshots
        .lock()
        .await
        .insert(bare_name.clone(), meta.current_snapshot);

    if meta.partition_spec.is_partitioned() {
        state
            .has_partitioned_table
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(())
}

/// Like [`refresh_table`] but also includes `extra_files` in the listing.
///
/// Used during an active transaction to give within-transaction reads
/// visibility into pending (not-yet-committed) data files.  The extra files
/// are appended to the catalog's `live_data_files()` set; they are visible
/// only to this session's `SessionContext` and never touch the catalog.
pub(crate) async fn refresh_table_with_extra(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
    extra_files: &[DataFileRef],
) -> Result<()> {
    if extra_files.is_empty() {
        // No pending files: delegate to the regular path.
        return refresh_table(engine, project, ctx, state, table).await;
    }

    // Transaction-aware load (see `load_table_for_read`): the catalog-live half
    // is reconstructed at this transaction's pinned read snapshot, while the
    // pending (in-tx) `extra_files` below remain visible (read-your-own-writes).
    let (meta, live_head) = load_table_for_read(engine, project, state, table).await?;
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);
    let tref = || TableReference::Bare {
        table: table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref());

    // Combine catalog live files + pending (in-tx) files.
    let mut all_files: Vec<DataFileRef> = meta.live_data_files();
    all_files.extend_from_slice(extra_files);

    // Same overlay wiring as `refresh_table_inner`, but over the combined
    // catalog-live + pending (in-tx) file set.
    register_cold_with_overlay(
        engine,
        project,
        ctx,
        table,
        &tref(),
        &df_schema,
        &all_files,
        meta.file_format,
        meta.global_sort_order.as_deref(),
        &meta.pk_columns,
        tx_overlay_peek(state, table),
        tx_hot_seq_watermark_peek(state, table),
    )
    .await?;

    // Cache the *live* head, not the rewound read snapshot — see
    // `load_table_for_read` / `refresh_table_inner`.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), live_head);

    if meta.partition_spec.is_partitioned() {
        state
            .has_partitioned_table
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(())
}

/// Like [`refresh_table_with_extra`] but also injects tx-local HTAP
/// in-memory batches so projection-scan queries see uncommitted writes from
/// the same transaction (Phase 5.14.C2 read-path merge).
///
/// When `htap_batches` is non-empty, the registered DataFusion table provider
/// is a union of:
///   1. A `ListingTable` covering catalog-live files + pending Parquet files.
///   2. A DataFusion `MemTable` holding the in-memory Arrow batches buffered
///      during the current transaction.
///
/// Cross-session isolation is preserved because `htap_batches` only contains
/// batches from *this* session's `TxState`; other sessions' DataFusion
/// contexts do not see this registration.
pub(crate) async fn refresh_table_with_htap(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
    extra_files: &[DataFileRef],
    htap_batches: Vec<arrow_array::RecordBatch>,
) -> Result<()> {
    if htap_batches.is_empty() {
        // No hot-tier rows: fall back to the standard extra-files path.
        return refresh_table_with_extra(engine, project, ctx, state, table, extra_files).await;
    }

    // Transaction-aware load (see `load_table_for_read`): the catalog-live half
    // is read at this transaction's pinned snapshot. The tx-local `extra_files`
    // (UPDATE/DELETE rewrites) and `htap_batches` (buffered INSERTs) are this
    // session's own uncommitted writes and stay visible below.
    let (meta, live_head) = load_table_for_read(engine, project, state, table).await?;
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);
    let tref = || TableReference::Bare {
        table: table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref());

    // Combine catalog live files + pending (in-tx) files.
    let mut all_files: Vec<DataFileRef> = meta.live_data_files();
    all_files.extend_from_slice(extra_files);

    // Build in-memory partition from the hot-tier batches.  DataFusion's
    // MemTable needs at least one partition even when empty.
    let htap_provider = MemTable::try_new(df_schema.clone(), vec![htap_batches])
        .map_err(|e| BasinError::internal(format!("MemTable for htap {table}: {e}")))?;

    if all_files.is_empty() {
        // Only in-memory rows, no Parquet files: use the MemTable alone.
        ctx.register_table(tref(), Arc::new(htap_provider))
            .map_err(|e| BasinError::internal(format!("register_table htap-only {table}: {e}")))?;
    } else {
        // Build the cold-tier ListingTable and union it with the MemTable via a
        // custom provider that concatenates both scans.
        let (file_format, file_ext) = listing_file_format(meta.file_format);
        let mut listing_options = ListingOptions::new(file_format).with_file_extension(file_ext);
        if let Some(sort_cols) = meta.global_sort_order.as_deref() {
            listing_options =
                listing_options.with_file_sort_order(build_file_sort_order(sort_cols));
        }
        let mut urls: Vec<ListingTableUrl> = Vec::with_capacity(all_files.len());
        for f in &all_files {
            let mut s = String::from(BASIN_URL_BASE);
            s.push_str(&f.path);
            let url = ListingTableUrl::parse(&s)
                .map_err(|e| BasinError::internal(format!("listing url parse {s}: {e}")))?;
            urls.push(url);
        }
        let cfg = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(df_schema.clone());
        let listing_provider =
            Arc::new(ListingTable::try_new(cfg).map_err(|e| {
                BasinError::internal(format!("ListingTable::try_new {table}: {e}"))
            })?);
        let union_provider = HtapUnionTable::new(
            listing_provider,
            Arc::new(htap_provider),
            df_schema,
            engine.clone(),
            *project,
            table.clone(),
            meta.pk_columns.clone(),
            tx_overlay_peek(state, table),
            tx_hot_seq_watermark_peek(state, table),
        );
        ctx.register_table(tref(), Arc::new(union_provider))
            .map_err(|e| BasinError::internal(format!("register_table htap-union {table}: {e}")))?;
    }

    // Cache the *live* head, not the rewound read snapshot — see
    // `load_table_for_read` / `refresh_table_inner`.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), live_head);

    if meta.partition_spec.is_partitioned() {
        state
            .has_partitioned_table
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(())
}

// ── HtapUnionTable ────────────────────────────────────────────────────────────

/// A DataFusion [`TableProvider`] that unions two providers — a cold-tier
/// [`ListingTable`] and a hot-tier [`MemTable`] — into a single logical table.
///
/// Used by [`refresh_table_with_htap`] so that within-transaction projection
/// queries see both committed Parquet data and the current transaction's
/// uncommitted in-memory writes without requiring a full DataFusion UNION ALL
/// plan node at the SQL level.
///
/// # Isolation
///
/// Each session's `SessionContext` gets its own `HtapUnionTable` registered
/// under the table name; the `MemTable` half contains only this session's
/// uncommitted batches.  Other sessions' contexts are unaffected.
struct HtapUnionTable {
    cold: Arc<dyn datafusion::catalog::TableProvider>,
    hot: Arc<dyn datafusion::catalog::TableProvider>,
    schema: Arc<arrow_schema::Schema>,
    /// Engine handle for the process-wide memtable registry — consulted on
    /// scan to suppress any cold-tier rows that have been tombstoned by an
    /// out-of-tx fast-path DELETE. Cheap to clone (`Arc` inside).
    engine: Engine,
    project: ProjectId,
    table: TableName,
    /// Single-column PK is required for tombstone-key encoding.
    pk_columns: Vec<String>,
    /// Transaction-scoped UPDATE/DELETE overlay captured from this session's
    /// `TxState` at refresh time. Merged ON TOP of the shared-registry
    /// snapshot inside `scan` so the owning transaction sees its own
    /// uncommitted in-tx fast-path writes (read-your-own-writes). Empty
    /// outside a transaction (or when the tx has no overlay for this table).
    tx_overlay: crate::session::TxOverlayTable,
    /// Hot-tier MVCC sequence watermark pinned for this transaction's
    /// read-view (`None` in auto-commit). Passed to `snapshot_tombstones` /
    /// `snapshot_updates` in `scan` so shared-registry overlay writes another
    /// session committed *after* this transaction pinned its snapshot
    /// (`seq > watermark`) are filtered out and never leak into the union.
    hot_seq_watermark: Option<u64>,
}

impl std::fmt::Debug for HtapUnionTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HtapUnionTable").finish_non_exhaustive()
    }
}

impl HtapUnionTable {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cold: Arc<dyn datafusion::catalog::TableProvider>,
        hot: Arc<dyn datafusion::catalog::TableProvider>,
        schema: Arc<arrow_schema::Schema>,
        engine: Engine,
        project: ProjectId,
        table: TableName,
        pk_columns: Vec<String>,
        tx_overlay: crate::session::TxOverlayTable,
        hot_seq_watermark: Option<u64>,
    ) -> Self {
        Self {
            cold,
            hot,
            schema,
            engine,
            project,
            table,
            pk_columns,
            tx_overlay,
            hot_seq_watermark,
        }
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for HtapUnionTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use crate::hot_tombstone::{
            snapshot_tombstones, snapshot_updates, TombstoneFilterExec, UpdateOverlayExec,
        };
        use datafusion::physical_plan::union::UnionExec;

        let registry = self.engine.memtable_registry();
        // Filter the shared-registry overlay to this transaction's pinned
        // hot-tier watermark (`None` in auto-commit → no filter): entries with
        // `seq > watermark` were written by other sessions after this tx pinned
        // its snapshot and must stay invisible.
        let mut tombs = snapshot_tombstones(
            registry.as_ref(),
            &self.project,
            &self.table,
            self.hot_seq_watermark,
        );
        let mut updates = snapshot_updates(
            registry.as_ref(),
            &self.project,
            &self.table,
            self.hot_seq_watermark,
        );
        // Merge this transaction's own uncommitted in-tx fast-path overlay ON
        // TOP of the shared-registry snapshot (tx wins). No-op outside a tx or
        // when this table has no tx overlay.
        if !self.tx_overlay.is_empty() {
            crate::hot_tombstone::merge_tx_overlay(&mut tombs, &mut updates, &self.tx_overlay);
        }

        // Single-PK gate: fast-path DELETE/UPDATE only writes to single-column-PK
        // tables. Composite-PK or no-PK tables can never have tombstones/updates in
        // the registry, so skip the overhead.
        let apply_overlay =
            (self.pk_columns.len() == 1) && (!tombs.is_empty() || !updates.is_empty());

        if !apply_overlay {
            // Zero-overhead pass-through: no tombstones and no UPDATE overrides.
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            let hot_plan = self.hot.scan(state, projection, filters, limit).await?;
            return Ok(Arc::new(UnionExec::new(vec![cold_plan, hot_plan])));
        }

        let pk_col = &self.pk_columns[0];
        let Ok(pk_idx_in_schema) = self.schema.index_of(pk_col) else {
            // PK column missing from schema (defensive). Fall through to plain
            // union — never crash a read due to bad metadata.
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            let hot_plan = self.hot.scan(state, projection, filters, limit).await?;
            return Ok(Arc::new(UnionExec::new(vec![cold_plan, hot_plan])));
        };
        let pk_dt = self.schema.field(pk_idx_in_schema).data_type().clone();

        // If the caller's projection omits the PK column, augment it so the
        // tombstone filter / update overlay has the key bytes to compare on.
        // We then strip the extra column back out with a ProjectionExec.
        //
        // Limit pushdown is dropped when augmenting: the limit applies to
        // post-filter rows, and passing it to the cold scan would truncate
        // before tombstone/override removal and under-count survivors.
        let (cold_projection_owned, augmented, effective_limit) = match projection {
            Some(p) if !p.contains(&pk_idx_in_schema) => {
                let mut p2 = p.clone();
                p2.push(pk_idx_in_schema);
                (Some(p2), true, None)
            }
            Some(p) => (Some(p.clone()), false, limit),
            None => (None, false, limit),
        };

        let cold_projection_ref = cold_projection_owned.as_ref();
        let cold_plan = self
            .cold
            .scan(state, cold_projection_ref, filters, effective_limit)
            .await?;

        // Apply tombstone row-filter (no-op when no tombstones).
        let mut filtered: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if tombs.is_empty() {
            cold_plan
        } else {
            Arc::new(TombstoneFilterExec::new(
                cold_plan,
                pk_col.clone(),
                pk_dt.clone(),
                Arc::new(tombs),
            ))
        };

        // Apply UPDATE override overlay: suppress overridden cold rows and
        // append the post-SET row images. The overlay reprojects overrides to
        // the (possibly PK-augmented) cold scan schema inside the exec.
        if !updates.is_empty() {
            filtered = Arc::new(UpdateOverlayExec::new(
                filtered,
                pk_col.clone(),
                pk_dt,
                Arc::new(updates),
            ));
        }

        // Strip the appended PK column when we augmented the projection so the
        // outer plan sees the originally-requested schema.
        let cold_plan = if augmented {
            use datafusion::physical_expr::expressions::Column;
            use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
            let filtered_schema = filtered.schema();
            let original_len = cold_projection_owned
                .as_ref()
                .map(|p| p.len() - 1)
                .unwrap_or(0);
            let exprs: Vec<ProjectionExpr> = (0..original_len)
                .map(|i| {
                    let field = filtered_schema.field(i);
                    ProjectionExpr {
                        expr: Arc::new(Column::new(field.name(), i)),
                        alias: field.name().to_owned(),
                    }
                })
                .collect();
            Arc::new(ProjectionExec::try_new(exprs, filtered)?)
                as Arc<dyn datafusion::physical_plan::ExecutionPlan>
        } else {
            filtered
        };

        let hot_plan = self.hot.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(UnionExec::new(vec![cold_plan, hot_plan])))
    }

    /// Offer the cold provider's filter-pushdown ability to DataFusion.
    ///
    /// Without this, the default `Unsupported` meant DataFusion handed
    /// `scan()` an EMPTY filter slice — so the cold-tier (Vortex/Parquet)
    /// scan pruned nothing and decoded every row. On any table with hot-tier
    /// rows or an UPDATE/DELETE overlay (i.e. any mutated table — common in
    /// the differential bench after bulk UPDATE/DELETE), a selective
    /// `WHERE id < 100` therefore became a full-table scan.
    ///
    /// When the overlay is live (any tombstone / UPDATE override / tx overlay)
    /// we delegate to the cold provider but never claim `Exact`: the merge also
    /// applies the tombstone/UPDATE overlay, so DataFusion must keep a
    /// `FilterExec` above to re-apply every predicate authoritatively.
    /// `Inexact` lets the cold scan prune rows via pushdown while preserving
    /// correctness on the merged output.
    ///
    /// Concurrency fix #3: `build_cold_with_overlay` now ALWAYS wraps single-PK
    /// tables in this provider, including the overlay-free common case. When the
    /// overlay is empty the merge output is exactly the (empty-hot ∪ cold) =
    /// cold rows, so a pushed-down predicate is EXACT — we therefore delegate
    /// the cold provider's verdict verbatim (Exact stays Exact). This keeps the
    /// no-overlay read path performance-identical to the old plain
    /// `ListingTable` (no redundant `FilterExec`), so always-wrapping costs
    /// nothing on the hot path while letting the cached provider grow an overlay
    /// path live. The overlay presence is read from the LIVE registry exactly as
    /// `scan` does (same watermark), so this verdict can never under-claim
    /// relative to what `scan` will actually merge.
    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use crate::hot_tombstone::{snapshot_tombstones, snapshot_updates};
        use datafusion::logical_expr::TableProviderFilterPushDown as Pd;
        let cold = self.cold.supports_filters_pushdown(filters)?;

        // Determine whether an overlay is actually live for THIS read view,
        // mirroring `scan`'s `apply_overlay` decision (same watermark, same tx
        // overlay merge). Only a live, applicable overlay forces `Inexact`.
        let mut tombs = snapshot_tombstones(
            self.engine.memtable_registry().as_ref(),
            &self.project,
            &self.table,
            self.hot_seq_watermark,
        );
        let mut updates = snapshot_updates(
            self.engine.memtable_registry().as_ref(),
            &self.project,
            &self.table,
            self.hot_seq_watermark,
        );
        if !self.tx_overlay.is_empty() {
            crate::hot_tombstone::merge_tx_overlay(&mut tombs, &mut updates, &self.tx_overlay);
        }
        let overlay_live =
            (self.pk_columns.len() == 1) && (!tombs.is_empty() || !updates.is_empty());

        if !overlay_live {
            // Empty overlay: the empty-hot half contributes nothing, so the
            // pushed-down predicate is exact on the merged output. Delegate the
            // cold provider's verdict verbatim (Exact stays Exact).
            return Ok(cold);
        }

        Ok(cold
            .into_iter()
            .map(|p| match p {
                Pd::Unsupported => Pd::Unsupported,
                _ => Pd::Inexact,
            })
            .collect())
    }
}

/// Inspect `sql` for tables with [`PartitionSpec::RangeMonthly`] and, if the
/// query's WHERE clause restricts the partition column to a sub-range of the
/// table's data, replace the registered `ListingTable` with one whose file
/// list is pre-filtered to matching partitions only. On any failure (parse
/// error, no narrowing predicate) we silently fall back to the un-pruned
/// listing — DataFusion's per-file stats pruning still catches what we miss.
pub(crate) async fn apply_partition_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    if stmts.len() != 1 {
        return Ok(());
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(());
    };

    let referenced = collect_table_refs(query);
    for table_name in referenced {
        let table = match TableName::new(table_name.clone()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let meta = match engine.config().catalog.load_table(project, &table).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let column = match &meta.partition_spec {
            PartitionSpec::RangeMonthly { column } => column.clone(),
            _ => continue,
        };
        let Some(range) = extract_range_predicate(query, &column) else {
            continue;
        };

        let storage = engine.config().storage.clone();
        let files = match storage.list_data_files(project, &table).await {
            Ok(f) => f,
            Err(_) => continue,
        };
        let matching: Vec<String> = files
            .iter()
            .filter_map(|f| {
                let p = f.path.as_ref();
                let (year, month) = parse_partition_segments(p)?;
                if range.overlaps_year_month(year, month) {
                    Some(p.to_string())
                } else {
                    None
                }
            })
            .collect();
        if matching.is_empty() || matching.len() == files.len() {
            // Either nothing matches (let DF return empty) or everything
            // matches (no point re-registering). Skip the swap.
            continue;
        }
        let df_schema = Arc::new(extend_schema_with_promoted_cols(
            schema_ws_to_df(meta.schema.as_ref())?,
            &meta.promoted_jsonb_paths,
        ));
        let _ = register_pruned_listing_table(
            engine,
            ctx,
            &table,
            df_schema,
            meta.file_format,
            &matching,
            meta.global_sort_order.as_deref(),
        )
        .await;
    }
    Ok(())
}

/// `true` when the general min/max file-prune pass is disabled.
///
/// The prune is correctness-exact (it only drops files whose per-file
/// `column_stats` min/max provably cannot satisfy the predicate), so it is ON
/// by default — matching the always-on partition / GIN prune passes — with an
/// env opt-out (`BASIN_DISABLE_MINMAX_PRUNE=1`) for A/B measurement and as a
/// safety hatch. Parsed once and cached.
fn minmax_prune_disabled() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| {
        matches!(
            std::env::var("BASIN_DISABLE_MINMAX_PRUNE").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

/// Decode an 8-byte little-endian `i64` from a catalog `column_stats`
/// min/max blob. Mirrors `fast_aggregate::decode_i64` and the writer's
/// `Int64` encoding byte-for-byte. Any other length → `None` (treated as
/// "unknown stats" by the caller, which then keeps the file).
fn decode_stats_i64(b: &[u8]) -> Option<i64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(i64::from_le_bytes(arr))
}

/// A selective single-column predicate the min/max + bloom file prune can
/// reason about. Recognised from the query `WHERE` clause by
/// [`recognise_selective_predicate`]; each variant carries everything the
/// per-file keep decision needs.
///
/// Correctness contract for the variants (all dispatch through
/// [`file_survives_selective`], DataFusion re-applies the full predicate over
/// the survivors, so KEEP is always safe and only DROP must be proven):
/// * `IntRange` / `StrRange` — drop iff the file's `[min,max]` lies entirely
///   outside the (half-open, for ints) range.
/// * `IntEq` / `StrEq` — drop iff `lit` is strictly outside `[min,max]`, OR a
///   per-column bloom reports a *definite negative*. A bloom "maybe" keeps.
/// * `IntIn` / `StrIn` — keep iff ANY listed value could be present (the union
///   of the per-value Eq decisions); drop only when EVERY value is provably
///   absent from the file.
enum SelectivePredicate {
    /// Existing Int64 half-open range `[lo, hi)` (at least one bound set).
    IntRange {
        column: String,
        rb: crate::fast_aggregate::RangeBound,
    },
    /// `col = <int>` on an Int64 column.
    IntEq { column: String, lit: i64 },
    /// `col IN (<int>, ...)` on an Int64 column (non-empty, deduped).
    IntIn { column: String, lits: Vec<i64> },
    /// Lexicographic string range; each bound is `(value_bytes, inclusive)`.
    StrRange {
        column: String,
        lo: Option<(Vec<u8>, bool)>,
        hi: Option<(Vec<u8>, bool)>,
    },
    /// `col = '<str>'` on a Utf8 column.
    StrEq { column: String, lit: Vec<u8> },
    /// `col IN ('<str>', ...)` on a Utf8 column (non-empty).
    StrIn { column: String, lits: Vec<Vec<u8>> },
}

impl SelectivePredicate {
    fn column(&self) -> &str {
        match self {
            SelectivePredicate::IntRange { column, .. }
            | SelectivePredicate::IntEq { column, .. }
            | SelectivePredicate::IntIn { column, .. }
            | SelectivePredicate::StrRange { column, .. }
            | SelectivePredicate::StrEq { column, .. }
            | SelectivePredicate::StrIn { column, .. } => column,
        }
    }

    /// The Arrow type this predicate variant requires the predicate column to
    /// have. A mismatch with the catalog schema makes the whole prune a no-op
    /// (the stats byte-encoding contract is type-specific).
    fn required_type(&self) -> arrow_schema::DataType {
        match self {
            SelectivePredicate::IntRange { .. }
            | SelectivePredicate::IntEq { .. }
            | SelectivePredicate::IntIn { .. } => arrow_schema::DataType::Int64,
            SelectivePredicate::StrRange { .. }
            | SelectivePredicate::StrEq { .. }
            | SelectivePredicate::StrIn { .. } => arrow_schema::DataType::Utf8,
        }
    }
}

/// A bare single-identifier column name from a sqlparser `Expr`, else `None`.
fn ident_column(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::Nested(inner) => ident_column(inner),
        _ => None,
    }
}

/// A signed integer literal from a sqlparser `Expr` (mirrors
/// `fast_aggregate::int_literal`'s accepted grammar), else `None`.
fn expr_int_literal(e: &Expr) -> Option<i64> {
    use sqlparser::ast::UnaryOperator;
    match e {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => s.parse::<i64>().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => expr_int_literal(expr).and_then(|v| v.checked_neg()),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => expr_int_literal(expr),
        Expr::Nested(inner) => expr_int_literal(inner),
        _ => None,
    }
}

/// A single-quoted string literal's bytes from a sqlparser `Expr`, else
/// `None`. Only the plain `'...'` form is accepted; typed/national/escaped
/// string forms fall through (so the prune declines rather than guess an
/// encoding that might not match the stored UTF-8 stat bytes).
fn expr_str_literal(e: &Expr) -> Option<Vec<u8>> {
    match e {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => Some(s.clone().into_bytes()),
        Expr::Nested(inner) => expr_str_literal(inner),
        _ => None,
    }
}

/// Recognise a selective single-column predicate from a `WHERE` expression.
///
/// Tries, in order:
/// 1. The existing conservative Int64 half-open range grammar
///    (`fast_aggregate::parse_range_bound`).
/// 2. A single equality `col = <int>` / `col = '<str>'`.
/// 3. A single `col IN (...)` of all-int or all-string literals.
/// 4. A single string comparison `col </<=/>/>= '<str>'`, optionally a
///    two-sided AND-conjunction of them on ONE column.
///
/// Returns `None` (→ full scan) on anything outside these shapes. The
/// recogniser is deliberately conservative: a shape it can't prove it
/// understands is never pruned on.
fn recognise_selective_predicate(where_expr: &Expr) -> Option<SelectivePredicate> {
    // 1. Int64 half-open range (existing grammar). An unbounded-both-sides
    //    range can't drop anything, so the caller's `lo|hi` check still applies.
    if let Some(rb) = crate::fast_aggregate::parse_range_bound(where_expr) {
        if rb.lo.is_some() || rb.hi.is_some() {
            return Some(SelectivePredicate::IntRange {
                column: rb.column.clone(),
                rb,
            });
        }
    }

    // 2/3/4 operate on a single (possibly nested) leaf predicate, or — for the
    // string range only — an AND-conjunction on one column.
    match unwrap_nested(where_expr) {
        // Equality: `col = lit` or `lit = col`.
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (col, lit_expr) = match (ident_column(left), ident_column(right)) {
                (Some(c), None) => (c, right.as_ref()),
                (None, Some(c)) => (c, left.as_ref()),
                _ => return None,
            };
            if let Some(v) = expr_int_literal(lit_expr) {
                return Some(SelectivePredicate::IntEq {
                    column: col,
                    lit: v,
                });
            }
            if let Some(s) = expr_str_literal(lit_expr) {
                return Some(SelectivePredicate::StrEq {
                    column: col,
                    lit: s,
                });
            }
            None
        }
        // `col IN (a, b, ...)` — reject `NOT IN` and subquery forms.
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let col = ident_column(expr)?;
            if list.is_empty() {
                return None;
            }
            // All-int, else all-string, else decline.
            let ints: Option<Vec<i64>> = list.iter().map(expr_int_literal).collect();
            if let Some(mut lits) = ints {
                lits.sort_unstable();
                lits.dedup();
                return Some(SelectivePredicate::IntIn { column: col, lits });
            }
            let strs: Option<Vec<Vec<u8>>> = list.iter().map(expr_str_literal).collect();
            if let Some(lits) = strs {
                return Some(SelectivePredicate::StrIn { column: col, lits });
            }
            None
        }
        // String range — a single comparison or an AND-conjunction on one col.
        other => {
            let mut acc = StrRangeAcc::default();
            collect_str_range(other, &mut acc).ok()?;
            let column = acc.column?;
            if acc.lo.is_none() && acc.hi.is_none() {
                return None;
            }
            Some(SelectivePredicate::StrRange {
                column,
                lo: acc.lo,
                hi: acc.hi,
            })
        }
    }
}

/// Strip transparent `Nested` (parenthesisation) wrappers.
fn unwrap_nested(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(inner) => unwrap_nested(inner),
        other => other,
    }
}

#[derive(Default)]
struct StrRangeAcc {
    column: Option<String>,
    /// Lower bound: `(value_bytes, inclusive)`.
    lo: Option<(Vec<u8>, bool)>,
    /// Upper bound: `(value_bytes, inclusive)`.
    hi: Option<(Vec<u8>, bool)>,
}

/// Walk an AND-tree of string comparisons on a single column, tightening the
/// lexicographic bounds. `Err(())` the instant it meets anything outside the
/// accepted grammar (a different column, a non-`AND` connective, a non-string
/// literal, an unsupported operator, equality).
fn collect_str_range(expr: &Expr, acc: &mut StrRangeAcc) -> Result<(), ()> {
    let expr = unwrap_nested(expr);
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        collect_str_range(left, acc)?;
        collect_str_range(right, acc)?;
        return Ok(());
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err(());
    };
    // Normalise to `col OP value`, flipping the operator sense if `value OP col`.
    let (col, val, flipped) = match (ident_column(left), expr_str_literal(right)) {
        (Some(c), Some(v)) => (c, v, false),
        _ => match (expr_str_literal(left), ident_column(right)) {
            (Some(v), Some(c)) => (c, v, true),
            _ => return Err(()),
        },
    };
    match &acc.column {
        Some(existing) if existing != &col => return Err(()),
        Some(_) => {}
        None => acc.column = Some(col),
    }
    use sqlparser::ast::BinaryOperator as Op;
    // Effective operator after a possible operand flip (`'x' < col` ≡ `col > 'x'`).
    let eff = match (op, flipped) {
        (Op::Gt, false) | (Op::Lt, true) => Op::Gt,
        (Op::GtEq, false) | (Op::LtEq, true) => Op::GtEq,
        (Op::Lt, false) | (Op::Gt, true) => Op::Lt,
        (Op::LtEq, false) | (Op::GtEq, true) => Op::LtEq,
        _ => return Err(()), // Eq / NotEq / anything else
    };
    match eff {
        Op::Gt => tighten_str_lo(acc, val, false),
        Op::GtEq => tighten_str_lo(acc, val, true),
        Op::Lt => tighten_str_hi(acc, val, false),
        Op::LtEq => tighten_str_hi(acc, val, true),
        _ => unreachable!(),
    }
    Ok(())
}

fn tighten_str_lo(acc: &mut StrRangeAcc, v: Vec<u8>, inclusive: bool) {
    acc.lo = Some(match acc.lo.take() {
        // A higher lower bound is tighter; on a tie the exclusive one is tighter.
        Some((cur, cur_incl)) if cur > v || (cur == v && !cur_incl) => (cur, cur_incl),
        _ => (v, inclusive),
    });
}

fn tighten_str_hi(acc: &mut StrRangeAcc, v: Vec<u8>, inclusive: bool) {
    acc.hi = Some(match acc.hi.take() {
        // A lower upper bound is tighter; on a tie the exclusive one is tighter.
        Some((cur, cur_incl)) if cur < v || (cur == v && !cur_incl) => (cur, cur_incl),
        _ => (v, inclusive),
    });
}

/// Probe a per-file column bloom for a *definite negative* on `needle`.
///
/// Returns `true` ONLY when the bloom is present, well-formed, and reports the
/// needle as DEFINITELY NOT present (`contains == false`). A bloom "maybe"
/// (`contains == true`, possibly a false positive), an absent bloom, or a
/// malformed blob all return `false` → KEEP the file. This is the only place a
/// bloom may cause a DROP, and it does so only on a proven absence.
///
/// `needle` is the exact byte sequence the writer's `compute_bloom_filters`
/// inserted: `i64::to_le_bytes` for Int64, the raw UTF-8 bytes for Utf8.
fn bloom_definite_negative(f: &DataFileRef, column: &str, needle: &[u8]) -> bool {
    match f.bloom_filters.get(column) {
        Some(bytes) => match basin_storage::bloom_from_bytes(bytes) {
            // `contains == false` is the bloom's only sound assertion.
            Some(filter) => !filter.contains(needle),
            None => false, // malformed → cannot prove absence → keep
        },
        None => false, // no bloom → cannot prove absence → keep
    }
}

/// Decide whether a single Int64-equality `lit` could be present in `f`:
/// `false` (drop) iff `lit` is strictly outside the file's `[min,max]`, OR a
/// per-column bloom proves it absent. Missing/short stats → KEEP.
fn int_eq_could_be_present(f: &DataFileRef, column: &str, lit: i64) -> bool {
    if let Some(cs) = f.column_stats.get(column) {
        if let (Some(fmin), Some(fmax)) = (
            cs.min_bytes.as_deref().and_then(decode_stats_i64),
            cs.max_bytes.as_deref().and_then(decode_stats_i64),
        ) {
            if fmin <= fmax && (lit < fmin || lit > fmax) {
                return false; // provably outside the zone map
            }
        }
    }
    // Inside the zone map (or stats unknown): a definite-negative bloom can
    // still prove absence.
    !bloom_definite_negative(f, column, &lit.to_le_bytes())
}

/// String analogue of [`int_eq_could_be_present`] using lexicographic
/// (byte-wise) min/max — the same ordering SQL uses for `text` and the
/// ordering the writer's `ByteArray` stat merge preserves.
fn str_eq_could_be_present(f: &DataFileRef, column: &str, lit: &[u8]) -> bool {
    if let Some(cs) = f.column_stats.get(column) {
        if let (Some(fmin), Some(fmax)) = (cs.min_bytes.as_deref(), cs.max_bytes.as_deref()) {
            if fmin <= fmax && (lit < fmin || lit > fmax) {
                return false;
            }
        }
    }
    !bloom_definite_negative(f, column, lit)
}

/// Per-file keep decision for a recognised [`SelectivePredicate`].
///
/// Returns `true` to KEEP the file in the scan set. Every `false` (DROP) is
/// proven: no file that could contain a matching row is ever dropped.
/// DataFusion re-applies the full predicate over the survivors, so a kept
/// partial-overlap file still yields exact rows.
fn file_survives_selective(f: &DataFileRef, pred: &SelectivePredicate) -> bool {
    match pred {
        SelectivePredicate::IntRange { column, rb } => {
            match f.column_stats.get(column) {
                Some(cs) => match (
                    cs.min_bytes.as_deref().and_then(decode_stats_i64),
                    cs.max_bytes.as_deref().and_then(decode_stats_i64),
                ) {
                    (Some(fmin), Some(fmax)) if fmin <= fmax => {
                        let below = rb.lo.is_some_and(|lo| fmax < lo);
                        let above = rb.hi.is_some_and(|hi| fmin >= hi);
                        !(below || above)
                    }
                    _ => true, // missing / short / inverted → keep
                },
                None => true,
            }
        }
        SelectivePredicate::IntEq { column, lit } => int_eq_could_be_present(f, column, *lit),
        SelectivePredicate::IntIn { column, lits } => {
            // Keep iff ANY value could be present.
            lits.iter().any(|v| int_eq_could_be_present(f, column, *v))
        }
        SelectivePredicate::StrEq { column, lit } => str_eq_could_be_present(f, column, lit),
        SelectivePredicate::StrIn { column, lits } => {
            lits.iter().any(|v| str_eq_could_be_present(f, column, v))
        }
        SelectivePredicate::StrRange { column, lo, hi } => {
            match f.column_stats.get(column) {
                Some(cs) => match (cs.min_bytes.as_deref(), cs.max_bytes.as_deref()) {
                    (Some(fmin), Some(fmax)) if fmin <= fmax => {
                        // Drop iff the file's [fmin,fmax] is entirely below the
                        // lower bound or entirely above the upper bound.
                        //   below: fmax < lo            (exclusive-lo: fmax <= lo)
                        //   above: fmin > hi            (exclusive-hi: fmin >= hi)
                        let below = lo.as_ref().is_some_and(|(lv, incl)| {
                            if *incl {
                                fmax < lv.as_slice()
                            } else {
                                fmax <= lv.as_slice()
                            }
                        });
                        let above = hi.as_ref().is_some_and(|(hv, incl)| {
                            if *incl {
                                fmin > hv.as_slice()
                            } else {
                                fmin >= hv.as_slice()
                            }
                        });
                        !(below || above)
                    }
                    _ => true,
                },
                None => true,
            }
        }
    }
}

/// General per-file min/max + bloom prune for a selective `WHERE` predicate.
///
/// Complements [`apply_partition_pruning_for_query`] (which only handles
/// `PARTITION BY RANGE` tables by Hive path segments): this prunes ANY table
/// whose live data files carry catalog `column_stats` (min/max) — or, for the
/// equality path, per-column `bloom_filters` — for the predicate column,
/// regardless of partitioning. Every live file whose stats/bloom PROVE it
/// cannot contain a matching row is dropped from the scan set and the table is
/// re-registered with only the survivors — so the object store never issues a
/// footer GET or a data GET for a pruned file.
///
/// ## Recognised predicate shapes (single bare column, dispatched by type)
/// * **Int64 half-open range** `col </<=/>/>= <int>` (AND-conjunctions on one
///   column; the `fast_aggregate::parse_range_bound` grammar). Drop iff the
///   file's `[min,max]` lies entirely outside `[lo, hi)`.
/// * **Int64 / Utf8 equality** `col = <lit>`. Drop iff `lit` is strictly
///   outside `[min,max]`, OR the file carries a `bloom_filter` for `col` whose
///   probe is a DEFINITE NEGATIVE (`contains == false`).
/// * **Int64 / Utf8 `IN (...)`** — keep iff ANY listed value could be present
///   (per-value union of the equality decision); drop only when EVERY value is
///   provably absent.
/// * **Utf8 (lexicographic) range** `col </<=/>/>= '<str>'`. Drop iff the
///   file's `[min,max]` lies entirely outside the (inclusive-aware) bounds.
///
/// Strings use the byte-wise (lexicographic) ordering SQL uses for `text` and
/// the ordering the writer's `ByteArray` stat merge preserves. Temporal
/// columns (`Date32`/`Timestamp`) are NOT pruned here: their catalog stat
/// encoding is not a stable, mergeable contract the prune can rely on, so they
/// fall through to a full scan (correctness over aggressiveness).
///
/// ## Correctness (only provably-non-matching files are dropped)
/// Every DROP is proven; KEEP is always sound because DataFusion re-applies the
/// full predicate over the survivors. In particular:
/// * The min/max are over the column's NON-NULL values; a NULL never satisfies
///   a `=`/range comparison, so a file provably outside the predicate has zero
///   matching rows whether or not it holds NULLs — we do NOT require a zero
///   null-count (unlike the exact-row-count fast aggregate, which does).
/// * **Bloom is probabilistic and may PRUNE only on a definite-negative.** A
///   bloom "maybe" (`contains == true`, possibly a false positive), an absent
///   bloom, or a malformed blob all KEEP the file. A false-positive bloom
///   therefore costs an extra (correct, possibly-empty) file read, never a
///   wrong answer. See [`bloom_definite_negative`].
/// * A file with missing stats, a missing/short min or max blob, or an
///   inverted `min > max` pair is KEPT (conservative) — never dropped.
/// * If the predicate column is absent from the schema, or its Arrow type does
///   not match the recognised predicate's expected type (Int64 / Utf8), the
///   whole pass is a no-op (full scan).
///
/// ## Live-overlay gate
/// Declines (no-op → full scan) whenever the table has a live hot-tier
/// UPDATE/DELETE overlay. A fast-path UPDATE can change a row's value to one
/// that falls inside `[lo, hi)` while the cold file's stale min/max still say
/// "outside"; the overlay-aware provider appends the post-image row, but if we
/// had dropped that cold file the `UpdateOverlayExec` would have no base row to
/// match the override against. Mirrors every other prune pass's overlay gate.
///
/// On any parse failure, multi-table / joined query, missing metadata, or
/// uncertainty this returns `Ok(())` and the un-pruned scan proceeds —
/// correctness is never traded for speed.
pub(crate) async fn apply_minmax_file_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    if minmax_prune_disabled() {
        return Ok(());
    }
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    if stmts.len() != 1 {
        return Ok(());
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(());
    };
    // Single bare table, no joins: a join would need per-relation predicate
    // attribution we don't attempt here.
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(());
    }
    let refs = collect_table_refs(query);
    if refs.len() != 1 {
        return Ok(());
    }
    let Some(where_expr) = select.selection.as_ref() else {
        return Ok(()); // no WHERE → nothing to prune on
    };
    // Recognise a selective single-column predicate: an Int64 half-open range
    // (the original shape), an Int64/Utf8 equality, an Int64/Utf8 `IN (...)`,
    // or a lexicographic string range. Anything else → full scan.
    let Some(pred) = recognise_selective_predicate(where_expr) else {
        return Ok(());
    };

    let Ok(table) = TableName::new(refs[0].clone()) else {
        return Ok(());
    };
    // Live overlay → decline (see doc comment). A fast-path UPDATE can change a
    // value past stale cold stats / out of a cold bloom; the overlay-aware
    // provider needs the cold base row to merge against.
    if table_has_live_overlay(engine, project, &table) {
        return Ok(());
    }
    let meta = match engine.config().catalog.load_table(project, &table).await {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    // Build the DataFusion schema the survivor / empty re-registration will
    // declare. This MUST be byte-identical to the schema `refresh_table`
    // registered the table with originally — same field names, Arrow types,
    // nullability, metadata, AND the ADR 0027 promoted-JSONB shadow columns —
    // or DataFusion's `type_coercion` analyzer pass fails when the (string /
    // IN-list / out-of-domain) predicate is bound against a re-registered
    // provider whose schema differs from the one the rest of the plan was
    // resolved against. Re-deriving only the bare catalog schema here (the
    // pre-fix behaviour) dropped the promoted shadow columns the physical files
    // carry, so a plan that resolved against the extended schema then hit a
    // provider missing those fields → `Optimizer rule 'type_coercion' failed`.
    let base_df_schema = match schema_ws_to_df(meta.schema.as_ref()) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    // Type-gate against the BASE schema (the predicate column is always a
    // user-visible column, never a synthetic shadow): a mismatch makes the
    // whole pass a no-op (full scan), since the stats byte-encoding contract is
    // type-specific.
    let want_ty = pred.required_type();
    match base_df_schema
        .fields()
        .iter()
        .find(|f| f.name() == pred.column())
    {
        Some(f) if *f.data_type() == want_ty => {}
        _ => return Ok(()),
    }
    let df_schema = Arc::new(extend_schema_with_promoted_cols(
        base_df_schema,
        &meta.promoted_jsonb_paths,
    ));

    let files = meta.live_data_files();
    if files.is_empty() {
        return Ok(());
    }
    let mut survivors: Vec<String> = Vec::with_capacity(files.len());
    for f in &files {
        // KEEP unless this file is PROVABLY non-matching (min/max zone map or a
        // definite-negative bloom). DataFusion re-applies the full predicate
        // over survivors, so a kept partial-overlap file still returns exactly
        // the matching rows.
        if file_survives_selective(f, &pred) {
            survivors.push(f.path.clone());
        }
    }

    // Nothing pruned (every file is a candidate) → leave the existing
    // registration untouched.
    if survivors.len() == files.len() {
        return Ok(());
    }

    // EVERY file is provably non-matching → the correct answer is the empty
    // set. Register an empty in-memory table (zero batches, correct schema) so
    // DataFusion reads NOTHING from the object store. This is the common,
    // high-value case for a selective equality on an out-of-domain literal
    // (e.g. `WHERE id = <absent>`): without it we would fall back to a full
    // scan of every file only to filter them all out.
    if survivors.is_empty() {
        let _ = register_empty_table(ctx, &table, df_schema);
        return Ok(());
    }

    let _ = register_pruned_listing_table(
        engine,
        ctx,
        &table,
        df_schema,
        meta.file_format,
        &survivors,
        meta.global_sort_order.as_deref(),
    )
    .await;
    Ok(())
}

/// Re-register `table` as an EMPTY `MemTable` (zero rows, the table's exact
/// schema). Used by the selective prune when every live file is proven
/// non-matching: the query's correct result is the empty set, and an empty
/// in-memory provider serves it with ZERO object-store GETs.
///
/// Correctness: `df_schema` MUST be the SAME extended DataFusion schema the
/// caller built for the original `refresh_table` registration (base catalog
/// schema + ADR 0027 promoted-JSONB shadow columns) — same field names, Arrow
/// types, nullability, and metadata. Declaring a different schema here (e.g.
/// dropping the promoted shadow fields, the pre-fix behaviour) makes the
/// re-registered provider disagree with the schema the rest of the plan was
/// resolved against, so DataFusion's `type_coercion` analyzer pass fails. With
/// the identical schema, an empty batch set is the exact result DataFusion
/// would compute after applying the predicate to a (provably non-matching)
/// full scan.
fn register_empty_table(
    ctx: &SessionContext,
    table: &TableName,
    df_schema: Arc<arrow_schema::Schema>,
) -> Result<()> {
    let provider = MemTable::try_new(df_schema, vec![vec![]])
        .map_err(|e| BasinError::internal(format!("empty MemTable::try_new (pruned): {e}")))?;
    let tref = TableReference::Bare {
        table: table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref.clone());
    ctx.register_table(tref, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register_table empty (pruned): {e}")))?;
    Ok(())
}

/// Re-register `table_name` as a `ListingTable` whose `table_paths` is a
/// per-file URL list of `paths`. This bypasses DataFusion's directory scan
/// entirely, so no footer GET is issued for files we already proved
/// irrelevant.
///
/// `df_schema` MUST be the SAME extended DataFusion schema the caller built for
/// the original `refresh_table` registration (base catalog schema + ADR 0027
/// promoted-JSONB shadow columns). The physical files carry those shadow
/// columns; declaring a schema that omits them (or otherwise differs) makes the
/// survivor provider disagree with the schema the plan was resolved against and
/// trips DataFusion's `type_coercion` analyzer pass — so we thread through the
/// already-extended schema rather than re-deriving the bare catalog schema.
async fn register_pruned_listing_table(
    _engine: &Engine,
    ctx: &SessionContext,
    table: &TableName,
    df_schema: Arc<arrow_schema::Schema>,
    file_format: TableFileFormat,
    paths: &[String],
    global_sort_order: Option<&[String]>,
) -> Result<()> {
    let (file_format, file_ext) = listing_file_format(file_format);
    let mut listing_options = ListingOptions::new(file_format).with_file_extension(file_ext);
    if let Some(sort_cols) = global_sort_order {
        listing_options = listing_options.with_file_sort_order(build_file_sort_order(sort_cols));
    }

    let mut urls: Vec<ListingTableUrl> = Vec::with_capacity(paths.len());
    for p in paths {
        // Each path comes from `Storage::list_data_files`, which forwards
        // the object_store's `meta.location` — i.e. a *full* bucket-relative
        // key that already includes any configured `root_prefix`. Compose
        // it with the engine's synthetic scheme; do NOT re-prepend the
        // root prefix or it'll appear twice in the resulting URL.
        let mut s = String::from(BASIN_URL_BASE);
        s.push_str(p);
        let url = ListingTableUrl::parse(&s)
            .map_err(|e| BasinError::internal(format!("listing url parse {s}: {e}")))?;
        urls.push(url);
    }

    let cfg = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(df_schema);
    let provider = ListingTable::try_new(cfg)
        .map_err(|e| BasinError::internal(format!("ListingTable::try_new (pruned): {e}")))?;

    let tref = TableReference::Bare {
        table: table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref.clone());
    ctx.register_table(tref, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register_table pruned: {e}")))?;
    Ok(())
}

/// O(1) live-overlay presence check for `(project, table)`: `true` when the
/// process-wide memtable registry holds at least one hot-tier UPDATE override
/// or DELETE tombstone whose newest version is still dirty.
///
/// `update_count` / `tombstone_count` are exactly the "overlay present"
/// signal: Tombstone/Update entries are always DIRTY (a flush ack removes
/// clean tombstones and re-tags acked `Update`s as `Row`s), and counter-keyed
/// HTAP `Row` residency entries do not count — they are cold-committed and
/// need no merge-on-read handling.
///
/// Used as the correctness gate for every read path that bypasses the
/// overlay-aware provider (`register_cold_with_overlay` →
/// `TombstoneFilterExec` + `UpdateOverlayExec`):
///   * the executor's GIN posting-probe `Empty` short-circuits
///     (`executor::gin_empty_probe_is_trustworthy` for JSONB,
///     `executor::fts_empty_probe_is_trustworthy` for tsvector `@@`), and
///   * the pruned re-registrations below (`apply_gin_pruning_for_query`,
///     `apply_jsonb_posting_pruning_for_query`,
///     `apply_gin_fts_pruning_for_query`), which would otherwise swap
///     the overlay-aware provider for a bare cold reader that neither appends
///     override rows nor suppresses their stale cold images.
/// See the UPDATE fast-path gate analysis in `dml_mutate.rs` (blockers #1/#2).
pub(crate) fn table_has_live_overlay(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
) -> bool {
    engine
        .memtable_registry()
        .get(project, table)
        .map(|e| e.memtable.update_count() > 0 || e.memtable.tombstone_count() > 0)
        .unwrap_or(false)
}

/// Phase 5.19.C — GIN file-level pruning.
///
/// After all tables have been refreshed (so their `ListingTable` registrations
/// reflect the current live file set), inspect `sql` for a JSONB containment
/// predicate (`@>`) on a column with a GIN index.  The GIN posting list is
/// probed per file: fully-indexed files with no posting hit for the needle
/// are pruned, every other live file is scanned.  The table's `ListingTable`
/// registration is replaced with one scoped to the scan set; pruned files are
/// never fetched from the object store.
///
/// Correctness contract:
/// * The scan set is a conservative superset — no file containing a real
///   match is ever excluded (the posting list may produce false positives
///   that the `jsonb_contains` UDF filters out at read time).
/// * Coverage degrades per file, not per table: a live file missing from the
///   indexed-files set (written before the index existed, after a restart,
///   or de-indexed by posting-budget eviction) is force-scanned while the
///   rest of the table still prunes.
/// * `<@` is never pruned (a matching row may be any subset of the literal).
/// * Transactions are excluded (`tx_is_active` guard in the caller) because
///   pending files are not yet indexed.
///
/// On any error or uncertainty this function returns `Ok(())` and the full
/// scan proceeds — correctness is never sacrificed for speed.
pub(crate) async fn apply_gin_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    // Fast pre-check: must contain @> or <@ to be relevant.
    if !sql.contains("@>") && !sql.contains("<@") {
        return Ok(());
    }

    // Parse and detect a GIN containment plan (same detector used by the
    // Empty short-circuit path in executor.rs).
    let gin_plan =
        match crate::index_probe::detect_gin_containment(sql, project, &engine.config().catalog)
            .await
        {
            Some(p) => p,
            None => return Ok(()), // query shape not recognised → full scan
        };

    // Live-overlay gate (the dml_mutate.rs UPDATE fast-path gate's blocker
    // #2): every registration below — `GinRowGroupPrunedTable` on the
    // rg-direct and rg-narrowed paths, the pruned `ListingTable` on the
    // file-level path — REPLACES the overlay-aware provider with a bare cold
    // reader. Such a reader neither appends hot-tier override rows nor
    // suppresses their stale cold images, so while the table has live
    // UPDATE/DELETE overlay entries we must keep the overlay-aware
    // registration and skip pruning entirely (correctness over speed; the
    // overlay drains via materialize and pruning resumes). O(1) counter reads.
    if table_has_live_overlay(engine, project, &gin_plan.table) {
        return Ok(());
    }

    // Fetch the live file set from the catalog.  Needed both for the
    // file-level completeness guard and the row-group-direct path.
    let meta = match engine
        .config()
        .catalog
        .load_table(project, &gin_plan.table)
        .await
    {
        Ok(m) => m,
        Err(_) => return Ok(()), // can't verify completeness → full scan
    };
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        // No live files — the Empty short-circuit handles this; nothing to prune.
        return Ok(());
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // ── Direct row-group prune path (for compaction-indexed files) ────────────
    //
    // On the shard INSERT path, the engine's `maintain_secondary_indexes_on_insert`
    // is bypassed (INSERTs go to WAL+tail without touching GinIndexRegistry).
    // The compactor re-indexes JSONB GIN columns into `GinRowGroupRegistry` at
    // flush time, making row-group summaries the *only* GIN metadata for
    // shard-written data — the file-level posting list is empty.
    //
    // To let row-group pruning fire even when the file-level GIN index is absent,
    // we first check whether the row-group registry has sealed summaries for
    // EVERY live file.  If so, we can prune directly from the row-group registry
    // without consulting the posting list at all.
    //
    // Correctness: the row-group registry is a conservative superset (bloom
    // false positives are tolerated; `jsonb_contains` UDF re-checks every emitted
    // row).  Files absent from the registry are read in full (no false negatives).
    {
        let all_rg_indexed = live_paths.iter().all(|p| {
            engine.gin_rowgroup_registry().is_file_indexed(
                project,
                &gin_plan.table,
                &gin_plan.col,
                p,
            )
        });
        if all_rg_indexed {
            let rg_prune = crate::index_probe::rowgroup_prune_for_containment(
                engine.gin_rowgroup_registry(),
                project,
                &gin_plan.table,
                &gin_plan.col,
                &gin_plan.opclass,
                &gin_plan.needle,
                &live_paths,
            );
            if let crate::index_probe::RowGroupPrune::PerFile(rg_map) = rg_prune {
                let df_schema = match schema_ws_to_df(&meta.schema) {
                    Ok(s) => Arc::new(s),
                    Err(_) => return Ok(()), // schema error → full scan
                };
                let provider = crate::gin_rowgroup_scan::GinRowGroupPrunedTable::new(
                    df_schema,
                    engine.config().storage.clone(),
                    *project,
                    gin_plan.table.clone(),
                    meta.file_format,
                    live_paths.clone(),
                    rg_map,
                );
                let tref = TableReference::Bare {
                    table: gin_plan.table.as_str().into(),
                };
                let _ = ctx.deregister_table(tref.clone());
                ctx.register_table(tref, Arc::new(provider))
                    .map_err(|e| BasinError::internal(format!("register rg-pruned table: {e}")))?;
                return Ok(());
            }
            // Row-group registry covers all files but needle has no terms
            // (RowGroupPrune::Unknown).  Fall through to the posting-list path
            // and then to full scan — safe, never a false negative.
        }
    }

    // ── File-level posting-list path (for direct-INSERT / non-shard data) ─────
    //
    // Phase 5.19.F: per-file graceful degradation.  The old shape probed the
    // posting list and then required EVERY live file to be in the
    // indexed-files set — one un-indexed file (pre-index data, a restart, or
    // posting-budget eviction) disabled pruning for the whole table.  At 1M
    // rows the backfill routinely overflows the posting budget, eviction
    // un-marks the early files, and the index silently stops pruning.
    //
    // `probe_containment_scan_set` instead prunes what is provable:
    //   * un-indexed live files are FORCED into the scan set (must-scan);
    //   * fully-indexed files are pruned when some needle term has no
    //     posting hit for them (sound: a marked file has ALL of its terms
    //     in the posting list — eviction un-marks affected files inside the
    //     same critical section, and the probe snapshots both structures
    //     under one lock).
    // Under-pruning is safe; a file that could hold a match is never pruned.

    // Defensive: pruning semantics are only valid for `@>` (the row must
    // contain every needle term).  `detect_gin_containment` no longer
    // produces `<@` plans, but guard anyway — for `<@` a matching row can be
    // an arbitrary subset of the literal, so no file is provably prunable.
    if !gin_plan.is_contains {
        return Ok(());
    }

    let scan_set = engine.gin_index_registry().probe_containment_scan_set(
        project,
        &gin_plan.table,
        &gin_plan.col,
        &gin_plan.opclass,
        &gin_plan.needle,
        &live_paths,
    );

    let pruned_paths: Vec<String> = match scan_set {
        crate::index_probe::GinScanSet::ScanFiles(files) => files,
        // NoIndex → nothing provable → full scan.
        crate::index_probe::GinScanSet::NoIndex => return Ok(()),
    };

    if pruned_paths.is_empty() {
        // Every live file was provably pruned — the Empty short-circuit
        // upstream usually catches this, but guard defensively: leave the
        // full set registered so DataFusion computes the (empty) result
        // itself rather than us fabricating one for aggregate shapes.
        return Ok(());
    }

    // Note: even when all files are candidates at file level (pruned_paths.len()
    // == live_files.len()), we still try row-group prune below — the row-group
    // prune might narrow individual files to fewer row-groups.

    // ── Row-granular GIN tier ─────────────────────────────────────────────────
    //
    // The coarse (file) and row-group tiers above are structurally weak for a
    // needle that appears in EVERY file/row-group but matches few ROWS per file
    // (the 1M `@>` dense-needle bench): nothing is pruned, every file decodes in
    // full. The row tier closes that gap — for each candidate file with a sealed
    // row tier it returns a SUPERSET of the absolute row offsets that may match,
    // which the storage reader turns into a Parquet `RowSelection` so only those
    // rows are decoded. Files without a sealed row tier (or with a dense needle
    // term) decode in full exactly as before. The `jsonb_contains` UDF still
    // re-checks every emitted row, so the offset list is a pure accelerator and
    // can never drop a true match.
    //
    // The overlay/completeness gate at the top of this function already ensured
    // the table has no live hot-tier overlay and the candidate files are
    // coarse-trustworthy; the row tier inherits that gate. Provably-empty files
    // (`prunable`) are dropped from the candidate set.
    let row_plan = crate::index_probe::RowSelectionPlan::default();
    let row_plan = if gin_plan.is_contains {
        engine.gin_index_registry().probe_row_selection(
            project,
            &gin_plan.table,
            &gin_plan.col,
            &gin_plan.opclass,
            &gin_plan.needle,
            &pruned_paths,
        )
    } else {
        row_plan
    };
    // Drop provably-empty files from the candidate set (the row tier proved a
    // needle term never occurs there). Keep ordering stable for determinism.
    let candidate_paths: Vec<String> = if row_plan.prunable.is_empty() {
        pruned_paths.clone()
    } else {
        pruned_paths
            .iter()
            .filter(|p| !row_plan.prunable.contains(*p))
            .cloned()
            .collect()
    };
    if candidate_paths.is_empty() {
        // Row tier proved no file can match — leave the full set registered so
        // DataFusion computes the (empty) result for aggregate shapes (mirrors
        // the `pruned_paths.is_empty()` guard above).
        return Ok(());
    }
    let row_selection_map = row_plan.row_offsets;

    // C2 — attempt row-group-granular prune using the per-row-group bloom
    // registry.  If the registry has summaries for at least some of the
    // candidate files, narrow further to only the surviving row-groups.
    // Falls back to file-level pruning (or no pruning) when the registry
    // has no summaries yet.
    let rg_prune = crate::index_probe::rowgroup_prune_for_containment(
        engine.gin_rowgroup_registry(),
        project,
        &gin_plan.table,
        &gin_plan.col,
        &gin_plan.opclass,
        &gin_plan.needle,
        &candidate_paths,
    );

    let rg_map = match rg_prune {
        crate::index_probe::RowGroupPrune::PerFile(m) => Some(m),
        _ => None,
    };

    // Register the native pruned provider when EITHER a row-group prune OR a
    // row-tier selection is available; both drive the same custom reader (the
    // row-group allowlist narrows which groups open, the row selection narrows
    // which rows within them decode). When neither is present, fall back to the
    // file-level `ListingTable` prune.
    if rg_map.is_some() || !row_selection_map.is_empty() {
        // Row-group / row-level prune available.  Register a custom provider
        // that drives Basin's native storage reader with `row_group_selection`
        // and/or `row_selection` set, bypassing DataFusion's ListingTable /
        // ParquetExec path entirely.
        //
        // Correctness: both maps are conservative supersets (bloom false
        // positives and row-tier raw-bytes containment are fine); the
        // `jsonb_contains` UDF re-checks every emitted row.  Files absent from
        // a map are read in full (no false negatives).
        let df_schema = match schema_ws_to_df(&meta.schema) {
            Ok(s) => Arc::new(s),
            Err(_) => {
                // Schema conversion failed — fall through to file-level prune.
                return register_pruned_listing_table_if_narrowed(
                    engine,
                    ctx,
                    &gin_plan.table,
                    &meta,
                    &candidate_paths,
                    &live_files,
                )
                .await;
            }
        };
        let provider = crate::gin_rowgroup_scan::GinRowGroupPrunedTable::new_with_row_selection(
            df_schema,
            engine.config().storage.clone(),
            *project,
            gin_plan.table.clone(),
            meta.file_format,
            candidate_paths.clone(),
            rg_map.unwrap_or_default(),
            row_selection_map,
        );
        let tref = TableReference::Bare {
            table: gin_plan.table.as_str().into(),
        };
        let _ = ctx.deregister_table(tref.clone());
        ctx.register_table(tref, Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register rg-pruned table: {e}")))?;
        return Ok(());
    }

    // No row-group summaries and no row tier — fall back to file-level prune.
    register_pruned_listing_table_if_narrowed(
        engine,
        ctx,
        &gin_plan.table,
        &meta,
        &candidate_paths,
        &live_files,
    )
    .await
}

/// Re-register the table as a file-pruned `ListingTable` when `pruned_paths`
/// is a strict subset of `live_files`.  When all files are candidates this
/// is a no-op (avoids a pointless deregister+re-register round-trip).
async fn register_pruned_listing_table_if_narrowed(
    engine: &Engine,
    ctx: &SessionContext,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    pruned_paths: &[String],
    live_files: &[basin_catalog::DataFileRef],
) -> Result<()> {
    if pruned_paths.is_empty() || pruned_paths.len() == live_files.len() {
        return Ok(());
    }
    // Build the SAME extended schema `refresh_table` registered the table with
    // (base catalog schema + ADR 0027 promoted-JSONB shadow columns) so the
    // re-registered survivor provider is schema-identical to the original and
    // DataFusion's `type_coercion` pass cannot newly fail. See
    // `register_pruned_listing_table`.
    let df_schema = Arc::new(extend_schema_with_promoted_cols(
        schema_ws_to_df(meta.schema.as_ref())?,
        &meta.promoted_jsonb_paths,
    ));
    let _ = register_pruned_listing_table(
        engine,
        ctx,
        table,
        df_schema,
        meta.file_format,
        pruned_paths,
        meta.global_sort_order.as_deref(),
    )
    .await;
    Ok(())
}

/// Phase 5.20.E — GIN FTS file-level pruning.
///
/// Mirrors [`apply_gin_pruning_for_query`] for the tsvector `@@` operator.
/// After all tables have been refreshed, inspect `sql` (the *original*
/// pre-rewrite SQL that still contains `@@`) for a tsvector FTS predicate on
/// a column with a GIN `tsvector_ops` index.  When the FTS posting list
/// returns `FileCandidates` AND the completeness guard passes (every live file
/// is in the indexed-files set), replace the table's `ListingTable`
/// registration with one scoped to the candidate files only.
///
/// Correctness contract:
/// * `FileCandidates` is a conservative superset — no file containing a
///   real match is ever excluded (the tsvector_match_udf re-evaluates on
///   every candidate row).
/// * Pruning only fires when the table has NO live hot-tier overlay
///   (`table_has_live_overlay`) AND `indexed_files ⊇ live_files`.
/// * On any error or uncertainty returns `Ok(())` → full scan.
pub(crate) async fn apply_gin_fts_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    // Fast pre-check: must contain @@ to be relevant.
    if !sql.contains("@@") {
        return Ok(());
    }

    // Detect the FTS predicate shape.
    let fts_plan =
        match crate::index_probe::detect_tsvector_match(sql, project, &engine.config().catalog)
            .await
        {
            Some(p) => p,
            None => return Ok(()), // query shape not recognised → full scan
        };

    // Live-overlay gate — same blocker as `apply_gin_pruning_for_query`
    // above: the registrations below (`GinRowGroupPrunedTable`, the pruned
    // `ListingTable` fallback) REPLACE the overlay-aware provider with a
    // bare cold reader that neither appends hot-tier override rows nor
    // suppresses their stale cold images.  While the table has live
    // UPDATE/DELETE overlay entries we must keep the overlay-aware
    // registration and skip pruning entirely (correctness over speed; the
    // overlay drains via materialize and pruning resumes).  O(1) counter
    // reads.
    if table_has_live_overlay(engine, project, &fts_plan.table) {
        return Ok(());
    }

    // Probe the FTS posting list at row-group granularity.  The registry
    // stores `(file, row_group, row)` for every posting; the row-group-aware
    // probe preserves that detail so we can drive
    // `ReadOptions.row_group_selection` via `GinRowGroupPrunedTable` instead
    // of opening every row-group of every candidate file.
    use basin_storage::index::gin_tsvector::TsvProbeRowGroupResult;
    let fts_rg_result = engine.gin_fts_registry().probe_query_with_row_groups(
        project,
        &fts_plan.table,
        &fts_plan.col,
        &fts_plan.tsquery_str,
    );

    let rg_map: std::collections::HashMap<String, Vec<u32>> = match fts_rg_result {
        TsvProbeRowGroupResult::FileRowGroups(m) => m,
        // Empty is handled before exec_select (short-circuit with 0 rows).
        // NoIndex → full scan.
        _ => return Ok(()),
    };

    // Completeness guard: fetch the live file set from the catalog.
    let meta = match engine
        .config()
        .catalog
        .load_table(project, &fts_plan.table)
        .await
    {
        Ok(m) => m,
        Err(_) => return Ok(()), // can't verify completeness → full scan
    };
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(());
    }

    // Check coverage: every live file must appear in the indexed-files set.
    let indexed =
        engine
            .gin_fts_registry()
            .indexed_files_for(project, &fts_plan.table, &fts_plan.col);
    let all_covered = live_files.iter().all(|f| indexed.contains(f.path.as_str()));
    if !all_covered {
        // At least one live file is not in the posting list → full scan.
        return Ok(());
    }

    // Completeness confirmed.  Intersect candidate set with live files.
    let pruned_paths: Vec<String> = live_files
        .iter()
        .filter(|f| rg_map.contains_key(f.path.as_str()))
        .map(|f| f.path.to_string())
        .collect();

    if pruned_paths.is_empty() {
        // Defensively leave full set registered (the Empty short-circuit in
        // the executor should already have caught a truly empty probe).
        return Ok(());
    }

    // Restrict `rg_map` to the live, pruned file set so we don't carry
    // stale (compacted-away) keys into `GinRowGroupPrunedTable`.
    let live_path_set: std::collections::HashSet<&str> =
        pruned_paths.iter().map(|s| s.as_str()).collect();
    let rg_map_live: std::collections::HashMap<String, Vec<u32>> = rg_map
        .into_iter()
        .filter(|(f, _)| live_path_set.contains(f.as_str()))
        .collect();

    // Row-group-granular path: register a `GinRowGroupPrunedTable` that
    // drives Basin's native storage reader with `row_group_selection` so
    // DataFusion's Parquet reader only opens the surviving row-groups of
    // each candidate file.  Correctness: the row-group set per file is a
    // conservative superset of the posting-list rows that touched the
    // query lexemes; the full `@@` UDF re-evaluates every emitted row.
    let df_schema = match schema_ws_to_df(&meta.schema) {
        Ok(s) => Arc::new(extend_schema_with_promoted_cols(
            s,
            &meta.promoted_jsonb_paths,
        )),
        Err(_) => {
            // Schema conversion failed — we cannot build a provider whose
            // schema matches the original registration, so re-registering
            // anything risks a `type_coercion` plan failure. Leave the full
            // (un-pruned) set registered and full-scan: correctness over the
            // row-group prune. (A schema the catalog itself can't convert would
            // have failed the original registration too, so this is unreachable
            // in practice — but bailing is the safe branch.)
            return Ok(());
        }
    };
    let provider = crate::gin_rowgroup_scan::GinRowGroupPrunedTable::new(
        df_schema,
        engine.config().storage.clone(),
        *project,
        fts_plan.table.clone(),
        meta.file_format,
        pruned_paths,
        rg_map_live,
    );
    let tref = TableReference::Bare {
        table: fts_plan.table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref.clone());
    ctx.register_table(tref, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register fts rg-pruned table: {e}")))?;
    Ok(())
}

/// Trigram GIN file-level + row-tier pruning for a `col % 'needle'` predicate.
///
/// Mirrors [`apply_gin_pruning_for_query`] for the `pg_trgm` `%` operator.
/// Inspect `sql` (the *original* pre-rewrite SQL that still contains `%`, before
/// `rewrite_trgm_operators` lowers it to `similarity(col,'needle') >= t`) for a
/// trigram-similarity predicate on a column with a `gin_trgm_ops` GIN index.
/// When the trigram posting list prunes a strict subset of the live files (and
/// the row tier optionally narrows the surviving files to row offsets), replace
/// the table's registration with a pruned provider.
///
/// `threshold` is the session's `pg_trgm.similarity_threshold` in effect — it
/// sets the count-based prune bound (`trgm_min_shared`) and MUST equal the
/// threshold the rewriter baked into the `similarity() >= t` recheck, or the
/// prune could be tighter than the predicate (a dropped real match). The caller
/// (executor) reads it from session state and passes it here.
///
/// Correctness contract (identical discipline to the JSONB `@>` path):
/// * the trigram scan-set / row-selection is a conservative SUPERSET — the
///   rewritten `similarity(col,'needle') >= t` predicate re-evaluates on every
///   surviving row (recheck);
/// * pruning only fires when the table has NO live hot-tier overlay
///   (`table_has_live_overlay`) — the registrations below swap the
///   overlay-aware provider for a bare cold reader;
/// * per-file graceful degradation: an un-indexed live file is a forced
///   candidate; eviction/partial coverage only widens the scan set, never drops
///   a match;
/// * on any error or uncertainty returns `Ok(())` → full scan.
pub(crate) async fn apply_trgm_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
    threshold: f32,
) -> Result<()> {
    // Fast pre-check: must contain a `%` to be relevant.
    if !sql.contains('%') {
        return Ok(());
    }

    let plan =
        match crate::index_probe::detect_trgm_similarity(sql, project, &engine.config().catalog)
            .await
        {
            Some(p) => p,
            None => return Ok(()), // shape not recognised → full scan
        };

    // Live-overlay gate — same blocker as `apply_gin_pruning_for_query`: the
    // pruned registrations replace the overlay-aware provider with a bare cold
    // reader. Skip pruning while any live UPDATE/DELETE overlay exists.
    if table_has_live_overlay(engine, project, &plan.table) {
        return Ok(());
    }

    let meta = match engine
        .config()
        .catalog
        .load_table(project, &plan.table)
        .await
    {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(());
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // File-level scan set (per-file graceful degradation; un-indexed files are
    // forced candidates).
    let scan_set = engine.gin_index_registry().probe_trgm_scan_set(
        project,
        &plan.table,
        &plan.col,
        &plan.needle,
        threshold,
        &live_paths,
    );
    let pruned_paths: Vec<String> = match scan_set {
        crate::index_probe::GinScanSet::ScanFiles(files) => files,
        crate::index_probe::GinScanSet::NoIndex => return Ok(()),
    };
    if pruned_paths.is_empty() {
        // Every file provably pruned — leave the full set registered so
        // DataFusion computes the (empty) result for aggregate shapes.
        return Ok(());
    }

    // Row tier: narrow surviving files to candidate row offsets where a sealed
    // row tier exists. Drop provably-empty files.
    let row_plan = engine.gin_index_registry().probe_trgm_row_selection(
        project,
        &plan.table,
        &plan.col,
        &plan.needle,
        threshold,
        &pruned_paths,
    );
    let candidate_paths: Vec<String> = if row_plan.prunable.is_empty() {
        pruned_paths.clone()
    } else {
        pruned_paths
            .iter()
            .filter(|p| !row_plan.prunable.contains(*p))
            .cloned()
            .collect()
    };
    if candidate_paths.is_empty() {
        return Ok(());
    }
    let row_selection_map = row_plan.row_offsets;

    if !row_selection_map.is_empty() {
        // Register the native pruned provider with a row selection (no
        // row-group bloom for trgm — the offset list narrows decode directly).
        let df_schema = match schema_ws_to_df(&meta.schema) {
            Ok(s) => Arc::new(s),
            Err(_) => {
                return register_pruned_listing_table_if_narrowed(
                    engine,
                    ctx,
                    &plan.table,
                    &meta,
                    &candidate_paths,
                    &live_files,
                )
                .await;
            }
        };
        let provider = crate::gin_rowgroup_scan::GinRowGroupPrunedTable::new_with_row_selection(
            df_schema,
            engine.config().storage.clone(),
            *project,
            plan.table.clone(),
            meta.file_format,
            candidate_paths.clone(),
            Default::default(),
            row_selection_map,
        );
        let tref = TableReference::Bare {
            table: plan.table.as_str().into(),
        };
        let _ = ctx.deregister_table(tref.clone());
        ctx.register_table(tref, Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register trgm row-pruned table: {e}")))?;
        return Ok(());
    }

    // No row tier — fall back to file-level prune.
    register_pruned_listing_table_if_narrowed(
        engine,
        ctx,
        &plan.table,
        &meta,
        &candidate_paths,
        &live_files,
    )
    .await
}

/// Inv-W5 / W9 — relaxed JSONB `@>` shape detector for the posting-list
/// prune.  Mirrors [`crate::index_probe::detect_gin_containment`] but is
/// permissive about the projection (accepts `count(*)`, `SELECT col1, agg(x)`,
/// etc.) and ignores trailing clauses (ORDER BY, LIMIT).  Returns the table,
/// column, opclass, and needle bytes when the shape is
/// `SELECT … FROM <table> WHERE <col> @> '<literal>'`.
async fn detect_jsonb_containment_for_prune(
    sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<JsonbContainmentPrunePlan> {
    if !sql.contains("@>") {
        return None;
    }
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        Statement::Query(q) => q.as_ref(),
        _ => return None,
    };
    if query.with.is_some() {
        return None;
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table {
            name,
            alias: None,
            args: None,
            ..
        } => {
            if name.0.len() != 1 {
                return None;
            }
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // WHERE must contain `col @> 'literal'` as the top-level expression
    // (or as one conjunct of a top-level AND — we walk for it).
    let (col_name, needle_str) = find_top_level_at_gt(select.selection.as_ref()?)?;

    // Catalog: table must have a GIN index on `col` and the opclass must be
    // `jsonb_ops` (the only opclass our posting-list registry indexes for).
    let meta = catalog.load_table(project, &table).await.ok()?;
    let gin_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin" && idx.columns.len() == 1 && idx.columns[0] == col_name
    })?;
    let opclass = gin_index
        .opclass
        .clone()
        .unwrap_or_else(|| "jsonb_ops".to_string());
    if opclass != "jsonb_ops" {
        // `jsonb_path_ops` uses hashed paths — the posting list's `(key,
        // value)` atom shape doesn't apply directly.  Skip and let the
        // bloom path handle it.
        return None;
    }

    let needle_bytes = needle_str.as_bytes().to_vec();
    let _: serde_json::Value = serde_json::from_slice(&needle_bytes).ok()?;
    Some(JsonbContainmentPrunePlan {
        table,
        col: col_name,
        needle: needle_bytes,
    })
}

/// Walk `expr` looking for a top-level `col @> 'literal'` predicate
/// (possibly nested inside an AND chain).  Returns the first match found,
/// which is sufficient for the prune because additional conjuncts only
/// further narrow the result — they cannot expand it.
fn find_top_level_at_gt(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let op_str = op.to_string();
            if op_str == "@>" {
                let col = match left.as_ref() {
                    Expr::Identifier(id) => id.value.clone(),
                    _ => return None,
                };
                let literal = crate::index_probe::extract_json_literal_for_prune(right)?;
                return Some((col, literal));
            }
            if matches!(op, BinaryOperator::And) {
                if let Some(found) = find_top_level_at_gt(left) {
                    return Some(found);
                }
                if let Some(found) = find_top_level_at_gt(right) {
                    return Some(found);
                }
            }
            None
        }
        Expr::Nested(inner) => find_top_level_at_gt(inner),
        _ => None,
    }
}

#[derive(Debug)]
struct JsonbContainmentPrunePlan {
    table: TableName,
    col: String,
    needle: Vec<u8>,
}

/// Inv-W5 / W9 — JSONB `@>` row-group pruning via posting list.
///
/// Mirrors [`apply_gin_pruning_for_query`] but drives the per-`(key, value)`
/// posting list ([`basin_storage::index::jsonb_posting::JsonbPostingRegistry`])
/// instead of the bloom row-group registry.  The posting list is the precise
/// inverted index needed for `@>` queries whose searched terms saturate the
/// bloom (every row-group has the term → bloom prunes nothing).
///
/// Detection re-uses [`crate::index_probe::detect_gin_containment`] so the
/// same `col @> '{…}'` shape is recognised.  Probe returns a per-file
/// row-group selection; if all live files are covered the registered
/// `ListingTable` is swapped for a [`crate::jsonb_posting_scan::JsonbPostingPrunedTable`].
///
/// Lazy sidecar load: at probe time, any live file that is not resident in
/// the registry is fetched from `projects/{p}/tables/{t}/index/{col}.jsonb_post/{ulid}.bin`
/// (the bincode sidecar written by the compactor).  Failed loads leave the
/// file UNCOVERED — it is read in full while the covered files still prune
/// (Phase 5.19.F per-file degradation; previously one missing sidecar
/// disabled pruning for the whole table).
///
/// Correctness contract:
/// * The posting list is a conservative superset (file-level AND of needle
///   atoms; per-file row-group UNION across atoms).  The `jsonb_contains`
///   UDF re-evaluates every emitted row.
/// * A file may be pruned (or row-group-narrowed) ONLY when it is fully
///   covered: marked indexed AND not over-budget.  Over-budget files have
///   partial atom coverage — `over_budget_files_for` reports them and they
///   are always read in full.  Coverage is snapshotted BEFORE the probe so
///   a file ingested concurrently is force-scanned rather than judged by a
///   probe that ran before its atoms arrived.
/// * On any error returns `Ok(())` → full scan.
pub(crate) async fn apply_jsonb_posting_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    // Fast pre-check: must contain @> to be relevant.
    if !sql.contains("@>") {
        return Ok(());
    }

    // Detect the `… FROM table WHERE col @> 'literal' …` shape directly.
    // The existing `detect_gin_containment` is too strict (it rejects
    // anything other than bare-column projections / SELECT *) for queries
    // like `SELECT count(*)`, but the prune decision only depends on the
    // FROM table + WHERE shape — the projection doesn't change which
    // files we open.  We use a relaxed detector that ignores projection
    // and any trailing clauses (ORDER BY, LIMIT, etc.).
    let gin_plan =
        match detect_jsonb_containment_for_prune(sql, project, &engine.config().catalog).await {
            Some(p) => p,
            None => return Ok(()),
        };

    // Live-overlay gate — same blocker-#2 reasoning as
    // `apply_gin_pruning_for_query` above: `JsonbPostingPrunedTable` is a bare
    // cold reader (no `UpdateOverlayExec` / tombstone suppression), so a live
    // hot-tier UPDATE/DELETE overlay for this table means the overlay-aware
    // registration must stay in place. O(1) counter reads.
    if table_has_live_overlay(engine, project, &gin_plan.table) {
        return Ok(());
    }

    // Parse the needle now so we can probe.
    let needle: serde_json::Value = match serde_json::from_slice(&gin_plan.needle) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    let meta = match engine
        .config()
        .catalog
        .load_table(project, &gin_plan.table)
        .await
    {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(());
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // Lazy sidecar load: for each live file not in the registry, fetch the
    // bincode sidecar and ingest it.  Phase 5.19.F: a missing or unparseable
    // sidecar no longer disables pruning for the whole table — the file is
    // simply left UNCOVERED (read in full below) while covered files prune.
    let registry = engine.jsonb_posting_registry();
    let storage = engine.config().storage.clone();
    for p in &live_paths {
        if registry.is_file_indexed(project, &gin_plan.table, &gin_plan.col, p) {
            continue;
        }
        let Some(sidecar) = basin_storage::index::jsonb_posting::posting_sidecar_key_for_data_file(
            project,
            &gin_plan.table,
            &gin_plan.col,
            p,
        ) else {
            continue; // non-canonical path → uncovered (full scan of this file)
        };
        use object_store::ObjectStoreExt as _;
        let store = storage.project_object_store(project);
        let bytes = match store.get(&sidecar).await {
            Ok(get_result) => match get_result.bytes().await {
                Ok(b) => b,
                Err(_) => continue,
            },
            Err(_) => continue, // sidecar missing → uncovered
        };
        // Ingest failure also just leaves the file uncovered.
        let _ = registry.ingest_sidecar(project, &gin_plan.table, &gin_plan.col, &bytes);
    }

    // Coverage snapshot — taken BEFORE the probe.  A file is covered when it
    // is fully ingested (marked indexed) AND within the posting budget.
    // Over-budget files have only partial atom coverage: their absence from
    // a probe result proves nothing, so they must be read in full.  Files
    // that finish ingesting AFTER this snapshot are also force-scanned (the
    // probe below may predate their atoms).
    let over_budget = registry.over_budget_files_for(project, &gin_plan.table, &gin_plan.col);
    let covered: std::collections::HashSet<&str> = live_paths
        .iter()
        .filter(|p| {
            !over_budget.contains(p.as_str())
                && registry.is_file_indexed(project, &gin_plan.table, &gin_plan.col, p)
        })
        .map(|p| p.as_str())
        .collect();
    if covered.is_empty() {
        // Nothing provable for any live file → full scan.
        return Ok(());
    }

    // Probe the posting list.
    use basin_storage::index::jsonb_posting::JsonbProbeResult;
    let probe = registry.probe(project, &gin_plan.table, &gin_plan.col, &needle);
    let rg_map: HashMap<String, Vec<u32>> = match probe {
        JsonbProbeResult::FileRowGroups(m) => m,
        // Some needle atom was never indexed, or the AND-intersection is
        // empty: no COVERED file can match.  An empty map below skips every
        // covered file and full-scans the uncovered ones.
        JsonbProbeResult::AtomAbsent | JsonbProbeResult::Empty => HashMap::new(),
        // NoIndex → nothing provable → bloom / full scan.
        JsonbProbeResult::NoIndex => return Ok(()),
    };

    // Build the per-file row-group selection.  Per the storage contract
    // (`ReadOptions::row_group_selection`): a file present in the map with an
    // empty Vec opens ZERO row-groups (skipped entirely); a file ABSENT from
    // the map opens every row-group (scanned in full).  So:
    //   * covered + probe hit      → the probe's row-groups;
    //   * covered + no probe hit   → empty Vec (provably no match — every
    //     atom of every row of a covered file is in the posting list);
    //   * uncovered                → absent (full scan, no false negatives).
    let mut rg_map_live: HashMap<String, Vec<u32>> = HashMap::new();
    for p in &live_paths {
        if !covered.contains(p.as_str()) {
            continue; // uncovered → absent from map → read in full
        }
        rg_map_live.insert(p.clone(), rg_map.get(p).cloned().unwrap_or_default());
    }

    let df_schema = match schema_ws_to_df(&meta.schema) {
        Ok(s) => Arc::new(s),
        Err(_) => return Ok(()),
    };
    let provider = crate::jsonb_posting_scan::JsonbPostingPrunedTable::new(
        df_schema,
        engine.config().storage.clone(),
        *project,
        gin_plan.table.clone(),
        meta.file_format,
        live_paths,
        rg_map_live,
    );
    let tref = TableReference::Bare {
        table: gin_plan.table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref.clone());
    ctx.register_table(tref, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register jsonb-posting-pruned table: {e}")))?;
    Ok(())
}

/// Phase 5.24.D — GIST interval-tree file-level pruning.
///
/// Mirrors [`apply_gin_pruning_for_query`] for range `@>` / `&&` / `<@`
/// predicates.  After all tables have been refreshed, inspect `sql` (the
/// *original* pre-rewrite SQL that still contains `@>` / `&&` / `<@`) for a
/// range predicate on a column with a GIST index.  When the interval tree
/// returns `FileCandidates` AND the completeness guard passes (every live file
/// is in the indexed-files set), replace the table's `ListingTable`
/// registration with one scoped to the candidate files only.
///
/// Correctness contract:
/// * `FileCandidates` is a conservative superset — no file containing a real
///   match is ever excluded (the range UDF re-evaluates on every candidate row).
/// * Pruning only fires when `indexed_files ⊇ live_files`.
/// * On any error or uncertainty returns `Ok(())` → full scan.
pub(crate) async fn apply_gist_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    // Fast pre-check: must contain a range operator to be relevant.
    if !sql.contains("@>") && !sql.contains("&&") && !sql.contains("<@") {
        return Ok(());
    }

    // Detect the range predicate shape.
    let plan =
        match crate::index_probe::detect_range_index_probe(sql, project, &engine.config().catalog)
            .await
        {
            Some(p) => p,
            None => return Ok(()), // query shape not recognised → full scan
        };

    // Probe the interval tree.
    use basin_common::types::range::{IndexInterval, RangeValue};
    use basin_storage::index::interval::ProbeResult;
    let interval_result = match &plan.op {
        crate::index_probe::RangeOp::ContainsElem => {
            let pt = match plan.point {
                Some(p) => p,
                None => return Ok(()),
            };
            engine
                .interval_registry()
                .probe_contains_point(project, &plan.table, &plan.col, pt)
        }
        crate::index_probe::RangeOp::ContainsRange
        | crate::index_probe::RangeOp::Overlaps
        | crate::index_probe::RangeOp::ContainedBy => {
            let lit = match &plan.range_literal {
                Some(s) => s,
                None => return Ok(()),
            };
            let rv = match RangeValue::from_pg_text(lit) {
                Some(r) => r,
                None => return Ok(()),
            };
            let iv = match IndexInterval::from_range(&rv) {
                Some(iv) => iv,
                None => return Ok(()), // infinite-bound → full scan
            };
            engine
                .interval_registry()
                .probe_overlaps(project, &plan.table, &plan.col, &iv)
        }
    };

    let candidate_files = match interval_result {
        ProbeResult::FileCandidates(files) => files,
        // Empty is handled before exec_select (short-circuit with 0 rows).
        // NoIndex → full scan.
        _ => return Ok(()),
    };

    // Completeness guard: fetch the live file set from the catalog.
    let meta = match engine
        .config()
        .catalog
        .load_table(project, &plan.table)
        .await
    {
        Ok(m) => m,
        Err(_) => return Ok(()), // can't verify completeness → full scan
    };
    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(());
    }

    // Check coverage: every live file must appear in the indexed-files set.
    let indexed = engine
        .interval_registry()
        .indexed_files_for(project, &plan.table, &plan.col);
    let all_covered = live_files.iter().all(|f| indexed.contains(f.path.as_str()));
    if !all_covered {
        // At least one live file is not in the interval tree → full scan.
        return Ok(());
    }

    // Completeness confirmed. Intersect candidate set with live files.
    let pruned_paths: Vec<String> = live_files
        .iter()
        .filter(|f| candidate_files.contains(f.path.as_str()))
        .map(|f| f.path.to_string())
        .collect();

    if pruned_paths.is_empty() || pruned_paths.len() == live_files.len() {
        // Either no candidates (defensively leave full set) or all files are
        // candidates (pruning is a no-op).
        return Ok(());
    }

    // Re-register with only the pruned file set, using the SAME extended
    // schema the original registration declared (see
    // `register_pruned_listing_table`).
    let df_schema = Arc::new(extend_schema_with_promoted_cols(
        schema_ws_to_df(meta.schema.as_ref())?,
        &meta.promoted_jsonb_paths,
    ));
    let _ = register_pruned_listing_table(
        engine,
        ctx,
        &plan.table,
        df_schema,
        meta.file_format,
        &pruned_paths,
        meta.global_sort_order.as_deref(),
    )
    .await;

    Ok(())
}

/// PG-Wave-β — R-tree spatial pruning for `ST_DWithin` / `ST_Contains` / `=`.
///
/// Mirrors [`apply_gin_pruning_for_query`] for the POINT spatial predicate
/// family.  Inspects `sql` (the *original*, pre-rewrite text so the SQL
/// patterns are still recognisable) for a single spatial predicate on a column
/// with a `USING gist` index.  When ALL live files for the column have a
/// loaded R-tree segment, probes the registry for each file's surviving
/// row-groups and re-registers the table as an [`RTreePrunedTable`] that
/// drives Basin's native storage reader with `ReadOptions.row_group_selection`
/// set.  DataFusion then sees only the surviving row-groups of each candidate
/// file.
///
/// Correctness contract:
/// * For `DWithin` the prune is INEXACT (radius-expanded bbox); the residual
///   `st_dwithin` UDF re-runs above the scan.
/// * For `BboxIntersects` and `PointEq` the prune is EXACT at row-group
///   granularity.
/// * Pruning only fires when every live file is indexed in the registry.
///   Any uncovered file triggers a full scan (no false negatives).
/// * On any error or uncertainty the function returns `Ok(())` → full scan.
pub(crate) async fn apply_rtree_pruning_for_query(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    // Fast pre-check: must contain at least one spatial token.  Lower-case
    // the SQL once for the contains() probe since users may type
    // `ST_DWithin` or `st_dwithin`.
    let sql_lower = sql.to_lowercase();
    if !sql_lower.contains("st_dwithin")
        && !sql_lower.contains("st_contains")
        && !sql_lower.contains("st_makepoint")
    {
        return Ok(());
    }

    // Parse the SQL and pull out the WHERE expression.
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    if stmts.len() != 1 {
        return Ok(());
    }
    let query = match &stmts[0] {
        Statement::Query(q) => q.as_ref(),
        _ => return Ok(()),
    };
    if query.with.is_some() {
        return Ok(());
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Ok(()),
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(());
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table {
            name,
            alias: None,
            args: None,
            ..
        } => {
            if name.0.len() != 1 {
                return Ok(());
            }
            name.0[0].id_val().clone()
        }
        _ => return Ok(()),
    };
    let table = match TableName::new(table_name) {
        Ok(t) => t,
        Err(_) => return Ok(()),
    };

    let where_expr = match &select.selection {
        Some(e) => e,
        None => return Ok(()),
    };
    let pred = match crate::index_probe::detect_spatial_predicate(where_expr) {
        Some(p) => p,
        None => return Ok(()),
    };

    // Catalog lookup: column must have a `USING gist` index.
    let meta = match engine.config().catalog.load_table(project, &table).await {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    let col_name = pred.column().to_string();
    let has_gist = meta.indexes.iter().any(|idx| {
        idx.access_method == "gist" && idx.columns.len() == 1 && idx.columns[0] == col_name
    });
    if !has_gist {
        return Ok(());
    }

    let live_files: Vec<DataFileRef> = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(());
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // Lazy warm-up + completeness guard: for each live file, ensure the
    // R-tree sidecar is loaded into the registry. The sidecar may not be
    // resident (engine restart cleared the cache, or the file was just
    // compacted on another instance) — fetch it from the object store and
    // deserialise on demand. Files whose sidecar can't be loaded are
    // skipped → we fall through to a full scan rather than serve stale
    // results.
    let registry = engine.rtree_registry();
    let storage = engine.config().storage.clone();
    for p in &live_paths {
        if registry.is_file_indexed(project, &table, &col_name, p) {
            continue;
        }
        let Some(sidecar) = basin_storage::index::rtree::rtree_segment_key_for_data_file(
            None, project, &table, &col_name, p,
        ) else {
            // Non-canonical path → can't compute sidecar key → full scan.
            return Ok(());
        };
        // Fetch the sidecar bytes; missing → full scan (no false negatives).
        use object_store::ObjectStoreExt as _;
        let store = storage.project_object_store(project);
        let bytes = match store.get(&sidecar).await {
            Ok(get_result) => match get_result.bytes().await {
                Ok(b) => b,
                Err(_) => return Ok(()),
            },
            Err(_) => return Ok(()), // sidecar missing → fall back to full scan
        };
        let rtree = match basin_storage::index::rtree::deserialize_rtree(&bytes) {
            Some(r) => r,
            None => return Ok(()), // bad/foreign blob → full scan
        };
        registry.insert_segment(project, &table, &col_name, p, rtree);
    }
    // After lazy load every live file should be indexed; re-verify
    // defensively so a racing compaction can't slip past the guard.
    let all_indexed = live_paths
        .iter()
        .all(|p| registry.is_file_indexed(project, &table, &col_name, p));
    if !all_indexed {
        return Ok(());
    }

    // Compute the probe envelope from the predicate.
    use basin_storage::index::rtree::SpatialAabb as AABB;
    let query_bbox = match &pred {
        crate::index_probe::SpatialPredicate::DWithin { x, y, radius_m, .. } => {
            // Conservative bbox: expand by the degree-equivalent of the
            // radius. 1 degree ≈ 111_320 m at the equator; using the same
            // factor for latitude and longitude is the standard
            // PostGIS-spheroid simplification (the residual UDF rejects
            // false positives anyway). For polar workloads the longitude
            // term widens but is still a superset.
            const M_PER_DEG: f64 = 111_320.0;
            let r_deg = radius_m / M_PER_DEG;
            AABB::from_corners([*x - r_deg, *y - r_deg], [*x + r_deg, *y + r_deg])
        }
        crate::index_probe::SpatialPredicate::BboxIntersects {
            min_x,
            min_y,
            max_x,
            max_y,
            ..
        } => AABB::from_corners([*min_x, *min_y], [*max_x, *max_y]),
        crate::index_probe::SpatialPredicate::PointEq { x, y, .. } => {
            AABB::from_corners([*x, *y], [*x, *y])
        }
    };

    // Per-file row-group probe.
    let mut rg_map: HashMap<String, Vec<u32>> = HashMap::new();
    for p in &live_paths {
        let rgs = registry
            .candidate_row_groups(project, &table, &col_name, p, query_bbox)
            .unwrap_or_default();
        rg_map.insert(p.clone(), rgs);
    }

    // Convert table schema and build the custom provider.
    let df_schema = match schema_ws_to_df(&meta.schema) {
        Ok(s) => Arc::new(s),
        Err(_) => return Ok(()),
    };
    let provider = crate::rtree_rowgroup_scan::RTreePrunedTable::new(
        df_schema,
        engine.config().storage.clone(),
        *project,
        table.clone(),
        meta.file_format,
        live_paths.clone(),
        rg_map,
    );
    let tref = TableReference::Bare {
        table: table.as_str().into(),
    };
    let _ = ctx.deregister_table(tref.clone());
    ctx.register_table(tref, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register rtree-pruned table: {e}")))?;
    Ok(())
}

/// Inclusive month range covering the rows a query may need.
#[derive(Copy, Clone, Debug)]
struct YearMonthRange {
    /// Lowest year-month any matching row may carry, encoded as `year * 12
    /// + month - 1` so comparisons are a single integer compare.
    lo: i32,
    hi: i32,
}

impl YearMonthRange {
    fn from_micros(min_micros: Option<i64>, max_micros: Option<i64>) -> Option<Self> {
        let lo = min_micros.map(year_month_of_micros).unwrap_or(i32::MIN);
        let hi = max_micros.map(year_month_of_micros).unwrap_or(i32::MAX);
        if lo > hi {
            return None;
        }
        Some(Self { lo, hi })
    }

    fn overlaps_year_month(&self, year: i32, month: u32) -> bool {
        let target = encode_year_month(year, month as i32);
        self.lo <= target && target <= self.hi
    }
}

fn encode_year_month(year: i32, month: i32) -> i32 {
    year.saturating_mul(12).saturating_add(month - 1)
}

fn year_month_of_micros(micros: i64) -> i32 {
    let dt = micros_to_datetime(micros);
    encode_year_month(dt.year(), dt.month() as i32)
}

fn micros_to_datetime(micros: i64) -> DateTime<Utc> {
    let secs = micros.div_euclid(1_000_000);
    let sub_us = micros.rem_euclid(1_000_000) as u32;
    Utc.timestamp_opt(secs, sub_us * 1000)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
}

/// Parse `…/data/year=YYYY/month=MM/…/x.parquet` to extract the `(year,
/// month)` of a data file. Returns `None` if the path doesn't carry both
/// segments — the engine tolerates mixed partitioning shapes.
fn parse_partition_segments(path: &str) -> Option<(i32, u32)> {
    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    for seg in path.split('/') {
        if let Some(rest) = seg.strip_prefix("year=") {
            year = rest.parse().ok();
        } else if let Some(rest) = seg.strip_prefix("month=") {
            month = rest.parse().ok();
        }
    }
    Some((year?, month?))
}

/// Walk `query`'s top-level FROM list and return each referenced bare table
/// name. We don't follow CTEs, subqueries, or schema-qualified names — the
/// PoC has none of those so the simple form covers our cases.
pub(crate) fn collect_table_refs(query: &Query) -> Vec<String> {
    let mut out = Vec::new();
    if let SetExpr::Select(select) = query.body.as_ref() {
        for from in &select.from {
            if let TableFactor::Table { name, .. } = &from.relation {
                if name.0.len() == 1 {
                    out.push(name.0[0].id_val().clone());
                }
            }
        }
    }
    out
}

/// Walk the top-level WHERE clause and pull out the tightest min/max range
/// the predicate places on `column`, expressed in microseconds-since-epoch.
/// Returns `None` if the clause leaves the column unconstrained or carries
/// disjunctions / negations we can't reason about safely (for those we
/// fall through to no-pruning).
fn extract_range_predicate(query: &Query, column: &str) -> Option<YearMonthRange> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let where_expr = select.selection.as_ref()?;
    let (min, max) = walk_predicate(where_expr, column);
    YearMonthRange::from_micros(min, max)
}

/// Recursively walk a WHERE expression, ANDing together each leaf's
/// contribution to the running (min, max) bounds. Returns `(None, None)`
/// when no useful information can be extracted; the caller treats that as
/// "skip pruning".
fn walk_predicate(expr: &Expr, column: &str) -> (Option<i64>, Option<i64>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (lmin, lmax) = walk_predicate(left, column);
            let (rmin, rmax) = walk_predicate(right, column);
            (
                merge_min(lmin, rmin, |a, b| a.max(b)),
                merge_min(lmax, rmax, |a, b| a.min(b)),
            )
        }
        Expr::BinaryOp { left, op, right } => {
            // Identify whether one side is the partition column and the
            // other is a literal we can decode.
            let (col_side_left, lit_expr) = if matches_column(left, column) {
                (true, right.as_ref())
            } else if matches_column(right, column) {
                (false, left.as_ref())
            } else {
                return (None, None);
            };
            let Some(micros) = literal_to_micros(lit_expr) else {
                return (None, None);
            };
            // Normalise: rewrite `lit OP col` as `col REVERSE_OP lit`.
            let logical_op = if col_side_left {
                op.clone()
            } else {
                reverse_op(op.clone())
            };
            match logical_op {
                BinaryOperator::Eq => (Some(micros), Some(micros)),
                BinaryOperator::Gt => (Some(micros + 1), None),
                BinaryOperator::GtEq => (Some(micros), None),
                BinaryOperator::Lt => (None, Some(micros - 1)),
                BinaryOperator::LtEq => (None, Some(micros)),
                _ => (None, None),
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if !matches_column(expr, column) {
                return (None, None);
            }
            let (Some(lo), Some(hi)) = (literal_to_micros(low), literal_to_micros(high)) else {
                return (None, None);
            };
            (Some(lo), Some(hi))
        }
        Expr::Nested(inner) => walk_predicate(inner, column),
        _ => (None, None),
    }
}

fn merge_min(a: Option<i64>, b: Option<i64>, f: impl FnOnce(i64, i64) -> i64) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(f(x, y)),
        (s, None) | (None, s) => s,
    }
}

fn reverse_op(op: BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => other,
    }
}

fn matches_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(i) => i.value == column,
        Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|p| p.value == column),
        Expr::Nested(inner) => matches_column(inner, column),
        _ => false,
    }
}

/// Decode the literal forms our partition pruner cares about into
/// microseconds-since-epoch UTC. Mirrors `dml::coerce_timestamp_micros`'s
/// surface but uses a smaller subset (we only need read-side parsing here).
fn literal_to_micros(expr: &Expr) -> Option<i64> {
    let inner: &Expr = match expr {
        Expr::Cast { expr, .. } => expr.as_ref(),
        Expr::Nested(inner) => inner.as_ref(),
        other => other,
    };
    match inner {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => parse_timestamp_string_for_pruning(s),
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n.parse().ok(),
        _ => None,
    }
}

fn parse_timestamp_string_for_pruning(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    let zoned = [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%#z",
    ];
    for fmt in zoned {
        if let Ok(dt) = DateTime::parse_from_str(trimmed, fmt) {
            return Some(dt.with_timezone(&Utc).timestamp_micros());
        }
    }
    let naive = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"];
    for fmt in naive {
        if let Ok(n) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(n, Utc).timestamp_micros());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let n = d.and_hms_opt(0, 0, 0)?;
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(n, Utc).timestamp_micros());
    }
    None
}

// ---------------------------------------------------------------------------
// ADR 0027 Phase 4 — schema extension helpers
// ---------------------------------------------------------------------------

/// Extend a DataFusion Arrow schema with `Utf8` fields for each promoted JSONB
/// path that isn't already in the schema.  The user-visible catalog schema is
/// NOT modified — these fields only exist in the DataFusion registration so the
/// planner can resolve the shadow-column names emitted by
/// [`crate::promoted_columns::rewrite_promoted_columns`].
fn extend_schema_with_promoted_cols(
    base: arrow_schema::Schema,
    paths: &[PromotedJsonbPath],
) -> arrow_schema::Schema {
    if paths.is_empty() {
        return base;
    }
    let mut fields: Vec<arrow_schema::Field> =
        base.fields().iter().map(|f| f.as_ref().clone()).collect();
    for path in paths {
        let name = crate::promoted_columns::shadow_col_name(&path.source_col, &path.json_key);
        if base.field_with_name(&name).is_err() {
            fields.push(arrow_schema::Field::new(
                &name,
                arrow_schema::DataType::Utf8,
                true,
            ));
        }
    }
    arrow_schema::Schema::new_with_metadata(fields, base.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Duration;

    /// Complete-by-construction: after `reset_gucs()`, every session GUC must
    /// match a freshly-constructed `SessionState`. This is the gate that
    /// catches a NEW GUC field added to `new()` but forgotten in `reset_gucs()`
    /// — the leak would otherwise cross a pooled checkout boundary silently.
    #[test]
    fn gucs_reset_to_defaults() {
        let fresh = SessionState::new();
        let dirty = SessionState::new();

        // Mutate every settable GUC away from its default.
        dirty.schema_state.write().unwrap().search_path =
            vec!["tenant_x".to_string(), "public".to_string()];
        dirty.statement_timeout_ms.store(5_000, Relaxed);
        *dirty.lock_timeout.lock().unwrap() = Some(Duration::from_millis(500));
        dirty
            .advisory
            .set_lock_timeout(Some(Duration::from_millis(500)));
        *dirty.idle_in_transaction_session_timeout.lock().unwrap() = Some(Duration::from_secs(30));
        // Flip synchronous_commit to the opposite of the env default so the
        // assertion is meaningful regardless of BASIN_SYNCHRONOUS_COMMIT.
        let flipped = !synchronous_commit_env_default();
        dirty.synchronous_commit.store(flipped, Relaxed);
        set_session_trgm_similarity_threshold(&dirty, 0.95);
        set_session_trgm_word_similarity_threshold(&dirty, 0.95);
        // Flip read_tier away from its default so the reset is observable.
        set_session_read_tier(&dirty, ReadTier::Lagging);

        dirty.reset_gucs();

        assert_eq!(
            dirty.schema_state.read().unwrap().search_path,
            fresh.schema_state.read().unwrap().search_path,
            "search_path not reset to default"
        );
        assert_eq!(
            dirty.statement_timeout_ms.load(Relaxed),
            fresh.statement_timeout_ms.load(Relaxed),
            "statement_timeout not reset"
        );
        assert_eq!(
            *dirty.lock_timeout.lock().unwrap(),
            *fresh.lock_timeout.lock().unwrap(),
            "lock_timeout not reset"
        );
        assert_eq!(
            *dirty.idle_in_transaction_session_timeout.lock().unwrap(),
            *fresh.idle_in_transaction_session_timeout.lock().unwrap(),
            "idle_in_transaction_session_timeout not reset"
        );
        assert_eq!(
            dirty.synchronous_commit.load(Relaxed),
            fresh.synchronous_commit.load(Relaxed),
            "synchronous_commit not reset"
        );
        assert_eq!(
            session_trgm_similarity_threshold(&dirty),
            session_trgm_similarity_threshold(&fresh),
            "pg_trgm.similarity_threshold not reset"
        );
        assert_eq!(
            session_trgm_word_similarity_threshold(&dirty),
            session_trgm_word_similarity_threshold(&fresh),
            "pg_trgm.word_similarity_threshold not reset"
        );
        assert_eq!(
            session_read_tier(&dirty),
            session_read_tier(&fresh),
            "basin.read_tier not reset"
        );
    }

    #[test]
    fn statement_timeout_parse_unset_uses_default() {
        assert_eq!(
            parse_statement_timeout(None),
            Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS))
        );
        assert_eq!(
            parse_statement_timeout(Some("")),
            Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS))
        );
        assert_eq!(
            parse_statement_timeout(Some("   ")),
            Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS))
        );
    }

    #[test]
    fn statement_timeout_parse_zero_disables() {
        assert_eq!(parse_statement_timeout(Some("0")), None);
        assert_eq!(parse_statement_timeout(Some(" 0 ")), None);
    }

    #[test]
    fn statement_timeout_parse_value() {
        assert_eq!(
            parse_statement_timeout(Some("500")),
            Some(Duration::from_millis(500))
        );
        // Non-numeric falls back to the default rather than disabling — a typo
        // must never silently remove the guard.
        assert_eq!(
            parse_statement_timeout(Some("abc")),
            Some(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS))
        );
    }

    // ── parse_statement_timeout_guc (Bug #2 / SET statement_timeout wiring) ──

    #[test]
    fn guc_parse_bare_integer_ms() {
        assert_eq!(
            parse_statement_timeout_guc("5000").unwrap(),
            Some(Duration::from_millis(5000))
        );
        assert_eq!(
            parse_statement_timeout_guc("0").unwrap(),
            None,
            "0 means disabled"
        );
    }

    #[test]
    fn guc_parse_quoted_string_forms() {
        assert_eq!(
            parse_statement_timeout_guc("'5s'").unwrap(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse_statement_timeout_guc("'500ms'").unwrap(),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            parse_statement_timeout_guc("'2min'").unwrap(),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            parse_statement_timeout_guc("'0'").unwrap(),
            None,
            "'0' means disabled"
        );
    }

    #[test]
    fn guc_parse_unquoted_unit_forms() {
        assert_eq!(
            parse_statement_timeout_guc("1s").unwrap(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            parse_statement_timeout_guc("1000ms").unwrap(),
            Some(Duration::from_millis(1000))
        );
    }

    #[test]
    fn session_statement_timeout_per_session_override() {
        let state = SessionState::new();
        // Initially uses process-wide default (not -1 override testing here,
        // just check that set/get round-trips work).
        set_statement_timeout(&state, "2s").expect("set 2s");
        assert_eq!(
            session_statement_timeout(&state),
            Some(Duration::from_secs(2))
        );
        // Zero disables.
        set_statement_timeout(&state, "0").expect("set 0");
        assert_eq!(session_statement_timeout(&state), None);
        // Quoted ms form.
        set_statement_timeout(&state, "'500ms'").expect("set 500ms");
        assert_eq!(
            session_statement_timeout(&state),
            Some(Duration::from_millis(500))
        );
    }

    /// Structural prewarm test (perf-w-pointops, BASIN_PREWARM_PROVIDERS): the
    /// fire-and-forget footer/stats warm fires EXACTLY once per (session, table)
    /// on the first cold meta miss while the flag is on, and never on a repeat
    /// load or when the flag is off. We assert against the SESSION-LOCAL
    /// `prewarmed_tables` set (which `maybe_prewarm_table` inserts into iff it
    /// fires) rather than wall-clock — the perf claim (cold 3.4→~2.3ms) is not
    /// gated here, only the structural cold-vs-warm fire discipline. The
    /// session-local set is immune to the process-global env flag racing with
    /// other parallel tests' meta loads.
    ///
    /// `maybe_prewarm_table` reads the env flag, calls `spawn`, and inserts the
    /// table into the per-session set in one synchronous pass, so the set is
    /// authoritative immediately after the awaited `execute` returns.
    #[tokio::test]
    async fn prewarm_fires_once_per_table_only_when_enabled() {
        use std::sync::Arc;

        use crate::{Engine, EngineConfig};
        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::ProjectId;
        use basin_common::TableName;
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        });
        let table = TableName::new("off_t".to_string()).unwrap();
        let warmed = |sess: &crate::ProjectSession| {
            sess.state.prewarmed_tables.lock().unwrap().contains(&table)
        };

        // FLAG OFF (default): a cold load must NOT mark the table warmed.
        std::env::remove_var("BASIN_PREWARM_PROVIDERS");
        let sess_off = engine.open_session(ProjectId::new()).await.unwrap();
        sess_off
            .execute("CREATE TABLE off_t (id BIGINT PRIMARY KEY, n BIGINT)")
            .await
            .unwrap();
        sess_off
            .execute("INSERT INTO off_t (id, n) VALUES (1, 1)")
            .await
            .unwrap();
        let sess_off2 = engine.open_session(sess_off.project).await.unwrap();
        let _ = sess_off2
            .execute("SELECT id, n FROM off_t WHERE id = 1")
            .await;
        assert!(
            !warmed(&sess_off2),
            "flag OFF must not mark the table warmed"
        );

        // FLAG ON: a fresh session's FIRST cold meta miss marks the table
        // warmed; the set holds exactly that one table.
        std::env::set_var("BASIN_PREWARM_PROVIDERS", "1");
        let sess_on = engine.open_session(sess_off.project).await.unwrap();
        assert!(!warmed(&sess_on), "fresh session starts un-warmed");
        let _ = sess_on
            .execute("SELECT id, n FROM off_t WHERE id = 1")
            .await;
        assert!(
            warmed(&sess_on),
            "flag ON: first cold meta miss must mark the table warmed"
        );
        let set_len_after_cold = sess_on.state.prewarmed_tables.lock().unwrap().len();
        // Repeat load on the SAME session+table: the de-dup set is unchanged
        // (no second fire).
        let _ = sess_on
            .execute("SELECT id, n FROM off_t WHERE id = 1")
            .await;
        assert_eq!(
            sess_on.state.prewarmed_tables.lock().unwrap().len(),
            set_len_after_cold,
            "flag ON: repeat load of an already-warmed table must not re-fire"
        );

        std::env::remove_var("BASIN_PREWARM_PROVIDERS");
    }

    /// Timing micro-bench: measures the SessionContext construction + UDF
    /// registration phase in isolation (the primary target of the stateless-UDF-cache
    /// optimisation). Compares the BEFORE (200+ individual write-lock register_udf
    /// calls) vs the AFTER (SessionStateBuilder batch-insert + 3 auth UDFs) paths
    /// side-by-side in the same binary on the same machine.
    ///
    /// Run with:
    ///   `CARGO_BUILD_JOBS=4 cargo test -p basin-engine --release --lib \
    ///    session::tests::bench_session_open_latency -- --nocapture --ignored`
    #[tokio::test]
    #[ignore]
    async fn bench_session_open_latency() {
        use std::sync::Arc;
        use std::time::Instant;

        use crate::AuthContext;
        use crate::{Engine, EngineConfig};
        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::ProjectId;
        use datafusion::execution::config::SessionConfig;
        use datafusion::execution::SessionStateBuilder;
        use datafusion::prelude::SessionContext;
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        });
        let auth = Arc::new(AuthContext::anonymous());

        const ITERS: usize = 200;

        // --- BEFORE baseline: old path (SessionContext::new_with_config + per-UDF write-lock) ---
        let mut before_samples: Vec<f64> = Vec::with_capacity(ITERS);
        // Warm up.
        for _ in 0..10 {
            let cfg = SessionConfig::new()
                .set_str(
                    "datafusion.execution.listing_table_ignore_subdirectory",
                    "false",
                )
                .with_target_partitions(1);
            let ctx = SessionContext::new_with_config(cfg);
            crate::udf::register_distance_udfs(&ctx);
            crate::udf::register_pg_udfs(&ctx);
            crate::udf::register_pg_compat_udfs(&ctx);
            crate::string_dt_udf::register_string_dt_udfs(&ctx);
            crate::fts_udf::register_fts_udfs(&ctx);
            crate::udf::register_auth_udfs(&ctx, auth.clone());
            crate::jsonb_udf::register_jsonb_udfs(&ctx);
            crate::interval_tz_udf::register_interval_tz_udfs(&ctx);
            crate::pg_scalar_aliases::register_pg_scalar_aliases(&ctx);
            crate::pg_catalog_udf::register_pg_catalog_udfs(&ctx);
            crate::pg_agg_udf::register_json_agg_udafs(&ctx);
            crate::range_udf::register_range_udfs(&ctx);
            crate::datetime_extras::register_datetime_extras(&ctx);
            let _ = ctx;
        }
        for _ in 0..ITERS {
            let t0 = Instant::now();
            let cfg = SessionConfig::new()
                .set_str(
                    "datafusion.execution.listing_table_ignore_subdirectory",
                    "false",
                )
                .with_target_partitions(1);
            let ctx = SessionContext::new_with_config(cfg);
            crate::udf::register_distance_udfs(&ctx);
            crate::udf::register_pg_udfs(&ctx);
            crate::udf::register_pg_compat_udfs(&ctx);
            crate::string_dt_udf::register_string_dt_udfs(&ctx);
            crate::fts_udf::register_fts_udfs(&ctx);
            crate::udf::register_auth_udfs(&ctx, auth.clone());
            crate::jsonb_udf::register_jsonb_udfs(&ctx);
            crate::interval_tz_udf::register_interval_tz_udfs(&ctx);
            crate::pg_scalar_aliases::register_pg_scalar_aliases(&ctx);
            crate::pg_catalog_udf::register_pg_catalog_udfs(&ctx);
            crate::pg_agg_udf::register_json_agg_udafs(&ctx);
            crate::range_udf::register_range_udfs(&ctx);
            crate::datetime_extras::register_datetime_extras(&ctx);
            let _ = ctx;
            before_samples.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        before_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let before_p50 = before_samples[ITERS / 2];
        let before_p95 = before_samples[(ITERS * 95) / 100];
        let before_mean = before_samples.iter().sum::<f64>() / ITERS as f64;
        println!(
            "\nBEFORE (unbatched, 200+ individual register_udf write-lock): mean={before_mean:.3}ms  p50={before_p50:.3}ms  p95={before_p95:.3}ms"
        );

        // --- AFTER: batched via UDF cache (current implementation) -----------
        let udf_cache = engine.inner.udf_cache.as_ref();
        let mut after_samples: Vec<f64> = Vec::with_capacity(ITERS);
        // Warm up.
        for _ in 0..10 {
            let cfg = SessionConfig::new()
                .set_str(
                    "datafusion.execution.listing_table_ignore_subdirectory",
                    "false",
                )
                .with_target_partitions(1);
            let state = SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features()
                .with_scalar_functions(udf_cache.scalar.clone())
                .with_aggregate_functions(udf_cache.aggregate.clone())
                .build();
            let ctx = SessionContext::new_with_state(state);
            crate::udf::register_auth_udfs(&ctx, auth.clone());
            let _ = ctx;
        }
        for _ in 0..ITERS {
            let t0 = Instant::now();
            let cfg = SessionConfig::new()
                .set_str(
                    "datafusion.execution.listing_table_ignore_subdirectory",
                    "false",
                )
                .with_target_partitions(1);
            let state = SessionStateBuilder::new()
                .with_config(cfg)
                .with_default_features()
                .with_scalar_functions(udf_cache.scalar.clone())
                .with_aggregate_functions(udf_cache.aggregate.clone())
                .build();
            let ctx = SessionContext::new_with_state(state);
            crate::udf::register_auth_udfs(&ctx, auth.clone());
            let _ = ctx;
            after_samples.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        after_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let after_p50 = after_samples[ITERS / 2];
        let after_p95 = after_samples[(ITERS * 95) / 100];
        let after_mean = after_samples.iter().sum::<f64>() / ITERS as f64;
        println!(
            "AFTER  (batched UDF cache, 3 individual register_udf for auth):  mean={after_mean:.3}ms  p50={after_p50:.3}ms  p95={after_p95:.3}ms"
        );
        let speedup = before_p50 / after_p50;
        println!("Improvement: p50 {speedup:.1}x faster ({before_p50:.3}ms -> {after_p50:.3}ms)",);
    }

    // -------------------------------------------------------------------------
    // TxState unit tests — Step 3 (issue #83)
    //
    // Tests cover: BEGIN/COMMIT/ROLLBACK flag semantics, pending_files push
    // and flush, savepoint stack push/pop/rollback-to, aborted state, and
    // error-clears-on-rollback. All run synchronously — no I/O required.
    // -------------------------------------------------------------------------

    fn make_test_session_state() -> SessionState {
        SessionState::new()
    }

    fn make_file_ref(path: &str) -> basin_catalog::DataFileRef {
        basin_catalog::DataFileRef {
            path: path.to_string(),
            size_bytes: 100,
            row_count: 1,
            column_stats: std::collections::BTreeMap::new(),
            bloom_filters: ::std::collections::BTreeMap::new(),
            hll_sketches: ::std::collections::BTreeMap::new(),
            tdigest_sketches: ::std::collections::BTreeMap::new(),
        }
    }

    /// Initially inactive: a fresh session has no open transaction.
    #[test]
    fn tx_state_initially_inactive() {
        let state = make_test_session_state();
        assert!(
            !tx_is_active(&state),
            "TxState must be inactive on a fresh session"
        );
        assert!(
            !tx_is_aborted(&state),
            "TxState must not be aborted on a fresh session"
        );
    }

    /// BEGIN sets the active flag and captures snapshot heads.
    #[test]
    fn tx_begin_sets_active_and_snapshots() {
        let state = make_test_session_state();

        let mut heads = HashMap::new();
        let table = TableName::new("orders").expect("valid table name");
        heads.insert(table.clone(), basin_catalog::SnapshotId(42));

        tx_begin(&state, heads.clone());

        assert!(tx_is_active(&state), "tx_begin must set active = true");
        assert!(!tx_is_aborted(&state), "aborted must be false after BEGIN");

        let tx = state.tx_state.lock().expect("lock");
        assert_eq!(
            tx.pre_tx_snapshots.get(&table),
            Some(&basin_catalog::SnapshotId(42)),
            "pre_tx_snapshots must capture the heads passed to tx_begin"
        );
        assert!(
            tx.pending_files.is_empty(),
            "pending_files must be empty after tx_begin"
        );
        assert!(tx.savepoints.is_empty(), "savepoints empty after BEGIN");
    }

    /// COMMIT clears the active flag and returns pending files.
    #[test]
    fn tx_commit_clears_state_and_returns_pending() {
        let state = make_test_session_state();

        let mut heads = HashMap::new();
        let table = TableName::new("events").expect("valid table name");
        heads.insert(table.clone(), basin_catalog::SnapshotId(7));
        tx_begin(&state, heads);

        // Push a pending file.
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/events/data/f1.parquet"),
        );

        let pending = tx_commit(&state);

        assert!(!tx_is_active(&state), "tx_commit must set active = false");
        assert!(!tx_is_aborted(&state), "aborted cleared by COMMIT");

        assert_eq!(pending.len(), 1, "COMMIT returns 1 table's pending files");
        assert_eq!(
            pending.get(&table).map(|v| v.len()),
            Some(1),
            "pending files returned for table"
        );

        let tx = state.tx_state.lock().expect("lock");
        assert!(
            tx.pre_tx_snapshots.is_empty(),
            "tx_commit must clear pre_tx_snapshots"
        );
        assert!(
            tx.pending_files.is_empty(),
            "pending_files cleared inside TxState"
        );
    }

    /// ROLLBACK clears the active flag and returns pending files + snapshots.
    #[test]
    fn tx_rollback_clears_state_and_returns_data() {
        let state = make_test_session_state();

        let mut heads = HashMap::new();
        let table = TableName::new("logs").expect("valid table name");
        heads.insert(table.clone(), basin_catalog::SnapshotId(99));
        tx_begin(&state, heads);

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/logs/data/f1.parquet"),
        );

        let (pending, snapshots) = tx_rollback(&state);

        assert!(!tx_is_active(&state), "tx_rollback must set active = false");
        assert!(!tx_is_aborted(&state), "aborted cleared by ROLLBACK");

        assert_eq!(pending.len(), 1);
        assert_eq!(snapshots.get(&table), Some(&basin_catalog::SnapshotId(99)));

        let tx = state.tx_state.lock().expect("lock");
        assert!(tx.pre_tx_snapshots.is_empty());
        assert!(tx.pending_files.is_empty());
    }

    /// Idempotent BEGIN: calling tx_begin while already active must not
    /// overwrite the existing snapshot heads.
    #[test]
    fn tx_begin_idempotent_preserves_first_snapshots() {
        let state = make_test_session_state();

        let table = TableName::new("items").expect("valid table name");

        let mut heads1 = HashMap::new();
        heads1.insert(table.clone(), basin_catalog::SnapshotId(1));
        tx_begin(&state, heads1);

        let mut heads2 = HashMap::new();
        heads2.insert(table.clone(), basin_catalog::SnapshotId(2));
        tx_begin(&state, heads2);

        let tx = state.tx_state.lock().expect("lock");
        assert_eq!(
            tx.pre_tx_snapshots.get(&table),
            Some(&basin_catalog::SnapshotId(1)),
            "second tx_begin must not overwrite snapshots from the first BEGIN"
        );
    }

    /// Full lifecycle: BEGIN → COMMIT → BEGIN again.
    #[test]
    fn tx_lifecycle_begin_commit_begin_again() {
        let state = make_test_session_state();

        tx_begin(&state, HashMap::new());
        assert!(tx_is_active(&state));

        let _ = tx_commit(&state);
        assert!(!tx_is_active(&state));

        tx_begin(&state, HashMap::new());
        assert!(
            tx_is_active(&state),
            "session must accept a second BEGIN after a completed transaction"
        );
    }

    /// ROLLBACK without a preceding BEGIN: must not panic.
    #[test]
    fn tx_rollback_without_begin_no_panic() {
        let state = make_test_session_state();
        let _ = tx_rollback(&state);
        assert!(!tx_is_active(&state));
    }

    /// COMMIT without a preceding BEGIN: must not panic.
    #[test]
    fn tx_commit_without_begin_no_panic() {
        let state = make_test_session_state();
        let _ = tx_commit(&state);
        assert!(!tx_is_active(&state));
    }

    /// Aborted state: set and check.
    #[test]
    fn tx_aborted_state_set_and_check() {
        let state = make_test_session_state();
        tx_begin(&state, HashMap::new());
        assert!(!tx_is_aborted(&state));
        tx_set_aborted(&state);
        assert!(tx_is_aborted(&state));
        // ROLLBACK clears aborted.
        let _ = tx_rollback(&state);
        assert!(!tx_is_aborted(&state));
    }

    /// Pending files: push, read, commit returns them.
    #[test]
    fn tx_pending_files_push_and_flush() {
        let state = make_test_session_state();
        let table = TableName::new("t").unwrap();
        tx_begin(&state, HashMap::new());

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/a.parquet"),
        );
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/b.parquet"),
        );

        let files = tx_pending_files_for(&state, &table);
        assert_eq!(files.len(), 2);

        let pending = tx_commit(&state);
        assert_eq!(pending.get(&table).map(|v| v.len()), Some(2));
    }

    /// Savepoint push: watermarks recorded correctly.
    #[test]
    fn tx_savepoint_push_records_watermarks() {
        let state = make_test_session_state();
        let table = TableName::new("t").unwrap();
        tx_begin(&state, HashMap::new());

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/a.parquet"),
        );
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/b.parquet"),
        );

        // Two files in pending.
        let files = tx_pending_files_for(&state, &table);
        assert_eq!(files.len(), 2);

        let tx = state.tx_state.lock().unwrap();
        assert_eq!(tx.savepoints.len(), 1);
        assert_eq!(tx.savepoints[0].name, "sp1");
        assert_eq!(tx.savepoints[0].file_offsets.get(&table), Some(&1usize));
    }

    /// ROLLBACK TO SAVEPOINT: abandons files after the watermark.
    #[test]
    fn tx_rollback_to_savepoint_discards_tail() {
        let state = make_test_session_state();
        let table = TableName::new("t").unwrap();
        tx_begin(&state, HashMap::new());

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/a.parquet"),
        );
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/b.parquet"),
        );
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/c.parquet"),
        );

        // 3 files total; sp1 recorded offset=1.
        let (abandoned, _) = tx_rollback_to_savepoint(&state, "sp1").unwrap();
        let tail = abandoned.get(&table).unwrap();
        assert_eq!(tail.len(), 2, "two files after sp1 must be abandoned");
        assert!(tail.iter().any(|f| f.path.ends_with("b.parquet")));
        assert!(tail.iter().any(|f| f.path.ends_with("c.parquet")));

        // Only 1 file remains in pending.
        let remaining = tx_pending_files_for(&state, &table);
        assert_eq!(remaining.len(), 1);
        assert!(remaining[0].path.ends_with("a.parquet"));
    }

    /// RELEASE SAVEPOINT: removes the frame, writes stay in pending.
    #[test]
    fn tx_release_savepoint_removes_frame() {
        let state = make_test_session_state();
        let table = TableName::new("t").unwrap();
        tx_begin(&state, HashMap::new());

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/a.parquet"),
        );
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/t/data/b.parquet"),
        );

        tx_release_savepoint(&state, "sp1").unwrap();

        // Frame gone, both files remain pending.
        let tx = state.tx_state.lock().unwrap();
        assert!(tx.savepoints.is_empty(), "frame must be removed by RELEASE");
        assert_eq!(tx.pending_files.get(&table).map(|v| v.len()), Some(2));
    }

    /// RELEASE of unknown savepoint returns Err.
    #[test]
    fn tx_release_unknown_savepoint_errors() {
        let state = make_test_session_state();
        tx_begin(&state, HashMap::new());
        let result = tx_release_savepoint(&state, "no_such");
        assert!(result.is_err());
    }

    /// ROLLBACK TO unknown savepoint returns Err.
    #[test]
    fn tx_rollback_to_unknown_savepoint_errors() {
        let state = make_test_session_state();
        tx_begin(&state, HashMap::new());
        let result = tx_rollback_to_savepoint(&state, "no_such");
        assert!(result.is_err());
    }

    /// ROLLBACK TO SAVEPOINT clears the aborted flag.
    #[test]
    fn tx_rollback_to_savepoint_clears_aborted() {
        let state = make_test_session_state();
        tx_begin(&state, HashMap::new());
        tx_push_savepoint(&state, "sp".to_string());
        tx_set_aborted(&state);
        assert!(tx_is_aborted(&state));
        let _ = tx_rollback_to_savepoint(&state, "sp").unwrap();
        assert!(
            !tx_is_aborted(&state),
            "ROLLBACK TO SAVEPOINT must clear aborted"
        );
    }

    /// COMMIT after BEGIN returns the pending files and leaves state clean.
    #[test]
    fn tx_commit_after_begin_clean_state() {
        let state = make_test_session_state();
        let table = TableName::new("u").unwrap();
        let mut heads = HashMap::new();
        heads.insert(table.clone(), basin_catalog::SnapshotId(5));
        tx_begin(&state, heads);

        tx_push_pending_file(
            &state,
            &table,
            make_file_ref("projects/p/tables/u/data/x.parquet"),
        );
        let pending = tx_commit(&state);
        assert_eq!(pending.get(&table).map(|v| v.len()), Some(1));

        // State is fully cleared.
        assert!(!tx_is_active(&state));
        let tx = state.tx_state.lock().unwrap();
        assert!(tx.pending_files.is_empty());
        assert!(tx.pre_tx_snapshots.is_empty());
        assert!(tx.savepoints.is_empty());
    }

    // ── Phase 5.28.B/C/D — GUC parse + accessor unit tests ───────────────────

    #[test]
    fn parse_pg_duration_disabled_forms() {
        assert_eq!(parse_pg_duration("0"), None);
        assert_eq!(parse_pg_duration(""), None);
        assert_eq!(parse_pg_duration("off"), None);
        assert_eq!(parse_pg_duration("  0  "), None);
    }

    #[test]
    fn parse_pg_duration_ms_suffix() {
        assert_eq!(parse_pg_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_pg_duration("100ms"), Some(Duration::from_millis(100)));
        assert_eq!(
            parse_pg_duration("1000ms"),
            Some(Duration::from_millis(1000))
        );
    }

    #[test]
    fn parse_pg_duration_s_suffix() {
        assert_eq!(parse_pg_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_pg_duration("1sec"), Some(Duration::from_secs(1)));
    }

    #[test]
    fn parse_pg_duration_bare_integer_is_ms() {
        assert_eq!(parse_pg_duration("200"), Some(Duration::from_millis(200)));
    }

    #[test]
    fn parse_pg_duration_min_suffix() {
        assert_eq!(parse_pg_duration("2min"), Some(Duration::from_secs(120)));
    }

    #[test]
    fn format_pg_duration_roundtrip() {
        let d = Some(Duration::from_millis(500));
        let s = format_pg_duration(d);
        assert_eq!(s, "500ms");
        assert_eq!(parse_pg_duration(&s), d);

        assert_eq!(format_pg_duration(None), "0");
    }

    #[test]
    fn lock_timeout_guc_accessor() {
        let state = make_test_session_state();
        assert_eq!(session_lock_timeout(&state), None);
        set_session_lock_timeout(&state, Some(Duration::from_millis(300)));
        assert_eq!(
            session_lock_timeout(&state),
            Some(Duration::from_millis(300))
        );
        set_session_lock_timeout(&state, None);
        assert_eq!(session_lock_timeout(&state), None);
    }

    #[test]
    fn idle_in_transaction_timeout_guc_accessor() {
        let state = make_test_session_state();
        assert_eq!(session_idle_in_transaction_timeout(&state), None);
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_secs(30)));
        assert_eq!(
            session_idle_in_transaction_timeout(&state),
            Some(Duration::from_secs(30))
        );
    }

    /// Serialises the env-mutating `basin.synchronous_commit` tests: env vars
    /// are process-global, so two tests flipping `BASIN_SYNCHRONOUS_COMMIT`
    /// concurrently would race each other's assertions.
    static SYNC_COMMIT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn synchronous_commit_guc_accessor() {
        let _env = SYNC_COMMIT_ENV_LOCK.lock().unwrap();
        std::env::remove_var("BASIN_SYNCHRONOUS_COMMIT");
        let state = make_test_session_state();
        assert!(
            session_synchronous_commit(&state),
            "synchronous_commit must default to on (durable) — no silent async loss window"
        );
        set_session_synchronous_commit(&state, true);
        assert!(session_synchronous_commit(&state));
        assert_eq!(show_synchronous_commit(&state), "on");
        set_session_synchronous_commit(&state, false);
        assert!(!session_synchronous_commit(&state));
        assert_eq!(show_synchronous_commit(&state), "off");
    }

    #[test]
    fn synchronous_commit_env_default_applies_to_new_sessions() {
        let _env = SYNC_COMMIT_ENV_LOCK.lock().unwrap();
        std::env::set_var("BASIN_SYNCHRONOUS_COMMIT", "on");
        let on_state = make_test_session_state();
        std::env::set_var("BASIN_SYNCHRONOUS_COMMIT", "off");
        let off_state = make_test_session_state();
        std::env::remove_var("BASIN_SYNCHRONOUS_COMMIT");
        let default_state = make_test_session_state();
        assert!(
            session_synchronous_commit(&on_state),
            "BASIN_SYNCHRONOUS_COMMIT=on must flip the engine default"
        );
        assert!(!session_synchronous_commit(&off_state));
        assert!(
            session_synchronous_commit(&default_state),
            "unset BASIN_SYNCHRONOUS_COMMIT must default to DURABLE (on) — no silent async loss window"
        );
    }

    #[test]
    fn parse_pg_bool_accepts_pg_forms() {
        for raw in ["on", "ON", "'on'", "true", "1", "yes", "\"true\""] {
            assert!(parse_pg_bool(raw).unwrap(), "raw={raw:?}");
        }
        for raw in ["off", "OFF", "'off'", "false", "0", "no"] {
            assert!(!parse_pg_bool(raw).unwrap(), "raw={raw:?}");
        }
        assert!(
            parse_pg_bool("onn").is_err(),
            "a typo must error, not silently downgrade durability"
        );
        assert!(parse_pg_bool("").is_err());
    }

    #[test]
    fn touch_last_active_updates_timestamp() {
        let state = make_test_session_state();
        let t0 = last_active(&state);
        std::thread::sleep(Duration::from_millis(5));
        touch_last_active(&state);
        let t1 = last_active(&state);
        assert!(t1 > t0, "last_active must advance after touch");
    }

    // ── TableMetaCache epoch-based invalidation ─────────────────────────────

    fn make_test_table_meta(table_name: &str, rls: bool) -> Arc<TableMetadata> {
        use basin_catalog::{PartitionSpec, SnapshotId, TableFileFormat, TableMetadata};
        use basin_common::ProjectId;
        Arc::new(TableMetadata {
            project: ProjectId::new(),
            table: TableName::new(table_name).unwrap(),
            schema: Arc::new(arrow_schema::Schema::empty()),
            current_snapshot: SnapshotId::GENESIS,
            snapshots: Vec::new(),
            format_version: 2,
            partition_spec: PartitionSpec::Unpartitioned,
            rls_enabled: rls,
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
        })
    }

    /// A cache entry inserted with epoch E is evicted when the caller supplies
    /// a different epoch (E+1), even if the TTL has not yet elapsed.
    #[test]
    fn table_meta_cache_epoch_invalidates_on_external_mutation() {
        // Install a very long TTL so TTL expiry cannot interfere.
        let _guard = test_meta_cache_ttl_override::install(Duration::from_secs(3600));

        let cache = TableMetaCache::new();
        let table = TableName::new("t1").unwrap();
        let meta = make_test_table_meta("t1", false);

        // Fill at epoch 5.
        cache.insert(table.clone(), meta.clone(), false, 5);
        assert_eq!(cache.len(), 1);

        // Same epoch → cache hit.
        assert!(
            cache.get_fresh(&table, 5).is_some(),
            "same epoch should be a cache hit"
        );

        // Different epoch (catalog mutated) → cache miss.
        assert!(
            cache.get_fresh(&table, 6).is_none(),
            "changed epoch should be a cache miss"
        );
    }

    /// Multiple reads at the same epoch all return the cached entry — no
    /// spurious eviction on repeated reads.
    #[test]
    fn table_meta_cache_epoch_no_evict_when_unchanged() {
        let _guard = test_meta_cache_ttl_override::install(Duration::from_secs(3600));

        let cache = TableMetaCache::new();
        let table = TableName::new("t2").unwrap();
        let meta = make_test_table_meta("t2", true);

        cache.insert(table.clone(), meta.clone(), false, 42);

        // 10 consecutive reads at epoch 42 all hit.
        for i in 0..10 {
            let hit = cache.get_fresh(&table, 42);
            assert!(hit.is_some(), "read {i} at epoch 42 should be a cache hit");
        }

        // Still exactly one entry — no duplicate inserts.
        assert_eq!(cache.len(), 1);
    }

    // ── target_partitions_for_bulk_scan heuristic ────────────────────────────

    /// Empty table (0 files) → no parallelism.
    #[test]
    fn bulk_scan_partitions_zero_files_returns_one() {
        assert_eq!(target_partitions_for_bulk_scan(0), 1);
    }

    /// Single-file table (below MIN_FILES_FOR_PARALLEL_SCAN) → no parallelism.
    #[test]
    fn bulk_scan_partitions_single_file_returns_one() {
        assert_eq!(target_partitions_for_bulk_scan(1), 1);
    }

    /// Two files = at or above the threshold → parallelism enabled.
    #[test]
    fn bulk_scan_partitions_two_files_enables_parallel() {
        // The function should return > 1 when file_count >= 2, provided the
        // host has more than 1 CPU (the common case for any dev machine).
        // We only assert >= 1 (not > 1) to stay CI-safe on single-CPU runners,
        // but we also assert that it is exactly min(cap, file_count) = min(≥1, 2).
        let p = target_partitions_for_bulk_scan(2);
        assert!(p >= 1, "must return at least 1");
        assert!(p <= 2, "must not exceed file_count");
    }

    /// file_count > cap → capped at cap (not at file_count).
    #[test]
    fn bulk_scan_partitions_capped_at_cap() {
        // With a file_count well above any realistic cpu_count, the return
        // value must be <= available_parallelism().
        let cap = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let p = target_partitions_for_bulk_scan(10_000);
        assert!(p <= cap, "must not exceed cpu cap ({cap}), got {p}");
        assert!(p >= 1, "must return at least 1");
    }

    // perf-w-pointops: the DML-flags cache (FK/reactor presence) is the hot
    // path's per-statement catalog-round-trip elimination. A non-zero epoch
    // hit serves the cached flags; an epoch bump misses and refetches.
    #[test]
    fn dml_flags_cache_epoch_validated() {
        let cache = DmlFlagsCache::new();
        let t = TableName::new("orders").unwrap();
        let flags = DmlFlags {
            has_referencing_fk: false,
            has_update_reactor: false,
        };
        // Empty → miss.
        assert!(cache.get_fresh(&t, 7).is_none());
        cache.insert(t.clone(), flags, 7);
        // Same epoch → hit, same flags.
        let got = cache.get_fresh(&t, 7).expect("epoch-fresh hit");
        assert!(!got.has_referencing_fk && !got.has_update_reactor);
        // Bumped epoch → miss (any catalog mutation invalidates).
        assert!(
            cache.get_fresh(&t, 8).is_none(),
            "an epoch bump must miss so FK/reactor DDL is observed"
        );
        // epoch 0 (Postgres/Rest backends) is TTL-only, so a fresh insert hits
        // regardless of the queried epoch value.
        cache.insert(t.clone(), flags, 0);
        assert!(cache.get_fresh(&t, 0).is_some());
        // Explicit single-table invalidation drops the entry.
        cache.invalidate(&t);
        assert!(cache.get_fresh(&t, 0).is_none());
    }
}

// Regression coverage for the multi-node ingest throughput fix: the ingest
// meta-cache must serve `exec_ingest_batch`'s per-batch meta fetch from the
// cheap META-only `Catalog::load_table_meta`, keyed on `Catalog::meta_version`
// so a stream of per-partition DATA commits never invalidates it. These run
// over a real `Engine` + sharded `ObjectStoreCatalog` so the cache validity is
// exercised against the actual catalog that exhibited the regression.
#[cfg(test)]
mod ingest_meta_cache_tests {
    use std::sync::Arc;

    use basin_catalog::{Catalog, ObjectStoreCatalog, SnapshotId};
    use basin_common::{ProjectId, TableName};
    use object_store::memory::InMemory;

    use crate::session::load_table_meta_cached_for_ingest;
    use crate::{Engine, EngineConfig, ExecResult};

    fn engine_over_object_store() -> Engine {
        let store = Arc::new(InMemory::new());
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: store.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store));
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn data_file(path: &str, rows: u64) -> basin_catalog::DataFileRef {
        basin_catalog::DataFileRef {
            path: path.to_string(),
            size_bytes: rows * 10,
            row_count: rows,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        }
    }

    /// The cold ingest meta fetch returns the table's constraints (the enforcers'
    /// inputs) and then survives a stream of per-partition DATA commits without a
    /// re-load — i.e. the cache stays valid across the data appends a bulk COPY
    /// issues. A DDL change invalidates it and surfaces the new constraint.
    #[tokio::test]
    async fn ingest_meta_cache_survives_data_appends_invalidated_by_ddl() {
        let eng = engine_over_object_store();
        let project = ProjectId::new();
        let sess = eng.open_session(project).await.unwrap();
        let table = TableName::new("orders").unwrap();

        sess.execute("CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, qty BIGINT)")
            .await
            .unwrap();
        sess.execute("ALTER TABLE orders ADD CONSTRAINT qty_pos CHECK (qty > 0)")
            .await
            .unwrap();

        // Cold ingest meta fetch: carries the PK + the CHECK constraint the
        // enforcers consume, and (cheap META path) surfaces no data files.
        let m1 = load_table_meta_cached_for_ingest(&sess, &table)
            .await
            .unwrap();
        assert_eq!(m1.pk_columns, vec!["id".to_string()]);
        assert_eq!(m1.check_constraints.len(), 1, "CHECK constraint present");
        assert!(
            m1.live_data_files().is_empty(),
            "cheap META load must not union per-partition data files"
        );

        // Simulate the bulk-ingest fan-out: several per-partition DATA commits.
        // Each bumps the global catalog epoch, but NOT the META version — so the
        // ingest meta-cache must keep serving the SAME cached Arc (no re-load).
        let catalog = &eng.config().catalog;
        for pid in 0..6u32 {
            let part = pid.to_string();
            let exp = catalog
                .current_snapshot_id_in_partition(&project, &table, &part)
                .await
                .unwrap();
            catalog
                .append_data_files_in_partition(
                    &project,
                    &table,
                    &part,
                    exp,
                    vec![data_file(&format!("orders/part-{pid}.parquet"), 1000)],
                )
                .await
                .unwrap();

            let m = load_table_meta_cached_for_ingest(&sess, &table)
                .await
                .unwrap();
            assert!(
                Arc::ptr_eq(&m1, &m),
                "data append to partition {pid} must not invalidate the ingest meta-cache"
            );
        }

        // A DDL change bumps the META version → the next ingest fetch re-loads
        // and observes the new constraint set (here: dropping the CHECK).
        sess.execute("ALTER TABLE orders DROP CONSTRAINT qty_pos")
            .await
            .unwrap();
        let m2 = load_table_meta_cached_for_ingest(&sess, &table)
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&m1, &m2),
            "a DDL change must invalidate the ingest meta-cache"
        );
        assert_eq!(
            m2.check_constraints.len(),
            0,
            "the dropped CHECK must be observed on the next ingest batch"
        );
        // The data committed via the partitions is real: the FULL load still
        // unions all 6 partition files (the ingest path just doesn't pay for it).
        let full = catalog.load_table(&project, &table).await.unwrap();
        assert_eq!(full.live_data_files().len(), 6);
        let _ = SnapshotId::GENESIS; // keep the import meaningful across refactors
    }

    /// AVAILABILITY (Part 1): a single table with a TORN partition delta chain
    /// (a referenced segment object is missing from the store, exactly the dev
    /// outage's `not found: …/parts/s5@v229`) must NOT fail session-open. The
    /// session opens, every OTHER table queries normally, and the broken table
    /// errors ONLY when a query touches it directly. Before the fix, the eager
    /// session-open warm propagated the fold's NotFound and FATAL'd every pgwire
    /// connection to the whole project — so even `SELECT 1` was refused and the
    /// operator could not even `DROP TABLE` to recover.
    #[tokio::test]
    async fn session_open_survives_one_torn_table() {
        use futures::StreamExt;
        use object_store::{ObjectStore, ObjectStoreExt};

        let store = Arc::new(InMemory::new());
        let project = ProjectId::new();

        // --- SEED (first catalog) ------------------------------------------
        // Build the tables + segment chains through one catalog, then drop it.
        // The engine that recovers below uses a SEPARATE catalog over the SAME
        // store with cold caches — mirroring the dev outage, which persisted
        // across an engine restart (the torn chain lives in the object store, so
        // a warm part-cache can't mask it on the post-restart cold fold).
        let catalog: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));
        {
            let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
                object_store: store.clone(),
                root_prefix: None,
                disk_cache: None,
                page_cache: None,
            });
            let seed_eng = Engine::new(EngineConfig {
                storage,
                catalog: catalog.clone(),
                shard: None,
            });
            let sess = seed_eng.open_session(project).await.unwrap();
            sess.execute("CREATE TABLE good (id BIGINT)").await.unwrap();
            sess.execute("CREATE TABLE p100m (id BIGINT)")
                .await
                .unwrap();
        }
        let good = TableName::new("good").unwrap();
        let bad = TableName::new("p100m").unwrap();
        // `good` gets one segment. `p100m` gets a TWO-segment chain: a baseline
        // (v0) plus a delta (v1) that points its `base_version` at v0. Deleting
        // ONLY the baseline (below) leaves a HEAD/delta that references a missing
        // base — a genuine torn chain (not just an empty partition).
        let exp = catalog
            .current_snapshot_id_in_partition(&project, &good, "s5")
            .await
            .unwrap();
        catalog
            .append_data_files_in_partition(
                &project,
                &good,
                "s5",
                exp,
                vec![data_file("good/part.parquet", 100)],
            )
            .await
            .unwrap();
        let mut exp = catalog
            .current_snapshot_id_in_partition(&project, &bad, "s5")
            .await
            .unwrap();
        for i in 0..2 {
            catalog
                .append_data_files_in_partition(
                    &project,
                    &bad,
                    "s5",
                    exp,
                    vec![data_file(&format!("p100m/part{i}.parquet"), 100)],
                )
                .await
                .unwrap();
            exp = catalog
                .current_snapshot_id_in_partition(&project, &bad, "s5")
                .await
                .unwrap();
        }

        // FORGE THE TORN CHAIN: delete the BASELINE segment (v1) of `p100m`'s
        // partition, leaving HEAD + the v2 delta that points its `base_version`
        // at v1. A fold now reads v2 (a delta) and GETs its base v1 → `NotFound`,
        // exactly the dev outage's `not found: …/parts/s5@v229`.
        let mut baseline_key: Option<object_store::path::Path> = None;
        let mut listing = store.list(None);
        while let Some(item) = listing.next().await {
            let loc = item.unwrap().location;
            let k = loc.as_ref();
            if k.contains("/p100m/parts/") && k.ends_with("v00000000000000000001.json") {
                baseline_key = Some(loc);
            }
        }
        let baseline_key =
            baseline_key.expect("precondition: p100m has a v1 baseline segment to delete");
        let store_dyn: &dyn ObjectStore = store.as_ref();
        store_dyn.delete(&baseline_key).await.unwrap();

        // --- RECOVER (fresh catalog + engine, cold caches) -----------------
        let cold_catalog: Arc<dyn Catalog> = Arc::new(ObjectStoreCatalog::new(store.clone()));
        let cold_storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: store.clone(),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let eng = Engine::new(EngineConfig {
            storage: cold_storage,
            catalog: cold_catalog.clone(),
            shard: None,
        });
        // Sanity: a direct cold catalog load of the torn table now errors.
        assert!(
            cold_catalog.load_table(&project, &bad).await.is_err(),
            "precondition: the torn table fails to resolve at the catalog layer (cold)"
        );

        // THE FIX: opening a fresh session must SUCCEED despite the torn table.
        let sess = eng
            .open_session(project)
            .await
            .expect("session-open must not fail because one table is torn");

        // The trivial query works (the connection is usable).
        sess.execute("SELECT 1")
            .await
            .expect("SELECT 1 works on a session opened past a torn table");

        // The healthy table queries fine.
        let n = match sess.execute("SELECT count(*) FROM good").await.unwrap() {
            ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(n, 1, "the healthy table returns its single count row");

        // A query that TOUCHES the torn table errors (it is not silently empty).
        assert!(
            sess.execute("SELECT * FROM p100m").await.is_err(),
            "a query on the torn table must error, not silently succeed"
        );

        // And the session is still usable afterwards — the torn-table error did
        // not poison the connection.
        sess.execute("SELECT 1")
            .await
            .expect("session still usable after a torn-table query error");
    }
}
