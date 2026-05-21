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
use std::sync::{Arc, OnceLock, RwLock};

use crate::AuthContext;

use basin_catalog::{DataFileRef, PartitionSpec, SnapshotId, TableFileFormat};
use basin_common::{BasinError, ProjectId, Result, TableName};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::logical_expr::{col, SortExpr};
use datafusion::datasource::MemTable;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::Mutex;
use tracing::instrument;
use url::Url;

use crate::convert::schema_ws_to_df;
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
    crate::datetime_extras::register_datetime_extras(&ctx);
    let state = ctx.state();
    StatelessUdfCache {
        scalar: state.scalar_functions().values().cloned().collect(),
        aggregate: state.aggregate_functions().values().cloned().collect(),
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
/// savepoint discards all files appended after the watermark.
#[derive(Debug)]
pub(crate) struct SavepointFrame {
    /// Name of the savepoint (case-sensitive, per PG).
    pub(crate) name: String,
    /// For each table touched *before* this savepoint, how many pending files
    /// existed at savepoint time. Files at or beyond this index are the ones
    /// that would be rolled back if we `ROLLBACK TO <name>`.
    pub(crate) file_offsets: HashMap<TableName, usize>,
}

impl SavepointFrame {
    /// Clone the `file_offsets` map for use during rollback-to-savepoint.
    fn clone_offsets(&self) -> HashMap<TableName, usize> {
        self.file_offsets.clone()
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
}

impl Drop for SessionState {
    fn drop(&mut self) {
        // Session end: release every advisory lock this session still holds
        // (session- and xact-scoped). Matches PG, which drops all advisory
        // locks held by a backend when the connection terminates.
        self.advisory.release_all_on_session_end();
    }
}

impl SessionState {
    fn new() -> Self {
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
        }
    }
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
        tx.pre_tx_snapshots = current_snapshots;
        tx.pending_files.clear();
        tx.savepoints.clear();
        tx.tx_id = None;
        tx.htap_rows.clear();
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
    tx.savepoints.clear();
    tx.tx_id = None;
    tx.htap_rows.clear();
    drop(tx);
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
    tx.active = false;
    tx.aborted = false;
    tx.savepoints.clear();
    // Discard tx-local HTAP buffers — they were never committed to the shared
    // MemTableRegistry so no cleanup is needed beyond dropping the batches.
    tx.htap_rows.clear();
    tx.tx_id = None;
    drop(tx);
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
    tx.savepoints.push(SavepointFrame {
        name,
        file_offsets: offsets,
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

/// Returns all tables that have pending files.
pub(crate) fn tx_touched_tables(state: &SessionState) -> Vec<TableName> {
    state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned")
        .pending_files
        .keys()
        .cloned()
        .collect()
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

#[instrument(skip(engine, current_user, auth_context), fields(project = %project))]
pub(crate) async fn open(
    engine: Engine,
    project: ProjectId,
    current_user: String,
    auth_context: Arc<AuthContext>,
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
    // into LeftMark NestedLoopJoin plans.
    let mut optimizer_rules = datafusion::optimizer::Optimizer::default().rules;
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::any_all_rewrite::AnyAllToScalarSubquery),
    );
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::union_scan_collapse::UnionScanCollapse),
    );
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::nullif_rewrite::NullifRewrite),
    );
    optimizer_rules.insert(
        0,
        std::sync::Arc::new(crate::is_distinct_rewrite::IsDistinctRewrite),
    );

    // Build a per-session RuntimeEnv that plugs in the process-wide file
    // metadata cache. Vortex/Parquet footer parses survive session recycling —
    // the dominant cost behind scale regressions at 100k rows / 50 files.
    // DefaultFilesMetadataCache validates entries via size + last_modified.
    let cache_cfg = CacheManagerConfig::default().with_file_metadata_cache(Some(
        engine.inner.file_metadata_cache.clone(),
    ));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_cfg)
        .build_arc()
        .map_err(|e| BasinError::internal(format!("RuntimeEnv build: {e}")))?;
    let state = SessionStateBuilder::new()
        .with_config(session_cfg)
        .with_runtime_env(runtime_env)
        // Non-UDF defaults: table factories, file formats, expr planners,
        // optimizer rules, window functions. We override scalar_functions and
        // aggregate_functions below with the combined (DF defaults + Basin)
        // cache, so `with_default_features` is not called for those.
        .with_default_features()
        // Inject our prepended optimizer rule list.
        .with_optimizer_rules(optimizer_rules)
        // Replace the default scalar/aggregate sets with the combined cache.
        // `with_scalar_functions` overwrites whatever `with_default_features`
        // set; since the cache includes DF's own defaults, nothing is lost.
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
    let store = engine.config().storage.project_object_store(&project);
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
    crate::info_schema_provider::register_info_schema_providers(
        &ctx,
        engine.config().catalog.clone(),
        project,
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

    let state = Arc::new(SessionState::new());

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
    let tables = engine.config().catalog.list_tables(&project).await?;
    for table in tables {
        refresh_table(&engine, &project, &ctx, &state, &table).await?;
    }

    Ok(ProjectSession {
        engine,
        project,
        current_user,
        auth_context,
        ctx,
        state,
    })
}

/// Build the DataFusion [`FileFormat`] + file extension for a table's
/// on-disk data format (#161/#162). Single format per table — opt-in Vortex,
/// Parquet remains the default and is byte-identical to the prior inline
/// expression so existing tables see zero regression.
fn listing_file_format(format: TableFileFormat) -> (Arc<dyn FileFormat>, &'static str) {
    match format {
        // KEEP byte-identical with the historical inline expression.
        TableFileFormat::Parquet => (Arc::new(ParquetFormat::default()), ".parquet"),
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
pub(crate) async fn refresh_table(
    engine: &Engine,
    project: &ProjectId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<()> {
    let meta = engine.config().catalog.load_table(project, table).await?;
    // The catalog hands us a workspace-version schema; convert into the
    // version DataFusion's `register_listing_table` expects.
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);

    // Drop any stale registration before re-registering. `deregister_table`
    // returns Ok(None) for the first-time path, which is exactly what we want.
    let _ = ctx.deregister_table(table.as_str());

    // Catalog-driven read path: enumerate exactly the files that are live at
    // `current_snapshot`. This is the fix for bug #41: a directory-URL
    // ListingTable would re-list the object store on every scan and return ALL
    // physical Parquet files, including those logically removed by a rollback
    // (GC is deferred by design). Using `live_data_files()` instead restricts
    // the scan to the canonical file set the catalog records for the current
    // snapshot, so post-rollback rows are never visible.
    let live_files: Vec<DataFileRef> = meta.live_data_files();

    if live_files.is_empty() {
        // Table has no data at this snapshot (genesis, TRUNCATE, or rolled back
        // to genesis). Register an empty in-memory table so queries return zero
        // rows with the correct schema rather than erroring.
        // MemTable requires at least one partition; supply an empty one.
        let provider = MemTable::try_new(df_schema, vec![vec![]])
            .map_err(|e| BasinError::internal(format!("MemTable empty {table}: {e}")))?;
        ctx.register_table(table.as_str(), Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;
    } else {
        // Build per-file listing URLs. Each `DataFileRef::path` is a full
        // bucket-relative key (no scheme, no leading slash) that already
        // includes any configured `root_prefix` — identical to the paths that
        // `Storage::list_data_files` emits and that `register_pruned_listing_table`
        // already handles the same way. Prepend the synthetic `basin://engine/`
        // scheme so DataFusion routes I/O through the registered ObjectStore.
        let (file_format, file_ext) = listing_file_format(meta.file_format);
        let mut listing_options =
            ListingOptions::new(file_format).with_file_extension(file_ext);
        if let Some(sort_cols) = meta.global_sort_order.as_deref() {
            listing_options =
                listing_options.with_file_sort_order(build_file_sort_order(sort_cols));
        }
        let mut urls: Vec<ListingTableUrl> = Vec::with_capacity(live_files.len());
        for f in &live_files {
            let mut s = String::from(BASIN_URL_BASE);
            s.push_str(&f.path);
            let url = ListingTableUrl::parse(&s)
                .map_err(|e| BasinError::internal(format!("listing url parse {s}: {e}")))?;
            urls.push(url);
        }
        let cfg = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(df_schema);
        let provider = ListingTable::try_new(cfg)
            .map_err(|e| BasinError::internal(format!("ListingTable::try_new {table}: {e}")))?;
        ctx.register_table(table.as_str(), Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;
    }

    // Cache the snapshot id for this session's INSERT path.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), meta.current_snapshot);

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

    let meta = engine.config().catalog.load_table(project, table).await?;
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);
    let _ = ctx.deregister_table(table.as_str());

    // Combine catalog live files + pending (in-tx) files.
    let mut all_files: Vec<DataFileRef> = meta.live_data_files();
    all_files.extend_from_slice(extra_files);

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
        .with_schema(df_schema);
    let provider = ListingTable::try_new(cfg)
        .map_err(|e| BasinError::internal(format!("ListingTable::try_new {table}: {e}")))?;
    ctx.register_table(table.as_str(), Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;

    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), meta.current_snapshot);

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

    let meta = engine.config().catalog.load_table(project, table).await?;
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);
    let _ = ctx.deregister_table(table.as_str());

    // Combine catalog live files + pending (in-tx) files.
    let mut all_files: Vec<DataFileRef> = meta.live_data_files();
    all_files.extend_from_slice(extra_files);

    // Build in-memory partition from the hot-tier batches.  DataFusion's
    // MemTable needs at least one partition even when empty.
    let htap_provider = MemTable::try_new(df_schema.clone(), vec![htap_batches])
        .map_err(|e| BasinError::internal(format!("MemTable for htap {table}: {e}")))?;

    if all_files.is_empty() {
        // Only in-memory rows, no Parquet files: use the MemTable alone.
        ctx.register_table(table.as_str(), Arc::new(htap_provider))
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
        let listing_provider = Arc::new(
            ListingTable::try_new(cfg)
                .map_err(|e| BasinError::internal(format!("ListingTable::try_new {table}: {e}")))?,
        );
        let union_provider =
            HtapUnionTable::new(listing_provider, Arc::new(htap_provider), df_schema);
        ctx.register_table(table.as_str(), Arc::new(union_provider))
            .map_err(|e| BasinError::internal(format!("register_table htap-union {table}: {e}")))?;
    }

    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), meta.current_snapshot);

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
}

impl std::fmt::Debug for HtapUnionTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HtapUnionTable").finish_non_exhaustive()
    }
}

impl HtapUnionTable {
    fn new(
        cold: Arc<dyn datafusion::catalog::TableProvider>,
        hot: Arc<dyn datafusion::catalog::TableProvider>,
        schema: Arc<arrow_schema::Schema>,
    ) -> Self {
        Self { cold, hot, schema }
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
        use datafusion::physical_plan::union::UnionExec;

        let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
        let hot_plan = self.hot.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(UnionExec::new(vec![cold_plan, hot_plan])))
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
        let _ = register_pruned_listing_table(
            engine,
            ctx,
            &table,
            &meta.schema,
            meta.file_format,
            &matching,
            meta.global_sort_order.as_deref(),
        )
        .await;
    }
    Ok(())
}

/// Re-register `table_name` as a `ListingTable` whose `table_paths` is a
/// per-file URL list of `paths`. This bypasses DataFusion's directory scan
/// entirely, so no footer GET is issued for files we already proved
/// irrelevant.
async fn register_pruned_listing_table(
    _engine: &Engine,
    ctx: &SessionContext,
    table: &TableName,
    schema: &arrow_schema::Schema,
    file_format: TableFileFormat,
    paths: &[String],
    global_sort_order: Option<&[String]>,
) -> Result<()> {
    let df_schema = Arc::new(schema_ws_to_df(schema)?);
    let (file_format, file_ext) = listing_file_format(file_format);
    let mut listing_options = ListingOptions::new(file_format).with_file_extension(file_ext);
    if let Some(sort_cols) = global_sort_order {
        listing_options =
            listing_options.with_file_sort_order(build_file_sort_order(sort_cols));
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

    let _ = ctx.deregister_table(table.as_str());
    ctx.register_table(table.as_str(), Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register_table pruned: {e}")))?;
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
fn collect_table_refs(query: &Query) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
}
