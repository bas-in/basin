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

use std::collections::HashMap;
use crate::pg_ast::ObjectNamePartExt;
use std::sync::{Arc, RwLock};

use crate::AuthContext;

use basin_catalog::{DataFileRef, PartitionSpec, SnapshotId};
use basin_common::{BasinError, Result, TableName, ProjectId};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::ast::ValueWithSpan;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::Mutex;
use tracing::instrument;
use url::Url;

use crate::convert::schema_ws_to_df;
use crate::{Engine, ProjectSession, StatelessUdfCache};

/// Synthetic URL we register the storage `ObjectStore` under. The scheme is
/// purely an internal protocol between `basin-engine` and DataFusion; it is
/// never exposed to clients.
pub(crate) const BASIN_URL_BASE: &str = "basin://engine/";

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
    crate::range_udf::register_range_udfs(&ctx);
    crate::jsonb_path_udf::register_jsonb_path_udfs(&ctx);
    crate::jsonb_modify_udf::register_jsonb_modify_udfs(&ctx);
    crate::json_build_udf::register_json_build_udfs(&ctx);
    crate::inet_udf::register_inet_udfs(&ctx);
    crate::datetime_more_udf::register_datetime_more_udfs(&ctx);
    crate::string_more_udf::register_string_more_udfs(&ctx);
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
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    // Idempotent: if already active, leave state unchanged (matches PG
    // WARNING: there is already a transaction in progress).
    if !tx.active {
        tx.active = true;
        tx.aborted = false;
        tx.pre_tx_snapshots = current_snapshots;
        tx.pending_files.clear();
        tx.savepoints.clear();
    }
}

/// Mark the end of an explicit transaction with `COMMIT`. Returns the
/// pending files map so the executor can flush them to the catalog.
/// Clears all txn state afterwards.
pub(crate) fn tx_commit(state: &SessionState) -> HashMap<TableName, Vec<DataFileRef>> {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    let pending = std::mem::take(&mut tx.pending_files);
    tx.active = false;
    tx.aborted = false;
    tx.pre_tx_snapshots.clear();
    tx.savepoints.clear();
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
) -> (HashMap<TableName, Vec<DataFileRef>>, HashMap<TableName, SnapshotId>) {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    let pending = std::mem::take(&mut tx.pending_files);
    let snapshots = std::mem::take(&mut tx.pre_tx_snapshots);
    tx.active = false;
    tx.aborted = false;
    tx.savepoints.clear();
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
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    tx.aborted = true;
}

/// Append a pending data file for `table` during an active transaction.
/// The file is visible only to this session until `COMMIT`.
pub(crate) fn tx_push_pending_file(state: &SessionState, table: &TableName, file: DataFileRef) {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    tx.pending_files
        .entry(table.clone())
        .or_default()
        .push(file);
}

/// Create a new savepoint with `name`, recording the current pending-file
/// watermark for every touched table.
pub(crate) fn tx_push_savepoint(state: &SessionState, name: String) {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    let offsets: HashMap<TableName, usize> = tx
        .pending_files
        .iter()
        .map(|(t, v)| (t.clone(), v.len()))
        .collect();
    tx.savepoints.push(SavepointFrame { name, file_offsets: offsets });
}

/// Release the named savepoint (PG: RELEASE SAVEPOINT <name>).
/// The writes that happened after it remain pending. Returns `Err` if
/// the savepoint name is not found.
pub(crate) fn tx_release_savepoint(state: &SessionState, name: &str) -> Result<()> {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    // Find the last frame with this name (PG allows name reuse; RELEASE
    // removes the most-recently-created one with that name).
    let pos = tx.savepoints.iter().rposition(|f| f.name == name);
    match pos {
        Some(i) => {
            tx.savepoints.remove(i);
            Ok(())
        }
        None => Err(BasinError::InvalidSchema(format!(
            "savepoint \"{name}\" does not exist"
        ))),
    }
}

/// Roll back to the named savepoint. Removes the named frame and all frames
/// created after it. Returns the abandoned pending files (tail beyond each
/// saved offset per table) so the executor can delete them from storage.
/// Also returns the pre-tx snapshot map (for restoring tables that now have
/// zero pending files after truncation, so reads can see the pre-tx state).
pub(crate) fn tx_rollback_to_savepoint(
    state: &SessionState,
    name: &str,
) -> Result<(HashMap<TableName, Vec<DataFileRef>>, HashMap<TableName, SnapshotId>)> {
    let mut tx = state
        .tx_state
        .lock()
        .expect("tx_state lock poisoned");
    // Find the savepoint frame.
    let pos = tx.savepoints.iter().rposition(|f| f.name == name);
    let pos = match pos {
        Some(i) => i,
        None => {
            return Err(BasinError::InvalidSchema(format!(
                "savepoint \"{name}\" does not exist"
            )))
        }
    };
    // Pop all frames at or after the target.
    let target_frame = tx.savepoints[pos].clone_offsets();
    tx.savepoints.truncate(pos);

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
pub(crate) fn tx_pending_files_for(
    state: &SessionState,
    table: &TableName,
) -> Vec<DataFileRef> {
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
    let state = SessionStateBuilder::new()
        .with_config(session_cfg)
        // Non-UDF defaults: table factories, file formats, expr planners,
        // optimizer rules, window functions. We override scalar_functions and
        // aggregate_functions below with the combined (DF defaults + Basin)
        // cache, so `with_default_features` is not called for those.
        .with_default_features()
        // Replace the default scalar/aggregate sets with the combined cache.
        // `with_scalar_functions` overwrites whatever `with_default_features`
        // set; since the cache includes DF's own defaults, nothing is lost.
        .with_scalar_functions(udf_cache.scalar.clone())
        .with_aggregate_functions(udf_cache.aggregate.clone())
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

    let state = Arc::new(SessionState::new());

    // Advisory-lock UDFs (BUG #138). Session-scoped: a lock owned by this
    // session must appear "held" to other sessions, so these capture the
    // per-session `Arc<AdvisorySessionLocks>` and overwrite (by name) the
    // removed stateless stubs. Registered here, like `register_auth_udfs`.
    crate::advisory_lock::register_advisory_lock_udfs(&ctx, state.advisory.clone());

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
        let listing_options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
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

    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
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
        let _ = register_pruned_listing_table(engine, ctx, &table, &meta.schema, &matching).await;
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
    paths: &[String],
) -> Result<()> {
    let df_schema = Arc::new(schema_ws_to_df(schema)?);
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

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
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::DoubleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::EscapedStringLiteral(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::NationalStringLiteral(s), .. }) => parse_timestamp_string_for_pruning(s),
        Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) => n.parse().ok(),
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

        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::ProjectId;
        use crate::{Engine, EngineConfig};
        use crate::AuthContext;
        use datafusion::prelude::SessionContext;
        use datafusion::execution::config::SessionConfig;
        use datafusion::execution::SessionStateBuilder;
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
        let engine = Engine::new(EngineConfig { storage, catalog, shard: None });
        let auth = Arc::new(AuthContext::anonymous());

        const ITERS: usize = 200;

        // --- BEFORE baseline: old path (SessionContext::new_with_config + per-UDF write-lock) ---
        let mut before_samples: Vec<f64> = Vec::with_capacity(ITERS);
        // Warm up.
        for _ in 0..10 {
            let cfg = SessionConfig::new()
                .set_str("datafusion.execution.listing_table_ignore_subdirectory", "false")
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
                .set_str("datafusion.execution.listing_table_ignore_subdirectory", "false")
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
                .set_str("datafusion.execution.listing_table_ignore_subdirectory", "false")
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
                .set_str("datafusion.execution.listing_table_ignore_subdirectory", "false")
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
        println!(
            "Improvement: p50 {speedup:.1}x faster ({before_p50:.3}ms -> {after_p50:.3}ms)",
        );
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
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/events/data/f1.parquet"));

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
        assert!(tx.pre_tx_snapshots.is_empty(), "tx_commit must clear pre_tx_snapshots");
        assert!(tx.pending_files.is_empty(), "pending_files cleared inside TxState");
    }

    /// ROLLBACK clears the active flag and returns pending files + snapshots.
    #[test]
    fn tx_rollback_clears_state_and_returns_data() {
        let state = make_test_session_state();

        let mut heads = HashMap::new();
        let table = TableName::new("logs").expect("valid table name");
        heads.insert(table.clone(), basin_catalog::SnapshotId(99));
        tx_begin(&state, heads);

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/logs/data/f1.parquet"));

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

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/a.parquet"));
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/b.parquet"));

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

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/a.parquet"));
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/b.parquet"));

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

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/a.parquet"));
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/b.parquet"));
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/c.parquet"));

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

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/a.parquet"));
        tx_push_savepoint(&state, "sp1".to_string());
        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/t/data/b.parquet"));

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
        assert!(!tx_is_aborted(&state), "ROLLBACK TO SAVEPOINT must clear aborted");
    }

    /// COMMIT after BEGIN returns the pending files and leaves state clean.
    #[test]
    fn tx_commit_after_begin_clean_state() {
        let state = make_test_session_state();
        let table = TableName::new("u").unwrap();
        let mut heads = HashMap::new();
        heads.insert(table.clone(), basin_catalog::SnapshotId(5));
        tx_begin(&state, heads);

        tx_push_pending_file(&state, &table, make_file_ref("projects/p/tables/u/data/x.parquet"));
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
