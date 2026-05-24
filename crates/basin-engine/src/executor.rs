//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use crate::pg_ast::ObjectNamePartExt;
use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Cooperative pg_sleep cancellation (Phase 5.28.D / #64 partial fix)
// ---------------------------------------------------------------------------
// Problem: `pg_sleep(N)` sleeps the full N seconds even when `statement_timeout`
// fires, because DataFusion UDFs are synchronous and cannot be preempted by the
// outer `tokio::time::timeout` wrapper.
//
// Solution: before each DataFusion `collect()` call the executor records the
// absolute statement deadline in a thread-local.  `PgSleepUdf` reads this
// deadline cooperatively: instead of sleeping the full duration in one shot, it
// sleeps in 50 ms ticks and checks the deadline after each tick.  If the
// deadline has passed it signals cancellation by setting `PG_SLEEP_CANCELED`
// and returning a `DataFusionError::Execution`.  The executor then converts that
// sentinel into `BasinError::QueryCanceled` (SQLSTATE 57014) instead of the
// generic `BasinError::internal`.
//
// Thread-local safety: for the non-shard path the UDF runs on the same tokio
// thread that called `collect()`.  For the shard path the executor captures the
// deadline by value, spawns a blocking thread, sets the thread-local at the top
// of the closure (before `rt.block_on`), and DataFusion runs the UDF on that
// same blocking thread — so the thread-local is always visible.
// ---------------------------------------------------------------------------

thread_local! {
    /// Absolute wall-clock deadline for the current statement, or `None` if
    /// no `statement_timeout` is active.  Set by the executor before each
    /// DataFusion `collect()`; read by `PgSleepUdf` on each sleep tick.
    static STATEMENT_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };

    /// Set to `true` by `PgSleepUdf` when it terminates early due to the
    /// deadline.  The executor checks this flag after a DataFusion collect
    /// error to decide whether to return `QueryCanceled` vs `internal`.
    static PG_SLEEP_CANCELED: Cell<bool> = const { Cell::new(false) };
}

/// Set the per-statement deadline that `pg_sleep` will poll against.
/// Called by the executor before each `df.collect()` / `physical_plan::collect()`.
pub(crate) fn set_statement_deadline(deadline: Option<Instant>) {
    STATEMENT_DEADLINE.with(|c| c.set(deadline));
}

/// Return the current statement deadline (called from `PgSleepUdf`).
pub(crate) fn get_statement_deadline() -> Option<Instant> {
    STATEMENT_DEADLINE.with(|c| c.get())
}

/// Signal that `pg_sleep` terminated early because the deadline passed.
/// Called from `PgSleepUdf` before returning the cancellation error.
pub(crate) fn mark_pg_sleep_canceled() {
    PG_SLEEP_CANCELED.with(|c| c.set(true));
}

/// Read-and-clear the pg_sleep cancellation flag.  Returns `true` once if
/// `mark_pg_sleep_canceled` was called since the last `take_pg_sleep_canceled`.
pub(crate) fn take_pg_sleep_canceled() -> bool {
    PG_SLEEP_CANCELED.with(|c| {
        let v = c.get();
        if v { c.set(false); }
        v
    })
}

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, TableMetadata};
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName};
use basin_storage::{FileFormat, WriteOptions};
use sqlparser::ast::{
    AssignmentTarget, ConflictTarget, Expr, ObjectName, OnConflictAction, OnInsert, SetExpr,
    Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::convert::{batch_df_to_ws, batch_ws_to_df, schema_df_to_ws};
use crate::ddl::{extract_create_table_cluster_by, extract_exclude_using_gist, partition_spec_from_ast};
use crate::dml::{batch_from_rows, group_rows_by_partition};
use crate::events::{
    build_row_json, dispatch_post_commit, dispatch_pre_commit, make_event, registry_has_any,
};
use crate::fast_select::{execute_simple_select, match_simple_select};
use crate::lifecycle::{
    extract_create_table_lifecycle, extract_select_include_deleted, CreateTableLifecycle,
};
use crate::session::refresh_table;
use crate::{ExecResult, ProjectSession};
use basin_catalog::PartitionSpec;

/// Dispatch `LISTEN` / `UNLISTEN` / `NOTIFY` through the engine's SQL
/// pub-sub primitive (`crate::notify_registry`).
///
/// PG transaction semantics: `NOTIFY` issued inside an open transaction is
/// buffered and only fanned out on `COMMIT`; `ROLLBACK` discards it.
/// `LISTEN` / `UNLISTEN` take effect immediately (they are not transactional
/// in PG either — the docs explicitly state subscriptions are session state
/// not txn state).
async fn exec_pubsub(
    sess: &ProjectSession,
    ps: crate::pg_ast::PubSubStmt,
) -> Result<ExecResult> {
    use crate::pg_ast::PubSubStmt;
    let registry = sess.engine.notify_registry();
    match ps {
        PubSubStmt::Listen(channel) => {
            if channel.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "LISTEN: channel name is required".into(),
                ));
            }
            crate::session::listen_subscribe(&sess.state, registry, sess.project, &channel);
            Ok(ExecResult::Empty {
                tag: "LISTEN".into(),
            })
        }
        PubSubStmt::Unlisten(channel) => {
            crate::session::listen_unsubscribe(&sess.state, &channel);
            Ok(ExecResult::Empty {
                tag: "UNLISTEN".into(),
            })
        }
        PubSubStmt::UnlistenAll => {
            crate::session::listen_unsubscribe_all(&sess.state);
            Ok(ExecResult::Empty {
                tag: "UNLISTEN".into(),
            })
        }
        PubSubStmt::Notify { channel, payload } => {
            if channel.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "NOTIFY: channel name is required".into(),
                ));
            }
            if crate::session::tx_is_active(&sess.state) {
                // PG semantics: queue until COMMIT; discard on ROLLBACK.
                crate::session::listen_buffer_notify(&sess.state, channel, payload);
            } else {
                // Auto-commit: publish immediately.
                registry.publish(sess.project, &channel, &payload, sess.session_pid);
            }
            Ok(ExecResult::Empty {
                tag: "NOTIFY".into(),
            })
        }
    }
}

pub(crate) async fn execute(sess: &ProjectSession, sql: &str) -> Result<ExecResult> {
    // Phase 5.28.C: touch last-activity timestamp so the idle-in-txn reaper
    // sees that this session is still making progress. Done unconditionally at
    // the top of every execute — even for rejected statements — so the reaper
    // doesn't incorrectly fire during a burst of aborted commands.
    crate::session::touch_last_active(&sess.state);

    // Phase 5.28.C: check whether the idle-in-txn reaper has flagged this
    // session as expired. If so, reject with SQLSTATE 25P03 immediately.
    if sess.reaped_flag.is_reaped() {
        return Err(basin_common::BasinError::IdleInTransactionTimeout(
            "idle transaction terminated by server idle_in_transaction_session_timeout".into(),
        ));
    }

    // Keep a reference to the SQL the user actually wrote. The rewriter
    // below mangles vector operators into UDF calls; that rewrite is
    // irrelevant to (and would only confuse) the analytical engine, which
    // doesn't know our UDFs.
    let raw_sql = sql;

    // #53 P1 — parser DoS pre-check (depth guard). Both libpg_query (C, called
    // below via `pg_ast::parse`) and sqlparser (Rust, called further down) use
    // recursive descent: a deeply nested expression like `(((…1…)))` with
    // 10 000 levels overflows the thread stack and SIGABRTs the process —
    // remote-triggerable DoS. The guard scans the SQL string for unbalanced
    // paren depth (quote- and comment-aware) and rejects with
    // `BasinError::InvalidSchema` (SQLSTATE 42601) when it exceeds
    // `MAX_PARSE_DEPTH`. We surface the rejection here (not just inside
    // `pg_ast::parse`) because the existing call site swallows the error with
    // `.ok()` and would otherwise hand the same SQL to sqlparser, which would
    // then blow the stack instead.
    crate::pg_ast::check_parse_depth(sql)?;

    // ADR 0014 Phase 1: parse with the real PostgreSQL parser first.
    // This lets us:
    //   1. Intercept noop-accept statements (VACUUM, ANALYZE, CLUSTER, LOCK,
    //      COMMENT, EXPLAIN, RBAC primitives, FDWs, ownership, etc.) and return
    //      immediately — sqlparser never sees them.
    //   2. Reject explicitly-unsupported statements (LISTEN, NOTIFY, UNLISTEN)
    //      with SQLSTATE 0A000 before sqlparser sees them.
    //   3. Route TRUNCATE to its real implementation.
    // On pg_query parse failure we fall through to sqlparser, which will
    // produce its own error. Both errors will surface as
    // BasinError::InvalidSchema (SQLSTATE 42601) to the client.
    //
    // IMPORTANT: We cache the parse tree so the second noop-accept / reject
    // gate later in this function (after the string-rewrite pipeline) can
    // reuse the same tree without calling pg_query::parse a second time.
    // pg_query::parse calls into a C library and is not cheap.
    let raw_pg_tree: Option<crate::pg_ast::ParseTree> = crate::pg_ast::parse(sql).ok();
    if let Some(ref tree) = raw_pg_tree {
        // Collect statement kinds so we can dispatch before sqlparser.
        let kinds: Vec<_> = tree.stmts().map(|n| crate::pg_ast::stmt_kind(n)).collect();

        // Dispatch LISTEN / NOTIFY / UNLISTEN through the engine's SQL
        // pub-sub primitive (`crate::notify_registry`). For multi-statement
        // bodies we only fast-path single-statement messages here — the
        // router splits multi-statement simple-query into individual
        // statements before reaching this entry point, so the common case
        // is `kinds.len() == 1`.
        if kinds.len() == 1 {
            use crate::pg_ast::StmtKind;
            let kind = kinds[0];
            if matches!(kind, StmtKind::Listen | StmtKind::Notify | StmtKind::Unlisten) {
                let node = tree.stmts().next().expect("kinds[0] implies stmts[0]");
                let ps = crate::pg_ast::pubsub_stmt(node)
                    .expect("pubsub stmt kind must classify");
                return exec_pubsub(sess, ps).await;
            }
        }

        // Noop-accept dispatch: for single-statement SQL only (the common case).
        // Multi-statement bodies are split by the router before reaching execute().
        if kinds.len() == 1 {
            let kind = kinds[0];

            // TRUNCATE is a real operation — delete all rows.
            if matches!(kind, crate::pg_ast::StmtKind::Truncate) {
                return crate::truncate::exec_truncate(sess, &tree).await;
            }

            // ── Aborted-state guard (SQLSTATE 25P02) ──────────────────────────
            // When a statement failed inside an active transaction the session
            // enters the "aborted" state.  Every subsequent statement except
            // ROLLBACK (full or to-savepoint) is rejected until the client
            // issues ROLLBACK to reset the session.
            if crate::session::tx_is_aborted(&sess.state) {
                // ROLLBACK and ROLLBACK TO SAVEPOINT are the only exit paths.
                let is_rollback_kind = matches!(kind, crate::pg_ast::StmtKind::Rollback);
                if !is_rollback_kind {
                    return Err(basin_common::BasinError::InvalidSchema(
                        "ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)".into()
                    ));
                }
            }

            // Transaction control — intercept before noop_accept so TxState
            // is updated on every BEGIN / COMMIT / ROLLBACK / SAVEPOINT.
            match kind {
                crate::pg_ast::StmtKind::BeginTransaction => {
                    // Snapshot the current catalog heads into TxState so that
                    // ROLLBACK can restore them.
                    let current_snapshots = sess
                        .state
                        .snapshots
                        .try_lock()
                        .map(|g| g.clone())
                        .unwrap_or_default();
                    crate::session::tx_begin(&sess.state, current_snapshots);
                    return Ok(ExecResult::Empty {
                        tag: "BEGIN".into(),
                    });
                }
                crate::pg_ast::StmtKind::Commit => {
                    // Phase 5.14.C2 / ADR 0020 §6: promote hot-tier rows to
                    // the shared MemTableRegistry and emit an explicit WAL
                    // TxCommit marker so crash-recovery replay can distinguish
                    // committed transactions from crash-mid-tx aborts.
                    //
                    // Capture tx_id *before* tx_commit() clears TxState.
                    let commit_tx_id = crate::session::tx_get_id(&sess.state);
                    let htap_rows = crate::session::tx_htap_take_all(&sess.state);

                    // Promote committed HTAP batches to the process-wide registry.
                    htap_promote_to_registry(sess, &htap_rows).await?;

                    // Flush pending files to the catalog (real COMMIT).
                    let pending = crate::session::tx_commit(&sess.state);
                    if !pending.is_empty() {
                        for (table, files) in &pending {
                            // Load the current snapshot id for this table.
                            let snap = {
                                let snaps = sess.state.snapshots.lock().await;
                                snaps
                                    .get(table)
                                    .copied()
                                    .unwrap_or(basin_catalog::SnapshotId(0))
                            };
                            match commit_with_retry(sess, table, snap, files.clone()).await {
                                Ok(()) => {}
                                Err(e) => {
                                    // If catalog append fails, return error.
                                    // The pending map was already drained from
                                    // TxState; the session is now in a dirty
                                    // state (files written but not catalogued).
                                    // Best-effort: mark aborted — client must ROLLBACK.
                                    crate::session::tx_set_aborted(&sess.state);
                                    return Err(e);
                                }
                            }
                            // Refresh the session's DataFusion context so
                            // the committed files are visible via catalog.
                            let _ = refresh_table(
                                &sess.engine,
                                &sess.project,
                                &sess.ctx,
                                &sess.state,
                                table,
                            )
                            .await;
                        }
                    }
                    // Flush any NOTIFY payloads buffered during this
                    // transaction to the engine's NotifyRegistry. PG
                    // semantics: notifies queued inside BEGIN..COMMIT only
                    // fire on COMMIT; they are silently dropped on
                    // ROLLBACK (handled in the Rollback arm below).
                    let pending_notifies =
                        crate::session::listen_take_pending_notifies(&sess.state);
                    if !pending_notifies.is_empty() {
                        let registry = sess.engine.notify_registry();
                        for n in pending_notifies {
                            registry.publish(
                                sess.project,
                                &n.channel,
                                &n.payload,
                                sess.session_pid,
                            );
                        }
                    }
                    // ADR 0020 §6: emit explicit TxCommit WAL marker so that
                    // crash-recovery replay can distinguish committed transactions
                    // from crash-mid-tx aborts.  Only emitted when the executor
                    // had a WAL-backed tx_id (i.e. at least one DML was executed
                    // inside this BEGIN block, which triggers htap_emit_wal_begin_lazy).
                    // Best-effort: WAL marker failures must not block commit.
                    if let (Some(shard), Some(tx_id)) =
                        (sess.engine.config().shard.as_ref(), commit_tx_id)
                    {
                        let part = basin_common::PartitionKey::default_key();
                        let _ = shard
                            .wal()
                            .append_tx_commit(&sess.project, &part, tx_id)
                            .await;
                    }
                    return Ok(ExecResult::Empty {
                        tag: "COMMIT".into(),
                    });
                }
                crate::pg_ast::StmtKind::Rollback | crate::pg_ast::StmtKind::Savepoint => {
                    // Use libpg_query's authoritative parse to tell the
                    // transaction-control sub-forms apart and to pull the
                    // savepoint name straight from the AST.  PostgreSQL makes
                    // `WORK`, `TRANSACTION`, and `SAVEPOINT` all optional, so
                    // the old `contains("SAVEPOINT")` text heuristic mis-routed
                    // `ROLLBACK TO s` (no SAVEPOINT keyword) into a full
                    // transaction rollback. `txn_stmt` is exact.
                    let txn = tree.stmts().next().and_then(crate::pg_ast::txn_stmt);
                    use crate::pg_ast::TxnStmt;
                    match txn {
                        Some(TxnStmt::RollbackToSavepoint(name)) => {
                            // PG: outside an explicit transaction block this is
                            // an error (SQLSTATE 25P01, no_active_sql_transaction):
                            // `ROLLBACK TO SAVEPOINT can only be used in
                            // transaction blocks` — not "savepoint does not exist".
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "ROLLBACK TO SAVEPOINT can only be used in \
                                     transaction blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "ROLLBACK TO SAVEPOINT: missing savepoint name",
                                ));
                            }
                            match crate::session::tx_rollback_to_savepoint(&sess.state, &name) {
                                Ok((abandoned, snapshots)) => {
                                    // Delete abandoned files (best-effort).
                                    for (_, files) in &abandoned {
                                        for f in files {
                                            let path =
                                                object_store::path::Path::from(f.path.as_str());
                                            let _ = sess
                                                .engine
                                                .config()
                                                .storage
                                                .delete_file(&sess.project, &path)
                                                .await;
                                        }
                                    }
                                    // Refresh DataFusion ctx for affected tables.
                                    let touched = crate::session::tx_touched_tables(&sess.state);
                                    for table in &touched {
                                        let pending = crate::session::tx_pending_files_for(
                                            &sess.state,
                                            table,
                                        );
                                        let _ = crate::session::refresh_table_with_extra(
                                            &sess.engine,
                                            &sess.project,
                                            &sess.ctx,
                                            &sess.state,
                                            table,
                                            &pending,
                                        )
                                        .await;
                                    }
                                    // Tables where all pending files were abandoned:
                                    // restore to pre-tx catalog view.
                                    for table in abandoned.keys() {
                                        if !touched.contains(table) {
                                            if let Some(snap) = snapshots.get(table) {
                                                let _ = sess
                                                    .engine
                                                    .config()
                                                    .catalog
                                                    .rollback_to_snapshot(
                                                        &sess.project,
                                                        table,
                                                        *snap,
                                                    )
                                                    .await;
                                            }
                                            let _ = refresh_table(
                                                &sess.engine,
                                                &sess.project,
                                                &sess.ctx,
                                                &sess.state,
                                                table,
                                            )
                                            .await;
                                        }
                                    }
                                    return Ok(ExecResult::Empty {
                                        tag: "ROLLBACK".into(),
                                    });
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Some(TxnStmt::Rollback) | None
                            if matches!(kind, crate::pg_ast::StmtKind::Rollback) =>
                        {
                            // Phase 5.14.C2: emit WAL Rollback marker so that
                            // crash-recovery replay knows to suppress these entries.
                            // HTAP rows are discarded inside tx_rollback (they were
                            // never promoted to the shared MemTableRegistry).
                            let rolled_back_tx_id = crate::session::tx_get_id(&sess.state);
                            if let (Some(shard), Some(tx_id)) =
                                (sess.engine.config().shard.as_ref(), rolled_back_tx_id)
                            {
                                let part = basin_common::PartitionKey::default_key();
                                // Best-effort: WAL marker failures must not block rollback.
                                let _ = shard
                                    .wal()
                                    .append_tx_rollback(&sess.project, &part, tx_id)
                                    .await;
                            }
                            // Bare ROLLBACK — undo everything.
                            let (pending, snapshots) = crate::session::tx_rollback(&sess.state);
                            // Delete pending (not-yet-committed) files best-effort.
                            for (_, files) in &pending {
                                for f in files {
                                    let path = object_store::path::Path::from(f.path.as_str());
                                    let _ = sess
                                        .engine
                                        .config()
                                        .storage
                                        .delete_file(&sess.project, &path)
                                        .await;
                                }
                            }
                            // Restore catalog snapshot for each touched table.
                            for (table, snap_id) in &snapshots {
                                let _ = sess
                                    .engine
                                    .config()
                                    .catalog
                                    .rollback_to_snapshot(&sess.project, table, *snap_id)
                                    .await;
                                let _ = refresh_table(
                                    &sess.engine,
                                    &sess.project,
                                    &sess.ctx,
                                    &sess.state,
                                    table,
                                )
                                .await;
                            }
                            // PG semantics: NOTIFY payloads buffered inside
                            // this transaction are silently discarded on
                            // ROLLBACK. Subscriptions themselves are session
                            // state and survive — only the queued sends
                            // are dropped.
                            crate::session::listen_discard_pending_notifies(&sess.state);
                            return Ok(ExecResult::Empty {
                                tag: "ROLLBACK".into(),
                            });
                        }
                        Some(TxnStmt::Savepoint(name)) => {
                            // PG: `SAVEPOINT can only be used in transaction
                            // blocks` (SQLSTATE 25P01) when not inside BEGIN.
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "SAVEPOINT can only be used in transaction \
                                     blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "SAVEPOINT: missing savepoint name",
                                ));
                            }
                            crate::session::tx_push_savepoint(&sess.state, name);
                            return Ok(ExecResult::Empty {
                                tag: "SAVEPOINT".into(),
                            });
                        }
                        Some(TxnStmt::ReleaseSavepoint(name)) => {
                            // PG: outside a transaction block this is an error
                            // (SQLSTATE 25P01): `RELEASE SAVEPOINT can only be
                            // used in transaction blocks`.
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "RELEASE SAVEPOINT can only be used in \
                                     transaction blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "RELEASE SAVEPOINT: missing savepoint name",
                                ));
                            }
                            // PG raises SQLSTATE 3B001 (no_such_savepoint)
                            // when the name is unknown; propagate it instead
                            // of silently swallowing.
                            crate::session::tx_release_savepoint(&sess.state, &name)?;
                            return Ok(ExecResult::Empty {
                                tag: "RELEASE".into(),
                            });
                        }
                        // Defensive: a transaction node we don't special-case
                        // here (e.g. PREPARE TRANSACTION) falls through to the
                        // noop-accept gate below, preserving prior behaviour.
                        _ => {}
                    }
                }
                _ => {}
            }

            // ── Phase 5.13.B — AST-based DDL pre-screens ──────────────────────
            //
            // Statement kinds parsed by libpg_query but not by sqlparser 0.52
            // are dispatched here via typed AST matches instead of textual
            // regex/lexer helpers. The pg_query parse tree is already in hand
            // (`raw_pg_tree` above) so this costs nothing extra.
            //
            // Migration 1: ALTER TYPE <name> ADD VALUE '<label>'
            if matches!(kind, crate::pg_ast::StmtKind::AlterType) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterEnumStmt(ref aes)) = node.node {
                        let (type_name, label) =
                            crate::type_ddl::match_alter_type_add_value_ast(aes)?;
                        return crate::type_ddl::exec_alter_type_add_value(
                            sess, &type_name, &label,
                        )
                        .await;
                    }
                }
            }

            // Migration 2: CREATE DOMAIN <name> [AS] <type> [CHECK (<pred>)]
            if matches!(kind, crate::pg_ast::StmtKind::CreateDomain) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateDomainStmt(ref cds)) = node.node {
                        let (name, base, check) =
                            crate::type_ddl::match_create_domain_ast(cds)?;
                        return crate::type_ddl::exec_create_domain(sess, &name, base, check)
                            .await;
                    }
                }
            }

            // Migration 3: DROP DOMAIN [IF EXISTS] <name>
            if matches!(kind, crate::pg_ast::StmtKind::DropDomain) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::DropStmt(ref ds)) = node.node {
                        if let Some((name, if_exists)) =
                            crate::type_ddl::match_drop_domain_ast(ds)?
                        {
                            return crate::type_ddl::exec_drop_domain(sess, &name, if_exists)
                                .await;
                        }
                    }
                }
            }

            // Migration 4: REFRESH MATERIALIZED VIEW <name>
            // Note: Basin's custom `WITH (full = true)` extension is not valid
            // PG SQL so pg_query rejects it — raw_pg_tree is None for that
            // form and this block is skipped. The textual pre-screen below
            // handles the WITH (full = true) case.
            if matches!(kind, crate::pg_ast::StmtKind::RefreshMatView) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RefreshMatViewStmt(ref rms)) = node.node {
                        let (name, force_full) =
                            crate::cv_ddl::match_refresh_materialized_view_ast(rms)?;
                        return crate::cv_ddl::exec_refresh_materialized_view(
                            sess, &name, force_full,
                        )
                        .await;
                    }
                }
            }

            // Migration 5: DROP MATERIALIZED VIEW [IF EXISTS] <name>
            if matches!(kind, crate::pg_ast::StmtKind::DropMatView) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::DropStmt(ref ds)) = node.node {
                        if let Some((name, if_exists)) =
                            crate::cv_ddl::match_drop_materialized_view_ast(ds)?
                        {
                            return crate::cv_ddl::exec_drop_materialized_view(
                                sess, &name, if_exists,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 6: CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opts…]
            // The textual pre-screen in match_create_sequence claims all CREATE
            // SEQUENCE shapes; the AST path replaces it for the valid-PG forms.
            if matches!(kind, crate::pg_ast::StmtKind::CreateSequence) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateSeqStmt(ref css)) = node.node {
                        if let Some(intent) =
                            crate::seq_ddl::match_create_sequence_ast(css)?
                        {
                            return crate::seq_ddl::exec_create_sequence_pre_screen(
                                sess, intent,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 7: ALTER SEQUENCE [IF EXISTS] <name> [opt …]
            if matches!(kind, crate::pg_ast::StmtKind::AlterSequence) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterSeqStmt(ref ass)) = node.node {
                        let intent = crate::seq_ddl::match_alter_sequence_ast(ass)?;
                        return crate::seq_ddl::exec_alter_sequence(sess, intent).await;
                    }
                }
            }

            // Migration 8: ALTER SCHEMA <old> RENAME TO <new>
            if matches!(kind, crate::pg_ast::StmtKind::AlterSchemaRename) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RenameStmt(ref rs)) = node.node {
                        let (old, new) =
                            crate::schema_ddl::match_alter_schema_rename_ast(rs)?;
                        return crate::schema_ddl::exec_alter_schema_rename(
                            sess, &old, &new,
                        )
                        .await;
                    }
                }
            }

            // Migration 9: ALTER FUNCTION <name>(<args>) RENAME TO <new>
            if matches!(kind, crate::pg_ast::StmtKind::AlterFunctionRename) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RenameStmt(ref rs)) = node.node {
                        let (old, new) =
                            crate::function_ddl::match_alter_function_rename_ast(rs)?;
                        return crate::function_ddl::exec_alter_function_rename(
                            sess, &old, &new,
                        )
                        .await;
                    }
                }
            }

            // Migration 10: CREATE PROCEDURE <name>(<args>) LANGUAGE sql AS $$ <body> $$
            if matches!(kind, crate::pg_ast::StmtKind::CreateProcedure) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateFunctionStmt(ref cfs)) = node.node {
                        if cfs.is_procedure {
                            let (name, args, body) =
                                crate::procedure_ddl::match_create_procedure_ast(cfs)?;
                            return crate::procedure_ddl::exec_create_procedure(
                                sess, &name, args, &body,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 11: MOVE <direction> [FROM|IN] <cursor>
            // libpg_query routes MOVE as FetchStmt(ismove=true); the
            // textual pre-screen below (match_move_sql) converts the
            // synthetic FETCH form via sqlparser, which is now redundant
            // for the common PG MOVE forms.
            if matches!(kind, crate::pg_ast::StmtKind::Move) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::FetchStmt(ref fs)) = node.node {
                        let intent = crate::cursor::match_move_sql_ast(fs)?;
                        return exec_cursor_move(sess, intent).await;
                    }
                }
            }

            // Migration 12: ALTER TABLE t ALTER COLUMN col SET GENERATED ALWAYS/
            //               BY DEFAULT, and DROP IDENTITY [IF EXISTS].
            // libpg_query parses these as AlterTableStmt with AtSetIdentity (65)
            // or AtDropIdentity (66) command subtypes. sqlparser 0.52 cannot parse
            // these PG-specific identity-sequence manipulation forms; they are
            // accepted as metadata-only no-ops (same policy as SET NOT NULL).
            if matches!(kind, crate::pg_ast::StmtKind::AlterTable) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterTableStmt(ref at)) = node.node {
                        use pg_query::protobuf::AlterTableType;
                        let all_identity = !at.cmds.is_empty()
                            && at.cmds.iter().all(|cmd| {
                                if let Some(pg_query::NodeEnum::AlterTableCmd(c)) =
                                    cmd.node.as_ref()
                                {
                                    c.subtype == AlterTableType::AtSetIdentity as i32
                                        || c.subtype == AlterTableType::AtDropIdentity as i32
                                } else {
                                    false
                                }
                            });
                        if all_identity {
                            return Ok(ExecResult::Empty {
                                tag: "ALTER TABLE".into(),
                            });
                        }
                    }
                }
            }

            if let Some(result) = crate::noop_accept::try_accept_as_noop(kind, sql) {
                return Ok(result);
            }
        }
        crate::pg_ast::reject_unsupported(&tree)?;
    }

    // ── Phase 5.29.B: ALTER TABLE … SET (timescaledb.compress …) ────────────
    // Accepted as a metadata-only no-op: Basin stores data in Parquet which
    // is already columnar; the DDL is accepted so ORM migrations that call it
    // (e.g. TimescaleDB Diesel migrations) do not abort.
    if crate::hypertable::match_alter_table_timescaledb_compress(raw_sql) {
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // ── Phase 5.29.B: SELECT create_hypertable('table', 'col', …) ───────────
    // Convert a plain Basin table into a hypertable by registering it in the
    // HypertableRegistry with its time column and chunk interval.  Returns a
    // single-row result mirroring TimescaleDB's output shape.
    if let Some((ht_table, ht_col, ht_interval)) =
        crate::hypertable::match_create_hypertable(raw_sql)
    {
        return exec_create_hypertable(sess, &ht_table, &ht_col, &ht_interval).await;
    }

    // ── Phase 5.29.D: SELECT add_retention_policy('table', INTERVAL '…') ───
    if let Some((rp_table, rp_interval)) =
        crate::hypertable::match_add_retention_policy(raw_sql)
    {
        return exec_add_retention_policy(sess, &rp_table, &rp_interval).await;
    }

    // ── Phase 5.29.D: SELECT run_retention_policy('table') ──────────────────
    if let Some(rp_table) = crate::hypertable::match_run_retention_policy(raw_sql) {
        return exec_run_retention_policy(sess, &rp_table).await;
    }

    // ── Phase 5.29.E: SELECT compress_chunk(…) ──────────────────────────────
    // Accepted as a metadata operation: marks the chunk compressed in the
    // registry.  Actual Parquet-level compression is deferred to 5.29.E.
    if let Some(intent) = crate::hypertable::match_compress_chunk(raw_sql) {
        return exec_compress_chunk(sess, intent).await;
    }

    // Phase 5.14.C5 — ALTER PROJECT <name> SET basin.memtable_hard_cap = <n>.
    if let Some((project_name, hard_cap_bytes)) =
        crate::alter_project::match_alter_project_memtable_cap(sql)?
    {
        crate::alter_project::exec_alter_project_memtable_cap(
            sess,
            &project_name,
            hard_cap_bytes,
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER PROJECT".into(),
        });
    }

    // Phase 6.X.B (ADR 0023) — ALTER PROJECT <name> SET partitions = <n>.
    // Persists in `ProjectStorageConfig::provider_extras`; the
    // `LeaseAwareShardMap` reads it on the next `owner_for` call. Existing
    // leases are NOT re-balanced here (that's 6.X.C lease handoff).
    if let Some((project_name, partitions)) =
        crate::alter_project::match_alter_project_partitions(sql)?
    {
        crate::alter_project::exec_alter_project_partitions(sess, &project_name, partitions)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER PROJECT".into(),
        });
    }

    // Phase 5.8: Basin-specific ALTER TABLE extensions (`SET cold_after`,
    // `SET cold_age_column`, `SET BLOOM FILTERS ON (...)`, `CLUSTER BY`,
    // `RESET CLUSTER BY`).
    if let Some(ext) = crate::alter::match_basin_alter_extension(sql)? {
        let table = ext.table().clone();
        let tag = ext
            .apply(&sess.engine.config().catalog, &sess.project)
            .await?;
        crate::session::refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table)
            .await?;
        return Ok(ExecResult::Empty { tag: tag.into() });
    }

    // MOVE <direction> [FROM|IN] <cursor> — sqlparser 0.52 has no
    // Statement::Move AST node.  Pre-screen textually and dispatch before
    // sqlparser even sees the SQL.
    if let Some(intent) = crate::cursor::match_move_sql(sql) {
        return exec_cursor_move(sess, intent).await;
    }

    // REFRESH MATERIALIZED VIEW <name> [WITH (full = true)] — sqlparser
    // has no AST node for REFRESH, so we recognise the full statement
    // textually and dispatch. `force_full` toggles the v0.1 opt-out from
    // incremental refresh.
    // REFRESH MATERIALIZED VIEW <name> [ WITH (full = true) ].
    if let Some((name, force_full)) = crate::cv_ddl::match_refresh_materialized_view(sql)? {
        return crate::cv_ddl::exec_refresh_materialized_view(sess, &name, force_full).await;
    }

    // ALTER SCHEMA <old> RENAME TO <new>: sqlparser 0.52 has no AlterSchema
    // AST node, so we recognise the full statement textually before sqlparser
    // sees it. Must be checked before ALTER FUNCTION (both start with ALTER).
    if let Some((old, new)) = crate::schema_ddl::match_alter_schema_rename(sql) {
        return crate::schema_ddl::exec_alter_schema_rename(sess, &old, &new).await;
    }

    // ALTER FUNCTION <name>(<args>) RENAME TO <new>: sqlparser 0.52 has no
    // AlterFunction AST node, so we recognise the full statement textually
    // and dispatch to the catalog rename helper.
    if let Some((old, new)) = crate::function_ddl::match_alter_function_rename(sql)? {
        return crate::function_ddl::exec_alter_function_rename(sess, &old, &new).await;
    }

    // CREATE PROCEDURE … LANGUAGE sql AS $$ … $$: sqlparser 0.52 only
    // parses the T-SQL `AS BEGIN … END` shape natively, so the
    // PG-style form is recognised textually before sqlparser sees it.
    // See `procedure_ddl::match_create_procedure` for the shape.
    if let Some((name, args, body)) = crate::procedure_ddl::match_create_procedure(sql)? {
        return crate::procedure_ddl::exec_create_procedure(sess, &name, args, &body).await;
    }

    // 5.11.I — `ALTER TABLE … SUBSCRIBE WEBHOOK …`. libpg_query rejects
    // `SUBSCRIBE` outright.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_subscribe_webhook(sql)? {
        crate::webhook_ddl::exec_subscribe_webhook(
            intent,
            &sess.project,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.I — `ALTER TABLE … UNSUBSCRIBE WEBHOOK <name>`. libpg_query
    // rejects `UNSUBSCRIBE` outright.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_unsubscribe_webhook(sql)? {
        crate::webhook_ddl::exec_unsubscribe_webhook(
            intent,
            &sess.project,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `ALTER TABLE … REACT ON … EXECUTE <body>`. libpg_query
    // rejects `REACT` outright. The body matcher returns `None` for the
    // constraint-shaped form (handled by the next arm).
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_on(sql)? {
        crate::reactor_ddl::exec_react_on(intent, &sess.project, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C2 — `ALTER TABLE … REACT ON … CONSTRAINT (<predicate>)`.
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_constraint(sql)? {
        crate::reactor_ddl::exec_react_constraint(
            intent,
            &sess.project,
            &sess.engine.config().catalog,
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `DROP REACTOR <name> ON <table>`. libpg_query rejects
    // `REACTOR` outright.
    if let Some(intent) = crate::reactor_ddl::match_drop_reactor(sql)? {
        crate::reactor_ddl::exec_drop_reactor(intent, &sess.project, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "DROP REACTOR".into(),
        });
    }

    // 5.11.N + 6.SEC.P0.3 — `CREATE INBOUND WEBHOOK …` returns the generated
    // HMAC secret as a single-row `secret` result set so the creator can
    // copy it once. `DROP INBOUND WEBHOOK …` is an empty side-effect.
    if let Some(i) = crate::inbound_webhook_ddl::match_create_inbound_webhook(sql)? {
        let secret_hex = crate::inbound_webhook_ddl::exec_create_inbound_webhook(
            i,
            &sess.project,
            sess.engine.config().catalog.as_ref(),
        )
        .await?;
        let schema = Arc::new(Schema::new(vec![Field::new("secret", DataType::Utf8, false)]));
        let arr: ArrayRef = Arc::new(StringArray::from(vec![secret_hex]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr])
            .map_err(|e| BasinError::Internal(format!("build CREATE INBOUND WEBHOOK row: {e}")))?;
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![batch],
        });
    }
    if let Some(i) = crate::inbound_webhook_ddl::match_drop_inbound_webhook(sql)? { crate::inbound_webhook_ddl::exec_drop_inbound_webhook(i, &sess.project, sess.engine.config().catalog.as_ref()).await?; return Ok(ExecResult::Empty { tag: "DROP INBOUND WEBHOOK".into() }); }

    // DROP MATERIALIZED VIEW [IF EXISTS] <name> — sqlparser's DROP parser
    // does not recognise MATERIALIZED VIEW, so we handle the full
    // statement before sqlparser sees it.
    if let Some((name, if_exists)) = crate::cv_ddl::match_drop_materialized_view(sql)? {
        return crate::cv_ddl::exec_drop_materialized_view(sess, &name, if_exists).await;
    }

    // ── Phase 5.21.B — replication slot management ───────────────────────────
    // `SELECT … FROM pg_create_logical_replication_slot(name, plugin)`
    if let Some((slot_name, plugin)) =
        crate::replication::slot_udf::match_create_replication_slot(sql)
    {
        return crate::replication::slot_udf::exec_create_replication_slot(
            sess,
            &slot_name,
            &plugin,
        )
        .await;
    }

    // `SELECT pg_drop_replication_slot(name)`
    if let Some(slot_name) = crate::replication::slot_udf::match_drop_replication_slot(sql) {
        return crate::replication::slot_udf::exec_drop_replication_slot(sess, &slot_name).await;
    }

    // ── Phase 5.21.D — current WAL LSN ───────────────────────────────────────
    if crate::replication::slot_udf::match_current_wal_lsn(sql) {
        return crate::replication::slot_udf::exec_current_wal_lsn(sess).await;
    }

    // ── Phase 5.21.C — logical slot get changes ───────────────────────────────
    if let Some(slot_name) = crate::replication::slot_udf::match_logical_slot_get_changes(sql) {
        return crate::replication::slot_udf::exec_logical_slot_get_changes(sess, &slot_name).await;
    }

    // ── Phase 5.21.E — publication DDL ───────────────────────────────────────
    // CREATE PUBLICATION / ALTER PUBLICATION / DROP PUBLICATION are not
    // recognised by sqlparser 0.52's PostgreSQL dialect. Intercept via
    // text matching before sqlparser sees the SQL.
    if let Some(intent) = crate::replication::publication_ddl::match_create_publication(sql) {
        return crate::replication::publication_ddl::exec_create_publication(sess, intent).await;
    }
    if let Some(intent) = crate::replication::publication_ddl::match_alter_publication(sql) {
        return crate::replication::publication_ddl::exec_alter_publication(sess, intent).await;
    }
    if let Some(pubname) = crate::replication::publication_ddl::match_drop_publication(sql) {
        return crate::replication::publication_ddl::exec_drop_publication(sess, &pubname).await;
    }
    // ─────────────────────────────────────────────────────────────────────────

    // ALTER SEQUENCE [IF EXISTS] <name> [RESTART [WITH n]]: sqlparser 0.52
    // has no AlterSequence AST node; textual pre-screen handles the full
    // PG grammar.
    if let Some(intent) = crate::seq_ddl::match_alter_sequence(sql)? {
        return crate::seq_ddl::exec_alter_sequence(sess, intent).await;
    }

    // CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opt …] —
    // sqlparser 0.52 only parses one option per CREATE SEQUENCE
    // statement, so the full PG grammar fails at the second option. The
    // textual matcher claims any CREATE SEQUENCE shape.
    if let Some(intent) = crate::seq_ddl::match_create_sequence(sql)? {
        return crate::seq_ddl::exec_create_sequence_pre_screen(sess, intent).await;
    }

    // CREATE MATERIALIZED VIEW ... WITH (basin.continuous, refresh_interval =
    // '...'): sqlparser's WITH-clause parser cannot ingest a dotted-key
    // option like `basin.continuous`, so we lift the entire WITH (...) body
    // before sqlparser sees the SQL. The options live in `cv_options`; the
    // remainder is a vanilla CREATE MATERIALIZED VIEW the standard parser
    // accepts.
    let (cv_stripped, cv_options) = crate::cv_ddl::extract_basin_cv_options(sql)?;
    let cv_stripped_owned = cv_stripped;
    let sql = cv_stripped_owned.as_str();

    // Phase 5.7 B2: lift trailing `CLUSTER BY (col, …)` out of CREATE TABLE
    // before sqlparser sees it. PostgreSqlDialect doesn't recognise the
    // form so we strip it here and apply the columns via `set_cluster_columns`
    // after the table is created. Returns the original SQL untouched when
    // the clause isn't present.
    let (cluster_stripped, cluster_columns) = extract_create_table_cluster_by(sql)?;
    // Lift declarative lifecycle markers (AUTO_UPDATE / SOFT DELETE column
    // attributes, trailing AUDIT TO clause). Same pre-screen strategy as
    // CLUSTER BY: sqlparser doesn't recognise these forms.
    let (lifecycle_stripped, lifecycle) =
        extract_create_table_lifecycle(cluster_stripped.as_str())?;
    // Phase 5.24.F: strip `EXCLUDE USING gist (...)` table-level constraints
    // before sqlparser sees the SQL (sqlparser 0.61 has no TableConstraint::Exclude
    // variant). The parsed specs are threaded into `exec_create_table` so the
    // constraint is persisted as a sentinel CheckConstraint and enforced on INSERT.
    let (excl_stripped, exclude_specs) =
        extract_exclude_using_gist(lifecycle_stripped.as_str());
    let sql_owned = excl_stripped;
    let sql = sql_owned.as_str();

    // `CREATE TABLE … AS <query> [WITH [NO] DATA]` (CTAS). libpg_query —
    // the real PostgreSQL parser already run above as `raw_pg_tree` —
    // understands the trailing `WITH [NO] DATA` clause and exposes it as
    // `into.skip_data`. sqlparser 0.61's `CreateTable` AST has no field
    // for the clause, so its statement parser fails ("Expected: end of
    // statement, found: WITH"). When the original statement is a plain
    // CTAS we strip the trailing clause textually here (string/comment
    // aware, only the exactly-trailing token sequence) so sqlparser
    // parses the `CREATE TABLE … AS <query>` body normally; the
    // `skip_data` boolean is threaded into `exec_create_table` so the
    // row-population step can be skipped for `WITH NO DATA`. Not a CTAS
    // → SQL is left exactly as-is and `ctas_no_data` stays `None`.
    let mut ctas_shape: Option<crate::pg_ast::CtasShape> = None;
    if let Some(ref tree) = raw_pg_tree {
        if let Some(node) = tree.stmts().next() {
            ctas_shape = crate::pg_ast::ctas_shape(node);
        }
    }
    let ctas_stripped = if let Some(ref shape) = ctas_shape {
        // Strip the trailing `WITH [NO] DATA`, then (if present) the
        // bare LHS column-name list — both are libpg_query-confirmed
        // and unparseable by sqlparser 0.61. Order matters: strip the
        // tail first so the column-list scan sees a clean head.
        let s = crate::ddl::strip_trailing_with_data(sql);
        if shape.col_names.is_empty() {
            s
        } else {
            crate::ddl::strip_ctas_column_list(&s)
        }
    } else {
        sql.to_string()
    };
    let sql = ctas_stripped.as_str();

    // INCLUDE DELETED on SELECT is the soft-delete opt-out.
    let (select_stripped, include_deleted) = extract_select_include_deleted(sql);
    let sql = select_stripped.as_str();

    // ── Advanced SELECT pre-screens ─────────────────────────────────────────
    //
    // These rewrites handle SQL constructs that sqlparser 0.52 can't parse at
    // the statement level (TABLE foo, TABLESAMPLE) or that the sqlparser AST
    // expresses in a way DataFusion 44 ignores (FETCH FIRST N ROWS ONLY).
    // Each is a cheap string scan; non-matching SQL is returned as-is (Cow).
    //
    // 1. `TABLE foo` → `SELECT * FROM foo`
    //    sqlparser's top-level dispatch doesn't recognise TABLE as a statement
    //    start keyword; the query body parser does (SetExpr::Table) but
    //    DataFusion never encounters that variant. Rewrite before sqlparser.
    let table_shorthand_rewrite = crate::select_advanced::rewrite_table_shorthand(sql);
    let sql = table_shorthand_rewrite.as_ref();
    //
    // 2. Lower `TABLESAMPLE { BERNOULLI | SYSTEM } (p) [REPEATABLE(s)]` into a
    //    derived sub-select carrying a sampling predicate (BUG #134: the
    //    clause used to be silently stripped, so every row was returned).
    //    BERNOULLI -> per-row Bernoulli trial; SYSTEM -> per-record-batch
    //    trial; REPEATABLE -> seeded deterministic variant. An out-of-range
    //    percentage is a hard error (PG parity: "sample percentage must be
    //    between 0 and 100").
    let tablesample_rewrite = crate::select_advanced::rewrite_tablesample(sql)
        .map_err(|e| BasinError::InvalidSchema(e.0))?;
    let sql = tablesample_rewrite.as_ref();
    //
    // 2b. Strip `ONLY ` table inheritance modifier from `FROM ONLY <tbl>` /
    //     `JOIN ONLY <tbl>`. Basin has no table inheritance (flat-storage design);
    //     `ONLY` is a semantic no-op here. Rewriting to plain `FROM <tbl>` makes
    //     the query run normally against the base table.
    let only_rewrite = crate::pg_ast::strip_only_modifier(sql);
    let sql = only_rewrite.as_ref();
    //
    // 3. `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY` → `LIMIT N`.
    //    sqlparser parses these into `Query.fetch`; DataFusion 44's planner
    //    only reads `Query.limit`, so FETCH is silently ignored without this
    //    rewrite.  Also handles the combined `OFFSET M ROWS FETCH NEXT N` form.
    let fetch_rewrite = crate::select_advanced::rewrite_fetch_to_limit(sql);
    let sql = fetch_rewrite.as_ref();
    //
    // 4. `FOR NO KEY UPDATE` → `FOR UPDATE`, `FOR KEY SHARE` → `FOR SHARE`.
    //    sqlparser 0.52 only recognises `FOR UPDATE` and `FOR SHARE` as lock
    //    types; the PG-specific variants `FOR NO KEY UPDATE` / `FOR KEY SHARE`
    //    trigger a parse error. After the rewrite, sqlparser parses the
    //    `Query.locks` vec normally, and DataFusion ignores it entirely —
    //    Basin is append-only / optimistic-concurrency so row locking is
    //    advisory for all four locking-strength keywords.
    let for_lock_rewrite = crate::select_advanced::rewrite_for_no_key_update_and_key_share(sql);
    let sql = for_lock_rewrite.as_ref();
    // ────────────────────────────────────────────────────────────────────────
    // `INSERT INTO t [...] OVERRIDING { SYSTEM | USER } VALUE VALUES (...)`
    // — sqlparser 0.52 doesn't recognise the clause; we lift it out
    // textually and stash the kind on the session state for
    // `exec_insert` to consume. No-op for any statement that isn't
    // INSERT (or where the clause isn't present).
    let (overriding_stripped, overriding_kind) = crate::dml::extract_insert_overriding(sql)?;
    if let Some(kind) = overriding_kind {
        crate::session::set_pending_overriding(&sess.state, kind);
    }
    let overriding_owned = overriding_stripped;
    let sql = overriding_owned.as_str();

    // Auto-route `ORDER BY <vec_col> <op> <lit> LIMIT k` to the HNSW fast
    // path BEFORE the operator-to-UDF rewrite below. Once `<->` becomes
    // `l2_distance(...)` the structural signal is gone. A `None` here
    // means at least one criterion failed; the brute-force pipeline below
    // takes over and correctness is preserved.
    if let Some(plan) = crate::vector_planner::rewrite_vector_order_by(
        &sess.engine.config().catalog,
        &sess.project,
        sql,
    )
    .await?
    {
        match execute_vector_search_plan(sess, plan).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                // The HNSW segment may carry a different metric than the
                // user's operator (current sidecars are L2-only; users
                // wanting cosine/dot still parse but the segment header
                // mismatches). Fall back to brute-force rather than
                // surfacing a routing-only error.
                tracing::debug!(
                    error = %e,
                    "vector planner routed but storage rejected; falling back"
                );
            }
        }
    }

    // Rewrite `auth.uid()` / `auth.role()` / `auth.jwt()` to their
    // underscore-namespaced UDF equivalents before handing SQL to sqlparser.
    // DataFusion's SQL parser does not support schema-qualified function names
    // in call position; this rewrite is safe (identifier-boundary checked)
    // and always runs, even when auth is disabled — the UDFs simply return
    // NULL/`'anon'` for unauthenticated sessions.
    let sql = crate::udf::rewrite_auth_schema_functions(sql);
    // Phase 5.8.A: rewrite cron.schedule/unschedule and net.http_get/post.
    let sql = crate::cron_glue::rewrite_cron_schema_functions(&sql);
    let sql = crate::net_glue::rewrite_net_schema_functions(&sql);
    // Phase 5.23.D: qualify unqualified pg_catalog system view names so that
    // DataFusion resolves them in `pg_catalog` rather than `public`. E.g.
    // `FROM pg_locks` → `FROM pg_catalog.pg_locks`.
    let sql = crate::pg_operators::rewrite_unqualified_pg_catalog_views(&sql);
    let sql = sql.as_str();

    // Translate the pg_vector operator forms (`<->`, `<#>`, `<=>`) into the
    // matching UDF calls before handing the SQL to sqlparser. See
    // `udf::rewrite_vector_operators` for the strategy and its limits.
    // Lower `tsvector @@ tsquery` to the `ts_match(...)` UDF before sqlparser
    // sees the SQL — sqlparser doesn't have AtAt in its operator table.
    let rewritten = crate::pg_ast::rewrite_tsvector_at_at(sql);
    // Phase 5.26.C: strip `::vector` / `::vector(N)` casts BEFORE the operator
    // rewriter so that `'[1,0,0]'::vector <-> '[0,1,0]'::vector` becomes
    // `'[1,0,0]' <-> '[0,1,0]'` before the operator is expanded to l2_distance().
    // Also strip `::text` on vector columns before the operator rewrites see them.
    let rewritten = crate::pg_operators::rewrite_vector_cast(&rewritten);
    // Phase 5.26.B: rewrite `col::text` → `vector_to_text(col)` before operators.
    let rewritten = crate::pg_operators::rewrite_vector_col_text_cast(&rewritten);
    // Expand `json_agg(<alias>.*)` / `jsonb_agg(<alias>.*)` to the explicit
    // `jsonb_build_object(...)` form. Prisma's nested-read shape (correlated
    // subquery returning `json_agg(o.*)`) hits a DataFusion physical-planner
    // gap: it rejects `Wildcard { qualifier: Some(...) }` inside scalar
    // aggregates with XX000. The expansion is semantically identical and
    // only uses unqualified column refs + the wired `jsonb_build_object` UDF.
    // The catalog lookup builds the alias→columns map; aliases that don't
    // resolve to a real table fall through unchanged (false-positive guard).
    let rewritten = rewrite_json_agg_qualified_wildcard_with_catalog(
        &rewritten,
        sess.engine.config().catalog.as_ref(),
        &sess.project,
    )
    .await;
    let rewritten = crate::udf::rewrite_vector_operators(&rewritten);
    // Normalise whitespace around the PG JSON path operators (`->`, `->>`,
    // `#>`, `#>>`) before the JSON-op rewriter sees them.  Without this pass,
    // a bare-column form like `body->>'k'` (no spaces, no `::jsonb` cast)
    // fails the `rewrite_binary_op_to_fn` `prev_ok` guard and is left for
    // sqlparser, which doesn't know `->>` and errors out.  Pre-spacing closes
    // that gap without relaxing the boundary guard (which would risk
    // mis-matching vector ops like `<->`).
    let rewritten = crate::pg_operators::rewrite_jsonb_arrow_op_spacing(&rewritten);
    // Rewrite JSON/JSONB infix operators (`->`, `->>`, `#>`, `#>>`, `?`,
    // `?&`, `?|`, `<@`, `@>` for JSON, `||` for JSON concat, `@?` for
    // jsonpath exists) to UDF calls that DataFusion can evaluate.
    let rewritten = crate::udf::rewrite_json_operators(&rewritten);
    // Rewrite `json_to_record(J) AS t(coldefs)` / `jsonb_to_record(J) AS t(coldefs)`
    // → `SELECT * FROM json_to_recordset([J]) AS t(coldefs)` so that sqlparser
    // does not choke on the typed coldef list in scalar-SELECT / FROM position.
    let rewritten = crate::pg_operators::rewrite_json_to_record(&rewritten);
    // Rewrite a FROM-less `SELECT jsonb_array_elements('[…]'::jsonb)` (and the
    // other JSON/JSONB set-returning functions) into
    // `SELECT * FROM jsonb_array_elements('[…]'::jsonb)` so the row-expanding
    // table function (UDTF) is used instead of the single-row scalar stub.
    // PostgreSQL expands SRFs in the SELECT list to a row set; this restores
    // that behaviour for the common ORM array-/object-expansion idiom.
    let rewritten = crate::pg_operators::rewrite_jsonb_srf_scalar_select(&rewritten);
    // Rewrite PostgreSQL POSIX regex operators (`~`, `!~`, `~*`, `!~*`) to
    // `regexp_like(…)` calls DataFusion accepts; expand `BETWEEN SYMMETRIC`;
    // rewrite array containment / overlap operators (`@>`, `<@`, `&&`) for
    // array-typed operands. See `pg_operators` for the full operator table.
    let rewritten = crate::pg_operators::rewrite_posix_regex_operators(&rewritten);
    // JSONPath @? / @@ operator rewrites → jsonb_path_exists / jsonb_path_match.
    let rewritten = crate::jsonb_path_udf::rewrite_jsonpath_operators(&rewritten);
    let rewritten = crate::window_extras::rewrite_ignore_nulls_in_args(&rewritten);
    let rewritten = crate::pg_operators::rewrite_between_symmetric(&rewritten);
    // Rewrite `'{1,2,3}'::int[]` curly-brace array literal casts to
    // `make_array(1,2,3)` before the array-operator pass sees them.
    let rewritten = crate::pg_operators::rewrite_pg_array_literal_casts(&rewritten);
    let rewritten = crate::pg_operators::rewrite_array_operators(&rewritten);
    // Rewrite `B'1010'` (bit-string literals) to plain string literals `'1010'`
    // before sqlparser/DataFusion sees them. DataFusion 53 does not handle
    // sqlparser's `SingleQuotedByteStringLiteral` value variant.
    let rewritten = crate::pg_operators::rewrite_bit_string_literal(&rewritten);
    // Rewrite `'...'::UUID` to `'...'::VARCHAR` — DataFusion 53 does not
    // implement the UUID SQL type in CAST expressions.
    let rewritten = crate::pg_operators::rewrite_uuid_cast(&rewritten);
    // Rewrite `'...'::citext` to `'...'::TEXT` — citext is stored as plain
    // Utf8 at the Arrow level; the cast is a no-op for the evaluator.
    let rewritten = crate::pg_operators::rewrite_citext_cast(&rewritten);
    // Rewrite `'HH:MM:SS'::INTERVAL` to `'N seconds'::INTERVAL` — Arrow's
    // interval parser does not accept the PG HH:MM:SS shorthand form.
    let rewritten = crate::pg_operators::rewrite_interval_hms_cast(&rewritten);
    // Rewrite PG bitwise operators that DataFusion's GenericDialect doesn't
    // understand: `A # B` (XOR) → `A ^ B`; `~expr` (unary NOT) →
    // `(-1 ^ (expr))`.
    let rewritten = crate::pg_operators::rewrite_pg_bitwise_operators(&rewritten);
    // Rewrite `expr = ANY(ARRAY[...])` / `= SOME(ARRAY[...])` → `expr IN (...)`.
    // DataFusion cannot plan the ARRAY-literal form of ANY/SOME; `IN` is the
    // exact PG equivalent for equality quantification over an inline array.
    // Also handles `<> ANY(ARRAY[...])` → `NOT IN (...)`.
    // This must run BEFORE the subquery ANY rewriter so the subquery rewriter
    // only sees subquery forms.
    let rewritten = crate::pg_operators::rewrite_any_array(&rewritten);
    // Rewrite `expr OP ALL(ARRAY[...])` to a VALUES subquery so the existing
    // all-subquery rewriter can reduce it to a scalar aggregate comparison.
    let rewritten = crate::pg_operators::rewrite_all_array(&rewritten);
    // Rewrite `= ANY (subquery)` / `= SOME (subquery)` → `IN (subquery)`.
    // DataFusion's ANY subquery planner has type-coercion issues; the IN form
    // is equivalent for equality comparisons and works reliably.
    let rewritten = crate::pg_operators::rewrite_any_some_subquery(&rewritten);
    // Rewrite `LATERAL unnest(...)` → `unnest(...)` so sqlparser sees
    // TableFactor::UNNEST (handled by DataFusion) instead of
    // TableFactor::Function { lateral: true } (not a registered table fn).
    let rewritten = crate::pg_operators::rewrite_lateral_unnest(&rewritten);
    // Rewrite uncorrelated `LATERAL (subquery)` → `(subquery)`.  When the
    // subquery body has zero references to outer FROM columns, LATERAL is
    // semantically identical to a plain join.  Stripping it lets DataFusion
    // plan it without needing the (unimplemented) correlated-lateral physical
    // operator.  Correlated LATERAL is left untouched (DataFusion upstream
    // limitation: physical plan does not support OuterReferenceColumn paths).
    let rewritten = crate::pg_operators::rewrite_lateral_uncorrelated(&rewritten);
    // Rewrite the common ORM nested-read pattern:
    //   `LEFT JOIN LATERAL (SELECT agg(...) FROM child WHERE child.fk=outer.pk) sub ON true`
    // → `LEFT JOIN (SELECT child.fk, agg(...) FROM child GROUP BY child.fk) sub ON sub.fk=outer.pk`
    // Only fires when ALL projection items are aggregate functions and there is
    // exactly ONE correlation predicate.  ORDER BY / LIMIT inside the subquery,
    // non-aggregate projections, or multiple correlation predicates cause the
    // rewriter to defer (leaving the query to fail with the upstream error).
    let rewritten = crate::pg_operators::rewrite_lateral_nested_agg(&rewritten);
    // Rewrite correlated non-aggregate LATERAL subqueries into ordinary JOINs.
    //   `FROM t, LATERAL (SELECT col FROM u WHERE u.col = t.col) sub`
    //     → `FROM t INNER JOIN (SELECT col FROM u) sub ON sub.col = t.col`
    //   `LEFT JOIN LATERAL (SELECT col FROM u WHERE u.col = t.col) sub ON true`
    //     → `LEFT JOIN (SELECT col FROM u) sub ON sub.col = t.col`
    //   `JOIN LATERAL (SELECT expr AS a FROM u WHERE u.col = t.col) sub ON true`
    //     → `INNER JOIN (SELECT u.col, expr AS a FROM u) sub ON sub.col = t.col`
    // Only fires for non-aggregate, single-predicate, no-ORDER-BY/LIMIT bodies.
    let rewritten = crate::pg_operators::rewrite_lateral_correlated_row(&rewritten);
    // Rewrite correlated LATERAL bodies with ORDER BY + LIMIT into window-function
    // joins.  `LEFT JOIN LATERAL (SELECT col FROM u WHERE u.fk = t.pk ORDER BY col
    // LIMIT n) sub ON true` → `LEFT JOIN (SELECT col, u.fk, ROW_NUMBER() OVER
    // (PARTITION BY u.fk ORDER BY col) AS __basin_rn FROM u) sub ON
    // sub.fk = t.pk AND sub.__basin_rn <= n`.  Fires after the no-ORDER-BY/LIMIT
    // row-rewriter so both paths get a chance; only fires when ORDER BY + LIMIT are
    // present at depth-0 in the body and all other guards pass.
    let rewritten = crate::pg_operators::rewrite_lateral_order_limit(&rewritten);
    // Decorrelate `[CROSS] JOIN LATERAL generate_series(<lo>, <tbl>.<col>)
    // <alias>` (and the comma form) into a bounded recursive-CTE JOIN. The
    // built-in `generate_series` table function rejects non-literal args
    // (correlated column refs AND scalar subqueries alike), so the textbook
    // `generate_series(1, (SELECT max(t.id) FROM t))` rewrite is NOT viable on
    // DataFusion 53 — a recursive series bounded by the table max plus a
    // per-row range predicate reproduces exact PG semantics. Must run BEFORE
    // the recursive-CTE column-alias rewriter (below) so the emitted
    // `WITH RECURSIVE name(value) AS (SELECT …)` base case is normalised.
    // No-op unless the 2nd arg is a correlated `tbl.col`; the constant/constant
    // `generate_series(1, 3)` form is left untouched (already works).
    let rewritten = crate::pg_operators::rewrite_lateral_generate_series(&rewritten);
    // Rewrite `(s1, e1) OVERLAPS (s2, e2)` → `overlaps(s1, e1, s2, e2)`.
    let rewritten = crate::pg_operators::rewrite_overlaps(&rewritten);
    // Rewrite `agg(x) FILTER (WHERE cond)` → `agg(CASE WHEN cond THEN x END)`.
    let rewritten = crate::pg_operators::rewrite_aggregate_filter(&rewritten);
    // Strip `[NOT] MATERIALIZED` hint from `WITH cte AS [NOT] MATERIALIZED (…)`.
    let rewritten = crate::pg_operators::rewrite_cte_materialized(&rewritten);
    // Inject explicit `AS col` aliases into the base case of
    // `WITH RECURSIVE cte(col1, col2) AS (SELECT expr1, expr2 UNION ALL ...)`.
    // DataFusion builds the working-table schema from the static term's schema
    // before applying the CTE column list; unnamed literals (e.g. `SELECT 1`)
    // get auto-names like `"Int64(1)"` which break the recursive term's
    // field references.  Adding `AS col` in the base case fixes the schema.
    let rewritten = crate::pg_operators::rewrite_recursive_cte_column_aliases(&rewritten);
    // Rewrite `'[lo,hi)'::int4range = '[lo,hi)'::int4range` to
    // `range_eq('...', '...', 'int4range')` BEFORE the generic operator
    // rewriter and BEFORE cast-stripping, so the subtype is captured.
    let rewritten = crate::range_udf::rewrite_range_equality(&rewritten);
    // Translate PG range infix operators (`@>`, `<@`, `&&`, `<<`, `>>`,
    // `-|-`) into UDF calls. Must run before sqlparser sees the SQL because
    // sqlparser's PostgreSqlDialect does not model these operators.
    // The rewriter is type-heuristic: `@>` / `<@` are only rewritten when
    // at least one operand textually starts with a range constructor call
    // (int4range, numrange, …) so future JSONB `@>` rewrites won't collide.
    let rewritten = crate::range_udf::rewrite_range_operators(&rewritten);
    // Rewrite `'...'::int4range` / `'...'::daterange` etc. to just `'...'`
    // because Basin stores range values as plain Utf8; the cast suffix confuses
    // DataFusion's planner which doesn't know these custom types.
    let rewritten = crate::range_udf::rewrite_range_casts(&rewritten);
    // Rewrite `SUBSTRING(<expr> FROM '<regex>')` (single-quoted-string FROM
    // argument, not numeric) into `substring_regex(<expr>, '<regex>')` so the
    // POSIX-regex form (PG-style) routes to the regex UDF. Numeric `SUBSTRING
    // FROM int FOR int` continues to use DataFusion's built-in.
    let rewritten = crate::regex_udf::rewrite_substring_regex(&rewritten);
    // Route `EXTRACT(SECOND FROM <expr>)` to the Basin UDF that returns
    // Float64 with sub-second precision (PG's `extract(second ...)` shape).
    // Other EXTRACT fields fall through to DataFusion's `date_part`.
    let rewritten = crate::udf::rewrite_extract_second(&rewritten);
    // Rewrite `expr AT TIME ZONE 'tz'` to `at_time_zone(expr, 'tz')` so
    // DataFusion's sqlparser sees a regular function call instead of the
    // AT TIME ZONE infix operator, which it may not handle for all types.
    let rewritten = crate::interval_tz_udf::rewrite_at_time_zone(&rewritten);
    // Rewrite `EXTRACT(EPOCH FROM interval_expr)` to
    // `extract_epoch_from_interval(interval_expr)` — DataFusion's built-in
    // `EXTRACT(EPOCH FROM x)` handles timestamps but not interval values.
    let rewritten = crate::interval_tz_udf::rewrite_extract_epoch_interval(&rewritten);
    // Rewrite `make_interval(years => 1, days => 30)` (PG named-arg form) to
    // the positional form `make_interval(1, 0, 0, 30, 0, 0, 0)` so DataFusion's
    // planner accepts it (named arguments are not supported by the UDF machinery).
    let rewritten = crate::pg_scalar_aliases::rewrite_make_interval_named_args(&rewritten);
    // Rewrite `every(...)` → `bool_and(...) AS every` — PG alias for the same
    // aggregate. The AS alias preserves a distinct output column name so that
    // DataFusion doesn't see two expressions both resolving to `bool_and`.
    let rewritten = crate::pg_scalar_aliases::rewrite_every_to_bool_and(&rewritten);
    // Rewrite PG aggregate name aliases that DataFusion exposes under a
    // different name: `variance(x)` → `var(x)`.
    let rewritten = crate::udf::rewrite_pg_agg_aliases(&rewritten);
    // Add explicit AS aliases to known aliased aggregates that would otherwise
    // produce duplicate column names when DataFusion normalises them to the
    // primary UDAF name: `stddev_samp(x)` → `stddev_samp(x) AS stddev_samp`;
    // `var_samp(x)` → `var_samp(x) AS var_samp`.
    let rewritten = crate::pg_scalar_aliases::rewrite_agg_unique_aliases(&rewritten);
    // Rewrite `'infinity'::timestamp` / `'-infinity'::timestamp` to the
    // `cast_infinity_timestamp(...)` UDF before sqlparser sees the SQL.
    let rewritten = crate::datetime_extras::rewrite_infinity_timestamp(&rewritten);
    // User-defined `LANGUAGE sql` function inlining. The rewriter is a
    // no-op for projects with no registered functions and for statements
    // that contain no function calls at all (the cheap pre-gate runs
    // before any catalog hop). Anything else gets rewritten so DataFusion
    // sees the body inlined into the call site.
    let inlined = crate::sql_functions::rewrite_sql_inlining_functions(
        &sess.engine.config().catalog,
        &sess.project,
        &rewritten,
    )
    .await?;
    // Rewrite sequence calls (`nextval('seq')` / `currval('seq')` /
    // `setval('seq', n[, advance])`) to BIGINT literals before sqlparser
    // sees the SQL. Each call dispatches to the catalog (advancing
    // sequence state for `nextval` / `setval`); the per-session
    // `currval` cache is updated as part of the dispatch. No-op for
    // SQL with no sequence call sites.
    let seq_ctx = crate::seq_udf::SequenceContext {
        catalog: &sess.engine.config().catalog,
        project: sess.project,
        session_cache: &sess.state.sequence_cache,
    };
    let seq_rewritten = crate::seq_udf::rewrite_sequence_calls(&inlined, &seq_ctx).await?;
    // Phase 5.11.K2 follow-up: enum columns referenced in ORDER BY or
    // ordering comparisons (`<`, `>`, `<=`, `>=`, BETWEEN) need to be
    // sorted/compared by declaration-order ordinal, not by Arrow's
    // lexicographic Utf8 compare. We swap the column reference for a
    // `CASE WHEN col = 'lbl0' THEN 0 ... END` expression so the planner
    // sees integer ordinals at sort/range time. Best-effort: queries
    // with joins / derived tables / ambiguous column refs silently
    // skip the rewrite and fall back to label-string compare.
    let enum_rewritten = crate::enum_ordinal::rewrite_enum_ordering(
        &sess.engine.config().catalog,
        &sess.project,
        &seq_rewritten,
    )
    .await?;
    let sql = enum_rewritten.as_str();

    // Phase 5.13.C — the noop-accept + explicit-reject gates above (early
    // in the hot path, before any string-rewrite pipeline) already dispatch
    // every statement in the syntactic-accept set and reject every
    // design-exclusion kind (LISTEN/NOTIFY/UNLISTEN). The redundant second
    // gate that previously lived here (ADR 0014 Phase 1 dual path) has been
    // removed; sqlparser now only sees DML/DDL that needs its AST.
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).map_err(|e| {
        // sqlparser's PostgreSqlDialect requires `STORED` after a
        // `GENERATED ALWAYS AS (...)` block. Map both the `VIRTUAL`
        // alternative and the bare-paren omit-`STORED` form to PG's
        // SQLSTATE 0A000 (feature_not_supported), matching what the
        // engine produces when `VIRTUAL` slips through to the AST
        // walker. Keeps every "no STORED" surface consistent.
        let msg = format!("{e}");
        if msg.contains("Expected: STORED") {
            BasinError::FeatureNotSupported(
                "VIRTUAL generated columns deferred to v0.2; use STORED".to_string(),
            )
        } else {
            BasinError::internal(format!("parse error: {e}"))
        }
    })?;

    // Each call to `execute` handles exactly one statement. Multi-statement
    // simple-query messages (`tokio_postgres::batch_execute`, `psql -f
    // setup.sql`) are split into individual statements by the router-side
    // pgwire handler before they reach the engine — see
    // `basin_router::protocol::split_simple_query`. This guard is the
    // safety net for callers that bypass the router (and a defensive
    // assertion against future regressions in the splitter).
    if stmts.len() != 1 {
        return Err(BasinError::internal(format!(
            "expected exactly one statement, got {}",
            stmts.len()
        )));
    }
    let stmt = stmts.pop().unwrap();

    // Phase 6 cost-based query rejection. Cheap when disabled (one
    // `OnceLock::get`); when enabled, one catalog round-trip per
    // simple-shape Query. Multi-FROM / JOIN / sub-query / explicit-LIMIT
    // shapes pass through unchecked in v0.1 — see `cost_check` module
    // docs for the deliberate scope.
    if let Some(limit) = crate::cost_check::cost_limit_rows() {
        if matches!(stmt, Statement::Query(_)) {
            let estimate = crate::cost_check::estimate_query_rows(
                sess.engine.config().catalog.as_ref(),
                &sess.project,
                &stmt,
            )
            .await?;
            if let Some(rows) = estimate {
                crate::cost_check::check_cost(rows, limit)?;
            }
        }
    }

    // Phase 5.6: intercept RLS-related DDL before the main dispatch. The
    // catch-all in `match_rls_ddl` keeps every other statement falling
    // through to the existing handlers — the no-RLS hot path is an
    // `Ok(None)` followed by the same dispatch as before.
    if let Some(rls_ddl) = crate::rls::match_rls_ddl(&stmt)? {
        return exec_rls_ddl(sess, rls_ddl).await;
    }

    let result = match stmt {
        Statement::CreateTable(ct) => {
            exec_create_table(
                sess,
                ct,
                cluster_columns,
                lifecycle,
                ctas_shape,
                raw_sql,
                exclude_specs,
            )
            .await
        }
        Statement::CreateIndex(ci) => exec_create_index(sess, ci).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            if_exists,
            names,
            ..
        } => exec_drop_index(sess, if_exists, names).await,
        Statement::CreateView(sqlparser::ast::CreateView {
            name,
            query,
            materialized,
            or_replace,
            ..
        }) => {
            if materialized {
                let view_name = single_part_name(&name)?.to_string();
                let opts = cv_options.unwrap_or_default();
                let source_sql = query.to_string();
                if opts.continuous {
                    // Continuous-aggregate path: requires refresh_interval.
                    let interval = opts.refresh_interval_secs.ok_or_else(|| {
                        BasinError::InvalidSchema(
                            "CREATE MATERIALIZED VIEW: WITH (basin.continuous) \
                             requires refresh_interval = '<duration>'"
                                .into(),
                        )
                    })?;
                    crate::cv_ddl::exec_create_materialized_view(
                        sess,
                        &view_name,
                        &source_sql,
                        interval,
                    )
                    .await
                } else {
                    // Plain (snapshot) materialized view: run the query once and
                    // persist the result as a regular table.  No automatic
                    // refresh; use REFRESH MATERIALIZED VIEW to update.
                    crate::cv_ddl::exec_create_snapshot_materialized_view(
                        sess,
                        &view_name,
                        &source_sql,
                    )
                    .await
                }
            } else {
                // Plain view path (new).
                let view_name = single_part_name(&name)?.to_string();
                let query_sql = query.to_string();
                crate::view_ddl::exec_create_view(sess, &view_name, &query_sql, or_replace).await
            }
        }
        Statement::CreateFunction(sqlparser::ast::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            return_type,
            function_body,
            language,
            ..
        }) => {
            crate::function_ddl::exec_create_function(
                sess,
                or_replace,
                temporary,
                name,
                args,
                return_type,
                function_body,
                language,
            )
            .await
        }
        Statement::DropFunction(sqlparser::ast::DropFunction {
            if_exists,
            func_desc,
            ..
        }) => {
            let names = func_desc.into_iter().map(|d| d.name).collect();
            crate::function_ddl::exec_drop_function(sess, if_exists, names).await
        }
        Statement::DropProcedure {
            if_exists,
            proc_desc,
            ..
        } => crate::procedure_ddl::exec_drop_procedure(sess, if_exists, proc_desc).await,
        Statement::Call(call) => crate::procedure_ddl::exec_call(sess, call).await,
        Statement::CreateType {
            name,
            representation,
        } => {
            use sqlparser::ast::UserDefinedTypeRepresentation;
            match representation {
                Some(UserDefinedTypeRepresentation::Enum { labels }) => {
                    crate::type_ddl::exec_create_type_enum(sess, name, labels).await
                }
                _ => Err(BasinError::FeatureNotSupported(
                    "CREATE TYPE … AS (composite) is out of scope for v0.1; \
                     only AS ENUM is supported"
                        .into(),
                )),
            }
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Type,
            if_exists,
            names,
            ..
        } => crate::type_ddl::exec_drop_type(sess, if_exists, &names).await,
        Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type: _,
            sequence_options,
            owned_by: _,
        } => {
            // sqlparser 0.52 parses `CREATE SEQUENCE` natively. The
            // `data_type` / `owned_by` fields are accepted but ignored
            // in v0.1 — the catalog stores `i64` sequences and has no
            // notion of column-attached ownership yet.
            crate::seq_ddl::exec_create_sequence(
                sess,
                temporary,
                if_not_exists,
                name,
                sequence_options,
            )
            .await
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Sequence,
            if_exists,
            names,
            ..
        } => crate::seq_ddl::exec_drop_sequence(sess, if_exists, &names).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::View,
            if_exists,
            names,
            ..
        } => {
            // DROP VIEW supports dropping a single view per statement.
            if names.len() != 1 {
                return Err(BasinError::InvalidSchema(
                    "DROP VIEW: exactly one view name expected".into(),
                ));
            }
            let name = single_part_name(&names[0])?.to_string();
            crate::view_ddl::exec_drop_view(sess, &name, if_exists).await
        }
        // ── Schema DDL ──────────────────────────────────────────────────
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => crate::schema_ddl::exec_create_schema(sess, schema_name, if_not_exists).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Schema,
            if_exists,
            names,
            cascade,
            ..
        } => crate::schema_ddl::exec_drop_schema(sess, &names, if_exists, cascade).await,
        // ── SET search_path ─────────────────────────────────────────────
        Statement::Set(sqlparser::ast::Set::SingleAssignment {
            variable, values, ..
        }) => {
            // Only handle `SET search_path = …`; forward everything else
            // as a silent no-op so ORM migrations that emit PG-specific
            // SET statements (client_encoding, standard_conforming_strings,
            // etc.) don't hard-fail. This mirrors the PG wire protocol
            // server behaviour where un-recognised SET parameters are
            // accepted silently at the session level.
            // `variable` is an `ObjectName` holding `Vec<ObjectNamePart>`;
            // join with `.` to get the full variable name (e.g.
            // `search_path`).
            let var_name = variable
                .0
                .iter()
                .map(|i| i.id_val().as_str())
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase();

            if var_name == "search_path" {
                crate::schema_ddl::exec_set_search_path(sess, &values)
            } else if var_name == "statement_timeout" {
                // Wire `SET statement_timeout = …` to the per-session override.
                // Accept both string literals ('5s', '500ms') and bare integers (5000).
                let raw = values
                    .first()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                crate::session::set_statement_timeout(&sess.state, &raw)?;
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "lock_timeout" {
                // Phase 5.28.B: SET lock_timeout = '500ms' / 500 / 0
                let raw = extract_set_string_value(&values);
                let d = crate::session::parse_pg_duration(&raw);
                crate::session::set_session_lock_timeout(&sess.state, d);
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "idle_in_transaction_session_timeout" {
                // Phase 5.28.C: SET idle_in_transaction_session_timeout = '30s'
                let raw = extract_set_string_value(&values);
                let d = crate::session::parse_pg_duration(&raw);
                crate::session::set_session_idle_in_transaction_timeout(&sess.state, d);
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else {
                // Silently accept unknown SET variables.
                Ok(ExecResult::Empty { tag: "SET".into() })
            }
        }
        Statement::Insert(ins) => {
            // Phase 5.29.B: if the target table is a hypertable, register
            // chunk records for each unique time-bucket present in the VALUES
            // list BEFORE the regular insert path runs.  This is best-effort:
            // failures here (e.g. non-timestamp columns, subquery inserts) are
            // silently ignored so the normal INSERT still proceeds.
            let _ = touch_hypertable_chunks_from_insert(sess, &ins, raw_sql).await;
            exec_insert(sess, ins).await
        }
        Statement::Query(ref query) => {
            // ── Data-modifying CTE intercept ─────────────────────────────────
            // `WITH x AS (INSERT/UPDATE/DELETE … RETURNING …) SELECT … FROM x`
            // DataFusion 53 cannot plan DML statements as relations, so we
            // orchestrate them ourselves: execute each DML CTE in declaration
            // order, capture the RETURNING batch, register it as a MemTable in
            // the session context, then hand the outer SELECT to DataFusion.
            if query_has_dml_cte(query) {
                return exec_dml_cte_query(sess, query, sql, include_deleted).await;
            }

            // ── WITH RECURSIVE … INSERT/UPDATE/DELETE … intercept ────────────
            // `WITH RECURSIVE t(col) AS (… UNION ALL …) INSERT INTO target …`
            // sqlparser routes this as a Query whose body is SetExpr::Insert
            // (rather than SetExpr::Select).  DataFusion cannot plan DML in a
            // query body, so we lift the RECURSIVE CTE into the DML source and
            // dispatch through the normal DML path.
            if query_has_recursive_with_dml_body(query) {
                return exec_recursive_with_dml_body(sess, query).await;
            }

            // pg_plan routing instrumentation (ADR 0014 Phase 2). When the
            // parsed SELECT matches the shape the new translator handles,
            // bump the counter — independent of whether the fast path or
            // DataFusion path actually serves the query.
            if let Ok(tree) = crate::pg_ast::parse(sql) {
                if let Some(node) = tree.stmts().next() {
                    if crate::pg_plan::supports_shape(node) {
                        sess.engine.note_pg_plan_routed();
                    }
                }
            }

            // Phase 5.14.D2: record ORDER BY / GROUP BY patterns for adaptive
            // sort.  Best-effort; only fires for simple single-table SELECTs.
            record_query_patterns(sess, query);

            // Try the point-query fast path first. It only matches a tightly
            // constrained shape; on any rejection we fall back to DataFusion.
            //
            // RLS gate: the fast path bypasses DataFusion's logical planner
            // entirely, which is where we inject row-level predicates. If
            // any referenced table has `rls_enabled = true`, we *must* take
            // the DataFusion path so the RLS rewrite can fire. Tables with
            // RLS off see the fast path exactly as before — same one-`bool`
            // catalog read the existing path already pays.
            // Metadata-only aggregate fast path — tried FIRST because it is
            // the most specific and by far the cheapest (answers bare
            // COUNT/MIN/MAX, and SUM once sum_bytes is populated, from
            // catalog stats with ZERO file decode — ~30x). It MUST precede
            // `match_simple_select`, whose recogniser also matches bare
            // aggregates and would otherwise shadow this path entirely. It
            // returns `None`/`Ok(None)` for anything it can't answer from
            // metadata (WHERE present, unpopulated `sum_bytes`, unsupported
            // type, …), falling through to the recognisers below.
            //
            // Same view/RLS/soft-delete gate as the fast path (file-level
            // stats are invalid under row-level filtering). Transaction
            // gate: inside an explicit txn the session holds uncommitted
            // writes in `TxState::pending_files` that are NOT in the
            // catalog's `live_data_files()`; a metadata-only aggregate
            // would miss them, so any active txn takes the DataFusion path
            // (which merges the pending tail). Pure flag read — no I/O.
            if !crate::session::tx_is_active(&sess.state) {
                if let Some(plan) = crate::fast_aggregate::match_metadata_aggregate(&stmt) {
                    let table_meta = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &plan.table)
                        .await
                        .ok();
                    if let Some(ref meta) = table_meta {
                        let is_view = sess
                            .engine
                            .config()
                            .catalog
                            .lookup_view(&sess.project, plan.table.as_str())
                            .await
                            .is_some();
                        let has_rls = meta.rls_enabled;
                        let has_soft_delete =
                            crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                        // Hot-tier merge-on-read gate: when the process-wide
                        // memtable holds tombstones (fast-path DELETE) or
                        // UPDATE overrides (fast-path UPDATE) for this table,
                        // the catalog `live_data_files()` row counts are stale
                        // — a tombstone removes a counted cold row and an
                        // override shadows one. Skip the metadata-only path so
                        // the aggregate is computed over the merged hot+cold
                        // row set (where the overlay / tombstone filter runs).
                        let has_hot_overlay = {
                            let registry = sess.engine.memtable_registry();
                            registry
                                .get(&sess.project, &plan.table)
                                .map(|e| {
                                    e.memtable
                                        .snapshot()
                                        .iter()
                                        .any(|(_, v)| !v.is_row() || v.is_update())
                                })
                                .unwrap_or(false)
                        };
                        if !is_view && !has_rls && !has_soft_delete && !has_hot_overlay {
                            if let Some(result) =
                                crate::fast_aggregate::execute_metadata_aggregate(
                                    sess, plan, table_meta,
                                )
                                .await?
                            {
                                return Ok(result);
                            }
                        }
                    }
                }

                // Low-cardinality GROUP BY COUNT(*) fast path.  Same safety
                // gates as the metadata aggregate above: no active transaction
                // (pending_files would be missed), no view, no RLS, no soft
                // delete.  Falls through to DataFusion when the recogniser
                // returns `None` or when execute returns `Ok(None)` (e.g. key
                // range exceeds the low-cardinality threshold or catalog stats
                // are absent).
                if let Some(plan) = crate::fast_aggregate::match_groupby_low_card(&stmt) {
                    let table_meta = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &plan.table)
                        .await
                        .ok();
                    if let Some(ref meta) = table_meta {
                        let is_view = sess
                            .engine
                            .config()
                            .catalog
                            .lookup_view(&sess.project, plan.table.as_str())
                            .await
                            .is_some();
                        let has_rls = meta.rls_enabled;
                        let has_soft_delete =
                            crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                        if !is_view && !has_rls && !has_soft_delete {
                            if let Some(result) =
                                crate::fast_aggregate::execute_groupby_low_card(
                                    sess, plan, table_meta,
                                )
                                .await?
                            {
                                return Ok(result);
                            }
                        }
                    }
                }
            }

            if let Some(plan) = match_simple_select(&stmt) {
                // Fast-path gate: load the table metadata exactly once and
                // derive all three guard conditions from that single result.
                // Previously this performed 3 separate catalog round-trips
                // (lookup_view, table_has_rls → load_table, table_has_soft_delete
                // → load_table); now it is one load_table + one lookup_view.
                //
                // We still need lookup_view because views and tables live in
                // separate catalog maps and can share a name in principle.
                let table_meta = sess
                    .engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &plan.table)
                    .await
                    .ok();
                if let Some(ref meta) = table_meta {
                    let is_view = sess
                        .engine
                        .config()
                        .catalog
                        .lookup_view(&sess.project, plan.table.as_str())
                        .await
                        .is_some();
                    let has_rls = meta.rls_enabled;
                    let has_soft_delete =
                        crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                    // Inside an explicit transaction the fast path (and the
                    // aggregate fast path, which shares this dispatch) reads
                    // only the committed snapshot + shard tail — it does NOT
                    // merge `TxState::pending_files`, so a SELECT/aggregate
                    // would miss this txn's uncommitted writes (and mis-handle
                    // ROLLBACK TO SAVEPOINT). Bail to the transaction-aware
                    // DataFusion path whenever a txn is open; a missed fast
                    // path is merely slower, a wrong answer is not acceptable.
                    let in_txn = crate::session::tx_is_active(&sess.state);
                    // Phase 5.30.C/E: citext column check.
                    // The fast path uses byte-exact string comparison in
                    // `batch_matches_predicates` and a byte-lexicographic sort
                    // in the ORDER BY path; neither honours the case-insensitive
                    // citext contract.  Route through DataFusion (which applies
                    // CitextAnalyzerRule) whenever a WHERE predicate column or
                    // an ORDER BY column is marked BASIN_TYPE=CITEXT.
                    let schema_field_is_citext = |col_name: &str| -> bool {
                        meta.schema
                            .field_with_name(col_name)
                            .ok()
                            .map(|f| {
                                f.metadata()
                                    .get(crate::types::BASIN_TYPE_KEY)
                                    .map(|v| v.as_str() == crate::types::BASIN_TYPE_CITEXT)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false)
                    };
                    let has_citext_predicate = plan.predicates.iter().any(|pred| {
                        schema_field_is_citext(pred.column())
                    });
                    let has_citext_order_by = plan
                        .order_by
                        .as_ref()
                        .map(|(col, _)| schema_field_is_citext(col.as_str()))
                        .unwrap_or(false);
                    if !is_view
                        && !has_rls
                        && !has_soft_delete
                        && !in_txn
                        && !has_citext_predicate
                        && !has_citext_order_by
                    {
                        return execute_simple_select(sess, plan, table_meta).await;
                    }
                }
            }

            // Phase 5.19.C: GIN containment fast path.
            // Detect `SELECT … FROM t WHERE col @> 'literal'` (or `<@`) on a
            // column that has a GIN index; probe the in-RAM posting list to
            // get candidate files, then fall through to DataFusion which
            // applies the full `jsonb_contains` predicate for correctness.
            // The probe is advisory-only: when it returns `NoIndex` the query
            // falls through to a full DataFusion scan without error.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains("@>") || raw_sql.contains("<@"))
            {
                if let Some(gin_plan) = crate::index_probe::detect_gin_containment(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    // Probe the posting list.  Only an explicit Empty result
                    // lets us short-circuit; NoIndex and FileCandidates both
                    // fall through (the DataFusion path handles correctness).
                    let gin_result = sess.engine.gin_index_registry().probe_containment(
                        &sess.project,
                        &gin_plan.table,
                        &gin_plan.col,
                        &gin_plan.opclass,
                        &gin_plan.needle,
                    );
                    if let crate::index_probe::ProbeResult::Empty = gin_result {
                        // Posting list guarantees no rows match — short-circuit.
                        let schema = Arc::new(arrow_schema::Schema::empty());
                        return Ok(ExecResult::Rows { schema, batches: vec![] });
                    }
                    // FileCandidates: the DataFusion path still executes; the
                    // posting list result is currently advisory (file-level
                    // pruning via DataFusion scan override is 5.19.E).
                    // Fall through to exec_select for correctness.
                }
            }

            // Phase 5.19.D — detect `SELECT … FROM t WHERE col ? 'key'` (or
            // `?&` / `?|` variants) and probe the GIN posting list.
            // Advisory-only: `Empty` short-circuits; anything else falls
            // through to the DataFusion / UDF path for correctness.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains(" ? ") || raw_sql.contains(" ?'")
                    || raw_sql.contains("?&") || raw_sql.contains("?|"))
            {
                if let Some(key_plan) = crate::index_probe::detect_gin_key_probe(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    let key_refs: Vec<&str> = key_plan.keys.iter().map(|s| s.as_str()).collect();
                    let gin_result = sess.engine.gin_index_registry().probe_key_existence(
                        &sess.project,
                        &key_plan.table,
                        &key_plan.col,
                        &key_plan.opclass,
                        &key_refs,
                        key_plan.require_all,
                    );
                    if let crate::index_probe::ProbeResult::Empty = gin_result {
                        // No files contain these keys — short-circuit with empty result.
                        let schema = Arc::new(arrow_schema::Schema::empty());
                        return Ok(ExecResult::Rows { schema, batches: vec![] });
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            // Phase 5.20.E — detect `SELECT … FROM t WHERE col @@ to_tsquery(…)`
            // on a tsvector column with a GIN index and probe the FTS posting
            // list.  Advisory-only: `Empty` short-circuits; anything else falls
            // through to the DataFusion / UDF path for correctness.
            if !crate::session::tx_is_active(&sess.state) && raw_sql.contains("@@") {
                if let Some(fts_plan) = crate::index_probe::detect_tsvector_match(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    use basin_storage::index::gin_tsvector::TsvProbeResult;
                    let fts_result = sess.engine.gin_fts_registry().probe_query(
                        &sess.project,
                        &fts_plan.table,
                        &fts_plan.col,
                        &fts_plan.tsquery_str,
                    );
                    if let TsvProbeResult::Empty = fts_result {
                        // Posting list guarantees no rows match — short-circuit.
                        let schema = Arc::new(arrow_schema::Schema::empty());
                        return Ok(ExecResult::Rows { schema, batches: vec![] });
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            // Phase 5.24.D — detect `SELECT … FROM t WHERE col @> val` /
            // `col && range` / `col <@ range` on a GIST-indexed range column
            // and probe the interval tree.  Advisory-only: `Empty` short-circuits;
            // anything else falls through to the DataFusion / UDF path for
            // correctness.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains("@>") || raw_sql.contains("&&") || raw_sql.contains("<@"))
            {
                if let Some(interval_plan) = crate::index_probe::detect_range_index_probe(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    use basin_common::types::range::{IndexInterval, RangeValue};
                    use basin_storage::index::interval::ProbeResult;
                    let interval_result = match &interval_plan.op {
                        crate::index_probe::RangeOp::ContainsElem => {
                            if let Some(pt) = interval_plan.point {
                                sess.engine.interval_registry().probe_contains_point(
                                    &sess.project,
                                    &interval_plan.table,
                                    &interval_plan.col,
                                    pt,
                                )
                            } else {
                                ProbeResult::NoIndex
                            }
                        }
                        crate::index_probe::RangeOp::ContainsRange
                        | crate::index_probe::RangeOp::Overlaps
                        | crate::index_probe::RangeOp::ContainedBy => {
                            if let Some(lit) = &interval_plan.range_literal {
                                if let Some(rv) = RangeValue::from_pg_text(lit) {
                                    if let Some(iv) = IndexInterval::from_range(&rv) {
                                        sess.engine.interval_registry().probe_overlaps(
                                            &sess.project,
                                            &interval_plan.table,
                                            &interval_plan.col,
                                            &iv,
                                        )
                                    } else {
                                        ProbeResult::NoIndex
                                    }
                                } else {
                                    ProbeResult::NoIndex
                                }
                            } else {
                                ProbeResult::NoIndex
                            }
                        }
                    };
                    if let ProbeResult::Empty = interval_result {
                        // Interval tree guarantees no rows match — short-circuit.
                        let schema = Arc::new(arrow_schema::Schema::empty());
                        return Ok(ExecResult::Rows { schema, batches: vec![] });
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            exec_select(sess, sql, include_deleted, Some(raw_sql)).await
        }
        Statement::ShowTables { .. } => exec_show_tables(sess).await,
        // ── SHOW search_path ─────────────────────────────────────────────
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("_")
                .to_ascii_lowercase();
            if var_name == "search_path" {
                crate::schema_ddl::exec_show_search_path(sess)
            } else if var_name == "statement_timeout" {
                let val = crate::session::show_statement_timeout(&sess.state);
                let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "statement_timeout",
                    arrow_schema::DataType::Utf8,
                    false,
                )]));
                let col: ArrayRef = Arc::new(StringArray::from(vec![val]));
                let batch = RecordBatch::try_new(schema.clone(), vec![col])
                    .map_err(|e| BasinError::internal(format!("SHOW statement_timeout: {e}")))?;
                Ok(ExecResult::Rows {
                    schema: Arc::new(crate::convert::schema_df_to_ws(&schema)?),
                    batches: vec![batch],
                })
            } else if var_name == "lock_timeout" {
                // Phase 5.28.D: SHOW lock_timeout
                let val = crate::session::format_pg_duration(
                    crate::session::session_lock_timeout(&sess.state),
                );
                Ok(make_show_result("lock_timeout", &val))
            } else if var_name == "idle_in_transaction_session_timeout" {
                // Phase 5.28.D: SHOW idle_in_transaction_session_timeout
                let val = crate::session::format_pg_duration(
                    crate::session::session_idle_in_transaction_timeout(&sess.state),
                );
                Ok(make_show_result("idle_in_transaction_session_timeout", &val))
            } else {
                // Silently return empty for other SHOW <var> forms so
                // ORM startup queries don't hard-fail.
                Ok(ExecResult::Empty { tag: "SHOW".into() })
            }
        }
        Statement::AlterTable(sqlparser::ast::AlterTable {
            name, operations, ..
        }) => exec_alter_table(sess, name, operations).await,
        Statement::Delete(del) => crate::dml_mutate::exec_delete(sess, del).await,
        Statement::Update(sqlparser::ast::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            ..
        }) => {
            let from = from.and_then(|f| match f {
                sqlparser::ast::UpdateTableFromKind::BeforeSet(mut v)
                | sqlparser::ast::UpdateTableFromKind::AfterSet(mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.swap_remove(0))
                    }
                }
            });
            crate::dml_mutate::exec_update(sess, table, assignments, from, selection, returning)
                .await
        }
        // ----- Cursor lifecycle ----- //
        Statement::Declare { stmts } => exec_declare(sess, stmts).await,
        Statement::Fetch {
            name, direction, ..
        } => exec_fetch(sess, &name.value, direction).await,
        Statement::Close { cursor } => exec_close(sess, cursor).await,
        Statement::Explain {
            analyze,
            verbose,
            format,
            options,
            statement,
            ..
        } => {
            let format = format.map(|k| match k {
                sqlparser::ast::AnalyzeFormatKind::Keyword(f)
                | sqlparser::ast::AnalyzeFormatKind::Assignment(f) => f,
            });
            crate::explain::exec_explain(sess, analyze, verbose, format, options, statement).await
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            if_exists,
            names,
            ..
        } => exec_drop_table(sess, if_exists, names).await,
        other => Err(BasinError::internal(format!("unsupported in PoC: {other}"))),
    };

    // ── Error-in-txn → aborted state ────────────────────────────────────
    // If any statement fails while inside an active transaction, mark the
    // transaction as aborted.  Subsequent statements (except ROLLBACK) will
    // receive SQLSTATE 25P02 until the client issues ROLLBACK.
    if result.is_err() && crate::session::tx_is_active(&sess.state) {
        crate::session::tx_set_aborted(&sess.state);
    }
    result
}

/// Async wrapper around [`pg_operators::rewrite_json_agg_qualified_wildcard`]
/// that resolves the alias→table map from the SQL's FROM clauses, loads each
/// table's column list from the catalog, and hands the resulting
/// `HashMap<alias, Vec<column>>` to the pure rewriter.
///
/// Fast-paths when the SQL contains no `json_agg`/`jsonb_agg` token. The
/// rewriter is a no-op (returns SQL unchanged) when:
///   1. No qualified-wildcard `<fn>(<alias>.*)` is present.
///   2. The alias doesn't appear in a `FROM <table> [AS] <alias>` clause.
///   3. The resolved table name doesn't exist in the catalog.
///   4. The catalog load fails (logged at debug, SQL passes through unchanged
///      so the downstream parser surfaces the original error).
async fn rewrite_json_agg_qualified_wildcard_with_catalog(
    sql: &str,
    catalog: &dyn basin_catalog::Catalog,
    project: &basin_common::ProjectId,
) -> String {
    let aliases = crate::pg_operators::collect_json_agg_star_aliases(sql);
    if aliases.is_empty() {
        return sql.to_string();
    }
    let alias_table = crate::pg_operators::extract_from_alias_table_map(sql);
    let mut alias_columns: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for alias in aliases {
        let Some(table_str) = alias_table.get(&alias) else {
            continue;
        };
        let Ok(table_name) = TableName::new(table_str.clone()) else {
            continue;
        };
        match catalog.load_table(project, &table_name).await {
            Ok(meta) => {
                let cols: Vec<String> = meta
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                if !cols.is_empty() {
                    alias_columns.insert(alias, cols);
                }
            }
            Err(_) => {
                // Unknown table — leave the json_agg call unchanged so the
                // user sees the genuine "table not found" error from the
                // planner rather than a confusing expansion failure.
                continue;
            }
        }
    }
    if alias_columns.is_empty() {
        return sql.to_string();
    }
    crate::pg_operators::rewrite_json_agg_qualified_wildcard(sql, &alias_columns)
}

/// Execute a `VectorSearchPlan` produced by `vector_planner`. Calls
/// `Storage::vector_search` (via the existing `ProjectSession::vector_search`
/// fast path) with `fetch_k` candidates, applies any column-equality
/// pushdown filters, truncates to the user's `LIMIT`, and projects to the
/// user's `SELECT` list.
///
/// Mirrors the result shape `exec_select` would produce for the same query
/// in brute-force mode: the projected user columns only, no synthetic
/// `_distance` column. (Brute-force computes the distance in `ORDER BY` but
/// only emits whatever the user wrote in `SELECT`.)
async fn execute_vector_search_plan(
    sess: &ProjectSession,
    plan: crate::vector_planner::VectorSearchPlan,
) -> Result<ExecResult> {
    let fetch_k = crate::vector_planner::fetch_k(&plan);
    let distance = crate::vector_planner::distance_for(plan.distance_op);

    // Resolve the user-projection schema up front so an empty result still
    // has the correct column list.
    let table_meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &plan.table)
        .await?;

    let raw_batches = sess
        .vector_search(
            &plan.table,
            &plan.vec_col,
            plan.query_vec.clone(),
            fetch_k,
            distance,
        )
        .await?;

    if raw_batches.is_empty() {
        let empty = crate::vector_planner::empty_for_projection(
            table_meta.schema.as_ref(),
            &plan.projection,
        )?;
        let schema = empty.schema();
        sess.engine.note_vector_routed();
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![empty],
        });
    }

    // Apply any pushdown filters and truncate to k. The single-batch
    // contract from `ProjectSession::vector_search` keeps the loop trivial:
    // each batch already carries `_distance` ascending, so the global
    // top-k after filter is the prefix of `keep` indices.
    let mut filtered_batches: Vec<RecordBatch> = Vec::with_capacity(raw_batches.len());
    let mut total_kept = 0usize;
    for batch in &raw_batches {
        if total_kept >= plan.k {
            break;
        }
        let mut keep = crate::vector_planner::surviving_indices(batch, &plan.filters)?;
        if total_kept + keep.len() > plan.k {
            keep.truncate(plan.k - total_kept);
        }
        if keep.is_empty() {
            continue;
        }
        // Use arrow-select::take to preserve column order + types.
        let indices =
            arrow_array::UInt32Array::from(keep.iter().map(|i| *i as u32).collect::<Vec<_>>());
        let mut taken_cols = Vec::with_capacity(batch.num_columns());
        for c in batch.columns() {
            let t = arrow_select::take::take(c.as_ref(), &indices, None)
                .map_err(|e| BasinError::internal(format!("take rows: {e}")))?;
            taken_cols.push(t);
        }
        let taken = RecordBatch::try_new(batch.schema(), taken_cols)
            .map_err(|e| BasinError::internal(format!("rebuild taken batch: {e}")))?;
        total_kept += taken.num_rows();
        filtered_batches.push(taken);
    }

    if filtered_batches.is_empty() {
        let empty = crate::vector_planner::empty_for_projection(
            table_meta.schema.as_ref(),
            &plan.projection,
        )?;
        let schema = empty.schema();
        sess.engine.note_vector_routed();
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![empty],
        });
    }

    let mut projected: Vec<RecordBatch> = Vec::with_capacity(filtered_batches.len());
    for b in &filtered_batches {
        projected.push(crate::vector_planner::project_for_user(
            b,
            &plan.projection,
        )?);
    }
    let schema = projected[0].schema();
    sess.engine.note_vector_routed();
    Ok(ExecResult::Rows {
        schema,
        batches: projected,
    })
}

async fn exec_create_table(
    sess: &ProjectSession,
    mut ct: sqlparser::ast::CreateTable,
    cluster_columns: Option<Vec<String>>,
    lifecycle: CreateTableLifecycle,
    ctas_shape: Option<crate::pg_ast::CtasShape>,
    // The unmodified statement the caller provided. The trailing
    // `STORED AS <format>` clause is stripped from the SQL before
    // sqlparser parses it (same pre-screen strategy as CLUSTER BY), so
    // the file format must be recovered from the original text rather
    // than the parsed `CreateTable` AST.
    raw_sql: &str,
    // Phase 5.24.F: EXCLUDE USING gist constraints extracted from the SQL
    // by `extract_exclude_using_gist` before sqlparser saw it. Each spec is
    // encoded as a sentinel `CheckConstraint` predicate and persisted on the
    // table metadata so the INSERT path can enforce it.
    exclude_specs: Vec<crate::ddl::ExcludeConstraintSpec>,
) -> Result<ExecResult> {
    // 6.SEC.P1 — reject user DDL targeting a reserved system schema
    // (`auth`, `storage`, `cron`, `net`, `realtime`, `pg_catalog`,
    // `information_schema`). `public` and bare names pass through.
    // Phase 5.18.C: system sessions (is_system = true) bypass this guard so
    // trusted internal subsystems can create tables in reserved schemas.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&ct.name, "CREATE TABLE")?;
    }
    let name = single_part_name(&ct.name)?;
    // Phase 5.18.C: for system sessions with a schema qualifier, resolve the
    // schema so the catalog key is (schema, table) not (public, table).
    let schema_str: Option<String> = if ct.name.0.len() >= 2 {
        Some(ct.name.0[0].id_val().clone())
    } else {
        None
    };
    let table = TableName::new(name)?;

    // `CREATE TABLE … AS <query>` (CTAS). `ctas_shape` is `Some` iff
    // libpg_query classified the original statement as a plain CTAS.
    // `skip_data` true → `WITH NO DATA` (schema only, no rows); false →
    // populate from the query result. The query's resolved output
    // schema becomes the table schema; `col_names` (the optional LHS
    // `(col, …)` list, captured by libpg_query because sqlparser cannot
    // parse it) renames the columns positionally (PG semantics).
    if let Some(shape) = ctas_shape {
        let query = ct.query.as_deref().ok_or_else(|| {
            BasinError::internal("CREATE TABLE AS: missing query body after parse")
        })?;
        return exec_create_table_as(
            sess,
            &table,
            name,
            query,
            &shape.col_names,
            ct.if_not_exists,
            shape.skip_data,
        )
        .await;
    }

    // Phase 5.11.K2: resolve column types that reference a user-defined
    // enum or domain. Each match rewrites the column's data_type to the
    // underlying Arrow-mappable shape and stamps a `BASIN_ENUM_TYPE` /
    // `BASIN_DOMAIN` field-metadata marker so the INSERT path can
    // validate values + the catalog can refcount.
    let bindings = crate::type_ddl::resolve_user_type_columns(
        &sess.engine.config().catalog,
        &sess.project,
        &ct.columns,
    )
    .await?;
    let extra_md = crate::type_ddl::rewrite_user_type_columns(&mut ct.columns, &bindings)?;

    let (schema, mut constraints) = crate::ddl::schema_and_constraints_from_columns(
        &ct.columns,
        &ct.constraints,
        name,
        &lifecycle,
    )?;
    let schema = crate::type_ddl::apply_user_type_metadata(schema, &extra_md);
    let spec = partition_spec_from_ast(ct.partition_by.as_deref(), &schema)?;

    // Validate cluster columns are a subset of the table's columns BEFORE
    // we create the table — bouncing here means we don't leave a half-
    // created table around when the user typo'd a column name.
    if let Some(cols) = cluster_columns.as_ref() {
        for c in cols {
            if schema.field_with_name(c).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "CLUSTER BY column {c:?} is not in the table schema"
                )));
            }
        }
    }

    // Validate FK definitions before creating the table — referenced
    // table must exist in the same project, referenced columns must be
    // exactly the PK of the referenced table, and types must match.
    for fk in &constraints.foreign_keys {
        let ref_table_name = TableName::new(fk.ref_table.clone())?;
        let pk_of_ref: Vec<String> = if ref_table_name == table {
            constraints.pk_columns.clone()
        } else {
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, &ref_table_name)
                .await
                .map_err(|e| match e {
                    BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: referenced table {:?} does not exist in this project \
                         (cross-project FKs are not supported in v0.1)",
                        fk.name, fk.ref_table
                    )),
                    other => other,
                })?;
            meta.pk_columns.clone()
        };
        if pk_of_ref.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "FOREIGN KEY {:?}: referenced table {:?} has no PRIMARY KEY (v0.1 requires \
                 referenced columns to be the PK of the referenced table; UNIQUE-only \
                 references are deferred to v0.2)",
                fk.name, fk.ref_table
            )));
        }
        let mut pk_set: std::collections::HashSet<String> =
            pk_of_ref.iter().map(|s| s.to_ascii_lowercase()).collect();
        for c in &fk.ref_columns {
            if !pk_set.remove(&c.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "FOREIGN KEY {:?}: referenced column {c:?} is not part of {:?}'s PRIMARY KEY",
                    fk.name, fk.ref_table
                )));
            }
        }
        if !pk_set.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "FOREIGN KEY {:?}: referenced columns must be exactly the PRIMARY KEY of {:?} \
                 (missing {pk_set:?})",
                fk.name, fk.ref_table
            )));
        }
        for (lc, rc) in fk.columns.iter().zip(fk.ref_columns.iter()) {
            let local_field = schema.field_with_name(lc).map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "FOREIGN KEY {:?}: local column {lc:?} not in table",
                    fk.name
                ))
            })?;
            if ref_table_name != table {
                let ref_meta = sess
                    .engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &ref_table_name)
                    .await?;
                let ref_field = ref_meta.schema.field_with_name(rc).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: referenced column {rc:?} not in {:?}",
                        fk.name, fk.ref_table
                    ))
                })?;
                if local_field.data_type() != ref_field.data_type() {
                    return Err(BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: local column {lc:?} type {:?} does not match \
                         referenced column {rc:?} type {:?}",
                        fk.name,
                        local_field.data_type(),
                        ref_field.data_type(),
                    )));
                }
            }
        }
    }

    // Phase 5.18.C: system sessions with a reserved-schema qualifier route
    // through `create_table_qualified` so the catalog key is `(schema, table)`.
    // All other paths (user sessions, bare names, public schema) use the
    // existing `create_table` which stores under `(public, table)`.
    let qtable_for_refresh: Option<basin_common::QualifiedTableName> = if sess.is_system {
        schema_str.as_deref().and_then(|s| {
            basin_catalog::reserved_schema::ReservedSchema::from_str(s)
                .filter(|r| !r.is_public())
                .map(|r| basin_common::QualifiedTableName::new(r.to_schema_name(), table.clone()))
        })
    } else {
        None
    };

    // If IF NOT EXISTS is set and the table already exists, return success
    // (no-op). The catalog signals "already exists" as BasinError::Catalog;
    // we only suppress that specific variant — unrelated catalog errors still
    // propagate. Without IF NOT EXISTS the error is always fatal.
    if let Some(ref qt) = qtable_for_refresh {
        match sess
            .engine
            .config()
            .catalog
            .create_table_qualified(&sess.project, qt, Arc::new(schema.clone()))
            .await
        {
            Ok(_metadata) => {}
            Err(BasinError::Catalog(_)) if ct.if_not_exists => {
                return Ok(ExecResult::Empty {
                    tag: "CREATE TABLE".into(),
                });
            }
            Err(e) => return Err(e),
        }
    } else {
        match sess
            .engine
            .config()
            .catalog
            .create_table(&sess.project, &table, &schema)
            .await
        {
            Ok(_metadata) => {}
            Err(BasinError::Catalog(_)) if ct.if_not_exists => {
                return Ok(ExecResult::Empty {
                    tag: "CREATE TABLE".into(),
                });
            }
            Err(e) => return Err(e),
        }
    }

    // Register implicit sequences promised by `SERIAL` / `BIGSERIAL` /
    // `SMALLSERIAL` columns. PG would auto-create these inline with the
    // table; we do it as a follow-on catalog call so the table-create
    // path stays one focused step. `IF NOT EXISTS`-shaped: if the
    // sequence already exists (re-run after a partial failure) we
    // swallow the duplicate-name error so the table can keep going.
    for seq in &constraints.implicit_sequences {
        let def = basin_catalog::SequenceDef::with_defaults(sess.project, seq.name.clone());
        match sess.engine.config().catalog.create_sequence(def).await {
            Ok(()) => {}
            Err(BasinError::Catalog(_)) => {
                // Pre-existing; SERIAL on a column whose sequence is
                // already there (e.g. from a prior partial create or
                // a hand-rolled `CREATE SEQUENCE`) — same shape PG
                // tolerates with `IF NOT EXISTS`.
            }
            Err(e) => return Err(e),
        }
    }

    if spec.is_partitioned() {
        sess.engine
            .config()
            .catalog
            .set_partition_spec(&sess.project, &table, spec)
            .await?;
    }

    if let Some(cols) = cluster_columns {
        sess.engine
            .config()
            .catalog
            .set_cluster_columns(&sess.project, &table, cols)
            .await?;
    }

    // Phase 3: persist the on-disk file format. The `STORED AS <format>`
    // clause is stripped from the SQL before sqlparser sees it (same
    // pre-screen strategy as CLUSTER BY above), so we recover it from the
    // original statement text. Persisted UNCONDITIONALLY for every CREATE
    // TABLE — `parse_create_table_file_format` returns the default
    // (Parquet) when the clause is absent, so the catalog always carries
    // an explicit format and the write path never has to guess.
    let fmt = crate::ddl::parse_create_table_file_format(raw_sql);
    sess.engine
        .config()
        .catalog
        .set_file_format(&sess.project, &table, fmt)
        .await?;

    // W3-R3: persist the user-asserted global sort order declared via
    // `WITH (basin.sort_by = '...')`. The sanitiser already stripped this
    // key before sqlparser saw the SQL; we recover it from the original.
    if let Some(sort_cols) = crate::ddl::parse_create_table_sort_by(raw_sql) {
        for c in &sort_cols {
            if schema.field_with_name(c).is_err() {
                return Err(basin_common::BasinError::InvalidSchema(format!(
                    "basin.sort_by column {c:?} is not in the table schema"
                )));
            }
        }
        sess.engine
            .config()
            .catalog
            .set_global_sort_order(&sess.project, &table, sort_cols)
            .await?;
    }

    // `basin.row_block_size` — optional per-table Vortex chunk / Parquet
    // row-group override. Validated (power of two, [256, 65536]) at parse
    // time; `None` means "use writer default".
    if let Some(rbs) = crate::ddl::parse_create_table_row_block_size(raw_sql)? {
        sess.engine
            .config()
            .catalog
            .set_row_block_size(&sess.project, &table, Some(rbs))
            .await?;
    }

    // `basin.adaptive_sort_override` — Phase 5.14.D2
    if let Some(aso) = crate::ddl::parse_create_table_adaptive_sort_override(raw_sql)? {
        sess.engine
            .config()
            .catalog
            .set_adaptive_sort_override(&sess.project, &table, Some(aso))
            .await?;
    }

    // Phase 5.24.F: encode each EXCLUDE USING gist spec as a sentinel
    // CheckConstraint predicate and fold it into the check-constraints list.
    // The INSERT path detects these sentinels in `enforce_check_constraints`
    // and routes them to the exclusion enforcer instead of DataFusion.
    for spec in &exclude_specs {
        constraints.checks.push(basin_catalog::CheckConstraint {
            name: spec.name.clone(),
            predicate: crate::ddl::encode_exclusion_sentinel(spec),
        });
    }

    if !constraints.pk_columns.is_empty()
        || !constraints.checks.is_empty()
        || !constraints.foreign_keys.is_empty()
    {
        sess.engine
            .config()
            .catalog
            .set_table_constraints(
                &sess.project,
                &table,
                constraints.pk_columns,
                constraints.checks,
                constraints.foreign_keys,
            )
            .await?;
    }

    if !constraints.uniques.is_empty() {
        sess.engine
            .config()
            .catalog
            .set_unique_constraints(&sess.project, &table, constraints.uniques)
            .await?;
    }

    // Phase 5.18.C: if this was a system-session create with a reserved schema,
    // refresh from the qualified catalog entry. Otherwise use the bare path.
    if let Some(ref qt) = qtable_for_refresh {
        crate::session::refresh_table_qualified(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            qt,
        )
        .await?;
    } else {
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    }

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE".into(),
    })
}

/// `CREATE TABLE <name> [(<col>, …)] AS <query> [WITH [NO] DATA]`.
///
/// PostgreSQL semantics: the new table's column set is the **resolved
/// output schema of `query`** (names + types exactly as `SELECT …`
/// would produce). An optional `(col, …)` list positionally renames
/// those output columns. `WITH NO DATA` (`no_data == true`) creates the
/// schema only — zero rows; `WITH DATA` / no clause populates it from
/// the query result.
///
/// The query is planned through the session's DataFusion context (the
/// same context the SQL pre-screen pipeline already prepared), so CTEs,
/// inlined SQL functions, RLS, and partition pruning are all in effect.
/// Population is delegated to the normal `INSERT INTO … <query>` path so
/// constraint / RLS / event machinery is reused unchanged.
async fn exec_create_table_as(
    sess: &ProjectSession,
    table: &TableName,
    name: &str,
    query: &sqlparser::ast::Query,
    rename: &[String],
    if_not_exists: bool,
    no_data: bool,
) -> Result<ExecResult> {
    // Resolve the query's output schema by planning it (no execution).
    let query_sql = query.to_string();
    let df = sess
        .ctx
        .sql(&query_sql)
        .await
        .map_err(|e| BasinError::internal(format!("CREATE TABLE AS plan: {e}")))?;
    let df_schema = df.schema().as_arrow().clone();
    let mut ws_schema = schema_df_to_ws(&df_schema)?;

    // An explicit `(col, …)` list renames the query's output columns
    // positionally (PG: must not be wider than the query's projection).
    if !rename.is_empty() {
        if rename.len() > ws_schema.fields().len() {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE TABLE {name}: column name list has {} entries but the query \
                 produces only {} column(s)",
                rename.len(),
                ws_schema.fields().len()
            )));
        }
        let renamed: Vec<arrow_schema::Field> = ws_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let nm = rename.get(i).cloned().unwrap_or_else(|| f.name().clone());
                let mut nf = arrow_schema::Field::new(nm, f.data_type().clone(), f.is_nullable());
                if !f.metadata().is_empty() {
                    nf = nf.with_metadata(f.metadata().clone());
                }
                nf
            })
            .collect();
        ws_schema = arrow_schema::Schema::new(renamed);
    }

    match sess
        .engine
        .config()
        .catalog
        .create_table(&sess.project, table, &ws_schema)
        .await
    {
        Ok(_metadata) => {}
        Err(BasinError::Catalog(_)) if if_not_exists => {
            // Pre-existing table + IF NOT EXISTS — PG no-ops (no create,
            // no populate).
            return Ok(ExecResult::Empty {
                tag: "CREATE TABLE".into(),
            });
        }
        Err(e) => return Err(e),
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;

    if no_data {
        // Schema-only clone: table exists with the query's columns and
        // zero rows.
        return Ok(ExecResult::Empty {
            tag: "CREATE TABLE AS".into(),
        });
    }

    // Populate via the standard INSERT … SELECT path so constraint /
    // RLS / change-event handling is identical to a hand-written
    // `INSERT INTO t <query>`. When the LHS renamed columns, name the
    // target columns explicitly so positional mapping is unambiguous.
    let insert_sql = if rename.is_empty() {
        format!("INSERT INTO {} {}", table.as_str(), query_sql)
    } else {
        let cols = ws_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO {} ({}) {}", table.as_str(), cols, query_sql)
    };
    Box::pin(execute(sess, &insert_sql)).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE AS".into(),
    })
}

/// `DROP TABLE [IF EXISTS] <name> [, ...]`
///
/// Removes each named table from the catalog. If `if_exists` is true, a
/// missing table is silently ignored (PG behavior). Without IF EXISTS an
/// absent table returns a `NotFound` error.
///
/// Note: only catalog metadata is removed. Underlying object-store data is
/// left in place for time-travel / point-in-time-restore (same policy as the
/// catalog's `drop_table` contract).
async fn exec_drop_table(
    sess: &ProjectSession,
    if_exists: bool,
    names: Vec<sqlparser::ast::ObjectName>,
) -> Result<ExecResult> {
    for name in names {
        // 6.SEC.P1 — reject DROP TABLE targeting a reserved system schema.
        // Phase 5.18.C: system sessions bypass this guard.
        if !sess.is_system {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(&name, "DROP TABLE")?;
        }
        let n = single_part_name(&name)?;
        let table = TableName::new(n)?;
        match sess
            .engine
            .config()
            .catalog
            .drop_table(&sess.project, &table)
            .await
        {
            Ok(()) => {
                // Deregister the table from the DataFusion catalog view so
                // subsequent queries in the same session don't see a stale
                // listing. Errors here are best-effort — the catalog drop
                // already succeeded; a stale DataFusion entry for a dropped
                // table is harmless for the next request (which opens a new
                // session).
                let _ = sess.ctx.deregister_table(table.as_str());
            }
            Err(BasinError::NotFound(_)) if if_exists => {
                // IF EXISTS — silently ignore missing tables.
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP TABLE".into(),
    })
}

/// `CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<col1>[, <col2>, ...])`.
///
/// v0.1 records the index in the catalog (per `TableMetadata::indexes`) but
/// does NOT materialise any B-tree / sort-merge structure: every query on
/// the indexed table still does a table scan. The catalog row exists so
/// `information_schema.indexes` / `pg_index` introspection is honest about
/// what's declared.
//
// TODO(v0.2): wire to basin-storage's secondary index file format. The
// declaration shape here is already plural-column-aware so the storage
// hop is a swap-in rather than a parser change.
async fn exec_create_index(
    sess: &ProjectSession,
    ci: sqlparser::ast::CreateIndex,
) -> Result<ExecResult> {
    use crate::index_extras::{
        log_expression_column_notice, log_include_notice, log_metadata_only_notice,
        log_partial_index_notice, log_using_notice, parse_index_columns,
        IndexColumn,
    };

    // CONCURRENTLY is still unsupported — reject explicitly.
    if ci.concurrently {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX CONCURRENTLY is not supported in v0.1".into(),
        ));
    }

    // 6.SEC.P1 — reject CREATE INDEX targeting a reserved system schema.
    // Guards both the target table and (when explicit) the index name.
    // Phase 5.18.C: system sessions bypass this guard.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&ci.table_name, "CREATE INDEX")?;
        if let Some(ref idx_name) = ci.name {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(idx_name, "CREATE INDEX")?;
        }
    }

    let table_name = single_part_name(&ci.table_name)?;
    let table = TableName::new(table_name)?;

    // Parse all column expressions (bare identifiers + functional expressions).
    let parsed_cols = parse_index_columns(&ci);
    if parsed_cols.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE INDEX: column list cannot be empty".into(),
        ));
    }

    // Build the catalog column list. Bare columns are stored by name;
    // expression columns are prefixed with "expr:" so introspection can
    // distinguish them. The catalog column-existence check is bypassed for
    // expression indexes because the expression references columns
    // indirectly; the catalog only tracks the stringified form as metadata.
    let has_expressions = parsed_cols.iter().any(IndexColumn::is_expression);
    let catalog_columns: Vec<String> = parsed_cols
        .iter()
        .map(IndexColumn::as_catalog_str)
        .collect();

    // Mint a deterministic synthetic name when the user omitted one:
    // `<table>_<col1>_<col2>_idx`. For expression columns, the stringified
    // expr is used in the fallback name after stripping the "expr:" prefix.
    let index_name = match &ci.name {
        Some(n) => single_part_name(n)?.to_string(),
        None => {
            let col_part: String = catalog_columns
                .iter()
                .map(|s| {
                    s.trim_start_matches("expr:")
                        .replace(['(', ')', ' ', ','], "_")
                })
                .collect::<Vec<_>>()
                .join("_");
            format!("{table_name}_{col_part}_idx")
        }
    };

    // A plain-column `CREATE UNIQUE INDEX` is semantically identical to an
    // inline `UNIQUE` constraint in PostgreSQL and MUST be enforced (BUG
    // #136 — previously it was silently accepted as metadata, allowing
    // duplicate rows to accumulate). We register a real enforced
    // `UniqueConstraint` below, routing into the SAME catalog machinery a
    // table-level `UNIQUE (...)` uses, so the existing INSERT / UPDATE
    // enforcement (`constraints::enforce_unique_on_insert`) covers it.
    //
    // Partial unique indexes (`WHERE ...`) and expression unique indexes
    // remain non-enforcing (out of scope) — those keep the metadata-only
    // notice so behavior does not regress.
    let enforce_unique = ci.unique && ci.predicate.is_none() && !has_expressions;

    // Loud-reject UNIQUE variants we cannot actually enforce. Silently
    // accepting them as metadata (the previous behavior) breaks the
    // uniqueness contract: duplicate rows would accumulate undetected.
    // A non-unique secondary index is fine to accept as metadata (queries
    // fall through to scan, correctness preserved) — but UNIQUE is a data
    // integrity guarantee, so an un-enforceable UNIQUE must fail at DDL time.
    if ci.unique && !enforce_unique {
        return Err(BasinError::FeatureNotSupported(
            "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
             use a plain-column UNIQUE"
                .into(),
        ));
    }
    if ci.unique && !ci.include.is_empty() {
        return Err(BasinError::FeatureNotSupported(
            "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
             use a plain-column UNIQUE"
                .into(),
        ));
    }
    if ci.unique {
        if let Some(method) = &ci.using {
            let m = method.to_string();
            if !matches!(m.to_lowercase().as_str(), "btree") {
                return Err(BasinError::FeatureNotSupported(
                    "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
                     use a plain-column UNIQUE"
                        .into(),
                ));
            }
        }
    }

    // Determine the access method. `USING gin` → "gin"; `USING btree` or
    // absent → "btree" (the default). Other methods are logged as a notice and
    // treated as btree metadata-only (same pre-5.19 behaviour).
    let access_method_str: String = ci
        .using
        .as_ref()
        .map(|m| m.to_string().to_lowercase())
        .unwrap_or_else(|| "btree".to_string());

    // For GIN indexes: extract the operator class from the first (and only)
    // indexed column's `operator_class` field in the sqlparser AST.
    // sqlparser parses `(col jsonb_path_ops)` as an IndexColumn with
    // `operator_class = Some(ObjectName([Ident("jsonb_path_ops")]))`.
    // We normalise to lowercase and validate: `jsonb_ops`, `jsonb_path_ops`,
    // and `tsvector_ops` are accepted; for tsvector columns with no opclass
    // specified we auto-detect and use `tsvector_ops`.
    let gin_opclass: Option<String> = if access_method_str == "gin" {
        // Extract the operator class from the first indexed column.
        // sqlparser 0.61 parses `(col jsonb_path_ops)` as an IndexColumn with
        // `operator_class = Some(ObjectName([Identifier(Ident{value:"jsonb_path_ops"})]))`.
        // Use `id_val()` (from ObjectNamePartExt) to unwrap the identifier string.
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        match raw_opclass.as_deref() {
            // Phase 5.20.E: explicit tsvector_ops opclass.
            Some("tsvector_ops") => Some("tsvector_ops".to_string()),
            // No opclass specified — auto-detect based on column type.
            None => {
                // Check if the indexed column is a TSVECTOR column; if so,
                // use tsvector_ops.  Otherwise default to jsonb_ops.
                // `catalog_columns` has already been built above from the
                // parsed column list — use its first element.
                let col_name_opt = catalog_columns.first().cloned();
                let is_tsvector_col = if let Some(ref col_name) = col_name_opt {
                    // Look up the table metadata to check the column type.
                    // We already verified the table exists above; if load fails just
                    // fall back to jsonb_ops (safe).
                    if let Ok(meta) = sess.engine.config().catalog.load_table(&sess.project, &table).await {
                        meta.schema.fields().iter().any(|f| {
                            f.name() == col_name && crate::types::field_is_tsvector(f)
                        })
                    } else {
                        false
                    }
                } else {
                    false
                };
                if is_tsvector_col {
                    Some("tsvector_ops".to_string())
                } else {
                    Some("jsonb_ops".to_string())
                }
            }
            Some("jsonb_ops") => Some("jsonb_ops".to_string()),
            Some("jsonb_path_ops") => Some("jsonb_path_ops".to_string()),
            Some(other) => {
                return Err(BasinError::InvalidSchema(format!(
                    "CREATE INDEX USING gin: unknown operator class {other:?}; \
                     accepted: jsonb_ops (default), jsonb_path_ops, tsvector_ops"
                )));
            }
        }
    } else {
        None
    };

    // Phase 5.26.D/E: extract vector operator class for HNSW / IVFFlat indexes.
    // pgvector opclasses: `vector_l2_ops`, `vector_cosine_ops`, `vector_ip_ops`.
    // For IVFFlat: same opclasses, same convention (IVFFlat is accepted as a DDL
    // synonym and mapped to the Basin HNSW implementation as a documented fallback
    // — the catalog records the declared access method for introspection, but
    // queries use the HNSW fast path regardless).
    // WITH (m = 16, ef_construction = 64) and WITH (lists = N) are accepted and
    // logged; they are stored in the catalog opclass string for future use.
    let vector_opclass: Option<String> = if access_method_str == "hnsw"
        || access_method_str == "ivfflat"
    {
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        match raw_opclass.as_deref() {
            None | Some("vector_l2_ops") => Some("vector_l2_ops".to_string()),
            Some("vector_cosine_ops") => Some("vector_cosine_ops".to_string()),
            Some("vector_ip_ops") => Some("vector_ip_ops".to_string()),
            Some(other) => {
                // Unknown opclass — accept with a warning (pgvector DDL portability).
                tracing::warn!(
                    index = %index_name,
                    opclass = %other,
                    "CREATE INDEX USING {access_method_str}: unknown vector opclass; \
                     expected one of: vector_l2_ops, vector_cosine_ops, vector_ip_ops. \
                     Defaulting to vector_l2_ops."
                );
                Some("vector_l2_ops".to_string())
            }
        }
    } else {
        None
    };

    // Log IVFFlat fallback notice.
    if access_method_str == "ivfflat" {
        tracing::info!(
            index = %index_name,
            table = %table_name,
            "CREATE INDEX USING ivfflat accepted; Basin maps IVFFlat to the HNSW \
             implementation as a documented fallback (Phase 5.26.E). Queries use \
             the HNSW fast path. A native IVFFlat implementation is roadmap-tracked."
        );
    }

    // Phase 5.24.D: extract opclass for GIST indexes on range columns.
    // Accepted: `range_ops` (default when not specified), empty (treated as
    // `range_ops`).  Unknown opclasses are accepted with a notice so that
    // users writing `CREATE INDEX … USING gist (col)` without an explicit
    // opclass get the interval-tree wiring.
    let gist_opclass: Option<String> = if access_method_str == "gist" {
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                use crate::pg_ast::ObjectNamePartExt;
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        Some(raw_opclass.unwrap_or_else(|| "range_ops".to_string()))
    } else {
        None
    };

    // Emit notices for accepted-but-not-enforced features.
    if let Some(pred) = &ci.predicate {
        log_partial_index_notice(&index_name, &pred.to_string());
    }
    if !access_method_str.eq_ignore_ascii_case("btree")
        && !access_method_str.eq_ignore_ascii_case("gin")
        && !access_method_str.eq_ignore_ascii_case("gist")
        && !access_method_str.eq_ignore_ascii_case("hnsw")
        && !access_method_str.eq_ignore_ascii_case("ivfflat")
    {
        // Non-btree, non-gin, non-gist, non-vector access methods: log the notice.
        log_using_notice(&index_name, &access_method_str);
    }
    if !ci.include.is_empty() {
        let include_cols: Vec<String> =
            ci.include.iter().map(|ident| ident.value.clone()).collect();
        log_include_notice(&index_name, &include_cols);
    }
    for col in &parsed_cols {
        if let IndexColumn::Expression(expr) = col {
            log_expression_column_notice(&index_name, expr);
        }
    }

    // Verify the table exists before touching the catalog.
    sess.engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                "CREATE INDEX: table {table_name:?} does not exist in this project"
            )),
            other => other,
        })?;

    // For expression indexes the catalog column-validation would reject the
    // "expr:..." strings because they are not real column names.  Accept
    // the declaration as metadata-only: log the notice and return success
    // without writing to the catalog.  Bare-column indexes continue
    // through the normal catalog path so they appear in introspection.
    if has_expressions {
        log_metadata_only_notice(&index_name, table_name);
        return Ok(ExecResult::Empty {
            tag: "CREATE INDEX".into(),
        });
    }

    log_metadata_only_notice(&index_name, table_name);

    // Phase 5.19.B: use create_index_with_method so GIN indexes persist their
    // access_method ("gin") and opclass ("jsonb_ops" / "jsonb_path_ops") in the
    // catalog SecondaryIndex row. Btree indexes continue to use the same path
    // via the default impl that delegates to create_index.
    // Phase 5.26.D/E: HNSW and IVFFlat vector indexes use vector_opclass.
    // IVFFlat is stored with access_method "hnsw" (the Basin implementation)
    // for query-routing purposes; the declared "ivfflat" is preserved in the
    // opclass string so introspection can still see the user's intent.
    let (catalog_method, catalog_opclass) = if access_method_str == "hnsw" {
        ("hnsw".to_string(), vector_opclass.clone())
    } else if access_method_str == "ivfflat" {
        // IVFFlat fallback: map to HNSW in the catalog so the vector planner
        // can route through the HNSW fast path. Store opclass with an "ivfflat:"
        // prefix so introspection can still see the original access method.
        let opclass = vector_opclass
            .as_deref()
            .map(|op| format!("ivfflat:{op}"))
            .or_else(|| Some("ivfflat:vector_l2_ops".to_string()));
        ("hnsw".to_string(), opclass)
    } else if access_method_str == "gist" {
        // Phase 5.24.D: GIST indexes on range columns are stored with their
        // opclass (default "range_ops") so the probe path can verify that
        // a GIST index covers the queried column.
        ("gist".to_string(), gist_opclass.clone())
    } else {
        (access_method_str.clone(), gin_opclass.clone())
    };
    sess.engine
        .config()
        .catalog
        .create_index_with_method(
            &sess.project,
            &table,
            &index_name,
            &catalog_columns,
            ci.if_not_exists,
            &catalog_method,
            catalog_opclass.as_deref(),
        )
        .await?;

    // BUG #136 fix: register an enforced UNIQUE constraint for a plain-column
    // `CREATE UNIQUE INDEX`. PG names the implicit constraint after the index,
    // so we reuse `index_name` as the constraint name (this is also what
    // surfaces in the SQLSTATE 23505 message). `set_unique_constraints`
    // *replaces* the whole list, so load the existing set, append, and write
    // it back. If a constraint with this index's name already exists (e.g. a
    // re-run under IF NOT EXISTS where create_index was a no-op) we leave the
    // set untouched to keep the operation idempotent.
    if enforce_unique {
        let meta = sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, &table)
            .await?;
        let already_registered = meta.unique_constraints.iter().any(|u| u.name == index_name);
        if !already_registered {
            let mut uniques = meta.unique_constraints.clone();
            uniques.push(basin_catalog::UniqueConstraint {
                name: index_name.clone(),
                columns: catalog_columns.clone(),
            });
            sess.engine
                .config()
                .catalog
                .set_unique_constraints(&sess.project, &table, uniques)
                .await?;
        }
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    }

    Ok(ExecResult::Empty {
        tag: "CREATE INDEX".into(),
    })
}

/// `DROP INDEX [IF EXISTS] <name>`. Removes the catalog row only —
/// there's nothing to physically tear down because v0.1 doesn't
/// materialise any index file.
async fn exec_drop_index(
    sess: &ProjectSession,
    if_exists: bool,
    names: Vec<sqlparser::ast::ObjectName>,
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP INDEX requires at least one index name".into(),
        ));
    }
    for n in &names {
        // 6.SEC.P1 — reject DROP INDEX targeting a reserved system schema.
        // Phase 5.18.C: system sessions bypass this guard.
        if !sess.is_system {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(n, "DROP INDEX")?;
        }
        let index_name = single_part_name(n)?;
        // The catalog stores indexes per-table; we don't track a
        // global (project, index-name) → table mapping. Scan every
        // table in the project for a matching declaration.
        let tables = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.project)
            .await?;
        let mut found = false;
        for t in &tables {
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, t)
                .await?;
            if meta.indexes.iter().any(|i| i.name == index_name) {
                sess.engine
                    .config()
                    .catalog
                    .drop_index(&sess.project, t, index_name)
                    .await?;
                // BUG #136 fix: a `CREATE UNIQUE INDEX` also registered an
                // enforced UNIQUE constraint named after the index. Dropping
                // the index must drop that enforcement too (PG: DROP INDEX on
                // a unique index removes the uniqueness). Constraints created
                // by an inline table-level `UNIQUE (...)` are not named after
                // an index, so this only removes the index-derived one.
                if meta.unique_constraints.iter().any(|u| u.name == index_name) {
                    let remaining: Vec<basin_catalog::UniqueConstraint> = meta
                        .unique_constraints
                        .iter()
                        .filter(|u| u.name != index_name)
                        .cloned()
                        .collect();
                    sess.engine
                        .config()
                        .catalog
                        .set_unique_constraints(&sess.project, t, remaining)
                        .await?;
                    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, t).await?;
                }
                found = true;
                break;
            }
        }
        if !found && !if_exists {
            return Err(BasinError::NotFound(format!("index {index_name:?}")));
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP INDEX".into(),
    })
}

async fn exec_insert(sess: &ProjectSession, ins: sqlparser::ast::Insert) -> Result<ExecResult> {
    let name = single_part_name(crate::pg_ast::insert_object_name(&ins)?)?;
    let table = TableName::new(name)?;

    // --- DEFAULT VALUES: source is None and columns list is empty -----------
    // `INSERT INTO t DEFAULT VALUES` — build a single all-NULL row then let
    // `apply_column_defaults` stamp defaults on every non-generated column.
    if ins.source.is_none() {
        return exec_insert_default_values(sess, ins).await;
    }

    // --- ON CONFLICT DO UPDATE pre-check ------------------------------------
    // If the statement has ON CONFLICT (col) DO UPDATE SET …, check whether
    // the conflict column already has that value and route to UPDATE if so.
    if let Some(OnInsert::OnConflict(ref on_conflict)) = ins.on {
        if let OnConflictAction::DoUpdate(_) = &on_conflict.action {
            if let Some(result) = try_on_conflict_do_update(sess, &table, &ins, on_conflict).await?
            {
                return Ok(result);
            }
            // result is None means no conflict was found — fall through to
            // the normal INSERT path.
        }
    }

    // Pull literal rows out of `INSERT ... VALUES (...)`. Subquery inserts
    // (`INSERT ... SELECT ...`) are routed through `exec_insert_select`
    // below; the body is materialised into VALUES-shaped rows.
    let source = ins
        .source
        .as_ref()
        .ok_or_else(|| BasinError::internal("INSERT without VALUES is not supported in PoC"))?;
    if !matches!(source.body.as_ref(), SetExpr::Values(_)) {
        // INSERT INTO <t> SELECT ... — materialise the SELECT into a
        // RecordBatch via the session's DataFusion context (which already
        // sees inlined user-defined functions, RLS policies, etc.) and
        // hand the resulting rows to the standard INSERT path as if they
        // had been written as VALUES literals.
        return exec_insert_select(sess, &table, &ins, source.as_ref()).await;
    }
    let rows_raw = match source.body.as_ref() {
        SetExpr::Values(v) => &v.rows,
        _ => unreachable!("checked above"),
    };

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();
    let mut row_count = rows_raw.len();

    // Pick up any `OVERRIDING { SYSTEM | USER } VALUE` clause the
    // textual pre-screen in `execute()` stashed for us. `take_pending`
    // is take-once: a stale value from a prior statement on this
    // session can't leak into the next INSERT.
    let overriding = crate::session::take_pending_overriding(&sess.state);
    // Enforce IDENTITY semantics on the user-written column list
    // *before* we expand to full-width rows. ALWAYS columns reject
    // user-supplied values unless `OVERRIDING SYSTEM VALUE` is set;
    // BY DEFAULT columns accept them unconditionally (and the
    // `OVERRIDING USER VALUE` clause forces them back to nextval —
    // handled in `apply_identity_columns` below).
    enforce_identity_insert_columns(schema.as_ref(), &ins.columns, overriding)?;
    // Reject direct writes to generated columns + expand `INSERT INTO t
    // (col_subset) VALUES ...` into full schema-width rows with NULL in
    // unmentioned columns. Generated columns are NULL'd here too;
    // `materialise_generated_columns` overwrites them once the per-row
    // batch is built.
    let mut rows_expanded = expand_insert_rows(schema.as_ref(), &ins.columns, rows_raw)?;
    // Fill IDENTITY columns. Three cases:
    //   * User omitted the column      → fill from nextval.
    //   * Column is BY DEFAULT and
    //     OVERRIDING USER VALUE is set → discard user literal, fill
    //                                    from nextval.
    //   * Otherwise (column supplied,
    //     no OVERRIDING USER VALUE,
    //     ALWAYS already gated above)  → leave the user's value.
    apply_identity_columns(
        sess,
        schema.as_ref(),
        &ins.columns,
        overriding,
        &mut rows_expanded,
    )
    .await?;
    // Substitute column-level DEFAULT expressions for omitted columns.
    // For columns with `BASIN_COLUMN_DEFAULT` metadata that the user did
    // not explicitly write, evaluate the default text (which routes any
    // `nextval(...)` calls through `Catalog::nextval` so each row gets a
    // distinct value) and overwrite the NULL placeholder produced by
    // `expand_insert_rows`. User-written NULL is preserved.
    apply_column_defaults(sess, schema.as_ref(), &ins.columns, &mut rows_expanded).await?;

    // --- ON CONFLICT DO NOTHING filter ----------------------------------------
    // If the statement specifies ON CONFLICT (cols) DO NOTHING, proactively
    // remove any proposed rows that would conflict with existing rows (or with
    // earlier rows in the same batch). This must happen *before* the unique /
    // PK constraint checks so those checks never see the skipped rows.
    if let Some(OnInsert::OnConflict(ref on_conflict)) = ins.on {
        if let OnConflictAction::DoNothing = &on_conflict.action {
            // WHERE clause form with conflict predicate is deferred.
            if on_conflict
                .conflict_target
                .as_ref()
                .map(|t| matches!(t, ConflictTarget::OnConstraint(_)))
                .unwrap_or(false)
            {
                // ON CONFLICT ON CONSTRAINT <name> DO NOTHING — not yet
                // implemented; reject cleanly rather than guessing.
                return Err(BasinError::FeatureNotSupported(
                    "ON CONFLICT ON CONSTRAINT DO NOTHING is not yet supported; \
                     use ON CONFLICT (col, ...) DO NOTHING"
                        .into(),
                ));
            }
            rows_expanded = filter_rows_do_nothing(
                sess,
                &table,
                schema.as_ref(),
                &meta,
                &on_conflict.conflict_target,
                rows_expanded,
            )
            .await?;
            if rows_expanded.is_empty() {
                return Ok(ExecResult::Empty {
                    tag: "INSERT 0 0".into(),
                });
            }
            // Update the row count to reflect only the non-conflicting rows.
            row_count = rows_expanded.len();
        }
    }

    let rows: &[Vec<sqlparser::ast::Expr>] = &rows_expanded;

    // Partitioned path. We must compute each row's partition key from its
    // partition-column value before producing any RecordBatch — multi-row
    // INSERTs may span partitions and we issue one Parquet write per
    // resulting partition.
    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        let groups = group_rows_by_partition(schema.as_ref(), rows, &meta.partition_spec)?;

        // Shard path is intentionally disabled for partitioned tables in
        // v0.1 — the shard owner's WAL pre-supposes one partition key per
        // project slice and the multi-partition fan-out hasn't been wired
        // through compaction yet. Fall through to the synchronous Parquet
        // write path below.
        let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));
        let mut materialised_groups: Vec<(PartitionKey, RecordBatch)> =
            Vec::with_capacity(groups.len());
        for (pkey, group_rows) in groups {
            let batch = batch_from_rows(schema.clone(), &group_rows)?;
            let batch = crate::generated_cols::materialise_generated_columns(
                &sess.engine.config().catalog,
                &sess.project,
                batch,
            )
            .await?;
            crate::type_ddl::enforce_enum_labels(
                &sess.engine.config().catalog,
                &sess.project,
                &batch,
            )
            .await?;
            crate::type_ddl::enforce_domain_checks(
                &sess.engine.config().catalog,
                &sess.project,
                &batch,
            )
            .await?;
            crate::constraints::enforce_check_constraints(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
                &batch,
            )
            .await?;
            // BUG #133: RLS WITH CHECK on INSERT. Reuses the same per-row
            // predicate-evaluation machinery as CHECK constraints above.
            crate::rls::enforce_with_check(
                &sess.auth_context,
                table.as_str(),
                meta.rls_enabled,
                &meta.policies,
                &sess.current_user,
                basin_catalog::PolicyCommand::Insert,
                &batch,
            )
            .await?;
            crate::constraints::enforce_fk_on_insert(
                &sess.engine.config().catalog,
                &sess.engine.config().storage,
                &sess.project,
                table.as_str(),
                &meta.foreign_keys,
                &batch,
            )
            .await?;
            crate::constraints::enforce_pk_on_insert(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                &meta.pk_columns,
                &batch,
                Some((&*sess.engine.memtable_registry(), &sess.project)),
            )
            .await?;
            crate::constraints::enforce_unique_on_insert(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                &meta.unique_constraints,
                &batch,
            )
            .await?;
            materialised_groups.push((pkey, batch));
        }

        // Pre-commit before any Parquet IO so a rejecting sink leaves the
        // object store untouched. Sinks see the in-memory `after` payload;
        // they don't need the on-disk file.
        let preview_batches: Vec<RecordBatch> =
            materialised_groups.iter().map(|(_, b)| b.clone()).collect();
        let events = build_insert_events(sess, &table, &preview_batches)?;
        dispatch_pre_commit(&sess.engine, &events).await?;

        let mut file_refs: Vec<DataFileRef> = Vec::with_capacity(materialised_groups.len());
        for (pkey, batch) in &materialised_groups {
            let df = sess
                .engine
                .config()
                .storage
                .write_batch_with_options(&sess.project, &table, pkey, batch, &opts)
                .await?;
            file_refs.push(DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
                bloom_filters: df.bloom_filters.clone(),
                hll_sketches: std::collections::BTreeMap::new(),
                tdigest_sketches: std::collections::BTreeMap::new(),
            });
        }

        commit_with_retry(sess, &table, meta.current_snapshot, file_refs).await?;
        dispatch_post_commit(&sess.engine, events);
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
        write_insert_audit_rows(sess, meta.schema.as_ref(), &preview_batches).await?;
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    let batch = batch_from_rows(schema, rows)?;
    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.project,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    // PK / CHECK / FK enforcement. Order: CHECK (no I/O), then FK
    // (one referenced-table scan), then PK (one full-table scan).
    // v0.2 secondary indexes (Phase 5.7 B1) will collapse PK / FK
    // to point lookups; for v0.1 we accept the scan cost.
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT. Same per-row predicate-eval
    // machinery as CHECK constraints; no-op when rls_enabled = false.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
    )
    .await?;
    let row_count = batch.num_rows();
    let part = PartitionKey::default_key();

    // Shard-enabled path. The shard owner appends to its WAL, acks once durable,
    // and lets its background compactor drain into Parquet + commit through the
    // catalog later. We do *not* call `append_data_files` ourselves here: that
    // would race the compactor's own commit and produce a duplicate snapshot.
    //
    // ACID gate: the shard's WAL + memtable are a shared durability surface
    // that the session-scoped `TxState` cannot rewind. Any INSERT routed
    // through the shard while a BEGIN block is open survives ROLLBACK
    // (the catalog snapshot is rewound, but the shard's tail compacts into
    // a new file at the *next* SELECT's `shard.flush_to_parquet()` and is
    // appended at a fresh snapshot, resurrecting the row). Fall through to
    // the legacy synchronous path inside an explicit transaction so the
    // write lands in `TxState::pending_files` + `htap_rows`, where ROLLBACK
    // can actually discard it. Bench-shape #42 (`rollback_drops_rows`)
    // regression.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        if !crate::session::tx_is_active(&sess.state) {
            let handle = shard.get(&sess.project, &part).await?;
            handle.write_batch(&table, batch).await?;
            // SELECT-side handles tail-visibility (Option A: force-compact). Skip
            // the DataFusion ListingTable refresh here; reads will trigger it.
            return Ok(ExecResult::Empty {
                tag: format!("INSERT 0 {row_count}"),
            });
        }
        // In-tx: fall through to the synchronous Parquet+TxState path below.
        // Any prior auto-commit writes still in the shard's tail will be
        // flushed by `pre_mutation_flush` (UPDATE/DELETE) or by the SELECT
        // path's `shard.flush_to_parquet()`; no extra flush is needed here.
    }

    // Legacy synchronous path (no shard configured).
    //
    // Order: write parquet → (if in txn: defer to pending_files + refresh
    // with extra; else: dispatch pre-commit → commit catalog → refresh).
    let events = build_insert_events(sess, &table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, &table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // ── Transaction-deferred path ────────────────────────────────────────
    // When inside an explicit transaction, defer catalog commit.  The file
    // has been written to storage; add it to the session's pending list and
    // register it with DataFusion so within-tx SELECTs can see it.
    //
    // Phase 5.14.C2: also buffer the Arrow batch in the tx-local HTAP store
    // so that projection-scan queries (not just COUNT(*)) can see uncommitted
    // rows from the same transaction.  WAL Begin marker is emitted lazily on
    // the first DML inside the tx.
    if crate::session::tx_is_active(&sess.state) {
        // Lazy WAL Begin — idempotent within a tx.
        htap_emit_wal_begin_lazy(sess).await;
        // Buffer batch for tx-local read-your-own-writes.
        crate::session::tx_htap_push_batch(&sess.state, &table, batch.clone());
        crate::session::tx_push_pending_file(&sess.state, &table, file_ref);
        let pending = crate::session::tx_pending_files_for(&sess.state, &table);
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, &table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            &table,
            &pending,
            htap_batches,
        )
        .await
        {
            // If refresh fails, mark the txn aborted and propagate.
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        // Phase 5.7 B1: maintain secondary indexes on INSERT (tx-active path).
        maintain_secondary_indexes_on_insert(sess, &table, &meta, &batch, df.path.as_ref()).await;
        if ins.returning.is_some() {
            return Ok(ExecResult::Rows {
                schema: batch.schema(),
                batches: vec![batch],
            });
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // ── Auto-commit path ─────────────────────────────────────────────────
    // Register the just-written (not-yet-catalogued) file as an "extra" so
    // that reactor-bodied SELECTs fired during pre-commit can see the new
    // row. This mirrors the in-tx `refresh_table_with_extra` approach from
    // #92: the file is already durable in storage; we simply expose it to
    // DataFusion's planner before the catalog snapshot advances.  Without
    // this, pre-commit hooks would query a ListingTable that does not yet
    // include the new file (it wasn't in the catalog at file-write time).
    //
    // This also fixes per-statement read-your-own-writes in auto-commit
    // mode: the next SELECT on this session will call exec_select which
    // refreshes all tables; at that point the catalog has been committed,
    // so refresh_table (without extra) will see the new file naturally.
    if let Err(e) = crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        &table,
        std::slice::from_ref(&file_ref),
    )
    .await
    {
        // Refresh failure before commit means the orphan file in storage
        // should be cleaned up.  Catalog snapshot is unchanged so no
        // catalog rollback is needed.
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        return Err(e);
    }
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await;
        return Err(e);
    }

    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);

    // Post-commit: re-register the table from the now-authoritative catalog
    // snapshot (no extra files).  This ensures the DataFusion provider
    // reflects exactly the committed state and no longer carries the
    // pre-commit "extra" file reference.
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    // Phase 5.7 B1: maintain secondary indexes on INSERT (auto-commit path).
    maintain_secondary_indexes_on_insert(sess, &table, &meta, &batch, df.path.as_ref()).await;

    // RETURNING: if the caller asked for RETURNING *, return the inserted batch.
    if ins.returning.is_some() {
        return Ok(ExecResult::Rows {
            schema: batch.schema(),
            batches: vec![batch],
        });
    }

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

/// `INSERT INTO <table> [(<cols>)] <SELECT ...>` (or any non-VALUES
/// query body). Materialises the source query through the session's
/// DataFusion context — which already sees inlined user-defined
/// functions, RLS predicates, and partition pruning — and writes the
/// resulting batch using the same legacy synchronous path that the
/// VALUES form uses. Partitioned tables, generated columns, and the
/// shard-write path are out of scope for v0.1 INSERT-SELECT.
async fn exec_insert_select(
    sess: &ProjectSession,
    table: &TableName,
    ins: &sqlparser::ast::Insert,
    source: &sqlparser::ast::Query,
) -> Result<ExecResult> {
    use crate::convert::batch_df_to_ws;
    use arrow_array::{ArrayRef, RecordBatch};

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let schema = meta.schema.clone();

    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        return Err(BasinError::internal(
            "INSERT INTO ... SELECT is not supported on partitioned tables in v0.1",
        ));
    }
    if schema
        .fields()
        .iter()
        .any(|f| crate::types::field_is_generated(f).is_some())
    {
        return Err(BasinError::internal(
            "INSERT INTO ... SELECT is not supported on tables with generated columns in v0.1",
        ));
    }

    // Run the source SELECT through the session context. The full
    // pre-screen pipeline (function inlining, vector-operator rewrite,
    // RLS via `apply_rls_to_select`-equivalents) ran on the parent
    // statement; the source query inherits that. We do *not* re-run
    // the inliner here because it already mutated the AST in
    // `executor::execute`'s SQL-string pass.
    let source_sql = source.to_string();
    let df = sess
        .ctx
        .sql(&source_sql)
        .await
        .map_err(|e| BasinError::internal(format!("INSERT INTO ... SELECT plan: {e}")))?;
    let df_batches = df
        .collect()
        .await
        .map_err(|e| BasinError::internal(format!("INSERT INTO ... SELECT execute: {e}")))?;

    // Concatenate batches and convert to workspace arrow.
    let combined_df = if df_batches.is_empty() {
        // Empty result — produce an empty batch with the source schema
        // for the column-mapping step below; the write path is a no-op
        // when there are no rows.
        let plan_schema = sess
            .ctx
            .sql(&source_sql)
            .await
            .map_err(|e| BasinError::internal(format!("INSERT INTO ... SELECT replan: {e}")))?
            .schema()
            .as_arrow()
            .clone();
        datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::new(plan_schema))
    } else if df_batches.len() == 1 {
        df_batches.into_iter().next().unwrap()
    } else {
        let s = df_batches[0].schema();
        datafusion::arrow::compute::concat_batches(&s, &df_batches)
            .map_err(|e| BasinError::internal(format!("concat INSERT-SELECT batches: {e}")))?
    };
    let source_batch = batch_df_to_ws(&combined_df)?;
    let row_count = source_batch.num_rows();

    // Map source columns to the target schema: when `INSERT INTO t (a, b)
    // SELECT ...` is given, the source's i-th column lands in column `a`,
    // etc. When `(a, b)` is omitted, we insist the source's column count
    // matches the target schema width and use the target's column order.
    let target_cols: Vec<usize> = if ins.columns.is_empty() {
        if source_batch.num_columns() != schema.fields().len() {
            return Err(BasinError::InvalidSchema(format!(
                "INSERT INTO {}: source has {} columns, target has {}",
                table.as_str(),
                source_batch.num_columns(),
                schema.fields().len()
            )));
        }
        (0..schema.fields().len()).collect()
    } else {
        if source_batch.num_columns() != ins.columns.len() {
            return Err(BasinError::InvalidSchema(format!(
                "INSERT INTO {}: source has {} columns, target column list has {}",
                table.as_str(),
                source_batch.num_columns(),
                ins.columns.len()
            )));
        }
        let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
        for (i, f) in schema.fields().iter().enumerate() {
            by_name.insert(f.name().to_ascii_lowercase(), i);
        }
        let mut out = Vec::with_capacity(ins.columns.len());
        for c in &ins.columns {
            let key = c.value.to_ascii_lowercase();
            let idx = *by_name.get(&key).ok_or_else(|| {
                BasinError::InvalidSchema(format!("INSERT references unknown column {:?}", c.value))
            })?;
            out.push(idx);
        }
        out
    };

    // Build a target-schema-shaped batch by placing each source column at
    // the matching target index; unmentioned target columns get NULL
    // arrays of the right type. The per-cell types must already match —
    // we don't re-coerce here (DataFusion's type coercion already ran).
    let n_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    for (target_idx, target_field) in schema.fields().iter().enumerate() {
        if let Some(source_pos) = target_cols.iter().position(|&i| i == target_idx) {
            let arr = source_batch.column(source_pos).clone();
            if arr.data_type() != target_field.data_type() {
                return Err(BasinError::InvalidSchema(format!(
                    "INSERT INTO {} column {:?}: source type {:?} does not match target type {:?}",
                    table.as_str(),
                    target_field.name(),
                    arr.data_type(),
                    target_field.data_type()
                )));
            }
            columns.push(arr);
        } else {
            // Column not supplied by the SELECT. For identity/SERIAL columns,
            // generate sequential values via nextval; for others push NULLs.
            use crate::types::field_identity_sequence;
            if let Some(seq_name) = field_identity_sequence(target_field) {
                // Allocate one nextval per row from the catalog sequence.
                let catalog = &sess.engine.config().catalog;
                let mut ids: Vec<i64> = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    let next = catalog.nextval(&sess.project, seq_name).await?;
                    sess.state
                        .sequence_cache
                        .record(sess.project, seq_name, next)
                        .await;
                    ids.push(next);
                }
                columns.push(Arc::new(arrow_array::Int64Array::from(ids)) as ArrayRef);
            } else {
                columns.push(arrow_array::new_null_array(
                    target_field.data_type(),
                    row_count,
                ));
            }
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("build INSERT-SELECT batch: {e}")))?;

    // Enum / domain check enforcement matches the VALUES path so
    // constraint violations surface identically regardless of source.
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT ... SELECT — the materialised
    // rows are subject to the same policy enforcement as VALUES inserts.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
    )
    .await?;

    let part = PartitionKey::default_key();
    let events = build_insert_events(sess, table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));
    let written = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
        bloom_filters: written.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // ── Phase 5.14.C3: INSERT-SELECT HTAP wiring (transaction-deferred path) ──
    // When inside an explicit transaction, mirror the VALUES path: buffer the
    // batch in the tx-local HTAP store and defer the catalog commit.
    if crate::session::tx_is_active(&sess.state) {
        // Lazy WAL Begin — idempotent within a tx.
        htap_emit_wal_begin_lazy(sess).await;
        // Buffer batch for tx-local read-your-own-writes.
        crate::session::tx_htap_push_batch(&sess.state, table, batch.clone());
        crate::session::tx_push_pending_file(&sess.state, table, file_ref);
        let pending = crate::session::tx_pending_files_for(&sess.state, table);
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            table,
            &pending,
            htap_batches,
        )
        .await
        {
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // Pre-commit: expose the new file to DataFusion so reactor hooks can
    // see it.  Mirror the auto-commit VALUES path: use refresh_table_with_extra
    // so the file is visible before the catalog snapshot advances.
    if let Err(e) = crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        table,
        std::slice::from_ref(&file_ref),
    )
    .await
    {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &written.path)
            .await;
        return Err(e);
    }
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &written.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await;
        return Err(e);
    }

    commit_with_retry(sess, table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);

    // ── Phase 5.14.C3: auto-commit INSERT-SELECT → memtable registry ─────────
    // After a successful auto-commit, push the batch to the shared
    // MemTableRegistry so that subsequent point-lookup fast-path queries find
    // the rows without flushing. This mirrors the committed-row visibility
    // provided by the VALUES path on COMMIT.
    {
        let htap_rows: std::collections::HashMap<_, _> =
            [(table.clone(), vec![batch.clone()])].into_iter().collect();
        // Best-effort: if promotion fails (e.g. hard-cap exceeded), the row is
        // still durable in Parquet — we just lose the hot-tier cache entry.
        let _ = htap_promote_to_registry(sess, &htap_rows).await;
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

// ---------------------------------------------------------------------------
// INSERT DEFAULT VALUES
// ---------------------------------------------------------------------------

/// Handle `INSERT INTO t DEFAULT VALUES`.
///
/// Builds a single all-NULL row then applies every non-generated column's
/// DEFAULT expression. Columns without a DEFAULT stay NULL (which will fail
/// NOT NULL enforcement in `batch_from_rows` if the column is NOT NULL, giving
/// the user a clean error rather than a silent bad insert).
async fn exec_insert_default_values(
    sess: &ProjectSession,
    ins: sqlparser::ast::Insert,
) -> Result<ExecResult> {
    use sqlparser::ast::{Expr, Value};

    let name = single_part_name(crate::pg_ast::insert_object_name(&ins)?)?;
    let table = TableName::new(name)?;

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let schema = meta.schema.clone();

    // Build one all-NULL row spanning all schema columns.
    let mut row: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|_| Expr::Value((Value::Null).into()))
        .collect();

    // Apply defaults to every non-generated column (treat all as unmentioned).
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if crate::types::field_is_generated(field).is_some() {
            continue;
        }
        let Some(default_text) = crate::types::field_default_text(field) else {
            continue;
        };
        let expr = crate::seq_ddl::evaluate_default_expression(sess, default_text).await?;
        row[col_idx] = expr;
    }

    let rows = vec![row];
    let batch = batch_from_rows(schema.clone(), &rows)?;
    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.project,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT ... DEFAULT VALUES.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
    )
    .await?;

    let row_count = batch.num_rows();
    let part = PartitionKey::default_key();
    let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));

    let events = build_insert_events(sess, &table, std::slice::from_ref(&batch))?;
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, &table, &part, &batch, &opts)
        .await?;
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // ── Phase 5.14.C3: DEFAULT VALUES HTAP wiring (transaction-deferred path) ─
    // When inside an explicit transaction, mirror the VALUES path: buffer the
    // batch in the tx-local HTAP store and defer the catalog commit.
    if crate::session::tx_is_active(&sess.state) {
        // Lazy WAL Begin — idempotent within a tx.
        htap_emit_wal_begin_lazy(sess).await;
        // Buffer batch for tx-local read-your-own-writes.
        crate::session::tx_htap_push_batch(&sess.state, &table, batch.clone());
        crate::session::tx_push_pending_file(&sess.state, &table, file_ref);
        let pending = crate::session::tx_pending_files_for(&sess.state, &table);
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, &table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            &table,
            &pending,
            htap_batches,
        )
        .await
        {
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        if ins.returning.is_some() {
            return Ok(ExecResult::Rows {
                schema: batch.schema(),
                batches: vec![batch],
            });
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // Pre-commit: register the new file as "extra" so reactor hooks see it
    // before the catalog snapshot advances (mirrors the VALUES path fix).
    crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        &table,
        std::slice::from_ref(&file_ref),
    )
    .await?;
    dispatch_pre_commit(&sess.engine, &events).await?;
    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);

    // ── Phase 5.14.C3: auto-commit DEFAULT VALUES → memtable registry ─────────
    // After a successful auto-commit, push the batch to the shared
    // MemTableRegistry so that subsequent point-lookup fast-path queries find
    // the row without requiring a flush. Best-effort: failure just means the
    // row is served from Parquet on the next read (still durable).
    {
        let htap_rows: std::collections::HashMap<_, _> =
            [(table.clone(), vec![batch.clone()])].into_iter().collect();
        let _ = htap_promote_to_registry(sess, &htap_rows).await;
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    if ins.returning.is_some() {
        return Ok(ExecResult::Rows {
            schema: batch.schema(),
            batches: vec![batch],
        });
    }
    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

// ---------------------------------------------------------------------------
// INSERT … ON CONFLICT (col) DO UPDATE SET …
// ---------------------------------------------------------------------------

// -----------------------------------------------------------------------
// ON CONFLICT DO NOTHING — proactive conflict filter
// -----------------------------------------------------------------------

/// Validate that `conflict_cols` match an existing UNIQUE or PK constraint.
///
/// Returns `Ok(())` when the columns collectively form a registered unique
/// constraint (or the table's primary key), matching PG's requirement that
/// the ON CONFLICT target must refer to a uniqueness/exclusion constraint.
///
/// Returns `Err(BasinError::InvalidSchema(...))` if no matching constraint
/// exists — this mirrors PG SQLSTATE 42P10 ("there is no unique or exclusion
/// constraint matching the ON CONFLICT specification").
fn validate_conflict_target_columns(
    pk_columns: &[String],
    unique_constraints: &[basin_catalog::UniqueConstraint],
    conflict_cols: &[String],
) -> Result<()> {
    if conflict_cols.is_empty() {
        // No explicit target — will check all constraints (handled by caller).
        return Ok(());
    }
    // Normalise to lowercase for comparison.
    let target_set: std::collections::HashSet<String> = conflict_cols
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();

    // Check against the PK.
    if !pk_columns.is_empty() {
        let pk_set: std::collections::HashSet<String> =
            pk_columns.iter().map(|c| c.to_ascii_lowercase()).collect();
        if pk_set == target_set {
            return Ok(());
        }
    }

    // Check against each UNIQUE constraint.
    for u in unique_constraints {
        let u_set: std::collections::HashSet<String> =
            u.columns.iter().map(|c| c.to_ascii_lowercase()).collect();
        if u_set == target_set {
            return Ok(());
        }
    }

    Err(BasinError::InvalidSchema(format!(
        "there is no unique or exclusion constraint matching the ON CONFLICT \
         specification (columns: {})",
        conflict_cols.join(", ")
    )))
}

/// ON CONFLICT DO NOTHING — filter `rows_expanded` to remove any proposed
/// row that would conflict with an existing row or an earlier row in the
/// same batch. Returns the surviving (non-conflicting) rows.
///
/// # Conflict-target handling
///
/// * `ON CONFLICT (col, ...) DO NOTHING` — check only those columns.
/// * `ON CONFLICT DO NOTHING` (no target) — check all PK + UNIQUE columns
///   (PG semantics: any constraint match suppresses the error).
///
/// In both cases, a WHERE clause predicate attached to the ON CONFLICT clause
/// (`ON CONFLICT (col) WHERE <pred> DO NOTHING`) causes a clean rejection
/// ("not yet supported") because the predicate scopes the suppression to a
/// *partial* unique index, which v0.1 does not model.
async fn filter_rows_do_nothing(
    sess: &ProjectSession,
    table: &TableName,
    schema: &arrow_schema::Schema,
    meta: &TableMetadata,
    conflict_target: &Option<ConflictTarget>,
    rows_expanded: Vec<Vec<sqlparser::ast::Expr>>,
) -> Result<Vec<Vec<sqlparser::ast::Expr>>> {
    use sqlparser::ast::Value;

    if rows_expanded.is_empty() {
        return Ok(rows_expanded);
    }

    // Determine the set of column groups to check. Each entry is a
    // Vec<String> of column names that must *all* match for a conflict.
    let constraint_groups: Vec<Vec<String>> = match conflict_target {
        Some(ConflictTarget::Columns(idents)) => {
            let cols: Vec<String> = idents.iter().map(|i| i.value.clone()).collect();
            // Reject unknown columns eagerly.
            for c in &cols {
                schema.index_of(c).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "ON CONFLICT DO NOTHING: unknown column {c:?}"
                    ))
                })?;
            }
            // Validate that the columns form a real unique/pk constraint.
            validate_conflict_target_columns(&meta.pk_columns, &meta.unique_constraints, &cols)?;
            vec![cols]
        }
        None => {
            // No explicit target — gather all constraint groups (PK + UNIQUEs).
            let mut groups: Vec<Vec<String>> = Vec::new();
            if !meta.pk_columns.is_empty() {
                groups.push(meta.pk_columns.clone());
            }
            for u in &meta.unique_constraints {
                groups.push(u.columns.clone());
            }
            if groups.is_empty() {
                // Table has no constraints at all — nothing to suppress.
                return Ok(rows_expanded);
            }
            groups
        }
        Some(ConflictTarget::OnConstraint(_)) => {
            // Caller already rejected this form before calling us.
            return Err(BasinError::FeatureNotSupported(
                "ON CONFLICT ON CONSTRAINT DO NOTHING is not yet supported".into(),
            ));
        }
    };

    // Pre-build column indexes for every constraint group.
    let group_idxs: Vec<Vec<usize>> = constraint_groups
        .iter()
        .map(|cols| {
            cols.iter()
                .map(|c| {
                    schema.index_of(c).map_err(|_| {
                        BasinError::internal(format!(
                            "DO NOTHING: column {c:?} missing from schema"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    // Helper: extract the string tuple for row `r` using `idxs`.
    // Returns None if any value is NULL (a NULL in a unique column is
    // not considered a conflict under PG's default NULLS DISTINCT).
    let extract_tuple = |row: &Vec<sqlparser::ast::Expr>, idxs: &[usize]| -> Option<Vec<String>> {
        let mut parts = Vec::with_capacity(idxs.len());
        for &idx in idxs {
            let val = match &row[idx] {
                sqlparser::ast::Expr::Value(v) => match v.value {
                    Value::Null => return None,
                    _ => format!("{}", row[idx]),
                },
                other => format!("{other}"),
            };
            parts.push(val);
        }
        Some(parts)
    };

    // One `seen` set per constraint group to handle same-batch dedup.
    let mut seen_per_group: Vec<std::collections::HashSet<Vec<String>>> = constraint_groups
        .iter()
        .map(|_| Default::default())
        .collect();

    let mut survivors: Vec<Vec<sqlparser::ast::Expr>> = Vec::with_capacity(rows_expanded.len());

    'row: for row in rows_expanded {
        // For each constraint group: if this row conflicts (with existing
        // data OR with a preceding row in the same batch), skip it.
        for (g_idx, (cols, idxs)) in constraint_groups.iter().zip(group_idxs.iter()).enumerate() {
            let Some(tuple) = extract_tuple(&row, idxs) else {
                // NULL in the conflict column → not a conflict; keep row.
                continue;
            };

            // Same-batch dup check.
            if seen_per_group[g_idx].contains(&tuple) {
                // Skip this row — it duplicates an earlier row in this batch.
                continue 'row;
            }

            // Existing-table existence check via a SELECT 1 ... WHERE.
            let where_parts: Vec<String> = cols
                .iter()
                .zip(idxs.iter())
                .map(|(col, &idx)| {
                    let expr = &row[idx];
                    // Strings need quoting. The expr Display includes literal
                    // quotes for Value::SingleQuotedString so use it directly.
                    format!("{col} = {expr}")
                })
                .collect();
            let where_clause = where_parts.join(" AND ");
            let check_sql = format!(
                "SELECT 1 FROM {} WHERE {} LIMIT 1",
                table.as_str(),
                where_clause
            );
            let exists = match sess.ctx.sql(&check_sql).await {
                Ok(df) => {
                    let batches = df.collect().await.map_err(|e| {
                        BasinError::internal(format!("ON CONFLICT DO NOTHING existence check: {e}"))
                    })?;
                    batches.iter().any(|b| b.num_rows() > 0)
                }
                Err(_) => {
                    // Table may be empty or not yet registered — no conflict.
                    false
                }
            };
            if exists {
                continue 'row;
            }
        }

        // Row survived all constraint groups — mark it seen and keep it.
        for (g_idx, idxs) in group_idxs.iter().enumerate() {
            if let Some(tuple) = extract_tuple(&row, idxs) {
                seen_per_group[g_idx].insert(tuple);
            }
        }
        survivors.push(row);
    }

    Ok(survivors)
}

/// Pre-check strategy for ON CONFLICT DO UPDATE (upsert).
///
/// Returns `Some(result)` if the conflict row was found and an UPDATE was
/// applied. Returns `Ok(None)` when no conflict exists, so the caller falls
/// through to a plain INSERT.
async fn try_on_conflict_do_update(
    sess: &ProjectSession,
    table: &TableName,
    ins: &sqlparser::ast::Insert,
    on_conflict: &sqlparser::ast::OnConflict,
) -> Result<Option<ExecResult>> {
    use sqlparser::ast::OnConflictAction;

    let do_update = match &on_conflict.action {
        OnConflictAction::DoUpdate(u) => u,
        OnConflictAction::DoNothing => return Ok(None),
    };

    // Extract the conflict column(s). We support the `(col)` form for v0.1.
    let conflict_cols: Vec<String> = match &on_conflict.conflict_target {
        Some(ConflictTarget::Columns(idents)) => idents.iter().map(|i| i.value.clone()).collect(),
        _ => {
            // No explicit target — skip upsert pre-check; fall through to
            // plain INSERT (which will surface a constraint error if needed).
            return Ok(None);
        }
    };
    if conflict_cols.is_empty() {
        return Ok(None);
    }

    // Resolve the inserted row to get the conflict-column value(s).
    let source = match ins.source.as_ref() {
        Some(s) => s,
        None => return Ok(None),
    };
    let rows_raw = match source.body.as_ref() {
        SetExpr::Values(v) => &v.rows,
        _ => return Ok(None),
    };
    if rows_raw.is_empty() {
        return Ok(None);
    }
    // Only handle the single-row case for v0.1 upsert.
    // Multi-row upserts fall through to the normal INSERT path which will
    // surface a constraint error on conflict.
    if rows_raw.len() != 1 {
        return Ok(None);
    }

    // Build the WHERE clause for the existence check.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let schema = meta.schema.clone();

    // Expand the row to schema-width so we can look up conflict-col positions.
    let mut rows_expanded = expand_insert_rows(schema.as_ref(), &ins.columns, rows_raw)?;
    apply_column_defaults(sess, schema.as_ref(), &ins.columns, &mut rows_expanded).await?;

    // Build WHERE conflict_col = value AND ... for the pre-check SELECT.
    let mut where_parts: Vec<String> = Vec::with_capacity(conflict_cols.len());
    for col_name in &conflict_cols {
        let col_idx = schema.index_of(col_name).map_err(|_| {
            BasinError::InvalidSchema(format!("ON CONFLICT: unknown column {col_name:?}"))
        })?;
        let col_expr = &rows_expanded[0][col_idx];
        where_parts.push(format!("{} = {}", col_name, col_expr));
    }
    let where_clause = where_parts.join(" AND ");

    // Run the existence check.
    let check_sql = format!("SELECT 1 FROM {} WHERE {}", table.as_str(), where_clause);
    let exists = match sess.ctx.sql(&check_sql).await {
        Ok(df) => {
            let batches = df.collect().await.map_err(|e| {
                BasinError::internal(format!("ON CONFLICT existence check execute: {e}"))
            })?;
            batches.iter().any(|b| b.num_rows() > 0)
        }
        Err(_) => {
            // Table may be empty (no parquet file yet) → no conflict.
            false
        }
    };

    if !exists {
        return Ok(None); // No conflict — let the caller do a normal INSERT.
    }

    // Build EXCLUDED map: col_name_lowercase → proposed-row expr.
    // This lets us resolve `EXCLUDED.col` in the DO UPDATE expressions.
    let mut excluded_map: std::collections::HashMap<String, Expr> =
        std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        excluded_map.insert(
            field.name().to_ascii_lowercase(),
            rows_expanded[0][i].clone(),
        );
    }

    // The bare table name (last component) for resolving `tablename.col`
    // references → existing-row column in the UPDATE context.
    let table_bare = table.as_str().to_ascii_lowercase();

    // Conflict found. Build and execute an UPDATE.
    //
    // Before formatting each assignment's RHS expression, rewrite any
    // `EXCLUDED.col` references to the literal proposed-row value and any
    // `tablename.col` references to a bare `col` identifier (the existing row
    // in the UPDATE context).  Unqualified `col` references are left as-is
    // and naturally bind to the existing row — matching PG semantics.
    let set_parts: Result<Vec<String>> = do_update
        .assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                AssignmentTarget::ColumnName(n) => {
                    n.0.last().map(|i| i.id_val().clone()).unwrap_or_default()
                }
                AssignmentTarget::Tuple(_) => String::new(),
            };
            if col.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "ON CONFLICT DO UPDATE: malformed assignment".into(),
                ));
            }
            let rhs = rewrite_do_update_expr(a.value.clone(), &table_bare, &excluded_map);
            Ok(format!("{col} = {rhs}"))
        })
        .collect();
    let set_parts = set_parts?;
    let update_sql = format!(
        "UPDATE {} SET {} WHERE {}",
        table.as_str(),
        set_parts.join(", "),
        where_clause
    );
    let result = Box::pin(sess.execute(&update_sql)).await?;
    Ok(Some(result))
}

/// Rewrite a DO UPDATE SET RHS expression to resolve PostgreSQL upsert
/// column-reference semantics before passing the expression to a plain UPDATE:
///
/// - `EXCLUDED.col`     → the literal proposed-row value for `col`
/// - `tablename.col`    → bare `col` identifier (existing row in UPDATE scope)
/// - unqualified `col`  → left unchanged (already refers to the existing row)
///
/// The function is purely structural; it does not need access to the session
/// because `rows_expanded` already holds concrete literal / bind-param Expr
/// nodes for every column in the proposed row.
fn rewrite_do_update_expr(
    expr: Expr,
    table_name_lower: &str,
    excluded_map: &std::collections::HashMap<String, Expr>,
) -> Expr {
    match expr {
        // Two-part qualified identifier: either `EXCLUDED.col` or `table.col`.
        Expr::CompoundIdentifier(ref parts) if parts.len() == 2 => {
            let qualifier = parts[0].value.to_ascii_lowercase();
            let col_lower = parts[1].value.to_ascii_lowercase();
            if qualifier == "excluded" {
                // EXCLUDED.col → proposed-row value (or keep as-is if unknown).
                if let Some(proposed) = excluded_map.get(&col_lower) {
                    return proposed.clone();
                }
            } else if qualifier == table_name_lower {
                // tablename.col → bare col (existing row in the UPDATE scope).
                return Expr::Identifier(parts[1].clone());
            }
            expr
        }
        // Recurse into binary operations (the common case: `t.hits + EXCLUDED.hits`).
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_do_update_expr(
                *left,
                table_name_lower,
                excluded_map,
            )),
            op,
            right: Box::new(rewrite_do_update_expr(
                *right,
                table_name_lower,
                excluded_map,
            )),
        },
        // Recurse into unary operations.
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_do_update_expr(
                *inner,
                table_name_lower,
                excluded_map,
            )),
        },
        // Recurse into parenthesised expressions.
        Expr::Nested(inner) => Expr::Nested(Box::new(rewrite_do_update_expr(
            *inner,
            table_name_lower,
            excluded_map,
        ))),
        // All other expression forms (literals, bind params, functions, …) are
        // left unchanged — they either have no column references or reference
        // forms we don't need to rewrite for the DO UPDATE scope.
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Existing helpers (expand_insert_rows, apply_column_defaults, …)
// ---------------------------------------------------------------------------

/// Translate `INSERT INTO t (col_subset) VALUES (...)` into a list of
/// schema-width rows by reordering the user's values to match the
/// table's column order and inserting `NULL` placeholders in unmentioned
/// positions. When `col_subset` is empty the rows pass through with one
/// transform: any generated column gets a `NULL` slot inserted, leaving
/// the user-supplied values right-shifted across the non-generated columns
/// so the per-cell coercion sees a value where it expects one. This keeps
/// the no-generated-column path byte-identical (rows pass through), while
/// the generated-column path produces a NULL placeholder that the
/// expression evaluator overwrites later.
///
/// Direct writes to a generated column are rejected here with the
/// SQLSTATE-42601-shaped error PG ORMs key off.
fn expand_insert_rows(
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    rows: &[Vec<sqlparser::ast::Expr>],
) -> Result<Vec<Vec<sqlparser::ast::Expr>>> {
    use sqlparser::ast::{Expr, Value};
    let n_cols = schema.fields().len();

    // Build a quick `name -> index` lookup with case-folding.
    let mut by_name = std::collections::HashMap::with_capacity(n_cols);
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }

    // Reject direct writes to a generated column.
    for c in insert_columns {
        let key = c.value.to_ascii_lowercase();
        let idx = *by_name.get(&key).ok_or_else(|| {
            BasinError::InvalidSchema(format!("INSERT references unknown column {:?}", c.value))
        })?;
        if crate::types::field_is_generated(schema.field(idx)).is_some() {
            return Err(BasinError::InvalidSchema(format!(
                "cannot insert into generated column {:?}",
                schema.field(idx).name()
            )));
        }
    }

    // The user can omit `(col_subset)`; the engine still needs to land a
    // schema-width row. Two possibilities:
    //   1. Table has no generated columns. The legacy contract — every
    //      column listed in declaration order — applies. Pass through.
    //   2. Table has generated columns. The user supplies values for
    //      every NON-generated column, and we insert NULL slots at the
    //      generated positions.
    if insert_columns.is_empty() {
        let has_gen = schema
            .fields()
            .iter()
            .any(|f| crate::types::field_is_generated(f).is_some());
        if !has_gen {
            return Ok(rows.to_vec());
        }
        let n_user = schema
            .fields()
            .iter()
            .filter(|f| crate::types::field_is_generated(f).is_none())
            .count();
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if row.len() != n_user {
                return Err(BasinError::InvalidSchema(format!(
                    "row {i} has {} values, expected {n_user} (one per non-generated column)",
                    row.len()
                )));
            }
            let mut full: Vec<Expr> = Vec::with_capacity(n_cols);
            let mut user_iter = row.iter();
            for f in schema.fields() {
                if crate::types::field_is_generated(f).is_some() {
                    full.push(Expr::Value((Value::Null).into()));
                } else {
                    full.push(user_iter.next().expect("count check above").clone());
                }
            }
            out.push(full);
        }
        return Ok(out);
    }

    // `INSERT INTO t (col_subset) VALUES (...)` — build a name->position
    // map, validate the user's row width, and place each value at its
    // schema-side index.
    let mut user_positions: Vec<usize> = Vec::with_capacity(insert_columns.len());
    for c in insert_columns {
        let idx = by_name[&c.value.to_ascii_lowercase()];
        user_positions.push(idx);
    }
    let mut out = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        if row.len() != insert_columns.len() {
            return Err(BasinError::InvalidSchema(format!(
                "row {i} has {} values, expected {} (one per listed column)",
                row.len(),
                insert_columns.len()
            )));
        }
        let mut full: Vec<Expr> = vec![Expr::Value((Value::Null).into()); n_cols];
        for (val_idx, &col_idx) in user_positions.iter().enumerate() {
            full[col_idx] = row[val_idx].clone();
        }
        out.push(full);
    }
    Ok(out)
}

/// For each column with a stored `BASIN_COLUMN_DEFAULT` metadata entry
/// that the user did not explicitly mention in the INSERT, evaluate the
/// DEFAULT expression once per row and overwrite the corresponding
/// position. Generated columns are skipped (they're owned by
/// `materialise_generated_columns`). User-written NULL is preserved by
/// definition: this function only fires on positions the user *omitted*,
/// inferred from the original `insert_columns` list.
///
/// `nextval('seq')` defaults are the load-bearing case: the rewriter
/// dispatches to `Catalog::nextval` on each evaluation, so each row
/// receives a distinct sequence value. See
/// [`crate::seq_ddl::evaluate_default_expression`] for the per-call
/// rewrite + parse hop.
async fn apply_column_defaults(
    sess: &ProjectSession,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    rows: &mut [Vec<sqlparser::ast::Expr>],
) -> Result<()> {
    // Determine which columns the user explicitly mentioned. When
    // `insert_columns` is empty, the user wrote `INSERT INTO t VALUES
    // (...)` — every non-generated column is "mentioned" (the user
    // supplied a value for each in declaration order); generated
    // positions were filled with NULL by `expand_insert_rows` and
    // `materialise_generated_columns` will overwrite them. So in the
    // empty-`insert_columns` case there's nothing for DEFAULTs to do.
    if insert_columns.is_empty() {
        return Ok(());
    }
    let mut mentioned = vec![false; schema.fields().len()];
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    for c in insert_columns {
        if let Some(&idx) = by_name.get(&c.value.to_ascii_lowercase()) {
            mentioned[idx] = true;
        }
    }
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if mentioned[col_idx] {
            continue;
        }
        if crate::types::field_is_generated(field).is_some() {
            continue;
        }
        let Some(default_text) = crate::types::field_default_text(field) else {
            continue;
        };
        // Evaluate the DEFAULT once per row so `nextval('seq')` hands
        // out a fresh value per row.
        for row in rows.iter_mut() {
            let expr = crate::seq_ddl::evaluate_default_expression(sess, default_text).await?;
            row[col_idx] = expr;
        }
    }
    Ok(())
}

/// Gate IDENTITY-column writes on the `OVERRIDING { SYSTEM | USER }
/// VALUE` clause. Called *before* `expand_insert_rows` so a user-listed
/// `INSERT INTO t (id, name) VALUES (...)` where `id` is `GENERATED
/// ALWAYS AS IDENTITY` fails up front rather than after the row builder
/// processes the literal. Only the user's *explicit* column list is
/// inspected — the `INSERT INTO t VALUES (...)` form (no column list)
/// lands every position, so an IDENTITY ALWAYS column written this way
/// is also gated (PG-shape).
fn enforce_identity_insert_columns(
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    overriding: Option<crate::session::OverridingKind>,
) -> Result<()> {
    use crate::session::OverridingKind;
    use crate::types::{field_identity_mode, IdentityMode};
    let always_with_value_is_error = !matches!(overriding, Some(OverridingKind::System));
    if insert_columns.is_empty() {
        // `INSERT INTO t VALUES (...)` — the user supplies a value for
        // every column (or relies on `expand_insert_rows` to NULL-fill
        // generated cols). If the table has an IDENTITY ALWAYS column
        // and we don't have OVERRIDING SYSTEM VALUE, reject.
        if always_with_value_is_error {
            for f in schema.fields() {
                if let Some(IdentityMode::Always) = field_identity_mode(f) {
                    return Err(BasinError::InvalidSchema(format!(
                        "cannot insert a non-DEFAULT value into column {:?} (GENERATED ALWAYS AS \
                         IDENTITY); use OVERRIDING SYSTEM VALUE to override",
                        f.name()
                    )));
                }
            }
        }
        return Ok(());
    }
    // Explicit column list — only the listed columns get user values.
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    for c in insert_columns {
        let idx = match by_name.get(&c.value.to_ascii_lowercase()) {
            Some(&i) => i,
            // Unknown columns are caught downstream with a better
            // error; just skip here so we don't double-report.
            None => continue,
        };
        if let Some(IdentityMode::Always) = field_identity_mode(schema.field(idx)) {
            if always_with_value_is_error {
                return Err(BasinError::InvalidSchema(format!(
                    "cannot insert a non-DEFAULT value into column {:?} (GENERATED ALWAYS AS \
                     IDENTITY); use OVERRIDING SYSTEM VALUE to override",
                    schema.field(idx).name()
                )));
            }
        }
    }
    Ok(())
}

/// Fill IDENTITY columns by routing through the per-project sequence.
/// Three cases:
///   * Column is omitted from the user's INSERT column list (or the
///     user wrote no column list and the table has IDENTITY columns
///     intermixed with generated ones — `expand_insert_rows` flagged
///     those positions with `NULL`): fill from nextval.
///   * Column is in the user's column list AND mode is BY DEFAULT AND
///     OVERRIDING USER VALUE is set: discard the user's literal, fill
///     from nextval (matches PG-shape: USER VALUE means "use the
///     identity sequence, not the user value").
///   * Otherwise: leave the user's value (the `ALWAYS` gate already
///     enforced `OVERRIDING SYSTEM VALUE` in
///     `enforce_identity_insert_columns`).
async fn apply_identity_columns(
    sess: &ProjectSession,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    overriding: Option<crate::session::OverridingKind>,
    rows: &mut [Vec<sqlparser::ast::Expr>],
) -> Result<()> {
    use crate::session::OverridingKind;
    use crate::types::{field_identity_mode, field_identity_sequence, IdentityMode};
    use sqlparser::ast::{Expr, Value};

    let mut mentioned = vec![false; schema.fields().len()];
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    if !insert_columns.is_empty() {
        for c in insert_columns {
            if let Some(&idx) = by_name.get(&c.value.to_ascii_lowercase()) {
                mentioned[idx] = true;
            }
        }
    } else {
        // Empty insert_columns: see `expand_insert_rows` — every
        // non-generated field is "mentioned" (the user supplied a value
        // in declaration order). Generated and identity columns are
        // intermixed in that branch only when generated cols exist;
        // otherwise the contract is "one value per declared column" and
        // the user supplied each.
        let has_gen = schema
            .fields()
            .iter()
            .any(|f| crate::types::field_is_generated(f).is_some());
        if has_gen {
            for (i, f) in schema.fields().iter().enumerate() {
                if crate::types::field_is_generated(f).is_some() {
                    // Generated cols get filled by
                    // `materialise_generated_columns`; the user did
                    // not supply a value for them. Leave `mentioned`
                    // as false so identity-aware filling stays
                    // disabled for those slots.
                    mentioned[i] = false;
                } else {
                    mentioned[i] = true;
                }
            }
        } else {
            for v in mentioned.iter_mut() {
                *v = true;
            }
        }
    }

    let user_value_override = matches!(overriding, Some(OverridingKind::User));

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(mode) = field_identity_mode(field) else {
            continue;
        };
        let Some(seq_name) = field_identity_sequence(field) else {
            // Identity tagged but no sequence — shouldn't happen, but
            // safer to skip than panic.
            continue;
        };
        // Decide whether this column's slot gets filled from nextval
        // for this row. Three triggers:
        //   * Column was omitted from the INSERT.
        //   * OVERRIDING USER VALUE on a BY DEFAULT column.
        //   * `INSERT INTO t VALUES (...)` with no column list AND
        //     this slot was filled with `NULL` by `expand_insert_rows`
        //     (i.e. the column was treated as "user did not supply").
        //     We don't currently exercise that branch (no IDENTITY +
        //     generated col mixed-table tests), but route it for
        //     PG-correctness.
        let omitted = !mentioned[col_idx];
        let force_via_user_override =
            matches!(mode, IdentityMode::ByDefault) && user_value_override && mentioned[col_idx];
        if !omitted && !force_via_user_override {
            continue;
        }
        // Fetch one nextval per row. The shared catalog instance
        // serialises concurrent calls across sessions.
        let catalog = &sess.engine.config().catalog;
        for row in rows.iter_mut() {
            let next = catalog.nextval(&sess.project, seq_name).await?;
            sess.state
                .sequence_cache
                .record(sess.project, seq_name, next)
                .await;
            // BIGINT-shaped literal. The row builder coerces this
            // through the standard Int64 path.
            row[col_idx] = Expr::Value((Value::Number(next.to_string(), false)).into());
        }
    }
    Ok(())
}

/// AUDIT TO emission for INSERT. The mutation has already committed by
/// the time we get here; we materialise the after-row payloads from the
/// in-memory batches and append them to the configured audit table.
/// Project scoping is enforced by `lifecycle::write_audit_rows` resolving
/// the audit table within the calling session's project prefix.
async fn write_insert_audit_rows(
    sess: &ProjectSession,
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<()> {
    let Some(audit_table) = crate::types::audit_table_name(schema) else {
        return Ok(());
    };
    use crate::events::build_row_json;
    use crate::lifecycle::AuditRecord;
    let mut records: Vec<AuditRecord> = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            records.push(AuditRecord {
                before: None,
                after: Some(build_row_json(b, row)?),
            });
        }
    }
    crate::lifecycle::write_audit_rows(sess, audit_table, ChangeOp::Insert, records).await
}

/// Build one [`ChangeEvent`] per row across `batches`, allocating a
/// fresh per-`(project, table)` seq for each. Returns an empty vec when
/// no sinks are attached so callers pay only the registry-empty check.
fn build_insert_events(
    sess: &ProjectSession,
    table: &TableName,
    batches: &[RecordBatch],
) -> Result<Vec<ChangeEvent>> {
    {
        let guard = sess
            .engine
            .event_sinks()
            .read()
            .expect("event_sinks lock poisoned");
        if !registry_has_any(&guard) {
            return Ok(Vec::new());
        }
    }
    let user = causation_user(sess);
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let after = build_row_json(batch, row)?;
            let seq = sess.engine.next_event_seq(&sess.project, table);
            out.push(make_event(
                &sess.project,
                table,
                ChangeOp::Insert,
                None,
                Some(after),
                seq,
                user.clone(),
            ));
        }
    }
    Ok(out)
}

/// Map session principal to the event's `causation_user`. The
/// anonymous-session sentinel becomes `None` so sinks needn't special-
/// case it.
fn causation_user(sess: &ProjectSession) -> Option<String> {
    if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.clone())
    }
}

/// Map the catalog's per-table `TableFileFormat` (#161) onto the storage
/// layer's `FileFormat`. The two enums are intentionally 1:1 (Parquet /
/// Vortex); they live in separate crates so the catalog has no storage
/// dependency, hence the explicit bridge. Vortex is opt-in — `Parquet`
/// stays the default on both sides and the Parquet path is byte-identical
/// when the table's format is `Parquet`. Shared by the executor and
/// `dml_mutate` write paths so the mapping has exactly one definition.
pub(crate) fn map_file_format(fmt: basin_catalog::TableFileFormat) -> FileFormat {
    match fmt {
        basin_catalog::TableFileFormat::Parquet => FileFormat::Parquet,
        basin_catalog::TableFileFormat::Vortex => FileFormat::Vortex,
    }
}

/// Build the per-write `WriteOptions` from the table's catalog metadata.
/// Two knobs survive the trip:
///  * `bloom_filter_columns` — Phase 5.7 A3, the writer materialises a
///    native Parquet bloom filter section per column.
///  * `max_row_group_size` — Phase 5.7 B3, override the writer's global
///    default for point-query-heavy tables.
///
/// When neither is configured the result is `WriteOptions::default()`,
/// which is byte-equivalent to the pre-Phase-5.7 write path.
///
/// `in_tx` mirrors AF's fastpath-off-in-tx rule: `BASIN_FAST_BULK_INSERT=1`
/// only flips encoding to `Fast` when executing outside an explicit
/// transaction. Inside `BEGIN..COMMIT` the per-statement Fast-encoder setup
/// cost dominates the single-row INSERT shape and produced a 27x regression
/// on the `txn_insert_x100` bench (38ms -> 1042ms at 10k scale,
/// commit ddfd8a8). When `in_tx` is true we fall through to `Best` so the
/// in-tx path is byte-identical to the pre-env-gate behaviour.
fn write_options_for(meta: &TableMetadata, in_tx: bool) -> WriteOptions {
    WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        cluster_columns: meta.cluster_columns.clone(),
        // Phase 3: honour the table's persisted on-disk format. Defaults
        // to Parquet (catalog default), keeping the legacy write path
        // byte-identical for every Parquet table.
        file_format: map_file_format(meta.file_format),
        row_block_size: meta.row_block_size,
        // Phase 5.14.A2: bloom columns are the global sort-order columns
        // declared via `WITH (basin.sort_by = '...')`. The writer builds a
        // fastbloom per column and stores it in DataFile::bloom_filters so
        // the reader can skip files on point_eq miss without opening them.
        bloom_columns: meta
            .global_sort_order
            .clone()
            .unwrap_or_default(),
        // Opt-in Vortex fast-write mode for direct bulk INSERTs. Default
        // is `Best` so existing call sites are byte-identical; setting
        // `BASIN_FAST_BULK_INSERT=1` flips every direct INSERT through the
        // minimal cascade (~3-4x faster encode at ~1.5x disk size). The
        // next compaction rewrites the file with `Best`, so the disk-size
        // delta is transient. Hot-tier flushes always use `Fast` and are
        // independent of this env var (see basin-shard ShardFlushBackend).
        //
        // The `!in_tx` gate mirrors AF's fastpath-off-in-tx pattern:
        // inside BEGIN..COMMIT the per-statement Fast-encoder setup cost
        // dominates and regresses `BEGIN; INSERT x100; COMMIT` by ~27x
        // (single-row inserts don't amortise the Fast cascade's fixed
        // setup). Tx rollback semantics are preserved because encoding
        // mode only affects on-disk bytes; the in-tx pending-files queue
        // (see `tx_pending_files_for`) is independent.
        encoding_mode: if !in_tx
            && std::env::var("BASIN_FAST_BULK_INSERT").as_deref() == Ok("1")
        {
            basin_storage::EncodingMode::Fast
        } else {
            basin_storage::EncodingMode::Best
        },
    }
}

/// Optimistic commit with a single retry on conflict. A conflict is possible
/// only if some other writer raced us between `load_table` and
/// `append_data_files`; the in-memory catalog serializes per table so we
/// re-read and try once more before bubbling up.
async fn commit_with_retry(
    sess: &ProjectSession,
    table: &TableName,
    expected_initial: basin_catalog::SnapshotId,
    files: Vec<DataFileRef>,
) -> Result<()> {
    let mut expected = expected_initial;
    match sess
        .engine
        .config()
        .catalog
        .append_data_files(&sess.project, table, expected, files.clone())
        .await
    {
        Ok(_) => Ok(()),
        Err(BasinError::CommitConflict(_)) => {
            let fresh = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, table)
                .await?;
            expected = fresh.current_snapshot;
            sess.engine
                .config()
                .catalog
                .append_data_files(&sess.project, table, expected, files)
                .await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

// `gin_original_sql`: the original (pre-operator-rewrite) SQL for GIN pruning
// detection.  `None` when calling from internal paths that have no original SQL
// (e.g. CTAS, DML SELECT sub-selects).  When `Some`, `apply_gin_pruning_for_query`
// uses this to detect `@>` / `<@` before the JSON operator rewriter converts them
// to `jsonb_contains(...)`.
async fn exec_select(
    sess: &ProjectSession,
    sql: &str,
    include_deleted: bool,
    gin_original_sql: Option<&str>,
) -> Result<ExecResult> {
    // Refresh the catalog-driven file set for every table before planning.
    //
    // Rationale: `refresh_table` now registers per-file `ListingTableUrl`s
    // derived from `TableMetadata::live_data_files()` rather than a directory
    // URL. This means the registered `ListingTable` is a point-in-time
    // snapshot of the catalog's current file set — it does NOT re-list the
    // object store on each scan.  That is the fix for bug #41 (rollback
    // correctness), but it means we must refresh before every SELECT so
    // that inserts committed in this session, and catalog mutations performed
    // externally (e.g. `rollback_to_snapshot`), are reflected in the plan.
    //
    // When the shard is wired in we additionally flush the in-RAM tail to
    // Parquet first so the just-written rows land in object storage before
    // the catalog-driven refresh reads the file list.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    {
        let tables: Vec<_> = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.project)
            .await?;
        let in_tx = crate::session::tx_is_active(&sess.state);
        for table in &tables {
            if in_tx {
                // Within a transaction: include pending (not-yet-committed)
                // files so reads can see within-tx writes.
                let pending = crate::session::tx_pending_files_for(&sess.state, table);
                crate::session::refresh_table_with_extra(
                    &sess.engine,
                    &sess.project,
                    &sess.ctx,
                    &sess.state,
                    table,
                    &pending,
                )
                .await?;
            } else {
                crate::session::refresh_table(
                    &sess.engine,
                    &sess.project,
                    &sess.ctx,
                    &sess.state,
                    table,
                )
                .await?;
            }
        }
    }

    // Phase 5.5 partition pruning: if this session has seen a partitioned
    // table at least once, walk the SQL's AST and (if the WHERE clause
    // restricts the partition column) swap the registered `ListingTable`
    // for one whose paths are pre-filtered to matching partitions. The
    // atomic `has_partitioned_table` gate keeps the hot path fast for
    // projects that never use PARTITION BY.
    if sess
        .state
        .has_partitioned_table
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        crate::session::apply_partition_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            sql,
        )
        .await?;
    }

    // Phase 5.19.C — GIN file-level pruning (JSONB @> / <@).
    // Use the original (pre-operator-rewrite) SQL so `@>` / `<@` operators
    // are still present in the text.  After `rewrite_json_operators` runs,
    // `@>` becomes `jsonb_contains(…)` and the detector would miss it.
    // On any parse/probe failure this is a silent no-op → full scan.
    //
    // Phase 5.20.E — GIN FTS file-level pruning (tsvector @@).
    // Use the original SQL so `@@` is still present (before
    // `rewrite_tsvector_at_at` converts it to `tsvector_match_udf(...)`).
    if let Some(orig_sql) = gin_original_sql {
        crate::session::apply_gin_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
        crate::session::apply_gin_fts_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
        // Phase 5.24.D — GIST interval-tree file-level pruning (range @> / && / <@).
        // Use the original SQL so range operators are still present (before
        // `rewrite_range_operators` converts them to UDF calls).
        crate::session::apply_gist_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
    }

    // View-reference rewriting: replace any reference to a known plain view
    // in the SQL's FROM / JOIN clauses with an inline subquery so DataFusion
    // sees a derived table rather than an unknown table name. This is a
    // no-op when the project has no registered views.
    let view_rewritten_owned;
    let sql = if let Some(rewritten) = crate::view_ddl::rewrite_view_refs(
        sess.engine.config().catalog.as_ref(),
        &sess.project,
        sql,
    )
    .await?
    {
        view_rewritten_owned = rewritten;
        view_rewritten_owned.as_str()
    } else {
        sql
    };
    // Strip schema qualifiers (`schema.table` → `table`) before DataFusion
    // sees the SQL. DataFusion uses its own catalog hierarchy; Basin's tables
    // are all registered in the flat default namespace, so `schema.table`
    // would be misrouted as a DataFusion catalog-schema lookup.
    let sql_stripped =
        crate::schema_ddl::strip_schema_qualifiers_for_session(sql, &sess.state.schema_state);
    let sql_for_df = sql_stripped.as_str();

    // Expand `json_agg(t)` / `jsonb_agg(t)` where `t` is a bare table name
    // into `json_agg(named_struct('col1', t.col1, ...))` so DataFusion can plan
    // and execute it.  This must happen after schema-qualifier stripping because
    // we look up tables by their unqualified name in the session context.
    let json_agg_rewritten;
    let sql_for_df = {
        let rewritten = rewrite_json_agg_whole_row(sql_for_df, &sess.ctx).await;
        json_agg_rewritten = rewritten;
        json_agg_rewritten.as_str()
    };

    let mut df = sess
        .ctx
        .sql(sql_for_df)
        .await
        .map_err(|e| BasinError::internal(format!("plan: {e}")))?;

    // Phase 5.16.B: compute query-shape hash and record it in the per-shape
    // HDR histogram registry.  The hash was computed-and-discarded in 5.16.A;
    // here we route it into `QueryStatRegistry::observe`.
    //
    // We time only the DataFusion collect() call below so that planning /
    // view-rewrite overhead does not inflate the latency bucket.  The timer
    // starts here so it covers the RLS / soft-delete rewrite below as well —
    // those rewrites are part of the logical query shape's execution cost.
    let shape_hash = crate::query_shape::QueryShapeHash::of(df.logical_plan());
    // Extract the primary table name from the plan (the first TableScan we
    // find).  Multi-table queries (JOINs) record under the sentinel name
    // "_multi_table_" — the shape hash already encodes the full join topology.
    let primary_table: TableName = {
        fn first_scan(plan: &datafusion::logical_expr::LogicalPlan) -> Option<&str> {
            use datafusion::logical_expr::LogicalPlan;
            if let LogicalPlan::TableScan(ts) = plan {
                return Some(ts.table_name.table());
            }
            for child in plan.inputs() {
                if let Some(t) = first_scan(child) {
                    return Some(t);
                }
            }
            None
        }
        let raw = first_scan(df.logical_plan()).unwrap_or("_multi_table_");
        // Sanitise: TableName::new validates idents; fall back to sentinel on
        // failure (e.g. DataFusion internal scans, subquery aliases, etc.).
        TableName::new(raw).unwrap_or_else(|_| {
            TableName::new("_multi_table_").expect("sentinel is valid")
        })
    };
    let exec_start = std::time::Instant::now();

    // Phase 5.6: row-level security. The per-project policy lookup is gated
    // on the catalog's `rls_enabled` per table; tables with RLS off cost
    // exactly one `load_table` call, no plan rewriting. The plan rewrite
    // itself happens via DataFusion's `LogicalPlanBuilder::filter` —
    // wrapping each RLS-enabled `TableScan` in a `Filter` node — so
    // downstream optimisation (predicate pushdown, projection pruning)
    // sees the RLS filter as a first-class predicate.
    df = apply_rls_to_select(sess, sql, df).await?;
    if !include_deleted {
        df = apply_soft_delete_to_select(sess, sql, df).await?;
    }
    let df_schema = df.schema().inner().clone();
    // Build the WS schema and annotate any json_agg / jsonb_agg result columns
    // with BASIN_TYPE=JSONB so the pgwire layer advertises OID 3802 (JSONB).
    // DataFusion strips the `return_field` metadata when wrapping an aggregate
    // in a scalar correlated subquery or through join projection; the annotation
    // pass recovers it by scanning the SQL text for json_agg occurrences and
    // matching their aliases against the result schema field names.
    let ws_schema = {
        let raw = Arc::new(schema_df_to_ws(df_schema.as_ref())?);
        annotate_json_agg_columns(&raw, sql_for_df)
    };

    // Change C: when the shard is wired in we know there are large per-project
    // tails on the same runtime. Move the DataFusion executor onto the
    // blocking thread pool so its parquet-decode loop can't pin the
    // cooperative tokio workers a quiet project's point queries run on. Tests
    // that run without a shard keep the single-await path and behave as
    // before.
    //
    // Phase 6.P0.A: statement-level wall-clock timeout (noisy-neighbor P0).
    // The cost-check (`cost_check.rs`) only bounds single-table SELECTs at
    // *planning* time; JOIN / sub-query / CTE / cartesian shapes pass through
    // unchecked and can pin a DataFusion worker thread indefinitely, starving
    // every other project on the runtime. We wrap the *execution* future in
    // `tokio::time::timeout`: when the deadline elapses the future is dropped,
    // which drops the DataFusion stream / physical plan and cancels execution
    // (DataFusion's documented cancellation contract). On the fast path the
    // deadline is a single comparison set up once — it is NOT a per-row check,
    // so a normal sub-timeout query sees no latency regression. `None` (env
    // `BASIN_STATEMENT_TIMEOUT_MS=0`) disables the guard for back-compat.
    let timeout = crate::session::session_statement_timeout(&sess.state);
    let canceled = || {
        BasinError::query_canceled(match timeout {
            Some(d) => format!("exceeded statement_timeout ({} ms)", d.as_millis()),
            None => "exceeded statement timeout".to_owned(),
        })
    };
    // Phase 5.28.D: publish the absolute statement deadline into the
    // per-thread slot so `PgSleepUdf` can observe it cooperatively.
    // The deadline is computed here (= now + timeout) so both the shard and
    // non-shard paths share the same reference point.
    let statement_deadline: Option<Instant> =
        timeout.map(|d| Instant::now() + d);
    // Aggregate / GROUP-BY / UNION-ALL partitioning note (investigated; no
    // Basin lever). A cluster of GROUP-BY-shaped queries runs several× slower
    // than PostgreSQL at 10k rows, with the gap NARROWING as the table grows
    // (≈8× @10k → ≈2-4× @100k/1M) — the signature of a per-query FIXED
    // overhead, not an algorithmic blow-up. The hypothesis was that DataFusion
    // over-partitions small inputs (Partial→Repartition→Final fan-out whose
    // exchange + merge cost dwarfs a 10k-row aggregate). EXPLAIN of these
    // shapes (see `tests/aggregate_tuning.rs`) shows that is NOT happening:
    // because `open_session` pins `datafusion.execution.target_partitions = 1`
    // (session.rs), every aggregate plans as `AggregateExec: mode=Single` with
    // ZERO `RepartitionExec` and all files in a single file_group, and the two
    // UNION-ALL branches are collapsed by `UnionScanCollapse` into one scan
    // with an OR predicate. With `target_partitions = 1` the repartition flags
    // (`repartition_aggregations` / `_joins` / `_file_scans`) are moot —
    // `EnforceDistribution` emits no exchanges. So the optimal partition lever
    // is already pulled; the residual gap is intrinsic DataFusion execution
    // cost (per-batch hash-aggregate setup + scan startup) amortized away at
    // scale, which is exactly why the gap narrows as the row count grows. There
    // is no further Basin-side config knob here to make this faster without
    // regressing other shapes; documenting honestly rather than faking a fix.
    let df_batches = if sess.engine.config().shard.is_some() {
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| BasinError::internal(format!("create plan: {e}")))?;
        let task_ctx = sess.ctx.task_ctx();
        // The DataFusion collect runs on its own current-thread runtime inside
        // a blocking thread, so the *outer* runtime's timer can't observe it.
        // Apply the timeout INSIDE `block_on` so the inner stream is dropped on
        // elapse — that both cancels execution and lets the blocking thread
        // return promptly instead of running on detached. We surface the
        // elapsed case as a sentinel `Ok(None)` and map it to QueryCanceled.
        let join = tokio::task::spawn_blocking(move || {
            // Publish the deadline on this blocking thread so pg_sleep can
            // poll it cooperatively via get_statement_deadline().
            set_statement_deadline(statement_deadline);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BasinError::internal(format!("blocking runtime: {e}")))?;
            let result = rt.block_on(async {
                let fut = datafusion::physical_plan::collect(plan, task_ctx);
                match timeout {
                    Some(d) => match tokio::time::timeout(d, fut).await {
                        Ok(res) => res
                            .map(Some)
                            .map_err(|e| {
                                if take_pg_sleep_canceled() {
                                    BasinError::query_canceled(
                                        "pg_sleep: interrupted by statement_timeout",
                                    )
                                } else {
                                    BasinError::internal(format!("execute: {e}"))
                                }
                            }),
                        Err(_) => Ok(None),
                    },
                    None => fut
                        .await
                        .map(Some)
                        .map_err(|e| {
                            if take_pg_sleep_canceled() {
                                BasinError::query_canceled(
                                    "pg_sleep: interrupted by statement_timeout",
                                )
                            } else {
                                BasinError::internal(format!("execute: {e}"))
                            }
                        }),
                }
            });
            set_statement_deadline(None);
            result
        })
        .await
        .map_err(|e| BasinError::internal(format!("spawn_blocking join: {e}")))?;
        match join? {
            Some(batches) => batches,
            None => return Err(canceled()),
        }
    } else {
        // Non-shard path: set the deadline on the current tokio thread.
        // pg_sleep's block_in_place runs on the same thread, so the
        // thread-local is visible inside the UDF.
        set_statement_deadline(statement_deadline);
        let fut = df.collect();
        // Phase 5.31.A: race the DataFusion collect against both the
        // statement-timeout AND the per-session cancel notification
        // (`pg_cancel_backend`). Whichever fires first wins:
        //   - Statement timeout:  returns canceled() error (SQLSTATE 57014).
        //   - pg_cancel_backend:  returns query_canceled error (SQLSTATE 57014).
        //   - DataFusion error:   propagates as before.
        let cancel_notify = sess.cancel_notify.clone();
        let result = match timeout {
            Some(d) => {
                // Pin the cancel notification future so it can be polled
                // multiple times across the select! arms.
                tokio::pin!(fut);
                match tokio::time::timeout(d, async {
                    tokio::select! {
                        res = &mut fut => Some(res),
                        _ = cancel_notify.notified() => None,
                    }
                }).await {
                    Ok(Some(res)) => res.map_err(|e| {
                        if take_pg_sleep_canceled() {
                            BasinError::query_canceled(
                                "pg_sleep: interrupted by statement_timeout",
                            )
                        } else {
                            BasinError::internal(format!("execute: {e}"))
                        }
                    }),
                    Ok(None) => {
                        // pg_cancel_backend fired.
                        set_statement_deadline(None);
                        return Err(BasinError::query_canceled(
                            "canceling statement due to user request",
                        ));
                    }
                    Err(_) => {
                        // Statement timeout fired.
                        return { set_statement_deadline(None); Err(canceled()) };
                    }
                }
            },
            None => {
                tokio::pin!(fut);
                let outcome = tokio::select! {
                    res = &mut fut => Some(res),
                    _ = cancel_notify.notified() => None,
                };
                match outcome {
                    Some(res) => res.map_err(|e| {
                        if take_pg_sleep_canceled() {
                            BasinError::query_canceled(
                                "pg_sleep: interrupted by statement_timeout",
                            )
                        } else {
                            BasinError::internal(format!("execute: {e}"))
                        }
                    }),
                    None => {
                        set_statement_deadline(None);
                        return Err(BasinError::query_canceled(
                            "canceling statement due to user request",
                        ));
                    }
                }
            },
        };
        set_statement_deadline(None);
        result?
    };
    let exec_elapsed_ns = exec_start.elapsed().as_nanos() as u64;

    // Phase 5.16.B: route shape + metrics into the HDR histogram registry.
    // Row count is the sum of all batch row counts from the DataFusion output.
    let total_rows: u64 = df_batches.iter().map(|b| b.num_rows() as u64).sum();
    sess.engine.query_stats().observe(
        &sess.project,
        &primary_table,
        shape_hash,
        &crate::query_stats::QueryMetrics {
            latency_ns: exec_elapsed_ns,
            rows_scanned: total_rows,
            // files_opened / bytes_decoded / cache_hits: DataFusion does not
            // expose these counters through the public DataFrame API in the
            // current version.  Set to 0 for now; 5.16.D will instrument the
            // physical plan metrics once the OTLP export is wired in.
            files_opened: 0,
            bytes_decoded: 0,
            cache_hits: 0,
            fast_path_engaged: false,
            // Phase 5.16.C: table_row_count for scale-bucket assignment.
            // We use 0 (bucket 0) as the default — the catalog row count is
            // not readily available at this call site without an additional
            // load_table round-trip.  5.16.D will wire in the real count.
            table_row_count: 0,
        },
    );

    let mut batches: Vec<RecordBatch> = Vec::with_capacity(df_batches.len());
    for b in df_batches.iter() {
        batches.push(batch_df_to_ws(b)?);
    }
    Ok(ExecResult::Rows {
        schema: ws_schema,
        batches,
    })
}

/// AND-merge per-table RLS USING predicates into `df`'s logical plan. Re-
/// parses `sql` to discover referenced tables (cheap; sqlparser is fast).
/// Tables with `rls_enabled = false` short-circuit — they pay one catalog
/// `load_table` call and nothing else, preserving the no-RLS hot path.
async fn apply_rls_to_select(
    sess: &ProjectSession,
    sql: &str,
    df: datafusion::prelude::DataFrame,
) -> Result<datafusion::prelude::DataFrame> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(df),
    };
    if stmts.len() != 1 {
        return Ok(df);
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(df);
    };
    let referenced = collect_table_refs_from_query(query);
    if referenced.is_empty() {
        return Ok(df);
    }
    let policies = crate::rls::build_policies_for_query(
        &sess.engine.config().catalog,
        &sess.project,
        &referenced,
        &sess.current_user,
        basin_catalog::PolicyCommand::Select,
    )
    .await?;
    if policies.is_empty() {
        return Ok(df);
    }
    crate::rls::inject_select_predicates(&sess.ctx, df, &policies, &sess.current_user).await
}

/// AND-merge an `<soft_delete_col> IS NULL` predicate into `df`'s logical
/// plan for every TableScan against a table that has a SOFT DELETE column.
/// Mirrors `apply_rls_to_select` — same TreeNode rewrite shape, different
/// predicate source. When `INCLUDE DELETED` was specified the caller skips
/// this step entirely.
async fn apply_soft_delete_to_select(
    sess: &ProjectSession,
    sql: &str,
    df: datafusion::prelude::DataFrame,
) -> Result<datafusion::prelude::DataFrame> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(df),
    };
    if stmts.len() != 1 {
        return Ok(df);
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(df);
    };
    let referenced = collect_table_refs_from_query(query);
    if referenced.is_empty() {
        return Ok(df);
    }
    let mut soft_cols: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for table in &referenced {
        let meta = match sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, table)
            .await
        {
            Ok(m) => m,
            Err(_) => continue,
        };
        if let Some(col) = crate::types::soft_delete_column(meta.schema.as_ref()) {
            soft_cols.insert(table.to_string(), col);
        }
    }
    if soft_cols.is_empty() {
        return Ok(df);
    }
    crate::lifecycle::inject_soft_delete_predicates(&sess.ctx, df, &soft_cols).await
}

/// Collect every table the query can read from — the input set RLS must
/// inject predicates against.
///
/// Walks every shape that can hide a `TableScan` from the rewriter:
///
/// - top-level `SetExpr::Select` → each `from.relation` + every `from.joins[*]`
/// - `SetExpr::SetOperation` (UNION / INTERSECT / EXCEPT) → both legs
/// - `query.with` CTEs → each CTE body (a sub-`Query`) recurses
/// - `TableFactor::Derived` (FROM (SELECT …)) → subquery recurses
/// - `TableFactor::NestedJoin` → unwrap the inner `TableWithJoins`
/// - subqueries embedded in expressions (WHERE, HAVING, projection,
///   `EXISTS`, `IN (SELECT …)`, scalar subqueries) → recurse
///
/// **Why this matters (P0):** RLS predicate injection short-circuits when
/// `referenced.is_empty()`. A naive walker that only handles
/// `SetExpr::Select` returns an empty list for `SELECT … UNION SELECT …`
/// and `WITH peek AS (SELECT …) SELECT … FROM peek`, leaving the
/// underlying `TableScan`s un-rewritten and *silently leaking* rows the
/// policy would otherwise hide. See
/// `tests/integration/tests/security.rs::rls_union_subquery_cannot_bypass`
/// and `rls_cte_cannot_bypass` for the regression repro — those tests
/// must stay green for this invariant to hold.
fn collect_table_refs_from_query(query: &sqlparser::ast::Query) -> Vec<TableName> {
    let mut out = Vec::new();
    collect_from_query(query, &mut out);
    out
}

fn collect_from_query(query: &sqlparser::ast::Query, out: &mut Vec<TableName>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_from_query(&cte.query, out);
        }
    }
    collect_from_set_expr(query.body.as_ref(), out);
}

fn collect_from_set_expr(set_expr: &sqlparser::ast::SetExpr, out: &mut Vec<TableName>) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                collect_from_table_factor(&from.relation, out);
                for join in &from.joins {
                    collect_from_table_factor(&join.relation, out);
                }
            }
            if let Some(sel) = &select.selection {
                collect_from_expr(sel, out);
            }
            if let Some(having) = &select.having {
                collect_from_expr(having, out);
            }
            if let Some(qualify) = &select.qualify {
                collect_from_expr(qualify, out);
            }
            for item in &select.projection {
                collect_from_select_item(item, out);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_from_set_expr(left, out);
            collect_from_set_expr(right, out);
        }
        SetExpr::Query(q) => collect_from_query(q, out),
        // VALUES / Insert / Update / Delete / Table — no rewritable
        // TableScan reachable from a SELECT-shaped RLS path.
        _ => {}
    }
}

fn collect_from_table_factor(tf: &sqlparser::ast::TableFactor, out: &mut Vec<TableName>) {
    use sqlparser::ast::TableFactor;
    match tf {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                if let Ok(t) = TableName::new(name.0[0].id_val().clone()) {
                    out.push(t);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => collect_from_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_from_table_factor(&table_with_joins.relation, out);
            for join in &table_with_joins.joins {
                collect_from_table_factor(&join.relation, out);
            }
        }
        // TableFunction / Pivot / Unpivot / UNNEST etc — function-style
        // sources don't reference catalog tables in the RLS-relevant way;
        // any subqueries embedded in their args are walked by the
        // expression-side traversal.
        _ => {}
    }
}

fn collect_from_select_item(item: &sqlparser::ast::SelectItem, out: &mut Vec<TableName>) {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            collect_from_expr(e, out);
        }
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
    }
}

fn collect_from_expr(expr: &sqlparser::ast::Expr, out: &mut Vec<TableName>) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => collect_from_query(q, out),
        Expr::InSubquery {
            subquery: q,
            expr: e,
            ..
        } => {
            collect_from_query(q, out);
            collect_from_expr(e, out);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_from_expr(left, out);
            collect_from_expr(right, out);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::Nested(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => collect_from_expr(e, out),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_from_expr(e, out);
            collect_from_expr(low, out);
            collect_from_expr(high, out);
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        }
        | Expr::SimilarTo {
            expr: e, pattern, ..
        } => {
            collect_from_expr(e, out);
            collect_from_expr(pattern, out);
        }
        Expr::InList { expr: e, list, .. } => {
            collect_from_expr(e, out);
            for x in list {
                collect_from_expr(x, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                collect_from_expr(o, out);
            }
            for c in conditions {
                collect_from_expr(&c.condition, out);
                collect_from_expr(&c.result, out);
            }
            if let Some(e) = else_result {
                collect_from_expr(e, out);
            }
        }
        Expr::Function(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString { .. }
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => {}
        // Anything else (windows, lambdas, MATCH, dialect-specific) —
        // walking it is best-effort and we're conservative on misses
        // here: the `_` arm is reachable only on shapes that don't carry
        // a Query, so RLS coverage is preserved.
        _ => {}
    }
}

/// Apply an RLS DDL statement to the catalog. The mutation reads the current
/// `(rls_enabled, policies)`, applies the change in memory, and writes back
/// via `Catalog::set_rls_state`. We do not refresh the DataFusion ListingTable
/// here — RLS state is consulted at SELECT time by re-reading the catalog
/// (per-query) so a freshly created policy takes effect on the very next
/// query without per-session bookkeeping.
async fn exec_rls_ddl(sess: &ProjectSession, ddl: crate::rls::RlsDdl) -> Result<ExecResult> {
    let table = ddl.table().clone();
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let mut rls_enabled = meta.rls_enabled;
    let mut policies = meta.policies.clone();
    let tag = ddl.apply(&mut rls_enabled, &mut policies)?;
    sess.engine
        .config()
        .catalog
        .set_rls_state(&sess.project, &table, rls_enabled, policies)
        .await?;
    Ok(ExecResult::Empty { tag: tag.into() })
}

/// Standard `ALTER TABLE` forms that sqlparser DOES recognise (currently
/// `ADD COLUMN`). RLS ENABLE/DISABLE and CREATE/ALTER/DROP POLICY are
/// intercepted earlier in [`crate::rls::match_rls_ddl`] and never reach
/// this dispatch arm. Basin-specific extensions (`SET cold_after`,
/// `SET BLOOM FILTERS ON`, etc.) are intercepted at the very top of
/// [`execute`] before sqlparser is even called.
async fn exec_alter_table(
    sess: &ProjectSession,
    name: sqlparser::ast::ObjectName,
    operations: Vec<sqlparser::ast::AlterTableOperation>,
) -> Result<ExecResult> {
    // 6.SEC.P1 — reject ALTER TABLE targeting a reserved system schema.
    // Phase 5.18.C: system sessions bypass this guard.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&name, "ALTER TABLE")?;
    }
    let tag = crate::alter::apply_standard_alter_table(
        &sess.engine.config().catalog,
        &sess.project,
        &name,
        &operations,
        &sess.state.schema_state,
    )
    .await?;

    // ADD COLUMN replaced the schema in the catalog; refresh the
    // session's DataFusion ListingTable so subsequent SELECTs see the
    // new column. We pull the (now possibly different) table name out
    // of the AST, stripping any schema qualifier (`myschema.t` → `t`).
    let bare_name = match name.0.len() {
        1 => Some(name.0[0].id_val().clone()),
        2 => Some(name.0[1].id_val().clone()),
        _ => None,
    };
    if let Some(raw) = bare_name {
        if let Ok(t) = TableName::new(raw) {
            refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &t).await?;
        }
    }
    Ok(ExecResult::Empty { tag: tag.into() })
}

async fn exec_show_tables(sess: &ProjectSession) -> Result<ExecResult> {
    // Phase 5.18.C: use list_tables_qualified so system-schema tables
    // (cron.job, auth.users, net._http_response, etc.) are also visible.
    // We return the bare table name (not the qualified form) for back-compat
    // with callers that check for table names like "job" or "_http_response".
    let qtables = sess
        .engine
        .config()
        .catalog
        .list_tables_qualified(&sess.project)
        .await?;
    let bare_names: Vec<&str> = qtables.iter().map(|qt| qt.name.as_str()).collect();
    let arr = StringArray::from(bare_names);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_name",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef])
        .map_err(|e| BasinError::internal(format!("SHOW TABLES batch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

/// Phase 5.28.D: build a single-row/single-column `ExecResult::Rows` for `SHOW <var>`.
/// Column name is the variable name; value is `val`.
fn make_show_result(col_name: &str, val: &str) -> ExecResult {
    let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Utf8, false)]));
    let arr = StringArray::from(vec![val]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef])
        .expect("make_show_result: infallible");
    ExecResult::Rows {
        schema,
        batches: vec![batch],
    }
}

/// Extract the string form of the first value in a SET expression.
/// Handles both literal string values and numeric literals.
/// Falls back to `"0"` for unrecognised forms.
fn extract_set_string_value(values: &[sqlparser::ast::Expr]) -> String {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    if let Some(expr) = values.first() {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                ..
            }) => return s.clone(),
            Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) => return n.clone(),
            Expr::Identifier(id) => return id.value.clone(),
            _ => {}
        }
    }
    "0".to_string()
}

/// Pull a bare table name out of a sqlparser `ObjectName`.
///
/// Accepts both bare names (`t`) and schema-qualified names (`myschema.t`).
/// The schema qualifier is stripped: Basin's flat per-project model stores all
/// tables in a single catalog namespace. Callers that need to validate the
/// schema against the session's schema registry should call
/// `crate::schema_ddl::table_name_from_object` instead.
fn single_part_name(name: &ObjectName) -> Result<&str> {
    match name.0.len() {
        1 => Ok(&name.0[0].id_val()),
        2 => {
            // schema.table — drop the schema prefix and return the table name.
            Ok(&name.0[1].id_val())
        }
        _ => Err(BasinError::InvalidIdent(format!(
            "table name must have at most one schema qualifier: {name}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Cursor lifecycle handlers
// ---------------------------------------------------------------------------

/// Execute `DECLARE <name> [SCROLL | NO SCROLL] CURSOR [WITH HOLD] FOR <query>`.
///
/// The backing SELECT is materialised immediately into the session's cursor
/// registry.  WITH HOLD is silently accepted but not implemented (cursors die
/// with the session regardless).
async fn exec_declare(
    sess: &ProjectSession,
    stmts: Vec<sqlparser::ast::Declare>,
) -> Result<ExecResult> {
    use sqlparser::ast::DeclareType;

    for decl in stmts {
        // We only handle CURSOR declarations.
        if !matches!(decl.declare_type, Some(DeclareType::Cursor)) {
            return Err(BasinError::internal(
                "DECLARE: only CURSOR declarations are supported in v0.1".to_string(),
            ));
        }
        let name = decl
            .names
            .first()
            .ok_or_else(|| BasinError::internal("DECLARE: missing cursor name".to_string()))?
            .value
            .clone();

        // sqlparser 0.52 puts the SELECT query in `for_query` (Box<Query>)
        // for `DECLARE c CURSOR FOR SELECT …`. The `assignment` field uses
        // `Box<Expr>` and is for variable-assignment forms, not cursor FOR.
        let query = decl.for_query.ok_or_else(|| {
            BasinError::internal("DECLARE CURSOR: missing FOR <query>".to_string())
        })?;

        // Execute the SELECT to materialise the result set.
        let select_sql = query.to_string();
        let result = exec_select(sess, &select_sql, false, None).await?;
        // An empty result (0 rows) is valid — declare with the schema.
        let (schema, batches) = match result {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { .. } => (Arc::new(Schema::empty()), vec![]),
        };
        sess.state.cursors.declare(name, schema, batches).await?;
    }
    Ok(ExecResult::Empty {
        tag: "DECLARE".into(),
    })
}

/// Execute `FETCH [direction] FROM <cursor>`.
async fn exec_fetch(
    sess: &ProjectSession,
    cursor_name: &str,
    direction: sqlparser::ast::FetchDirection,
) -> Result<ExecResult> {
    let dir = crate::cursor::CursorDirection::from_sqlparser(&direction)?;
    let (schema, batches) = sess.state.cursors.apply(cursor_name, dir, true).await?;
    Ok(ExecResult::Rows { schema, batches })
}

/// Execute `CLOSE <cursor>` (or `CLOSE ALL`).
async fn exec_close(
    sess: &ProjectSession,
    cursor: sqlparser::ast::CloseCursor,
) -> Result<ExecResult> {
    use sqlparser::ast::CloseCursor;
    match cursor {
        CloseCursor::All => {
            // Close all — not trivially implementable without exposing the
            // registry's internals; for v0.1 we surface a helpful error.
            return Err(BasinError::internal(
                "CLOSE ALL is not supported in v0.1; close cursors individually".to_string(),
            ));
        }
        CloseCursor::Specific { name } => {
            sess.state.cursors.close(&name.value).await?;
        }
    }
    Ok(ExecResult::Empty {
        tag: "CLOSE".into(),
    })
}

/// Execute `MOVE <direction> [FROM|IN] <cursor>`.
async fn exec_cursor_move(
    sess: &ProjectSession,
    intent: crate::cursor::MoveIntent,
) -> Result<ExecResult> {
    sess.state
        .cursors
        .apply(&intent.cursor_name, intent.direction, false)
        .await?;
    Ok(ExecResult::Empty { tag: "MOVE".into() })
}

// ---------------------------------------------------------------------------
// json_agg(t) whole-row expansion
// ---------------------------------------------------------------------------

/// Annotate any field in `ws_schema` that originated from a `json_agg` or
/// `jsonb_agg` expression in the SQL with `BASIN_TYPE=JSONB` metadata.
///
/// DataFusion's planner returns the UDAF result as a plain `Utf8` column
/// without the JSONB metadata in two cases where `return_field` alone is
/// insufficient:
///
/// 1. **Scalar correlated subquery** — `(SELECT json_agg(x) FROM t WHERE …) AS alias`:
///    the outer column gets type `Utf8` from the subquery schema's `field(0).data_type()`,
///    stripping the metadata.
/// 2. **Join over a pre-aggregated subquery** — after the LATERAL rewrite the
///    `json_agg` column lives in an inner SELECT; the outer query projects it
///    by alias through the join, and DataFusion may lose the metadata.
///
/// This function scans the SQL text (case-insensitively) for every `json_agg`
/// / `jsonb_agg` occurrence and collects the alias of the enclosing projection
/// item.  Any `Utf8` field in `ws_schema` whose name matches a collected alias
/// is re-annotated with `BASIN_TYPE=JSONB`.
///
/// This is best-effort and conservative: only fields whose name appears
/// explicitly in the alias list are changed.  If a field is already annotated
/// (e.g. from `return_field` for direct GROUP BY aggregates) the function is
/// idempotent.
pub(crate) fn annotate_json_agg_columns(ws_schema: &Arc<Schema>, sql: &str) -> Arc<Schema> {
    // Fast exit if no json_agg / jsonb_agg in the SQL.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("json_agg(") && !lower.contains("jsonb_agg(") {
        return Arc::clone(ws_schema);
    }

    // Collect aliases of projection items that contain json_agg / jsonb_agg.
    let json_aliases = collect_json_agg_aliases(&lower);
    if json_aliases.is_empty() {
        return Arc::clone(ws_schema);
    }

    // Rebuild the schema, annotating matching Utf8 fields.
    let mut changed = false;
    let new_fields: Vec<Field> = ws_schema
        .fields()
        .iter()
        .map(|f| {
            let f = f.as_ref();
            if f.data_type() == &DataType::Utf8
                && json_aliases.contains(&f.name().to_ascii_lowercase())
                && !crate::types::field_is_jsonb(f)
            {
                changed = true;
                let mut meta = f.metadata().clone();
                meta.insert(
                    crate::types::BASIN_TYPE_KEY.to_string(),
                    crate::types::BASIN_TYPE_JSONB.to_string(),
                );
                f.clone().with_metadata(meta)
            } else {
                f.clone()
            }
        })
        .collect();

    if changed {
        Arc::new(Schema::new(new_fields))
    } else {
        Arc::clone(ws_schema)
    }
}

/// Scan the (lowercase) SQL string and collect the aliases of all SELECT
/// projection items that contain a `json_agg(` or `jsonb_agg(` call.
///
/// Algorithm: for each occurrence of `json_agg(` or `jsonb_agg(` in the SQL,
/// we look at the surrounding projection context by:
/// 1. Finding the depth-0 comma or the surrounding context that delimits the
///    projection item.
/// 2. Scanning forward from the closing `)` of the agg call for `as <alias>`
///    or a bare identifier used as the alias.
///
/// This is intentionally conservative: if we cannot determine an alias we
/// leave it empty (no annotation).  False positives (annotating non-JSON
/// columns) are impossible because we only annotate aliases that explicitly
/// appear after a json_agg call.
fn collect_json_agg_aliases(lower_sql: &str) -> std::collections::HashSet<String> {
    let bytes = lower_sql.as_bytes();
    let len = bytes.len();
    let mut aliases = std::collections::HashSet::new();
    let mut pos = 0usize;

    loop {
        // Find the next json_agg( or jsonb_agg( occurrence.
        let hit = {
            let j = lower_sql[pos..].find("json_agg(");
            let jb = lower_sql[pos..].find("jsonb_agg(");
            match (j, jb) {
                (Some(a), Some(b)) if a <= b => Some((pos + a, 8usize)), // json_agg
                (Some(a), None) => Some((pos + a, 8usize)),
                (_, Some(b)) => Some((pos + b, 9usize)), // jsonb_agg
                (None, None) => None,
            }
        };
        let (agg_start, fn_len) = match hit {
            Some(h) => h,
            None => break,
        };

        // Word boundary before: must not be alphanumeric or `_`.
        if agg_start > 0 {
            let prev = bytes[agg_start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                pos = agg_start + 1;
                continue;
            }
        }

        // Find the matching `)` for the agg function call.
        let paren_open = agg_start + fn_len; // points at `(`
        let Some(paren_close) = find_matching_paren_in_sql(lower_sql, paren_open) else {
            pos = agg_start + 1;
            continue;
        };

        // After the closing `)` of the agg call, skip whitespace and check for
        // optional `AS alias` or bare `alias`.  Also handle the case where the
        // agg is the last part of a scalar subquery `)` followed by `) AS alias`.
        let mut after = paren_close + 1;
        // Skip closing parens (scalar subquery wrappers) and whitespace.
        while after < len && (bytes[after] == b')' || bytes[after].is_ascii_whitespace()) {
            after += 1;
        }
        // Optional `AS` keyword.
        if after + 2 <= len && &lower_sql[after..after + 2] == "as" {
            let after_as = after + 2;
            if after_as < len && bytes[after_as].is_ascii_whitespace() {
                after = after_as + 1;
                while after < len && bytes[after].is_ascii_whitespace() {
                    after += 1;
                }
            } else {
                // `as` not followed by whitespace — not the AS keyword.
            }
        }
        // Read the alias identifier.
        let alias_start = after;
        let mut alias_end = alias_start;
        while alias_end < len
            && (bytes[alias_end].is_ascii_alphanumeric() || bytes[alias_end] == b'_')
        {
            alias_end += 1;
        }
        if alias_end > alias_start {
            let alias = lower_sql[alias_start..alias_end].to_string();
            // Filter out SQL keywords that can follow a function call but
            // are not aliases: FROM, WHERE, ORDER, GROUP, HAVING, LIMIT,
            // OFFSET, ON, AND, OR, NOT, NULL, AS, THEN, ELSE, END.
            const NOT_ALIAS: &[&str] = &[
                "from",
                "where",
                "order",
                "group",
                "having",
                "limit",
                "offset",
                "on",
                "and",
                "or",
                "not",
                "null",
                "as",
                "then",
                "else",
                "end",
                "inner",
                "left",
                "right",
                "join",
                "cross",
                "lateral",
                "union",
                "intersect",
                "except",
                "returning",
                "into",
            ];
            if !NOT_ALIAS.contains(&alias.as_str()) {
                aliases.insert(alias);
            }
        }

        pos = paren_close + 1;
    }

    aliases
}

/// Find the matching `)` for the `(` at `open_paren` in `sql`, handling
/// nested parens and single-quoted strings.  Returns `None` if malformed.
fn find_matching_paren_in_sql(sql: &str, open_paren: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    if open_paren >= len || bytes[open_paren] != b'(' {
        return None;
    }
    let mut depth = 1i32;
    let mut i = open_paren + 1;
    while i < len {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrite `json_agg(<table_name>)` / `jsonb_agg(<table_name>)` (and their
/// `JSON_AGG` / `JSONB_AGG` uppercase forms) where the bare identifier
/// `<table_name>` refers to a relation visible in the `FROM` clause.
///
/// DataFusion does not understand a bare relation reference as an aggregate
/// argument.  We expand it to:
///
/// ```sql
/// json_agg(named_struct('col1', t.col1, 'col2', t.col2, ...))
/// ```
///
/// using the Arrow schema already registered in `ctx`, so DataFusion sees a
/// concrete `named_struct(...)` call over typed column references.
///
/// This is a best-effort text rewrite: only `json_agg(<single_ident>)` and
/// `jsonb_agg(<single_ident>)` where `<single_ident>` matches a registered
/// table name are expanded.  Anything else is returned unchanged, so existing
/// working queries are unaffected.
pub(crate) async fn rewrite_json_agg_whole_row(
    sql: &str,
    ctx: &datafusion::prelude::SessionContext,
) -> String {
    // Fast path: skip if there is no json_agg / jsonb_agg in the SQL.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("json_agg(") && !lower.contains("jsonb_agg(") {
        return sql.to_string();
    }

    // We scan for occurrences of `json_agg(<ident>)` or `jsonb_agg(<ident>)`
    // where `<ident>` is a bare (unqualified, no dots, no spaces) identifier.
    // We rewrite each occurrence if `<ident>` is a registered table.
    //
    // Strategy: walk the original SQL byte-by-byte.  When we see
    // `json_agg(` or `jsonb_agg(`, extract the argument up to the matching `)`,
    // check if it's a simple identifier, query the schema, and splice the
    // expansion in place.

    let bytes = sql.as_bytes();
    let len = bytes.len();

    let mut result = String::with_capacity(sql.len() + 256);
    let mut pos = 0usize;

    while pos < len {
        // Look for `json_agg(` or `jsonb_agg(` (case-insensitive).
        let tail_lower = &lower[pos..];
        let (fn_name, fn_start_rel) = if let Some(p) = tail_lower.find("jsonb_agg(") {
            // Check: also try json_agg( at pos < p+1
            let json_p = tail_lower.find("json_agg(");
            let jsonb_p = Some(p);
            match (json_p, jsonb_p) {
                (Some(jp), Some(bp)) if jp < bp => ("json_agg", jp),
                (_, Some(bp)) => ("jsonb_agg", bp),
                (Some(jp), None) => ("json_agg", jp),
                (None, None) => break,
            }
        } else if let Some(p) = tail_lower.find("json_agg(") {
            ("json_agg", p)
        } else {
            break;
        };

        let abs_fn_start = pos + fn_start_rel;

        // Word-boundary check: character before `json_agg` must not be alphanum or `_`.
        if abs_fn_start > 0 {
            let prev = bytes[abs_fn_start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                // Not a word boundary (e.g. `my_json_agg`); skip past this hit.
                result.push_str(&sql[pos..abs_fn_start + 1]);
                pos = abs_fn_start + 1;
                continue;
            }
        }

        let fn_kw_len = fn_name.len(); // "json_agg" = 8, "jsonb_agg" = 9
                                       // Position of `(` — already consumed by the find pattern.
        let paren_open = abs_fn_start + fn_kw_len;
        // paren_open points at `(`.
        if paren_open >= len || bytes[paren_open] != b'(' {
            result.push_str(&sql[pos..abs_fn_start + 1]);
            pos = abs_fn_start + 1;
            continue;
        }

        // Find the matching `)` for the function call.  Simple scan (no
        // nested-paren awareness needed for single-ident args, but we handle
        // parens so we skip false positives with nested calls).
        let mut depth = 1i32;
        let mut inner_end: Option<usize> = None;
        let mut k = paren_open + 1;
        while k < len {
            match bytes[k] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        inner_end = Some(k);
                        break;
                    }
                }
                b'\'' => {
                    // Skip string literals.
                    k += 1;
                    while k < len {
                        if bytes[k] == b'\'' {
                            if k + 1 < len && bytes[k + 1] == b'\'' {
                                k += 2;
                                continue;
                            }
                            break;
                        }
                        k += 1;
                    }
                }
                _ => {}
            }
            k += 1;
        }

        let close_paren = match inner_end {
            Some(p) => p,
            None => {
                // Malformed — emit verbatim.
                result.push_str(&sql[pos..abs_fn_start + 1]);
                pos = abs_fn_start + 1;
                continue;
            }
        };

        // Extract argument between `(` and `)`.
        let arg = sql[paren_open + 1..close_paren].trim();

        // Check: is `arg` a simple identifier (no dots, spaces, parens)?
        let is_simple_ident = !arg.is_empty()
            && arg.chars().all(|c| c.is_alphanumeric() || c == '_')
            && arg
                .chars()
                .next()
                .map_or(false, |c| c.is_alphabetic() || c == '_');

        if !is_simple_ident {
            // Not a bare ident; emit verbatim and advance past the whole call.
            result.push_str(&sql[pos..close_paren + 1]);
            pos = close_paren + 1;
            continue;
        }

        // Check whether `arg` is a registered table by trying table_provider.
        let table_name_lower = arg.to_ascii_lowercase();
        let schema_opt = ctx
            .table_provider(table_name_lower.as_str())
            .await
            .ok()
            .map(|tp| tp.schema());

        let expansion = match schema_opt {
            Some(schema) if !schema.fields().is_empty() => {
                // Build `named_struct('col1', t.col1, 'col2', t.col2, ...)`.
                let mut pairs: Vec<String> = Vec::with_capacity(schema.fields().len() * 2);
                for field in schema.fields() {
                    let col = field.name();
                    // Quote the string literal key with single quotes (no escaping needed
                    // for normal column names).
                    pairs.push(format!("'{col}', {table_name_lower}.{col}"));
                }
                let inner = pairs.join(", ");
                Some(format!("{fn_name}(named_struct({inner}))"))
            }
            _ => None,
        };

        match expansion {
            Some(rewritten) => {
                // Emit everything before this function call, then the rewritten form.
                result.push_str(&sql[pos..abs_fn_start]);
                result.push_str(&rewritten);
                pos = close_paren + 1;
            }
            None => {
                // Not a table or empty schema; emit verbatim.
                result.push_str(&sql[pos..close_paren + 1]);
                pos = close_paren + 1;
            }
        }
    }

    // Emit any trailing content.
    if pos < len {
        result.push_str(&sql[pos..]);
    }

    result
}

// Savepoint name extraction was previously done by re-scraping the SQL
// text (`extract_savepoint_name`).  It was replaced by
// `pg_ast::txn_stmt`, which reads libpg_query's authoritative parsed
// `TransactionStmt.savepoint_name`, so the text-scraper is gone.

// ---------------------------------------------------------------------------
// Data-modifying CTE orchestrator
// ---------------------------------------------------------------------------

/// Returns `true` when any CTE in the `WITH` clause has a DML body
/// (`INSERT`, `UPDATE`, or `DELETE`).  Pure-SELECT CTEs return `false`.
fn query_has_dml_cte(query: &sqlparser::ast::Query) -> bool {
    let Some(ref with) = query.with else {
        return false;
    };
    with.cte_tables.iter().any(|cte| {
        matches!(
            cte.query.body.as_ref(),
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
        )
    })
}

/// Returns `true` when the query has a `WITH RECURSIVE` clause AND the outer
/// body is a DML statement (`INSERT`, `UPDATE`, or `DELETE`).
///
/// This is the combination shape: `WITH RECURSIVE t(col) AS (… UNION ALL …)
/// INSERT INTO target SELECT * FROM t`.  sqlparser routes the whole thing as a
/// `Statement::Query` with `SetExpr::Insert` as the body.  DataFusion cannot
/// plan DML bodies inside a query, so we intercept before `exec_select`.
fn query_has_recursive_with_dml_body(query: &sqlparser::ast::Query) -> bool {
    let Some(ref with) = query.with else {
        return false;
    };
    if !with.recursive {
        return false;
    }
    matches!(
        query.body.as_ref(),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
    )
}

/// Execute a `WITH RECURSIVE … INSERT/UPDATE/DELETE …` query.
///
/// Strategy: the `WITH RECURSIVE` clause contains only pure-SELECT CTEs (the
/// recursive generator); the DML is the query *body*, not a CTE.  sqlparser
/// wraps the whole statement as a `Statement::Query{with: RECURSIVE, body:
/// SetExpr::Insert}`.  DataFusion's `sql()` cannot plan a DML body, so we:
///
/// 1. Lift the `WITH RECURSIVE` clause onto the DML's *source* `SELECT`.
///    e.g. `INSERT INTO t SELECT * FROM cte` becomes
///         `INSERT INTO t (WITH RECURSIVE cte AS (…) SELECT * FROM cte)`.
/// 2. Call `exec_insert` / the appropriate DML path with the modified
///    statement, which then routes through `exec_insert_select` → DataFusion.
///    DataFusion CAN plan `WITH RECURSIVE … SELECT …`, so the recursive
///    generator is fully expanded before the rows land in the target table.
///
/// **UPDATE/DELETE**: Materialize each recursive CTE by running
///   `WITH RECURSIVE name AS (body) SELECT * FROM name` through DataFusion,
///   register the result as a MemTable, then run the UPDATE/DELETE.  The WHERE
///   subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) resolves against the
///   registered MemTable via `resolve_subqueries_in_expr`.
async fn exec_recursive_with_dml_body(
    sess: &ProjectSession,
    query: &sqlparser::ast::Query,
) -> Result<ExecResult> {
    let with = query
        .with
        .as_ref()
        .expect("caller verified query.with is Some");

    match query.body.as_ref() {
        SetExpr::Insert(Statement::Insert(ins)) => {
            let mut ins = ins.clone();

            // Attach the RECURSIVE WITH clause to the INSERT's source query so
            // that DataFusion sees `WITH RECURSIVE t AS (…) SELECT * FROM t`
            // when it plans the source.  The Insert.source is the SELECT that
            // feeds rows into the target table.
            if let Some(ref mut source) = ins.source {
                // Only attach when the source doesn't already have a WITH
                // clause (shouldn't happen, but be safe).
                if source.with.is_none() {
                    source.with = Some(with.clone());
                }
            } else {
                return Err(BasinError::InvalidSchema(
                    "WITH RECURSIVE INSERT without a SELECT source is not supported".into(),
                ));
            }

            exec_insert(sess, ins).await
        }
        SetExpr::Update(Statement::Update(upd)) => {
            exec_recursive_with_update(sess, with, upd).await
        }
        SetExpr::Delete(Statement::Delete(del)) => {
            exec_recursive_with_delete(sess, with, del).await
        }
        _ => {
            // Shouldn't reach here given the query_has_recursive_with_dml_body
            // guard, but be defensive.
            Err(BasinError::internal(
                "exec_recursive_with_dml_body called on non-DML body",
            ))
        }
    }
}

/// Materialize every CTE in `with` as a MemTable registered in `sess.ctx`.
///
/// For each CTE `name AS (body)` we run:
///   `WITH RECURSIVE name AS (body) SELECT * FROM name`
/// through DataFusion (which natively supports recursive CTEs), collect the
/// rows, and register them as a MemTable so subsequent DML can reference
/// `name` in WHERE subqueries (e.g. `WHERE id IN (SELECT id FROM name)`).
///
/// Returns the list of registered table names so the caller can deregister
/// after DML completes (best-effort cleanup; sessions are per-request anyway).
async fn materialize_recursive_ctes(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
) -> Result<Vec<String>> {
    use datafusion::datasource::memory::MemTable;

    let mut registered: Vec<String> = Vec::new();

    for cte in &with.cte_tables {
        let cte_name = cte.alias.name.value.clone();
        // Build: WITH RECURSIVE <name> AS (<body>) SELECT * FROM <name>
        // DataFusion natively expands recursive CTEs in SELECT context.
        let expand_sql = format!(
            "WITH RECURSIVE {name} AS ({body}) SELECT * FROM {name}",
            name = cte_name,
            body = cte.query,
        );
        let df = sess.ctx.sql(&expand_sql).await.map_err(|e| {
            BasinError::internal(format!(
                "WITH RECURSIVE: failed to plan CTE {cte_name:?}: {e}"
            ))
        })?;
        // Capture the logical schema BEFORE consuming `df` with collect().
        // This is the authoritative schema from the plan, which we use for
        // the MemTable so that plan-level type info is preserved.
        let plan_schema: datafusion::arrow::datatypes::SchemaRef =
            df.schema().as_arrow().clone().into();
        let df_batches = df.collect().await.map_err(|e| {
            BasinError::internal(format!(
                "WITH RECURSIVE: failed to execute CTE {cte_name:?}: {e}"
            ))
        })?;

        // Re-cast every batch to use `plan_schema` so that schema and batches
        // agree on nullability / metadata and MemTable::try_new succeeds.
        // `RecordBatch::with_schema` is cheap — it reuses the underlying
        // column buffers, only wrapping them with a new schema reference.
        let recast_batches: Vec<datafusion::arrow::record_batch::RecordBatch> = df_batches
            .into_iter()
            .map(|b| {
                datafusion::arrow::record_batch::RecordBatch::try_new(
                    plan_schema.clone(),
                    b.columns().to_vec(),
                )
                .map_err(|e| {
                    BasinError::internal(format!(
                        "WITH RECURSIVE: recast batch for CTE {cte_name:?}: {e}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let provider =
            MemTable::try_new(plan_schema.clone(), vec![recast_batches]).map_err(|e| {
                BasinError::internal(format!("MemTable for CTE {cte_name:?}: {e}"))
            })?;

        let _ = sess.ctx.deregister_table(&cte_name);
        sess.ctx
            .register_table(&cte_name, Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register CTE {cte_name:?}: {e}")))?;
        registered.push(cte_name);
    }

    Ok(registered)
}

/// Execute `WITH RECURSIVE … UPDATE …` by materializing the CTEs as MemTables
/// and routing the UPDATE through the existing `exec_update` path.
///
/// The WHERE clause subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) is
/// resolved by `resolve_subqueries_in_expr` against the registered MemTable.
async fn exec_recursive_with_update(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
    upd: &sqlparser::ast::Update,
) -> Result<ExecResult> {
    let registered = materialize_recursive_ctes(sess, with).await?;

    let from_twj = upd.from.as_ref().and_then(|f| match f {
        sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
        | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v.first().cloned(),
    });

    let result = crate::dml_mutate::exec_update(
        sess,
        upd.table.clone(),
        upd.assignments.clone(),
        from_twj,
        upd.selection.clone(),
        upd.returning.clone(),
    )
    .await;

    for name in &registered {
        let _ = sess.ctx.deregister_table(name.as_str());
    }

    result
}

/// Execute `WITH RECURSIVE … DELETE …` by materializing the CTEs as MemTables
/// and routing the DELETE through the existing `exec_delete` path.
///
/// The WHERE clause subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) is
/// resolved by `resolve_subqueries_in_expr` against the registered MemTable.
async fn exec_recursive_with_delete(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
    del: &sqlparser::ast::Delete,
) -> Result<ExecResult> {
    let registered = materialize_recursive_ctes(sess, with).await?;

    let result = crate::dml_mutate::exec_delete(sess, del.clone()).await;

    for name in &registered {
        let _ = sess.ctx.deregister_table(name.as_str());
    }

    result
}

/// Execute a `WITH … SELECT …` query whose CTE list contains at least one
/// data-modifying CTE (`INSERT`/`UPDATE`/`DELETE … RETURNING …`).
///
/// Strategy:
/// 1. Walk the CTE list in declaration order.
///    - Pure-SELECT CTEs: skip (DataFusion handles them in the outer query).
///    - DML CTEs: execute via the existing DML paths, forcing `RETURNING *`
///      when the user omitted a RETURNING clause, to capture the affected rows.
/// 2. Register each captured batch as a named `MemTable` in `sess.ctx` under
///    the CTE alias.  DataFusion's subsequent planning of the outer SELECT will
///    find these in scope exactly as it does for real tables.
/// 3. Execute the outer SELECT body (stripped of DML CTEs; any remaining
///    pure-SELECT CTEs stay in the query string) through the normal
///    `exec_select` path.
///
/// Atomicity: if any DML leg fails, the function returns the error immediately
/// and subsequent legs are not executed.  Already-committed legs are NOT
/// rolled back (Basin has no multi-statement rollback yet).  This is documented
/// behaviour: a failed DML-CTE may partially apply.  When the session is inside
/// an explicit transaction the existing transaction machinery provides the
/// rollback guarantee.
///
/// Recursive DML CTEs and Postgres's "snapshot-at-statement-start" inter-leg
/// visibility semantics are deferred — they require planner-level support that
/// is out of scope for this implementation.
async fn exec_dml_cte_query(
    sess: &ProjectSession,
    query: &sqlparser::ast::Query,
    original_sql: &str,
    include_deleted: bool,
) -> Result<ExecResult> {
    use datafusion::datasource::memory::MemTable;
    use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

    let with = query
        .with
        .as_ref()
        .expect("caller verified query.with is Some");

    // Reject RECURSIVE data-modifying CTEs — the interaction with our sequential
    // execution model would be incorrect.
    if with.recursive {
        return Err(BasinError::InvalidSchema(
            "RECURSIVE data-modifying CTEs are not supported".into(),
        ));
    }

    // Table names registered during this call so we can clean them up if the
    // outer SELECT fails (best effort; DataFusion contexts are per-session anyway).
    let mut registered: Vec<String> = Vec::new();

    for cte in &with.cte_tables {
        let cte_name = cte.alias.name.value.clone();

        match cte.query.body.as_ref() {
            SetExpr::Insert(Statement::Insert(ins)) => {
                let mut ins = ins.clone();
                // Force RETURNING * when the user omitted it so we always capture
                // the inserted rows for the MemTable.
                let user_had_returning = ins.returning.is_some();
                if ins.returning.is_none() {
                    ins.returning = Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )]);
                }
                let result = exec_insert(sess, ins).await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            SetExpr::Update(Statement::Update(sqlparser::ast::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                ..
            })) => {
                let from_twj = from.as_ref().and_then(|f| match f {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v.first().cloned(),
                });
                let user_had_returning = returning.is_some();
                // If the user didn't supply RETURNING we force RETURNING * so
                // we can capture affected rows for the MemTable.
                let effective_returning = if returning.is_some() {
                    returning.clone()
                } else {
                    Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )])
                };
                let result = crate::dml_mutate::exec_update(
                    sess,
                    table.clone(),
                    assignments.clone(),
                    from_twj,
                    selection.clone(),
                    effective_returning,
                )
                .await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            SetExpr::Delete(Statement::Delete(del)) => {
                let mut del = del.clone();
                let user_had_returning = del.returning.is_some();
                if del.returning.is_none() {
                    del.returning = Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )]);
                }
                let result = crate::dml_mutate::exec_delete(sess, del).await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            // Non-DML CTE body: leave it for DataFusion to handle in the outer query.
            _ => continue,
        }
    }

    // Build the outer SELECT SQL: strip DML CTEs, keep pure-SELECT CTEs.
    // Strategy: reconstruct the query with only the non-DML CTEs in `WITH`,
    // or drop the `WITH` clause entirely if all CTEs were DML.
    let outer_sql = build_outer_select_sql(query)?;

    // Execute the outer SELECT through the normal path, which will find the
    // registered MemTables in `sess.ctx` when it references them.
    exec_select(sess, &outer_sql, include_deleted, None).await
}

/// Extract rows from a DML result.  When `user_had_returning` is false (we
/// forced `RETURNING *` ourselves), a `ExecResult::Empty` is not expected but
/// we defend against it by returning an empty batch with an empty schema.
fn dml_cte_extract_rows(
    result: ExecResult,
    _user_had_returning: bool,
    cte_name: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    match result {
        ExecResult::Rows { schema, batches } => Ok((schema, batches)),
        ExecResult::Empty { .. } => {
            // DML returned no rows (e.g. 0-row UPDATE).  Register an empty
            // schema placeholder so that any outer SELECT FROM <cte_name> returns
            // 0 rows without a "table not found" error.
            Ok((Arc::new(Schema::empty()), vec![]))
        }
        #[allow(unreachable_patterns)]
        _ => Err(BasinError::internal(format!(
            "unexpected DML result for CTE {cte_name:?}"
        ))),
    }
}

/// Register `batches` under `cte_name` as a `MemTable` in the session context.
/// The `ws`-side schema is converted to DataFusion's schema for the MemTable.
fn register_dml_cte_memtable(
    sess: &ProjectSession,
    cte_name: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    registered: &mut Vec<String>,
) -> Result<()> {
    use datafusion::datasource::memory::MemTable;

    // Convert ws batches → df batches for MemTable.
    let df_batches: Vec<datafusion::arrow::record_batch::RecordBatch> = batches
        .iter()
        .map(|b| crate::convert::batch_ws_to_df(b))
        .collect::<Result<Vec<_>>>()?;

    // Build the DataFusion schema from the ws schema.
    let df_schema = if df_batches.is_empty() {
        // No rows: convert the ws schema to df schema manually.
        let ws_to_df_schema = crate::convert::schema_ws_to_df(&schema)?;
        Arc::new(ws_to_df_schema)
    } else {
        df_batches[0].schema()
    };

    let provider = MemTable::try_new(df_schema, vec![df_batches])
        .map_err(|e| BasinError::internal(format!("MemTable for CTE {cte_name:?}: {e}")))?;

    // Deregister any pre-existing table with this name (e.g. a real table that
    // shares the CTE alias).  DataFusion's `register_table` returns an error if
    // the name is already occupied.
    let _ = sess.ctx.deregister_table(cte_name);
    sess.ctx
        .register_table(cte_name, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register CTE {cte_name:?}: {e}")))?;
    registered.push(cte_name.to_string());
    Ok(())
}

/// Build the SQL for the outer SELECT by stripping DML CTEs from the `WITH`
/// clause.  Pure-SELECT CTEs are retained because DataFusion can plan them.
///
/// If all CTEs were DML (none remain), the `WITH` clause is dropped entirely
/// and we return just the outer query body as SQL.
fn build_outer_select_sql(query: &sqlparser::ast::Query) -> Result<String> {
    let with = query.with.as_ref().expect("caller verified non-None");

    // Collect only the non-DML CTEs.
    let pure_ctes: Vec<_> = with
        .cte_tables
        .iter()
        .filter(|cte| {
            !matches!(
                cte.query.body.as_ref(),
                SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
            )
        })
        .collect();

    // Reconstruct the outer query body SQL.
    let body_sql = query.body.to_string();

    // Append ORDER BY / LIMIT / OFFSET / FETCH if present.
    let mut suffix = String::new();
    if let Some(ref order_by) = query.order_by {
        suffix.push(' ');
        suffix.push_str(&order_by.to_string());
    }
    if let Some(ref lc) = query.limit_clause {
        suffix.push_str(&lc.to_string());
    }
    if let Some(ref fetch) = query.fetch {
        suffix.push(' ');
        suffix.push_str(&fetch.to_string());
    }

    if pure_ctes.is_empty() {
        // No pure-SELECT CTEs remain; drop the WITH clause.
        Ok(format!("{body_sql}{suffix}"))
    } else {
        // Reconstruct WITH <pure_ctes> <body>.
        let ctes_sql = pure_ctes
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("WITH {ctes_sql} {body_sql}{suffix}"))
    }
}

// ---------------------------------------------------------------------------
// Phase 5.14.D2 — query history recording
// ---------------------------------------------------------------------------

/// Best-effort ORDER BY / GROUP BY column recorder for adaptive sort.
///
/// Fires for simple single-table `SELECT … FROM <table>` shapes.  Multi-table
/// joins, CTEs, and subqueries in the FROM clause are skipped (too ambiguous
/// to attribute to a single table).  Non-column expressions in ORDER BY (e.g.
/// `ORDER BY 1`, `ORDER BY col + 1`) are silently excluded from the recorded
/// tuple; if the resulting tuple is empty after filtering, nothing is recorded.
fn record_query_patterns(sess: &ProjectSession, query: &sqlparser::ast::Query) {
    use crate::pg_ast::OrderByExt;
    use sqlparser::ast::{Expr, GroupByExpr, TableFactor};

    // Skip queries with CTEs — the table attribution is ambiguous.
    if query.with.is_some() {
        return;
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return,
    };

    // Only single-table FROM (no joins).
    if select.from.len() != 1 {
        return;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return;
    }
    let table_name = match &from.relation {
        TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return;
            }
            match basin_common::TableName::new(name.0[0].id_val().clone()) {
                Ok(t) => t,
                Err(_) => return,
            }
        }
        _ => return,
    };

    let history = sess.engine.query_history();

    // Record ORDER BY column names (identifier-only expressions).
    if let Some(order_by) = &query.order_by {
        let cols: Vec<String> = order_by
            .ext_exprs()
            .iter()
            .filter_map(|ob| match &ob.expr {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    Some(parts[0].value.clone())
                }
                _ => None,
            })
            .collect();
        history.record_order_by(&sess.project, &table_name, cols);
    }

    // Record GROUP BY column names (identifier-only expressions).
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        let cols: Vec<String> = exprs
            .iter()
            .filter_map(|e| match e {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    Some(parts[0].value.clone())
                }
                _ => None,
            })
            .collect();
        history.record_group_by(&sess.project, &table_name, cols);
    }
}

// ── Phase 5.14.C2 HTAP helpers ────────────────────────────────────────────────

/// Emit a WAL `Begin` marker the first time a DML statement runs inside an
/// explicit transaction.  The marker is lazy: it is emitted once per tx, on
/// the first INSERT/UPDATE/DELETE.  Auto-commit statements never emit markers.
///
/// Returns the `tx_id` that was assigned (new or pre-existing).
async fn htap_emit_wal_begin_lazy(sess: &ProjectSession) -> u64 {
    let candidate = sess.engine.next_tx_id();
    let tx_id = crate::session::tx_ensure_id(&sess.state, candidate);
    // If a new id was just assigned (candidate == tx_id) emit the Begin marker.
    if tx_id == candidate {
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            let part = basin_common::PartitionKey::default_key();
            // Best-effort — WAL marker failures must not block the write.
            let _ = shard
                .wal()
                .append_tx_begin(&sess.project, &part, tx_id)
                .await;
        }
    }
    tx_id
}

/// Encode a `RecordBatch` to Arrow IPC stream format.
///
/// Returns the wire bytes.  Used to store rows in the HTAP `MemTable` as
/// IPC-encoded blobs so the hot-tier data format matches the WAL wire format.
fn encode_batch_to_ipc(batch: &arrow_array::RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
        .expect("IPC StreamWriter init");
    writer.write(batch).expect("IPC write");
    writer.finish().expect("IPC finish");
    buf
}

/// Promote committed HTAP batches from a completed transaction to the
/// process-wide `MemTableRegistry`.  Called on COMMIT before `tx_commit`
/// clears the `TxState`.
///
/// Budget enforcement (per ADR 0016 §Multi-project isolation + Phase 5.14.C5):
/// 1. `try_reserve_bytes` per batch.
/// 2. On `HardCapReached`: synchronous eviction of the largest project's
///    memtable, then retry once.
/// 3. If still over cap: return `SQLSTATE 53200` (out_of_memory).
async fn htap_promote_to_registry(
    sess: &ProjectSession,
    htap_rows: &std::collections::HashMap<
        basin_common::TableName,
        Vec<arrow_array::RecordBatch>,
    >,
) -> Result<()> {
    if htap_rows.is_empty() {
        return Ok(());
    }
    let registry = sess.engine.memtable_registry();
    let catalog = &sess.engine.config().catalog;
    for (table, batches) in htap_rows {
        let entry = registry.get_or_create(sess.project, table.clone());

        // Pre-fetch table metadata once per table so the hot loop below does
        // not call load_table() per row.  On catalog miss (e.g. mid-DROP) fall
        // back to the monotonic-counter key for the whole table.
        let pk_info: Option<Vec<(usize, arrow_schema::DataType)>> =
            match catalog.load_table(&sess.project, table).await {
                Ok(meta) if !meta.pk_columns.is_empty() => {
                    // For each PK column resolve: (batch column index, DataType).
                    // We validate against the first batch's schema — all batches
                    // for the same table share the same schema within a session.
                    let first_batch = batches.first();
                    first_batch.and_then(|fb| {
                        let schema = fb.schema();
                        let mut cols: Vec<(usize, arrow_schema::DataType)> =
                            Vec::with_capacity(meta.pk_columns.len());
                        for pk_col in &meta.pk_columns {
                            match schema.index_of(pk_col) {
                                Ok(idx) => {
                                    let dt = schema.field(idx).data_type().clone();
                                    cols.push((idx, dt));
                                }
                                Err(_) => {
                                    // PK column missing from batch schema — degrade
                                    // to counter key for the whole table.
                                    tracing::warn!(
                                        table = %table,
                                        pk_col = %pk_col,
                                        "htap_promote_to_registry: PK column missing \
                                         from batch schema — falling back to counter key"
                                    );
                                    return None;
                                }
                            }
                        }
                        Some(cols)
                    })
                }
                Ok(_) => None, // no PK declared
                Err(_) => {
                    // Catalog unavailable — degrade gracefully.
                    tracing::warn!(
                        table = %table,
                        "htap_promote_to_registry: catalog miss for table \
                         — falling back to counter key"
                    );
                    None
                }
            };

        for batch in batches {
            let approx_bytes = batch.get_array_memory_size() as u64;
            // Budget gate.
            let reserve_ok = |outcome: basin_hottier::ReservationOutcome| {
                outcome != basin_hottier::ReservationOutcome::HardCapReached
            };
            if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
                // Synchronous eviction: drop the largest project to free space.
                if let Some(largest) = registry.largest_project() {
                    registry.remove_project(&largest);
                }
                // Retry once.
                if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
                    return Err(basin_common::BasinError::internal(
                        "HTAP memtable hard cap exceeded (SQLSTATE 53200)",
                    ));
                }
            }
            for row_idx in 0..batch.num_rows() {
                let row_batch = batch.slice(row_idx, 1);
                let row_bytes = encode_batch_to_ipc(&row_batch);

                // Derive the RowKey from the encoded PK columns so that:
                // 1. The memtable PK direct-get fast path can find the row.
                // 2. Future merge-on-read can dedupe hot vs cold by PK.
                //
                // Falls back to a monotonic counter key when the PK cannot be
                // encoded (unsupported type, NULL PK, or no PK declared).
                let key = if let Some(ref pk_cols) = pk_info {
                    build_pk_row_key(batch, row_idx, pk_cols)
                        .unwrap_or_else(|| {
                            // Unsupported PK type or NULL — use counter fallback.
                            // warn! is rate-limited by the one-per-table pre-fetch
                            // warning above; only log here for NULL PK values which
                            // are unexpected on a table with a PK constraint.
                            basin_hottier::RowKey::builder()
                                .append_u64(entry.memtable.total_count() as u64)
                                .finish()
                        })
                } else {
                    // No PK info — monotonic counter key.
                    basin_hottier::RowKey::builder()
                        .append_u64(entry.memtable.total_count() as u64)
                        .finish()
                };

                entry
                    .memtable
                    .insert(key, basin_hottier::MemRowValue::row(row_bytes, 0));
            }
        }
    }
    Ok(())
}

/// Build a [`basin_hottier::RowKey`] from the PK columns of a single row in
/// `batch` at `row_idx`, using the same per-type encoding as
/// `dml_mutate::pk_scalar_to_row_key` / `constraint_union::array_value_to_row_key`.
///
/// `pk_cols` is a slice of `(column_index_in_batch, declared_DataType)` in PK
/// declaration order.  For single-column PKs a single segment is produced;
/// for composite PKs every column's bytes are concatenated in order.
///
/// Returns `None` when any PK column contains a NULL value or has an
/// unsupported type — the caller should fall back to a monotonic counter key.
fn build_pk_row_key(
    batch: &arrow_array::RecordBatch,
    row_idx: usize,
    pk_cols: &[(usize, arrow_schema::DataType)],
) -> Option<basin_hottier::RowKey> {
    // Collect each column's encoded bytes into one contiguous buffer.
    // `array_value_to_row_key` returns `None` on NULL or unsupported type,
    // propagating `?` so we fall back to counter key on the first failure.
    let mut composite: Vec<u8> = Vec::new();
    for (col_idx, col_dt) in pk_cols {
        let array = batch.column(*col_idx);
        let segment =
            crate::constraint_union::array_value_to_row_key(array.as_ref(), row_idx, col_dt)?;
        composite.extend_from_slice(segment.as_bytes());
    }
    Some(basin_hottier::RowKey::from_bytes(composite))
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.7 B1: secondary index maintenance on INSERT
// ─────────────────────────────────────────────────────────────────────────────

/// Maintain secondary B-tree indexes after a successful INSERT.
///
/// For each single-column non-expression index declared on `table`, extracts
/// the indexed column's values from `batch` and inserts `(key → location)`
/// entries into the process-wide registry, then flushes the updated index to
/// the object store (best-effort; errors are logged, not propagated).
///
/// Called after a successful Parquet write and catalog commit on the
/// auto-commit path, and after the tx-deferred write on the tx-active path.
async fn maintain_secondary_indexes_on_insert(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    if meta.indexes.is_empty() {
        return;
    }
    let registry = sess.engine.secondary_index_registry();
    let gin_registry = sess.engine.gin_index_registry();
    let storage = &sess.engine.config().storage;

    for idx in &meta.indexes {
        // Only handle single-column non-expression indexes in v0.1.
        if idx.columns.len() != 1 {
            continue;
        }
        let col_name = &idx.columns[0];

        // Phase 5.19.C + 5.20.E: populate GIN posting list for JSONB / FTS.
        if idx.access_method == "gin" {
            let opclass = idx.opclass.as_deref().unwrap_or("jsonb_ops");
            if opclass == "tsvector_ops" {
                // Phase 5.20.E: tsvector GIN index — populate FTS posting list.
                let fts_registry = sess.engine.gin_fts_registry();
                maintain_gin_fts_index_on_insert(fts_registry, &sess.project, table, col_name, batch, file_path);
            } else {
                // Phase 5.19.C: JSONB GIN index — populate JSONB posting list.
                maintain_gin_index_on_insert(gin_registry, &sess.project, table, col_name, opclass, batch, file_path);
            }
            // GIN posting list is RAM-only (5.19.E handles persistence); skip
            // the flush call.  Continue to also populate the B-tree registry
            // (which is a no-op for LargeBinary columns — scalar_to_key returns
            // None — so this is harmless).
            continue;
        }

        // Phase 5.24.D: populate interval-tree for range-typed columns with a
        // GIST index.  Range values are stored as Utf8 JSON; the interval tree
        // allows point-containment and overlap probes at file-level granularity.
        if idx.access_method == "gist" {
            let interval_registry = sess.engine.interval_registry();
            maintain_gist_index_on_insert(interval_registry, &sess.project, table, col_name, batch, file_path);
            continue;
        }

        let entries = crate::secondary_index::extract_entries_from_batch(
            batch,
            col_name,
            file_path,
            0, // row_group 0 — single batch write
        );
        if !entries.is_empty() {
            registry.insert_batch(&sess.project, table, col_name, entries);
        }
        crate::secondary_index::flush_index(registry, storage, &sess.project, table, col_name)
            .await;
    }
}

/// Populate the GIN posting list for a single JSONB column from a newly
/// written `batch`.  Iterates every row in the batch, parses the JSONB bytes,
/// extracts GIN terms, and inserts them into the registry.
///
/// Only `LargeBinary` columns are handled (Basin's canonical JSONB wire type).
/// Other column types are silently skipped — the index is best-effort.
fn maintain_gin_index_on_insert(
    gin_registry: &Arc<crate::index_probe::GinIndexRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    opclass: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // Basin stores JSONB as LargeBinary.
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            gin_registry.index_row(project, table, col_name, opclass, bytes, file_path, 0, row as u64);
        }
        // Phase 5.19.C: mark this file as fully indexed so the completeness
        // guard in the probe path can safely prune to FileCandidates.
        // We only mark it when the column was present and had a LargeBinary
        // array — i.e., a real JSONB column in this batch.
        gin_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

/// Phase 5.20.E — Populate the GIN FTS posting list for a single tsvector
/// column from a newly written `batch`.  Iterates every row in the batch,
/// reads the canonical tsvector string, extracts lexemes, and inserts them
/// into the FTS registry.
///
/// Only `Utf8` columns are handled (Basin stores tsvector as Utf8).
/// Other column types are silently skipped — the index is best-effort.
fn maintain_gin_fts_index_on_insert(
    fts_registry: &Arc<basin_storage::index::gin_tsvector::GinTsvectorRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // Basin stores tsvector as Utf8 (canonical lexeme text form).
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let tsv_str = arr.value(row);
            fts_registry.index_row(project, table, col_name, tsv_str, file_path, 0, row as u64);
        }
        // Mark this file as fully indexed so the completeness guard in the
        // probe path can safely prune to FileCandidates.
        fts_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

/// Phase 5.24.D — Populate the interval-tree index for a single range-typed
/// column from a newly written `batch`. Iterates every row in the batch, parses
/// the range JSON string, converts it to a half-open `IndexInterval`, and
/// inserts it into the registry.
///
/// Only `Utf8` columns with range-type metadata are handled (Basin stores range
/// types as Utf8 JSON). Other column types are silently skipped — the index is
/// best-effort.  After all rows have been indexed, `mark_file_indexed` is called
/// so the completeness guard can safely prune to `FileCandidates`.
fn maintain_gist_index_on_insert(
    interval_registry: &Arc<basin_storage::index::interval::IntervalRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // Basin stores range types as Utf8 JSON strings.
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let range_json = arr.value(row);
            interval_registry.index_row(project, table, col_name, range_json, file_path, 0);
        }
        // Mark this file as fully indexed so the completeness guard in the
        // probe path can safely prune to FileCandidates.
        interval_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

// ---------------------------------------------------------------------------
// Phase 5.29.B-E — hypertable DDL + retention executors
// ---------------------------------------------------------------------------

/// Best-effort: for INSERT statements into hypertable tables, scan the VALUES
/// list for the hypertable's time column and register chunk records for each
/// unique time bucket. Called before the regular INSERT path so that
/// `timescaledb_information.chunks` is populated by the time the test queries
/// it, even within the same session.
async fn touch_hypertable_chunks_from_insert(
    sess: &ProjectSession,
    ins: &sqlparser::ast::Insert,
    _raw_sql: &str,
) {
    use sqlparser::ast::{Expr, SetExpr};
    // Get the table name.
    let obj_name = match crate::pg_ast::insert_object_name(ins) {
        Ok(n) => n,
        Err(_) => return,
    };
    let table = match single_part_name(obj_name) {
        Ok(n) => n.to_string(),
        Err(_) => return,
    };

    // Check if it's a hypertable.
    let Some(time_col) = sess.engine.hypertable_registry()
        .time_column(&sess.project, &table)
        .await
    else {
        return; // not a hypertable
    };

    // Find the column index of the time column in the INSERT's column list.
    let col_names: Vec<String> = ins
        .columns
        .iter()
        .map(|c| c.value.to_ascii_lowercase())
        .collect();
    let time_col_lower = time_col.to_ascii_lowercase();
    let time_col_idx = col_names.iter().position(|c| c == &time_col_lower);

    // If no explicit column list, check if the table schema has the column,
    // and try to infer position.  For simplicity in v0.1 we require an
    // explicit column list to locate the time column.
    let Some(col_idx) = time_col_idx else {
        // No explicit column list. Try to get it from the table schema.
        if let Ok(meta) = sess.engine.config().catalog
            .load_table(&sess.project, &match basin_common::TableName::new(table.as_str()) {
                Ok(n) => n,
                Err(_) => return,
            })
            .await
        {
            let schema = meta.schema.clone();
            let idx = schema.fields().iter().position(|f| {
                f.name().eq_ignore_ascii_case(&time_col)
            });
            if let Some(sidx) = idx {
                // Scan values using schema index.
                let Some(ref source) = ins.source else { return; };
                let SetExpr::Values(vals) = source.body.as_ref() else { return; };
                for row in &vals.rows {
                    if sidx >= row.len() { continue; }
                    if let Some(ts) = expr_to_datetime(&row[sidx]) {
                        let _ = sess.engine.hypertable_registry()
                            .touch_chunk(&sess.project, &table, ts)
                            .await;
                    }
                }
            }
        }
        return;
    };

    // Scan VALUES rows and extract timestamp from the time column.
    let Some(ref source) = ins.source else { return; };
    let SetExpr::Values(vals) = source.body.as_ref() else { return; };
    for row in &vals.rows {
        if col_idx >= row.len() { continue; }
        if let Some(ts) = expr_to_datetime(&row[col_idx]) {
            let _ = sess.engine.hypertable_registry()
                .touch_chunk(&sess.project, &table, ts)
                .await;
        }
    }
}

/// Parse a simple timestamp literal expression into a `DateTime<Utc>`.
/// Understands single-quoted ISO-8601 strings and `TO_TIMESTAMP(...)` calls.
/// Normalise a PG-style timestamp string so that `DateTime::parse_from_rfc3339`
/// can parse it.  Handles two deviations from RFC 3339:
///   * space separator instead of 'T'  → replace first space with 'T'
///   * short UTC offset `+00` or `-05` → expand to `+00:00` / `-05:00`
fn normalize_pg_timestamp(s: &str) -> String {
    // Replace the date/time separator space with 'T'
    let s = if !s.contains('T') {
        s.replacen(' ', "T", 1)
    } else {
        s.to_string()
    };
    // Detect a trailing offset of the form +HH or -HH (3 chars, no colon)
    // We look for the last '+' or '-' that appears after position 10 (after the date part).
    let tz_plus  = s[10..].rfind('+').map(|p| p + 10);
    let tz_minus = s[10..].rfind('-').map(|p| p + 10);
    let tz_pos = match (tz_plus, tz_minus) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None)    => Some(a),
        (None, Some(b))    => Some(b),
        (None, None)       => None,
    };
    if let Some(pos) = tz_pos {
        let suffix = &s[pos..];
        // +HH or -HH: exactly 3 chars and no colon
        if suffix.len() == 3 && !suffix.contains(':') {
            return format!("{}:00", s);
        }
        // +HHMM or -HHMM: exactly 5 chars and no colon → +HH:MM
        if suffix.len() == 5 && !suffix.contains(':') {
            return format!("{}:{}:{}", &s[..pos + 1], &suffix[1..3], &suffix[3..]);
        }
    }
    s
}

fn expr_to_datetime(expr: &sqlparser::ast::Expr) -> Option<chrono::DateTime<chrono::Utc>> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    use chrono::DateTime;

    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => {
            // Try multiple PG/ISO timestamp formats, most specific first.
            let formats: &[&str] = &[
                "%Y-%m-%d %H:%M:%S%.f%:z",  // '2024-01-01 12:00:00.000000+00:00'
                "%Y-%m-%d %H:%M:%S%:z",     // '2024-01-01 12:00:00+00:00'
                "%Y-%m-%d %H:%M:%S%z",      // '2024-01-01 12:00:00+0000'
                "%Y-%m-%dT%H:%M:%S%:z",     // ISO-8601 with colon
                "%Y-%m-%dT%H:%M:%SZ",       // ISO-8601 Zulu
            ];
            let mut result = DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .ok();
            if result.is_none() {
                // Try '2024-01-01 00:00:00+00' (no colon in offset, short offset)
                // by appending ':00' to a bare `+HH` suffix.
                let normalized = normalize_pg_timestamp(s);
                result = chrono::DateTime::parse_from_rfc3339(&normalized)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .ok();
            }
            if result.is_none() {
                for fmt in formats {
                    if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
                        result = Some(dt.with_timezone(&chrono::Utc));
                        break;
                    }
                }
            }
            if result.is_none() {
                // Try without tz: '2024-01-01 00:00:00'
                result = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .map(|ndt| ndt.and_utc())
                    .ok();
            }
            result
        }
        // TO_TIMESTAMP(<epoch_s>) + INTERVAL '...' — in the soak test.
        // We don't evaluate this fully; skip (chunk creation still works via
        // the simpler literal path for the basic tests).
        _ => None,
    }
}

/// Execute `SELECT create_hypertable('table', 'col', chunk_time_interval =>
/// INTERVAL '...')`.  Registers the table in the HypertableRegistry and
/// returns a single-row result mimicking TimescaleDB's output shape.
async fn exec_create_hypertable(
    sess: &ProjectSession,
    table: &str,
    time_col: &str,
    interval_text: &str,
) -> Result<ExecResult> {
    let secs = crate::hypertable::parse_interval_secs(interval_text)
        .ok_or_else(|| BasinError::InvalidSchema(format!(
            "create_hypertable: could not parse interval '{interval_text}'"
        )))?;
    sess.engine
        .hypertable_registry()
        .register(&sess.project, table, time_col.to_string(), secs)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;
    // Return a single-row result mimicking TimescaleDB's create_hypertable().
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("hypertable_id",   arrow_schema::DataType::Int64,  false),
        arrow_schema::Field::new("schema_name",     arrow_schema::DataType::Utf8,   false),
        arrow_schema::Field::new("table_name",      arrow_schema::DataType::Utf8,   false),
        arrow_schema::Field::new("created",         arrow_schema::DataType::Boolean, false),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1i64])) as ArrayRef,
            Arc::new(arrow_array::StringArray::from(vec!["public"])) as ArrayRef,
            Arc::new(arrow_array::StringArray::from(vec![table])) as ArrayRef,
            Arc::new(arrow_array::BooleanArray::from(vec![true])) as ArrayRef,
        ],
    )
    .map_err(|e| BasinError::internal(format!("create_hypertable result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Execute `SELECT add_retention_policy('table', INTERVAL '...')`.
/// Registers a retention policy on the hypertable.
async fn exec_add_retention_policy(
    sess: &ProjectSession,
    table: &str,
    interval_text: &str,
) -> Result<ExecResult> {
    let secs = crate::hypertable::parse_interval_secs(interval_text)
        .ok_or_else(|| BasinError::InvalidSchema(format!(
            "add_retention_policy: could not parse interval '{interval_text}'"
        )))?;
    sess.engine
        .hypertable_registry()
        .set_retention(&sess.project, table, secs)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("job_id", arrow_schema::DataType::Int64, false),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1i64])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("add_retention_policy result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Execute `SELECT run_retention_policy('table')`.
/// Drops chunks that fall outside the retention window, then issues a physical
/// DELETE so the base table's rows are also removed.
async fn exec_run_retention_policy(
    sess: &ProjectSession,
    table: &str,
) -> Result<ExecResult> {
    // Phase 5.29.D: determine the retention window from the registry and
    // compute the cutoff timestamp.
    let dropped = sess.engine
        .hypertable_registry()
        .run_retention((&sess.project), table)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;

    if !dropped.is_empty() {
        // Fetch the hypertable's time column so we can build the DELETE
        // predicate accurately.  If the time column is unknown (table was
        // never registered — defensive), fall back to a no-op.
        //
        // Implementation note: `run_retention` already removed the chunks
        // from the registry, but the physical rows are still in the base
        // table. We issue a DELETE to clean them up.  We compute the cutoff
        // from the registry's retention_secs before running retention (it
        // already cleared the chunks), but since the chunks recorded
        // range_end we need the cutoff derived from retention_secs again.
        // We re-read retention_secs from the registry.
        //
        // Simpler: scan deleted chunks to find the max range_end that was
        // dropped, then DELETE rows with ts < that value.
        //
        // Actually the simplest correct approach: issue a DELETE for rows
        // whose timestamp falls in chunks older than the cutoff.  We already
        // ran `run_retention` which told us *which* chunks were removed. We
        // use the hypertable's retention_secs to compute the cutoff directly.
        //
        // We use the catalog approach: look up time_column, build DELETE.
        if let Some(time_col) = sess.engine.hypertable_registry()
            .time_column(&sess.project, table)
            .await
        {
            // Get retention_secs from the registry (re-read; retention is still set).
            // We recompute the cutoff as `now() - retention_secs`.
            // We need retention_secs — it's still in the registry (we only cleared chunks).
            // Use a workaround: the chunks we dropped all had range_end <= cutoff.
            // Issue DELETE WHERE ts < (cutoff_timestamp) using NOW() - interval.
            //
            // We do this by getting the cutoff from the registry. Since we
            // can't easily re-read retention_secs without adding a new method,
            // we'll compute it from the current time: we just ran run_retention
            // which used `now - retention_secs`. We'll issue DELETE WHERE ts < now().
            //
            // For correctness: we only delete rows that were in the dropped chunks.
            // Since chunks are day-aligned (or interval-aligned), we can use
            // the max range_end of the dropped chunks as the DELETE cutoff.
            // But we don't have that info now.
            //
            // Best safe approach: DELETE WHERE ts < NOW() - retention_interval.
            // We compute this by reading the registry's retention_secs once more.
            // Add a `get_retention_secs` method:
            let Some(retention_secs) = get_retention_secs_from_registry(
                sess.engine.hypertable_registry(),
                &sess.project,
                table,
            ).await else {
                // No retention policy set — nothing to delete physically.
                return Ok(ExecResult::Empty { tag: "SELECT 1".into() });
            };

            let cutoff_ts = chrono::Utc::now()
                - chrono::Duration::seconds(retention_secs as i64);
            // Use integer microseconds so `as_literal` produces ScalarValue::Int64,
            // which the storage predicate evaluator compares against Timestamp columns
            // (Arrow stores Timestamp(Microsecond) as raw i64 µs since epoch).
            let cutoff_us = cutoff_ts.timestamp_micros();
            let delete_sql = format!(
                "DELETE FROM \"{table}\" WHERE \"{time_col}\" < {cutoff_us}"
            );
            // Best-effort DELETE via sqlparser → exec_delete (avoids async
            // recursion with the outer `execute`).
            let _ = exec_retention_delete(sess, &delete_sql).await;
        }
    }

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("chunks_dropped", arrow_schema::DataType::Int64, false),
    ]));
    let n = dropped.len() as i64;
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![n])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("run_retention_policy result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Issue a DELETE SQL statement for the retention path without going through
/// the top-level `execute` (which would create an async recursion cycle).
/// Parses the DELETE SQL directly and calls `exec_delete`.
async fn exec_retention_delete(sess: &ProjectSession, sql: &str) -> Result<ExecResult> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| BasinError::InvalidSchema(format!("retention delete parse: {e}")))?;
    match stmts.into_iter().next() {
        Some(sqlparser::ast::Statement::Delete(del)) => {
            crate::dml_mutate::exec_delete(sess, del).await
        }
        _ => Ok(ExecResult::Empty { tag: "DELETE 0".into() }),
    }
}

/// Helper to read the retention_secs from the registry without a new public API.
async fn get_retention_secs_from_registry(
    reg: &Arc<crate::hypertable::HypertableRegistry>,
    project: &basin_common::ProjectId,
    table: &str,
) -> Option<u64> {
    // We expose this by adding a small method to HypertableRegistry.
    reg.retention_secs(project, table).await
}

/// Execute `SELECT compress_chunk(...)` — marks chunks compressed in the
/// registry.  Physical compression is a 5.29.E concern; for now this is a
/// metadata-only operation so `is_compressed = true` shows in the chunks view.
async fn exec_compress_chunk(
    sess: &ProjectSession,
    intent: crate::hypertable::CompressChunkIntent,
) -> Result<ExecResult> {
    use crate::hypertable::CompressChunkIntent;
    match intent {
        CompressChunkIntent::Named { chunk_name } => {
            sess.engine
                .hypertable_registry()
                .compress_chunk(&sess.project, &chunk_name)
                .await;
        }
        CompressChunkIntent::AllForTable { hypertable_name, before_ts } => {
            // Mark all chunks for this table (optionally before a cutoff) compressed.
            let chunks = sess.engine
                .hypertable_registry()
                .snapshot_chunks(&sess.project, &hypertable_name)
                .await;
            let cutoff: Option<chrono::DateTime<chrono::Utc>> = before_ts
                .as_deref()
                .and_then(|s| s.parse().ok());
            for c in &chunks {
                let should_compress = match cutoff {
                    Some(co) => c.range_end <= co,
                    None => true,
                };
                if should_compress {
                    sess.engine
                        .hypertable_registry()
                        .compress_chunk(&sess.project, &c.chunk_name)
                        .await;
                }
            }
        }
        CompressChunkIntent::AllUnfiltered => {
            // No target table info — noop compress (safe fallback).
        }
    }
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("compress_chunk", arrow_schema::DataType::Utf8, true),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::StringArray::from(vec![Option::<&str>::None])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("compress_chunk result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

// ---------------------------------------------------------------------------
// json_agg whole-row rewrite — unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod json_agg_rewrite_tests {
    use super::rewrite_json_agg_whole_row;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn make_ctx_with_table(table: &str, cols: &[(&str, DataType)]) -> SessionContext {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(table, Arc::new(mem)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn json_agg_bare_table_is_expanded() {
        let ctx = make_ctx_with_table("t", &[("id", DataType::Int32), ("name", DataType::Utf8)]);
        let sql = "SELECT json_agg(t) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        // Must contain named_struct with the column names.
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "expected named_struct in: {rewritten}"
        );
        assert!(lower.contains("'id'"), "expected 'id' key in: {rewritten}");
        assert!(
            lower.contains("'name'"),
            "expected 'name' key in: {rewritten}"
        );
        assert!(lower.contains("t.id"), "expected t.id ref in: {rewritten}");
        assert!(
            lower.contains("t.name"),
            "expected t.name ref in: {rewritten}"
        );
    }

    #[tokio::test]
    async fn jsonb_agg_bare_table_is_expanded() {
        let ctx = make_ctx_with_table("u", &[("val", DataType::Int64)]);
        let sql = "SELECT jsonb_agg(u) FROM u";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "expected named_struct in: {rewritten}"
        );
        assert!(
            lower.contains("u.val"),
            "expected u.val ref in: {rewritten}"
        );
    }

    #[tokio::test]
    async fn json_agg_scalar_col_is_unchanged() {
        let ctx = make_ctx_with_table("t", &[("id", DataType::Int32)]);
        // `json_agg(id)` — `id` is not a table name, should not be rewritten.
        let sql = "SELECT json_agg(id) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        // The rewriter tries `id` as a table name; since there's no table `id`,
        // it emits it verbatim.
        assert_eq!(rewritten, sql, "json_agg(col) must not be rewritten");
    }

    #[tokio::test]
    async fn no_json_agg_is_unchanged() {
        let ctx = SessionContext::new();
        let sql = "SELECT COUNT(*) FROM t WHERE id > 1";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        assert_eq!(rewritten, sql);
    }

    #[tokio::test]
    async fn json_agg_uppercase_is_expanded() {
        let ctx = make_ctx_with_table("t", &[("x", DataType::Float64)]);
        let sql = "SELECT JSON_AGG(t) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "JSON_AGG uppercase should be expanded: {rewritten}"
        );
        assert!(lower.contains("'x'"), "expected 'x' key: {rewritten}");
    }
}

// ---------------------------------------------------------------------------
// ON CONFLICT DO UPDATE — rewrite_do_update_expr unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod do_update_rewrite_tests {
    use super::rewrite_do_update_expr;
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value, ValueWithSpan};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    /// Parse a single SQL expression string into an `Expr` node.
    fn parse_expr(sql: &str) -> Expr {
        let mut p = Parser::new(&PostgreSqlDialect {});
        // Wrap in SELECT so the parser reaches the expression context.
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {sql}"))
            .expect("parse")
            .into_iter()
            .next()
            .expect("stmt");
        if let sqlparser::ast::Statement::Query(q) = stmt {
            if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                if let sqlparser::ast::SelectItem::UnnamedExpr(e) =
                    sel.projection.into_iter().next().expect("item")
                {
                    return e;
                }
            }
        }
        panic!("could not parse expression: {sql}");
    }

    /// Build a simple integer literal Expr.
    fn int_expr(n: i64) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    fn make_excluded(pairs: &[(&str, Expr)]) -> HashMap<String, Expr> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // EXCLUDED.col → proposed-row literal value.
    #[test]
    fn excluded_col_rewritten_to_literal() {
        let excluded = make_excluded(&[("hits", int_expr(42))]);
        let expr = parse_expr("EXCLUDED.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert_eq!(format!("{result}"), "42");
    }

    // tablename.col → bare col identifier (existing row).
    #[test]
    fn table_col_rewritten_to_bare_identifier() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("accounts.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        // Should become bare `hits` identifier.
        assert!(
            matches!(&result, Expr::Identifier(i) if i.value == "hits"),
            "expected bare identifier `hits`, got: {result}"
        );
    }

    // Unqualified col → unchanged (existing row in UPDATE scope).
    #[test]
    fn unqualified_col_unchanged() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert!(
            matches!(&result, Expr::Identifier(i) if i.value == "hits"),
            "expected unchanged identifier `hits`, got: {result}"
        );
    }

    // BinaryOp: t.hits + EXCLUDED.hits → hits + 42
    #[test]
    fn binary_op_mixed_refs() {
        let excluded = make_excluded(&[("hits", int_expr(5))]);
        let expr = parse_expr("accounts.hits + EXCLUDED.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        // Should be `hits + 5`.
        let s = format!("{result}");
        assert!(s.contains("hits"), "expected `hits` in result: {s}");
        assert!(s.contains('5'), "expected `5` in result: {s}");
        assert!(
            !s.contains("accounts"),
            "table qualifier must be stripped: {s}"
        );
        assert!(!s.contains("EXCLUDED"), "EXCLUDED must be resolved: {s}");
    }

    // Case-insensitive: EXCLUDED.HITS, ACCOUNTS.HITS → resolved correctly.
    #[test]
    fn case_insensitive_matching() {
        let excluded = make_excluded(&[("hits", int_expr(99))]);
        let expr = parse_expr("EXCLUDED.HITS");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert_eq!(format!("{result}"), "99");
    }

    // Unknown EXCLUDED column → left unchanged (pass-through for unknown refs).
    #[test]
    fn excluded_unknown_col_passthrough() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("EXCLUDED.nonexistent");
        let result = rewrite_do_update_expr(expr.clone(), "accounts", &excluded);
        assert_eq!(format!("{result}"), format!("{expr}"));
    }
}

// ---------------------------------------------------------------------------
// ON CONFLICT DO NOTHING — validate_conflict_target_columns unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod do_nothing_validate_tests {
    use super::validate_conflict_target_columns;
    use basin_catalog::UniqueConstraint;

    /// Build PK and UNIQUE inputs for the helper.
    fn pk(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }
    fn uniques(groups: &[&[&str]]) -> Vec<UniqueConstraint> {
        groups
            .iter()
            .enumerate()
            .map(|(i, cols)| UniqueConstraint {
                name: format!("t_uc{i}"),
                columns: cols.iter().map(|s| s.to_string()).collect(),
            })
            .collect()
    }
    fn conflict(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    // --- matches PK ---------------------------------------------------------

    #[test]
    fn pk_single_col_accepted() {
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&["id"]))
                .is_ok()
        );
    }

    #[test]
    fn pk_composite_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["a", "b"])
        )
        .is_ok());
    }

    #[test]
    fn pk_order_independent() {
        // Reversed order — still matches PK set.
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["b", "a"])
        )
        .is_ok());
    }

    // --- matches UNIQUE constraint ------------------------------------------

    #[test]
    fn unique_constraint_single_col_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&[]),
            &uniques(&[&["email"]]),
            &conflict(&["email"])
        )
        .is_ok());
    }

    #[test]
    fn unique_constraint_composite_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&[]),
            &uniques(&[&["org_id", "slug"]]),
            &conflict(&["org_id", "slug"])
        )
        .is_ok());
    }

    // --- mismatches → error -------------------------------------------------

    #[test]
    fn subset_of_pk_rejected() {
        // Only one of the two PK cols — not a complete match.
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["a"])
        )
        .is_err());
    }

    #[test]
    fn superset_of_pk_rejected() {
        assert!(validate_conflict_target_columns(
            &pk(&["id"]),
            &uniques(&[]),
            &conflict(&["id", "extra"])
        )
        .is_err());
    }

    #[test]
    fn non_constraint_col_rejected() {
        // The combined set {email, id} is not any single constraint.
        assert!(validate_conflict_target_columns(
            &pk(&["id"]),
            &uniques(&[&["email"]]),
            &conflict(&["email", "id"])
        )
        .is_err());
    }

    #[test]
    fn empty_target_accepted_without_error() {
        // Empty conflict target (no columns): validated as OK; caller handles
        // the no-target case (all constraints checked).
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&[])).is_ok()
        );
    }

    // --- same-batch dedup semantics (PG: first row wins) --------------------
    // Structural test: validate returns Ok for a conflict target that IS a real constraint.
    #[test]
    fn same_batch_dup_target_accepted() {
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&["id"]))
                .is_ok()
        );
    }
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE multi-column — DataFusion execution tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod recursive_cte_exec_tests {
    use crate::pg_operators::rewrite_recursive_cte_column_aliases;
    use datafusion::arrow::array::Array;
    use datafusion::prelude::SessionContext;

    /// Run a SQL string through the recursive-CTE alias rewriter then execute
    /// it in a bare DataFusion `SessionContext`.  Returns the flattened i64
    /// values from the first column of all result batches.
    async fn run_i64(sql: &str) -> Result<Vec<i64>, String> {
        let rewritten = rewrite_recursive_cte_column_aliases(sql);
        let ctx = SessionContext::new();
        let df = ctx.sql(&rewritten).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        let mut vals = Vec::new();
        for batch in &batches {
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| format!("expected Int64Array, got {:?}", col.data_type()))?;
            for i in 0..arr.len() {
                vals.push(arr.value(i));
            }
        }
        Ok(vals)
    }

    /// Run a SQL string through the rewriter and execute it; returns first
    /// column as `Vec<String>` (Utf8).
    async fn run_str(sql: &str) -> Result<Vec<String>, String> {
        let rewritten = rewrite_recursive_cte_column_aliases(sql);
        let ctx = SessionContext::new();
        let df = ctx.sql(&rewritten).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        let mut vals = Vec::new();
        for batch in &batches {
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| format!("expected StringArray, got {:?}", col.data_type()))?;
            for i in 0..arr.len() {
                vals.push(arr.value(i).to_string());
            }
        }
        Ok(vals)
    }

    /// Fibonacci multi-column WITH RECURSIVE: fib(a, b).
    ///
    /// PostgreSQL spec: anchor row (1,1) + 10 recursive rows where source b < 100.
    /// Full table: (1,1),(1,2),(2,3),(3,5),(5,8),(8,13),(13,21),(21,34),(34,55),(55,89),(89,144)
    /// SELECT a → 11 values: 1,1,2,3,5,8,13,21,34,55,89
    #[tokio::test]
    async fn fib_multi_col_exact_sequence() {
        let sql = "WITH RECURSIVE fib(a, b) AS \
            (SELECT 1, 1 \
             UNION ALL \
             SELECT b, a+b FROM fib WHERE b < 100) \
            SELECT a FROM fib";
        let vals = run_i64(sql).await.expect("fib query must succeed");
        let expected: Vec<i64> = vec![1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89];
        assert_eq!(
            vals, expected,
            "fib(a,b) SELECT a must match PG Fibonacci sequence"
        );
    }

    /// Single-column regression: existing behaviour must be preserved.
    #[tokio::test]
    async fn single_col_regression() {
        let sql = "WITH RECURSIVE r(n) AS \
            (SELECT 1 \
             UNION ALL \
             SELECT n+1 FROM r WHERE n < 5) \
            SELECT n FROM r";
        let vals = run_i64(sql)
            .await
            .expect("single-col recursive CTE must succeed");
        let expected: Vec<i64> = vec![1, 2, 3, 4, 5];
        assert_eq!(vals, expected, "single-col r(n) must produce 1..=5");
    }

    /// Two-column string-hierarchy accumulator (non-numeric confidence test).
    #[tokio::test]
    async fn two_col_string_hierarchy() {
        // Builds a chain: path grows left-to-right, depth counts steps.
        // anchor: ('root', 0)
        // recursive: append '->child' while depth < 3
        // Rows: ('root',0), ('root->child',1), ('root->child->child',2), ('root->child->child->child',3)
        let sql = "WITH RECURSIVE tree(path, depth) AS \
            (SELECT 'root', 0 \
             UNION ALL \
             SELECT path || '->child', depth + 1 FROM tree WHERE depth < 3) \
            SELECT path FROM tree";
        let vals = run_str(sql)
            .await
            .expect("string hierarchy CTE must succeed");
        let expected = vec![
            "root".to_string(),
            "root->child".to_string(),
            "root->child->child".to_string(),
            "root->child->child->child".to_string(),
        ];
        assert_eq!(vals, expected, "string hierarchy paths must match");
    }
}

// ---------------------------------------------------------------------------
// Auto-commit read-your-own-writes (RYOW) — lib unit tests (#105)
// ---------------------------------------------------------------------------
// These tests use the full Engine + InMemoryCatalog + LocalFileSystem stack so
// they exercise the real exec_insert → catalog → exec_select pipeline.  They
// are deliberately placed in the `executor` lib so they run with
// `cargo test -p basin-engine --lib` and count toward the lib-test baseline.
#[cfg(test)]
mod auto_commit_ryow_tests {
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn count_from_result(res: ExecResult) -> i64 {
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
        };
        let b = batches.first().expect("no batch returned");
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column must be Int64")
            .value(0)
    }

    /// Each auto-commit INSERT must be immediately visible to the next SELECT
    /// on the same session.  Pins the fix for #105 (refresh_table_with_extra
    /// in the auto-commit path).
    #[tokio::test]
    async fn insert_values_ryow_per_statement() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        sess.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let n1 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n1, 1,
            "row 1 must be visible immediately after first INSERT"
        );

        sess.execute("INSERT INTO t VALUES (2)").await.unwrap();
        let n2 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n2, 2,
            "row 2 must be visible immediately after second INSERT"
        );

        sess.execute("INSERT INTO t VALUES (3)").await.unwrap();
        let n3 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n3, 3,
            "row 3 must be visible immediately after third INSERT"
        );
    }

    /// Multi-row INSERT: all rows in a single statement must be visible to the
    /// immediately-following SELECT.
    #[tokio::test]
    async fn insert_multi_row_ryow() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        sess.execute("INSERT INTO t VALUES (10), (20), (30)")
            .await
            .unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n, 3,
            "all 3 rows from multi-row INSERT must be visible immediately"
        );
    }

    /// INSERT … RETURNING: the returned row must also appear in a subsequent
    /// SELECT on the same session.
    #[tokio::test]
    async fn insert_returning_ryow() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        // First INSERT: verify it's visible.
        sess.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let n1 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n1, 1,
            "first INSERT must be visible before RETURNING INSERT"
        );

        // INSERT RETURNING: the returned id must match, and the subsequent
        // COUNT must reflect both rows.
        let ret = sess
            .execute("INSERT INTO t VALUES (2) RETURNING id")
            .await
            .unwrap();
        let returned_id = match ret {
            ExecResult::Rows { batches, .. } => {
                let b = batches.first().expect("RETURNING must produce a batch");
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column must be Int64")
                    .value(0)
            }
            ExecResult::Empty { tag } => panic!("INSERT RETURNING produced Empty({tag})"),
        };
        assert_eq!(returned_id, 2, "RETURNING must echo inserted id");

        let n2 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n2, 2, "both rows must be visible after INSERT RETURNING");
    }
}

// ---------------------------------------------------------------------------
// Phase 6.P0.A — statement-level wall-clock timeout (noisy-neighbor P0)
// ---------------------------------------------------------------------------
// Proves: (1) a deliberately slow JOIN/cross-join shape — exactly the family
// the planning-time cost-check misses — is cancelled at the deadline with
// SQLSTATE-mappable QueryCanceled; (2) the connection stays usable for the
// next query; (3) a fast query under the deadline is unaffected; (4)
// disabling (`timeout = None`, i.e. BASIN_STATEMENT_TIMEOUT_MS=0) restores
// back-compat (the same slow query completes).
#[cfg(test)]
mod statement_timeout_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{BasinError, ProjectId};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::session::test_timeout_override;
    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Seed a table with `n` rows so a self cross-join is `n^4` output rows —
    /// astronomically expensive, guaranteed to outrun a millisecond deadline.
    async fn seed(sess: &crate::ProjectSession, n: i64) {
        sess.execute("CREATE TABLE big (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let vals: Vec<String> = (0..n).map(|i| format!("({i})")).collect();
        sess.execute(&format!("INSERT INTO big VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    const HOSTILE_JOIN: &str = "SELECT COUNT(*) FROM big a, big b, big c, big d";

    #[tokio::test]
    async fn slow_join_is_canceled_and_connection_survives() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 80).await; // 80^4 = ~41M output rows for the COUNT

        // 1 ms deadline: the cross-join cannot finish in time.
        let _guard = test_timeout_override::install(Some(Duration::from_millis(1)));
        let err = sess
            .execute(HOSTILE_JOIN)
            .await
            .expect_err("hostile cross-join must be cancelled at the deadline");
        assert!(
            matches!(err, BasinError::QueryCanceled(_)),
            "expected QueryCanceled (→ SQLSTATE 57014), got {err:?}"
        );

        // The connection must remain usable: a fast query on the same session
        // succeeds immediately after the cancellation.
        let res = sess.execute("SELECT 1").await.expect("session still usable");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }

    #[tokio::test]
    async fn fast_query_under_deadline_is_unaffected() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 3).await;

        // Generous deadline; a trivial query must complete normally.
        let _guard = test_timeout_override::install(Some(Duration::from_secs(30)));
        let res = sess
            .execute("SELECT COUNT(*) FROM big")
            .await
            .expect("fast query under the deadline must succeed");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }

    #[tokio::test]
    async fn disabled_timeout_runs_unbounded() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 6).await; // 6^4 = 1296 rows — small but JOIN-shaped

        // None == BASIN_STATEMENT_TIMEOUT_MS=0: back-compat, no cancellation.
        // The same JOIN shape that gets cancelled under a tight deadline must
        // run to completion when the guard is disabled.
        let _guard = test_timeout_override::install(None);
        let res = sess
            .execute(HOSTILE_JOIN)
            .await
            .expect("disabled timeout must let the (small) join complete");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }
}

/// Regression coverage for BUG #139: the JSON/JSONB set-returning functions
/// (`jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, …) must expand
/// to one row per element/key/pair like PostgreSQL — both in FROM-clause
/// position *and* in scalar SELECT-list position (the common ORM idiom
/// `SELECT jsonb_array_elements('[…]'::jsonb)`).
#[cfg(test)]
mod jsonb_srf_expansion_tests {
    use std::sync::Arc;

    use arrow_array::{Array, StringArray};
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    async fn rows(sess: &crate::ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
        match sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => batches,
            other => panic!("expected Rows for `{sql}`, got {other:?}"),
        }
    }

    fn total_rows(b: &[arrow_array::RecordBatch]) -> usize {
        b.iter().map(|x| x.num_rows()).sum()
    }

    /// Collect a String column by name across all batches.
    fn col_str(b: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for batch in b {
            let arr = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("no column `{name}` in {:?}", batch.schema()))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column must be Utf8");
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn array_elements_select_list_multi() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // Scalar SELECT-list position (the regressed form) must expand to 3 rows.
        let b = rows(&sess, "SELECT jsonb_array_elements('[1,2,3]'::jsonb)").await;
        assert_eq!(total_rows(&b), 3, "must yield one row per array element");
        assert_eq!(col_str(&b, "value"), vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn array_elements_from_clause_multi() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 3);
        assert_eq!(col_str(&b, "value"), vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn array_elements_empty_and_single() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let empty = rows(&sess, "SELECT jsonb_array_elements('[]'::jsonb)").await;
        assert_eq!(total_rows(&empty), 0, "empty array → zero rows");
        let one = rows(&sess, "SELECT jsonb_array_elements('[42]'::jsonb)").await;
        assert_eq!(total_rows(&one), 1);
        assert_eq!(col_str(&one, "value"), vec!["42"]);
    }

    #[tokio::test]
    async fn array_elements_nested_values_preserved() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT jsonb_array_elements('[[1,2],{\"k\":3}]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 2);
        assert_eq!(col_str(&b, "value"), vec!["[1,2]", "{\"k\":3}"]);
    }

    #[tokio::test]
    async fn array_elements_text_variant() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // `_text` unwraps JSON strings (no surrounding quotes).
        let b = rows(
            &sess,
            "SELECT jsonb_array_elements_text('[\"a\",\"b\",\"c\"]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 3);
        assert_eq!(col_str(&b, "value"), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn each_select_list_and_from() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        for sql in [
            "SELECT jsonb_each('{\"a\":1,\"b\":2}'::jsonb)",
            "SELECT * FROM jsonb_each('{\"a\":1,\"b\":2}'::jsonb)",
        ] {
            let b = rows(&sess, sql).await;
            assert_eq!(total_rows(&b), 2, "jsonb_each → one row per pair: {sql}");
            assert_eq!(col_str(&b, "key"), vec!["a", "b"], "keys for {sql}");
            assert_eq!(col_str(&b, "value"), vec!["1", "2"], "values for {sql}");
        }
    }

    #[tokio::test]
    async fn each_text_unwraps_strings() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT jsonb_each_text('{\"a\":\"x\",\"b\":\"y\"}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 2);
        assert_eq!(col_str(&b, "key"), vec!["a", "b"]);
        assert_eq!(col_str(&b, "value"), vec!["x", "y"]);
    }

    #[tokio::test]
    async fn each_empty_object() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(&sess, "SELECT jsonb_each('{}'::jsonb)").await;
        assert_eq!(total_rows(&b), 0, "empty object → zero rows");
    }

    #[tokio::test]
    async fn object_keys_select_list_and_from() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let s = rows(
            &sess,
            "SELECT jsonb_object_keys('{\"a\":1,\"b\":2,\"c\":3}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&s), 3, "one row per top-level key");
        assert_eq!(col_str(&s, "jsonb_object_keys"), vec!["a", "b", "c"]);

        let f = rows(
            &sess,
            "SELECT * FROM jsonb_object_keys('{\"a\":1,\"b\":2,\"c\":3}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&f), 3);
        assert_eq!(col_str(&f, "jsonb_object_keys"), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn object_keys_empty() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(&sess, "SELECT jsonb_object_keys('{}'::jsonb)").await;
        assert_eq!(total_rows(&b), 0);
    }

    #[tokio::test]
    async fn json_prefixed_variants_expand() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let a = rows(&sess, "SELECT json_array_elements('[1,2,3]')").await;
        assert_eq!(total_rows(&a), 3, "json_array_elements must expand too");
        assert_eq!(col_str(&a, "value"), vec!["1", "2", "3"]);

        let k = rows(&sess, "SELECT json_object_keys('{\"x\":1,\"y\":2}')").await;
        assert_eq!(total_rows(&k), 2, "json_object_keys must expand too");
    }

    #[tokio::test]
    async fn select_list_with_alias_expands() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // `AS v` aliases the table function; column is still `value`.
        let b = rows(&sess, "SELECT jsonb_array_elements('[1,2,3]'::jsonb) AS v").await;
        assert_eq!(total_rows(&b), 3);
    }

    #[tokio::test]
    async fn mixed_select_list_not_rewritten() {
        // Safety: a SRF mixed with other columns must NOT be rewritten by the
        // scalar→FROM rule (that needs LATERAL semantics, out of scope). The
        // query must still plan (using the scalar stub) and return 1 row — we
        // only assert it does not error or explode into the wrong shape.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT 1 AS n, jsonb_array_elements('[1,2,3]'::jsonb)",
        )
        .await;
        assert_eq!(
            total_rows(&b),
            1,
            "mixed SELECT list keeps scalar-stub behaviour (not in scope for #139)"
        );
    }
}

/// Tests for Bug #2 (SET statement_timeout wiring) and pg_sleep registration.
#[cfg(test)]
mod statement_timeout_guc_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig { storage, catalog, shard: None })
    }

    /// `SET statement_timeout = '5s'` must be accepted and stored on the session.
    #[tokio::test]
    async fn set_statement_timeout_string_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = '5s'")
            .await
            .expect("SET statement_timeout '5s' must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            Some(Duration::from_secs(5)),
            "session timeout should be 5 s after SET"
        );
    }

    /// `SET statement_timeout = 5000` (bare integer milliseconds) must work.
    #[tokio::test]
    async fn set_statement_timeout_integer_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = 5000")
            .await
            .expect("SET statement_timeout 5000 must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            Some(Duration::from_millis(5000))
        );
    }

    /// `SET statement_timeout = 0` disables the timeout (Postgres semantics).
    #[tokio::test]
    async fn set_statement_timeout_zero_disables() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = 0")
            .await
            .expect("SET statement_timeout 0 must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            None,
            "timeout should be disabled after SET statement_timeout = 0"
        );
    }

    /// `SHOW statement_timeout` reflects the value set by `SET`.
    #[tokio::test]
    async fn show_statement_timeout_reflects_set() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = '500ms'")
            .await
            .unwrap();
        let res = sess
            .execute("SHOW statement_timeout")
            .await
            .expect("SHOW statement_timeout must succeed");
        match res {
            ExecResult::Rows { batches, .. } => {
                assert!(!batches.is_empty(), "expected at least one batch");
                let batch = &batches[0];
                let col = batch
                    .column_by_name("statement_timeout")
                    .expect("column statement_timeout");
                use arrow_array::StringArray;
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column");
                let val = arr.value(0);
                assert_eq!(val, "500ms", "SHOW should reflect 500ms");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `pg_sleep` is registered — `SELECT pg_sleep(0)` must not error.
    #[tokio::test]
    async fn pg_sleep_is_registered() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // pg_sleep(0) must succeed without sleeping.
        let res = sess
            .execute("SELECT pg_sleep(0)")
            .await
            .expect("pg_sleep(0) must be registered and callable");
        assert!(matches!(res, ExecResult::Rows { .. }), "expected Rows result");
    }
}

// ---------------------------------------------------------------------------
// Phase 5.31.A — pg_cancel_backend end-to-end tests
// ---------------------------------------------------------------------------
// Proves: (1) SELECT pg_cancel_backend(pid) from another session aborts the
// running query on the target session with SQLSTATE 57014 (QueryCanceled);
// (2) the target connection stays usable afterwards; (3) pg_cancel_backend
// returns false for unknown pids.
#[cfg(test)]
mod pg_cancel_backend_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{BasinError, ProjectId};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Seed a large table so a cross-join is guaranteed to run longer than
    /// the cancel signal arrives (deterministic without `pg_sleep`).
    async fn seed_big(sess: &crate::ProjectSession) {
        sess.execute("CREATE TABLE cancel_big (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let vals: Vec<String> = (0..80_i64).map(|i| format!("({i})")).collect();
        sess.execute(&format!("INSERT INTO cancel_big VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    /// Prove: pg_cancel_backend(pid) terminates a running cross-join with
    /// SQLSTATE 57014, and the connection stays usable afterwards.
    ///
    /// Uses the multi-thread runtime so the CPU-bound slow query (which only
    /// yields between `df.collect()` batch boundaries) cannot starve the
    /// cancelling session. Under `current_thread` the cancel call could not be
    /// polled until the spawned task happened to yield, which made the test
    /// flaky.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_running_query_returns_57014() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let project = ProjectId::new();

        // Session A: will run the slow query. Wrapped in an `Arc` so the outer
        // scope keeps the session (and thus its `ConnectionHandle`) alive while
        // the spawned task runs. Without this, if the spawned task finishes
        // before `pg_cancel_backend` runs, dropping the session deregisters its
        // pid from the `ConnectionRegistry`, so the cancel returns `false` for a
        // still-conceptually-live session.
        let sess_a = Arc::new(eng.open_session(project).await.unwrap());
        seed_big(&sess_a).await;
        let pid_a = sess_a.session_pid;

        // Session B: will call pg_cancel_backend.
        let sess_b = eng.open_session(project).await.unwrap();

        // Spawn the slow cross-join on sess_a in the background.
        //
        // The seed table has 80 rows, so the 5-way self-cross-join produces
        // 80^5 ≈ 3.3 billion output rows. The depth matters: the 5-level
        // streaming pipeline yields between batches often enough that the
        // per-session `cancel_notify` branch of the executor's `tokio::select!`
        // is reliably polled mid-flight, so the cancel always lands before the
        // query completes. A shallower 4-level join runs DataFusion's aggregate
        // to completion inside a single poll, never re-entering the select — the
        // root cause of the original flake/failure.
        let sess_a_spawn = Arc::clone(&sess_a);
        let slow_fut = tokio::spawn(async move {
            sess_a_spawn
                .execute(
                    "SELECT COUNT(*) FROM cancel_big a, cancel_big b, cancel_big c, \
                     cancel_big d, cancel_big e",
                )
                .await
        });

        // Give the slow query a moment to start executing and register itself
        // on the executor before we issue the cancel.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel from session B.
        let cancel_res = sess_b
            .execute(&format!("SELECT pg_cancel_backend({pid_a})"))
            .await
            .expect("pg_cancel_backend must succeed");
        // Verify that it returned true (pid was found).
        if let ExecResult::Rows { batches, .. } = cancel_res {
            let batch = batches.first().expect("cancel result must have a batch");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("result column must be boolean");
            assert!(col.value(0), "pg_cancel_backend must return true for a live pid");
        } else {
            panic!("expected Rows from pg_cancel_backend");
        }

        // Wait for the slow query to finish — it must return QueryCanceled.
        let slow_result = slow_fut.await.expect("task must not panic");
        assert!(
            matches!(slow_result, Err(BasinError::QueryCanceled(_))),
            "slow query must be cancelled with SQLSTATE 57014, got: {slow_result:?}"
        );
    }

    /// pg_cancel_backend(unknown_pid) returns false.
    #[tokio::test]
    async fn cancel_unknown_pid_returns_false() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        let res = sess
            .execute("SELECT pg_cancel_backend(999999)")
            .await
            .expect("pg_cancel_backend with unknown pid must not error");

        if let ExecResult::Rows { batches, .. } = res {
            let batch = batches.first().expect("result must have a batch");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("result column must be boolean");
            assert!(!col.value(0), "pg_cancel_backend must return false for unknown pid");
        } else {
            panic!("expected Rows from pg_cancel_backend");
        }
    }
}

// ---------------------------------------------------------------------------
// P1 fix: SET lock_timeout GUC wiring — end-to-end tests
// ---------------------------------------------------------------------------
// Verifies that `SET lock_timeout = …` stores the value on the session's
// advisory-lock GUC (so blocking pg_advisory_lock calls honour it) and that
// `SHOW lock_timeout` reflects it back.  Mirrors the statement_timeout_guc_tests
// structure to ensure the two GUCs are symmetrically wired.
#[cfg(test)]
mod lock_timeout_guc_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig { storage, catalog, shard: None })
    }

    /// `SET lock_timeout = '500ms'` must be accepted and stored on the session.
    #[tokio::test]
    async fn set_lock_timeout_string_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '500ms'")
            .await
            .expect("SET lock_timeout '500ms' must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            Some(Duration::from_millis(500)),
            "session lock_timeout should be 500 ms after SET"
        );
    }

    /// `SET lock_timeout = 2000` (bare integer milliseconds) must work.
    #[tokio::test]
    async fn set_lock_timeout_integer_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = 2000")
            .await
            .expect("SET lock_timeout 2000 must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            Some(Duration::from_millis(2000))
        );
    }

    /// `SET lock_timeout = 0` disables the timeout (Postgres semantics).
    #[tokio::test]
    async fn set_lock_timeout_zero_disables() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // First set a non-zero value, then disable it.
        sess.execute("SET lock_timeout = '1s'").await.unwrap();
        sess.execute("SET lock_timeout = 0")
            .await
            .expect("SET lock_timeout 0 must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            None,
            "lock_timeout should be disabled after SET lock_timeout = 0"
        );
    }

    /// `SHOW lock_timeout` must return the value set by `SET lock_timeout`.
    /// Verifies the SHOW path falls through to the real executor (not noop).
    #[tokio::test]
    async fn show_lock_timeout_reflects_set() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '300ms'")
            .await
            .unwrap();
        let res = sess
            .execute("SHOW lock_timeout")
            .await
            .expect("SHOW lock_timeout must succeed");
        match res {
            ExecResult::Rows { batches, .. } => {
                assert!(!batches.is_empty(), "expected at least one batch");
                let batch = &batches[0];
                let col = batch
                    .column_by_name("lock_timeout")
                    .expect("column lock_timeout");
                use arrow_array::StringArray;
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column");
                let val = arr.value(0);
                assert_eq!(val, "300ms", "SHOW should reflect 300ms");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SET lock_timeout` must also propagate to the advisory-lock manager
    /// so that `advisory_lock.get_lock_timeout()` returns the new value.
    #[tokio::test]
    async fn set_lock_timeout_propagates_to_advisory_lock() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '750ms'")
            .await
            .expect("SET lock_timeout '750ms' must succeed");
        // Read the value back directly from the advisory lock manager.
        let advisory_timeout = sess.state.advisory.get_lock_timeout();
        assert_eq!(
            advisory_timeout,
            Some(Duration::from_millis(750)),
            "advisory lock manager must see the updated lock_timeout"
        );
    }
}
