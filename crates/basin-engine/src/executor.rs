//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, TableMetadata};
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName};
use basin_storage::WriteOptions;
use sqlparser::ast::{ObjectName, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::analytical_route::is_analytical;
use crate::convert::{batch_df_to_ws, schema_df_to_ws};
use crate::ddl::{extract_create_table_cluster_by, partition_spec_from_ast};
use crate::dml::{batch_from_rows, group_rows_by_partition};
use crate::events::{
    build_row_json, dispatch_post_commit, dispatch_pre_commit, make_event, registry_has_any,
};
use crate::fast_select::{execute_simple_select, match_simple_select};
use crate::lifecycle::{
    extract_create_table_lifecycle, extract_select_include_deleted, CreateTableLifecycle,
};
use crate::session::refresh_table;
use crate::{ExecResult, TenantSession};
use basin_catalog::PartitionSpec;

pub(crate) async fn execute(sess: &TenantSession, sql: &str) -> Result<ExecResult> {
    // Keep a reference to the SQL the user actually wrote. The rewriter
    // below mangles vector operators into UDF calls; that rewrite is
    // irrelevant to (and would only confuse) the analytical engine, which
    // doesn't know our UDFs.
    let raw_sql = sql;

    // Phase 5.8: Basin-specific ALTER TABLE extensions (`SET cold_after`,
    // `SET cold_age_column`, `SET BLOOM FILTERS ON (...)`) that sqlparser
    // 0.52 doesn't model. Pre-screen the raw SQL before sqlparser sees
    // it; on a match we route to the catalog mutator directly. Anything
    // else (including standard ALTER TABLE forms) returns Ok(None) and
    // falls through.
    if let Some(ext) = crate::alter::match_basin_alter_extension(sql)? {
        let table = ext.table().clone();
        let tag = ext
            .apply(&sess.engine.config().catalog, &sess.tenant)
            .await?;
        crate::session::refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table)
            .await?;
        return Ok(ExecResult::Empty { tag: tag.into() });
    }

    // REFRESH MATERIALIZED VIEW <name> [WITH (full = true)] — sqlparser
    // has no AST node for REFRESH, so we recognise the full statement
    // textually and dispatch. `force_full` toggles the v0.1 opt-out from
    // incremental refresh.
    if let Some((name, force_full)) = crate::cv_ddl::match_refresh_materialized_view(sql)? {
        return crate::cv_ddl::exec_refresh_materialized_view(sess, &name, force_full).await;
    }

    // ALTER FUNCTION <name>(<args>) RENAME TO <new>: sqlparser 0.52 has no
    // AlterFunction AST node, so we recognise the full statement textually
    // and dispatch to the catalog rename helper.
    if let Some((old, new)) = crate::function_ddl::match_alter_function_rename(sql)? {
        return crate::function_ddl::exec_alter_function_rename(sess, &old, &new).await;
    }

    // ALTER TYPE <name> ADD VALUE 'label': sqlparser 0.52 has no
    // AlterType AST node either; textual pre-screen + dispatch.
    if let Some((name, value)) = crate::type_ddl::match_alter_type_add_value(sql)? {
        return crate::type_ddl::exec_alter_type_add_value(sess, &name, &value).await;
    }

    // CREATE DOMAIN: sqlparser 0.52's CREATE parser rejects `DOMAIN`.
    if let Some((name, base, check)) = crate::type_ddl::match_create_domain(sql)? {
        return crate::type_ddl::exec_create_domain(sess, &name, base, check).await;
    }

    // DROP DOMAIN: sqlparser 0.52's DROP parser rejects `DOMAIN`.
    if let Some((name, if_exists)) = crate::type_ddl::match_drop_domain(sql)? {
        return crate::type_ddl::exec_drop_domain(sess, &name, if_exists).await;
    }

    // CREATE PROCEDURE … LANGUAGE sql AS $$ … $$: sqlparser 0.52 only
    // parses the T-SQL `AS BEGIN … END` shape natively, so the
    // PG-style form is recognised textually before sqlparser sees it.
    // See `procedure_ddl::match_create_procedure` for the shape.
    if let Some((name, args, body)) = crate::procedure_ddl::match_create_procedure(sql)? {
        return crate::procedure_ddl::exec_create_procedure(sess, &name, args, &body).await;
    }

    // 5.11.I — `ALTER TABLE … SUBSCRIBE WEBHOOK …`. sqlparser 0.52's
    // ALTER TABLE AST has no SUBSCRIBE arm; the matcher in
    // `crate::webhook_ddl` recognises the full shape textually.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_subscribe_webhook(sql)? {
        crate::webhook_ddl::exec_subscribe_webhook(
            intent,
            &sess.tenant,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.I — `ALTER TABLE … UNSUBSCRIBE WEBHOOK <name>`. Same rationale
    // as the SUBSCRIBE arm above.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_unsubscribe_webhook(sql)? {
        crate::webhook_ddl::exec_unsubscribe_webhook(
            intent,
            &sess.tenant,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `ALTER TABLE … REACT ON … EXECUTE <body>`. The body matcher
    // returns `None` when the statement is the C2 constraint-shaped form
    // (which the next arm handles), so the dispatch order is:
    //   body matcher first → `None` for constraint shape →
    //   constraint matcher → DROP REACTOR.
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_on(sql)? {
        crate::reactor_ddl::exec_react_on(intent, &sess.tenant, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C2 — `ALTER TABLE … REACT ON … CONSTRAINT (<predicate>)`.
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_constraint(sql)? {
        crate::reactor_ddl::exec_react_constraint(
            intent,
            &sess.tenant,
            &sess.engine.config().catalog,
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `DROP REACTOR <name> ON <table>`. sqlparser has no
    // `DROP REACTOR` AST node.
    if let Some(intent) = crate::reactor_ddl::match_drop_reactor(sql)? {
        crate::reactor_ddl::exec_drop_reactor(intent, &sess.tenant, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "DROP REACTOR".into(),
        });
    }

    // DROP MATERIALIZED VIEW [IF EXISTS] <name> — sqlparser's DROP parser
    // does not recognise MATERIALIZED VIEW, so we handle the full
    // statement before sqlparser sees it. DROP TABLE / DROP VIEW return
    // None and fall through to sqlparser's standard path.
    if let Some((name, if_exists)) = crate::cv_ddl::match_drop_materialized_view(sql)? {
        return crate::cv_ddl::exec_drop_materialized_view(sess, &name, if_exists).await;
    }

    // CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opt …] —
    // sqlparser 0.52 only parses one option per CREATE SEQUENCE
    // statement, so the full PG grammar (`START 100 INCREMENT 5 MINVALUE
    // 1 MAXVALUE 1000 CACHE 1 NO CYCLE`) fails at the second option.
    // The textual matcher claims any CREATE SEQUENCE shape; the
    // single-option AST-driven path remains as a fallback for SQL we
    // somehow miss here.
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
    let sql_owned = lifecycle_stripped;
    let sql = sql_owned.as_str();

    // INCLUDE DELETED on SELECT is the soft-delete opt-out.
    let (select_stripped, include_deleted) = extract_select_include_deleted(sql);
    let sql = select_stripped.as_str();

    // Auto-route `ORDER BY <vec_col> <op> <lit> LIMIT k` to the HNSW fast
    // path BEFORE the operator-to-UDF rewrite below. Once `<->` becomes
    // `l2_distance(...)` the structural signal is gone. A `None` here
    // means at least one criterion failed; the brute-force pipeline below
    // takes over and correctness is preserved.
    if let Some(plan) = crate::vector_planner::rewrite_vector_order_by(
        &sess.engine.config().catalog,
        &sess.tenant,
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
    let sql = sql.as_str();

    // Translate the pg_vector operator forms (`<->`, `<#>`, `<=>`) into the
    // matching UDF calls before handing the SQL to sqlparser. See
    // `udf::rewrite_vector_operators` for the strategy and its limits.
    let rewritten = crate::udf::rewrite_vector_operators(sql);
    // Route `EXTRACT(SECOND FROM <expr>)` to the Basin UDF that returns
    // Float64 with sub-second precision (PG's `extract(second ...)` shape).
    // Other EXTRACT fields fall through to DataFusion's `date_part`.
    let rewritten = crate::udf::rewrite_extract_second(&rewritten);
    // Rewrite PG aggregate name aliases that DataFusion exposes under a
    // different name: `variance(x)` → `var(x)`, `every(x)` → `bool_and(x)`.
    let rewritten = crate::udf::rewrite_pg_agg_aliases(&rewritten);
    // User-defined `LANGUAGE sql` function inlining. The rewriter is a
    // no-op for tenants with no registered functions and for statements
    // that contain no function calls at all (the cheap pre-gate runs
    // before any catalog hop). Anything else gets rewritten so DataFusion
    // sees the body inlined into the call site.
    let inlined = crate::sql_functions::rewrite_sql_inlining_functions(
        &sess.engine.config().catalog,
        &sess.tenant,
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
        tenant: sess.tenant,
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
        &sess.tenant,
        &seq_rewritten,
    )
    .await?;
    let sql = enum_rewritten.as_str();
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
                &sess.tenant,
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

    match stmt {
        Statement::CreateTable(ct) => exec_create_table(sess, ct, cluster_columns, lifecycle).await,
        Statement::CreateIndex(ci) => exec_create_index(sess, ci).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            if_exists,
            names,
            cascade: _,
            restrict: _,
            purge: _,
            temporary: _,
        } => exec_drop_index(sess, if_exists, names).await,
        Statement::CreateView {
            name,
            query,
            materialized,
            ..
        } => {
            if !materialized {
                return Err(BasinError::internal(
                    "CREATE VIEW (non-materialised) is not supported in v0.1; \
                     use CREATE MATERIALIZED VIEW ... WITH (basin.continuous, ...)",
                ));
            }
            let view_name = single_part_name(&name)?.to_string();
            let opts = cv_options.unwrap_or_default();
            if !opts.continuous {
                return Err(BasinError::InvalidSchema(
                    "CREATE MATERIALIZED VIEW requires WITH (basin.continuous, \
                     refresh_interval = '<duration>')"
                        .into(),
                ));
            }
            let interval = opts.refresh_interval_secs.ok_or_else(|| {
                BasinError::InvalidSchema(
                    "CREATE MATERIALIZED VIEW: WITH (basin.continuous) \
                     requires refresh_interval = '<duration>'"
                        .into(),
                )
            })?;
            let source_sql = query.to_string();
            crate::cv_ddl::exec_create_materialized_view(sess, &view_name, &source_sql, interval)
                .await
        }
        Statement::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            return_type,
            function_body,
            language,
            behavior: _,
            called_on_null: _,
            parallel: _,
            using: _,
            if_not_exists: _,
            determinism_specifier: _,
            options: _,
            remote_connection: _,
        } => {
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
        Statement::DropFunction {
            if_exists,
            func_desc,
            option: _,
        } => {
            let names = func_desc.into_iter().map(|d| d.name).collect();
            crate::function_ddl::exec_drop_function(sess, if_exists, names).await
        }
        Statement::DropProcedure {
            if_exists,
            proc_desc,
            option: _,
        } => crate::procedure_ddl::exec_drop_procedure(sess, if_exists, proc_desc).await,
        Statement::Call(call) => crate::procedure_ddl::exec_call(sess, call).await,
        Statement::CreateType {
            name,
            representation,
        } => {
            use sqlparser::ast::UserDefinedTypeRepresentation;
            match representation {
                UserDefinedTypeRepresentation::Enum { labels } => {
                    crate::type_ddl::exec_create_type_enum(sess, name, labels).await
                }
                UserDefinedTypeRepresentation::Composite { .. } => {
                    Err(BasinError::FeatureNotSupported(
                        "CREATE TYPE … AS (composite) is out of scope for v0.1; \
                         only AS ENUM is supported"
                            .into(),
                    ))
                }
            }
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Type,
            if_exists,
            names,
            cascade: _,
            restrict: _,
            purge: _,
            temporary: _,
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
            cascade: _,
            restrict: _,
            purge: _,
            temporary: _,
        } => crate::seq_ddl::exec_drop_sequence(sess, if_exists, &names).await,
        Statement::Insert(ins) => exec_insert(sess, ins).await,
        Statement::Query(_) => {
            // Analytical routing happens before the point-query fast path so
            // an explicit `/*+ analytical */` hint on a shape that the fast
            // path would otherwise grab still gets DuckDB. Aggregate /
            // GROUP-BY queries don't match the fast path's pattern, so for
            // those the order doesn't matter.
            if let Some(analytical) = sess.engine.analytical() {
                if is_analytical(&stmt, raw_sql) {
                    match analytical.query(&sess.tenant, raw_sql).await {
                        Ok(batches) => {
                            sess.engine.note_analytical_routed();
                            return Ok(rows_from_batches(batches));
                        }
                        Err(e) => {
                            // Fallback: DuckDB rejects a fraction of the
                            // dialect (e.g. some PG-isms, our vector UDFs).
                            // The caller wrote the same SQL the OLTP engine
                            // accepts; surface a hard error only if both
                            // engines fail. v0.3 may tighten this.
                            tracing::warn!(
                                error = %e,
                                "analytical path failed, falling back to OLTP engine"
                            );
                        }
                    }
                }
            }

            // Try the point-query fast path first. It only matches a tightly
            // constrained shape; on any rejection we fall back to DataFusion.
            //
            // RLS gate: the fast path bypasses DataFusion's logical planner
            // entirely, which is where we inject row-level predicates. If
            // any referenced table has `rls_enabled = true`, we *must* take
            // the DataFusion path so the RLS rewrite can fire. Tables with
            // RLS off see the fast path exactly as before — same one-`bool`
            // catalog read the existing path already pays.
            if let Some(plan) = match_simple_select(&stmt) {
                if !table_has_rls(sess, &plan.table).await?
                    && !table_has_soft_delete(sess, &plan.table).await?
                {
                    return execute_simple_select(sess, plan).await;
                }
            }
            exec_select(sess, sql, include_deleted).await
        }
        Statement::ShowTables { .. } => exec_show_tables(sess).await,
        Statement::AlterTable {
            name, operations, ..
        } => exec_alter_table(sess, name, operations).await,
        Statement::Delete(del) => crate::dml_mutate::exec_delete(sess, del).await,
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        } => {
            crate::dml_mutate::exec_update(sess, table, assignments, from, selection, returning)
                .await
        }
        other => Err(BasinError::internal(format!("unsupported in PoC: {other}"))),
    }
}

/// Wrap analytical-engine output in [`ExecResult::Rows`]. The schema comes
/// from the first batch; an empty result still needs *some* schema, so we
/// hand back an empty [`Schema`] in that case (the analytical engine doesn't
/// expose a typed empty-result API in v0.1).
fn rows_from_batches(batches: Vec<RecordBatch>) -> ExecResult {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => Arc::new(Schema::empty()),
    };
    ExecResult::Rows { schema, batches }
}

/// Execute a `VectorSearchPlan` produced by `vector_planner`. Calls
/// `Storage::vector_search` (via the existing `TenantSession::vector_search`
/// fast path) with `fetch_k` candidates, applies any column-equality
/// pushdown filters, truncates to the user's `LIMIT`, and projects to the
/// user's `SELECT` list.
///
/// Mirrors the result shape `exec_select` would produce for the same query
/// in brute-force mode: the projected user columns only, no synthetic
/// `_distance` column. (Brute-force computes the distance in `ORDER BY` but
/// only emits whatever the user wrote in `SELECT`.)
async fn execute_vector_search_plan(
    sess: &TenantSession,
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
        .load_table(&sess.tenant, &plan.table)
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
    // contract from `TenantSession::vector_search` keeps the loop trivial:
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
    sess: &TenantSession,
    mut ct: sqlparser::ast::CreateTable,
    cluster_columns: Option<Vec<String>>,
    lifecycle: CreateTableLifecycle,
) -> Result<ExecResult> {
    let name = single_part_name(&ct.name)?;
    let table = TableName::new(name)?;

    // Phase 5.11.K2: resolve column types that reference a user-defined
    // enum or domain. Each match rewrites the column's data_type to the
    // underlying Arrow-mappable shape and stamps a `BASIN_ENUM_TYPE` /
    // `BASIN_DOMAIN` field-metadata marker so the INSERT path can
    // validate values + the catalog can refcount.
    let bindings = crate::type_ddl::resolve_user_type_columns(
        &sess.engine.config().catalog,
        &sess.tenant,
        &ct.columns,
    )
    .await?;
    let extra_md = crate::type_ddl::rewrite_user_type_columns(&mut ct.columns, &bindings)?;

    let (schema, constraints) = crate::ddl::schema_and_constraints_from_columns(
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
    // table must exist in the same tenant, referenced columns must be
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
                .load_table(&sess.tenant, &ref_table_name)
                .await
                .map_err(|e| match e {
                    BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: referenced table {:?} does not exist in this tenant \
                         (cross-tenant FKs are not supported in v0.1)",
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
                    .load_table(&sess.tenant, &ref_table_name)
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

    sess.engine
        .config()
        .catalog
        .create_table(&sess.tenant, &table, &schema)
        .await?;

    // Register implicit sequences promised by `SERIAL` / `BIGSERIAL` /
    // `SMALLSERIAL` columns. PG would auto-create these inline with the
    // table; we do it as a follow-on catalog call so the table-create
    // path stays one focused step. `IF NOT EXISTS`-shaped: if the
    // sequence already exists (re-run after a partial failure) we
    // swallow the duplicate-name error so the table can keep going.
    for seq in &constraints.implicit_sequences {
        let def = basin_catalog::SequenceDef::with_defaults(sess.tenant, seq.name.clone());
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
            .set_partition_spec(&sess.tenant, &table, spec)
            .await?;
    }

    if let Some(cols) = cluster_columns {
        sess.engine
            .config()
            .catalog
            .set_cluster_columns(&sess.tenant, &table, cols)
            .await?;
    }

    if !constraints.pk_columns.is_empty()
        || !constraints.checks.is_empty()
        || !constraints.foreign_keys.is_empty()
    {
        sess.engine
            .config()
            .catalog
            .set_table_constraints(
                &sess.tenant,
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
            .set_unique_constraints(&sess.tenant, &table, constraints.uniques)
            .await?;
    }

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE".into(),
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
    sess: &TenantSession,
    ci: sqlparser::ast::CreateIndex,
) -> Result<ExecResult> {
    use sqlparser::ast::{Expr, OrderByExpr};
    // UNIQUE / CONCURRENTLY / INCLUDE / WHERE / WITH on the index are
    // out of scope for v0.1; reject explicitly so the user knows
    // their constraint isn't being silently enforced.
    if ci.unique {
        return Err(BasinError::FeatureNotSupported(
            "CREATE UNIQUE INDEX is not supported in v0.1; declare UNIQUE on the table \
             (`UNIQUE (col, ...)` at CREATE TABLE) for uniqueness enforcement"
                .into(),
        ));
    }
    if ci.concurrently {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX CONCURRENTLY is not supported in v0.1".into(),
        ));
    }
    if !ci.include.is_empty() {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX ... INCLUDE (...) is not supported in v0.1".into(),
        ));
    }
    if ci.predicate.is_some() {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX ... WHERE <predicate> (partial index) is not supported in v0.1".into(),
        ));
    }
    if !ci.with.is_empty() {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX ... WITH (...) is not supported in v0.1".into(),
        ));
    }

    let table_name = single_part_name(&ci.table_name)?;
    let table = TableName::new(table_name)?;

    // PG requires an index name; sqlparser accepts the omitted form
    // (anonymous indexes). Mint a deterministic synthetic name when
    // the user didn't write one: `<table>_<col1>_<col2>_idx`. The
    // catalog still rejects duplicates against existing indexes, so
    // anonymous CREATE INDEX twice in a row will collide unless
    // `IF NOT EXISTS` was specified.
    let index_name = match &ci.name {
        Some(n) => single_part_name(n)?.to_string(),
        None => {
            // Build a fallback from the column list; resolved below.
            String::new()
        }
    };

    // Pull bare identifier columns out of the OrderByExpr list. ASC /
    // DESC / NULLS FIRST / NULLS LAST are accepted (and ignored at this
    // stage — v0.1 storage is order-agnostic).
    let mut columns: Vec<String> = Vec::with_capacity(ci.columns.len());
    for ob in &ci.columns {
        let OrderByExpr { expr, .. } = ob;
        match expr {
            Expr::Identifier(ident) => columns.push(ident.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                columns.push(parts[0].value.clone())
            }
            other => {
                return Err(BasinError::FeatureNotSupported(format!(
                    "CREATE INDEX expression columns are not supported in v0.1: {other}"
                )));
            }
        }
    }
    if columns.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE INDEX: column list cannot be empty".into(),
        ));
    }

    let index_name = if index_name.is_empty() {
        format!("{}_{}_idx", table_name, columns.join("_"))
    } else {
        index_name
    };

    // Verify the table exists in this tenant before we touch the
    // catalog's create_index entry point (it would surface NotFound
    // otherwise; we'd rather a clear up-front error).
    sess.engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                "CREATE INDEX: table {table_name:?} does not exist in this tenant"
            )),
            other => other,
        })?;

    sess.engine
        .config()
        .catalog
        .create_index(
            &sess.tenant,
            &table,
            &index_name,
            &columns,
            ci.if_not_exists,
        )
        .await?;

    Ok(ExecResult::Empty {
        tag: "CREATE INDEX".into(),
    })
}

/// `DROP INDEX [IF EXISTS] <name>`. Removes the catalog row only —
/// there's nothing to physically tear down because v0.1 doesn't
/// materialise any index file.
async fn exec_drop_index(
    sess: &TenantSession,
    if_exists: bool,
    names: Vec<sqlparser::ast::ObjectName>,
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP INDEX requires at least one index name".into(),
        ));
    }
    for n in &names {
        let index_name = single_part_name(n)?;
        // The catalog stores indexes per-table; we don't track a
        // global (tenant, index-name) → table mapping. Scan every
        // table in the tenant for a matching declaration.
        let tables = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.tenant)
            .await?;
        let mut found = false;
        for t in &tables {
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.tenant, t)
                .await?;
            if meta.indexes.iter().any(|i| i.name == index_name) {
                sess.engine
                    .config()
                    .catalog
                    .drop_index(&sess.tenant, t, index_name)
                    .await?;
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

async fn exec_insert(sess: &TenantSession, ins: sqlparser::ast::Insert) -> Result<ExecResult> {
    let name = single_part_name(&ins.table_name)?;
    let table = TableName::new(name)?;

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
        .load_table(&sess.tenant, &table)
        .await?;
    let schema = meta.schema.clone();
    let row_count = rows_raw.len();

    // Reject direct writes to generated columns + expand `INSERT INTO t
    // (col_subset) VALUES ...` into full schema-width rows with NULL in
    // unmentioned columns. Generated columns are NULL'd here too;
    // `materialise_generated_columns` overwrites them once the per-row
    // batch is built.
    let mut rows_expanded = expand_insert_rows(schema.as_ref(), &ins.columns, rows_raw)?;
    // Substitute column-level DEFAULT expressions for omitted columns.
    // For columns with `BASIN_COLUMN_DEFAULT` metadata that the user did
    // not explicitly write, evaluate the default text (which routes any
    // `nextval(...)` calls through `Catalog::nextval` so each row gets a
    // distinct value) and overwrite the NULL placeholder produced by
    // `expand_insert_rows`. User-written NULL is preserved.
    apply_column_defaults(sess, schema.as_ref(), &ins.columns, &mut rows_expanded).await?;
    let rows: &[Vec<sqlparser::ast::Expr>] = &rows_expanded;

    // Partitioned path. We must compute each row's partition key from its
    // partition-column value before producing any RecordBatch — multi-row
    // INSERTs may span partitions and we issue one Parquet write per
    // resulting partition.
    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        let groups = group_rows_by_partition(schema.as_ref(), rows, &meta.partition_spec)?;

        // Shard path is intentionally disabled for partitioned tables in
        // v0.1 — the shard owner's WAL pre-supposes one partition key per
        // tenant slice and the multi-partition fan-out hasn't been wired
        // through compaction yet. Fall through to the synchronous Parquet
        // write path below.
        let opts = write_options_for(&meta);
        let mut materialised_groups: Vec<(PartitionKey, RecordBatch)> =
            Vec::with_capacity(groups.len());
        for (pkey, group_rows) in groups {
            let batch = batch_from_rows(schema.clone(), &group_rows)?;
            let batch = crate::generated_cols::materialise_generated_columns(
                &sess.engine.config().catalog,
                &sess.tenant,
                batch,
            )
            .await?;
            crate::type_ddl::enforce_enum_labels(
                &sess.engine.config().catalog,
                &sess.tenant,
                &batch,
            )
            .await?;
            crate::type_ddl::enforce_domain_checks(
                &sess.engine.config().catalog,
                &sess.tenant,
                &batch,
            )
            .await?;
            crate::constraints::enforce_check_constraints(
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
                &batch,
            )
            .await?;
            crate::constraints::enforce_fk_on_insert(
                &sess.engine.config().catalog,
                &sess.engine.config().storage,
                &sess.tenant,
                table.as_str(),
                &meta.foreign_keys,
                &batch,
            )
            .await?;
            crate::constraints::enforce_pk_on_insert(
                &sess.engine.config().storage,
                &sess.tenant,
                &table,
                table.as_str(),
                &meta.pk_columns,
                &batch,
            )
            .await?;
            crate::constraints::enforce_unique_on_insert(
                &sess.engine.config().storage,
                &sess.tenant,
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
                .write_batch_with_options(&sess.tenant, &table, pkey, batch, &opts)
                .await?;
            file_refs.push(DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
            });
        }

        commit_with_retry(sess, &table, meta.current_snapshot, file_refs).await?;
        dispatch_post_commit(&sess.engine, events);
        refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;
        write_insert_audit_rows(sess, meta.schema.as_ref(), &preview_batches).await?;
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    let batch = batch_from_rows(schema, rows)?;
    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.tenant,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.tenant, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.tenant, &batch)
        .await?;
    // PK / CHECK / FK enforcement. Order: CHECK (no I/O), then FK
    // (one referenced-table scan), then PK (one full-table scan).
    // v0.2 secondary indexes (Phase 5.7 B1) will collapse PK / FK
    // to point lookups; for v0.1 we accept the scan cost.
    crate::constraints::enforce_check_constraints(
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.tenant,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.tenant,
        &table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.tenant,
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
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        let handle = shard.get(&sess.tenant, &part).await?;
        handle.write_batch(&table, batch).await?;
        // SELECT-side handles tail-visibility (Option A: force-compact). Skip
        // the DataFusion ListingTable refresh here; reads will trigger it.
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // Legacy synchronous path (no shard configured).
    //
    // Order: write parquet → refresh listing table → dispatch pre-commit
    // → commit catalog. This lets pre-commit reactors (5.11.C2 constraint
    // reactors in particular) see the in-flight row when they evaluate
    // their predicates against the table. If a reactor rejects, we delete
    // the orphan parquet file and re-refresh so the failure is invisible
    // to subsequent queries.
    let events = build_insert_events(sess, &table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta);
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.tenant, &table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
    };

    // Refresh the listing table now so reactor-bodied SELECTs see the
    // new row. Then dispatch pre-commit; on error, roll back by deleting
    // the orphan file (the catalog snapshot is unchanged at this point,
    // so cleanup is just removing the orphan parquet).
    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.tenant, &df.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await;
        return Err(e);
    }

    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

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
    sess: &TenantSession,
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
        .load_table(&sess.tenant, table)
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
            columns.push(arrow_array::new_null_array(
                target_field.data_type(),
                row_count,
            ));
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("build INSERT-SELECT batch: {e}")))?;

    // Enum / domain check enforcement matches the VALUES path so
    // constraint violations surface identically regardless of source.
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.tenant, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.tenant, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.tenant,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.tenant,
        table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.tenant,
        table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
    )
    .await?;

    let part = PartitionKey::default_key();
    let events = build_insert_events(sess, table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta);
    let written = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.tenant, table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
    };

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, table).await?;
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.tenant, &written.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, table).await;
        return Err(e);
    }

    commit_with_retry(sess, table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

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
                    full.push(Expr::Value(Value::Null));
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
        let mut full: Vec<Expr> = vec![Expr::Value(Value::Null); n_cols];
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
    sess: &TenantSession,
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

/// AUDIT TO emission for INSERT. The mutation has already committed by
/// the time we get here; we materialise the after-row payloads from the
/// in-memory batches and append them to the configured audit table.
/// Tenant scoping is enforced by `lifecycle::write_audit_rows` resolving
/// the audit table within the calling session's tenant prefix.
async fn write_insert_audit_rows(
    sess: &TenantSession,
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
/// fresh per-`(tenant, table)` seq for each. Returns an empty vec when
/// no sinks are attached so callers pay only the registry-empty check.
fn build_insert_events(
    sess: &TenantSession,
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
            let seq = sess.engine.next_event_seq(&sess.tenant, table);
            out.push(make_event(
                &sess.tenant,
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
fn causation_user(sess: &TenantSession) -> Option<String> {
    if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.clone())
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
fn write_options_for(meta: &TableMetadata) -> WriteOptions {
    WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        cluster_columns: meta.cluster_columns.clone(),
    }
}

/// Optimistic commit with a single retry on conflict. A conflict is possible
/// only if some other writer raced us between `load_table` and
/// `append_data_files`; the in-memory catalog serializes per table so we
/// re-read and try once more before bubbling up.
async fn commit_with_retry(
    sess: &TenantSession,
    table: &TableName,
    expected_initial: basin_catalog::SnapshotId,
    files: Vec<DataFileRef>,
) -> Result<()> {
    let mut expected = expected_initial;
    match sess
        .engine
        .config()
        .catalog
        .append_data_files(&sess.tenant, table, expected, files.clone())
        .await
    {
        Ok(_) => Ok(()),
        Err(BasinError::CommitConflict(_)) => {
            let fresh = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.tenant, table)
                .await?;
            expected = fresh.current_snapshot;
            sess.engine
                .config()
                .catalog
                .append_data_files(&sess.tenant, table, expected, files)
                .await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn exec_select(sess: &TenantSession, sql: &str, include_deleted: bool) -> Result<ExecResult> {
    // Option A for tail-visibility: when the shard is wired in, the in-RAM
    // tail produced by INSERTs hasn't yet landed in Parquet. Force a synchronous
    // flush + catalog commit before planning so DataFusion's ListingTable scan
    // sees the just-written rows. After the flush we refresh every table this
    // session has touched so the cached `ListingTable` picks up the new data
    // file. This trades a small per-SELECT latency cost for keeping joins /
    // aggregations / projections on the existing planner without teaching them
    // about the tail.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
        let tables: Vec<_> = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.tenant)
            .await?;
        for table in &tables {
            crate::session::refresh_table(
                &sess.engine,
                &sess.tenant,
                &sess.ctx,
                &sess.state,
                table,
            )
            .await?;
        }
    }

    // Phase 5.5 partition pruning: if this session has seen a partitioned
    // table at least once, walk the SQL's AST and (if the WHERE clause
    // restricts the partition column) swap the registered `ListingTable`
    // for one whose paths are pre-filtered to matching partitions. The
    // atomic `has_partitioned_table` gate keeps the hot path fast for
    // tenants that never use PARTITION BY.
    if sess
        .state
        .has_partitioned_table
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        crate::session::apply_partition_pruning_for_query(
            &sess.engine,
            &sess.tenant,
            &sess.ctx,
            sql,
        )
        .await?;
    }

    let mut df = sess
        .ctx
        .sql(sql)
        .await
        .map_err(|e| BasinError::internal(format!("plan: {e}")))?;

    // Phase 5.6: row-level security. The per-tenant policy lookup is gated
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
    let ws_schema = Arc::new(schema_df_to_ws(df_schema.as_ref())?);

    // Change C: when the shard is wired in we know there are large per-tenant
    // tails on the same runtime. Move the DataFusion executor onto the
    // blocking thread pool so its parquet-decode loop can't pin the
    // cooperative tokio workers a quiet tenant's point queries run on. Tests
    // that run without a shard keep the single-await path and behave as
    // before.
    let df_batches = if sess.engine.config().shard.is_some() {
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| BasinError::internal(format!("create plan: {e}")))?;
        let task_ctx = sess.ctx.task_ctx();
        let join = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BasinError::internal(format!("blocking runtime: {e}")))?;
            rt.block_on(async {
                datafusion::physical_plan::collect(plan, task_ctx)
                    .await
                    .map_err(|e| BasinError::internal(format!("execute: {e}")))
            })
        })
        .await
        .map_err(|e| BasinError::internal(format!("spawn_blocking join: {e}")))?;
        join?
    } else {
        df.collect()
            .await
            .map_err(|e| BasinError::internal(format!("execute: {e}")))?
    };
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
    sess: &TenantSession,
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
        &sess.tenant,
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

/// One catalog `load_table` call. Returns `true` only when the table has
/// `rls_enabled = true`. We don't cache this on the session because policy
/// state can change mid-session (every `CREATE/ALTER/DROP POLICY` /
/// `ALTER TABLE … ENABLE/DISABLE ROW LEVEL SECURITY` writes through to the
/// catalog), and a per-query catalog hop is an O(microsecond) cost on the
/// SELECT path that's already doing one anyway.
async fn table_has_rls(sess: &TenantSession, table: &TableName) -> Result<bool> {
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, table)
        .await?;
    Ok(meta.rls_enabled)
}

/// Companion of [`table_has_rls`] for the soft-delete predicate-injection
/// gate. Tables without a SOFT DELETE column take the simple-select fast
/// path unchanged.
async fn table_has_soft_delete(sess: &TenantSession, table: &TableName) -> Result<bool> {
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, table)
        .await?;
    Ok(crate::types::soft_delete_column(meta.schema.as_ref()).is_some())
}

/// AND-merge an `<soft_delete_col> IS NULL` predicate into `df`'s logical
/// plan for every TableScan against a table that has a SOFT DELETE column.
/// Mirrors `apply_rls_to_select` — same TreeNode rewrite shape, different
/// predicate source. When `INCLUDE DELETED` was specified the caller skips
/// this step entirely.
async fn apply_soft_delete_to_select(
    sess: &TenantSession,
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
            .load_table(&sess.tenant, table)
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
                if let Ok(t) = TableName::new(name.0[0].value.clone()) {
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
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_from_expr(o, out);
            }
            for c in conditions {
                collect_from_expr(c, out);
            }
            for r in results {
                collect_from_expr(r, out);
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
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_) => {}
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
async fn exec_rls_ddl(sess: &TenantSession, ddl: crate::rls::RlsDdl) -> Result<ExecResult> {
    let table = ddl.table().clone();
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await?;
    let mut rls_enabled = meta.rls_enabled;
    let mut policies = meta.policies.clone();
    let tag = ddl.apply(&mut rls_enabled, &mut policies)?;
    sess.engine
        .config()
        .catalog
        .set_rls_state(&sess.tenant, &table, rls_enabled, policies)
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
    sess: &TenantSession,
    name: sqlparser::ast::ObjectName,
    operations: Vec<sqlparser::ast::AlterTableOperation>,
) -> Result<ExecResult> {
    let tag = crate::alter::apply_standard_alter_table(
        &sess.engine.config().catalog,
        &sess.tenant,
        &name,
        &operations,
    )
    .await?;

    // ADD COLUMN replaced the schema in the catalog; refresh the
    // session's DataFusion ListingTable so subsequent SELECTs see the
    // new column. We pull the (now possibly different) table name out
    // of the AST.
    if name.0.len() == 1 {
        if let Ok(t) = TableName::new(name.0[0].value.clone()) {
            refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &t).await?;
        }
    }
    Ok(ExecResult::Empty { tag: tag.into() })
}

async fn exec_show_tables(sess: &TenantSession) -> Result<ExecResult> {
    let tables = sess
        .engine
        .config()
        .catalog
        .list_tables(&sess.tenant)
        .await?;
    let names: Vec<&str> = tables.iter().map(|t| t.as_str()).collect();
    let arr = StringArray::from(names);
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

/// Pull a bare table name out of a sqlparser `ObjectName`. Schema-qualified
/// names are out of scope for the PoC.
fn single_part_name(name: &ObjectName) -> Result<&str> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified table names not supported in PoC: {name}"
        )));
    }
    Ok(&name.0[0].value)
}
