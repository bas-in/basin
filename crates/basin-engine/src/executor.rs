//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use std::sync::Arc;
use crate::pg_ast::ObjectNamePartExt;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, TableMetadata};
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName};
use basin_storage::WriteOptions;
use sqlparser::ast::{
    AssignmentTarget, ConflictTarget, ObjectName, OnConflictAction, OnInsert, SetExpr, Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

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
use crate::{ExecResult, ProjectSession};
use basin_catalog::PartitionSpec;

pub(crate) async fn execute(sess: &ProjectSession, sql: &str) -> Result<ExecResult> {
    // Keep a reference to the SQL the user actually wrote. The rewriter
    // below mangles vector operators into UDF calls; that rewrite is
    // irrelevant to (and would only confuse) the analytical engine, which
    // doesn't know our UDFs.
    let raw_sql = sql;

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
        let kinds: Vec<_> = tree
            .stmts()
            .map(|n| crate::pg_ast::stmt_kind(n))
            .collect();

        // Reject LISTEN/NOTIFY/UNLISTEN — not on the roadmap (ADR 0012 / pub/sub).
        for kind in &kinds {
            use crate::pg_ast::StmtKind;
            if matches!(kind, StmtKind::Listen | StmtKind::Notify) {
                return Err(basin_common::BasinError::FeatureNotSupported(format!(
                    "{} is not supported (SQLSTATE 0A000)",
                    kind.as_label()
                )));
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

            if let Some(result) =
                crate::noop_accept::try_accept_as_noop(kind, sql)
            {
                return Ok(result);
            }
        }
        crate::pg_ast::reject_unsupported(&tree)?;
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

    // DROP MATERIALIZED VIEW [IF EXISTS] <name> — sqlparser's DROP parser
    // does not recognise MATERIALIZED VIEW, so we handle the full
    // statement before sqlparser sees it.
    if let Some((name, if_exists)) = crate::cv_ddl::match_drop_materialized_view(sql)? {
        return crate::cv_ddl::exec_drop_materialized_view(sess, &name, if_exists).await;
    }

    // ALTER SEQUENCE [IF EXISTS] <name> [RESTART [WITH n]]: sqlparser 0.52
    // has no AlterSequence AST node; textual pre-screen handles the full
    // PG grammar.
    if let Some(intent) = crate::seq_ddl::match_alter_sequence(sql)? {
        return crate::seq_ddl::exec_alter_sequence(sess, intent).await;
    }

    // `ALTER TABLE t ALTER COLUMN col SET GENERATED ALWAYS/BY DEFAULT` and
    // `ALTER TABLE t ALTER COLUMN col DROP IDENTITY` — sqlparser 0.52 does not
    // parse these PG-specific identity-sequence manipulation forms.  We accept
    // them as metadata-only no-ops (same policy as SET NOT NULL / SET DEFAULT).
    if match_alter_column_identity(sql) {
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
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
    let sql_owned = lifecycle_stripped;
    let sql = sql_owned.as_str();

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
    // 2. Strip `TABLESAMPLE BERNOULLI(N)` / `SYSTEM(N)`.
    //    sqlparser 0.52 has the keyword but no grammar production for it, so
    //    the parse fails.  We strip the clause (best-effort: DataFusion returns
    //    all rows un-sampled) so the query reaches DataFusion at all.
    let tablesample_rewrite = crate::select_advanced::strip_tablesample(sql);
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
    let sql = sql.as_str();

    // Translate the pg_vector operator forms (`<->`, `<#>`, `<=>`) into the
    // matching UDF calls before handing the SQL to sqlparser. See
    // `udf::rewrite_vector_operators` for the strategy and its limits.
    // Lower `tsvector @@ tsquery` to the `ts_match(...)` UDF before sqlparser
    // sees the SQL — sqlparser doesn't have AtAt in its operator table.
    let rewritten = crate::pg_ast::rewrite_tsvector_at_at(sql);
    let rewritten = crate::udf::rewrite_vector_operators(&rewritten);
    // Rewrite JSON/JSONB infix operators (`->`, `->>`, `#>`, `#>>`, `?`,
    // `?&`, `?|`, `<@`, `@>` for JSON, `||` for JSON concat, `@?` for
    // jsonpath exists) to UDF calls that DataFusion can evaluate.
    let rewritten = crate::udf::rewrite_json_operators(&rewritten);
    // Rewrite PostgreSQL POSIX regex operators (`~`, `!~`, `~*`, `!~*`) to
    // `regexp_like(…)` calls DataFusion accepts; expand `BETWEEN SYMMETRIC`;
    // rewrite array containment / overlap operators (`@>`, `<@`, `&&`) for
    // array-typed operands. See `pg_operators` for the full operator table.
    let rewritten = crate::pg_operators::rewrite_posix_regex_operators(&rewritten);
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
    // Rewrite `(s1, e1) OVERLAPS (s2, e2)` → `overlaps(s1, e1, s2, e2)`.
    let rewritten = crate::pg_operators::rewrite_overlaps(&rewritten);
    // Rewrite `agg(x) FILTER (WHERE cond)` → `agg(CASE WHEN cond THEN x END)`.
    let rewritten = crate::pg_operators::rewrite_aggregate_filter(&rewritten);
    // Strip `[NOT] MATERIALIZED` hint from `WITH cte AS [NOT] MATERIALIZED (…)`.
    let rewritten = crate::pg_operators::rewrite_cte_materialized(&rewritten);
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

    // ADR 0014 Phase 1 — noop-accept intercept + explicit-reject gate.
    //
    // Parse with libpg_query and intercept any statement in the syntactic-
    // accept set *before* sqlparser sees the SQL. sqlparser's
    // PostgreSqlDialect cannot parse many PG-native forms (`BEGIN`,
    // `SAVEPOINT`, `PREPARE`, `CREATE TRIGGER`, etc.) so this intercept
    // must run before the `Parser::parse_sql` call below. Any kind not in
    // the noop set falls through to sqlparser as before.
    //
    // The explicit-reject gate (is_unsupported) fires here so that kinds
    // like LISTEN / NOTIFY / UNLISTEN get a clean SQLSTATE 0A000 error
    // instead of a confusing sqlparser parse-error (sqlparser's
    // PostgreSqlDialect can't parse those forms either, and its error
    // messages don't mention 0A000).
    //
    // We use `raw_sql` (the unmodified statement the caller provided)
    // rather than `sql` (which has been through Basin's pre-screen
    // pipeline) because statements in the noop / reject sets are simple
    // PG-native forms that are not rewritten by any pre-screen above.
    // Reuse `raw_pg_tree` from the Phase 1 parse above — same SQL, no
    // need to call the C library parser a second time.
    if let Some(ref tree) = raw_pg_tree {
        if let Some(node) = tree.stmts().next() {
            let kind = crate::pg_ast::stmt_kind(node);
            // Noop-accept: return an empty ok result without reaching sqlparser.
            if let Some(result) = crate::noop_accept::try_accept_as_noop(kind, raw_sql) {
                return Ok(result);
            }
            // Explicit-reject: LISTEN / NOTIFY / UNLISTEN are permanent
            // non-goals per ADR 0012; surface 0A000 before sqlparser fails
            // with a confusing parse error.
            if kind.is_unsupported() {
                return Err(BasinError::FeatureNotSupported(format!(
                    "{} is not supported (SQLSTATE 0A000)",
                    kind.as_label()
                )));
            }
        }
    }

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

    match stmt {
        Statement::CreateTable(ct) => exec_create_table(sess, ct, cluster_columns, lifecycle).await,
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
            variable,
            values,
            ..
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
            } else {
                // Silently accept unknown SET variables.
                Ok(ExecResult::Empty { tag: "SET".into() })
            }
        }
        Statement::Insert(ins) => exec_insert(sess, ins).await,
        Statement::Query(_) => {
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
                    if !is_view && !has_rls && !has_soft_delete {
                        return execute_simple_select(sess, plan, table_meta).await;
                    }
                }
            }
            exec_select(sess, sql, include_deleted).await
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
        Statement::Fetch { name, direction, .. } => exec_fetch(sess, &name.value, direction).await,
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
    }
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
        &sess.project,
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

    // If IF NOT EXISTS is set and the table already exists, return success
    // (no-op). The catalog signals "already exists" as BasinError::Catalog;
    // we only suppress that specific variant — unrelated catalog errors still
    // propagate. Without IF NOT EXISTS the error is always fatal.
    match sess
        .engine
        .config()
        .catalog
        .create_table(&sess.project, &table, &schema)
        .await
    {
        Ok(_metadata) => {}
        Err(BasinError::Catalog(_)) if ct.if_not_exists => {
            // Table already exists and IF NOT EXISTS was specified — PG
            // behavior: succeed silently (no error, no row change).
            return Ok(ExecResult::Empty {
                tag: "CREATE TABLE".into(),
            });
        }
        Err(e) => return Err(e),
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

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE".into(),
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
        log_partial_index_notice, log_unique_notice, log_using_notice, parse_index_columns,
        IndexColumn,
    };

    // CONCURRENTLY is still unsupported — reject explicitly.
    if ci.concurrently {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX CONCURRENTLY is not supported in v0.1".into(),
        ));
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
                .map(|s| s.trim_start_matches("expr:").replace(['(', ')', ' ', ','], "_"))
                .collect::<Vec<_>>()
                .join("_");
            format!("{table_name}_{col_part}_idx")
        }
    };

    // Emit notices for accepted-but-not-enforced features.
    if ci.unique {
        log_unique_notice(&index_name);
    }
    if let Some(pred) = &ci.predicate {
        log_partial_index_notice(&index_name, &pred.to_string());
    }
    if let Some(method) = &ci.using {
        let m = method.to_string();
        if !matches!(m.to_lowercase().as_str(), "btree") {
            log_using_notice(&index_name, &m);
        }
    }
    if !ci.include.is_empty() {
        let include_cols: Vec<String> = ci
            .include
            .iter()
            .map(|ident| ident.value.clone())
            .collect();
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

    sess.engine
        .config()
        .catalog
        .create_index(
            &sess.project,
            &table,
            &index_name,
            &catalog_columns,
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
            if let Some(result) =
                try_on_conflict_do_update(sess, &table, &ins, on_conflict).await?
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
    let row_count = rows_raw.len();

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
        let opts = write_options_for(&meta);
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
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
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
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
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
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        let handle = shard.get(&sess.project, &part).await?;
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
        .write_batch_with_options(&sess.project, &table, &part, &batch, &opts)
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
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
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

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

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
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
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

    let opts = write_options_for(&meta);
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
    };

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
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
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
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
    let opts = write_options_for(&meta);

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
    };

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    dispatch_pre_commit(&sess.engine, &events).await?;
    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);
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
        Some(ConflictTarget::Columns(idents)) => {
            idents.iter().map(|i| i.value.clone()).collect()
        }
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
    let check_sql = format!(
        "SELECT 1 FROM {} WHERE {}",
        table.as_str(),
        where_clause
    );
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

    // Conflict found. Build and execute an UPDATE.
    let set_parts: Vec<String> = do_update
        .assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                AssignmentTarget::ColumnName(n) => n
                    .0
                    .last()
                    .map(|i| i.id_val().clone())
                    .unwrap_or_default(),
                AssignmentTarget::Tuple(_) => String::new(),
            };
            format!("{col} = {}", a.value)
        })
        .collect();
    if set_parts.iter().any(|s| s.starts_with(" =")) {
        return Err(BasinError::InvalidSchema(
            "ON CONFLICT DO UPDATE: malformed assignment".into(),
        ));
    }
    let update_sql = format!(
        "UPDATE {} SET {} WHERE {}",
        table.as_str(),
        set_parts.join(", "),
        where_clause
    );
    let result = Box::pin(sess.execute(&update_sql)).await?;
    Ok(Some(result))
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
            sess.state.sequence_cache.record(sess.project, seq_name, next).await;
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

async fn exec_select(sess: &ProjectSession, sql: &str, include_deleted: bool) -> Result<ExecResult> {
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
        for table in &tables {
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

    // View-reference rewriting: replace any reference to a known plain view
    // in the SQL's FROM / JOIN clauses with an inline subquery so DataFusion
    // sees a derived table rather than an unknown table name. This is a
    // no-op when the project has no registered views.
    let view_rewritten_owned;
    let sql = if let Some(rewritten) =
        crate::view_ddl::rewrite_view_refs(sess.engine.config().catalog.as_ref(), &sess.project, sql)
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
    let sql_stripped = crate::schema_ddl::strip_schema_qualifiers_for_session(
        sql,
        &sess.state.schema_state,
    );
    let sql_for_df = sql_stripped.as_str();

    let mut df = sess
        .ctx
        .sql(sql_for_df)
        .await
        .map_err(|e| BasinError::internal(format!("plan: {e}")))?;

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
    let ws_schema = Arc::new(schema_df_to_ws(df_schema.as_ref())?);

    // Change C: when the shard is wired in we know there are large per-project
    // tails on the same runtime. Move the DataFusion executor onto the
    // blocking thread pool so its parquet-decode loop can't pin the
    // cooperative tokio workers a quiet project's point queries run on. Tests
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
    let tag = crate::alter::apply_standard_alter_table(
        &sess.engine.config().catalog,
        &sess.project,
        &name,
        &operations,
    )
    .await?;

    // ADD COLUMN replaced the schema in the catalog; refresh the
    // session's DataFusion ListingTable so subsequent SELECTs see the
    // new column. We pull the (now possibly different) table name out
    // of the AST.
    if name.0.len() == 1 {
        if let Ok(t) = TableName::new(name.0[0].id_val().clone()) {
            refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &t).await?;
        }
    }
    Ok(ExecResult::Empty { tag: tag.into() })
}

async fn exec_show_tables(sess: &ProjectSession) -> Result<ExecResult> {
    let tables = sess
        .engine
        .config()
        .catalog
        .list_tables(&sess.project)
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
        let result = exec_select(sess, &select_sql, false).await?;
        // An empty result (0 rows) is valid — declare with the schema.
        let (schema, batches) = match result {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { .. } => (Arc::new(Schema::empty()), vec![]),
        };
        sess.state
            .cursors
            .declare(name, schema, batches)
            .await?;
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
    let (schema, batches) = sess
        .state
        .cursors
        .apply(cursor_name, dir, true)
        .await?;
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
    Ok(ExecResult::Empty {
        tag: "MOVE".into(),
    })
}

/// Return `true` if `sql` is one of the PG-specific identity-sequence forms
/// that sqlparser 0.52 cannot parse:
///
/// - `ALTER TABLE t ALTER COLUMN col SET GENERATED ALWAYS`
/// - `ALTER TABLE t ALTER COLUMN col SET GENERATED BY DEFAULT`
/// - `ALTER TABLE t ALTER COLUMN col DROP IDENTITY [IF EXISTS]`
///
/// These are accepted as metadata-only no-ops (same policy as SET NOT NULL).
fn match_alter_column_identity(sql: &str) -> bool {
    // Normalise: collapse whitespace, upper-case, trim trailing semicolon.
    let norm: String = sql
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let upper = norm.trim_end_matches(';').trim().to_ascii_uppercase();

    // Must start with ALTER TABLE … ALTER COLUMN …
    if !upper.starts_with("ALTER TABLE ") {
        return false;
    }
    // Look for SET GENERATED or DROP IDENTITY anywhere after ALTER COLUMN.
    let has_alter_column = {
        let uc = upper.as_str();
        uc.contains(" ALTER COLUMN ") || uc.contains(" ALTER ")
    };
    if !has_alter_column {
        return false;
    }
    upper.contains(" SET GENERATED ALWAYS")
        || upper.contains(" SET GENERATED BY DEFAULT")
        || upper.contains(" DROP IDENTITY")
}
