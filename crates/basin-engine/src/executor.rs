//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, TableMetadata};
use basin_common::{BasinError, PartitionKey, Result, TableName};
use basin_storage::WriteOptions;
use sqlparser::ast::{ObjectName, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::analytical_route::is_analytical;
use crate::convert::{batch_df_to_ws, schema_df_to_ws};
use crate::ddl::{partition_spec_from_ast, schema_from_columns};
use crate::dml::{batch_from_rows, group_rows_by_partition};
use crate::fast_select::{execute_simple_select, match_simple_select};
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
        let tag = ext.apply(&sess.engine.config().catalog, &sess.tenant).await?;
        crate::session::refresh_table(
            &sess.engine,
            &sess.tenant,
            &sess.ctx,
            &sess.state,
            &table,
        )
        .await?;
        return Ok(ExecResult::Empty { tag: tag.into() });
    }

    // Translate the pg_vector operator forms (`<->`, `<#>`, `<=>`) into the
    // matching UDF calls before handing the SQL to sqlparser. See
    // `udf::rewrite_vector_operators` for the strategy and its limits.
    let rewritten = crate::udf::rewrite_vector_operators(sql);
    let sql = rewritten.as_str();
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| BasinError::internal(format!("parse error: {e}")))?;

    if stmts.len() != 1 {
        return Err(BasinError::internal(format!(
            "expected exactly one statement, got {}",
            stmts.len()
        )));
    }
    let stmt = stmts.pop().unwrap();

    // Phase 5.6: intercept RLS-related DDL before the main dispatch. The
    // catch-all in `match_rls_ddl` keeps every other statement falling
    // through to the existing handlers — the no-RLS hot path is an
    // `Ok(None)` followed by the same dispatch as before.
    if let Some(rls_ddl) = crate::rls::match_rls_ddl(&stmt)? {
        return exec_rls_ddl(sess, rls_ddl).await;
    }

    match stmt {
        Statement::CreateTable(ct) => exec_create_table(sess, ct).await,
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
                if !table_has_rls(sess, &plan.table).await? {
                    return execute_simple_select(sess, plan).await;
                }
            }
            exec_select(sess, sql).await
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
            crate::dml_mutate::exec_update(
                sess,
                table,
                assignments,
                from,
                selection,
                returning,
            )
            .await
        }
        other => Err(BasinError::internal(format!(
            "unsupported in PoC: {other}"
        ))),
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

async fn exec_create_table(
    sess: &TenantSession,
    ct: sqlparser::ast::CreateTable,
) -> Result<ExecResult> {
    let name = single_part_name(&ct.name)?;
    let table = TableName::new(name)?;
    let schema = schema_from_columns(&ct.columns)?;
    let spec = partition_spec_from_ast(ct.partition_by.as_deref(), &schema)?;

    sess.engine
        .config()
        .catalog
        .create_table(&sess.tenant, &table, &schema)
        .await?;

    if spec.is_partitioned() {
        sess.engine
            .config()
            .catalog
            .set_partition_spec(&sess.tenant, &table, spec)
            .await?;
    }

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE".into(),
    })
}

async fn exec_insert(sess: &TenantSession, ins: sqlparser::ast::Insert) -> Result<ExecResult> {
    let name = single_part_name(&ins.table_name)?;
    let table = TableName::new(name)?;

    // Pull literal rows out of `INSERT ... VALUES (...)`. Subquery inserts
    // (`INSERT ... SELECT ...`) are deliberately rejected here for the PoC.
    let source = ins.source.as_ref().ok_or_else(|| {
        BasinError::internal("INSERT without VALUES is not supported in PoC")
    })?;
    let rows = match source.body.as_ref() {
        SetExpr::Values(v) => &v.rows,
        _ => {
            return Err(BasinError::internal(
                "only INSERT INTO ... VALUES (...) is supported in PoC",
            ));
        }
    };

    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &table)
        .await?;
    let schema = meta.schema.clone();
    let row_count = rows.len();

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
        let mut file_refs: Vec<DataFileRef> = Vec::with_capacity(groups.len());
        for (pkey, group_rows) in groups {
            let batch = batch_from_rows(schema.clone(), &group_rows)?;
            let df = sess
                .engine
                .config()
                .storage
                .write_batch_with_options(&sess.tenant, &table, &pkey, &batch, &opts)
                .await?;
            file_refs.push(DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
            });
        }

        commit_with_retry(sess, &table, meta.current_snapshot, file_refs).await?;
        refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    let batch = batch_from_rows(schema, rows)?;
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
    };

    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
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

async fn exec_select(sess: &TenantSession, sql: &str) -> Result<ExecResult> {
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

fn collect_table_refs_from_query(query: &sqlparser::ast::Query) -> Vec<TableName> {
    use sqlparser::ast::{SetExpr, TableFactor};
    let mut out = Vec::new();
    if let SetExpr::Select(select) = query.body.as_ref() {
        for from in &select.from {
            if let TableFactor::Table { name, .. } = &from.relation {
                if name.0.len() == 1 {
                    if let Ok(t) = TableName::new(name.0[0].value.clone()) {
                        out.push(t);
                    }
                }
            }
        }
    }
    out
}

/// Apply an RLS DDL statement to the catalog. The mutation reads the current
/// `(rls_enabled, policies)`, applies the change in memory, and writes back
/// via `Catalog::set_rls_state`. We do not refresh the DataFusion ListingTable
/// here — RLS state is consulted at SELECT time by re-reading the catalog
/// (per-query) so a freshly created policy takes effect on the very next
/// query without per-session bookkeeping.
async fn exec_rls_ddl(
    sess: &TenantSession,
    ddl: crate::rls::RlsDdl,
) -> Result<ExecResult> {
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
