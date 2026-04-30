//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::DataFileRef;
use basin_common::{BasinError, PartitionKey, Result, TableName};
use sqlparser::ast::{ObjectName, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::convert::{batch_df_to_ws, schema_df_to_ws};
use crate::ddl::schema_from_columns;
use crate::dml::batch_from_rows;
use crate::session::refresh_table;
use crate::{ExecResult, TenantSession};

pub(crate) async fn execute(sess: &TenantSession, sql: &str) -> Result<ExecResult> {
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

    match stmt {
        Statement::CreateTable(ct) => exec_create_table(sess, ct).await,
        Statement::Insert(ins) => exec_insert(sess, ins).await,
        Statement::Query(_) => exec_select(sess, sql).await,
        Statement::ShowTables { .. } => exec_show_tables(sess).await,
        other => Err(BasinError::internal(format!(
            "unsupported in PoC: {other}"
        ))),
    }
}

async fn exec_create_table(
    sess: &TenantSession,
    ct: sqlparser::ast::CreateTable,
) -> Result<ExecResult> {
    let name = single_part_name(&ct.name)?;
    let table = TableName::new(name)?;
    let schema = schema_from_columns(&ct.columns)?;

    sess.engine
        .config()
        .catalog
        .create_table(&sess.tenant, &table, &schema)
        .await?;

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

    let batch = batch_from_rows(schema, rows)?;
    let row_count = batch.num_rows();
    let part = PartitionKey::default_key();

    let df = sess
        .engine
        .config()
        .storage
        .write_batch(&sess.tenant, &table, &part, &batch)
        .await?;

    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
    };

    // Optimistic commit with a single retry on conflict. A conflict here is
    // possible only if some other writer raced us between `load_table` and
    // `append_data_files`; the in-memory catalog serializes per table so we
    // re-read and try once more before bubbling up.
    let mut expected = meta.current_snapshot;
    match sess
        .engine
        .config()
        .catalog
        .append_data_files(&sess.tenant, &table, expected, vec![file_ref.clone()])
        .await
    {
        Ok(_) => {}
        Err(BasinError::CommitConflict(_)) => {
            let fresh = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.tenant, &table)
                .await?;
            expected = fresh.current_snapshot;
            sess.engine
                .config()
                .catalog
                .append_data_files(&sess.tenant, &table, expected, vec![file_ref])
                .await?;
        }
        Err(e) => return Err(e),
    }

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

async fn exec_select(sess: &TenantSession, sql: &str) -> Result<ExecResult> {
    let df = sess
        .ctx
        .sql(sql)
        .await
        .map_err(|e| BasinError::internal(format!("plan: {e}")))?;
    // Snapshot the schema before consuming the DataFrame so we can return it
    // even if the result set is empty. Convert DataFusion's arrow-53 schema
    // into the workspace's arrow-54 schema (the public engine API speaks the
    // workspace version).
    let df_schema = df.schema().inner().clone();
    let ws_schema = Arc::new(schema_df_to_ws(df_schema.as_ref())?);
    let df_batches = df
        .collect()
        .await
        .map_err(|e| BasinError::internal(format!("execute: {e}")))?;
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(df_batches.len());
    for b in df_batches.iter() {
        batches.push(batch_df_to_ws(b)?);
    }
    Ok(ExecResult::Rows {
        schema: ws_schema,
        batches,
    })
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
