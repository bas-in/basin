//! EXPLAIN statement routing — DataFusion plan text back to the client.
//!
//! # PG wire shape
//! PostgreSQL's `EXPLAIN` returns a single-column result set whose column is
//! named `QUERY PLAN`. Each row holds one line of the plan text. We match
//! that exactly so `psql`, `pgAdmin`, and ORMs that parse EXPLAIN output
//! work without modification.
//!
//! # Variants handled
//! - `EXPLAIN <stmt>` — logical plan, text format, non-verbose
//! - `EXPLAIN ANALYZE <stmt>` — executes the query and adds timing
//! - `EXPLAIN VERBOSE <stmt>` — verbose logical plan
//! - `EXPLAIN (ANALYZE) <stmt>` / `EXPLAIN (VERBOSE) <stmt>` — PG options form
//! - `EXPLAIN (FORMAT JSON) <stmt>` — JSON output (DataFusion's JSON formatter)
//! - `EXPLAIN (ANALYZE, VERBOSE) <stmt>` — both flags via PG options

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{BasinError, Result};
use sqlparser::ast::{AnalyzeFormat, Statement};

use crate::{ExecResult, TenantSession};

/// Dispatched from `executor::execute` when the parsed statement is
/// `Statement::Explain { … }`. Re-runs the inner SQL through DataFusion's
/// planner, wraps it in an `Explain` logical plan, collects the result, and
/// reformats to a PG-shaped `QUERY PLAN` column.
pub(crate) async fn exec_explain(
    sess: &TenantSession,
    analyze: bool,
    verbose: bool,
    format: Option<AnalyzeFormat>,
    options: Option<Vec<sqlparser::ast::UtilityOption>>,
    inner_stmt: Box<Statement>,
) -> Result<ExecResult> {
    // Resolve flags from either the legacy positional fields or the PG-style
    // `EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)` options list. Options take
    // precedence over positional flags when both are present.
    let (mut do_analyze, mut do_verbose, mut do_json) = (analyze, verbose, false);

    if let Some(opts) = options {
        for opt in &opts {
            let name = opt.name.value.to_uppercase();
            match name.as_str() {
                "ANALYZE" => do_analyze = true,
                "VERBOSE" => do_verbose = true,
                "FORMAT" => {
                    // arg is an Ident or Value whose string value is the format name
                    if let Some(arg) = &opt.arg {
                        let s = arg.to_string().to_uppercase();
                        let s = s.trim_matches('\'').trim_matches('"');
                        if s == "JSON" {
                            do_json = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // The `format` positional field (e.g. `EXPLAIN FORMAT JSON`) overrides.
    if matches!(format, Some(AnalyzeFormat::JSON)) {
        do_json = true;
    }

    // Re-render the inner statement to a SQL string so DataFusion's
    // `ctx.sql(...)` can parse and plan it.
    let inner_sql = inner_stmt.to_string();

    // Plan the inner SQL. We accept DataFusion planning errors as user errors
    // (invalid inner query) rather than internal errors.
    let df = sess
        .ctx
        .sql(&inner_sql)
        .await
        .map_err(|e| BasinError::internal(format!("EXPLAIN: could not plan inner query: {e}")))?;

    // Attach the Explain node. DataFusion's `.explain()` returns a new
    // DataFrame whose logical plan is `LogicalPlan::Explain { … }`.
    // When `do_analyze = true` it executes the underlying query and gathers
    // runtime statistics.
    let explain_df = df
        .explain(do_verbose, do_analyze)
        .map_err(|e| BasinError::internal(format!("EXPLAIN: {e}")))?;

    // Collect the plan batches. Schema is:
    //   plan_type Utf8 | plan Utf8
    // Each row is one "stringified" plan step (initial plan, optimised plan,
    // physical plan, …). We concatenate every `plan` value into a flat list
    // of lines for the PG-shaped output.
    let batches = explain_df
        .collect()
        .await
        .map_err(|e| BasinError::internal(format!("EXPLAIN: collect: {e}")))?;

    // DataFusion's `collect()` returns Arrow v53 `RecordBatch` (DataFusion's
    // bundled version). Our build helpers use the workspace Arrow v54 type.
    // `batch_df_to_ws` does the cross-version translation, same as the rest
    // of the engine boundary.
    let ws_batches: Vec<arrow_array::RecordBatch> = batches
        .iter()
        .map(|b| crate::convert::batch_df_to_ws(b))
        .collect::<Result<Vec<_>>>()?;

    if do_json {
        build_json_result(ws_batches)
    } else {
        build_text_result(ws_batches)
    }
}

/// Format the plan batches as PG plain-text `QUERY PLAN` rows.
///
/// DataFusion produces multiple rows (one per plan stringification stage).
/// We emit each row on its own line, prepending the `plan_type` as a header
/// so the caller can tell which stage each block belongs to — mirrors what
/// `psql` shows when DataFusion's own `EXPLAIN` SQL is routed through the
/// DataFusion wire server.
fn build_text_result(batches: Vec<RecordBatch>) -> Result<ExecResult> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "QUERY PLAN",
        DataType::Utf8,
        false,
    )]));

    let mut lines: Vec<String> = Vec::new();

    for batch in &batches {
        // DataFusion's explain schema: col 0 = plan_type, col 1 = plan
        if batch.num_columns() < 2 {
            continue;
        }
        let plan_type_col = batch.column(0);
        let plan_col = batch.column(1);

        let plan_type_arr = plan_type_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BasinError::internal("EXPLAIN: plan_type column is not Utf8"))?;
        let plan_arr = plan_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BasinError::internal("EXPLAIN: plan column is not Utf8"))?;

        for i in 0..batch.num_rows() {
            let pt = plan_type_arr.value(i);
            let p = plan_arr.value(i);
            // Emit a blank separator before each new stage (except the first).
            if !lines.is_empty() {
                lines.push(String::new());
            }
            lines.push(format!("[{pt}]"));
            for line in p.lines() {
                lines.push(line.to_string());
            }
        }
    }

    // If DataFusion returned nothing, emit a placeholder so the client still
    // receives a CommandComplete (RowDescription + zero DataRows isn't wrong,
    // but an empty plan text is more useful for debugging).
    if lines.is_empty() {
        lines.push("(no plan)".to_string());
    }

    let arr: ArrayRef = Arc::new(StringArray::from(lines));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])
        .map_err(|e| BasinError::internal(format!("EXPLAIN: build batch: {e}")))?;

    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

/// Format the plan batches as a single JSON `QUERY PLAN` row.
///
/// We emit one row whose value is a JSON array of `{plan_type, plan}` objects,
/// matching the general shape of `EXPLAIN (FORMAT JSON)` in PostgreSQL
/// (which returns a single cell containing the JSON).
fn build_json_result(batches: Vec<RecordBatch>) -> Result<ExecResult> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "QUERY PLAN",
        DataType::Utf8,
        false,
    )]));

    let mut entries: Vec<serde_json::Value> = Vec::new();

    for batch in &batches {
        if batch.num_columns() < 2 {
            continue;
        }
        let plan_type_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BasinError::internal("EXPLAIN: plan_type column is not Utf8"))?;
        let plan_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| BasinError::internal("EXPLAIN: plan column is not Utf8"))?;

        for i in 0..batch.num_rows() {
            entries.push(serde_json::json!({
                "plan_type": plan_type_arr.value(i),
                "plan": plan_arr.value(i),
            }));
        }
    }

    let json_str = serde_json::to_string_pretty(&entries)
        .map_err(|e| BasinError::internal(format!("EXPLAIN JSON: serialize: {e}")))?;

    let arr: ArrayRef = Arc::new(StringArray::from(vec![json_str]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])
        .map_err(|e| BasinError::internal(format!("EXPLAIN JSON: build batch: {e}")))?;

    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}
