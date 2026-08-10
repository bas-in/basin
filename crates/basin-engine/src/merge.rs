//! `MERGE INTO` (Postgres 15+) execution.
//!
//! `MERGE INTO target USING source ON cond
//!    WHEN MATCHED [AND p] THEN UPDATE SET … / DELETE / DO NOTHING
//!    WHEN NOT MATCHED [AND p] THEN INSERT (cols) VALUES (…) / DO NOTHING`
//!
//! ## Design — reuse, do not reimplement
//!
//! Row mutation is **not** reimplemented here. MERGE compiles each per-row
//! action into ordinary `UPDATE` / `DELETE` / `INSERT` text SQL and drives it
//! through [`ProjectSession::execute`], exactly the way `exec_update_from` and
//! `exec_delete_via_df_rowset` (in `dml_mutate.rs`) drive their re-issued DML.
//! Routing through the normal statement pipeline means MERGE inherits, for
//! free and without duplication:
//!   * **RLS** — `INSERT` runs `rls::enforce_with_check`; `UPDATE` enforces
//!     WITH CHECK on the post-SET image; `DELETE` folds the USING predicate
//!     into its WHERE. No action can bypass a policy because every action is a
//!     real statement.
//!   * **Constraints / FKs / soft-delete / audit / secondary indexes** — all
//!     fire from inside the reused per-action paths.
//!   * **The hot-tier fast paths** — pk-keyed UPDATE/DELETE and bulk INSERT.
//!   * **Atomicity** — see below.
//!
//! ## Match computation (one DataFusion join)
//!
//! A single SELECT joins source against target and, per source row, projects:
//!   * the target PRIMARY KEY columns (NULL ⇒ this source row matched no
//!     target row), used both as the per-action row identity and for
//!     duplicate-match detection;
//!   * one boolean column per WHEN clause: the clause's optional `AND`
//!     predicate (or literal TRUE when absent), evaluated by DataFusion with
//!     both source and target columns in scope;
//!   * the fully-evaluated per-row action payload — the UPDATE SET right-hand
//!     sides and the NOT-MATCHED INSERT value list — also projected as columns
//!     so DataFusion does the expression evaluation and Rust only renders the
//!     resulting scalars back as literals into the generated statement.
//!
//! DataFusion's `target LEFT JOIN source` is reversed here to
//! `source LEFT JOIN target` so unmatched **source** rows survive (they feed
//! the NOT MATCHED clauses); matched rows carry a non-NULL target PK.
//!
//! ## WHEN-clause evaluation order (Postgres semantics)
//!
//! For each source row the clauses are scanned in textual order; the **first**
//! clause whose kind matches the row's matched/unmatched state AND whose
//! predicate is true wins, and only that clause's action runs. A row matching
//! no clause does nothing. This is "first-match-wins", per PG.
//!
//! ## Duplicate-match → SQLSTATE 21000
//!
//! PG requires every target row be touched at most once: if the join matches a
//! single target PK from two or more source rows, PG raises 21000
//! (`cardinality_violation`, "MERGE command cannot affect row a second time").
//! We detect it on the materialised match set before any mutation runs.
//!
//! ## Atomicity
//!
//! All generated actions execute inside one explicit `BEGIN … COMMIT`. The
//! transaction-control interceptor in `executor.rs` snapshots catalog heads on
//! `BEGIN` and restores them on `ROLLBACK`; a failed statement inside an active
//! transaction puts the session into the aborted state. MERGE therefore: opens
//! a transaction (unless the caller already has one open — then it nests into
//! the caller's transaction and lets the caller's COMMIT/ROLLBACK decide),
//! runs every action, and on the first error issues `ROLLBACK` and propagates
//! the error, so a constraint violation midway leaves the target untouched.
//!
//! ## Not yet supported
//!   * `RETURNING` (PG 17) → typed 0A000 with a hint.
//!   * `WHEN MATCHED BY SOURCE` / `BY TARGET` (BigQuery/Snowflake extensions,
//!     not PG 15/16) → typed 0A000.
//!   * `DO UPDATE … WHERE` / `DELETE WHERE` Oracle tails on the action.

use arrow_array::{Array, RecordBatch};
use basin_common::{BasinError, Result};
use sqlparser::ast::{
    Expr, MergeAction, MergeClause, MergeClauseKind, MergeInsertKind, ObjectName, TableFactor,
};

use crate::{ExecResult, ProjectSession};

/// Per-source-row action selected after evaluating the WHEN clauses.
enum RowAction<'a> {
    /// Update the target row identified by its PK using the clause's SET.
    Update { set_cols: &'a [String] },
    /// Delete the target row identified by its PK.
    Delete,
    /// Insert a new row from the source row.
    Insert,
    /// Matched DO NOTHING / no clause matched — skip.
    Nothing,
}

/// Execute a `MERGE INTO` statement.
pub(crate) async fn exec_merge(
    sess: &ProjectSession,
    merge: sqlparser::ast::Merge,
) -> Result<ExecResult> {
    let sqlparser::ast::Merge {
        table,
        source,
        on,
        clauses,
        output,
        ..
    } = merge;

    // ── Reject unsupported surface up front (typed 0A000) ─────────────────
    if output.is_some() {
        return Err(BasinError::feature_not_supported(
            "MERGE ... OUTPUT (MSSQL) is not supported",
        ));
    }
    for c in &clauses {
        match c.clause_kind {
            MergeClauseKind::Matched | MergeClauseKind::NotMatched => {}
            MergeClauseKind::NotMatchedBySource | MergeClauseKind::NotMatchedByTarget => {
                return Err(BasinError::feature_not_supported(
                    "MERGE WHEN [NOT] MATCHED BY SOURCE/TARGET is not supported \
                     (PostgreSQL 15/16 only support WHEN MATCHED / WHEN NOT MATCHED)",
                ));
            }
        }
        if let MergeAction::Update(u) = &c.action {
            if u.update_predicate.is_some() || u.delete_predicate.is_some() {
                return Err(BasinError::feature_not_supported(
                    "MERGE UPDATE ... WHERE / DELETE WHERE (Oracle) is not supported",
                ));
            }
        }
        if let MergeAction::Insert(i) = &c.action {
            if i.insert_predicate.is_some() {
                return Err(BasinError::feature_not_supported(
                    "MERGE INSERT ... WHERE (Oracle) is not supported",
                ));
            }
        }
    }

    // ── Resolve target + its primary key ──────────────────────────────────
    let (target_name, target_alias) = resolve_target(&table)?;
    let target = basin_common::TableName::new(target_name.clone())?;
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &target)
        .await?;
    if meta.pk_columns.is_empty() {
        return Err(BasinError::feature_not_supported(format!(
            "MERGE requires the target table {target_name:?} to have a PRIMARY KEY in v0.1 \
             (the primary key is used as the per-row action identity)"
        )));
    }
    let pk_cols: Vec<String> = meta.pk_columns.clone();
    let target_q = target_alias.clone().unwrap_or_else(|| target_name.clone());

    // ── Render the source as a referenceable relation with an alias ───────
    let source_sql = render_source(&source)?;

    // ── Build the join projection ─────────────────────────────────────────
    //
    // Projection layout (stable, index-addressed below):
    //   [0 .. pk_cols.len())                : target PK columns
    //   next clauses.len() columns          : each clause's AND-predicate bool
    //   then, per UPDATE clause, its SET RHS columns (flattened in clause order)
    //   then, per INSERT clause, its VALUE columns (flattened in clause order)
    let mut proj: Vec<String> = Vec::new();

    // 1) target PK (qualified by target alias/name).
    for c in &pk_cols {
        proj.push(format!("{target_q}.{c} AS __mrg_pk_{c}"));
    }

    // 2) per-clause predicate booleans.
    for (i, c) in clauses.iter().enumerate() {
        let pred = match &c.predicate {
            Some(p) => p.to_string(),
            None => "TRUE".to_string(),
        };
        // A NOT MATCHED clause's AND-predicate must only see source columns;
        // DataFusion enforces scope naturally (target columns are NULL on an
        // unmatched row, and the NOT MATCHED branch is only taken when the PK
        // is NULL, so we still gate on PK-NULL in Rust below).
        proj.push(format!("({pred}) AS __mrg_p{i}"));
    }

    // 3) UPDATE SET RHS, flattened. Record (clause_idx -> (set_cols, base_col_offset)).
    //    4) INSERT VALUES, flattened. Record (clause_idx -> (cols, base_col_offset)).
    let mut update_plans: Vec<(usize, Vec<String>, usize)> = Vec::new();
    let mut insert_plans: Vec<(usize, Vec<String>, usize)> = Vec::new();
    let mut next_off = pk_cols.len() + clauses.len();

    for (ci, c) in clauses.iter().enumerate() {
        match &c.action {
            MergeAction::Update(u) => {
                let mut set_cols = Vec::with_capacity(u.assignments.len());
                for (k, a) in u.assignments.iter().enumerate() {
                    let col = assignment_target_col(a)?;
                    proj.push(format!("({}) AS __mrg_u{ci}_{k}", a.value));
                    set_cols.push(col);
                }
                let base = next_off;
                next_off += u.assignments.len();
                update_plans.push((ci, set_cols, base));
            }
            MergeAction::Insert(ins) => {
                // Column list: explicit, else fall back to the table's columns
                // in declaration order.
                let cols: Vec<String> = if ins.columns.is_empty() {
                    meta.schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect()
                } else {
                    ins.columns
                        .iter()
                        .map(|c| last_ident(c))
                        .collect::<Result<Vec<_>>>()?
                };
                let value_exprs: Vec<Expr> = match &ins.kind {
                    MergeInsertKind::Values(values) => {
                        if values.rows.len() != 1 {
                            return Err(BasinError::feature_not_supported(
                                "MERGE INSERT VALUES must be a single row referencing the source",
                            ));
                        }
                        values.rows[0].clone()
                    }
                    MergeInsertKind::Row => {
                        return Err(BasinError::feature_not_supported(
                            "MERGE INSERT ROW (BigQuery) is not supported",
                        ));
                    }
                };
                if value_exprs.len() != cols.len() {
                    return Err(BasinError::InvalidSchema(format!(
                        "MERGE INSERT has {} target columns but {} values",
                        cols.len(),
                        value_exprs.len()
                    )));
                }
                for (k, e) in value_exprs.iter().enumerate() {
                    proj.push(format!("({e}) AS __mrg_i{ci}_{k}"));
                }
                let base = next_off;
                next_off += value_exprs.len();
                insert_plans.push((ci, cols, base));
            }
            MergeAction::Delete { .. } => {
                // No payload columns; identity comes from the PK projection.
            }
        }
    }

    // ── Assemble + run the match query ────────────────────────────────────
    //
    // `source LEFT JOIN target ON cond` keeps every source row; the target PK
    // is NULL exactly when the source row matched nothing.
    let join_sql = format!(
        "SELECT {proj} FROM {source_sql} LEFT JOIN {target_from} ON {on}",
        proj = proj.join(", "),
        target_from = match &target_alias {
            Some(a) if a != &target_name => format!("{target_name} {a}"),
            _ => target_name.clone(),
        },
    );

    let batches = match Box::pin(sess.execute(&join_sql)).await? {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => Vec::new(),
    };

    // ── Duplicate-match detection (SQLSTATE 21000) ────────────────────────
    detect_duplicate_target_match(&batches, pk_cols.len())?;

    // ── Drive the per-row actions inside one transaction ──────────────────
    let opened_txn = !crate::session::tx_is_active(&sess.state);
    if opened_txn {
        Box::pin(sess.execute("BEGIN")).await?;
    }

    let result = drive_actions(
        sess,
        &batches,
        &clauses,
        &pk_cols,
        &target_name,
        &update_plans,
        &insert_plans,
    )
    .await;

    let affected = match result {
        Ok(n) => n,
        Err(e) => {
            if opened_txn {
                // The failing statement already aborted the txn; ROLLBACK is
                // the only legal exit and restores the pre-MERGE catalog heads.
                let _ = Box::pin(sess.execute("ROLLBACK")).await;
            }
            return Err(e);
        }
    };

    if opened_txn {
        Box::pin(sess.execute("COMMIT")).await?;
    }

    Ok(ExecResult::Empty {
        tag: format!("MERGE {affected}"),
    })
}

/// Run the per-row actions; returns the total number of affected rows.
#[allow(clippy::too_many_arguments)]
async fn drive_actions(
    sess: &ProjectSession,
    batches: &[RecordBatch],
    clauses: &[MergeClause],
    pk_cols: &[String],
    target_name: &str,
    update_plans: &[(usize, Vec<String>, usize)],
    insert_plans: &[(usize, Vec<String>, usize)],
) -> Result<usize> {
    let pk_count = pk_cols.len();
    let mut affected = 0usize;

    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            // matched ⇔ every PK column is non-NULL (a PK column is NOT NULL,
            // so a NULL here can only come from the LEFT JOIN miss).
            let matched = (0..pk_count).all(|i| !batch.column(i).is_null(row));

            // First-match-wins over the clauses in textual order.
            let mut chosen: Option<usize> = None;
            for (ci, c) in clauses.iter().enumerate() {
                let kind_ok = match c.clause_kind {
                    MergeClauseKind::Matched => matched,
                    MergeClauseKind::NotMatched => !matched,
                    _ => false,
                };
                if !kind_ok {
                    continue;
                }
                // Predicate boolean lives at column pk_count + ci.
                let pred_idx = pk_count + ci;
                let pred_arr = batch.column(pred_idx);
                let pred_true = !pred_arr.is_null(row)
                    && pred_arr
                        .as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .map(|b| b.value(row))
                        .unwrap_or(false);
                if pred_true {
                    chosen = Some(ci);
                    break;
                }
            }

            let Some(ci) = chosen else {
                continue;
            };
            let action = resolve_row_action(ci, clauses, update_plans);

            match action {
                RowAction::Nothing => {}
                RowAction::Delete => {
                    let pk_pred = render_pk_predicate(batch, pk_cols, row)?;
                    let sql = format!("DELETE FROM {target_name} WHERE {pk_pred}");
                    affected += run_dml_count(sess, &sql).await?;
                }
                RowAction::Update { set_cols } => {
                    let (_, _, base) = update_plans
                        .iter()
                        .find(|(c, _, _)| *c == ci)
                        .expect("update plan present for chosen update clause");
                    let mut sets = Vec::with_capacity(set_cols.len());
                    for (k, col) in set_cols.iter().enumerate() {
                        let arr = batch.column(base + k);
                        let dt = schema.field(base + k).data_type().clone();
                        let lit = render_scalar(arr.as_ref(), row, &dt)?;
                        sets.push(format!("{col} = {lit}"));
                    }
                    let pk_pred = render_pk_predicate(batch, pk_cols, row)?;
                    let sql = format!(
                        "UPDATE {target_name} SET {sets} WHERE {pk_pred}",
                        sets = sets.join(", "),
                    );
                    affected += run_dml_count(sess, &sql).await?;
                }
                RowAction::Insert => {
                    let (_, cols, base) = insert_plans
                        .iter()
                        .find(|(c, _, _)| *c == ci)
                        .expect("insert plan present for chosen insert clause");
                    let mut vals = Vec::with_capacity(cols.len());
                    for k in 0..cols.len() {
                        let arr = batch.column(base + k);
                        let dt = schema.field(base + k).data_type().clone();
                        vals.push(render_scalar(arr.as_ref(), row, &dt)?);
                    }
                    let sql = format!(
                        "INSERT INTO {target_name} ({cols}) VALUES ({vals})",
                        cols = cols.join(", "),
                        vals = vals.join(", "),
                    );
                    affected += run_dml_count(sess, &sql).await?;
                }
            }
        }
    }

    Ok(affected)
}

fn resolve_row_action<'a>(
    ci: usize,
    clauses: &'a [MergeClause],
    update_plans: &'a [(usize, Vec<String>, usize)],
) -> RowAction<'a> {
    match &clauses[ci].action {
        MergeAction::Update(_) => {
            let set_cols = update_plans
                .iter()
                .find(|(c, _, _)| *c == ci)
                .map(|(_, s, _)| s.as_slice())
                .unwrap_or(&[]);
            RowAction::Update { set_cols }
        }
        MergeAction::Delete { .. } => RowAction::Delete,
        MergeAction::Insert(_) => RowAction::Insert,
    }
}

/// Run a generated DML statement and add its `<verb> N` count.
async fn run_dml_count(sess: &ProjectSession, sql: &str) -> Result<usize> {
    match Box::pin(sess.execute(sql)).await? {
        ExecResult::Empty { tag } => Ok(parse_tag_count(&tag)),
        ExecResult::Rows { batches, .. } => Ok(batches.iter().map(|b| b.num_rows()).sum()),
    }
}

/// Parse the affected-row count out of a command tag like `"UPDATE 3"` or
/// `"INSERT 0 1"`. The count is always the last whitespace-separated token.
fn parse_tag_count(tag: &str) -> usize {
    tag.rsplit(char::is_whitespace)
        .next()
        .and_then(|t| t.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Materialise the matched-target PK set and reject a PK matched twice.
fn detect_duplicate_target_match(batches: &[RecordBatch], pk_count: usize) -> Result<()> {
    use std::collections::HashSet;
    let mut seen: HashSet<Vec<String>> = HashSet::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            // Only matched rows (all PK columns non-NULL) participate.
            if (0..pk_count).any(|i| batch.column(i).is_null(row)) {
                continue;
            }
            let mut key = Vec::with_capacity(pk_count);
            for i in 0..pk_count {
                let arr = batch.column(i);
                let dt = schema.field(i).data_type().clone();
                key.push(render_scalar(arr.as_ref(), row, &dt)?);
            }
            if !seen.insert(key) {
                return Err(BasinError::CardinalityViolation(
                    "MERGE command cannot affect row a second time. Ensure that not more \
                     than one source row matches any one target row."
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

/// Build `pk = lit [AND …]` for the target row in `batch[row]`.
fn render_pk_predicate(batch: &RecordBatch, pk_cols: &[String], row: usize) -> Result<String> {
    let schema = batch.schema();
    let mut parts = Vec::with_capacity(pk_cols.len());
    for (i, col) in pk_cols.iter().enumerate() {
        let arr = batch.column(i);
        let dt = schema.field(i).data_type().clone();
        let lit = render_scalar(arr.as_ref(), row, &dt)?;
        parts.push(format!("{col} = {lit}"));
    }
    Ok(parts.join(" AND "))
}

/// Resolve the target table name and optional alias from the MERGE target.
fn resolve_target(table: &TableFactor) -> Result<(String, Option<String>)> {
    match table {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if args.is_some() {
                return Err(BasinError::InvalidSchema(
                    "MERGE target must be a plain table name".into(),
                ));
            }
            let name = single_part_name(name)?;
            let alias = alias.as_ref().map(|a| a.name.value.clone());
            Ok((name, alias))
        }
        _ => Err(BasinError::InvalidSchema(
            "MERGE target must be a table name".into(),
        )),
    }
}

/// Render the source factor to SQL plus the name it is referenced by.
///
/// A bare table renders as-is (referenced by alias or table name). A derived
/// table (subquery / VALUES) must carry an alias so its columns are
/// referenceable in the ON condition and action expressions — PG requires the
/// same.
fn render_source(source: &TableFactor) -> Result<String> {
    match source {
        TableFactor::Table { name, .. } => {
            // Validate the name is single-part; render with any alias preserved.
            let _ = single_part_name(name)?;
            Ok(source.to_string())
        }
        TableFactor::Derived { alias, .. } => {
            if alias.is_none() {
                return Err(BasinError::InvalidSchema(
                    "MERGE source subquery / VALUES must have an alias".into(),
                ));
            }
            Ok(source.to_string())
        }
        _ => Err(BasinError::feature_not_supported(
            "MERGE source must be a table, subquery, or VALUES",
        )),
    }
}

fn assignment_target_col(a: &sqlparser::ast::Assignment) -> Result<String> {
    match &a.target {
        sqlparser::ast::AssignmentTarget::ColumnName(name) => {
            Ok(name.0.last().map(|i| i.to_string()).unwrap_or_default())
        }
        sqlparser::ast::AssignmentTarget::Tuple(_) => Err(BasinError::feature_not_supported(
            "MERGE UPDATE tuple assignment targets are not supported",
        )),
    }
}

fn last_ident(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .map(|i| i.to_string())
        .ok_or_else(|| BasinError::InvalidSchema("empty column reference".into()))
}

/// Single-part (optionally `public.`-qualified) table name → bare name.
fn single_part_name(name: &ObjectName) -> Result<String> {
    let parts: Vec<String> = name.0.iter().map(|p| p.to_string()).collect();
    match parts.as_slice() {
        [t] => Ok(t.clone()),
        [s, t] if s.eq_ignore_ascii_case("public") => Ok(t.clone()),
        _ => Err(BasinError::InvalidSchema(format!(
            "MERGE: unsupported qualified table name {name}"
        ))),
    }
}

/// Render an Arrow scalar at `row` as a SQL literal. Self-contained (the
/// `dml_mutate` twin of this is private); covers the same type set.
fn render_scalar(
    arr: &dyn arrow_array::Array,
    row: usize,
    dt: &arrow_schema::DataType,
) -> Result<String> {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    if arr.is_null(row) {
        return Ok("NULL".into());
    }
    match dt {
        DataType::Utf8 => Ok(quote(arr.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => Ok(quote(arr.as_string::<i64>().value(row))),
        DataType::Int8 => Ok(arr
            .as_primitive::<arrow_array::types::Int8Type>()
            .value(row)
            .to_string()),
        DataType::Int16 => Ok(arr
            .as_primitive::<arrow_array::types::Int16Type>()
            .value(row)
            .to_string()),
        DataType::Int32 => Ok(arr
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(row)
            .to_string()),
        DataType::Int64 => Ok(arr
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float32 => Ok(arr
            .as_primitive::<arrow_array::types::Float32Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(arr
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(row)
            .to_string()),
        DataType::Boolean => Ok(if arr.as_boolean().value(row) {
            "TRUE".into()
        } else {
            "FALSE".into()
        }),
        DataType::Timestamp(_, _) => {
            use arrow_array::types::TimestampMicrosecondType;
            Ok(arr
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string())
        }
        DataType::Date32 => {
            use arrow_array::types::Date32Type;
            let days = arr.as_primitive::<Date32Type>().value(row);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let d = epoch + chrono::Duration::days(days as i64);
            Ok(format!("'{}'", d.format("%Y-%m-%d")))
        }
        DataType::Binary => Ok(format!(
            "'\\x{}'",
            hex::encode(arr.as_binary::<i32>().value(row))
        )),
        DataType::LargeBinary => Ok(format!(
            "'\\x{}'",
            hex::encode(arr.as_binary::<i64>().value(row))
        )),
        other => Err(BasinError::feature_not_supported(format!(
            "MERGE: unsupported column type {other:?} in a projected value (v0.1)"
        ))),
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
