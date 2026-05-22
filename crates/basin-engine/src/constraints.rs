//! PRIMARY KEY / CHECK / FOREIGN KEY enforcement on INSERT, UPDATE, DELETE.
//!
//! Three surfaces share this module:
//!
//! - `enforce_pk_on_insert` — full-table scan for any row whose PK
//!   tuple matches a row already in the table. v0.2 secondary indexes
//!   (Phase 5.7 B1) will replace the scan with a B-tree probe; for
//!   v0.1 the cost is `O(rows_in_table)` per INSERT batch. Documented
//!   in code as the dependency.
//! - `enforce_check_constraints` — evaluates each CHECK predicate over
//!   the batch via DataFusion (same template as
//!   `crate::generated_cols::eval_expression` and
//!   `crate::type_ddl::enforce_domain_checks`).
//! - `enforce_fk_on_insert` — for each referencing row, verifies the
//!   referenced PK tuple exists on the referenced table (same-project
//!   only). NULL in any FK column makes the row exempt (PG MATCH SIMPLE
//!   default).
//! - `enforce_fk_on_delete_or_pk_update` — for DELETE / UPDATE-of-PK
//!   on a parent table, finds all child tables whose FK references it
//!   and either rejects (NO ACTION) or cascades (CASCADE).
//!
//! 5.11.C2 reactor-style constraint reactors share SQLSTATE 23514 with
//! CHECK but live in a separate module (`reactor_ddl`) — those fire on
//! published events; CHECK fires inside the write path.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema};
use basin_catalog::{
    Catalog, CheckConstraint, ForeignKeyDef, RefAction, TableMetadata, UniqueConstraint,
};
use basin_common::{BasinError, ProjectId, Result, TableName};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::convert::{batch_df_to_ws, batch_ws_to_df};
use crate::types::field_is_citext;

// --------------------------------------------------------------------
// PRIMARY KEY enforcement
// --------------------------------------------------------------------

/// Reject the new batch if any row's PK tuple already exists in the
/// table, OR if two rows in the same batch share a PK tuple.
///
/// Cost note: this scans the entire `(project, table)` data file set
/// once per call. v0.2 (Phase 5.7 B1) secondary indexes will replace
/// the scan with a B-tree probe. For v0.1 the per-INSERT cost is
/// `O(rows_in_table + rows_in_batch)`.
pub(crate) async fn enforce_pk_on_insert(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    pk_columns: &[String],
    batch: &RecordBatch,
) -> Result<()> {
    if pk_columns.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }

    // Resolve PK column indexes in the batch's schema.
    let pk_idx: Vec<usize> = pk_columns
        .iter()
        .map(|c| {
            batch
                .schema()
                .index_of(c)
                .map_err(|_| BasinError::internal(format!("PK column {c:?} missing from batch")))
        })
        .collect::<Result<Vec<_>>>()?;

    // 1. Intra-batch dup check.
    let mut seen: std::collections::HashSet<Vec<String>> =
        std::collections::HashSet::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = pk_tuple_for_row(batch, &pk_idx, row)?;
        // Any null in the PK is a hard error — PK columns are NOT
        // NULL by construction, but defend against future regressions.
        if key.is_none() {
            return Err(BasinError::CheckViolation(format!(
                "null value in column violates not-null constraint on PRIMARY KEY of \
                 \"{table_name_str}\""
            )));
        }
        let k = key.unwrap();
        if !seen.insert(k.clone()) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                 Key ({})=({}) already exists.",
                pk_columns.join(", "),
                k.join(", ")
            )));
        }
    }

    // 2. Existing-table scan. Read every data file's PK columns.
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    if data_files.is_empty() {
        return Ok(());
    }
    let mut existing: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    for f in &data_files {
        let mut stream = storage.read_file(project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let rb_pk_idx: Vec<usize> = pk_columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!("PK column {c:?} missing from data file"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rb.num_rows() {
                if let Some(k) = pk_tuple_for_row(&rb, &rb_pk_idx, row)? {
                    existing.insert(k);
                }
            }
        }
    }
    for row in 0..batch.num_rows() {
        let Some(k) = pk_tuple_for_row(batch, &pk_idx, row)? else {
            continue;
        };
        if existing.contains(&k) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                 Key ({})=({}) already exists.",
                pk_columns.join(", "),
                k.join(", ")
            )));
        }
    }
    Ok(())
}

/// Reject the new batch if any row's UNIQUE-constraint tuple already
/// exists in the table, OR if two rows in the same batch share a
/// UNIQUE tuple.
///
/// NULL handling: PG's default `UNIQUE (...)` treats every NULL as
/// distinct — any number of rows may have NULL in a UNIQUE column.
/// We mirror that: rows with NULL in *any* UNIQUE column are skipped
/// entirely from the comparison. (`NULLS NOT DISTINCT`, introduced
/// in PG 15, is not modelled in v0.1.)
///
/// Cost note: same shape as `enforce_pk_on_insert` — one full scan
/// of every data file per UNIQUE constraint per call. v0.2 (Phase 5.7
/// B1) secondary indexes will replace the scan with a B-tree probe.
/// For v0.1 the per-INSERT cost is `O(rows_in_table * unique_count +
/// rows_in_batch * unique_count)`. basin-auth's targeted tables
/// (users, sessions, api_keys, refresh_tokens, magic_links) stay in
/// the thousands-to-low-millions range where the scan is fine.
pub(crate) async fn enforce_unique_on_insert(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    unique_constraints: &[UniqueConstraint],
    batch: &RecordBatch,
) -> Result<()> {
    if unique_constraints.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    for u in unique_constraints {
        enforce_one_unique(
            storage,
            project,
            table,
            table_name_str,
            &u.name,
            &u.columns,
            batch,
            &[],
        )
        .await?;
    }
    Ok(())
}

/// UPDATE-side UNIQUE enforcement: same as `enforce_unique_on_insert`
/// but excludes data files in `replaced_paths` from the
/// existing-table scan (those files are being rewritten in this
/// transaction so the rows they contain aren't really "still present").
pub(crate) async fn enforce_unique_on_update(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    unique_constraints: &[UniqueConstraint],
    batches: &[RecordBatch],
    replaced_paths: &[String],
) -> Result<()> {
    if unique_constraints.is_empty() {
        return Ok(());
    }
    for u in unique_constraints {
        for b in batches {
            enforce_one_unique(
                storage,
                project,
                table,
                table_name_str,
                &u.name,
                &u.columns,
                b,
                replaced_paths,
            )
            .await?;
        }
    }
    Ok(())
}

/// Workhorse for both the INSERT and UPDATE flavours. Scans the
/// (non-replaced) data files, then walks `batch` row-by-row checking
/// for collisions both against the existing set and within the batch
/// itself. NULL in any UNIQUE column makes the row exempt.
#[allow(clippy::too_many_arguments)]
async fn enforce_one_unique(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    constraint_name: &str,
    columns: &[String],
    batch: &RecordBatch,
    replaced_paths: &[String],
) -> Result<()> {
    if columns.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    let col_idx: Vec<usize> = columns
        .iter()
        .map(|c| {
            batch.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("UNIQUE column {c:?} missing from batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Determine which constraint columns are citext so we can apply
    // case-folding when building the dedup key. citext UNIQUE treats
    // 'Foo' and 'foo' as duplicates (PG citext extension semantics).
    let batch_schema = batch.schema();
    let citext_positions: std::collections::HashSet<usize> = col_idx
        .iter()
        .enumerate()
        .filter_map(|(pos, &arr_idx)| {
            let field = batch_schema.field(arr_idx);
            if field_is_citext(field) { Some(pos) } else { None }
        })
        .collect();

    // 1. Existing-table scan (skip replaced files on UPDATE).
    let replaced: std::collections::HashSet<&str> =
        replaced_paths.iter().map(|s| s.as_str()).collect();
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    let mut existing: std::collections::HashSet<Vec<String>> = Default::default();
    for f in &data_files {
        if replaced.contains(f.path.as_ref()) {
            continue;
        }
        let mut stream = storage.read_file(project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let rb_idx: Vec<usize> = columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!("UNIQUE column {c:?} missing from data file"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // Reuse `citext_positions` derived from the INSERT batch's schema
            // (which HAS Arrow field metadata) rather than re-deriving from the
            // Parquet-read-back schema.  Parquet's `ArrowWriter` does NOT
            // preserve Arrow field metadata in the per-batch schema emitted by
            // the reader — only the `ARROW:schema` blob in the file footer
            // carries it, and `read_file` reads without a catalog_schema hint
            // so the re-stamping path does not run here.  The INSERT batch
            // schema and the on-disk schema have the same columns in the same
            // logical positions for the columns listed in the constraint, so
            // reusing `citext_positions` is correct.
            for row in 0..rb.num_rows() {
                if let Some(k) =
                    pk_tuple_for_row_citext(&rb, &rb_idx, row, &citext_positions)?
                {
                    existing.insert(k);
                }
            }
        }
    }

    // 2. Intra-batch + existing check.
    let mut seen: std::collections::HashSet<Vec<String>> = Default::default();
    for row in 0..batch.num_rows() {
        // NULL in any column → row exempt (PG default).
        let Some(k) = pk_tuple_for_row_citext(batch, &col_idx, row, &citext_positions)? else {
            continue;
        };
        if existing.contains(&k) || !seen.insert(k.clone()) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{constraint_name}\" on \
                 table \"{table_name_str}\": Key ({})=({}) already exists.",
                columns.join(", "),
                k.join(", ")
            )));
        }
    }
    Ok(())
}

/// Variant of `pk_tuple_for_row` that lower-cases string values for columns
/// whose position in the key is in `citext_positions` (citext column indexes).
/// This ensures UNIQUE constraints on citext columns treat 'Foo' and 'foo'
/// as duplicates, matching PostgreSQL citext semantics.
fn pk_tuple_for_row_citext(
    batch: &RecordBatch,
    pk_idx: &[usize],
    row: usize,
    citext_positions: &std::collections::HashSet<usize>,
) -> Result<Option<Vec<String>>> {
    let mut out = Vec::with_capacity(pk_idx.len());
    for (pos, &i) in pk_idx.iter().enumerate() {
        let arr = batch.column(i);
        if arr.is_null(row) {
            return Ok(None);
        }
        let s = scalar_to_canonical_string(arr.as_ref(), row)?;
        // Apply case-folding for citext columns so 'Foo' == 'foo'.
        let s = if citext_positions.contains(&pos) {
            s.to_lowercase()
        } else {
            s
        };
        out.push(s);
    }
    Ok(Some(out))
}

/// Format a row's PK tuple as a Vec<String>. Returns None if any PK
/// column is null (caller decides whether that's a violation; for PK
/// columns it always is, for FK columns it makes the row exempt).
pub(crate) fn pk_tuple_for_row(
    batch: &RecordBatch,
    pk_idx: &[usize],
    row: usize,
) -> Result<Option<Vec<String>>> {
    let mut out = Vec::with_capacity(pk_idx.len());
    for &i in pk_idx {
        let arr = batch.column(i);
        if arr.is_null(row) {
            return Ok(None);
        }
        out.push(scalar_to_canonical_string(arr.as_ref(), row)?);
    }
    Ok(Some(out))
}

/// Convert an Arrow scalar at `(arr, row)` into a canonical string the
/// PK tuple comparison hashes on. Two rows with equal logical values
/// must hash to the same string.
fn scalar_to_canonical_string(arr: &dyn Array, row: usize) -> Result<String> {
    match arr.data_type() {
        DataType::Int16 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| BasinError::internal("Int16Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Int32 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| BasinError::internal("Int32Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("Int64Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BasinError::internal("Float64Array downcast"))?;
            // Use a fixed precision so 1.0 == 1.000000 hash the same.
            Ok(format!("{:?}", a.value(row)))
        }
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("StringArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Boolean => {
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("BooleanArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Timestamp(_, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .ok_or_else(|| BasinError::internal("TimestampMicrosecondArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        // UUID columns land as `FixedSizeBinary(16)` (see
        // `crate::types::arrow_data_type`). Canonicalise by formatting the
        // 16 bytes back through `uuid::Uuid::from_bytes` so two PK rows
        // with identical UUIDs hash to the same string regardless of byte
        // ordering quirks.
        DataType::FixedSizeBinary(16) => {
            let a = arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| BasinError::internal("FixedSizeBinaryArray downcast"))?;
            let bytes = a.value(row);
            let mut buf = [0u8; 16];
            buf.copy_from_slice(bytes);
            Ok(uuid::Uuid::from_bytes(buf).hyphenated().to_string())
        }
        other => Err(BasinError::InvalidSchema(format!(
            "PRIMARY KEY / FOREIGN KEY column type {other:?} is not supported in v0.1 (use \
             BIGINT / INTEGER / SMALLINT / TEXT / BOOLEAN / TIMESTAMPTZ / UUID)"
        ))),
    }
}

// --------------------------------------------------------------------
// CHECK enforcement
// --------------------------------------------------------------------

/// Normalise a stored CHECK predicate so it is a bare boolean expression
/// suitable for `SELECT (<expr>) FROM t`.
///
/// The column-level DDL path historically called `CheckConstraint::to_string()`
/// which renders `CHECK (<inner>)` including the keyword wrapper. DataFusion
/// would then see `check(...)` as an unknown scalar function call. This helper
/// strips a leading (case-insensitive) `CHECK` keyword followed by a balanced
/// outer `(...)`, returning only the inner expression text. If the predicate
/// does not start with the `CHECK` keyword the function is a no-op (idempotent).
///
/// Robustness: we locate the matching close-paren by counting nesting depth
/// rather than naive string-slicing, so expressions that contain function
/// calls with their own parentheses are handled correctly.
fn strip_check_wrapper(predicate: &str) -> &str {
    let trimmed = predicate.trim();
    // Case-insensitive prefix check for "CHECK".
    let after_kw = if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("CHECK") {
        trimmed[5..].trim_start()
    } else {
        return predicate;
    };
    // Expect the next character to be '('.
    if !after_kw.starts_with('(') {
        return predicate;
    }
    // Walk the string tracking paren depth to find the matching ')'.
    let mut depth: usize = 0;
    let mut close_pos: Option<usize> = None;
    for (i, ch) in after_kw.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_pos = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    match close_pos {
        // The matching ')' must be the last non-whitespace character.
        Some(end) if after_kw[end + 1..].trim().is_empty() => &after_kw[1..end],
        _ => predicate,
    }
}

/// Evaluate every CHECK predicate against the batch via DataFusion.
/// Rows where the predicate is false (or NULL — PG treats NULL as
/// satisfying the predicate, so we accept NULL) raise SQLSTATE 23514.
pub(crate) async fn enforce_check_constraints(
    table_name_str: &str,
    schema: &Schema,
    checks: &[CheckConstraint],
    batch: &RecordBatch,
) -> Result<()> {
    if checks.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    // One DataFusion context per call; the predicates are evaluated as
    // projections against a temp `t` table whose schema is the input
    // batch.
    let ctx = SessionContext::new();
    crate::udf::register_distance_udfs(&ctx);
    crate::udf::register_pg_udfs(&ctx);
    crate::udf::register_pg_compat_udfs(&ctx);

    let df_batch = batch_ws_to_df(batch)?;
    let df_schema = df_batch.schema();
    let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
        .map_err(|e| BasinError::internal(format!("CHECK MemTable: {e}")))?;
    ctx.register_table("t", Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("CHECK register: {e}")))?;

    for c in checks {
        // Normalize: strip any leading CHECK (...) wrapper that may have been
        // stored by an earlier version of the DDL path, making evaluation
        // idempotent whether the stored form is bare or wrapped.
        let bare = strip_check_wrapper(&c.predicate);
        let sql = format!("SELECT ({bare}) AS ok FROM t");
        let df = ctx.sql(&sql).await.map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CHECK constraint {:?} predicate {:?}: {e}",
                c.name, c.predicate
            ))
        })?;
        let results = df.collect().await.map_err(|e| {
            BasinError::InvalidSchema(format!("CHECK constraint {:?} evaluation: {e}", c.name))
        })?;
        let mut row_global: usize = 0;
        for rb in &results {
            let ws_rb = batch_df_to_ws(rb)?;
            let ok = ws_rb
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "CHECK constraint {:?}: predicate must return BOOLEAN",
                        c.name
                    ))
                })?;
            for r in 0..ws_rb.num_rows() {
                // PG: NULL satisfies CHECK (only FALSE fails). Match.
                if !ok.is_null(r) && !ok.value(r) {
                    let row_text = render_row_as_pg_tuple(schema, batch, row_global)?;
                    return Err(BasinError::CheckViolation(format!(
                        "new row for relation \"{table_name_str}\" violates check constraint \
                         \"{}\": Failing row contains ({row_text}).",
                        c.name
                    )));
                }
                row_global += 1;
            }
        }
    }
    Ok(())
}

/// Render `(v1, v2, ...)` in the loose PG style for the failing-row
/// hint in a CHECK violation message. We don't mirror PG byte-for-byte
/// — drivers key off the SQLSTATE, not the message text.
fn render_row_as_pg_tuple(_schema: &Schema, batch: &RecordBatch, row: usize) -> Result<String> {
    let mut parts = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let arr = batch.column(i);
        if arr.is_null(row) {
            parts.push("null".to_string());
            continue;
        }
        parts.push(scalar_to_canonical_string(arr.as_ref(), row)?);
    }
    Ok(parts.join(", "))
}

// --------------------------------------------------------------------
// FOREIGN KEY enforcement (insert / update side)
// --------------------------------------------------------------------

/// For each FK on `meta`, verify each row of `batch` references an
/// existing row in `fk.ref_table` (PK tuple match). NULL in any FK
/// column exempts the row (PG MATCH SIMPLE).
pub(crate) async fn enforce_fk_on_insert(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table_name_str: &str,
    foreign_keys: &[ForeignKeyDef],
    batch: &RecordBatch,
) -> Result<()> {
    if foreign_keys.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    for fk in foreign_keys {
        // Local column indexes.
        let local_idx: Vec<usize> = fk
            .columns
            .iter()
            .map(|c| {
                batch.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("FK local column {c:?} missing from batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build the tuple set we need to look up.
        let mut needed: std::collections::HashSet<Vec<String>> = Default::default();
        let mut row_keys: Vec<Option<Vec<String>>> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let k = pk_tuple_for_row(batch, &local_idx, row)?;
            if let Some(k) = k.as_ref() {
                needed.insert(k.clone());
            }
            row_keys.push(k);
        }
        if needed.is_empty() {
            continue;
        }
        // Load referenced table (same project).
        let ref_table_name = TableName::new(fk.ref_table.clone())?;
        let ref_meta = catalog
            .load_table(project, &ref_table_name)
            .await
            .map_err(|e| match e {
                BasinError::NotFound(_) => BasinError::ForeignKeyViolation(format!(
                    "insert or update on table \"{table_name_str}\" violates foreign key \
                     constraint \"{}\": referenced table {:?} no longer exists",
                    fk.name, fk.ref_table
                )),
                other => other,
            })?;
        let ref_pk_set = collect_pk_tuples(&ref_meta, storage, project, &fk.ref_columns).await?;
        for (row, key) in row_keys.iter().enumerate() {
            let Some(k) = key else { continue };
            if !ref_pk_set.contains(k) {
                return Err(BasinError::ForeignKeyViolation(format!(
                    "insert or update on table \"{table_name_str}\" violates foreign key \
                     constraint \"{}\": Key ({})=({}) is not present in table \"{}\".",
                    fk.name,
                    fk.columns.join(", "),
                    k.join(", "),
                    fk.ref_table
                )));
            }
            let _ = row;
        }
    }
    Ok(())
}

/// Read every existing row of `meta`'s table and return the set of PK
/// tuples (using the columns named in `pk_cols`, which v0.1 always
/// equals the table's PK).
async fn collect_pk_tuples(
    meta: &TableMetadata,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    pk_cols: &[String],
) -> Result<std::collections::HashSet<Vec<String>>> {
    let mut out: std::collections::HashSet<Vec<String>> = Default::default();
    let data_files = storage
        .list_data_files_with_stats(project, &meta.table)
        .await?;
    for f in &data_files {
        let mut stream = storage.read_file(project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let idx: Vec<usize> = pk_cols
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!(
                            "PK column {c:?} missing from referenced data file"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rb.num_rows() {
                if let Some(k) = pk_tuple_for_row(&rb, &idx, row)? {
                    out.insert(k);
                }
            }
        }
    }
    Ok(out)
}

// --------------------------------------------------------------------
// FOREIGN KEY enforcement (parent delete / update side)
// --------------------------------------------------------------------

/// Walk every table in `project` and find FKs that point at
/// `parent_table`. Returns `(child_table, fk)` pairs.
pub(crate) async fn fks_referencing(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    parent_table: &str,
) -> Result<Vec<(TableName, ForeignKeyDef)>> {
    let mut out = Vec::new();
    let names = catalog.list_tables(project).await?;
    for n in &names {
        let meta = catalog.load_table(project, n).await?;
        for fk in &meta.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(parent_table) {
                out.push((n.clone(), fk.clone()));
            }
        }
    }
    Ok(out)
}

/// For each parent-row PK tuple in `deleted_keys`, decide whether the
/// referencing children must be rejected (NO ACTION) or cascaded
/// (CASCADE). Returns the child rows to delete keyed by `(child_table,
/// row_predicate)` so the caller can dispatch a CASCADE DELETE through
/// the standard DELETE path.
///
/// For NO ACTION (the default), the function returns
/// `BasinError::ForeignKeyViolation` if any referencing rows exist.
pub(crate) async fn check_parent_delete(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    parent_table_str: &str,
    deleted_pk_tuples: &std::collections::HashSet<Vec<String>>,
    parent_pk_columns: &[String],
) -> Result<Vec<CascadeDelete>> {
    let mut out: Vec<CascadeDelete> = Vec::new();
    if deleted_pk_tuples.is_empty() {
        return Ok(out);
    }
    let referencing = fks_referencing(catalog, project, parent_table_str).await?;
    for (child_table, fk) in &referencing {
        // Map child FK columns → parent PK columns by ref_columns ↔ parent PK position.
        // Build the rows-of-child-with-FK-tuple-in-deleted_pk_tuples.
        let child_meta = catalog.load_table(project, child_table).await?;
        let data_files = storage
            .list_data_files_with_stats(project, child_table)
            .await?;
        // Re-order child FK columns to match the parent's PK order
        // so the tuple lookup against `deleted_pk_tuples` is byte-
        // identical.
        let mut local_in_parent_pk_order: Vec<String> = Vec::with_capacity(parent_pk_columns.len());
        for p in parent_pk_columns {
            // Find index of p in fk.ref_columns; the corresponding
            // local column is the one to read.
            let pos = fk
                .ref_columns
                .iter()
                .position(|r| r.eq_ignore_ascii_case(p))
                .ok_or_else(|| {
                    BasinError::internal(format!(
                        "FK {:?} ref_columns does not include parent PK column {p:?}",
                        fk.name
                    ))
                })?;
            local_in_parent_pk_order.push(fk.columns[pos].clone());
        }

        let mut matching_rows: Vec<HashMap<String, String>> = Vec::new();
        for f in &data_files {
            let mut stream = storage.read_file(project, &f.path).await?;
            while let Some(rb) = stream.next().await {
                let rb = rb?;
                let idx: Vec<usize> = local_in_parent_pk_order
                    .iter()
                    .map(|c| {
                        rb.schema().index_of(c).map_err(|_| {
                            BasinError::internal(format!("FK column {c:?} missing from data file"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..rb.num_rows() {
                    let Some(k) = pk_tuple_for_row(&rb, &idx, row)? else {
                        continue;
                    };
                    if deleted_pk_tuples.contains(&k) {
                        // Capture the matching child row's FK columns
                        // as a HashMap so the caller can build a
                        // WHERE clause for CASCADE DELETE.
                        let mut m = HashMap::new();
                        for (col_name, col_idx) in local_in_parent_pk_order.iter().zip(idx.iter()) {
                            let arr = rb.column(*col_idx);
                            if arr.is_null(row) {
                                m.insert(col_name.clone(), "NULL".into());
                            } else {
                                m.insert(
                                    col_name.clone(),
                                    scalar_to_canonical_string(arr.as_ref(), row)?,
                                );
                            }
                        }
                        matching_rows.push(m);
                    }
                }
            }
        }
        if matching_rows.is_empty() {
            continue;
        }
        match fk.on_delete {
            RefAction::NoAction => {
                let example_row = &matching_rows[0];
                let example_tuple: Vec<String> = local_in_parent_pk_order
                    .iter()
                    .map(|c| example_row.get(c).cloned().unwrap_or_else(|| "?".into()))
                    .collect();
                return Err(BasinError::ForeignKeyViolation(format!(
                    "update or delete on table \"{parent_table_str}\" violates foreign key \
                     constraint \"{}\" on table \"{}\": Key ({})=({}) is still referenced from \
                     table \"{}\".",
                    fk.name,
                    child_table,
                    parent_pk_columns.join(", "),
                    example_tuple.join(", "),
                    child_table
                )));
            }
            RefAction::Cascade => {
                out.push(CascadeDelete {
                    child_table: child_table.clone(),
                    fk_columns: local_in_parent_pk_order,
                    rows: matching_rows,
                });
            }
        }
        let _ = child_meta;
    }
    Ok(out)
}

/// One pending CASCADE DELETE the parent operation has authorised.
/// The caller dispatches a DELETE on `child_table` whose predicate
/// matches every captured row.
#[derive(Debug)]
pub(crate) struct CascadeDelete {
    pub child_table: TableName,
    pub fk_columns: Vec<String>,
    pub rows: Vec<HashMap<String, String>>,
}

/// Helper exported for the DELETE path to extract PK tuples from a
/// captured "before" RecordBatch.
pub(crate) fn pk_tuples_from_batches(
    batches: &[RecordBatch],
    pk_columns: &[String],
) -> Result<std::collections::HashSet<Vec<String>>> {
    let mut out: std::collections::HashSet<Vec<String>> = Default::default();
    for b in batches {
        let idx: Vec<usize> = pk_columns
            .iter()
            .map(|c| {
                b.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("PK column {c:?} missing from before-batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..b.num_rows() {
            if let Some(k) = pk_tuple_for_row(b, &idx, row)? {
                out.insert(k);
            }
        }
    }
    Ok(out)
}

/// Render a WHERE clause matching exactly the PK tuples in `tuples`
/// (one big OR of column-tuple equality literals). The caller routes
/// the resulting SQL through the standard DELETE path so RLS, AUDIT,
/// and the sink registry all fire normally.
pub(crate) fn build_in_predicate_sql(
    rows: &[HashMap<String, String>],
    fk_columns: &[String],
    schema: &Schema,
) -> Result<String> {
    let mut clauses = Vec::with_capacity(rows.len());
    for r in rows {
        let mut parts = Vec::with_capacity(fk_columns.len());
        for c in fk_columns {
            let raw = r
                .get(c)
                .ok_or_else(|| BasinError::internal(format!("cascade row missing column {c}")))?;
            let field = schema
                .field_with_name(c)
                .map_err(|_| BasinError::internal(format!("cascade column {c} missing")))?;
            let lit = match field.data_type() {
                DataType::Utf8 => format!("'{}'", raw.replace('\'', "''")),
                _ => raw.clone(),
            };
            parts.push(format!("{c} = {lit}"));
        }
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    Ok(clauses.join(" OR "))
}

// --------------------------------------------------------------------
// Unit tests
// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::strip_check_wrapper;

    // --- strip_check_wrapper ------------------------------------------------

    /// Bare expression without a CHECK wrapper must be returned unchanged.
    #[test]
    fn strip_bare_expression_is_noop() {
        assert_eq!(strip_check_wrapper("score >= 0"), "score >= 0");
        assert_eq!(strip_check_wrapper("price > 0"), "price > 0");
        assert_eq!(strip_check_wrapper("a AND b"), "a AND b");
    }

    /// Typical column-level predicate stored with the CHECK (...) wrapper.
    #[test]
    fn strip_column_level_check_wrapper() {
        assert_eq!(strip_check_wrapper("CHECK (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("CHECK (price > 0)"), "price > 0");
    }

    /// Case-insensitive matching.
    #[test]
    fn strip_check_wrapper_case_insensitive() {
        assert_eq!(strip_check_wrapper("check (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("Check (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("CHECK (score >= 0)"), "score >= 0");
    }

    /// Whitespace between CHECK and the opening paren.
    #[test]
    fn strip_check_wrapper_with_extra_whitespace() {
        assert_eq!(strip_check_wrapper("CHECK  (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("  CHECK (score >= 0)  "), "score >= 0");
    }

    /// Nested parentheses inside the expression must not be stripped incorrectly.
    #[test]
    fn strip_check_wrapper_nested_parens() {
        // The outer CHECK (...) wraps an expression that contains a function call.
        let stored = "CHECK (coalesce(score, 0) >= 0)";
        assert_eq!(strip_check_wrapper(stored), "coalesce(score, 0) >= 0");
    }

    /// Named constraint form: `CONSTRAINT name CHECK (expr)` — the stored predicate
    /// is just the bare inner expr (table-level path uses `expr.to_string()`),
    /// so the wrapper is not present and strip is a no-op.
    #[test]
    fn strip_bare_named_constraint_predicate_is_noop() {
        assert_eq!(
            strip_check_wrapper("end_day >= start_day"),
            "end_day >= start_day"
        );
    }

    /// If `CHECK` appears but is not followed by `(`, treat as bare expression.
    #[test]
    fn strip_check_without_paren_is_noop() {
        // Pathological but must not panic.
        assert_eq!(strip_check_wrapper("CHECK_COLUMN > 0"), "CHECK_COLUMN > 0");
    }

    /// Idempotent: stripping twice gives the same result as stripping once.
    #[test]
    fn strip_check_wrapper_idempotent() {
        let stored = "CHECK (score >= 0)";
        let once = strip_check_wrapper(stored);
        let twice = strip_check_wrapper(once);
        assert_eq!(once, twice);
        assert_eq!(once, "score >= 0");
    }
}
