//! `COPY … FROM STDIN` engine-internal ingest path.
//!
//! Converts pre-parsed CSV rows (`Vec<Option<String>>`) directly into an Arrow
//! [`RecordBatch`] by type-coercing each field from its text representation,
//! then routes the batch through `crate::executor::exec_ingest_batch` which
//! runs the same post-batch pipeline as `exec_insert` (constraints, write,
//! commit, refresh).  The SQL-parse layer is bypassed entirely.
//!
//! ## Why this exists
//!
//! `insert_write_tax_probe` showed that the existing `COPY FROM STDIN` path
//! re-builds a `INSERT INTO t VALUES (…)` string per row and calls
//! `session.execute()`, paying full SQL-parse + single-row Arrow-build cost
//! for every row (~57 µs/row at 200 k rows).  This module is the fix:
//! parse CSV once in the pgwire framing layer, accumulate rows into a batch,
//! then call `exec_ingest_batch` once per batch.
//!
//! ## Scope (first increment)
//!
//! - TEXT / CSV format; any single-character delimiter (resolved by caller).
//! - All Arrow column types that `batch_from_rows` handles: Int16/32/64,
//!   Float32/64, Boolean, Timestamp(µs), Binary, LargeBinary (JSONB),
//!   FixedSizeBinary(16) (UUID), FixedSizeList<f32> (vector), Decimal128,
//!   Utf8 (TEXT, VARCHAR, INET, CIDR, MACADDR, BIT, VARBIT, TSVECTOR, TSQUERY).
//! - Full-width COPY (no column list, or column list == all columns):
//!   zero-overhead for the common case.
//! - Column-list COPY: unlisted columns are set to NULL; `apply_column_defaults`
//!   fills them in via the engine's DEFAULT evaluator.
//! - Constraints (CHECK, PK, FK, RLS WITH CHECK) run identically to INSERT.
//! - Shard + synchronous-Parquet write paths both handled.
//!
//! ## Not in scope (follow-up items)
//!
//! 1. Partitioned tables — returns `Err(FeatureNotSupported)` so the caller
//!    can fall back to the per-row INSERT path.
//! 2. BINARY format — not implemented; callers must reject before reaching here.
//! 3. RETURNING clause — not applicable to the COPY protocol.
//! 4. Generated columns with complex expressions — `materialise_generated_columns`
//!    handles them as it does for INSERT.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::types::Float32Type;
use arrow_array::{ArrayRef, Decimal128Array, FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, Result, TableName};

use crate::types::{
    bit_fixed_len, field_is_bit, field_is_cidr, field_is_inet, field_is_jsonb, field_is_macaddr,
    field_is_macaddr8, field_is_money, field_is_tsquery, field_is_tsvector, field_is_uuid,
    field_is_varbit, varbit_max_len,
};

// ── Text-to-Arrow coercers (string → typed value) ────────────────────────────

/// Check whether inserting NULL into `field` is allowed.
///
/// When the column has a `DEFAULT` expression (indicated by `BASIN_COLUMN_DEFAULT`
/// metadata), we skip the NOT NULL check here: the default-fill pass will
/// overwrite the NULL slot before the batch is written.  This matches the
/// `exec_insert` behaviour where `expand_insert_rows` puts `Expr::Null` for
/// unlisted columns and `apply_column_defaults` fills them in before
/// `batch_from_rows` is called.
fn null_check(field: &Field) -> Result<()> {
    if !field.is_nullable() {
        // Column has a DEFAULT → the null slot will be replaced by the default.
        if crate::types::field_default_text(field).is_some() {
            return Ok(());
        }
        return Err(BasinError::InvalidSchema(format!(
            "NULL in NOT NULL column {}",
            field.name()
        )));
    }
    Ok(())
}

fn parse_i64(s: &str, col: &str) -> Result<i64> {
    s.trim().parse::<i64>().map_err(|_| {
        BasinError::InvalidSchema(format!("invalid integer {:?} for column {col}", s.trim()))
    })
}

fn parse_f64(s: &str, col: &str) -> Result<f64> {
    s.trim().parse::<f64>().map_err(|_| {
        BasinError::InvalidSchema(format!("invalid float {:?} for column {col}", s.trim()))
    })
}

fn parse_bool(s: &str, col: &str) -> Result<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "yes" | "y" | "on" => Ok(true),
        "f" | "false" | "0" | "no" | "n" | "off" => Ok(false),
        _ => Err(BasinError::InvalidSchema(format!(
            "invalid boolean {:?} for column {col}",
            s.trim()
        ))),
    }
}

fn parse_timestamp_micros(s: &str, col: &str) -> Result<i64> {
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
    let s = s.trim();
    // Integer (µs since epoch) — hot path for the bench shape.
    if let Ok(v) = s.parse::<i64>() {
        return Ok(v);
    }
    // RFC3339 / ISO8601 with explicit timezone.
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt.timestamp_micros());
    }
    // "YYYY-MM-DD HH:MM:SS[.ffffff]" (no zone → UTC). `%.f` matches an
    // optional fractional-seconds part — binary COPY decodes timestamps to
    // microsecond precision, so the dotted form is the common case there.
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(Utc.from_utc_datetime(&ndt).timestamp_micros());
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(Utc.from_utc_datetime(&ndt).timestamp_micros());
    }
    Err(BasinError::InvalidSchema(format!(
        "cannot parse timestamp {:?} for column {col}",
        s
    )))
}

/// Parse a `YYYY-MM-DD` cell into Arrow `Date32` (days since 1970-01-01).
fn parse_date32(s: &str, col: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").map_err(|_| {
        BasinError::InvalidSchema(format!("cannot parse date {:?} for column {col}", s.trim()))
    })?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch");
    Ok((date - epoch).num_days() as i32)
}

fn parse_uuid(s: &str, col: &str) -> Result<[u8; 16]> {
    uuid::Uuid::parse_str(s.trim())
        .map(|u| *u.as_bytes())
        .map_err(|_| {
            BasinError::InvalidSchema(format!("invalid UUID {:?} for column {col}", s.trim()))
        })
}

fn parse_bytea(s: &str, col: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    // PG hex-escape: \xDEADBEEF
    if let Some(hex) = s.strip_prefix("\\x") {
        return hex::decode(hex).map_err(|_| {
            BasinError::InvalidSchema(format!(
                "invalid bytea hex {:?} for column {col}",
                s
            ))
        });
    }
    // Raw UTF-8 bytes (test/simple use-cases).
    Ok(s.as_bytes().to_vec())
}

fn parse_jsonb(s: &str, col: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    let v: serde_json::Value = serde_json::from_str(s).map_err(|e| {
        BasinError::InvalidSchema(format!("invalid JSON for column {col}: {e}"))
    })?;
    serde_json::to_vec(&v)
        .map_err(|e| BasinError::internal(format!("jsonb re-serialise: {e}")))
}

fn parse_decimal128(s: &str, precision: u8, scale: i8, col: &str) -> Result<i128> {
    // Simple f64 → scaled i128 coercion.  Accurate enough for CSV ingest of
    // NUMERIC values up to ~15 significant digits.
    let s = s.trim();
    let v: f64 = s.parse().map_err(|_| {
        BasinError::InvalidSchema(format!("invalid NUMERIC {:?} for column {col}", s))
    })?;
    let factor = 10f64.powi(scale as i32);
    let scaled = (v * factor).round();
    if scaled < i128::MIN as f64 || scaled > i128::MAX as f64 {
        return Err(BasinError::InvalidSchema(format!(
            "NUMERIC {:?} out of range for column {col} (precision={precision}, scale={scale})",
            s
        )));
    }
    Ok(scaled as i128)
}

// ── UTF-8 semantic validation ─────────────────────────────────────────────────

/// Light-weight format validation for Utf8 fields that carry a semantic type
/// (INET, BIT, VARBIT). Everything else passes through unchanged; the engine
/// stores them as plain text and semantic validation is deferred to query time
/// (consistent with the INSERT path for CIDR, MACADDR, TSVECTOR, TSQUERY).
fn validate_utf8_cell(s: &str, field: &Field) -> Result<()> {
    if field_is_inet(field) {
        // Accept either bare IP ("192.168.1.1") or CIDR-notation ("192.168.1.0/24").
        let ip_part = s.split('/').next().unwrap_or(s);
        ip_part
            .parse::<std::net::IpAddr>()
            .map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "invalid INET address {:?} for column {}",
                    s,
                    field.name()
                ))
            })?;
    }
    if field_is_bit(field) {
        let n = bit_fixed_len(field) as usize;
        if s.len() != n || !s.chars().all(|c| c == '0' || c == '1') {
            return Err(BasinError::InvalidSchema(format!(
                "invalid BIT({n}) value {:?} for column {}",
                s,
                field.name()
            )));
        }
    }
    if field_is_varbit(field) {
        let max_n = varbit_max_len(field);
        if let Some(max) = max_n {
            if s.len() > max as usize {
                return Err(BasinError::InvalidSchema(format!(
                    "VARBIT({max}) value too long ({} bits) for column {}",
                    s.len(),
                    field.name()
                )));
            }
        }
        if !s.chars().all(|c| c == '0' || c == '1') {
            return Err(BasinError::InvalidSchema(format!(
                "invalid VARBIT value {:?} for column {}",
                s,
                field.name()
            )));
        }
    }
    // Suppress unused-variable warnings for the type-check predicates we don't
    // act on (CIDR, MACADDR, MACADDR8, TSVECTOR, TSQUERY are accepted as-is).
    let _ = (
        field_is_cidr(field),
        field_is_macaddr(field),
        field_is_macaddr8(field),
        field_is_tsvector(field),
        field_is_tsquery(field),
    );
    Ok(())
}

// ── Main batch builder ────────────────────────────────────────────────────────

/// Build a [`RecordBatch`] matching `schema` from pre-parsed CSV rows.
///
/// `rows[i][j]` is the value for row `i`, column `j` in `schema` order.
/// `None` = NULL cell.  No SQL parsing.  All type coercion is done from the
/// string representation, which is typically 30–50× faster than the INSERT
/// VALUES path for bulk ingest (avoids sqlparser AST construction + eval).
pub(crate) fn batch_from_csv_rows(
    schema: Arc<Schema>,
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(BasinError::InvalidSchema(
            "COPY: at least one row is required".into(),
        ));
    }
    let n_cols = schema.fields().len();
    for (i, row) in rows.iter().enumerate() {
        if row.len() != n_cols {
            return Err(BasinError::InvalidSchema(format!(
                "COPY row {i}: {} columns, expected {n_cols}",
                row.len()
            )));
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col: ArrayRef = match field.data_type() {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_i64(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => {
                            let v = parse_i64(s, field.name())?;
                            let v32 = i32::try_from(v).map_err(|_| {
                                BasinError::InvalidSchema(format!(
                                    "integer out of range for INT4 column {}: {v}",
                                    field.name()
                                ))
                            })?;
                            b.append_value(v32);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int16 => {
                let mut b = Int16Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => {
                            let v = parse_i64(s, field.name())?;
                            let v16 = i16::try_from(v).map_err(|_| {
                                BasinError::InvalidSchema(format!(
                                    "integer out of range for INT2 column {}: {v}",
                                    field.name()
                                ))
                            })?;
                            b.append_value(v16);
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_f64(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_f64(s, field.name())? as f32),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_bool(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len())
                    .with_data_type(field.data_type().clone());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_timestamp_micros(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Date32 => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(parse_date32(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(&parse_bytea(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary if field_is_jsonb(field) => {
                let mut b = LargeBinaryBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(&parse_jsonb(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary => {
                let mut b = LargeBinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => b.append_value(&parse_bytea(s, field.name())?),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeBinary(16) if field_is_uuid(field) => {
                let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => {
                            let bytes = parse_uuid(s, field.name())?;
                            b.append_value(bytes).map_err(|e| {
                                BasinError::internal(format!(
                                    "UUID append for column {}: {e}",
                                    field.name()
                                ))
                            })?;
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeList(child, dim) => {
                if *child.data_type() != DataType::Float32 {
                    return Err(BasinError::InvalidSchema(format!(
                        "COPY: only FixedSizeList<Float32> (vector) is supported, \
                         got child {:?} for column {}",
                        child.data_type(),
                        field.name()
                    )));
                }
                let dim_usize = *dim as usize;
                let mut row_iter: Vec<Option<Vec<Option<f32>>>> = Vec::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; row_iter.push(None); }
                        Some(s) => {
                            let v = crate::types::parse_vector_literal(s)?;
                            if v.len() != dim_usize {
                                return Err(BasinError::InvalidSchema(format!(
                                    "COPY: vector length {} != column {} dim {dim_usize}",
                                    v.len(),
                                    field.name()
                                )));
                            }
                            row_iter.push(Some(v.into_iter().map(Some).collect()));
                        }
                    }
                }
                Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    row_iter, *dim,
                ))
            }
            DataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                let is_money = field_is_money(field);
                let mut values: Vec<Option<i128>> = Vec::with_capacity(rows.len());
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; values.push(None); }
                        Some(text) => {
                            let stripped = if is_money {
                                text.trim().trim_start_matches(|c: char| {
                                    !c.is_ascii_digit() && c != '-' && c != '.'
                                })
                            } else {
                                text.trim()
                            };
                            values.push(Some(parse_decimal128(stripped, p, s, field.name())?));
                        }
                    }
                }
                let arr = Decimal128Array::from(values)
                    .with_precision_and_scale(p, s)
                    .map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "Decimal128({p},{s}) for column {}: {e}",
                            field.name()
                        ))
                    })?;
                Arc::new(arr)
            }
            // Utf8 covers: TEXT, VARCHAR, CHAR, INET, CIDR, MACADDR, MACADDR8,
            // BIT(n), VARBIT, TSVECTOR, TSQUERY — all stored as plain Utf8.
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match &row[col_idx] {
                        None => { null_check(field)?; b.append_null(); }
                        Some(s) => {
                            validate_utf8_cell(s, field)?;
                            b.append_value(s);
                        }
                    }
                }
                let arr = Arc::new(b.finish());
                // VARCHAR(n) / CHAR(n) length enforcement.
                crate::dml::enforce_charlen_array(field, arr)?
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "COPY: unsupported column type {:?} for column {}",
                    other,
                    field.name()
                )));
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| BasinError::internal(format!("RecordBatch build (COPY): {e}")))
}

// ── Schema helpers ────────────────────────────────────────────────────────────

/// Return a copy of `schema` where every NOT-NULL field that has a DEFAULT
/// expression is temporarily marked nullable.  Used when building a batch for
/// a column-list COPY so that Arrow's RecordBatch validator doesn't reject
/// the NULL placeholders for defaultable columns before we fill them in.
fn make_nullable_schema_for_defaultable(schema: &Schema) -> Arc<Schema> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if !f.is_nullable() && crate::types::field_default_text(f).is_some() {
                // Make a nullable copy so Arrow accepts the null slot.
                f.as_ref().clone().with_nullable(true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

// ── Column-list expansion ─────────────────────────────────────────────────────

/// Expand column-list rows to full-schema width.
///
/// Given CSV rows whose columns match `column_names` (in that order), produces
/// full-schema rows where unlisted columns are `None` (NULL).  The engine's
/// `apply_column_defaults` / `materialise_generated_columns` calls will fill
/// DEFAULT and GENERATED values in after the batch is built.
fn expand_csv_rows_to_full_schema(
    table: &str,
    full_schema: &Schema,
    column_names: &[String],
    rows: Vec<Vec<Option<String>>>,
) -> Result<Vec<Vec<Option<String>>>> {
    let indices: Vec<usize> = column_names
        .iter()
        .map(|n| {
            full_schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(n))
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "COPY: column {n:?} does not exist on table {table:?}"
                    ))
                })
        })
        .collect::<Result<_>>()?;

    let n_full = full_schema.fields().len();
    let mut expanded = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != column_names.len() {
            return Err(BasinError::InvalidSchema(format!(
                "COPY: row has {} columns, column list has {}",
                row.len(),
                column_names.len()
            )));
        }
        let mut full: Vec<Option<String>> = vec![None; n_full];
        for (list_idx, &schema_idx) in indices.iter().enumerate() {
            full[schema_idx] = row[list_idx].clone();
        }
        expanded.push(full);
    }
    Ok(expanded)
}

// ── Public ingest entry point ─────────────────────────────────────────────────

/// Ingest a pre-parsed batch of CSV rows into `table_name`, bypassing SQL parse.
///
/// `full_schema` is the full table schema (all columns).  `column_names` is
/// the ordered column list from the COPY statement (`None` = all columns in
/// schema order).  `rows` has one entry per CSV row; each entry has one
/// `Option<String>` per column in `column_names` order (or full schema order
/// when `column_names` is `None`).
///
/// Returns the number of rows ingested.  On `BasinError::FeatureNotSupported`
/// (partitioned table) the caller should fall back to the per-row INSERT path.
pub(crate) async fn exec_copy_from_batch(
    sess: &crate::ProjectSession,
    table_name: &str,
    full_schema: Arc<Schema>,
    column_names: Option<&[String]>,
    rows: Vec<Vec<Option<String>>>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Quick check: partitioned tables are not yet supported in this increment.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &TableName::new(table_name.to_owned())?)
        .await?;
    if !matches!(meta.partition_spec, PartitionSpec::Unpartitioned) {
        return Err(BasinError::FeatureNotSupported(
            "COPY fast-path: partitioned tables not yet supported; \
             use INSERT INTO … VALUES … instead"
                .into(),
        ));
    }

    // Use the catalog's authoritative schema rather than the one passed by the
    // router. The router derives its schema from a `SELECT * LIMIT 0` prepare,
    // and DataFusion strips per-field metadata off projection output — so
    // `BASIN_COLUMN_DEFAULT` (the `nextval(...)` expression behind SERIAL and
    // any user DEFAULT) is invisible there. Without it the default-fill pass
    // below silently no-ops and an omitted NOT-NULL DEFAULT column (e.g. a
    // SERIAL primary key on a column-list COPY) wrongly fails the null check.
    // `meta.schema` comes straight from the catalog and keeps that metadata.
    let full_schema = meta.schema.clone();

    // Expand column-list rows to full-schema width (no-op when no column list).
    let full_rows = match column_names {
        None => rows,
        Some(names) => expand_csv_rows_to_full_schema(table_name, &full_schema, names, rows)?,
    };

    // When a column list was given, some NOT-NULL columns may be NULL at this
    // point (they'll be filled by DEFAULT later).  Build the batch using a
    // temporarily-nullable schema so Arrow's RecordBatch::try_new doesn't
    // reject null slots for those columns.
    let build_schema = if column_names.is_some() {
        make_nullable_schema_for_defaultable(&full_schema)
    } else {
        full_schema.clone()
    };

    // Build the RecordBatch from CSV text.
    let batch = batch_from_csv_rows(build_schema, &full_rows)?;

    // Apply column defaults for unlisted columns when a column list was given.
    let batch = if let Some(names) = column_names {
        apply_column_defaults_to_batch(sess, full_schema.clone(), names, batch).await?
    } else {
        batch
    };

    let table = TableName::new(table_name.to_owned())?;
    crate::executor::exec_ingest_batch(sess, &table, batch).await
}

/// Apply column defaults for any unlisted columns.
///
/// Builds one NULL-initialised Expr row per batch row, runs
/// `apply_column_defaults` on it (which fills in DEFAULT expressions like
/// `nextval(...)`), then splices the resulting values back into the batch.
///
/// Columns that were explicitly supplied in the COPY column list are left
/// unchanged (they already carry the CSV value).
async fn apply_column_defaults_to_batch(
    sess: &crate::ProjectSession,
    schema: Arc<Schema>,
    column_names: &[String],
    batch: RecordBatch,
) -> Result<RecordBatch> {
    use arrow_array::Array;

    let n_rows = batch.num_rows();
    let n_cols = schema.fields().len();

    // Index of listed columns in full schema (already validated in expand_csv_rows_to_full_schema).
    let supplied_set: std::collections::HashSet<String> = column_names
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();

    // Build null Expr rows.  apply_column_defaults fills DEFAULT positions;
    // other positions are ignored (we only care about the result for unlisted
    // DEFAULT columns).
    let null_expr = || -> sqlparser::ast::Expr {
        sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Null,
            span: sqlparser::tokenizer::Span::empty(),
        })
    };
    let col_idents: Vec<sqlparser::ast::Ident> = column_names
        .iter()
        .map(|n| sqlparser::ast::Ident::new(n.clone()))
        .collect();
    let mut null_rows: Vec<Vec<sqlparser::ast::Expr>> =
        vec![vec![null_expr(); n_cols]; n_rows];

    // apply_column_defaults is private to executor.rs; call via the public
    // wrapper added in executor.rs for this purpose.
    crate::executor::apply_column_defaults_pub(
        sess,
        schema.as_ref(),
        &col_idents,
        &mut null_rows,
    )
    .await?;

    // Splice defaults back: for unlisted columns that had a DEFAULT,
    // build a single-column batch from the filled Expr values.
    let mut new_columns: Vec<std::sync::Arc<dyn arrow_array::Array>> =
        Vec::with_capacity(n_cols);
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col_name_lower = field.name().to_ascii_lowercase();
        if supplied_set.contains(&col_name_lower) {
            // Supplied by COPY data — use as-is.
            new_columns.push(batch.column(col_idx).clone());
        } else {
            // Potentially filled by apply_column_defaults.
            let col_rows: Vec<Vec<sqlparser::ast::Expr>> = null_rows
                .iter()
                .map(|row| vec![row[col_idx].clone()])
                .collect();
            let single_schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
            let col_batch = crate::dml::batch_from_rows(single_schema, &col_rows)?;
            new_columns.push(col_batch.column(0).clone());
        }
    }

    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| BasinError::internal(format!("RecordBatch splice (COPY defaults): {e}")))
}
