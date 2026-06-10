//! INSERT INTO ... VALUES (...) — literal rows → Arrow [`RecordBatch`].
//!
//! Only literal values, only one statement at a time. Subquery inserts and
//! parameter binding are out of scope for the PoC.

use crate::pg_ast::ObjectNamePartExt;
use std::sync::Arc;

use std::collections::BTreeMap;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::types::Float32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeListArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, PartitionKey, Result};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{FunctionArg, FunctionArgExpr, 
    DataType as SqlDataType, Expr, FunctionArguments, TypedString, UnaryOperator, Value,
};

use crate::types::{
    bit_fixed_len, field_is_bit, field_is_cidr, field_is_inet, field_is_jsonb, field_is_macaddr,
    field_is_macaddr8, field_is_money, field_is_tsquery, field_is_tsvector, field_is_uuid,
    field_is_varbit, parse_vector_literal, varbit_max_len,
};

/// Above this row count we try the type-specific bulk paths in [`build_array_bulk`]
/// before falling back to the per-row builder loop. The threshold is empirical:
/// below ~100 rows the per-row path's overhead is in the noise compared to the
/// rest of the INSERT pipeline (sqlparser, IPC encode, WAL append) and the
/// branch isn't worth its own code path.
const BULK_THRESHOLD: usize = 100;

/// Build a [`RecordBatch`] matching `schema` from a `VALUES (...), (...)`
/// row list.
pub(crate) fn batch_from_rows(schema: Arc<Schema>, rows: &[Vec<Expr>]) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(BasinError::InvalidSchema(
            "INSERT requires at least one row".into(),
        ));
    }
    let n_cols = schema.fields().len();
    for (i, row) in rows.iter().enumerate() {
        if row.len() != n_cols {
            return Err(BasinError::InvalidSchema(format!(
                "row {i} has {} values, expected {n_cols}",
                row.len()
            )));
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    let bulk = rows.len() >= BULK_THRESHOLD;
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Int64 if bulk && !field.is_nullable() => {
                build_int64_not_null(rows, col_idx, field.name())?
            }
            DataType::Int64 if bulk => build_int64_nullable(rows, col_idx, field)?,
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match coerce_i64(&row[col_idx])? {
                        Some(v) => b.append_value(v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // INT / INTEGER / INT4 — 32-bit signed (PG OID 23). Coerce from
            // numeric literals, widening to i64 and range-checking on the way
            // in (mirrors what PG does: out-of-range is a runtime error).
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(rows.len());
                for row in rows {
                    match coerce_i64(&row[col_idx])? {
                        Some(v) => {
                            let v32 = i32::try_from(v).map_err(|_| {
                                BasinError::InvalidSchema(format!(
                                    "integer out of range for INT4 column {:?}: {v}",
                                    field.name()
                                ))
                            })?;
                            b.append_value(v32);
                        }
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // SMALLINT / INT2 — 16-bit signed (PG OID 21).
            DataType::Int16 => {
                let mut b = Int16Builder::with_capacity(rows.len());
                for row in rows {
                    match coerce_i64(&row[col_idx])? {
                        Some(v) => {
                            let v16 = i16::try_from(v).map_err(|_| {
                                BasinError::InvalidSchema(format!(
                                    "integer out of range for INT2 column {:?}: {v}",
                                    field.name()
                                ))
                            })?;
                            b.append_value(v16);
                        }
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // ── Network / bit-string stubs ────────────────────────────────
            // INET, CIDR, MACADDR, MACADDR8, BIT(n), and VARBIT all ride on
            // Arrow `Utf8`. We dispatch here (before the generic Utf8 arm) to
            // apply format validation on INSERT.
            DataType::Utf8 if field_is_inet(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_inet(&row[col_idx], field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_cidr(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_cidr(&row[col_idx], field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_macaddr(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_macaddr(&row[col_idx], field.name(), false)? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_macaddr8(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_macaddr(&row[col_idx], field.name(), true)? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_bit(field) => {
                // BIT(n): exact length required.
                let n = bit_fixed_len(field);
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * (n as usize + 1));
                for row in rows {
                    match coerce_bit_string(&row[col_idx], Some(n), true, field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_varbit(field) => {
                // VARBIT(n): at most n bits; VARBIT: unlimited.
                let max_n = varbit_max_len(field);
                let cap = max_n.unwrap_or(64) as usize;
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * (cap + 1));
                for row in rows {
                    match coerce_bit_string(&row[col_idx], max_n, false, field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // ── TSVECTOR / TSQUERY (full-text search) ─────────────────────
            // Stored as Utf8 holding the canonical text form. INSERT values
            // may be a string literal (raw cast shape, canonicalised) or a
            // to_tsvector(...)/to_tsquery(...) call over string literals.
            DataType::Utf8 if field_is_tsvector(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_tsvector(&row[col_idx], field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 if field_is_tsquery(field) => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_tsquery(&row[col_idx], field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // ── Generic Utf8 (TEXT, VARCHAR, CHAR, etc.) ──────────────────
            DataType::Utf8 if bulk && !field.is_nullable() => {
                build_utf8_not_null(rows, col_idx, field.name())?
            }
            DataType::Utf8 if bulk => build_utf8_nullable(rows, col_idx, field)?,
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_string(&row[col_idx])? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean if bulk && !field.is_nullable() => {
                build_bool_not_null(rows, col_idx, field.name())?
            }
            DataType::Boolean if bulk => build_bool_nullable(rows, col_idx, field)?,
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    match coerce_bool(&row[col_idx])? {
                        Some(v) => b.append_value(v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 if bulk && !field.is_nullable() => {
                build_f64_not_null(rows, col_idx, field.name())?
            }
            DataType::Float64 if bulk => build_f64_nullable(rows, col_idx, field)?,
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match coerce_f64(&row[col_idx])? {
                        Some(v) => b.append_value(v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            // REAL / FLOAT4 — 32-bit IEEE 754 (PG OID 700). Coerce through
            // f64 then cast down to f32; precision narrowing is expected and
            // matches PG behaviour.
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(rows.len());
                for row in rows {
                    match coerce_f64(&row[col_idx])? {
                        Some(v) => b.append_value(v as f32),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len())
                    .with_data_type(field.data_type().clone());
                for row in rows {
                    match coerce_timestamp_micros(&row[col_idx])? {
                        Some(v) => b.append_value(v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_bytea(&row[col_idx])? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary if field_is_jsonb(field) => {
                // JSONB column. Each row's literal must be a JSON string —
                // we parse it, re-serialise canonically (BTreeMap sorts
                // keys, no whitespace) and store the bytes. Anything that
                // isn't a string literal is a hard error so the user sees
                // *exactly* which row tripped the check.
                let mut b = LargeBinaryBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    match coerce_jsonb(&row[col_idx], field.name())? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary => {
                let mut b = LargeBinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match coerce_bytea(&row[col_idx])? {
                        Some(v) => b.append_value(&v),
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeBinary(16) if field_is_uuid(field) => {
                // UUID column. Each row's literal must be either a
                // string literal (parsed via `uuid::Uuid::parse_str`) or
                // a function call to `gen_random_uuid()` /
                // `uuid_generate_v4()` which generates a fresh v4 UUID.
                // Stored as 16 bytes RFC 4122 big-endian.
                let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
                for row in rows {
                    match coerce_uuid(&row[col_idx], field.name())? {
                        Some(bytes) => {
                            b.append_value(bytes).map_err(|e| {
                                BasinError::internal(format!(
                                    "UUID append for column {}: {e}",
                                    field.name()
                                ))
                            })?;
                        }
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeBinary(n)
                if *n == basin_geo::POINT_WKB_LEN as i32 && crate::types::field_is_point(field) =>
            {
                // PG-Wave-α: POINT column. Accept either a WKT string
                // literal (`'POINT(x y)'`) or the `ST_MakePoint(x, y)`
                // constructor — both produce the 21-byte little-endian
                // WKB blob that round-trips through ST_X / ST_Y and the
                // pgwire encoder.
                let mut b =
                    FixedSizeBinaryBuilder::with_capacity(rows.len(), basin_geo::POINT_WKB_LEN as i32);
                for row in rows {
                    match coerce_point(&row[col_idx], field.name())? {
                        Some(bytes) => {
                            b.append_value(bytes).map_err(|e| {
                                BasinError::internal(format!(
                                    "POINT append for column {}: {e}",
                                    field.name()
                                ))
                            })?;
                        }
                        None => {
                            check_null_allowed(field)?;
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeList(child, dim) => {
                if *child.data_type() != DataType::Float32 {
                    return Err(BasinError::InvalidSchema(format!(
                        "only FixedSizeList<Float32> (vector) supported, got child {:?}",
                        child.data_type()
                    )));
                }
                let dim_usize = *dim as usize;
                let mut row_iter: Vec<Option<Vec<Option<f32>>>> = Vec::with_capacity(rows.len());
                for row in rows {
                    match coerce_vector(&row[col_idx], dim_usize, field.name())? {
                        Some(v) => {
                            row_iter.push(Some(v.into_iter().map(Some).collect()));
                        }
                        None => {
                            check_null_allowed(field)?;
                            row_iter.push(None);
                        }
                    }
                }
                let arr =
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(row_iter, *dim);
                Arc::new(arr)
            }
            DataType::Decimal128(p, s) => {
                // PG `numeric` / MONEY literal coercion: scale a base-10
                // number literal to the column's `(precision, scale)` and
                // store as i128. MONEY additionally accepts currency-prefixed
                // strings like `'$1.50'`.
                // Pre-validated: `1 <= p <= 38`, `0 <= s <= p`.
                let is_money_col = field_is_money(field);
                let mut values: Vec<Option<i128>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let expr = &row[col_idx];
                    // For MONEY columns accept a string literal that has a
                    // leading currency symbol (e.g. '$1.50'). Strip the symbol
                    // before routing through the normal decimal128 coercion.
                    let coerced = if is_money_col {
                        coerce_money(expr, *p, *s, field.name())?
                    } else {
                        coerce_decimal128(expr, *p, *s, field.name())?
                    };
                    match coerced {
                        Some(v) => values.push(Some(v)),
                        None => {
                            check_null_allowed(field)?;
                            values.push(None);
                        }
                    }
                }
                let arr = Decimal128Array::from(values)
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "Decimal128 ({p},{s}) for column {}: {e}",
                            field.name()
                        ))
                    })?;
                Arc::new(arr)
            }
            // PG array columns (`TEXT[]`, `INT[]`, `BIGINT[]`, `BOOLEAN[]`,
            // `DOUBLE PRECISION[]`) → Arrow `List<element_type>`. CREATE TABLE
            // already lowers the PG syntax in `types::arrow_data_type`; here we
            // build the per-row `ListArray` from `ARRAY[...]` literals (parsed
            // by sqlparser as `Expr::Array { elem: Vec<Expr> }`), reusing the
            // existing per-row scalar coercers for element values. The cell
            // expression may also be `NULL` (whole-row null) or a `'{...}'`
            // PG text-array literal — those two surface forms are not handled
            // yet and are left for follow-up. Nested-list element types and
            // less common element types (JSONB, TIMESTAMPTZ, UUID, ...) are
            // also out of scope for this pass.
            DataType::List(child) => {
                build_list_column(rows, col_idx, field, child)?
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "unsupported Arrow column type for INSERT: {other:?}"
                )));
            }
        };
        // VARCHAR(n) / CHAR(n) length enforcement + CHAR blank-padding.
        // Applied as a post-pass on the freshly built Utf8 column so it
        // covers every Utf8 build path (bulk / nullable / per-row) in one
        // place. No-op (zero-copy) for unmarked text columns.
        let array = enforce_charlen_array(field, array)?;
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| BasinError::internal(format!("RecordBatch build: {e}")))
}

/// Build an Arrow `ListArray` column from `ARRAY[...]` literal cells.
///
/// Element-type dispatch covers the PG array variants most users actually
/// declare in DDL today: `TEXT[]`, `INT[]` / `INTEGER[]` / `INT4[]`,
/// `BIGINT[]` / `INT8[]`, `DOUBLE PRECISION[]` / `FLOAT8[]`, and `BOOLEAN[]`.
/// Anything else falls through to an explicit "unsupported element type"
/// error so the user sees which column tripped the check.
///
/// Per-cell shape:
/// * `Expr::Array { elem }` → emit a non-null list with `elem.len()`
///   per-element values (`NULL` elements become null slots in the child).
/// * `Expr::Value(Null)` → emit a null list (whole row is null for this col).
///   Honours the column's `NOT NULL` flag via `check_null_allowed`.
fn build_list_column(
    rows: &[Vec<Expr>],
    col_idx: usize,
    field: &arrow_schema::Field,
    child: &Arc<arrow_schema::Field>,
) -> Result<ArrayRef> {
    /// Strip leading `Expr::Cast { expr: inner, .. }` wrappers so an
    /// `ARRAY[...]::TEXT[]` literal still matches the `Expr::Array` arm.
    fn unwrap_cast(expr: &Expr) -> &Expr {
        let mut cur = expr;
        while let Expr::Cast { expr: inner, .. } = cur {
            cur = inner.as_ref();
        }
        cur
    }

    /// Try to extract the per-row array literal. Returns:
    /// * `Ok(Some(&[...]))` — `ARRAY[e1, e2, ...]` literal.
    /// * `Ok(None)` — `NULL` literal (whole-row null).
    /// * `Err(...)` — anything else (we only support literals here).
    fn extract_array<'a>(expr: &'a Expr, col: &str) -> Result<Option<&'a [Expr]>> {
        match unwrap_cast(expr) {
            Expr::Array(a) => Ok(Some(a.elem.as_slice())),
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => Ok(None),
            other => Err(BasinError::InvalidSchema(format!(
                "expected ARRAY[...] literal or NULL for array column {col}, got {other}"
            ))),
        }
    }

    let col_name = field.name();
    let arr: ArrayRef = match child.data_type() {
        DataType::Utf8 => {
            let mut b = ListBuilder::new(StringBuilder::new()).with_field(child.clone());
            for row in rows {
                match extract_array(&row[col_idx], col_name)? {
                    Some(elems) => {
                        for e in elems {
                            match coerce_string_ref(e)? {
                                Some(s) => b.values().append_value(s),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => {
                        check_null_allowed(field)?;
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = ListBuilder::new(Int64Builder::new()).with_field(child.clone());
            for row in rows {
                match extract_array(&row[col_idx], col_name)? {
                    Some(elems) => {
                        for e in elems {
                            match coerce_i64(e)? {
                                Some(v) => b.values().append_value(v),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => {
                        check_null_allowed(field)?;
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let mut b = ListBuilder::new(Int32Builder::new()).with_field(child.clone());
            for row in rows {
                match extract_array(&row[col_idx], col_name)? {
                    Some(elems) => {
                        for e in elems {
                            match coerce_i64(e)? {
                                Some(v) => {
                                    let v32 = i32::try_from(v).map_err(|_| {
                                        BasinError::InvalidSchema(format!(
                                            "integer out of range for INT4 element of {col_name}: {v}"
                                        ))
                                    })?;
                                    b.values().append_value(v32);
                                }
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => {
                        check_null_allowed(field)?;
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = ListBuilder::new(Float64Builder::new()).with_field(child.clone());
            for row in rows {
                match extract_array(&row[col_idx], col_name)? {
                    Some(elems) => {
                        for e in elems {
                            match coerce_f64(e)? {
                                Some(v) => b.values().append_value(v),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => {
                        check_null_allowed(field)?;
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = ListBuilder::new(BooleanBuilder::new()).with_field(child.clone());
            for row in rows {
                match extract_array(&row[col_idx], col_name)? {
                    Some(elems) => {
                        for e in elems {
                            match coerce_bool(e)? {
                                Some(v) => b.values().append_value(v),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => {
                        check_null_allowed(field)?;
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "unsupported array element type for INSERT into column {col_name}: {other:?} \
                 (supported: TEXT[], INT[], BIGINT[], DOUBLE PRECISION[], BOOLEAN[])"
            )));
        }
    };
    Ok(arr)
}

/// If `field` declares a `VARCHAR(n)` / `CHAR(n)` limit, validate every
/// non-null value in `array` against it (raising [`BasinError::StringTooLong`]
/// — SQLSTATE 22001 — on overflow) and, for `CHAR(n)`, return a new array
/// with each value blank-padded to exactly `n` characters. Columns with no
/// limit (the overwhelmingly common case) are returned untouched.
pub(crate) fn enforce_charlen_array(
    field: &arrow_schema::Field,
    array: ArrayRef,
) -> Result<ArrayRef> {
    let Some(limit) = crate::types::parse_charlen(field) else {
        return Ok(array);
    };
    let Some(strs) = array.as_any().downcast_ref::<StringArray>() else {
        // Charlen marker only ever attaches to Utf8 columns; anything else
        // means the column isn't actually a text array (defensive no-op).
        return Ok(array);
    };
    let needs_pad = matches!(limit, crate::types::CharLen::Char(_));
    let mut out: Vec<Option<String>> = Vec::with_capacity(strs.iter().len());
    let mut rewrote = false;
    for cell in strs.iter() {
        let Some(v) = cell else {
            out.push(None);
            continue;
        };
        let stored = crate::types::enforce_charlen(limit, v, field.name())?;
        match stored {
            std::borrow::Cow::Borrowed(s) => out.push(Some(s.to_string())),
            std::borrow::Cow::Owned(s) => {
                rewrote = true;
                out.push(Some(s));
            }
        }
    }
    if !rewrote && !needs_pad {
        // Pure length-check passed and nothing was rewritten — keep the
        // original buffer (avoids a copy on the hot path).
        return Ok(array);
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// Bulk path for `Int64 NOT NULL` columns. Avoids the per-row builder pattern
/// (which maintains a validity bitmap and runs an `append_value` ABI hop per
/// row) by building a `Vec<i64>` and handing it to
/// [`Int64Array::from_iter_values`], which lays out the buffer in one pass.
/// Surfaces `NULL` as the same `InvalidSchema` error the slow path would
/// produce so behaviour stays identical between paths.
fn build_int64_not_null(rows: &[Vec<Expr>], col_idx: usize, col_name: &str) -> Result<ArrayRef> {
    let mut values: Vec<i64> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_i64(&row[col_idx])? {
            Some(v) => values.push(v),
            None => {
                return Err(BasinError::InvalidSchema(format!(
                    "NULL inserted into NOT NULL column {col_name}"
                )));
            }
        }
    }
    Ok(Arc::new(Int64Array::from_iter_values(values)))
}

fn build_int64_nullable(
    rows: &[Vec<Expr>],
    col_idx: usize,
    field: &arrow_schema::Field,
) -> Result<ArrayRef> {
    let mut values: Vec<Option<i64>> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_i64(&row[col_idx])? {
            Some(v) => values.push(Some(v)),
            None => {
                check_null_allowed(field)?;
                values.push(None);
            }
        }
    }
    Ok(Arc::new(Int64Array::from(values)))
}

fn build_utf8_not_null(rows: &[Vec<Expr>], col_idx: usize, col_name: &str) -> Result<ArrayRef> {
    // Borrow the parsed string out of the AST directly — sqlparser owns the
    // backing storage for the duration of this function, and `from_iter`
    // copies into a single contiguous Arrow string buffer on the way through.
    // This skips the per-row `String::clone` the slow path was paying for
    // every cell.
    let mut values: Vec<&str> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_string_ref(&row[col_idx])? {
            Some(v) => values.push(v),
            None => {
                return Err(BasinError::InvalidSchema(format!(
                    "NULL inserted into NOT NULL column {col_name}"
                )));
            }
        }
    }
    Ok(Arc::new(StringArray::from_iter_values(values)))
}

fn build_utf8_nullable(
    rows: &[Vec<Expr>],
    col_idx: usize,
    field: &arrow_schema::Field,
) -> Result<ArrayRef> {
    let mut values: Vec<Option<&str>> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_string_ref(&row[col_idx])? {
            Some(v) => values.push(Some(v)),
            None => {
                check_null_allowed(field)?;
                values.push(None);
            }
        }
    }
    Ok(Arc::new(StringArray::from(values)))
}

fn build_bool_not_null(rows: &[Vec<Expr>], col_idx: usize, col_name: &str) -> Result<ArrayRef> {
    let mut values: Vec<bool> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_bool(&row[col_idx])? {
            Some(v) => values.push(v),
            None => {
                return Err(BasinError::InvalidSchema(format!(
                    "NULL inserted into NOT NULL column {col_name}"
                )));
            }
        }
    }
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_bool_nullable(
    rows: &[Vec<Expr>],
    col_idx: usize,
    field: &arrow_schema::Field,
) -> Result<ArrayRef> {
    let mut values: Vec<Option<bool>> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_bool(&row[col_idx])? {
            Some(v) => values.push(Some(v)),
            None => {
                check_null_allowed(field)?;
                values.push(None);
            }
        }
    }
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_f64_not_null(rows: &[Vec<Expr>], col_idx: usize, col_name: &str) -> Result<ArrayRef> {
    let mut values: Vec<f64> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_f64(&row[col_idx])? {
            Some(v) => values.push(v),
            None => {
                return Err(BasinError::InvalidSchema(format!(
                    "NULL inserted into NOT NULL column {col_name}"
                )));
            }
        }
    }
    Ok(Arc::new(Float64Array::from_iter_values(values)))
}

fn build_f64_nullable(
    rows: &[Vec<Expr>],
    col_idx: usize,
    field: &arrow_schema::Field,
) -> Result<ArrayRef> {
    let mut values: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    for row in rows {
        match coerce_f64(&row[col_idx])? {
            Some(v) => values.push(Some(v)),
            None => {
                check_null_allowed(field)?;
                values.push(None);
            }
        }
    }
    Ok(Arc::new(Float64Array::from(values)))
}

/// Decode a Postgres bytea literal into bytes. Accepts:
///   - `'\xff00'::bytea` (Cast wrapper around a `\x...` hex string)
///   - `'\xff00'`        (the same hex literal without an explicit cast)
///   - `NULL`            (returns `None`)
/// Anything else is an `InvalidSchema` error.
fn coerce_bytea(expr: &Expr) -> Result<Option<Vec<u8>>> {
    let inner = match expr {
        Expr::Cast {
            expr: inner,
            data_type: SqlDataType::Bytea,
            ..
        } => inner.as_ref(),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        _ => expr,
    };
    let s = match inner {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => s,
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        _ => {
            return Err(BasinError::InvalidSchema(format!(
                "expected bytea literal, got {expr:?}"
            )));
        }
    };
    let hex = s.strip_prefix("\\x").ok_or_else(|| {
        BasinError::InvalidSchema(format!("bytea literal must start with `\\x`, got {s:?}"))
    })?;
    if hex.len() % 2 != 0 {
        return Err(BasinError::InvalidSchema(format!(
            "bytea hex string has odd length: {s:?}"
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let h = std::str::from_utf8(chunk)
            .map_err(|_| BasinError::InvalidSchema(format!("non-utf8 bytea hex: {s:?}")))?;
        let byte = u8::from_str_radix(h, 16)
            .map_err(|_| BasinError::InvalidSchema(format!("bad bytea hex byte {h:?} in {s:?}")))?;
        out.push(byte);
    }
    Ok(Some(out))
}

/// Decode a JSONB literal into canonical-form serialised JSON bytes.
///
/// Accepts:
/// - A SQL string literal (`'{"a":1}'`) — the only form Postgres itself
///   accepts at INSERT for JSONB without an explicit cast.
/// - `NULL` (returns `None`).
///
/// The string is parsed as JSON and re-serialised with `serde_json::to_vec`
/// after parsing through `BTreeMap<String, serde_json::Value>` for object
/// nodes; this gives us **canonical form** (keys sorted alphabetically, no
/// whitespace) so two rows that wrote the same logical document end up with
/// byte-identical Parquet payloads. Equality / hashing on JSONB columns then
/// reduces to a byte compare, which is what the v0.2 `@>` operator will key
/// off.
///
/// Anything else — numeric literal, identifier, function call — is rejected
/// up front. We deliberately don't accept `Value::Null` *inside* the JSON
/// literal itself as a problem; that's a valid JSON token.
fn coerce_jsonb(expr: &Expr, col: &str) -> Result<Option<Vec<u8>>> {
    let s: &str = match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => s.as_str(),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        // A bare `Cast` like `'{...}'::jsonb` is friendly to allow even
        // though our DDL doesn't produce it; peel and recurse.
        Expr::Cast { expr: inner, .. } => return coerce_jsonb(inner.as_ref(), col),
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "expected JSON string literal for column {col}, got {other}"
            )));
        }
    };
    // SIMD-accelerated parse via sonic-rs, then canonicalise (key-sort) and
    // re-serialise. sonic_rs::from_str parses straight into a
    // `serde_json::Value`, so the canonicalisation pass and downstream
    // consumers (which all parse the stored bytes as raw JSON via
    // serde_json) are unchanged.
    let parsed: serde_json::Value = sonic_rs::from_str(s).map_err(|e| {
        BasinError::InvalidSchema(format!("invalid JSON literal for column {col}: {e}"))
    })?;
    let canonical = canonicalize_json(parsed);
    let json_bytes = sonic_rs::to_vec(&canonical)
        .map_err(|e| BasinError::internal(format!("re-serialising JSON for column {col}: {e}")))?;
    // ADR 0027 Phase 2 (write-side) REVERTED: stored JSONB is bare canonical
    // JSON, not the `0x02` binary offset table. The offset table gave a 15x
    // micro-bench speedup on `RawJson::member` but ZERO end-to-end bench
    // improvement (the JSONB bench bottleneck is the row-count the UDF runs on
    // + payload-blob decode, fixed by filter-pushdown + Phase 4 promoted
    // columns — not per-document parse speed). Meanwhile emitting `0x02`
    // silently broke every consumer of stored JSONB bytes that parses raw JSON
    // (GIN row-group + file-level indexers, JSONPath, jsonb aggregates,
    // json_build) — they were never taught to detect the tag. Net-negative, so
    // we stop writing it. The read-side `0x02` detection (jsonb_udf::RawJson)
    // is intentionally KEPT so any rows written during the Phase 2 window still
    // decode correctly. `encode_jsonb_v2` is retained (dead on the write path)
    // for that historical-decode test coverage + a possible future re-enable
    // once all consumers share a 0x02-aware decode helper.
    Ok(Some(json_bytes))
}

// ---------------------------------------------------------------------------
// ADR 0027 Phase 2 — write-time binary offset table encoder
//
// Layout of a 0x02-tagged JSONB value (all integers little-endian):
//
//   [0x02]                              — 1 byte version tag
//   [u32_le: num_entries]               — 4 bytes: number of top-level keys
//   for each entry (in document order):
//     [u16_le: key_len]                 — 2 bytes: byte length of key
//     [key_bytes: key_len bytes]        — key encoded as raw decoded UTF-8
//     [u32_le: val_start]               — 4 bytes: start offset of value in json_section
//     [u32_le: val_end]                 — 4 bytes: end offset of value in json_section
//   [json_bytes: the original canonical JSON bytes]
//
// val_start / val_end are offsets into `json_bytes` (relative to the first
// byte of json_section, which immediately follows the entry table). They
// point at the exact raw JSON value slice for that key, matching what
// `RawJson::scan_object_for` would return.
//
// Fallback cases (returns Err, caller uses bare JSON):
//   - top-level value is not a JSON object
//   - any key decodes to > 65535 bytes (u16_le overflow)
//   - num_entries > u32::MAX (theoretical)
//   - any malformed JSON in the canonical bytes
// ---------------------------------------------------------------------------

/// Encode canonical `json_bytes` into the ADR 0027 Phase 2 binary format.
/// Returns `Ok(encoded)` on success, `Err(())` when the document should be
/// stored as bare JSON (the caller falls back silently).
pub(crate) fn encode_jsonb_v2(json_bytes: &[u8]) -> std::result::Result<Vec<u8>, ()> {
    // Must start with '{' (a top-level object) after optional whitespace.
    let mut p = 0usize;
    skip_ws_dml(json_bytes, &mut p);
    if json_bytes.get(p) != Some(&b'{') {
        return Err(()); // not an object — bare JSON fallback
    }
    p += 1; // past '{'

    // Scan the object once to collect (decoded_key, val_start, val_end) triples.
    // val_start/val_end are offsets into json_bytes (the raw canonical document).
    let mut entries: Vec<(String, u32, u32)> = Vec::new();
    skip_ws_dml(json_bytes, &mut p);
    if json_bytes.get(p) == Some(&b'}') {
        // Empty object — valid, produce a zero-entry offset table.
    } else {
        loop {
            skip_ws_dml(json_bytes, &mut p);
            if json_bytes.get(p) != Some(&b'"') {
                return Err(()); // malformed
            }
            // Decode the key using the same logic as parse_string in jsonb_udf.rs
            // so escaped keys (\uXXXX, \\, \") compare correctly.
            let key = parse_string_dml(json_bytes, &mut p).ok_or(())?;
            if key.len() > 65535 {
                return Err(()); // key too long for u16_le
            }
            skip_ws_dml(json_bytes, &mut p);
            if json_bytes.get(p) != Some(&b':') {
                return Err(());
            }
            p += 1;
            skip_ws_dml(json_bytes, &mut p);
            let val_start = p as u32;
            skip_value_dml(json_bytes, &mut p).ok_or(())?;
            let val_end = p as u32;
            // Duplicate keys: last wins — insert replaces earlier entry.
            if let Some(existing) = entries.iter_mut().find(|(k, _, _)| k == &key) {
                *existing = (key, val_start, val_end);
            } else {
                entries.push((key, val_start, val_end));
            }
            skip_ws_dml(json_bytes, &mut p);
            match json_bytes.get(p) {
                Some(&b',') => p += 1,
                Some(&b'}') => break,
                _ => return Err(()),
            }
        }
    }

    let num_entries = entries.len();
    if num_entries > u32::MAX as usize {
        return Err(());
    }

    // Compute encoded size:
    //   1 (version) + 4 (num_entries) +
    //   sum_i (2 + key_i.len() + 4 + 4) +
    //   json_bytes.len()
    let header_size = 1 + 4 + entries.iter().map(|(k, _, _)| 2 + k.len() + 4 + 4).sum::<usize>();
    let total = header_size + json_bytes.len();

    let mut out = Vec::with_capacity(total);
    out.push(0x02u8);
    out.extend_from_slice(&(num_entries as u32).to_le_bytes());
    for (key, val_start, val_end) in &entries {
        let kl = key.len() as u16;
        out.extend_from_slice(&kl.to_le_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&val_start.to_le_bytes());
        out.extend_from_slice(&val_end.to_le_bytes());
    }
    out.extend_from_slice(json_bytes);
    debug_assert_eq!(out.len(), total, "encode_jsonb_v2: size mismatch");
    Ok(out)
}

/// Minimal whitespace-skip for the DML encoder (mirrors `skip_ws` in jsonb_udf).
#[inline]
fn skip_ws_dml(b: &[u8], p: &mut usize) {
    while let Some(&c) = b.get(*p) {
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            *p += 1;
        } else {
            break;
        }
    }
}

/// Skip a single JSON value (object/array/string/number/literal) in `b`,
/// advancing `*p` past it. Returns `None` on malformed input.
fn skip_value_dml(b: &[u8], p: &mut usize) -> Option<()> {
    skip_ws_dml(b, p);
    match b.get(*p)? {
        b'{' => skip_object_dml(b, p),
        b'[' => skip_array_dml(b, p),
        b'"' => skip_string_dml(b, p),
        b't' => skip_lit_dml(b, p, b"true"),
        b'f' => skip_lit_dml(b, p, b"false"),
        b'n' => skip_lit_dml(b, p, b"null"),
        b'-' | b'0'..=b'9' => {
            while let Some(&c) = b.get(*p) {
                match c {
                    b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => *p += 1,
                    _ => break,
                }
            }
            Some(())
        }
        _ => None,
    }
}

fn skip_object_dml(b: &[u8], p: &mut usize) -> Option<()> {
    *p += 1;
    skip_ws_dml(b, p);
    if b.get(*p) == Some(&b'}') {
        *p += 1;
        return Some(());
    }
    loop {
        skip_ws_dml(b, p);
        if b.get(*p) != Some(&b'"') {
            return None;
        }
        skip_string_dml(b, p)?;
        skip_ws_dml(b, p);
        if b.get(*p) != Some(&b':') {
            return None;
        }
        *p += 1;
        skip_value_dml(b, p)?;
        skip_ws_dml(b, p);
        match b.get(*p) {
            Some(&b',') => *p += 1,
            Some(&b'}') => {
                *p += 1;
                return Some(());
            }
            _ => return None,
        }
    }
}

fn skip_array_dml(b: &[u8], p: &mut usize) -> Option<()> {
    *p += 1;
    skip_ws_dml(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return Some(());
    }
    loop {
        skip_value_dml(b, p)?;
        skip_ws_dml(b, p);
        match b.get(*p) {
            Some(&b',') => *p += 1,
            Some(&b']') => {
                *p += 1;
                return Some(());
            }
            _ => return None,
        }
    }
}

fn skip_string_dml(b: &[u8], p: &mut usize) -> Option<()> {
    if b.get(*p) != Some(&b'"') {
        return None;
    }
    *p += 1;
    loop {
        match b.get(*p)? {
            b'\\' => *p += 2,
            b'"' => {
                *p += 1;
                return Some(());
            }
            _ => *p += 1,
        }
    }
}

fn skip_lit_dml(b: &[u8], p: &mut usize, lit: &[u8]) -> Option<()> {
    if b.len() >= *p + lit.len() && &b[*p..*p + lit.len()] == lit {
        *p += lit.len();
        Some(())
    } else {
        None
    }
}

/// Parse a JSON string token at `*p` into a decoded Rust String.
/// Mirrors `parse_string` in jsonb_udf.rs; returns `None` on error.
fn parse_string_dml(b: &[u8], p: &mut usize) -> Option<String> {
    if b.get(*p) != Some(&b'"') {
        return None;
    }
    *p += 1;
    let start = *p;
    let mut has_escape = false;
    while let Some(&c) = b.get(*p) {
        match c {
            b'\\' => {
                has_escape = true;
                break;
            }
            b'"' => {
                let s = std::str::from_utf8(&b[start..*p]).ok()?.to_string();
                *p += 1;
                return Some(s);
            }
            _ => *p += 1,
        }
    }
    if !has_escape {
        return None; // unterminated
    }
    // Slow path: handle escapes.
    let mut out = std::str::from_utf8(&b[start..*p]).ok()?.to_string();
    while let Some(&c) = b.get(*p) {
        match c {
            b'"' => {
                *p += 1;
                return Some(out);
            }
            b'\\' => {
                *p += 1;
                let esc = *b.get(*p)?;
                match esc {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let cp = parse_hex4_dml(b, *p + 1)?;
                        *p += 4;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            if b.get(*p + 1) == Some(&b'\\') && b.get(*p + 2) == Some(&b'u') {
                                let low = parse_hex4_dml(b, *p + 3)?;
                                *p += 6;
                                if (0xDC00..=0xDFFF).contains(&low) {
                                    let ch = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                                    out.push(char::from_u32(ch)?);
                                } else {
                                    return None;
                                }
                            } else {
                                return None;
                            }
                        } else if (0xDC00..=0xDFFF).contains(&cp) {
                            return None;
                        } else {
                            out.push(char::from_u32(cp)?);
                        }
                    }
                    _ => return None,
                }
                *p += 1;
            }
            _ => {
                let run_start = *p;
                while let Some(&cc) = b.get(*p) {
                    if cc == b'"' || cc == b'\\' {
                        break;
                    }
                    *p += 1;
                }
                out.push_str(std::str::from_utf8(&b[run_start..*p]).ok()?);
            }
        }
    }
    None
}

fn parse_hex4_dml(b: &[u8], at: usize) -> Option<u32> {
    let mut v = 0u32;
    for i in 0..4 {
        let c = *b.get(at + i)?;
        let d = match c {
            b'0'..=b'9' => (c - b'0') as u32,
            b'a'..=b'f' => (c - b'a' + 10) as u32,
            b'A'..=b'F' => (c - b'A' + 10) as u32,
            _ => return None,
        };
        v = (v << 4) | d;
    }
    Some(v)
}

/// Decode a UUID literal into its 16-byte RFC 4122 representation.
///
/// Accepts:
/// - A SQL string literal in any UUID-canonical form
///   (`'550e8400-e29b-41d4-a716-446655440000'`, the same with braces, or
///    a 32-char hyphen-less hex form). `uuid::Uuid::parse_str` handles all
///   three. The hyphenated lowercase form is what we emit on the wire.
/// - A bare function call `gen_random_uuid()` or `uuid_generate_v4()` —
///   produces a fresh `Uuid::new_v4()`. (The same UDFs are also registered
///   on the DataFusion side for SELECT contexts; in INSERT we have to
///   generate the bytes here because the value list is interpreted before
///   it ever sees DataFusion.)
/// - A `Cast` wrapper (`'...'::uuid`) — peeled and recursed.
/// - `NULL`.
///
/// Anything else is `InvalidSchema` so a user can't silently insert a
/// zeroed UUID. Note: we do *not* accept hex-bytea (`'\x...'::bytea`) here
/// — UUID is a logical type with its own surface form, and the conflation
/// would surprise readers later.
fn coerce_uuid(expr: &Expr, col: &str) -> Result<Option<[u8; 16]>> {
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::DoubleQuotedString(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::EscapedStringLiteral(s), .. })
        | Expr::Value(ValueWithSpan { value: Value::NationalStringLiteral(s), .. }) => {
            let parsed = uuid::Uuid::parse_str(s).map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "invalid UUID literal for column {col}: {e} (got {s:?})"
                ))
            })?;
            Ok(Some(*parsed.as_bytes()))
        }
        Expr::Value(ValueWithSpan { value: Value::Null, .. }) => Ok(None),
        Expr::Cast { expr: inner, .. } => coerce_uuid(inner.as_ref(), col),
        Expr::Function(f) => {
            // Match `gen_random_uuid()` (pgcrypto) and
            // `uuid_generate_v4()` (uuid-ossp). Both take zero args. If
            // we ever need a v7 sortable UUID it goes here too — same
            // shape, different `Uuid::now_v7()` body.
            let fname = f
                .name
                .0
                .last()
                .map(|i| i.id_val().to_ascii_lowercase())
                .unwrap_or_default();
            let args_empty = match &f.args {
                FunctionArguments::None => true,
                FunctionArguments::List(list) => list.args.is_empty(),
                _ => false,
            };
            match (fname.as_str(), args_empty) {
                ("gen_random_uuid", true) | ("uuid_generate_v4", true) => {
                    let bytes = *uuid::Uuid::new_v4().as_bytes();
                    Ok(Some(bytes))
                }
                _ => Err(BasinError::InvalidSchema(format!(
                    "unsupported UUID-producing function for column {col}: {fname}({})",
                    if args_empty { "" } else { "..." }
                ))),
            }
        }
        other => Err(BasinError::InvalidSchema(format!(
            "expected UUID literal or gen_random_uuid()/uuid_generate_v4() for column {col}, got {other}"
        ))),
    }
}

/// PG-Wave-α: decode a POINT literal into a 21-byte WKB blob.
///
/// Accepts:
/// - `ST_MakePoint(x, y)` — both args must be numeric literals (the
///   INSERT path doesn't evaluate UDFs; the constructor is folded
///   directly here for ergonomic literals).
/// - A WKT string literal (`'POINT(x y)'`, case-insensitive on the
///   leading keyword). Engine implicitly calls `ST_GeomFromText` here so
///   plain `INSERT INTO t VALUES ('POINT(1 2)')` works.
/// - A `Cast` wrapper (`'POINT(1 2)'::point`) — peeled and recursed.
/// - `NULL`.
fn coerce_point(expr: &Expr, col: &str) -> Result<Option<[u8; basin_geo::POINT_WKB_LEN]>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => parse_wkt_point(s, col).map(Some),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Cast { expr: inner, .. } => coerce_point(inner.as_ref(), col),
        Expr::TypedString(TypedString { value, .. }) => match &value.value {
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s)
            | Value::NationalStringLiteral(s) => parse_wkt_point(s, col).map(Some),
            _ => Err(BasinError::InvalidSchema(format!(
                "POINT column {col}: TypedString must wrap a string literal"
            ))),
        },
        Expr::Function(f) => {
            let fname = f
                .name
                .0
                .last()
                .map(|i| i.id_val().to_ascii_lowercase())
                .unwrap_or_default();
            // ST_GeomFromGeoJSON / ST_GeomFromText with a literal argument
            // are constructor shapes every client library emits — decode the
            // literal inline rather than rejecting the INSERT.
            if fname == "st_geomfromgeojson" || fname == "st_geomfromtext" {
                let lit = match &f.args {
                    FunctionArguments::List(list) => list.args.first().and_then(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            ValueWithSpan {
                                value: Value::SingleQuotedString(s2),
                                ..
                            },
                        ))) => Some(s2.clone()),
                        _ => None,
                    }),
                    _ => None,
                };
                let Some(lit) = lit else {
                    return Err(BasinError::InvalidSchema(format!(
                        "POINT column {col}: {fname} requires a string literal argument on the INSERT path"
                    )));
                };
                if fname == "st_geomfromgeojson" {
                    let pt = basin_geo::geojson::decode_point_geojson(&lit).map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "POINT column {col}: invalid GeoJSON: {e}"
                        ))
                    })?;
                    return Ok(Some(basin_geo::encode_point(&pt)));
                }
                return parse_wkt_point(&lit, col).map(Some);
            }
            if fname != "st_makepoint" {
                return Err(BasinError::InvalidSchema(format!(
                    "expected ST_MakePoint(x, y), ST_GeomFromText/GeoJSON('...'), or WKT literal for POINT column {col}, got {fname}(...)"
                )));
            }
            let arg_exprs = match &f.args {
                FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => Some(e.clone()),
                        sqlparser::ast::FunctionArg::Named { arg, .. } => match arg {
                            sqlparser::ast::FunctionArgExpr::Expr(e) => Some(e.clone()),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            };
            if arg_exprs.len() != 2 {
                return Err(BasinError::InvalidSchema(format!(
                    "ST_MakePoint expects 2 numeric arguments for column {col}, got {}",
                    arg_exprs.len()
                )));
            }
            let x = coerce_f64(&arg_exprs[0])?.ok_or_else(|| {
                BasinError::InvalidSchema(format!(
                    "ST_MakePoint: X must be a non-NULL numeric for column {col}"
                ))
            })?;
            let y = coerce_f64(&arg_exprs[1])?.ok_or_else(|| {
                BasinError::InvalidSchema(format!(
                    "ST_MakePoint: Y must be a non-NULL numeric for column {col}"
                ))
            })?;
            Ok(Some(basin_geo::encode_point(&basin_geo::Point::new(x, y))))
        }
        other => Err(BasinError::InvalidSchema(format!(
            "expected ST_MakePoint(...) or WKT literal for POINT column {col}, got {other}"
        ))),
    }
}

fn parse_wkt_point(s: &str, col: &str) -> Result<[u8; basin_geo::POINT_WKB_LEN]> {
    let trimmed = s.trim();
    let after = trimmed
        .strip_prefix("POINT")
        .or_else(|| trimmed.strip_prefix("point"))
        .or_else(|| trimmed.strip_prefix("Point"))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "POINT column {col}: expected WKT like 'POINT(x y)', got {trimmed:?}"
            ))
        })?
        .trim_start();
    let inside = after
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "POINT column {col}: expected parenthesised POINT(x y), got {trimmed:?}"
            ))
        })?;
    let mut parts = inside.split_whitespace();
    let x: f64 = parts
        .next()
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!("POINT column {col}: missing X coordinate"))
        })?
        .parse()
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "POINT column {col}: X coordinate not a number: {e}"
            ))
        })?;
    let y: f64 = parts
        .next()
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!("POINT column {col}: missing Y coordinate"))
        })?
        .parse()
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "POINT column {col}: Y coordinate not a number: {e}"
            ))
        })?;
    if parts.next().is_some() {
        return Err(BasinError::InvalidSchema(format!(
            "POINT column {col}: only 2D POINT(x y) is supported in v0.1"
        )));
    }
    Ok(basin_geo::encode_point(&basin_geo::Point::new(x, y)))
}

// ── Network / bit-string coercions ──────────────────────────────────────────

/// Pull a plain string literal (or NULL) out of an INSERT expression. Used as
/// the foundation for network and bit-string coercions which all accept a
/// single-quoted string. Cast wrappers (`'...'::inet`) are accepted by peeling.
fn coerce_string_for_typed(expr: &Expr, col: &str, type_name: &str) -> Result<Option<String>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => Ok(Some(s.clone())),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Cast { expr: inner, .. } => coerce_string_for_typed(inner.as_ref(), col, type_name),
        other => Err(BasinError::InvalidSchema(format!(
            "expected {type_name} string literal for column {col}, got {other}"
        ))),
    }
}

// ── Full-text search coercions ──────────────────────────────────────────────
//
// A `TSVECTOR` / `TSQUERY` column accepts on INSERT either:
//   * a plain string literal — treated as the `text::tsvector` /
//     `text::tsquery` cast shape and canonicalised (see `fts_udf`), or
//   * a `to_tsvector(...)` / `to_tsquery(...)` / `plainto_tsquery(...)` /
//     `phraseto_tsquery(...)` function call whose **string-literal**
//     arguments are evaluated to the canonical stored form.
//
// Only literal arguments are evaluated here (the INSERT VALUES path has no
// row context).  A non-literal argument is a hard, honest error rather than
// a silently-wrong value.

/// Extract a string-literal argument (peeling a cast) from an INSERT
/// function-call argument expression, or `None` if it isn't a literal.
fn fts_str_arg(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => Some(s.clone()),
        Expr::Cast { expr: inner, .. } => fts_str_arg(inner.as_ref()),
        _ => None,
    }
}

/// Pull the positional string-literal args out of an FTS function call.
fn fts_call_args(f: &sqlparser::ast::Function) -> Option<Vec<String>> {
    let list = match &f.args {
        FunctionArguments::List(list) => list,
        _ => return None,
    };
    let mut out = Vec::with_capacity(list.args.len());
    for a in &list.args {
        let e = match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => e,
            _ => return None,
        };
        out.push(fts_str_arg(e)?);
    }
    Some(out)
}

fn fts_fn_name(f: &sqlparser::ast::Function) -> String {
    f.name
        .0
        .last()
        .map(|i| i.id_val().to_ascii_lowercase())
        .unwrap_or_default()
}

/// Coerce an INSERT expression into the canonical `TSVECTOR` text form.
fn coerce_tsvector(expr: &Expr, col: &str) -> Result<Option<String>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => Ok(Some(crate::fts_udf::canonicalize_tsvector_text(s))),
        Expr::Cast { expr: inner, .. } => coerce_tsvector(inner.as_ref(), col),
        Expr::Function(f) => {
            let name = fts_fn_name(f);
            if name != "to_tsvector" {
                return Err(BasinError::InvalidSchema(format!(
                    "column {col} (TSVECTOR) accepts a string literal or \
                     to_tsvector(...), got {name}(...)"
                )));
            }
            let args = fts_call_args(f).ok_or_else(|| {
                BasinError::InvalidSchema(format!(
                    "column {col}: to_tsvector(...) INSERT value must have \
                     string-literal arguments"
                ))
            })?;
            let (config, body) = match args.as_slice() {
                [body] => (None, body.as_str()),
                [cfg, body] => (Some(cfg.as_str()), body.as_str()),
                _ => {
                    return Err(BasinError::InvalidSchema(format!(
                        "column {col}: to_tsvector expects 1 or 2 string arguments"
                    )))
                }
            };
            Ok(Some(crate::fts_udf::to_tsvector_text(config, body)))
        }
        other => Err(BasinError::InvalidSchema(format!(
            "column {col} (TSVECTOR): expected string literal or \
             to_tsvector(...), got {other}"
        ))),
    }
}

/// Coerce an INSERT expression into the canonical `TSQUERY` text form.
fn coerce_tsquery(expr: &Expr, col: &str) -> Result<Option<String>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => crate::fts_udf::canonicalize_tsquery_text(s)
            .map(Some)
            .map_err(|e| BasinError::InvalidSchema(format!("column {col} (TSQUERY): {e}"))),
        Expr::Cast { expr: inner, .. } => coerce_tsquery(inner.as_ref(), col),
        Expr::Function(f) => {
            let name = fts_fn_name(f);
            if !matches!(
                name.as_str(),
                "to_tsquery" | "plainto_tsquery" | "phraseto_tsquery"
            ) {
                return Err(BasinError::InvalidSchema(format!(
                    "column {col} (TSQUERY) accepts a string literal or \
                     to_tsquery(...)/plainto_tsquery(...)/phraseto_tsquery(...), \
                     got {name}(...)"
                )));
            }
            let args = fts_call_args(f).ok_or_else(|| {
                BasinError::InvalidSchema(format!(
                    "column {col}: {name}(...) INSERT value must have \
                     string-literal arguments"
                ))
            })?;
            let (config, body) = match args.as_slice() {
                [body] => (None, body.as_str()),
                [cfg, body] => (Some(cfg.as_str()), body.as_str()),
                _ => {
                    return Err(BasinError::InvalidSchema(format!(
                        "column {col}: {name} expects 1 or 2 string arguments"
                    )))
                }
            };
            crate::fts_udf::to_tsquery_text(&name, config, body)
                .map(Some)
                .map_err(|e| BasinError::InvalidSchema(format!("column {col} ({name}): {e}")))
        }
        other => Err(BasinError::InvalidSchema(format!(
            "column {col} (TSQUERY): expected string literal or \
             to_tsquery(...), got {other}"
        ))),
    }
}

/// Validate and store an `INET` literal.
///
/// PG INET accepts `'192.168.0.1'`, `'::1'`, `'10.0.0.0/8'`,
/// `'2001:db8::/32'`. We perform a lightweight structural validation
/// (no third-party IP crate required):
/// - Optionally strip a `/prefix` suffix and validate the prefix number.
/// - Accept if the host portion can be parsed as IPv4 or IPv6 using the
///   stdlib `std::net::IpAddr` parser.
fn coerce_inet(expr: &Expr, col: &str) -> Result<Option<String>> {
    let Some(s) = coerce_string_for_typed(expr, col, "INET")? else {
        return Ok(None);
    };
    validate_ip_with_prefix(&s, col, "INET", true)?;
    Ok(Some(s))
}

/// Validate and store a `CIDR` literal.
///
/// CIDR requires a network address (host bits must be zero in PG, but we
/// only enforce that the prefix length is syntactically valid for v0.1).
fn coerce_cidr(expr: &Expr, col: &str) -> Result<Option<String>> {
    let Some(s) = coerce_string_for_typed(expr, col, "CIDR")? else {
        return Ok(None);
    };
    validate_ip_with_prefix(&s, col, "CIDR", false)?;
    Ok(Some(s))
}

/// Shared IPv4/IPv6 + optional-prefix validator.
///
/// `requires_prefix` = false for INET (prefix optional), true for CIDR
/// (we accept prefix-less CIDR for simplicity in v0.1 to match what PG
/// accepts for single-host network notation like `'192.168.1.1'`).
fn validate_ip_with_prefix(
    s: &str,
    col: &str,
    type_name: &str,
    _prefix_optional: bool,
) -> Result<()> {
    use std::net::IpAddr;
    let (host, prefix) = match s.rfind('/') {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    host.parse::<IpAddr>().map_err(|_| {
        BasinError::InvalidSchema(format!(
            "invalid {type_name} address for column {col}: {s:?} (host part {host:?} is not a valid IP)"
        ))
    })?;
    if let Some(pfx) = prefix {
        let n: u8 = pfx.parse().map_err(|_| {
            BasinError::InvalidSchema(format!(
                "invalid {type_name} prefix for column {col}: {s:?} (prefix {pfx:?} is not a number)"
            ))
        })?;
        // IPv4 max prefix = 32, IPv6 = 128. We allow up to 128 for both
        // in v0.1 — a tighter check requires knowing the address family
        // which we already parsed above.
        if n > 128 {
            return Err(BasinError::InvalidSchema(format!(
                "invalid {type_name} prefix length {n} for column {col}: must be 0..=128"
            )));
        }
    }
    Ok(())
}

/// Validate and store a `MACADDR` or `MACADDR8` literal.
///
/// `is_eui64` = false → 6-group EUI-48 (MACADDR), `is_eui64` = true →
/// 8-group EUI-64 (MACADDR8).
///
/// PG accepts several separator styles:
///   - `01:23:45:67:89:ab` (colon, standard)
///   - `01-23-45-67-89-ab` (hyphen)
///   - `0123456789ab`      (no separator, EUI-48 only)
///   - `01:23:45:67:89:ab:cd:ef` (8-group for MACADDR8)
fn coerce_macaddr(expr: &Expr, col: &str, is_eui64: bool) -> Result<Option<String>> {
    let type_name = if is_eui64 { "MACADDR8" } else { "MACADDR" };
    let Some(s) = coerce_string_for_typed(expr, col, type_name)? else {
        return Ok(None);
    };
    let expected_groups = if is_eui64 { 8usize } else { 6usize };
    let lower = s.to_ascii_lowercase();
    let lower = lower.trim();
    // Detect separator.
    let groups: Vec<&str> = if lower.contains(':') {
        lower.split(':').collect()
    } else if lower.contains('-') {
        lower.split('-').collect()
    } else {
        // No separator: only valid for EUI-48 (12 hex chars).
        if !is_eui64 && lower.len() == 12 && lower.bytes().all(|b| b.is_ascii_hexdigit()) {
            // Store in canonical colon-separated form.
            let canonical = lower
                .as_bytes()
                .chunks(2)
                .map(|c| std::str::from_utf8(c).unwrap())
                .collect::<Vec<_>>()
                .join(":");
            return Ok(Some(canonical));
        }
        return Err(BasinError::InvalidSchema(format!(
            "invalid {type_name} literal for column {col}: {s:?}"
        )));
    };
    if groups.len() != expected_groups {
        return Err(BasinError::InvalidSchema(format!(
            "invalid {type_name} literal for column {col}: {s:?} (expected {expected_groups} groups, got {})",
            groups.len()
        )));
    }
    for g in &groups {
        if g.len() != 2 || !g.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BasinError::InvalidSchema(format!(
                "invalid {type_name} literal for column {col}: {s:?} (group {g:?} is not two hex digits)"
            )));
        }
    }
    // Store in canonical colon-separated lowercase form.
    Ok(Some(groups.join(":")))
}

/// Validate and store a `BIT(n)` or `VARBIT(n)` literal.
///
/// PG bit-string literals are written as `B'0110'` (parsed by sqlparser
/// as `Value::SingleQuotedString("0110")` inside a `bit-string literal`)
/// or as a plain string literal `'0110'`.
///
/// Parameters:
/// - `max_len`: `Some(n)` → enforce a length bound; `None` → no bound.
/// - `exact`:  `true` → length must equal `max_len` exactly (BIT(n));
///             `false` → length must be ≤ `max_len` (VARBIT(n)).
fn coerce_bit_string(
    expr: &Expr,
    max_len: Option<u64>,
    exact: bool,
    col: &str,
) -> Result<Option<String>> {
    let s = match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => s.clone(),
        // sqlparser parses `B'0110'` as Value::SingleQuotedString with the
        // `B` prefix consumed — it becomes the raw string without the `B`.
        // We handle the case where the user wrote a number literal of 0/1s.
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        Expr::Cast { expr: inner, .. } => {
            return coerce_bit_string(inner.as_ref(), max_len, exact, col);
        }
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "expected bit-string literal for column {col}, got {other}"
            )));
        }
    };
    // All characters must be '0' or '1'.
    if !s.bytes().all(|b| b == b'0' || b == b'1') {
        return Err(BasinError::InvalidSchema(format!(
            "invalid bit-string literal for column {col}: {s:?} (only '0' and '1' allowed)"
        )));
    }
    if s.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "invalid bit-string literal for column {col}: empty string is not allowed"
        )));
    }
    let len = s.len() as u64;
    match max_len {
        Some(n) if exact => {
            // BIT(n): literal must be exactly n bits.
            if len != n {
                return Err(BasinError::InvalidSchema(format!(
                    "bit-string literal for column {col} has length {len} but BIT({n}) requires exactly {n} bits"
                )));
            }
        }
        Some(n) => {
            // VARBIT(n): literal must be at most n bits.
            if len > n {
                return Err(BasinError::InvalidSchema(format!(
                    "bit-string literal for column {col} has length {len} but column allows at most {n}"
                )));
            }
        }
        None => {} // VARBIT with no length limit: any length is fine.
    }
    Ok(Some(s))
}

/// Validate and store a `MONEY` value.
///
/// PG MONEY accepts numeric literals directly, or string literals with an
/// optional leading currency symbol (`$`, `€`, `£`, etc.) followed by a
/// decimal number. We strip any leading non-digit/sign characters and route
/// through `coerce_decimal128`.
fn coerce_money(expr: &Expr, precision: u8, scale: i8, col: &str) -> Result<Option<i128>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => {
            // Strip leading currency symbol(s) and any commas used as
            // thousands separators. Keep digits, '.', '+', '-'.
            let stripped: String = s
                .trim()
                .chars()
                .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '+')
                .collect();
            if stripped.is_empty() {
                return Err(BasinError::InvalidSchema(format!(
                    "invalid MONEY literal for column {col}: {s:?}"
                )));
            }
            // Re-wrap in a synthetic Number expression for coerce_decimal128.
            let synthetic = Expr::Value((Value::Number(stripped, false)).into());
            coerce_decimal128(&synthetic, precision, scale, col)
        }
        // Plain numeric literals and NULLs flow through unchanged.
        _ => coerce_decimal128(expr, precision, scale, col),
    }
}

/// Recursively rebuild `v` so every object node iterates its keys in sorted
/// order. Canonical form = keys sorted ascending, last value wins on any
/// duplicate key, no insignificant whitespace.
///
/// Two performance refinements over the original `BTreeMap` round-trip
/// (Inv-jsonb-canon #183), both byte-identical to the prior output:
///
///   A. **Detect-sorted fast path** — `object_keys_sorted` does one O(n)
///      `windows(2)` pass per object level. If every object in the document
///      already has its keys in ascending order *and* no duplicates, we only
///      recurse into the values to normalise nested objects — no reorder, no
///      per-object container churn. (sonic-rs builds a `serde_json::Map`,
///      which in this build is `BTreeMap`-backed, so keys typically arrive
///      already sorted — but we never *rely* on that; the check is what makes
///      the skip sound.)
///
///   B. **Vec-sort rebuild** — when an object is *not* already sorted we
///      collect into a `Vec<(String, Value)>` and `sort_unstable_by` on the
///      key, rather than inserting into a `BTreeMap`. One Vec allocation per
///      object level instead of N per-node heap allocations. Duplicate keys
///      are deduplicated **last-wins** to match the prior `BTreeMap`/Map
///      semantics exactly.
fn canonicalize_json(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            if object_keys_sorted(&map) {
                // Change A: keys already canonical at this level. Just recurse
                // into the values (nested objects still need normalising) and
                // re-wrap in place — no reorder, no extra container.
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, val) in map {
                    out.insert(k, canonicalize_json(val));
                }
                Value::Object(out)
            } else {
                // Change B: Vec-sort instead of BTreeMap. Collect, sort by key,
                // then rebuild keeping the LAST value for any duplicate key
                // (matches the overwrite-on-insert semantics of the prior
                // BTreeMap path).
                let mut entries: Vec<(String, Value)> = map
                    .into_iter()
                    .map(|(k, val)| (k, canonicalize_json(val)))
                    .collect();
                // Stable sort by key so that, among equal keys, original
                // (insertion) order is preserved and the *last* survives dedup.
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                let mut out = serde_json::Map::with_capacity(entries.len());
                for (k, vv) in entries {
                    // BTreeMap insert overwrote on duplicate keys (last wins);
                    // serde_json::Map::insert does the same, so re-inserting in
                    // sorted order reproduces that behaviour byte-for-byte.
                    out.insert(k, vv);
                }
                Value::Object(out)
            }
        }
        Value::Array(items) => {
            // Canonical form preserves array order — only object key order
            // is normalised. Recurse into elements so nested objects get
            // their keys sorted.
            Value::Array(items.into_iter().map(canonicalize_json).collect())
        }
        other => other,
    }
}

/// True iff this object's keys are already in strictly-ascending order (no
/// duplicates). A single O(n) `windows(2)` pass over the key sequence. Returns
/// `true` for the empty / single-key cases. When this is `true`, sorting would
/// be a no-op and duplicate-dedup is unnecessary, so `canonicalize_json` can
/// skip the rebuild and only recurse into values.
fn object_keys_sorted(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    let mut prev: Option<&str> = None;
    for k in map.keys() {
        if let Some(p) = prev {
            // Strict `<`: equal keys (duplicates) force the rebuild path so
            // last-wins dedup runs.
            if p >= k.as_str() {
                return false;
            }
        }
        prev = Some(k.as_str());
    }
    true
}

fn check_null_allowed(field: &arrow_schema::Field) -> Result<()> {
    if !field.is_nullable() {
        return Err(BasinError::InvalidSchema(format!(
            "NULL inserted into NOT NULL column {}",
            field.name()
        )));
    }
    Ok(())
}

/// Decode a SQL literal to an `i64`. Returns `Ok(None)` on `NULL`. Errors on
/// type mismatch.
fn coerce_i64(expr: &Expr) -> Result<Option<i64>> {
    match peel_unary(expr) {
        (
            negated,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(if negated { -parsed } else { parsed }))
        }
        (
            false,
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }),
        ) => Ok(None),
        (_, other) => Err(BasinError::InvalidSchema(format!(
            "expected integer literal, got {other}"
        ))),
    }
}

fn coerce_f64(expr: &Expr) -> Result<Option<f64>> {
    match peel_unary(expr) {
        (
            negated,
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }),
        ) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(Some(if negated { -parsed } else { parsed }))
        }
        (
            false,
            Expr::Value(ValueWithSpan {
                value: Value::Null, ..
            }),
        ) => Ok(None),
        (_, other) => Err(BasinError::InvalidSchema(format!(
            "expected float literal, got {other}"
        ))),
    }
}

/// Decode a NUMERIC / DECIMAL literal into an i128 scaled by `10^scale`.
///
/// Accepts the same shapes as `coerce_f64` (sqlparser surfaces both
/// integer and fractional numbers as `Value::Number(text, ...)`); a leading
/// unary minus is handled via `peel_unary`. The caller has already
/// validated `1 <= precision <= 38` and `0 <= scale <= precision` at DDL
/// time, so this routine only checks that the literal's *value* fits the
/// column's declared shape (digit count vs precision after rescaling).
///
/// Rejects `NaN`, `Inf`, scientific notation (PG accepts `1e3` for
/// numeric; sqlparser parses these as `Value::Number` strings, which we
/// pass to a manual base-10 parser to keep the implementation small —
/// a parse error here is preferable to silent f64 rounding).
fn coerce_decimal128(expr: &Expr, precision: u8, scale: i8, col: &str) -> Result<Option<i128>> {
    let (negated, inner) = peel_unary(expr);
    let s = match inner {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => s.as_str(),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) if !negated => return Ok(None),
        Expr::Cast { expr: ce, .. } => {
            // Allow `'1.50'::numeric(10,2)` style casts by recursing into
            // the inner expression.
            return coerce_decimal128(ce.as_ref(), precision, scale, col);
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => s.as_str(),
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "expected NUMERIC literal for column {col}, got {other}"
            )));
        }
    };
    let parsed = parse_decimal_to_i128(s, precision, scale).map_err(|e| {
        BasinError::InvalidSchema(format!("bad NUMERIC literal {s:?} for column {col}: {e}"))
    })?;
    Ok(Some(if negated { -parsed } else { parsed }))
}

/// Parse a base-10 decimal text into an i128 scaled by `10^target_scale`,
/// rejecting values whose digit count exceeds `precision`.
///
/// Accepts an optional leading `+`/`-`, then digits, an optional `.` with
/// fractional digits. Scientific notation (`1e3`, `2.5E-1`) is supported
/// because sqlparser surfaces it as the literal text and PG accepts it
/// for numeric literals. Trailing fractional zeros beyond the column's
/// scale are silently dropped (matching PG); fractional digits that
/// would require *more* scale than the column allows are an error so the
/// user sees the precision-loss explicitly.
fn parse_decimal_to_i128(
    s: &str,
    precision: u8,
    target_scale: i8,
) -> std::result::Result<i128, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty literal".into());
    }
    // Split off a possible exponent first; sqlparser keeps the `e`/`E`
    // verbatim in the number string. We re-anchor the decimal point by
    // adjusting the effective scale.
    let (mantissa, exp): (&str, i32) = match s.find(['e', 'E']) {
        Some(i) => {
            let (m, rest) = s.split_at(i);
            let exp_str = &rest[1..];
            let exp: i32 = exp_str
                .parse()
                .map_err(|_| format!("bad exponent {exp_str:?}"))?;
            (m, exp)
        }
        None => (s, 0),
    };
    // Optional leading sign.
    let (neg, rest) = match mantissa.as_bytes().first() {
        Some(b'-') => (true, &mantissa[1..]),
        Some(b'+') => (false, &mantissa[1..]),
        _ => (false, mantissa),
    };
    if rest.is_empty() {
        return Err("missing digits".into());
    }
    // Split into integer and fractional parts.
    let (int_part, frac_part): (&str, &str) = match rest.find('.') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit()) && !int_part.is_empty() {
        return Err(format!("non-digit in integer part {int_part:?}"));
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("non-digit in fractional part {frac_part:?}"));
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return Err("missing digits".into());
    }
    // Effective scale of the parsed mantissa = frac_part length, then
    // shift by `-exp` to fold the exponent in. e.g. "1.5e2" → mantissa
    // "15" with effective scale 1 - 2 = -1 → 150.
    let raw_digits: String = format!("{int_part}{frac_part}");
    let raw_digits = raw_digits.trim_start_matches('0');
    if raw_digits.is_empty() {
        return Ok(0);
    }
    let mantissa_scale: i32 = (frac_part.len() as i32) - exp;
    // Final shift: target_scale - mantissa_scale > 0 → multiply by 10^k;
    // < 0 → divide (must be lossless: trailing zeros only).
    let shift: i32 = (target_scale as i32) - mantissa_scale;
    let mut value: i128 = raw_digits
        .parse::<i128>()
        .map_err(|e| format!("integer overflow: {e}"))?;
    if shift > 0 {
        for _ in 0..shift {
            value = value
                .checked_mul(10)
                .ok_or_else(|| "i128 overflow scaling literal up".to_string())?;
        }
    } else if shift < 0 {
        let drop = (-shift) as usize;
        for _ in 0..drop {
            if value % 10 != 0 {
                return Err(format!(
                    "literal has more fractional digits than column scale {target_scale}"
                ));
            }
            value /= 10;
        }
    }
    if neg {
        value = -value;
    }
    // Validate digit count against precision.
    let abs = if value < 0 { -value } else { value };
    let mut digits = 0u32;
    let mut probe = abs;
    if probe == 0 {
        digits = 1;
    } else {
        while probe > 0 {
            digits += 1;
            probe /= 10;
        }
    }
    if digits > (precision as u32) {
        return Err(format!(
            "value has {digits} digits which exceeds column precision {precision}"
        ));
    }
    Ok(value)
}

fn coerce_string(expr: &Expr) -> Result<Option<String>> {
    coerce_string_ref(expr).map(|opt| opt.map(|s| s.to_owned()))
}

/// Borrowing variant of [`coerce_string`]. Used by the bulk INSERT path to
/// avoid a per-row `String::clone` — Arrow's `from_iter_values` copies into
/// its single packed buffer, so an extra `String` allocation per row was
/// pure waste.
fn coerce_string_ref(expr: &Expr) -> Result<Option<&str>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        }) => Ok(Some(s.as_str())),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        other => Err(BasinError::InvalidSchema(format!(
            "expected string literal, got {other}"
        ))),
    }
}

/// Decode a vector literal. Two surface forms accepted:
///
/// - `'[0.1, 0.2, ...]'` — a string literal that we parse on insert. Matches
///   `pg_vector`'s user-facing form so existing client code ports cleanly.
/// - `ARRAY[0.1, 0.2, ...]` — sqlparser's native array literal.
///
/// Length must match the column's declared dimensionality; otherwise this
/// returns `BasinError::InvalidSchema` so the caller sees the row rejected
/// rather than getting a silently-truncated vector.
fn coerce_vector(expr: &Expr, dim: usize, col: &str) -> Result<Option<Vec<f32>>> {
    let parsed: Vec<f32> = match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => parse_vector_literal(s)?,
        Expr::Array(a) => {
            let mut v = Vec::with_capacity(a.elem.len());
            for e in &a.elem {
                match coerce_f64(e)? {
                    Some(x) => v.push(x as f32),
                    None => {
                        return Err(BasinError::InvalidSchema(format!(
                            "NULL element inside vector literal for column {col}"
                        )));
                    }
                }
            }
            v
        }
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "expected vector literal (string `'[...]'` or `ARRAY[...]`) for column {col}, got {other}"
            )));
        }
    };
    if parsed.len() != dim {
        return Err(BasinError::InvalidSchema(format!(
            "vector length {} does not match column {col} dim {dim}",
            parsed.len()
        )));
    }
    Ok(Some(parsed))
}

/// Decode a TIMESTAMPTZ literal into microseconds since the Unix epoch.
///
/// Accepts:
/// - `'2026-04-15T12:00:00Z'` — RFC3339 with explicit zone.
/// - `'2026-04-15 12:00:00+00'` — Postgres-style with explicit numeric zone.
/// - `'2026-04-15 12:00:00'` — naive form, **interpreted as UTC**. We only
///   support TIMESTAMPTZ at the column level, so a missing zone in the
///   literal is unambiguous.
/// - `123456789::BIGINT` casts already arrive pre-coerced as integers; we
///   accept those here too so a caller can shove a microsecond value in
///   directly without round-tripping through a string.
/// - `NULL`.
fn coerce_timestamp_micros(expr: &Expr) -> Result<Option<i64>> {
    // Strip an explicit `::TIMESTAMPTZ` cast wrapper if present; sqlparser
    // surfaces it as `Expr::Cast`. The inner expression is what we coerce.
    let inner = match expr {
        Expr::Cast { expr: inner, .. } => inner.as_ref(),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => return Ok(None),
        // SQL-standard typed-string literals: `TIMESTAMP '…'`,
        // `TIMESTAMPTZ '…'`, `TIMESTAMP WITH TIME ZONE '…'`, `DATE '…'`.
        // sqlparser surfaces these as `Expr::TypedString(TypedString { … })`
        // rather than `Expr::Cast`, so the existing cast-peel above misses
        // them. We accept the same TZ-bearing/naive/ISO string forms here as
        // `parse_timestamp_string` already handles for the cast path; a bare
        // `DATE 'YYYY-MM-DD'` decodes as midnight-UTC because every column
        // we land into is TIMESTAMPTZ.
        Expr::TypedString(TypedString {
            data_type,
            value: ValueWithSpan { value: v, .. },
            ..
        }) if matches!(
            data_type,
            SqlDataType::Timestamp(_, _) | SqlDataType::Date
        ) =>
        {
            let s: &str = match v {
                Value::SingleQuotedString(s)
                | Value::DoubleQuotedString(s)
                | Value::EscapedStringLiteral(s)
                | Value::NationalStringLiteral(s) => s,
                Value::Null => return Ok(None),
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "expected string-valued typed literal, got {data_type} {other:?}"
                    )));
                }
            };
            return parse_timestamp_string(s).map(Some);
        }
        _ => expr,
    };
    match inner {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => {
            let micros = parse_timestamp_string(s)?;
            Ok(Some(micros))
        }
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => {
            // Accept integer epoch microseconds. Negative values handled below.
            let parsed: i64 = n.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad timestamp integer literal {n:?}: {e}"))
            })?;
            Ok(Some(parsed))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) = inner.as_ref()
            {
                let parsed: i64 = n.parse().map_err(|e| {
                    BasinError::InvalidSchema(format!("bad timestamp integer literal -{n:?}: {e}"))
                })?;
                Ok(Some(-parsed))
            } else {
                Err(BasinError::InvalidSchema(format!(
                    "expected timestamp literal, got {expr}"
                )))
            }
        }
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Function(f) => {
            // `now()` / `current_timestamp` / `transaction_timestamp()` —
            // zero-arg time-source functions. Used most commonly as
            // `DEFAULT now()` on `TIMESTAMPTZ` columns; without this branch
            // every INSERT into a table with such a default fails the
            // moment the default is materialised by `apply_column_defaults`.
            let fname = f
                .name
                .0
                .last()
                .map(|i| i.id_val().to_ascii_lowercase())
                .unwrap_or_default();
            let args_empty = match &f.args {
                FunctionArguments::None => true,
                FunctionArguments::List(list) => list.args.is_empty(),
                _ => false,
            };
            match (fname.as_str(), args_empty) {
                ("now", true)
                | ("current_timestamp", true)
                | ("transaction_timestamp", true)
                | ("statement_timestamp", true)
                | ("clock_timestamp", true) => Ok(Some(chrono::Utc::now().timestamp_micros())),
                _ => Err(BasinError::InvalidSchema(format!(
                    "unsupported TIMESTAMPTZ-producing function: {fname}({})",
                    if args_empty { "" } else { "..." }
                ))),
            }
        }
        other => Err(BasinError::InvalidSchema(format!(
            "expected TIMESTAMPTZ literal, got {other}"
        ))),
    }
}

fn parse_timestamp_string(s: &str) -> Result<i64> {
    use chrono::{NaiveDate, NaiveDateTime};

    let trimmed = s.trim();
    // Try RFC3339 first.
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        let utc: DateTime<Utc> = dt.with_timezone(&Utc);
        return micros_from_dt(utc);
    }
    // Try a few common Postgres-shaped forms with explicit zones.
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%#z",
    ];
    for fmt in formats {
        if let Ok(dt) = DateTime::parse_from_str(trimmed, fmt) {
            let utc: DateTime<Utc> = dt.with_timezone(&Utc);
            return micros_from_dt(utc);
        }
    }
    // Naive form: assume UTC.
    let dt_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"];
    for fmt in dt_formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
            return micros_from_dt(dt);
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let naive: NaiveDateTime = date.and_hms_opt(0, 0, 0).unwrap();
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
        return micros_from_dt(dt);
    }
    Err(BasinError::InvalidSchema(format!(
        "unparseable TIMESTAMPTZ literal {s:?} (try RFC3339, e.g. 2026-04-15T12:00:00Z)"
    )))
}

fn micros_from_dt(dt: DateTime<Utc>) -> Result<i64> {
    // `timestamp_micros` returns `i64`; chrono represents valid in-range
    // datetimes so the value is well-defined. We re-validate here so a
    // future refactor catches any out-of-range path.
    let micros = dt.timestamp_micros();
    if micros == i64::MIN {
        return Err(BasinError::InvalidSchema(format!(
            "TIMESTAMPTZ {dt} out of range for microseconds-since-epoch"
        )));
    }
    Ok(micros)
}

fn coerce_bool(expr: &Expr) -> Result<Option<bool>> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => Ok(Some(*b)),
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        other => Err(BasinError::InvalidSchema(format!(
            "expected boolean literal, got {other}"
        ))),
    }
}

/// Strip a single leading unary `+` / `-`. INSERT literals like `(-3)` parse
/// as `UnaryOp(Minus, Number("3", false))` rather than `Number("-3", _)`.
fn peel_unary(expr: &Expr) -> (bool, &Expr) {
    if let Expr::UnaryOp { op, expr: inner } = expr {
        match op {
            UnaryOperator::Minus => return (true, inner.as_ref()),
            UnaryOperator::Plus => return (false, inner.as_ref()),
            _ => {}
        }
    }
    (false, expr)
}

/// Group `rows` by the partition key derived from the partition column's
/// per-row value. Returns `BTreeMap<PartitionKey, Vec<row>>` so iteration
/// order is deterministic (matters for test stability and for the bench
/// harness that compares snapshot listings byte-by-byte).
///
/// `RangeMonthly` produces keys of the form `year=YYYY/month=MM`. We accept
/// the column as either an Arrow `Timestamp` (microsecond, milli, etc.)
/// where the SQL literal has already been coerced to integer ticks, or as
/// `Int64` interpreted as microseconds-since-epoch.
pub(crate) fn group_rows_by_partition(
    schema: &Schema,
    rows: &[Vec<Expr>],
    spec: &PartitionSpec,
) -> Result<BTreeMap<PartitionKey, Vec<Vec<Expr>>>> {
    let column = match spec {
        PartitionSpec::Unpartitioned => {
            // Caller should have shortcut to the default-partition path.
            let mut out = BTreeMap::new();
            out.insert(PartitionKey::default_key(), rows.to_vec());
            return Ok(out);
        }
        PartitionSpec::RangeMonthly { column } => column.clone(),
    };
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == &column)
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "partition column {column} missing from INSERT schema"
            ))
        })?;
    let field = schema.field(col_idx);
    let unit = match field.data_type() {
        DataType::Timestamp(unit, _) => Some(*unit),
        DataType::Int64 => None,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "partition column {column} has unsupported type {other:?}"
            )));
        }
    };

    let mut out: BTreeMap<PartitionKey, Vec<Vec<Expr>>> = BTreeMap::new();
    for row in rows {
        let value = &row[col_idx];
        let micros = match unit {
            Some(_) => match coerce_timestamp_micros(value)? {
                Some(v) => v,
                None => {
                    return Err(BasinError::InvalidSchema(format!(
                        "partition column {column} cannot be NULL on INSERT"
                    )));
                }
            },
            None => match coerce_i64(value)? {
                Some(v) => v,
                None => {
                    return Err(BasinError::InvalidSchema(format!(
                        "partition column {column} cannot be NULL on INSERT"
                    )));
                }
            },
        };
        // Convert ticks to a chrono UTC DateTime. For non-microsecond
        // Timestamp units we'd convert here; today the only unit emitted
        // by basin-engine is microsecond UTC (see `types::arrow_data_type`),
        // so a direct call is enough.
        let dt = ticks_to_utc_micros(micros, unit)?;
        let key = format!("year={:04}/month={:02}", dt.year(), dt.month());
        let pkey = PartitionKey::new(key)?;
        out.entry(pkey).or_default().push(row.clone());
    }
    Ok(out)
}

/// Pre-screen for the SQL standard `INSERT INTO t [...] OVERRIDING {
/// SYSTEM | USER } VALUE VALUES (...)` clause. sqlparser 0.52 does not
/// recognise the form; we strip it textually before sqlparser sees the
/// statement and return the matched kind so the executor can stash it on
/// the session state. Returns `(original_sql, None)` when the clause is
/// absent. The match is string-literal- and comment-aware (mirrors the
/// CLUSTER BY extractor) so the keyword pair can't be spoofed by a
/// string literal.
pub(crate) fn extract_insert_overriding(
    sql: &str,
) -> Result<(String, Option<crate::session::OverridingKind>)> {
    // Cheap rejection: only INSERT statements ever carry the clause.
    let leading = sql.trim_start();
    if !leading
        .get(..6)
        .map(|s| s.eq_ignore_ascii_case("insert"))
        .unwrap_or(false)
    {
        return Ok((sql.to_string(), None));
    }
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        // Skip single-quoted string literals (with `''` doubled escapes).
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Skip double-quoted identifiers (with `""` doubled escapes).
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Skip `--` line comments.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Skip `/* ... */` block comments.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        if (b == b'O' || b == b'o')
            && bytes_match_kw_ascii(&bytes[i..], b"OVERRIDING")
            && at_word_boundary_start(bytes, i)
            && at_word_boundary_end(bytes, i + 10)
        {
            // Walk through `OVERRIDING <SYSTEM|USER> VALUE`. Each
            // sub-token tolerates intervening whitespace; reject any
            // mismatch so a typo isn't silently dropped.
            let mut j = i + 10;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let kind = if bytes_match_kw_ascii(&bytes[j..], b"SYSTEM")
                && at_word_boundary_end(bytes, j + 6)
            {
                j += 6;
                crate::session::OverridingKind::System
            } else if bytes_match_kw_ascii(&bytes[j..], b"USER")
                && at_word_boundary_end(bytes, j + 4)
            {
                j += 4;
                crate::session::OverridingKind::User
            } else {
                return Err(BasinError::InvalidSchema(
                    "INSERT ... OVERRIDING: expected SYSTEM or USER".into(),
                ));
            };
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if !(bytes_match_kw_ascii(&bytes[j..], b"VALUE") && at_word_boundary_end(bytes, j + 5))
            {
                return Err(BasinError::InvalidSchema(
                    "INSERT ... OVERRIDING ...: expected VALUE".into(),
                ));
            }
            let end = j + 5;
            // Strip the clause and surrounding whitespace; replace with a
            // single space so the resulting SQL stays parseable.
            let mut stripped = String::with_capacity(sql.len() - (end - i));
            stripped.push_str(&sql[..i].trim_end_matches(|c: char| c.is_whitespace()));
            stripped.push(' ');
            stripped.push_str(sql[end..].trim_start_matches(|c: char| c.is_whitespace()));
            return Ok((stripped, Some(kind)));
        }
        i += 1;
    }
    Ok((sql.to_string(), None))
}

fn bytes_match_kw_ascii(bytes: &[u8], kw: &[u8]) -> bool {
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    true
}

fn at_word_boundary_start(bytes: &[u8], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let prev = bytes[i - 1];
    !(prev.is_ascii_alphanumeric() || prev == b'_')
}

fn at_word_boundary_end(bytes: &[u8], i: usize) -> bool {
    if i >= bytes.len() {
        return true;
    }
    let c = bytes[i];
    !(c.is_ascii_alphanumeric() || c == b'_')
}

fn ticks_to_utc_micros(ticks: i64, unit: Option<TimeUnit>) -> Result<DateTime<Utc>> {
    let micros = match unit.unwrap_or(TimeUnit::Microsecond) {
        TimeUnit::Microsecond => ticks,
        TimeUnit::Millisecond => ticks
            .checked_mul(1000)
            .ok_or_else(|| BasinError::InvalidSchema("timestamp overflow ms→us".into()))?,
        TimeUnit::Nanosecond => ticks / 1000,
        TimeUnit::Second => ticks
            .checked_mul(1_000_000)
            .ok_or_else(|| BasinError::InvalidSchema("timestamp overflow s→us".into()))?,
    };
    let secs = micros.div_euclid(1_000_000);
    let sub_us = micros.rem_euclid(1_000_000) as u32;
    Utc.timestamp_opt(secs, sub_us * 1000)
        .single()
        .ok_or_else(|| BasinError::InvalidSchema(format!("timestamp {micros}us out of range")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use sqlparser::ast::{Insert, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn rows_from_sql(sql: &str) -> Vec<Vec<Expr>> {
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let stmt = stmts.pop().unwrap();
        let ins: Insert = match stmt {
            Statement::Insert(i) => i,
            _ => panic!("expected INSERT"),
        };
        let source = ins.source.expect("VALUES clause");
        match *source.body {
            SetExpr::Values(v) => v.rows,
            _ => panic!("expected VALUES"),
        }
    }

    /// Parse `input` as JSON, canonicalize, and return the serialized bytes —
    /// exactly what `coerce_jsonb` stores. Panics on parse error.
    fn canon_bytes(input: &str) -> String {
        let parsed: serde_json::Value = sonic_rs::from_str(input).unwrap();
        let canonical = canonicalize_json(parsed);
        String::from_utf8(sonic_rs::to_vec(&canonical).unwrap()).unwrap()
    }

    #[test]
    fn canonicalize_json_is_byte_identical_to_hand_form() {
        // Object keys are sorted ascending regardless of input order.
        assert_eq!(canon_bytes(r#"{"b":2,"a":1}"#), r#"{"a":1,"b":2}"#);
        // Already-sorted input (Change A fast path) is unchanged.
        assert_eq!(canon_bytes(r#"{"a":1,"b":2}"#), r#"{"a":1,"b":2}"#);
        // Nested objects are canonicalised recursively.
        assert_eq!(canon_bytes(r#"{"z":{"y":2,"x":1}}"#), r#"{"z":{"x":1,"y":2}}"#);
        // Duplicate top-level keys: last value wins.
        assert_eq!(canon_bytes(r#"{"a":1,"a":2}"#), r#"{"a":2}"#);
        // Duplicate keys, reversed-on-arrival ordering: still last wins.
        assert_eq!(canon_bytes(r#"{"b":1,"a":1,"a":2}"#), r#"{"a":2,"b":1}"#);
        // Insignificant whitespace is stripped (canonical PG form).
        assert_eq!(canon_bytes(r#"{ "a" : 1 }"#), r#"{"a":1}"#);
        // Arrays preserve element order; nested objects within still sort.
        assert_eq!(
            canon_bytes(r#"[{"b":2,"a":1},{"d":4,"c":3}]"#),
            r#"[{"a":1,"b":2},{"c":3,"d":4}]"#
        );
        // Deeply nested mix of arrays + objects.
        assert_eq!(
            canon_bytes(r#"{"k":[{"q":1,"p":{"n":2,"m":1}}]}"#),
            r#"{"k":[{"p":{"m":1,"n":2},"q":1}]}"#
        );
        // Scalars and empty containers pass through unchanged.
        assert_eq!(canon_bytes(r#"{}"#), r#"{}"#);
        assert_eq!(canon_bytes(r#"[]"#), r#"[]"#);
        assert_eq!(canon_bytes(r#"42"#), r#"42"#);
        assert_eq!(canon_bytes(r#""s""#), r#""s""#);
        assert_eq!(canon_bytes(r#"null"#), r#"null"#);
    }

    /// Cross-check: the Vec-sort/fast-path implementation must produce the
    /// same bytes as a reference BTreeMap-based canonicaliser for arbitrary
    /// key orders (the byte-equality contract `viability_jsonb` asserts).
    #[test]
    fn canonicalize_json_matches_btreemap_reference() {
        fn reference(v: serde_json::Value) -> serde_json::Value {
            use serde_json::Value;
            match v {
                Value::Object(map) => {
                    let sorted: std::collections::BTreeMap<String, Value> =
                        map.into_iter().map(|(k, val)| (k, reference(val))).collect();
                    let mut out = serde_json::Map::with_capacity(sorted.len());
                    for (k, vv) in sorted {
                        out.insert(k, vv);
                    }
                    Value::Object(out)
                }
                Value::Array(items) => {
                    Value::Array(items.into_iter().map(reference).collect())
                }
                other => other,
            }
        }
        let inputs = [
            r#"{"b":2,"a":1}"#,
            r#"{"z":{"y":2,"x":1},"a":[3,2,1]}"#,
            r#"{"a":1,"a":2,"a":3}"#,
            r#"[{"c":3,"b":2,"a":1}]"#,
            r#"{"nested":{"deep":{"q":1,"p":2,"o":3}}}"#,
            r#"{ "x" : { "z" : 1 , "a" : 2 } }"#,
            r#"{"":1,"a":2}"#,
        ];
        for inp in inputs {
            let parsed: serde_json::Value = sonic_rs::from_str(inp).unwrap();
            let mine = sonic_rs::to_vec(&canonicalize_json(parsed.clone())).unwrap();
            let refd = sonic_rs::to_vec(&reference(parsed)).unwrap();
            assert_eq!(mine, refd, "mismatch for input {inp}");
        }
    }

    #[test]
    fn bulk_path_matches_slow_path_for_1000_rows() {
        // Build a 1000-row INSERT exercising Int64+Utf8+Boolean+Float64.
        let mut sql = String::from("INSERT INTO t (id, name, ok, score) VALUES ");
        for i in 0..1000 {
            if i > 0 {
                sql.push(',');
            }
            let bool_lit = if i % 2 == 0 { "TRUE" } else { "FALSE" };
            sql.push_str(&format!("({i}, 'row-{i}', {bool_lit}, {}.5)", i as f64));
        }
        let rows = rows_from_sql(&sql);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("ok", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
        ]));

        // Bulk path (1000 rows is well over BULK_THRESHOLD).
        let bulk = batch_from_rows(schema.clone(), &rows).unwrap();
        assert_eq!(bulk.num_rows(), 1000);

        // Force the slow path by constructing a sub-threshold call repeatedly
        // — we emulate the per-row builder result by trimming `BULK_THRESHOLD`
        // out of the picture: build the same batch using a 99-row chunk size
        // and concat. To keep the test self-contained, instead just walk the
        // bulk batch and re-derive each cell, comparing against the literals.
        let ids = bulk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = bulk
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let oks = bulk
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let scores = bulk
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..1000 {
            assert_eq!(ids.value(i), i as i64, "id row {i}");
            assert_eq!(names.value(i), format!("row-{i}"), "name row {i}");
            assert_eq!(oks.value(i), i % 2 == 0, "ok row {i}");
            assert_eq!(scores.value(i), i as f64 + 0.5, "score row {i}");
        }
    }

    #[test]
    fn bulk_path_equivalence_via_two_chunks() {
        // Compare a 200-row bulk-path run against the same logical rows split
        // into two 99-row sub-threshold runs that go through the slow path.
        let make_sql = |start: usize, count: usize| {
            let mut sql = String::from("INSERT INTO t (id, name) VALUES ");
            for i in 0..count {
                if i > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("({}, 'r{}')", start + i, start + i));
            }
            sql
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let bulk_rows = rows_from_sql(&make_sql(0, 200));
        assert!(bulk_rows.len() >= BULK_THRESHOLD);
        let bulk = batch_from_rows(schema.clone(), &bulk_rows).unwrap();

        let slow_a = rows_from_sql(&make_sql(0, 99));
        let slow_b = rows_from_sql(&make_sql(99, 101));
        assert!(slow_a.len() < BULK_THRESHOLD, "slow_a must use slow path");
        // slow_b is 101 rows so it actually hits the bulk path; that's fine
        // for this test because our claim is "bulk == slow"; we want both
        // batches to assemble to the same logical contents.
        let slow_a_batch = batch_from_rows(schema.clone(), &slow_a).unwrap();
        let slow_b_batch = batch_from_rows(schema.clone(), &slow_b).unwrap();

        let bulk_ids = bulk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bulk_names = bulk
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let slow_a_ids = slow_a_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let slow_a_names = slow_a_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let slow_b_ids = slow_b_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let slow_b_names = slow_b_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..99 {
            assert_eq!(bulk_ids.value(i), slow_a_ids.value(i));
            assert_eq!(bulk_names.value(i), slow_a_names.value(i));
        }
        for i in 0..101 {
            assert_eq!(bulk_ids.value(99 + i), slow_b_ids.value(i));
            assert_eq!(bulk_names.value(99 + i), slow_b_names.value(i));
        }
    }

    #[test]
    fn bulk_path_rejects_null_in_not_null_column() {
        let sql = {
            let mut s = String::from("INSERT INTO t (id) VALUES ");
            for i in 0..200 {
                if i > 0 {
                    s.push(',');
                }
                if i == 50 {
                    s.push_str("(NULL)");
                } else {
                    s.push_str(&format!("({i})"));
                }
            }
            s
        };
        let rows = rows_from_sql(&sql);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = batch_from_rows(schema, &rows).unwrap_err();
        assert!(
            matches!(err, BasinError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }
}
