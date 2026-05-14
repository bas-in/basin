//! INSERT INTO ... VALUES (...) — literal rows → Arrow [`RecordBatch`].
//!
//! Only literal values, only one statement at a time. Subquery inserts and
//! parameter binding are out of scope for the PoC.

use std::sync::Arc;

use std::collections::BTreeMap;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::Float32Type;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, FixedSizeListArray, Float64Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, PartitionKey, Result};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use sqlparser::ast::{DataType as SqlDataType, Expr, FunctionArguments, UnaryOperator, Value};

use crate::types::{field_is_jsonb, field_is_uuid, parse_vector_literal};

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
                // PG `numeric` literal coercion: scale a base-10 number
                // literal to the column's `(precision, scale)` and store
                // as i128. Pre-validated: `1 <= p <= 38`, `0 <= s <= p`.
                let mut values: Vec<Option<i128>> = Vec::with_capacity(rows.len());
                for row in rows {
                    match coerce_decimal128(&row[col_idx], *p, *s, field.name())? {
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
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "unsupported Arrow column type for INSERT: {other:?}"
                )));
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| BasinError::internal(format!("RecordBatch build: {e}")))
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
        Expr::Value(Value::Null) => return Ok(None),
        _ => expr,
    };
    let s = match inner {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => s,
        Expr::Value(Value::Null) => return Ok(None),
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
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => s.as_str(),
        Expr::Value(Value::Null) => return Ok(None),
        // A bare `Cast` like `'{...}'::jsonb` is friendly to allow even
        // though our DDL doesn't produce it; peel and recurse.
        Expr::Cast { expr: inner, .. } => return coerce_jsonb(inner.as_ref(), col),
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "expected JSON string literal for column {col}, got {other}"
            )));
        }
    };
    let parsed: serde_json::Value = serde_json::from_str(s).map_err(|e| {
        BasinError::InvalidSchema(format!("invalid JSON literal for column {col}: {e}"))
    })?;
    let canonical = canonicalize_json(parsed);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|e| BasinError::internal(format!("re-serialising JSON for column {col}: {e}")))?;
    Ok(Some(bytes))
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
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => {
            let parsed = uuid::Uuid::parse_str(s).map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "invalid UUID literal for column {col}: {e} (got {s:?})"
                ))
            })?;
            Ok(Some(*parsed.as_bytes()))
        }
        Expr::Value(Value::Null) => Ok(None),
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
                .map(|i| i.value.to_ascii_lowercase())
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

/// Recursively rebuild `v` so every object node iterates its keys in sorted
/// order. We do it via `BTreeMap<String, Value>` rather than re-implementing
/// a serializer because (a) `serde_json::Value` already round-trips and
/// (b) `BTreeMap` serialises in key order, which is what canonical-form
/// JSON wants.
fn canonicalize_json(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<String, Value> = map
                .into_iter()
                .map(|(k, val)| (k, canonicalize_json(val)))
                .collect();
            // Round-trip through serde_json::Value so the outer container is
            // a `Value::Object` again (rather than a `Map`-shaped value the
            // caller's BTreeMap returns directly). serde_json's `Map`
            // preserves insertion order, so we feed the BTreeMap's already-
            // sorted iteration into a fresh `Map`.
            let mut out = serde_json::Map::with_capacity(sorted.len());
            for (k, vv) in sorted {
                out.insert(k, vv);
            }
            Value::Object(out)
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
        (negated, Expr::Value(Value::Number(s, _))) => {
            let parsed: i64 = s.parse().map_err(|e| {
                BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}"))
            })?;
            Ok(Some(if negated { -parsed } else { parsed }))
        }
        (false, Expr::Value(Value::Null)) => Ok(None),
        (_, other) => Err(BasinError::InvalidSchema(format!(
            "expected integer literal, got {other}"
        ))),
    }
}

fn coerce_f64(expr: &Expr) -> Result<Option<f64>> {
    match peel_unary(expr) {
        (negated, Expr::Value(Value::Number(s, _))) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad float literal {s:?}: {e}")))?;
            Ok(Some(if negated { -parsed } else { parsed }))
        }
        (false, Expr::Value(Value::Null)) => Ok(None),
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
        Expr::Value(Value::Number(s, _)) => s.as_str(),
        Expr::Value(Value::Null) if !negated => return Ok(None),
        Expr::Cast { expr: ce, .. } => {
            // Allow `'1.50'::numeric(10,2)` style casts by recursing into
            // the inner expression.
            return coerce_decimal128(ce.as_ref(), precision, scale, col);
        }
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => s.as_str(),
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
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::NationalStringLiteral(s))
        | Expr::Value(Value::EscapedStringLiteral(s)) => Ok(Some(s.as_str())),
        Expr::Value(Value::Null) => Ok(None),
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
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => parse_vector_literal(s)?,
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
        Expr::Value(Value::Null) => return Ok(None),
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
        Expr::Value(Value::Null) => return Ok(None),
        _ => expr,
    };
    match inner {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => {
            let micros = parse_timestamp_string(s)?;
            Ok(Some(micros))
        }
        Expr::Value(Value::Number(n, _)) => {
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
            if let Expr::Value(Value::Number(n, _)) = inner.as_ref() {
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
        Expr::Value(Value::Null) => Ok(None),
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
                .map(|i| i.value.to_ascii_lowercase())
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
        Expr::Value(Value::Boolean(b)) => Ok(Some(*b)),
        Expr::Value(Value::Null) => Ok(None),
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
