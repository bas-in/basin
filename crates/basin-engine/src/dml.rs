//! INSERT INTO ... VALUES (...) — literal rows → Arrow [`RecordBatch`].
//!
//! Only literal values, only one statement at a time. Subquery inserts and
//! parameter binding are out of scope for the PoC.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::types::Float32Type;
use arrow_array::{ArrayRef, FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, Schema};
use basin_common::{BasinError, Result};
use sqlparser::ast::{DataType as SqlDataType, Expr, UnaryOperator, Value};

use crate::types::parse_vector_literal;

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
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
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
                let arr = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    row_iter, *dim,
                );
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

/// Decode a Postgres bytea literal into bytes. Accepts:
///   - `'\xff00'::bytea` (Cast wrapper around a `\x...` hex string)
///   - `'\xff00'`        (the same hex literal without an explicit cast)
///   - `NULL`            (returns `None`)
/// Anything else is an `InvalidSchema` error.
fn coerce_bytea(expr: &Expr) -> Result<Option<Vec<u8>>> {
    let inner = match expr {
        Expr::Cast { expr: inner, data_type: SqlDataType::Bytea, .. } => inner.as_ref(),
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
        BasinError::InvalidSchema(format!(
            "bytea literal must start with `\\x`, got {s:?}"
        ))
    })?;
    if hex.len() % 2 != 0 {
        return Err(BasinError::InvalidSchema(format!(
            "bytea hex string has odd length: {s:?}"
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let h = std::str::from_utf8(chunk).map_err(|_| {
            BasinError::InvalidSchema(format!("non-utf8 bytea hex: {s:?}"))
        })?;
        let byte = u8::from_str_radix(h, 16).map_err(|_| {
            BasinError::InvalidSchema(format!("bad bytea hex byte {h:?} in {s:?}"))
        })?;
        out.push(byte);
    }
    Ok(Some(out))
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
            let parsed: i64 = s
                .parse()
                .map_err(|e| BasinError::InvalidSchema(format!("bad integer literal {s:?}: {e}")))?;
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

fn coerce_string(expr: &Expr) -> Result<Option<String>> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::NationalStringLiteral(s))
        | Expr::Value(Value::EscapedStringLiteral(s)) => Ok(Some(s.clone())),
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
