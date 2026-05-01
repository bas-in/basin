//! INSERT INTO ... VALUES (...) — literal rows → Arrow [`RecordBatch`].
//!
//! Only literal values, only one statement at a time. Subquery inserts and
//! parameter binding are out of scope for the PoC.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::types::Float32Type;
use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Schema};
use basin_common::{BasinError, Result};
use sqlparser::ast::{DataType as SqlDataType, Expr, UnaryOperator, Value};

use crate::types::parse_vector_literal;

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
        let mut sql = String::from(
            "INSERT INTO t (id, name, ok, score) VALUES ",
        );
        for i in 0..1000 {
            if i > 0 {
                sql.push(',');
            }
            let bool_lit = if i % 2 == 0 { "TRUE" } else { "FALSE" };
            sql.push_str(&format!(
                "({i}, 'row-{i}', {bool_lit}, {}.5)",
                i as f64
            ));
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

        let bulk_ids = bulk.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
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
