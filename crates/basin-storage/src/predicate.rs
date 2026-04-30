//! Read-time predicates.
//!
//! We define our own enum (rather than reusing `datafusion`'s `Expr` or
//! `parquet`'s `RowFilter` directly) so this crate stays small and so
//! callers in higher layers — once they exist — can map their richer
//! expression languages down to a constrained set we know how to push down.
//!
//! Supported today: column equality, less-than, greater-than against an
//! `Int64`, `UInt64`, or `Utf8` literal. Anything else is evaluated lazily
//! (i.e. not pushed into Parquet stats pruning) — see `apply_to_batch`.

use basin_common::{BasinError, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
}

#[derive(Clone, Debug)]
pub enum Predicate {
    Eq(String, ScalarValue),
    Gt(String, ScalarValue),
    Lt(String, ScalarValue),
}

impl Predicate {
    pub fn column(&self) -> &str {
        match self {
            Predicate::Eq(c, _) | Predicate::Gt(c, _) | Predicate::Lt(c, _) => c,
        }
    }
}

/// Apply a single predicate to a `RecordBatch` and return a boolean mask.
/// Used as a fallback when row-group / page pruning isn't enough; i.e. the
/// final per-row filter after Parquet has handed us back a coarse-grained
/// superset.
pub(crate) fn evaluate(
    batch: &arrow_array::RecordBatch,
    predicate: &Predicate,
) -> Result<arrow_array::BooleanArray> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
    use arrow_array::Array;
    use arrow_array::BooleanArray;

    let col_name = predicate.column();
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col_name}")))?;

    macro_rules! cmp_primitive {
        ($arrow_ty:ty, $val:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let v = $val;
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) $op v);
                }
            }
            b.finish()
        }};
    }

    let mask: BooleanArray = match (predicate, &predicate_value(predicate)) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Utf8(v))
        | (Predicate::Gt(_, _), ScalarValue::Utf8(v))
        | (Predicate::Lt(_, _), ScalarValue::Utf8(v)) => {
            let arr = col.as_string::<i32>();
            let op: fn(&str, &str) -> bool = match predicate {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
            };
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(op(arr.value(i), v.as_str()));
                }
            }
            b.finish()
        }
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) => {
            let arr = col.as_boolean();
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) == *v);
                }
            }
            b.finish()
        }
        (op, val) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate combination: {op:?} on {val:?}"
            )));
        }
    };

    Ok(mask)
}

fn predicate_value(p: &Predicate) -> ScalarValue {
    match p {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v.clone(),
    }
}
