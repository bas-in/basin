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
//!
//! [`CompoundPredicate`] sits one level up: an `AND`/`OR`/`NOT` tree over
//! [`Predicate`] atoms plus `IS NULL` / `IS NOT NULL` / `IN`. The DML
//! mutation path uses it for richer WHERE clauses; the read path still
//! flattens to `Vec<Predicate>` because Parquet row-group pruning works
//! best on a conjunction of atoms.

use std::collections::BTreeMap;

use arrow_array::{builder::BooleanBuilder, Array, BooleanArray, RecordBatch};
use basin_common::{BasinError, Result};

use crate::data_file::ColumnStats;

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
    /// Anchored prefix match: column starts with the given literal.
    /// Translates `WHERE col LIKE 'foo%'` and `col ILIKE 'foo%'` when the
    /// pattern is a pure prefix (no `%`/`_` before the trailing `%`).
    /// `case_insensitive=true` is ILIKE (ASCII-only case folding).
    StartsWith {
        column: String,
        prefix: String,
        case_insensitive: bool,
    },
    /// Membership test: `column IN (k0, k1, …)` over an `Int64`-family column.
    /// The key vector is the deduplicated, ASC-sorted set of i64 keys (the
    /// engine sorts + dedups before constructing this so the storage skip
    /// path can binary-search both the file's PK column AND the key vector).
    ///
    /// NOT pushable into a Vortex scan expression — it is evaluated Arrow-side
    /// only (see `vortex_filter_expr`, which leaves it for the post-decode
    /// pass). NULL column elements never match (PG `IN` semantics).
    InInt64(String, Vec<i64>),
}

impl Predicate {
    pub fn column(&self) -> &str {
        match self {
            Predicate::Eq(c, _) | Predicate::Gt(c, _) | Predicate::Lt(c, _) => c,
            Predicate::StartsWith { column, .. } => column,
            Predicate::InInt64(c, _) => c,
        }
    }
}

/// Normalize a comparison-kernel result so NULL entries become `false`.
///
/// Arrow's `cmp` kernels propagate NULL: a comparison against a NULL column
/// element yields NULL. Our predicate contract (matching the prior scalar
/// `BooleanBuilder` loop and Parquet's `RowFilter`) treats a NULL operand as
/// "does not match" → `false`. We fold the validity bitmap into the values:
/// `result = values AND validity`, then drop the null buffer. The fast path
/// (no nulls) returns the array unchanged.
fn null_to_false(arr: BooleanArray) -> BooleanArray {
    if arr.null_count() == 0 {
        return arr;
    }
    let (values, nulls) = arr.into_parts();
    // `values & validity`: a slot is true only where it compared true AND was
    // non-null. Then the result carries no null buffer.
    let validity = nulls
        .expect("null_count > 0 implies a null buffer")
        .into_inner();
    BooleanArray::new(&values & &validity, None)
}

/// Apply a single predicate to a `RecordBatch` and return a boolean mask.
/// Used as a fallback when row-group / page pruning isn't enough; i.e. the
/// final per-row filter after Parquet has handed us back a coarse-grained
/// superset.
pub fn evaluate(
    batch: &arrow_array::RecordBatch,
    predicate: &Predicate,
) -> Result<arrow_array::BooleanArray> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt64Type,
    };
    use arrow_array::Array;
    use arrow_array::BooleanArray;
    use arrow_schema::DataType as Dt;

    let col_name = predicate.column();
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col_name}")))?;

    // StartsWith on Utf8/LargeUtf8 — early dispatch so we don't enter the
    // generic `(predicate, scalar)` matcher below (StartsWith has no
    // associated ScalarValue).
    if let Predicate::StartsWith {
        prefix,
        case_insensitive,
        ..
    } = predicate
    {
        return Ok(eval_starts_with(col, prefix, *case_insensitive));
    }

    // InInt64 membership — no associated ScalarValue, so dispatch early like
    // StartsWith. NULL column elements never match (the validity check below).
    if let Predicate::InInt64(_, keys) = predicate {
        return Ok(eval_in_int64(col, keys));
    }

    // PG coerces an unknown/text literal to the column's type before
    // comparing (e.g. `WHERE int_col = '1'` parses '1' as int8). Without
    // this, a `ScalarValue::Utf8` against a numeric/boolean column would
    // fall through to the generic Utf8 arm and call `as_string()` on a
    // non-string Arrow array — an unchecked downcast that panics
    // ("string array", arrow cast.rs). Coerce here and re-dispatch so the
    // typed comparison arms handle it. Text/timestamp/UUID columns keep the
    // Utf8 scalar (they have dedicated arms below).
    if let Some(coerced) = coerce_utf8_scalar_for_column(predicate, col.data_type())? {
        return evaluate(batch, &coerced);
    }

    // Map a Rust comparison token to Arrow's vectorized SIMD comparison
    // kernel (`arrow::compute::kernels::cmp`). These run ~1-2 orders of
    // magnitude faster than a scalar per-element `BooleanBuilder` loop in
    // both debug and release, which is the hot path when a non-PK predicate
    // is applied in Arrow over a fully-decoded stripe (the unfiltered-decode
    // reuse path and every cold-tier WHERE that isn't fully pushed down).
    macro_rules! cmp_kernel {
        (==) => {
            arrow::compute::kernels::cmp::eq
        };
        (>) => {
            arrow::compute::kernels::cmp::gt
        };
        (<) => {
            arrow::compute::kernels::cmp::lt
        };
    }

    // Vectorized primitive comparison: column array vs a single scalar.
    // Arrow's kernel returns NULL where the column element is NULL; our
    // contract maps NULL → false (the same result the old scalar loop
    // produced via `is_null → append_value(false)`), so we normalize the
    // result's validity into the values buffer with `null_to_false`.
    macro_rules! cmp_primitive {
        ($arrow_ty:ty, $val:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let scalar = arrow_array::PrimitiveArray::<$arrow_ty>::new_scalar($val);
            let cmp = cmp_kernel!($op)(arr, &scalar)
                .map_err(|e| BasinError::storage(format!("predicate cmp: {e}")))?;
            null_to_false(cmp)
        }};
    }

    // Compare an Int64 scalar against a narrower integer column (Int32 /
    // Int16) or a Timestamp column (raw i64). We cast the column to Int64
    // (a vectorized kernel) then compare against the i64 scalar.
    macro_rules! cmp_narrow_int_as_i64 {
        ($arrow_ty:ty, $v:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let widened: arrow_array::Int64Array =
                arrow::compute::kernels::cast::cast(arr, &Dt::Int64)
                    .map_err(|e| BasinError::storage(format!("predicate widen: {e}")))?
                    .as_primitive::<Int64Type>()
                    .clone();
            let scalar = arrow_array::Int64Array::new_scalar($v);
            let cmp = cmp_kernel!($op)(&widened, &scalar)
                .map_err(|e| BasinError::storage(format!("predicate cmp: {e}")))?;
            null_to_false(cmp)
        }};
    }

    // Compare a Float64 scalar against a Float32 column (widen to f64).
    macro_rules! cmp_f32_as_f64 {
        ($v:expr, $op:tt) => {{
            let arr = col.as_primitive::<Float32Type>();
            let widened: arrow_array::Float64Array =
                arrow::compute::kernels::cast::cast(arr, &Dt::Float64)
                    .map_err(|e| BasinError::storage(format!("predicate widen: {e}")))?
                    .as_primitive::<Float64Type>()
                    .clone();
            let scalar = arrow_array::Float64Array::new_scalar($v);
            let cmp = cmp_kernel!($op)(&widened, &scalar)
                .map_err(|e| BasinError::storage(format!("predicate cmp: {e}")))?;
            null_to_false(cmp)
        }};
    }

    let mask: BooleanArray = match (predicate, &predicate_value(predicate)) {
        // INT8 column vs Int64 scalar — direct.
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int64 => {
            cmp_primitive!(Int64Type, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int64 => {
            cmp_primitive!(Int64Type, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int64 => {
            cmp_primitive!(Int64Type, *v, <)
        }
        // INT4 column vs Int64 scalar — widen column element to i64.
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int32 => {
            cmp_narrow_int_as_i64!(Int32Type, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int32 => {
            cmp_narrow_int_as_i64!(Int32Type, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int32 => {
            cmp_narrow_int_as_i64!(Int32Type, *v, <)
        }
        // INT2 column vs Int64 scalar — widen column element to i64.
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int16 => {
            cmp_narrow_int_as_i64!(Int16Type, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int16 => {
            cmp_narrow_int_as_i64!(Int16Type, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) if *col.data_type() == Dt::Int16 => {
            cmp_narrow_int_as_i64!(Int16Type, *v, <)
        }
        // Timestamp(Microsecond) column vs Int64 scalar (µs since epoch).
        // Arrow's TimestampMicrosecondType stores raw i64 µs; comparing an
        // Int64 scalar directly gives the correct result.
        (Predicate::Eq(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMicrosecondType, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMicrosecondType, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMicrosecondType, *v, <)
        }
        // Timestamp(Nanosecond) column vs Int64 scalar (ns since epoch).
        (Predicate::Eq(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Nanosecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampNanosecondType, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Nanosecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampNanosecondType, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Nanosecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampNanosecondType, *v, <)
        }
        // Timestamp(Millisecond) column vs Int64 scalar (ms since epoch).
        (Predicate::Eq(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Millisecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMillisecondType, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Millisecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMillisecondType, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Millisecond, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampMillisecondType, *v, <)
        }
        // Timestamp(Second) column vs Int64 scalar (s since epoch).
        (Predicate::Eq(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Second, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampSecondType, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Second, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampSecondType, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v))
            if matches!(
                col.data_type(),
                Dt::Timestamp(arrow_schema::TimeUnit::Second, _)
            ) =>
        {
            cmp_narrow_int_as_i64!(TimestampSecondType, *v, <)
        }
        // Int64 / UInt64 fallback. The earlier arms cover Int64/Int32/Int16
        // and every Timestamp unit; ANY other column type reaching this
        // point (Date32, Decimal128, UInt32, Duration, Interval, Time*,
        // …) would previously have hit an unchecked
        // `as_primitive::<Int64Type>()` downcast and panicked — a DoS-class
        // bug under PG's permissive numeric coercion. Bubble a typed error
        // instead. Same shape as the Utf8 fallback at the tail of the
        // string arm.
        (Predicate::Eq(_, _), ScalarValue::Int64(v))
        | (Predicate::Gt(_, _), ScalarValue::Int64(v))
        | (Predicate::Lt(_, _), ScalarValue::Int64(v)) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate: integer literal {v} against column type {:?}",
                col.data_type()
            )));
        }
        (Predicate::Eq(_, _), ScalarValue::UInt64(v)) if *col.data_type() == Dt::UInt64 => {
            cmp_primitive!(UInt64Type, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::UInt64(v)) if *col.data_type() == Dt::UInt64 => {
            cmp_primitive!(UInt64Type, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::UInt64(v)) if *col.data_type() == Dt::UInt64 => {
            cmp_primitive!(UInt64Type, *v, <)
        }
        (Predicate::Eq(_, _), ScalarValue::UInt64(v))
        | (Predicate::Gt(_, _), ScalarValue::UInt64(v))
        | (Predicate::Lt(_, _), ScalarValue::UInt64(v)) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate: unsigned integer literal {v} against column type {:?}",
                col.data_type()
            )));
        }
        // FLOAT8 column vs Float64 scalar — direct.
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float64 => {
            cmp_primitive!(Float64Type, *v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float64 => {
            cmp_primitive!(Float64Type, *v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float64 => {
            cmp_primitive!(Float64Type, *v, <)
        }
        // FLOAT4 column vs Float64 scalar — widen column element to f64.
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float32 => {
            cmp_f32_as_f64!(*v, ==)
        }
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float32 => {
            cmp_f32_as_f64!(*v, >)
        }
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) if *col.data_type() == Dt::Float32 => {
            cmp_f32_as_f64!(*v, <)
        }
        // Float64 fallback. The guarded arms above handle Dt::Float64 and
        // Dt::Float32; any other column type (Float16, Decimal*, …) would
        // previously have hit an unchecked `as_primitive::<Float64Type>()`
        // downcast. Bubble a typed error instead.
        (Predicate::Eq(_, _), ScalarValue::Float64(v))
        | (Predicate::Gt(_, _), ScalarValue::Float64(v))
        | (Predicate::Lt(_, _), ScalarValue::Float64(v)) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate: float literal {v} against column type {:?}",
                col.data_type()
            )));
        }
        // UUID column: Arrow stores UUIDs as FixedSizeBinary(16) (RFC 4122
        // big-endian bytes). The predicate value arrives as a
        // `ScalarValue::Utf8` canonical hyphenated string (e.g.
        // "550e8400-e29b-41d4-a716-446655440000") — coerce it to 16 bytes
        // before comparing. Only `Eq` is meaningful for UUIDs; `Gt`/`Lt`
        // fall through to the generic byte-lexicographic path below.
        (Predicate::Eq(_, _), ScalarValue::Utf8(v))
            if *col.data_type() == Dt::FixedSizeBinary(16) =>
        {
            let needle: [u8; 16] = match uuid::Uuid::parse_str(v.as_str()) {
                Ok(u) => *u.as_bytes(),
                Err(e) => {
                    return Err(BasinError::storage(format!(
                        "predicate: invalid UUID literal {v:?}: {e}"
                    )));
                }
            };
            use arrow_array::cast::AsArray;
            let arr = col.as_fixed_size_binary();
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) == needle);
                }
            }
            b.finish()
        }
        // Timestamp column vs Utf8 scalar — parse the string as a PG/ISO-8601
        // timestamp and compare as i64 in the COLUMN's time unit.  This
        // handles pushdown of queries like
        // `WHERE ts < '2023-02-01 00:00:00+00'` where the literal arrives as a
        // Utf8 scalar from `fast_select::literal_value`.
        //
        // We parse the literal once to microseconds (the canonical PG storage
        // unit) and then rescale to whatever unit the column physically uses
        // — Ns/Ms/Sec all reach this arm and previously panicked on an
        // unchecked `as_primitive::<TimestampMicrosecondType>()` downcast.
        (Predicate::Eq(_, _), ScalarValue::Utf8(v))
        | (Predicate::Gt(_, _), ScalarValue::Utf8(v))
        | (Predicate::Lt(_, _), ScalarValue::Utf8(v))
            if matches!(col.data_type(), Dt::Timestamp(..)) =>
        {
            // Parse the timestamp string into microseconds since epoch.
            let us: i64 = parse_ts_str_to_us(v);
            let op: fn(i64, i64) -> bool = match predicate {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
                // Outer match arm above gates on Eq|Gt|Lt — StartsWith
                // never reaches here. StartsWith on a Timestamp column is
                // a type-mismatch and is rejected upstream.
                Predicate::StartsWith { .. } => {
                    unreachable!("StartsWith routed to timestamp-utf8 path")
                }
                Predicate::InInt64(..) => {
                    unreachable!("InInt64 dispatched before the scalar matcher")
                }
            };

            // Helper: scan a primitive timestamp column against a rescaled
            // needle. Saturating arithmetic on the rescale keeps the sentinel
            // `i64::MIN` (returned by `parse_ts_str_to_us` on parse failure)
            // from overflowing; the comparison still evaluates to false on
            // every real timestamp, matching today's semantics.
            macro_rules! cmp_ts {
                ($arrow_ty:ty, $needle:expr) => {{
                    let arr = col.as_primitive::<$arrow_ty>();
                    let n: i64 = $needle;
                    let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            b.append_value(false);
                        } else {
                            b.append_value(op(arr.value(i), n));
                        }
                    }
                    b.finish()
                }};
            }

            match col.data_type() {
                Dt::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                    cmp_ts!(TimestampMicrosecondType, us)
                }
                Dt::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                    cmp_ts!(TimestampNanosecondType, us.saturating_mul(1_000))
                }
                Dt::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                    // Floor-divide so a literal like `…:00.000001` (1 µs)
                    // doesn't accidentally round up to 1 ms.
                    cmp_ts!(TimestampMillisecondType, us.div_euclid(1_000))
                }
                Dt::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                    cmp_ts!(TimestampSecondType, us.div_euclid(1_000_000))
                }
                // Outer guard restricts us to `Dt::Timestamp(..)`.
                _ => unreachable!("non-Timestamp routed to timestamp-utf8 path"),
            }
        }
        // String column vs Utf8 scalar — dispatch on the column's string
        // variant. `Utf8` (i32 offsets), `LargeUtf8` (i64 offsets), and
        // `Utf8View` are all valid string Arrow encodings and an unchecked
        // `as_string::<i32>()` on the latter two panics.
        (Predicate::Eq(_, _), ScalarValue::Utf8(v))
        | (Predicate::Gt(_, _), ScalarValue::Utf8(v))
        | (Predicate::Lt(_, _), ScalarValue::Utf8(v)) => {
            let op: fn(&str, &str) -> bool = match predicate {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
                // Outer match gates on Eq|Gt|Lt; StartsWith handled in
                // the dedicated arm above (see line ~85).
                Predicate::StartsWith { .. } => {
                    unreachable!("StartsWith routed to generic utf8 cmp path")
                }
                Predicate::InInt64(..) => {
                    unreachable!("InInt64 dispatched before the scalar matcher")
                }
            };
            macro_rules! cmp_str {
                ($arr:expr, $needle:expr) => {{
                    let arr = $arr;
                    let n: &str = $needle;
                    let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            b.append_value(false);
                        } else {
                            b.append_value(op(arr.value(i), n));
                        }
                    }
                    b.finish()
                }};
            }
            match col.data_type() {
                Dt::Utf8 => cmp_str!(col.as_string::<i32>(), v.as_str()),
                Dt::LargeUtf8 => cmp_str!(col.as_string::<i64>(), v.as_str()),
                Dt::Utf8View => cmp_str!(col.as_string_view(), v.as_str()),
                // Non-string column type with a Utf8 scalar that wasn't
                // coerced upstream — this is a user-facing type error, not
                // a panic-class bug. Bubble it up so the caller surfaces a
                // PG-style "operator does not exist" message.
                other => {
                    return Err(BasinError::storage(format!(
                        "unsupported predicate: text literal {v:?} against column type {other:?}"
                    )));
                }
            }
        }
        // Boolean scalar — gate on the column actually being Boolean.
        // Without the guard, `col.as_boolean()` would panic on any other
        // physical type (Int*, Utf8, Decimal*, …) — same DoS-class shape
        // as the int / float / string fallbacks above.
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) if *col.data_type() == Dt::Boolean => {
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
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate: boolean literal {v} against column type {:?}",
                col.data_type()
            )));
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
        // StartsWith / InInt64 are dispatched before this helper is consulted
        // (see `evaluate`). Returning a sentinel keeps the function total.
        Predicate::StartsWith { prefix, .. } => ScalarValue::Utf8(prefix.clone()),
        Predicate::InInt64(_, keys) => ScalarValue::Int64(keys.first().copied().unwrap_or(0)),
    }
}

/// Vectorized `column IN (keys)` membership over an Int64-family column.
///
/// `keys` is the engine-provided ASC-sorted, deduplicated key set. We widen
/// narrow integer columns (Int8/16/32) to i64 once (a vectorized cast), then
/// test each element by binary-search against `keys`. NULL elements yield
/// `false` (PG `IN` semantics). This is the *fallback* membership path used
/// when the sorted-skip take optimisation does not apply; it still beats the
/// previous per-row `HashSet`-per-row approach because the key set is searched
/// with a cache-friendly sorted-slice `binary_search` and no per-row hashing.
fn eval_in_int64(col: &arrow_array::ArrayRef, keys: &[i64]) -> arrow_array::BooleanArray {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, BooleanArray};
    use arrow_schema::DataType as Dt;

    let n = col.len();
    if keys.is_empty() {
        // Empty IN-list matches nothing.
        return BooleanArray::from(vec![false; n]);
    }

    // Widen to an Int64 view (cheap clone for native Int64; vectorized cast
    // otherwise). On an unexpected non-integer column type we return an
    // all-false mask rather than panic — the engine only constructs InInt64
    // for integer PK columns, so this is purely defensive.
    let widened: arrow_array::Int64Array = match col.data_type() {
        Dt::Int64 => col.as_primitive::<Int64Type>().clone(),
        Dt::Int32 | Dt::Int16 | Dt::Int8 => {
            match arrow::compute::kernels::cast::cast(col.as_ref(), &Dt::Int64) {
                Ok(a) => a.as_primitive::<Int64Type>().clone(),
                Err(_) => return BooleanArray::from(vec![false; n]),
            }
        }
        // Defensive widen attempt for any other integer-castable type.
        _ => match arrow::compute::kernels::cast::cast(col.as_ref(), &Dt::Int64) {
            Ok(a) => a.as_primitive::<Int64Type>().clone(),
            Err(_) => return BooleanArray::from(vec![false; n]),
        },
    };

    let mut b = arrow_array::builder::BooleanBuilder::with_capacity(n);
    for i in 0..n {
        if widened.is_null(i) {
            b.append_value(false);
        } else {
            b.append_value(keys.binary_search(&widened.value(i)).is_ok());
        }
    }
    b.finish()
}

/// Compute the row indices of an Int64-family `column` whose values are present
/// in `keys`, exploiting that the column is ASC-sorted within `col` (the
/// per-file decode chunk). For each key we `partition_point` into the column to
/// locate its run, then collect every matching row index (handles duplicate PK
/// values defensively, though PKs are unique). `keys` MUST be ASC-sorted +
/// deduplicated. NULLs sort last in Arrow ASC order and are never matched.
///
/// Returns indices in ascending order (the take preserves PK order). When the
/// column type is not a widenable integer, returns `None` so the caller falls
/// back to the mask path.
pub(crate) fn sorted_in_int64_indices(
    col: &arrow_array::ArrayRef,
    keys: &[i64],
) -> Option<Vec<u32>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::Array;
    use arrow_schema::DataType as Dt;

    let widened: arrow_array::Int64Array = match col.data_type() {
        Dt::Int64 => col.as_primitive::<Int64Type>().clone(),
        Dt::Int32 | Dt::Int16 | Dt::Int8 => {
            arrow::compute::kernels::cast::cast(col.as_ref(), &Dt::Int64)
                .ok()?
                .as_primitive::<Int64Type>()
                .clone()
        }
        _ => return None,
    };

    let n = widened.len();
    // The non-null prefix length. Arrow ASC sort places NULLs last; the
    // writer's clustered sort follows the same convention, so the valid
    // (non-null) values occupy a sorted prefix `[0, valid_len)`. We bound
    // the binary search to that prefix. If NULLs were interleaved (they are
    // not for a clustered PK), search would still be correct over the whole
    // array only if values are globally sorted — to stay safe we restrict to
    // the leading non-null sorted run.
    let valid_len = if widened.null_count() == 0 {
        n
    } else {
        // First null index = length of the leading non-null prefix.
        let mut p = 0usize;
        while p < n && widened.is_valid(p) {
            p += 1;
        }
        p
    };

    let mut out: Vec<u32> = Vec::with_capacity(keys.len());
    for &k in keys {
        // Locate the first index >= k within the sorted non-null prefix.
        let mut lo = 0usize;
        let mut hi = valid_len;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if widened.value(mid) < k {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Collect the (possibly multi-row) run equal to k.
        let mut j = lo;
        while j < valid_len && widened.value(j) == k {
            out.push(j as u32);
            j += 1;
        }
    }
    // Indices are emitted in key order; sort so the take preserves the file's
    // ascending PK order (keys are sorted, so this is already nearly sorted —
    // an unstable sort over a small vec is cheap).
    out.sort_unstable();
    Some(out)
}

/// PG-style coercion of an unknown/text literal to the column's type.
///
/// When a comparison's literal arrives as [`ScalarValue::Utf8`] (every
/// single-quoted SQL literal does) but the column is numeric or boolean,
/// PostgreSQL coerces the text literal to the column type before comparing
/// (`WHERE int_col = '1'` parses `'1'` as int8). This returns a rewritten
/// [`Predicate`] carrying the coerced scalar so the caller can re-dispatch
/// through [`evaluate`]'s typed arms.
///
/// Returns `Ok(None)` when no coercion is needed — the scalar isn't `Utf8`,
/// or the column type already has a dedicated `Utf8` arm (text, timestamp,
/// UUID/FixedSizeBinary). Returns `Err` (a typed SQL error, never a panic)
/// when the literal can't be parsed as the column type, matching PG's
/// "invalid input syntax for type …".
fn coerce_utf8_scalar_for_column(
    predicate: &Predicate,
    col_type: &arrow_schema::DataType,
) -> Result<Option<Predicate>> {
    use arrow_schema::DataType as Dt;

    // Only Eq/Gt/Lt carry a comparable scalar; StartsWith is handled earlier.
    let (col, val) = match predicate {
        Predicate::Eq(c, v) | Predicate::Gt(c, v) | Predicate::Lt(c, v) => (c, v),
        // StartsWith / InInt64 carry no single coercible scalar — InInt64 is
        // already typed Int64 and dispatched before this helper runs.
        Predicate::StartsWith { .. } | Predicate::InInt64(..) => return Ok(None),
    };
    let ScalarValue::Utf8(s) = val else {
        return Ok(None);
    };

    // Determine the target scalar for the column type. Text, timestamp,
    // and UUID columns keep the Utf8 scalar (dedicated arms handle them).
    let coerced: ScalarValue = match col_type {
        Dt::Int8 | Dt::Int16 | Dt::Int32 | Dt::Int64 => {
            let n: i64 = s.trim().parse().map_err(|_| {
                BasinError::storage(format!("invalid input syntax for type integer: {s:?}"))
            })?;
            ScalarValue::Int64(n)
        }
        Dt::UInt8 | Dt::UInt16 | Dt::UInt32 | Dt::UInt64 => {
            let n: u64 = s.trim().parse().map_err(|_| {
                BasinError::storage(format!("invalid input syntax for type integer: {s:?}"))
            })?;
            ScalarValue::UInt64(n)
        }
        Dt::Float16 | Dt::Float32 | Dt::Float64 => {
            let n: f64 = s.trim().parse().map_err(|_| {
                BasinError::storage(format!(
                    "invalid input syntax for type double precision: {s:?}"
                ))
            })?;
            ScalarValue::Float64(n)
        }
        Dt::Boolean => {
            // PG accepts t/f/true/false/yes/no/on/off/1/0 (case-insensitive).
            let b = match s.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "yes" | "on" | "1" => true,
                "f" | "false" | "no" | "off" | "0" => false,
                _ => {
                    return Err(BasinError::storage(format!(
                        "invalid input syntax for type boolean: {s:?}"
                    )));
                }
            };
            ScalarValue::Boolean(b)
        }
        // Utf8/LargeUtf8, Timestamp(..), FixedSizeBinary (UUID), and any
        // other type keep the Utf8 scalar — no coercion needed/possible here.
        _ => return Ok(None),
    };

    let col = col.clone();
    Ok(Some(match predicate {
        Predicate::Eq(..) => Predicate::Eq(col, coerced),
        Predicate::Gt(..) => Predicate::Gt(col, coerced),
        Predicate::Lt(..) => Predicate::Lt(col, coerced),
        Predicate::StartsWith { .. } => unreachable!("StartsWith handled above"),
        Predicate::InInt64(..) => unreachable!("InInt64 handled above"),
    }))
}

/// Evaluate `StartsWith` against a Utf8/LargeUtf8 column. Anything else
/// (e.g. dictionary-encoded string, FixedSizeBinary) returns an all-false
/// mask — caller still gets correct semantics (no spurious matches) and we
/// avoid a panic on unexpected column types.
fn eval_starts_with(
    col: &std::sync::Arc<dyn arrow_array::Array>,
    prefix: &str,
    case_insensitive: bool,
) -> arrow_array::BooleanArray {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType as Dt;

    let dt = col.data_type();
    let mut b = arrow_array::builder::BooleanBuilder::with_capacity(col.len());

    // Lowercase the prefix once when ILIKE.
    let prefix_lc: String;
    let prefix_ref: &str = if case_insensitive {
        prefix_lc = prefix.to_ascii_lowercase();
        prefix_lc.as_str()
    } else {
        prefix
    };

    match dt {
        Dt::Utf8 => {
            let arr = col.as_string::<i32>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else if case_insensitive {
                    let v = arr.value(i);
                    // Cheap length check before lowercasing to avoid
                    // allocating on rows that obviously can't match.
                    if v.len() < prefix_ref.len() {
                        b.append_value(false);
                    } else {
                        b.append_value(v.to_ascii_lowercase().starts_with(prefix_ref));
                    }
                } else {
                    b.append_value(arr.value(i).starts_with(prefix_ref));
                }
            }
        }
        Dt::LargeUtf8 => {
            let arr = col.as_string::<i64>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else if case_insensitive {
                    let v = arr.value(i);
                    if v.len() < prefix_ref.len() {
                        b.append_value(false);
                    } else {
                        b.append_value(v.to_ascii_lowercase().starts_with(prefix_ref));
                    }
                } else {
                    b.append_value(arr.value(i).starts_with(prefix_ref));
                }
            }
        }
        _ => {
            // Unsupported column type for prefix match — return all-false so
            // the caller still gets correct semantics.
            for _ in 0..col.len() {
                b.append_value(false);
            }
        }
    }
    b.finish()
}

/// A boolean expression over [`Predicate`] atoms plus null-checks and `IN`
/// lists. The DML mutation path parses the WHERE clause into this shape;
/// reads still consume `Vec<Predicate>` (a conjunction of atoms) because
/// Parquet pruning is friendliest there.
#[derive(Clone, Debug)]
pub enum CompoundPredicate {
    Atom(Predicate),
    And(Vec<CompoundPredicate>),
    Or(Vec<CompoundPredicate>),
    Not(Box<CompoundPredicate>),
    /// `<col> IS NULL`.
    IsNull(String),
    /// `<col> IS NOT NULL`.
    IsNotNull(String),
    /// `<col> IN (lit, lit, ...)`. An empty list always evaluates to
    /// false, matching SQL semantics.
    In(String, Vec<ScalarValue>),
}

/// Apply a [`CompoundPredicate`] to a batch and return a boolean mask the
/// same length as the batch. NULL outcomes inside atoms are preserved (a
/// NULL on either operand of a comparison yields a NULL mask bit, just as
/// Postgres returns UNKNOWN). Logical combinators follow Kleene's
/// three-valued logic:
///   - `NULL AND false` → false
///   - `NULL AND true`  → NULL
///   - `NULL OR true`   → true
///   - `NULL OR false`  → NULL
///   - `NOT NULL`       → NULL
///
/// Three-valued logic matters for DELETE: a NULL mask bit means "we don't
/// know if this row matched", and the caller must conservatively keep
/// such rows (see `dml_mutate::invert_mask`).
pub fn evaluate_compound(batch: &RecordBatch, pred: &CompoundPredicate) -> Result<BooleanArray> {
    match pred {
        CompoundPredicate::Atom(p) => evaluate(batch, p),
        CompoundPredicate::And(children) => {
            if children.is_empty() {
                // Vacuously true.
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            }
            let mut acc: Option<BooleanArray> = None;
            for c in children {
                let m = evaluate_compound(batch, c)?;
                acc = Some(match acc {
                    None => m,
                    Some(prev) => and_kleene(&prev, &m),
                });
            }
            Ok(acc.expect("at least one child"))
        }
        CompoundPredicate::Or(children) => {
            if children.is_empty() {
                // Vacuously false.
                return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
            }
            let mut acc: Option<BooleanArray> = None;
            for c in children {
                let m = evaluate_compound(batch, c)?;
                acc = Some(match acc {
                    None => m,
                    Some(prev) => or_kleene(&prev, &m),
                });
            }
            Ok(acc.expect("at least one child"))
        }
        CompoundPredicate::Not(inner) => {
            let m = evaluate_compound(batch, inner)?;
            Ok(not_kleene(&m))
        }
        CompoundPredicate::IsNull(col) => is_null_mask(batch, col, true),
        CompoundPredicate::IsNotNull(col) => is_null_mask(batch, col, false),
        CompoundPredicate::In(col, values) => in_mask(batch, col, values),
    }
}

fn and_kleene(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        let av = if a.is_null(i) { None } else { Some(a.value(i)) };
        let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
        match (av, bv) {
            (Some(false), _) | (_, Some(false)) => out.append_value(false),
            (Some(true), Some(true)) => out.append_value(true),
            _ => out.append_null(),
        }
    }
    out.finish()
}

fn or_kleene(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        let av = if a.is_null(i) { None } else { Some(a.value(i)) };
        let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
        match (av, bv) {
            (Some(true), _) | (_, Some(true)) => out.append_value(true),
            (Some(false), Some(false)) => out.append_value(false),
            _ => out.append_null(),
        }
    }
    out.finish()
}

fn not_kleene(a: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            out.append_null();
        } else {
            out.append_value(!a.value(i));
        }
    }
    out.finish()
}

fn is_null_mask(batch: &RecordBatch, col: &str, want_null: bool) -> Result<BooleanArray> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col}")))?;
    let mut out = BooleanBuilder::with_capacity(arr.len());
    for i in 0..arr.len() {
        out.append_value(arr.is_null(i) == want_null);
    }
    Ok(out.finish())
}

fn in_mask(batch: &RecordBatch, col: &str, values: &[ScalarValue]) -> Result<BooleanArray> {
    if values.is_empty() {
        return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
    }
    // Reuse the per-atom evaluator: build an OR over `Eq(col, v)` atoms.
    // This keeps type-coercion centralised in `evaluate`.
    let or_pred = CompoundPredicate::Or(
        values
            .iter()
            .map(|v| CompoundPredicate::Atom(Predicate::Eq(col.to_string(), v.clone())))
            .collect(),
    );
    evaluate_compound(batch, &or_pred)
}

/// Coarse outcome of pruning a predicate against a single file's stats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneOutcome {
    /// Stats prove no row in the file can match. Caller may skip the file.
    NoMatch,
    /// Stats prove every row in the file matches. Caller can shortcut for
    /// DELETE (drop the file outright); UPDATE still needs to read for SET
    /// application.
    AllMatch,
    /// Stats are inconclusive (or the predicate isn't prunable). Caller
    /// must read the file and evaluate per row.
    Mixed,
}

impl PruneOutcome {
    fn invert(self) -> Self {
        match self {
            PruneOutcome::NoMatch => PruneOutcome::AllMatch,
            PruneOutcome::AllMatch => PruneOutcome::NoMatch,
            PruneOutcome::Mixed => PruneOutcome::Mixed,
        }
    }
}

/// Decide whether a file with the supplied per-column stats can be pruned
/// against `pred`. The caller passes the table's Arrow schema so we can
/// decode the bytes Parquet stores in `min_bytes` / `max_bytes`.
///
/// CORRECTNESS RULE: when in doubt, return [`PruneOutcome::Mixed`] — that
/// forces the caller to read and re-evaluate, which is wasteful but never
/// loses data. Returning `NoMatch` for a file that contains a matching row
/// is a data-loss bug.
pub fn evaluate_compound_for_pruning(
    pred: &CompoundPredicate,
    stats: &BTreeMap<String, ColumnStats>,
    schema: &arrow_schema::Schema,
    row_count: u64,
) -> PruneOutcome {
    match pred {
        CompoundPredicate::Atom(atom) => prune_atom(atom, stats, schema),
        CompoundPredicate::And(children) => {
            // AND: any NoMatch child proves the whole thing is NoMatch.
            // Otherwise, AllMatch only if every child is AllMatch.
            let mut all_match = true;
            for c in children {
                match evaluate_compound_for_pruning(c, stats, schema, row_count) {
                    PruneOutcome::NoMatch => return PruneOutcome::NoMatch,
                    PruneOutcome::Mixed => all_match = false,
                    PruneOutcome::AllMatch => {}
                }
            }
            if children.is_empty() || all_match {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        CompoundPredicate::Or(children) => {
            // OR: any AllMatch child proves AllMatch. Otherwise, NoMatch
            // only if every child is NoMatch.
            let mut all_no_match = true;
            for c in children {
                match evaluate_compound_for_pruning(c, stats, schema, row_count) {
                    PruneOutcome::AllMatch => return PruneOutcome::AllMatch,
                    PruneOutcome::Mixed => all_no_match = false,
                    PruneOutcome::NoMatch => {}
                }
            }
            if children.is_empty() || all_no_match {
                PruneOutcome::NoMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        CompoundPredicate::Not(inner) => {
            evaluate_compound_for_pruning(inner, stats, schema, row_count).invert()
        }
        CompoundPredicate::IsNull(col) => match stats.get(col) {
            Some(s) => match (s.null_count, row_count) {
                (Some(0), _) => PruneOutcome::NoMatch,
                (Some(n), rc) if n == rc => PruneOutcome::AllMatch,
                _ => PruneOutcome::Mixed,
            },
            None => PruneOutcome::Mixed,
        },
        CompoundPredicate::IsNotNull(col) => match stats.get(col) {
            Some(s) => match (s.null_count, row_count) {
                (Some(0), _) => PruneOutcome::AllMatch,
                (Some(n), rc) if n == rc => PruneOutcome::NoMatch,
                _ => PruneOutcome::Mixed,
            },
            None => PruneOutcome::Mixed,
        },
        CompoundPredicate::In(col, values) => {
            // IN is an OR of Eq atoms; reuse the OR rule.
            let alts: Vec<CompoundPredicate> = values
                .iter()
                .map(|v| CompoundPredicate::Atom(Predicate::Eq(col.clone(), v.clone())))
                .collect();
            evaluate_compound_for_pruning(&CompoundPredicate::Or(alts), stats, schema, row_count)
        }
    }
}

/// Prune a single atom against per-column stats. Returns `Mixed` if the
/// stats can't decide either way — this is the safe default and the rule
/// that keeps us from ever skipping a file that contains a matching row.
fn prune_atom(
    atom: &Predicate,
    stats: &BTreeMap<String, ColumnStats>,
    schema: &arrow_schema::Schema,
) -> PruneOutcome {
    let col = atom.column();
    let cs = match stats.get(col) {
        Some(s) => s,
        None => return PruneOutcome::Mixed,
    };
    // If ANY value in this column might be NULL, a comparison against a
    // non-null literal evaluates to UNKNOWN for those rows (treated as
    // false for matching, but it means we can never prove AllMatch).
    let has_nulls = cs.null_count.map(|n| n > 0).unwrap_or(true);

    let field = match schema.fields().iter().find(|f| f.name() == col) {
        Some(f) => f,
        None => return PruneOutcome::Mixed,
    };
    let dt = field.data_type();
    let value = match atom {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v,
        // StartsWith has its own pruning path below — handle it before we
        // try to extract a ScalarValue (which doesn't exist for prefixes).
        Predicate::StartsWith {
            prefix,
            case_insensitive,
            ..
        } => {
            return prune_starts_with(
                prefix,
                *case_insensitive,
                cs.min_bytes.as_deref(),
                cs.max_bytes.as_deref(),
                has_nulls,
            );
        }
        // IN-list membership cannot be proven AllMatch/NoMatch from a single
        // [min,max] zone (a file overlapping the key range might contain no
        // listed key, and vice versa). The engine pushes a separate range
        // predicate for stats pruning; this atom stays Mixed (always re-check).
        Predicate::InInt64(..) => return PruneOutcome::Mixed,
    };
    let (Some(min), Some(max)) = (cs.min_bytes.as_deref(), cs.max_bytes.as_deref()) else {
        return PruneOutcome::Mixed;
    };
    use arrow_schema::DataType;
    match (dt, value) {
        (DataType::Int64, ScalarValue::Int64(v)) => {
            let (Some(min), Some(max)) = (decode_i64(min), decode_i64(max)) else {
                return PruneOutcome::Mixed;
            };
            decide_numeric(atom, *v, min, max, has_nulls)
        }
        (DataType::Float64, ScalarValue::Float64(v)) => {
            let (Some(min), Some(max)) = (decode_f64(min), decode_f64(max)) else {
                return PruneOutcome::Mixed;
            };
            decide_numeric(atom, *v, min, max, has_nulls)
        }
        (DataType::Boolean, ScalarValue::Boolean(v)) => {
            // Parquet encodes a boolean stat as a single byte 0/1.
            let min = min.first().copied().map(|b| b != 0);
            let max = max.first().copied().map(|b| b != 0);
            match (min, max, atom) {
                (Some(min), Some(max), Predicate::Eq(_, _)) => {
                    if min == max && min == *v {
                        if has_nulls {
                            PruneOutcome::Mixed
                        } else {
                            PruneOutcome::AllMatch
                        }
                    } else if min == max && min != *v {
                        PruneOutcome::NoMatch
                    } else {
                        PruneOutcome::Mixed
                    }
                }
                _ => PruneOutcome::Mixed,
            }
        }
        (DataType::Utf8, ScalarValue::Utf8(v)) => {
            let bytes = v.as_bytes();
            decide_byte_lex(atom, bytes, min, max, has_nulls)
        }
        _ => PruneOutcome::Mixed,
    }
}

fn decide_numeric<T: PartialOrd + Copy>(
    atom: &Predicate,
    v: T,
    min: T,
    max: T,
    has_nulls: bool,
) -> PruneOutcome {
    match atom {
        Predicate::Eq(_, _) => {
            if v < min || v > max {
                PruneOutcome::NoMatch
            } else if min == max && min == v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Lt(_, _) => {
            if min >= v {
                PruneOutcome::NoMatch
            } else if max < v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Gt(_, _) => {
            if max <= v {
                PruneOutcome::NoMatch
            } else if min > v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        // StartsWith is string-only; this numeric pruner is never the
        // dispatch target for it. Caller (decide_string) handles the
        // prefix-range pruning logic for StartsWith.
        Predicate::StartsWith { .. } => PruneOutcome::Mixed,
        // InInt64 returns Mixed before value extraction; never reaches here.
        Predicate::InInt64(..) => PruneOutcome::Mixed,
    }
}

fn decide_byte_lex(
    atom: &Predicate,
    v: &[u8],
    min: &[u8],
    max: &[u8],
    has_nulls: bool,
) -> PruneOutcome {
    match atom {
        Predicate::Eq(_, _) => {
            if v < min || v > max {
                PruneOutcome::NoMatch
            } else if min == max && min == v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Lt(_, _) => {
            if min >= v {
                PruneOutcome::NoMatch
            } else if max < v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Gt(_, _) => {
            if max <= v {
                PruneOutcome::NoMatch
            } else if min > v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        // StartsWith has its own pruner (`decide_starts_with_bytes` below).
        // This arm should never fire — caller routes StartsWith there
        // directly. Return Mixed as a safe fallback.
        Predicate::StartsWith { .. } => PruneOutcome::Mixed,
        // InInt64 returns Mixed before value extraction; never reaches here.
        Predicate::InInt64(..) => PruneOutcome::Mixed,
    }
}

/// Prune a `StartsWith` predicate against per-column min/max byte stats.
///
/// Semantics: a row matches iff `value` starts with `prefix` (case-sensitive
/// or ASCII-folded). For pruning we use the lex-ordered min/max bytes:
///
/// * If max < prefix (lex) → no value can start with prefix → NoMatch.
/// * If min > prefix_end (lex) → no value can start with prefix → NoMatch.
///
/// For case-insensitive (ILIKE) we conservatively bail to Mixed: the
/// on-disk min/max is in the column's native casing, so an ILIKE prefix
/// can match values whose lex-bytes are outside the `[prefix, prefix_end)`
/// range. Skipping pruning is always safe.
///
/// `prefix_end` is computed by incrementing the last byte of `prefix`. If
/// the last byte is `0xFF` (or the prefix is empty) we give up the upper
/// bound and only use the `max < prefix` rule. This keeps the function
/// allocation-light and avoids unicode edge cases.
fn prune_starts_with(
    prefix: &str,
    case_insensitive: bool,
    min: Option<&[u8]>,
    max: Option<&[u8]>,
    _has_nulls: bool,
) -> PruneOutcome {
    if case_insensitive {
        // ASCII-fold pruning would need both min and max folded, plus careful
        // handling of code points where lowercasing changes byte length.
        // Skip — the per-row filter still gives us the materialization-cost
        // win, and an inconclusive Mixed never loses data.
        return PruneOutcome::Mixed;
    }
    if prefix.is_empty() {
        // Empty prefix matches every non-null value — pruning is uninformative.
        return PruneOutcome::Mixed;
    }
    let (Some(min), Some(max)) = (min, max) else {
        return PruneOutcome::Mixed;
    };
    let pbytes = prefix.as_bytes();

    // Lower bound check: max < prefix → NoMatch.
    if max < pbytes {
        return PruneOutcome::NoMatch;
    }

    // Upper bound: bump the last byte to derive `prefix_end` such that any
    // string starting with `prefix` is lex < prefix_end. If the last byte
    // is `0xFF` we can't represent the successor in the same length without
    // appending — bail to `Mixed` for the upper bound check.
    let last = *pbytes.last().expect("non-empty prefix");
    if last != 0xFF {
        let mut pend = pbytes.to_vec();
        let n = pend.len();
        pend[n - 1] = last + 1;
        if min.as_ref() >= pend.as_slice() {
            return PruneOutcome::NoMatch;
        }
    }
    PruneOutcome::Mixed
}

fn decode_i64(b: &[u8]) -> Option<i64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(i64::from_le_bytes(arr))
}

fn decode_f64(b: &[u8]) -> Option<f64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(f64::from_le_bytes(arr))
}

/// Parse a PG/ISO-8601 timestamp string to microseconds since Unix epoch.
///
/// Accepted forms (examples):
///   * `'2023-02-01 00:00:00+00'`   (PG short UTC offset — most common)
///   * `'2023-02-01T00:00:00+00:00'` (RFC 3339)
///   * `'2023-02-01 00:00:00'`       (no timezone → UTC assumed)
///
/// Returns `i64::MIN` on parse failure so the predicate never matches — a
/// safe sentinel since valid timestamps are far from `i64::MIN`.
fn parse_ts_str_to_us(s: &str) -> i64 {
    use chrono::{NaiveDateTime, TimeZone, Utc};

    // 1) RFC 3339 / ISO-8601 with full offset.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.with_timezone(&Utc).timestamp_micros();
    }

    // 2) Normalise: replace space separator with 'T', then expand short UTC
    //    offset `+HH` → `+HH:00`.
    let mut norm = s.to_string();
    if !norm.contains('T') {
        if let Some(pos) = norm.find(' ') {
            norm.replace_range(pos..=pos, "T");
        }
    }
    // Expand trailing +HH or -HH (3 chars, no colon).
    let maybe_tz = norm[10..]
        .rfind('+')
        .map(|p| p + 10)
        .or_else(|| norm[10..].rfind('-').map(|p| p + 10));
    if let Some(pos) = maybe_tz {
        let suffix = &norm[pos..];
        if suffix.len() == 3 && !suffix.contains(':') {
            norm.push_str(":00");
        }
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&norm) {
        return dt.with_timezone(&Utc).timestamp_micros();
    }

    // 3) No timezone — assume UTC.
    let no_tz = s.trim_end_matches('+').trim_end_matches("00");
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Utc.from_utc_datetime(&ndt).timestamp_micros();
    }
    let _ = no_tz;

    i64::MIN
}

#[cfg(test)]
mod compound_tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn small_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from(vec![Some(1), Some(2), None, Some(4), Some(5)]);
        let names = StringArray::from(vec![Some("a"), Some("b"), Some("c"), None, Some("e")]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn collect_mask(arr: &BooleanArray) -> Vec<Option<bool>> {
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn and_combines_atoms() {
        let pred = CompoundPredicate::And(vec![
            CompoundPredicate::Atom(Predicate::Gt("id".into(), ScalarValue::Int64(1))),
            CompoundPredicate::Atom(Predicate::Lt("id".into(), ScalarValue::Int64(5))),
        ]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // ids: 1,2,NULL,4,5 → only 2 and 4 match (>1 AND <5).
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(false), // 1 > 1 = false
                Some(true),
                Some(false), // null id → atom returns false; AND with false = false
                Some(true),
                Some(false),
            ]
        );
    }

    #[test]
    fn or_combines_atoms() {
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(1))),
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(4))),
        ]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(true),
                Some(false)
            ]
        );
    }

    #[test]
    fn in_list_matches() {
        let pred = CompoundPredicate::In(
            "id".into(),
            vec![ScalarValue::Int64(2), ScalarValue::Int64(5)],
        );
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true)
            ]
        );
    }

    #[test]
    fn empty_in_is_false() {
        let pred = CompoundPredicate::In("id".into(), vec![]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(collect_mask(&mask), vec![Some(false); 5]);
    }

    #[test]
    fn is_null_finds_nulls() {
        let pred = CompoundPredicate::IsNull("id".into());
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // Only index 2 (the None id) is null.
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false)
            ]
        );
        let pred = CompoundPredicate::IsNotNull("name".into());
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(true), Some(true), Some(false), Some(true)]
        );
    }

    /// Regression: `WHERE int_col = '1'` (string literal vs integer column)
    /// must coerce the literal to the column type like PostgreSQL, not panic
    /// by calling `as_string()` on a non-string Arrow array.
    ///
    /// This is the SELECT/UPDATE/DELETE predicate path — every WHERE clause
    /// against this storage layer routes through `evaluate_compound`, so a
    /// single mask assertion covers all three statement kinds.
    #[test]
    fn utf8_literal_vs_int_column_coerces_no_panic() {
        // Eq: '1' against Int64 id column.
        let pred =
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Utf8("1".into())));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // ids: 1,2,NULL,4,5 → only the first row (id=1) matches.
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ]
        );

        // Gt: id > '1' (string) → coerced to int8.
        let pred =
            CompoundPredicate::Atom(Predicate::Gt("id".into(), ScalarValue::Utf8("1".into())));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false), Some(true), Some(true)]
        );

        // Lt: id < '5' (string) → coerced to int8.
        let pred =
            CompoundPredicate::Atom(Predicate::Lt("id".into(), ScalarValue::Utf8("5".into())));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(true), Some(false), Some(true), Some(false)]
        );

        // A non-numeric string against an int column is a typed error, never
        // a panic (PG: "invalid input syntax for type integer").
        let pred =
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Utf8("abc".into())));
        assert!(evaluate_compound(&small_batch(), &pred).is_err());

        // The Utf8-column path is unaffected: '1' against the name column
        // still compares as a string.
        let pred =
            CompoundPredicate::Atom(Predicate::Eq("name".into(), ScalarValue::Utf8("a".into())));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ]
        );
    }

    /// Regression: `WHERE str_col = 'foo'` against a `LargeUtf8` or `Utf8View`
    /// column must dispatch to the matching string-array downcast — not
    /// `as_string::<i32>()` (the `Utf8` arm), which panics on either of those
    /// physical encodings. Same DoS-class shape as the int-column panic above.
    #[test]
    fn utf8_literal_vs_largeutf8_and_utf8view_columns_no_panic() {
        use arrow_array::{LargeStringArray, StringViewArray};
        // LargeUtf8 column ----------------------------------------------------
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::LargeUtf8,
            true,
        )]));
        let names = LargeStringArray::from(vec![Some("alpha"), Some("beta"), None, Some("gamma")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(names)]).unwrap();

        // Eq
        let pred = Predicate::Eq("name".into(), ScalarValue::Utf8("beta".into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false), Some(false)]
        );
        // Gt — lex-greater than "beta": only "gamma" matches.
        let pred = Predicate::Gt("name".into(), ScalarValue::Utf8("beta".into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(false), Some(false), Some(true)]
        );
        // Lt — lex-less than "beta": only "alpha" matches.
        let pred = Predicate::Lt("name".into(), ScalarValue::Utf8("beta".into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false), Some(false)]
        );

        // Utf8View column -----------------------------------------------------
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));
        let names = StringViewArray::from(vec![Some("alpha"), Some("beta"), None, Some("gamma")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(names)]).unwrap();

        let pred = Predicate::Eq("name".into(), ScalarValue::Utf8("gamma".into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(false), Some(false), Some(true)]
        );
        let pred = Predicate::Lt("name".into(), ScalarValue::Utf8("beta".into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false), Some(false)]
        );
    }

    /// Regression: `WHERE ts < '…'` against a non-microsecond Timestamp column
    /// must dispatch to the matching `Timestamp<TimeUnit>` primitive type
    /// rather than always downcasting to `TimestampMicrosecondType` (which
    /// panics on Ns/Ms/Sec columns). Covers Ns / Ms / Sec.
    #[test]
    fn timestamp_non_microsecond_utf8_literal_no_panic() {
        use arrow_array::{
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        use arrow_schema::TimeUnit;

        // Reference moment: 2023-02-01 00:00:00+00 = 1675209600 s = 1675209600000 ms
        // = 1675209600000000 µs = 1675209600000000000 ns.
        let lit = "2023-02-01 00:00:00+00";

        // ── Nanosecond ────────────────────────────────────────────────────
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        // One ns before, one ns at, one ns after the reference moment, one null.
        let arr = TimestampNanosecondArray::from(vec![
            Some(1_675_209_599_999_999_999),
            Some(1_675_209_600_000_000_000),
            Some(1_675_209_600_000_000_001),
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Lt("ts".into(), ScalarValue::Utf8(lit.into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false), Some(false)]
        );
        let pred = Predicate::Eq("ts".into(), ScalarValue::Utf8(lit.into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false), Some(false)]
        );

        // ── Millisecond ───────────────────────────────────────────────────
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let arr = TimestampMillisecondArray::from(vec![
            Some(1_675_209_599_999),
            Some(1_675_209_600_000),
            Some(1_675_209_600_001),
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Gt("ts".into(), ScalarValue::Utf8(lit.into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(false), Some(true), Some(false)]
        );

        // ── Second ────────────────────────────────────────────────────────
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let arr = TimestampSecondArray::from(vec![
            Some(1_675_209_599),
            Some(1_675_209_600),
            Some(1_675_209_601),
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Eq("ts".into(), ScalarValue::Utf8(lit.into()));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false), Some(false)]
        );
    }

    /// Float and boolean columns also coerce a Utf8 literal rather than
    /// panicking on the unchecked `as_string()` downcast.
    #[test]
    fn utf8_literal_vs_float_and_bool_columns_coerce() {
        use arrow_array::{BooleanArray, Float64Array};
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let f = Float64Array::from(vec![Some(1.5), Some(2.0), None]);
        let b = BooleanArray::from(vec![Some(true), Some(false), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(f), Arc::new(b)]).unwrap();

        let pred =
            CompoundPredicate::Atom(Predicate::Eq("f".into(), ScalarValue::Utf8("2.0".into())));
        let mask = evaluate_compound(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false)]
        );

        let pred =
            CompoundPredicate::Atom(Predicate::Eq("b".into(), ScalarValue::Utf8("true".into())));
        let mask = evaluate_compound(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false)]
        );
    }

    /// Regression for the adjacent unchecked-downcast arms in `predicate.rs`:
    /// the Utf8 / Timestamp / Boolean coercion arms got dedicated fixes
    /// (commits `68b1ba8` and `coerce_utf8_scalar_for_column`) but the FALLBACK
    /// arms for the OTHER scalar types still called
    /// `cmp_primitive!(Int64Type, …)` / `cmp_primitive!(UInt64Type, …)` /
    /// `cmp_primitive!(Float64Type, …)` / `col.as_boolean()` with no
    /// `col.data_type()` guard — so an Int64 / UInt64 / Float64 / Boolean
    /// scalar against a column with an unhandled physical type
    /// (Date32, Decimal128, UInt32, Float32-via-Float64, Int64-via-Boolean)
    /// would hit the unchecked Arrow downcast inside the macro and panic.
    ///
    /// Same DoS-class shape as the original Utf8 panic. Fixed by gating the
    /// numeric arms on `col.data_type()` and returning a typed
    /// `BasinError::storage` for unsupported pairings.
    #[test]
    fn numeric_and_bool_literals_against_unsupported_columns_return_error() {
        use arrow_array::{
            BooleanArray, Date32Array, Decimal128Array, Float32Array, Int64Array, UInt32Array,
        };
        use arrow_schema::{DataType, Field, Schema};

        // ── Int64 literal vs Date32 column ──────────────────────────────
        // Previously: as_primitive::<Int64Type>() on a Date32Array panics.
        // Now: typed `BasinError::storage`.
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let arr = Date32Array::from(vec![Some(19_357), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Eq("d".into(), ScalarValue::Int64(19_357));
        let err = evaluate(&batch, &pred).unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::Storage(_)),
            "expected typed storage error, got {err:?}"
        );

        // ── Int64 literal vs Decimal128 column ──────────────────────────
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let arr = Decimal128Array::from(vec![Some(12345), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Gt("x".into(), ScalarValue::Int64(100));
        let err = evaluate(&batch, &pred).unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::Storage(_)),
            "expected typed storage error, got {err:?}"
        );

        // ── Int64 literal vs UInt32 column ──────────────────────────────
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt32, true)]));
        let arr = UInt32Array::from(vec![Some(7u32), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Lt("u".into(), ScalarValue::Int64(10));
        let err = evaluate(&batch, &pred).unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::Storage(_)),
            "expected typed storage error, got {err:?}"
        );

        // ── Float64 literal vs Float32 column — coerce path stays intact ──
        // This was already a guarded arm (Float32-widen). Sanity-check
        // the typed-comparison still works and was not regressed by the
        // fallback-arm rewrite.
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float32, true)]));
        let arr = Float32Array::from(vec![Some(1.5_f32), Some(2.5_f32), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Gt("f".into(), ScalarValue::Float64(2.0));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false)]
        );

        // ── Float64 literal vs Int64 column — typed error, not a panic ──
        // Float64Type downcast on an Int64Array previously panicked.
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![Some(1), Some(2), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Eq("i".into(), ScalarValue::Float64(1.5));
        let err = evaluate(&batch, &pred).unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::Storage(_)),
            "expected typed storage error, got {err:?}"
        );

        // ── Boolean literal vs Int64 column — typed error, not a panic ──
        // `col.as_boolean()` on an Int64Array previously panicked.
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let arr = Int64Array::from(vec![Some(1), Some(0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Eq("i".into(), ScalarValue::Boolean(true));
        let err = evaluate(&batch, &pred).unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::Storage(_)),
            "expected typed storage error, got {err:?}"
        );

        // Boolean literal vs Boolean column — sanity check the matched arm.
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let pred = Predicate::Eq("b".into(), ScalarValue::Boolean(true));
        let mask = evaluate(&batch, &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false)]
        );
    }

    #[test]
    fn not_negates() {
        let pred = CompoundPredicate::Not(Box::new(CompoundPredicate::Atom(Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(2),
        ))));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // id=2 is true → NOT = false. Others are false → NOT = true. The NULL
        // id evaluates Eq to false (per atom rules), so NOT(false) = true.
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)]
        );
    }

    fn stats(min: i64, max: i64, nulls: u64) -> BTreeMap<String, ColumnStats> {
        let mut m = BTreeMap::new();
        m.insert(
            "id".into(),
            ColumnStats {
                null_count: Some(nulls),
                min_bytes: Some(min.to_le_bytes().to_vec()),
                max_bytes: Some(max.to_le_bytes().to_vec()),
                ..Default::default()
            },
        );
        m
    }

    fn id_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, true)])
    }

    #[test]
    fn pruning_eq_outside_range_is_no_match() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42)));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_eq_inside_range_is_mixed() {
        let stats = stats(0, 1000, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42)));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn pruning_and_short_circuits_no_match() {
        let stats = stats(100, 200, 0);
        // id = 42 prunes; combined with anything via AND prunes the whole.
        let pred = CompoundPredicate::And(vec![
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42))),
            CompoundPredicate::Atom(Predicate::Lt("id".into(), ScalarValue::Int64(1_000_000))),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_or_requires_all_branches_no_match() {
        let stats = stats(100, 200, 0);
        // 42 is outside range (NoMatch); 150 is inside (Mixed). OR result =
        // Mixed because one branch can't be ruled out.
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42))),
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(150))),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );

        // Both outside: NoMatch.
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42))),
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(999))),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_is_null_with_zero_nulls_proves_no_match() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::IsNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
        let pred = CompoundPredicate::IsNotNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::AllMatch
        );
    }

    #[test]
    fn pruning_is_null_with_partial_nulls_is_mixed() {
        let stats = stats(100, 200, 5);
        let pred = CompoundPredicate::IsNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn pruning_in_list_uses_or_rule() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::In(
            "id".into(),
            vec![ScalarValue::Int64(1), ScalarValue::Int64(2)],
        );
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_eq_singleton_with_no_nulls_is_all_match() {
        // min == max == v and zero nulls → every row matches.
        let stats = stats(42, 42, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(42)));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::AllMatch
        );
    }

    // -----------------------------------------------------------------------
    // UUID coercion regression tests (v0.2 gap fix)
    // -----------------------------------------------------------------------

    /// Build a one-column RecordBatch whose "id" column is FixedSizeBinary(16)
    /// holding three UUIDs (plus one NULL).
    fn uuid_batch() -> RecordBatch {
        use arrow_array::FixedSizeBinaryArray;

        let u1: [u8; 16] = *uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
            .unwrap()
            .as_bytes();
        let u2: [u8; 16] = *uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
            .unwrap()
            .as_bytes();
        let u3: [u8; 16] = *uuid::Uuid::parse_str("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
            .unwrap()
            .as_bytes();

        // Build a FixedSizeBinaryArray: u1, u2, NULL, u3
        let values: Vec<Option<&[u8]>> = vec![Some(&u1[..]), Some(&u2[..]), None, Some(&u3[..])];
        let arr =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 16).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::FixedSizeBinary(16),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn uuid_eq_literal_matches_row() {
        // `WHERE id = '550e8400-e29b-41d4-a716-446655440000'`
        let batch = uuid_batch();
        let pred = Predicate::Eq(
            "id".into(),
            ScalarValue::Utf8("550e8400-e29b-41d4-a716-446655440000".into()),
        );
        let mask = evaluate(&batch, &pred).unwrap();
        let result: Vec<Option<bool>> = (0..mask.len())
            .map(|i| {
                if mask.is_null(i) {
                    None
                } else {
                    Some(mask.value(i))
                }
            })
            .collect();
        // Only index 0 (u1) matches; index 2 is NULL → false.
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(false)]
        );
    }

    #[test]
    fn uuid_eq_no_match_returns_all_false() {
        // A UUID that does not exist in the batch.
        let batch = uuid_batch();
        let pred = Predicate::Eq(
            "id".into(),
            ScalarValue::Utf8("ffffffff-ffff-ffff-ffff-ffffffffffff".into()),
        );
        let mask = evaluate(&batch, &pred).unwrap();
        let result: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(result, vec![false, false, false, false]);
    }

    #[test]
    fn uuid_eq_invalid_literal_is_error() {
        let batch = uuid_batch();
        let pred = Predicate::Eq("id".into(), ScalarValue::Utf8("not-a-uuid".into()));
        assert!(evaluate(&batch, &pred).is_err());
    }

    // -----------------------------------------------------------------------
    // StartsWith / LIKE-prefix tests
    // -----------------------------------------------------------------------

    fn string_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let names = StringArray::from(vec![
            Some("pending_alpha"),
            Some("pending_beta"),
            Some("processed_gamma"),
            None,
            Some("PENDING_UPPER"),
            Some("paid"),
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(names)]).unwrap()
    }

    fn mask_to_bools(arr: &BooleanArray) -> Vec<bool> {
        (0..arr.len())
            .map(|i| if arr.is_null(i) { false } else { arr.value(i) })
            .collect()
    }

    #[test]
    fn starts_with_case_sensitive_filters_rows() {
        let batch = string_batch();
        let pred = Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: false,
        };
        let mask = evaluate(&batch, &pred).unwrap();
        // "pending_alpha" yes, "pending_beta" yes, processed no, NULL no,
        // "PENDING_UPPER" no (case-sensitive), "paid" no.
        assert_eq!(
            mask_to_bools(&mask),
            vec![true, true, false, false, false, false]
        );
    }

    #[test]
    fn starts_with_case_insensitive_matches_mixed_case() {
        let batch = string_batch();
        let pred = Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: true,
        };
        let mask = evaluate(&batch, &pred).unwrap();
        // Now "PENDING_UPPER" matches too via ASCII fold.
        assert_eq!(
            mask_to_bools(&mask),
            vec![true, true, false, false, true, false]
        );
    }

    #[test]
    fn starts_with_via_compound_predicate() {
        let batch = string_batch();
        let pred = CompoundPredicate::Atom(Predicate::StartsWith {
            column: "name".into(),
            prefix: "paid".into(),
            case_insensitive: false,
        });
        let mask = evaluate_compound(&batch, &pred).unwrap();
        assert_eq!(
            mask_to_bools(&mask),
            vec![false, false, false, false, false, true]
        );
    }

    #[test]
    fn starts_with_column_helper_returns_column() {
        let p = Predicate::StartsWith {
            column: "status".into(),
            prefix: "pending".into(),
            case_insensitive: false,
        };
        assert_eq!(p.column(), "status");
    }

    fn string_stats(min: &str, max: &str, nulls: u64) -> BTreeMap<String, ColumnStats> {
        let mut m = BTreeMap::new();
        m.insert(
            "name".into(),
            ColumnStats {
                null_count: Some(nulls),
                min_bytes: Some(min.as_bytes().to_vec()),
                max_bytes: Some(max.as_bytes().to_vec()),
                ..Default::default()
            },
        );
        m
    }

    fn name_schema() -> Schema {
        Schema::new(vec![Field::new("name", DataType::Utf8, true)])
    }

    #[test]
    fn pruning_starts_with_when_max_below_prefix_is_no_match() {
        // All values lex-sort before "pending" — no row can start with it.
        let stats = string_stats("apple", "orange", 0);
        let pred = CompoundPredicate::Atom(Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: false,
        });
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &name_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_starts_with_when_min_above_prefix_end_is_no_match() {
        // All values lex-sort after "pendinh" (the successor of "pending"),
        // so no row can start with "pending".
        let stats = string_stats("zebra", "zulu", 0);
        let pred = CompoundPredicate::Atom(Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: false,
        });
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &name_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_starts_with_overlapping_range_is_mixed() {
        // Range straddles "pending" — pruning can't decide.
        let stats = string_stats("apple", "zulu", 0);
        let pred = CompoundPredicate::Atom(Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: false,
        });
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &name_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn pruning_starts_with_case_insensitive_bails_to_mixed() {
        // ILIKE pruning is conservative — even when the range clearly excludes
        // the prefix, we return Mixed to avoid a folding-edge-case bug.
        let stats = string_stats("apple", "orange", 0);
        let pred = CompoundPredicate::Atom(Predicate::StartsWith {
            column: "name".into(),
            prefix: "pending".into(),
            case_insensitive: true,
        });
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &name_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn uuid_eq_via_compound_predicate() {
        // Exercises the `CompoundPredicate::Atom` path — mirrors the
        // `WHERE id = $1` parameterised-query shape.
        let batch = uuid_batch();
        let pred = CompoundPredicate::Atom(Predicate::Eq(
            "id".into(),
            ScalarValue::Utf8("6ba7b810-9dad-11d1-80b4-00c04fd430c8".into()),
        ));
        let mask = evaluate_compound(&batch, &pred).unwrap();
        let result: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        // Only index 1 (u2) should match.
        assert_eq!(result, vec![false, true, false, false]);
    }
}

#[cfg(test)]
mod in_int64_tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array};

    fn i64_col(vals: Vec<Option<i64>>) -> arrow_array::ArrayRef {
        Arc::new(Int64Array::from(vals)) as arrow_array::ArrayRef
    }

    fn mask_vec(a: &BooleanArray) -> Vec<bool> {
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    // ── eval_in_int64 (vectorized membership fallback) ──────────────────────

    #[test]
    fn eval_in_int64_basic_membership_and_nulls() {
        // keys MUST be sorted+deduped per the InInt64 contract.
        let col = i64_col(vec![Some(1), Some(2), None, Some(4), Some(5)]);
        let m = eval_in_int64(&col, &[2, 4]);
        // 2 and 4 match; NULL never matches.
        assert_eq!(mask_vec(&m), vec![false, true, false, true, false]);
    }

    #[test]
    fn eval_in_int64_empty_keys_matches_nothing() {
        let col = i64_col(vec![Some(1), Some(2), Some(3)]);
        let m = eval_in_int64(&col, &[]);
        assert_eq!(mask_vec(&m), vec![false, false, false]);
    }

    #[test]
    fn eval_in_int64_widens_int32_column() {
        let col = Arc::new(Int32Array::from(vec![Some(7), Some(8), None, Some(9)]))
            as arrow_array::ArrayRef;
        let m = eval_in_int64(&col, &[7, 9]);
        assert_eq!(mask_vec(&m), vec![true, false, false, true]);
    }

    // ── sorted_in_int64_indices (binary-search skip path) ───────────────────

    #[test]
    fn sorted_indices_match_plain_filter_random() {
        // Equivalence check: the sorted-skip indices select exactly the rows
        // the plain vectorized membership mask selects, on a sorted column.
        let mut vals: Vec<i64> = (0..500).map(|i| i * 3).collect(); // sorted, gaps
        vals.sort_unstable();
        let col = i64_col(vals.iter().map(|v| Some(*v)).collect());

        // Mixed present/absent keys; sorted+deduped.
        let mut keys = vec![0i64, 3, 4, 297, 298, 1497, 999_999];
        keys.sort_unstable();
        keys.dedup();

        let idx = sorted_in_int64_indices(&col, &keys).expect("int64 column");
        // Reference: plain mask → indices.
        let mask = eval_in_int64(&col, &keys);
        let want: Vec<u32> = (0..mask.len() as u32)
            .filter(|&i| mask.value(i as usize))
            .collect();
        assert_eq!(idx, want);
    }

    #[test]
    fn sorted_indices_boundary_keys_first_and_last() {
        let col = i64_col(vec![Some(10), Some(20), Some(30), Some(40)]);
        // First and last element keys.
        let idx = sorted_in_int64_indices(&col, &[10, 40]).unwrap();
        assert_eq!(idx, vec![0u32, 3u32]);
    }

    #[test]
    fn sorted_indices_absent_keys_yield_empty() {
        let col = i64_col(vec![Some(10), Some(20), Some(30)]);
        let idx = sorted_in_int64_indices(&col, &[5, 15, 35]).unwrap();
        assert!(idx.is_empty());
    }

    #[test]
    fn sorted_indices_duplicate_keys_in_list_dedup_safe() {
        // The engine dedups, but be defensive: a key appearing once produces
        // one index even if the column has a single matching row.
        let col = i64_col(vec![Some(1), Some(2), Some(3)]);
        let idx = sorted_in_int64_indices(&col, &[2]).unwrap();
        assert_eq!(idx, vec![1u32]);
    }

    #[test]
    fn sorted_indices_duplicate_values_in_column_collected() {
        // Defensive: a (non-unique) sorted column run is fully collected.
        let col = i64_col(vec![Some(1), Some(2), Some(2), Some(2), Some(3)]);
        let idx = sorted_in_int64_indices(&col, &[2]).unwrap();
        assert_eq!(idx, vec![1u32, 2u32, 3u32]);
    }

    #[test]
    fn sorted_indices_trailing_nulls_excluded() {
        // Arrow ASC clustering puts NULLs last; they must never match and the
        // search is bounded to the non-null prefix.
        let col = i64_col(vec![Some(1), Some(2), Some(3), None, None]);
        let idx = sorted_in_int64_indices(&col, &[2, 3]).unwrap();
        assert_eq!(idx, vec![1u32, 2u32]);
    }

    #[test]
    fn sorted_indices_int32_column_widens() {
        let col =
            Arc::new(Int32Array::from(vec![Some(5), Some(6), Some(7)])) as arrow_array::ArrayRef;
        let idx = sorted_in_int64_indices(&col, &[6]).unwrap();
        assert_eq!(idx, vec![1u32]);
    }

    #[test]
    fn sorted_indices_none_on_non_integer_column() {
        let col = Arc::new(arrow_array::StringArray::from(vec![Some("a"), Some("b")]))
            as arrow_array::ArrayRef;
        assert!(sorted_in_int64_indices(&col, &[1]).is_none());
    }
}
