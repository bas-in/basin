//! DataFusion scalar UDFs for vector distance functions.
//!
//! Three UDFs match `pg_vector` semantics so users can write portable SQL:
//!
//! - `l2_distance(a, b) -> Float64`
//! - `cosine_distance(a, b) -> Float64`
//! - `dot_product(a, b) -> Float64`
//!
//! Inputs may be `FixedSizeList<Float32>` arrays (the `vector(N)` column
//! storage form) or string literals like `'[0.1, 0.2, ...]'` that pg_vector
//! users habitually write as the right-hand side of distance comparisons.
//! The latter case is materialised into a `FixedSizeList<Float32>` row using
//! the matching dim from the array side.
//!
//! Dim mismatch at runtime is a `DataFusionError::Execution` rather than a
//! panic — it's almost always a schema/query bug rather than a substrate
//! failure, and we want the user-visible error message to say which UDF and
//! which dims tripped on it.
//!
//! See ADR 0003 for why this lives in `basin-engine` rather than re-exported
//! from `basin-vector`.

use std::any::Any;
use std::sync::Arc;

use base64::Engine as _;
use basin_vector::{cosine_distance as v_cosine, dot_product as v_dot, l2_distance as v_l2};
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use datafusion::arrow::array::types::IntervalMonthDayNano;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int64Array, IntervalMonthDayNanoArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use sha2::Digest;

use crate::types::parse_vector_literal;

/// Register the three pg_vector-shaped distance UDFs on `ctx`. Idempotent
/// (DataFusion overwrites by name).
pub(crate) fn register_distance_udfs(ctx: &SessionContext) {
    ctx.register_udf(make_udf("l2_distance", DistanceFn::L2));
    ctx.register_udf(make_udf("cosine_distance", DistanceFn::Cosine));
    ctx.register_udf(make_udf("dot_product", DistanceFn::Dot));
}

/// Register the UUID + pgcrypto-shaped UDFs on `ctx`. Idempotent. Surface:
///
/// - `gen_random_uuid()` / `uuid_generate_v4()` -> `FixedSizeBinary(16)`
///   v4 random; matches Postgres pgcrypto + uuid-ossp.
/// - `digest(text, algo)` -> `Binary` cryptographic hash. Algorithms:
///   `md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512`.
/// - `encode(bytes, fmt)` -> `Utf8`. Formats: `hex`, `base64`, `escape`.
/// - `decode(text, fmt)` -> `Binary`. Inverse of `encode`.
/// - `crypt(password, salt)` -> `Utf8`. bcrypt via the `bcrypt` crate
///   (the only algorithm we expose; matches PG pgcrypto's `bf` choice).
/// - `gen_salt(algo)` / `gen_salt(algo, cost)` -> `Utf8`. `bf` produces a
///   bcrypt salt + cost prefix; `cost` defaults to 12 (matches pgcrypto).
///
/// Registered at session-open time alongside the vector distance UDFs.
pub(crate) fn register_pg_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(GenRandomUuid {
        name: "gen_random_uuid".to_string(),
        signature: Signature::nullary(Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(GenRandomUuid {
        name: "uuid_generate_v4".to_string(),
        signature: Signature::nullary(Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(DigestUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(EncodeUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(DecodeUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(CryptUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(GenSaltUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            ],
            Volatility::Volatile,
        ),
    }));
}

/// Register the PG-compat scalar function set (Phase 5.11.A): `mod`, `age`,
/// and PG-format-aware overrides for `to_char` / `to_timestamp`.
///
/// `to_char` and `to_timestamp` here *replace* DataFusion's same-named
/// builtins: DataFusion's versions accept chrono `%Y-%m-%d`-style format
/// strings, but every PG-targeted ORM emits the PG style (`YYYY-MM-DD`).
/// We translate PG → chrono inside the UDF and dispatch from there. The
/// chrono-style format still works through the same UDFs (`%`-prefixed
/// directives are kept verbatim), so SQL written against the previous
/// behaviour does not break.
///
/// `mod(a, b)` is a thin alias for the `%` operator. `age(ts1, ts2)` returns
/// a native `Interval(MonthDayNano)` matching PG's `interval` type, so that
/// arithmetic like `age(...) + interval '1 day'` type-checks against
/// downstream consumers.
pub(crate) fn register_pg_compat_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ModUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(AgeUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Second, None),
                ]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(ToCharPgUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(ToTimestampPgUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));
    // PG-shape `power(x, y)` — always returns Float64. Overrides
    // DataFusion's default `power`, which returns Int64 for two integer
    // inputs and trips up downstream callers expecting `double precision`
    // (the PG return type). Inputs are coerced to Float64 by DataFusion's
    // signature machinery.
    ctx.register_udf(ScalarUDF::from(PowerFloat64Udf {
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            Volatility::Immutable,
        ),
    }));
    // PG-shape `extract(second FROM ts)` — always returns Float64 with
    // sub-second precision. The SQL-string rewriter routes the parser-level
    // `EXTRACT(SECOND FROM x)` form to a function call against this UDF
    // before sqlparser runs; non-second EXTRACTs fall through to
    // DataFusion's default `date_part` (Int32 / Int64-shaped). See
    // `rewrite_extract_second` for the rewrite rule.
    ctx.register_udf(ScalarUDF::from(ExtractSecondPgUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Microsecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Millisecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Second, None)]),
            ],
            Volatility::Immutable,
        ),
    }));
    // C2 constraint reactor assertion: returns 1 when predicate is true,
    // raises an Err carrying the supplied message when predicate is false.
    // Used by `reactor_ddl::exec_react_constraint` to encode `CHECK
    // (<predicate>)`-shaped constraint reactors as a SQL probe whose body
    // is `SELECT __basin_assert(<predicate>, '<msg>')`. The CASE/CAST shape
    // we used previously evaluated the cast eagerly during type-checking,
    // tripping even when the predicate would have been TRUE at runtime.
    ctx.register_udf(ScalarUDF::from(BasinAssertUdf {
        signature: Signature::exact(
            vec![DataType::Boolean, DataType::Utf8],
            Volatility::Volatile,
        ),
    }));
    // Sequence UDFs — `nextval(text) / currval(text) / setval(text, bigint
    // [, bool])`. The actual catalog interaction happens at the SQL-string
    // rewrite layer (see `seq_udf::rewrite_sequence_calls`) which runs
    // before sqlparser sees the SQL. The UDFs registered here are
    // tombstones: if a query somehow reaches them (the rewrite missed the
    // call site, the call has dynamic arguments, etc.) they raise an
    // execution-time error rather than silently mis-evaluating. Volatile
    // so the planner doesn't fold their results across rows.
    ctx.register_udf(ScalarUDF::from(crate::seq_udf::NextvalUdf::default()));
    ctx.register_udf(ScalarUDF::from(crate::seq_udf::CurrvalUdf::default()));
    ctx.register_udf(ScalarUDF::from(crate::seq_udf::SetvalUdf::default()));
}

#[derive(Clone, Copy, Debug)]
enum DistanceFn {
    L2,
    Cosine,
    Dot,
}

impl DistanceFn {
    fn apply(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceFn::L2 => v_l2(a, b),
            DistanceFn::Cosine => v_cosine(a, b),
            DistanceFn::Dot => v_dot(a, b),
        }
    }
    fn name(self) -> &'static str {
        match self {
            DistanceFn::L2 => "l2_distance",
            DistanceFn::Cosine => "cosine_distance",
            DistanceFn::Dot => "dot_product",
        }
    }
}

fn make_udf(name: &str, kind: DistanceFn) -> ScalarUDF {
    // `Signature::any(2, Immutable)` opts out of DataFusion's input-type
    // coercion entirely — we accept whatever the planner hands us
    // (FixedSizeList<Float32> array, Utf8 vector literal, or a mix) and do
    // the dispatch ourselves inside `invoke`.
    ScalarUDF::from(VectorDistanceUdf {
        name: name.to_string(),
        signature: Signature::any(2, Volatility::Immutable),
        kind,
    })
}

#[derive(Debug)]
struct VectorDistanceUdf {
    name: String,
    signature: Signature,
    kind: DistanceFn,
}

impl ScalarUDFImpl for VectorDistanceUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_distance(self.kind, args)
    }
}

fn invoke_distance(kind: DistanceFn, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("{} expects 2 arguments, got {}", kind.name(), args.len());
    }
    // Determine row count: prefer an Array; default to 1 for scalar/scalar.
    let num_rows = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);

    let lhs = args[0].clone().into_array(num_rows)?;
    let rhs = args[1].clone().into_array(num_rows)?;

    // For each row, materialise both sides as `&[f32]` and apply the metric.
    let lhs_view = VectorView::from_array(&lhs, kind.name(), "lhs")?;
    let rhs_view = VectorView::from_array(&rhs, kind.name(), "rhs")?;

    // Validate dim once. We pull the dim from whichever side has a typed
    // FixedSizeList; otherwise from the first row of each side at iteration.
    let mut out = Float64Array::builder(num_rows);
    for i in 0..num_rows {
        let a = lhs_view.row(i)?;
        let b = rhs_view.row(i)?;
        match (a, b) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return exec_err!(
                        "{}: vector dim mismatch ({} vs {})",
                        kind.name(),
                        a.len(),
                        b.len()
                    );
                }
                out.append_value(kind.apply(&a, &b) as f64);
            }
            _ => out.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

/// View over an arrow array that lets us pull row `i` as a `Vec<f32>`. Two
/// underlying shapes are accepted — `FixedSizeList<Float32>` (the storage
/// form) and `Utf8` (the literal `'[..]'` form).
enum VectorView<'a> {
    Fsl {
        array: &'a FixedSizeListArray,
        values: &'a Float32Array,
        dim: usize,
    },
    Strs {
        array: &'a StringArray,
    },
}

impl<'a> VectorView<'a> {
    fn from_array(arr: &'a ArrayRef, fn_name: &str, side: &str) -> DFResult<Self> {
        match arr.data_type() {
            DataType::FixedSizeList(child, n) => {
                if *child.data_type() != DataType::Float32 {
                    return exec_err!(
                        "{} {}: expected FixedSizeList<Float32>, got child {:?}",
                        fn_name,
                        side,
                        child.data_type()
                    );
                }
                let array = arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{fn_name} {side}: not a FixedSizeListArray"
                        ))
                    })?;
                let values = array
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{fn_name} {side}: child not Float32Array"
                        ))
                    })?;
                Ok(VectorView::Fsl {
                    array,
                    values,
                    dim: *n as usize,
                })
            }
            DataType::Utf8 => {
                let array = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution(format!("{fn_name} {side}: not a StringArray"))
                })?;
                Ok(VectorView::Strs { array })
            }
            other => exec_err!(
                "{} {}: cannot interpret {:?} as a vector",
                fn_name,
                side,
                other
            ),
        }
    }

    fn row(&self, i: usize) -> DFResult<Option<Vec<f32>>> {
        match self {
            VectorView::Fsl { array, values, dim } => {
                if array.is_null(i) {
                    return Ok(None);
                }
                let mut v = Vec::with_capacity(*dim);
                let base = i * *dim;
                for k in 0..*dim {
                    let idx = base + k;
                    if values.is_null(idx) {
                        return Ok(None);
                    }
                    v.push(values.value(idx));
                }
                Ok(Some(v))
            }
            VectorView::Strs { array } => {
                if array.is_null(i) {
                    return Ok(None);
                }
                let s = array.value(i);
                let parsed = parse_vector_literal(s)
                    .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
                Ok(Some(parsed))
            }
        }
    }
}

/// Pre-DataFusion SQL rewriter for the three `pg_vector` operator forms.
///
/// `a <-> b`  ->  `l2_distance(a, b)`
/// `a <#> b`  ->  `(- dot_product(a, b))`
/// `a <=> b`  ->  `cosine_distance(a, b)`
///
/// This is intentionally a string rewrite: sqlparser 0.52 does not accept the
/// `<->` / `<#>` / `<=>` operators as Postgres custom operators, so we'd
/// otherwise need to fork the parser. The rewriter scans the input and
/// substitutes `<expr> OP <expr>` with the function-call form, where `<expr>`
/// is bounded by the surrounding token (column name, string literal, ARRAY
/// literal, parenthesised group).
///
/// LIMITATION: this rewrite does not understand string literals or comments.
/// An operator sequence appearing inside a quoted string will be substituted
/// the same as a real operator. For the PoC that's acceptable — the smoke
/// test exercises both rewrite paths and brute force; production scope can
/// move this into the parser proper.
pub(crate) fn rewrite_vector_operators(sql: &str) -> String {
    // Walk left-to-right finding the operators in order of priority. Each
    // pass walks the entire string, so cascading `a <-> b <-> c` gets
    // wrapped left-associatively, matching how sqlparser would parse it.
    let mut s = sql.to_string();
    loop {
        let Some(found) = find_first_op(&s) else {
            break;
        };
        let (op_start, op_end, op) = found;

        let (lhs_start, lhs_end) = extract_left_operand(&s, op_start);
        let (rhs_start, rhs_end) = extract_right_operand(&s, op_end);
        let lhs = &s[lhs_start..lhs_end];
        let rhs = &s[rhs_start..rhs_end];

        let func = match op {
            "<->" => format!("l2_distance({lhs}, {rhs})"),
            "<=>" => format!("cosine_distance({lhs}, {rhs})"),
            "<#>" => format!("(- dot_product({lhs}, {rhs}))"),
            _ => unreachable!(),
        };

        s.replace_range(lhs_start..rhs_end, &func);
    }
    s
}

/// Find the first occurrence of any of `<->`, `<#>`, `<=>` and return its
/// byte range and the operator string. Returns `None` if none present.
fn find_first_op(s: &str) -> Option<(usize, usize, &'static str)> {
    let mut best: Option<(usize, usize, &'static str)> = None;
    for op in ["<->", "<#>", "<=>"] {
        if let Some(pos) = s.find(op) {
            match best {
                Some((p, _, _)) if pos >= p => {}
                _ => best = Some((pos, pos + op.len(), op)),
            }
        }
    }
    best
}

/// Walk back from `end` capturing one operand. Returns the inclusive
/// `(start, end)` byte range of the operand within `s`, where `end` is the
/// exclusive upper bound. Operands recognised:
/// - parenthesised group `(...)`
/// - bracketed array `[...]`
/// - quoted string `'...'`
/// - identifier / number run (alphanumeric, `_`, `.`)
fn extract_left_operand(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    // Skip whitespace.
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    if i == 0 {
        return (i, operand_end);
    }
    let last = bytes[i - 1];
    if last == b')' {
        let mut depth = 1i32;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            match bytes[i] {
                b')' => depth += 1,
                b'(' => depth -= 1,
                _ => {}
            }
        }
    } else if last == b']' {
        let mut depth = 1i32;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            match bytes[i] {
                b']' => depth += 1,
                b'[' => depth -= 1,
                _ => {}
            }
        }
        // Include preceding `ARRAY` keyword if present.
        let mut j = i;
        while j > 0 && bytes[j - 1].is_ascii_whitespace() {
            j -= 1;
        }
        let pre_end = j;
        while j > 0 && (bytes[j - 1].is_ascii_alphanumeric() || bytes[j - 1] == b'_') {
            j -= 1;
        }
        if &bytes[j..pre_end].to_ascii_lowercase()[..] == b"array" {
            i = j;
        }
    } else if last == b'\'' {
        // Walk back to matching unescaped quote.
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'\'' {
                break;
            }
        }
    } else {
        // Identifier / number run.
        while i > 0 {
            let c = bytes[i - 1];
            if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' {
                i -= 1;
            } else {
                break;
            }
        }
    }
    (i, operand_end)
}

fn extract_right_operand(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    if i >= bytes.len() {
        return (operand_start, i);
    }
    let first = bytes[i];
    if first == b'(' {
        let mut depth = 1i32;
        i += 1;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
    } else if first == b'\'' {
        i += 1;
        while i < bytes.len() && bytes[i] != b'\'' {
            i += 1;
        }
        if i < bytes.len() {
            i += 1; // include closing quote
        }
    } else if first == b'[' {
        let mut depth = 1i32;
        i += 1;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'[' => depth += 1,
                b']' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
    } else if first.is_ascii_alphabetic() || first == b'_' {
        // Identifier — but if the run spells `ARRAY` followed by `[...]`,
        // include the bracketed part.
        let ident_start = i;
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
        let ident = &bytes[ident_start..i];
        let mut k = i;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        if &ident.to_ascii_lowercase()[..] == b"array" && k < bytes.len() && bytes[k] == b'[' {
            i = k + 1;
            let mut depth = 1i32;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'[' => depth += 1,
                    b']' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
        }
    } else {
        // Numeric / signed numeric.
        if first == b'+' || first == b'-' {
            i += 1;
        }
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
    }
    (operand_start, i)
}

// ---------------------------------------------------------------------------
// UUID + pgcrypto UDFs.
//
// These mirror the Postgres `uuid-ossp` and `pgcrypto` extension surfaces
// closely enough that ORM-generated SQL ports cleanly. They sit on the
// DataFusion side because they're consumed inside SELECT projections; the
// INSERT-time path through `dml::coerce_uuid` recognises the same function
// names so `INSERT ... VALUES (gen_random_uuid(), ...)` works without
// running the full DataFusion pipeline.
//
// Volatility note: `gen_random_uuid`, `uuid_generate_v4`, `crypt`, and
// `gen_salt` are marked `Volatile`. `digest`/`encode`/`decode` are
// `Immutable` — same input, same output, every time. DataFusion uses this
// to decide whether to cache evaluations across rows; getting it wrong
// yields the same UUID for every row of a multi-row INSERT, which is why
// `Volatile` is load-bearing here.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct GenRandomUuid {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for GenRandomUuid {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::FixedSizeBinary(16))
    }
    #[allow(deprecated)]
    fn invoke_no_args(&self, number_rows: usize) -> DFResult<ColumnarValue> {
        // Generate one fresh v4 UUID per row. The `Volatile` signature
        // disables DataFusion's "evaluate once and broadcast" path so
        // every row really does get a distinct id.
        let arr = build_uuid_array(number_rows.max(1));
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

fn build_uuid_array(n: usize) -> FixedSizeBinaryArray {
    let rows: Vec<Option<Vec<u8>>> = (0..n)
        .map(|_| Some(uuid::Uuid::new_v4().as_bytes().to_vec()))
        .collect();
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(rows.into_iter(), 16)
        .expect("16-byte UUID slices are uniformly sized")
}

#[derive(Debug)]
struct DigestUdf {
    signature: Signature,
}

impl ScalarUDFImpl for DigestUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "digest"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_digest(args)
    }
}

fn invoke_digest(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("digest expects 2 arguments, got {}", args.len());
    }
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let input = args[0].clone().into_array(n)?;
    let algo = args[1].clone().into_array(n)?;
    let input = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("digest: arg 1 must be Utf8".into()))?;
    let algo = algo
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("digest: arg 2 must be Utf8".into()))?;
    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    for i in 0..n {
        if input.is_null(i) || algo.is_null(i) {
            out.push(None);
            continue;
        }
        let bytes = compute_digest(algo.value(i), input.value(i).as_bytes())?;
        out.push(Some(bytes));
    }
    let arr = BinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
    Ok(ColumnarValue::Array(Arc::new(arr)))
}

fn compute_digest(algo: &str, input: &[u8]) -> DFResult<Vec<u8>> {
    let lower = algo.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "md5" => {
            let mut h = md5::Md5::new();
            h.update(input);
            h.finalize().to_vec()
        }
        "sha1" => {
            let mut h = sha1::Sha1::new();
            h.update(input);
            h.finalize().to_vec()
        }
        "sha224" => {
            let mut h = sha2::Sha224::new();
            h.update(input);
            h.finalize().to_vec()
        }
        "sha256" => {
            let mut h = sha2::Sha256::new();
            h.update(input);
            h.finalize().to_vec()
        }
        "sha384" => {
            let mut h = sha2::Sha384::new();
            h.update(input);
            h.finalize().to_vec()
        }
        "sha512" => {
            let mut h = sha2::Sha512::new();
            h.update(input);
            h.finalize().to_vec()
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "digest: unsupported algorithm {other:?} (supported: md5, sha1, sha224, sha256, sha384, sha512)"
            )));
        }
    })
}

#[derive(Debug)]
struct EncodeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for EncodeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "encode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_encode(args)
    }
}

fn invoke_encode(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("encode expects 2 arguments, got {}", args.len());
    }
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let bytes_arr = args[0].clone().into_array(n)?;
    let fmt_arr = args[1].clone().into_array(n)?;
    let fmt = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("encode: arg 2 must be Utf8".into()))?;
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if fmt.is_null(i) {
            out.push(None);
            continue;
        }
        let cell_bytes = match bytes_arr.data_type() {
            DataType::Binary => {
                let arr = bytes_arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("encode: not a BinaryArray".into())
                    })?;
                if arr.is_null(i) {
                    out.push(None);
                    continue;
                }
                arr.value(i)
            }
            DataType::LargeBinary => {
                let arr = bytes_arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("encode: not a LargeBinaryArray".into())
                    })?;
                if arr.is_null(i) {
                    out.push(None);
                    continue;
                }
                arr.value(i)
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "encode: arg 1 must be Binary or LargeBinary, got {other:?}"
                )));
            }
        };
        let s = encode_bytes(fmt.value(i), cell_bytes)?;
        out.push(Some(s));
    }
    let arr = StringArray::from(out);
    Ok(ColumnarValue::Array(Arc::new(arr)))
}

fn encode_bytes(fmt: &str, bytes: &[u8]) -> DFResult<String> {
    match fmt.to_ascii_lowercase().as_str() {
        "hex" => Ok(hex::encode(bytes)),
        "base64" => Ok(base64::engine::general_purpose::STANDARD.encode(bytes)),
        "escape" => {
            // Postgres' `escape` format: printable ASCII passes through;
            // backslash and non-printable bytes become `\NNN` octal
            // escapes; backslash itself is doubled.
            let mut s = String::with_capacity(bytes.len());
            for &b in bytes {
                if b == b'\\' {
                    s.push_str("\\\\");
                } else if (32..127).contains(&b) {
                    s.push(b as char);
                } else {
                    s.push_str(&format!("\\{b:03o}"));
                }
            }
            Ok(s)
        }
        other => Err(DataFusionError::Execution(format!(
            "encode: unsupported format {other:?} (supported: hex, base64, escape)"
        ))),
    }
}

#[derive(Debug)]
struct DecodeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for DecodeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "decode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_decode(args)
    }
}

fn invoke_decode(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("decode expects 2 arguments, got {}", args.len());
    }
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let txt = args[0].clone().into_array(n)?;
    let fmt_arr = args[1].clone().into_array(n)?;
    let txt = txt
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("decode: arg 1 must be Utf8".into()))?;
    let fmt = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("decode: arg 2 must be Utf8".into()))?;
    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    for i in 0..n {
        if txt.is_null(i) || fmt.is_null(i) {
            out.push(None);
            continue;
        }
        let bytes = decode_bytes(fmt.value(i), txt.value(i))?;
        out.push(Some(bytes));
    }
    let arr = BinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
    Ok(ColumnarValue::Array(Arc::new(arr)))
}

fn decode_bytes(fmt: &str, s: &str) -> DFResult<Vec<u8>> {
    match fmt.to_ascii_lowercase().as_str() {
        "hex" => hex::decode(s).map_err(|e| DataFusionError::Execution(format!("decode hex: {e}"))),
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| DataFusionError::Execution(format!("decode base64: {e}"))),
        "escape" => {
            // Inverse of the encode path. `\\` -> backslash; `\NNN` -> octal byte.
            let mut out = Vec::with_capacity(s.len());
            let mut bytes = s.bytes();
            while let Some(b) = bytes.next() {
                if b != b'\\' {
                    out.push(b);
                    continue;
                }
                match bytes.next() {
                    Some(b'\\') => out.push(b'\\'),
                    Some(d1 @ b'0'..=b'7') => {
                        let d2 = bytes.next().ok_or_else(|| {
                            DataFusionError::Execution("decode escape: truncated octal".into())
                        })?;
                        let d3 = bytes.next().ok_or_else(|| {
                            DataFusionError::Execution("decode escape: truncated octal".into())
                        })?;
                        let val = ((d1 - b'0') * 64) + ((d2 - b'0') * 8) + (d3 - b'0');
                        out.push(val);
                    }
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "decode escape: bad escape sequence after \\: {:?}",
                            other.map(|c| c as char)
                        )));
                    }
                }
            }
            Ok(out)
        }
        other => Err(DataFusionError::Execution(format!(
            "decode: unsupported format {other:?} (supported: hex, base64, escape)"
        ))),
    }
}

#[derive(Debug)]
struct CryptUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CryptUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "crypt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_crypt(args)
    }
}

fn invoke_crypt(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("crypt expects 2 arguments, got {}", args.len());
    }
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let pw = args[0].clone().into_array(n)?;
    let salt = args[1].clone().into_array(n)?;
    let pw = pw
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("crypt: arg 1 must be Utf8".into()))?;
    let salt = salt
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("crypt: arg 2 must be Utf8".into()))?;
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if pw.is_null(i) || salt.is_null(i) {
            out.push(None);
            continue;
        }
        let hashed = hash_with_salt(pw.value(i), salt.value(i))?;
        out.push(Some(hashed));
    }
    Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
}

fn hash_with_salt(password: &str, salt: &str) -> DFResult<String> {
    // Salt format: `$2a$NN$XXXXXXXXXXXXXXXXXXXXXX` (the bcrypt-modular
    // form). `bcrypt::hash_with_salt` re-uses an existing salt (matching
    // pgcrypto's `crypt(pw, hash)` semantics where `hash` doubles as
    // both the salt source *and* the comparison target). For a fresh
    // hash, `gen_salt('bf')` is the producer.
    //
    // Lift the cost prefix and the 22-char salt out of the pgcrypto-style
    // string. We accept either the full hash (60 chars: `$2a$NN$...22+31`)
    // or the salt-only prefix (29 chars: `$2a$NN$...22`).
    let parts: Vec<&str> = salt.splitn(4, '$').collect();
    if parts.len() < 4 || !parts[0].is_empty() {
        return Err(DataFusionError::Execution(format!(
            "crypt: bad salt {salt:?} (expected $2a$NN$... or $2b$NN$...)"
        )));
    }
    let scheme = parts[1];
    if scheme != "2a" && scheme != "2b" && scheme != "2y" {
        return Err(DataFusionError::Execution(format!(
            "crypt: unsupported scheme {scheme:?} (only bcrypt $2a/$2b/$2y supported)"
        )));
    }
    let cost: u32 = parts[2]
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("crypt: bad cost in salt {salt:?}")))?;
    let raw_salt: &str = parts[3].get(..22).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "crypt: salt too short — need 22 base64 chars after $NN$, got {salt:?}"
        ))
    })?;
    // The `bcrypt` crate's `hash_with_salt` takes 16 raw bytes; convert
    // from the bcrypt-base64 alphabet pgcrypto stores in the salt prefix.
    let raw = bcrypt_base64_decode(raw_salt).ok_or_else(|| {
        DataFusionError::Execution(format!("crypt: bad bcrypt-base64 salt {raw_salt:?}"))
    })?;
    let salt16: [u8; 16] = raw
        .try_into()
        .map_err(|_| DataFusionError::Execution("crypt: salt did not decode to 16 bytes".into()))?;
    let parts = bcrypt::hash_with_salt(password.as_bytes(), cost, salt16)
        .map_err(|e| DataFusionError::Execution(format!("crypt: bcrypt failed: {e}")))?;
    Ok(parts.format_for_version(bcrypt::Version::TwoB))
}

/// bcrypt's modified base64 alphabet (`./A-Za-z0-9`). Decodes 22 chars to
/// 16 bytes; anything else is rejected. We re-implement instead of pulling
/// in another base64 dialect because the alphabet swap is two characters.
fn bcrypt_base64_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() != 22 {
        return None;
    }
    const ALPHABET: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = Vec::with_capacity(16);
    let mut bits: u32 = 0;
    let mut nbits: u32 = 0;
    for c in s.bytes() {
        let v = ALPHABET.iter().position(|&a| a == c)? as u32;
        bits = (bits << 6) | v;
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            let byte = ((bits >> nbits) & 0xff) as u8;
            out.push(byte);
            if out.len() == 16 {
                break;
            }
        }
    }
    if out.len() == 16 {
        Some(out)
    } else {
        None
    }
}

#[derive(Debug)]
struct GenSaltUdf {
    signature: Signature,
}

impl ScalarUDFImpl for GenSaltUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "gen_salt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        invoke_gen_salt(args)
    }
}

fn invoke_gen_salt(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!("gen_salt expects 1 or 2 arguments, got {}", args.len());
    }
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let algo = args[0].clone().into_array(n)?;
    let algo = algo
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("gen_salt: arg 1 must be Utf8".into()))?;
    let cost_arr = if args.len() == 2 {
        Some(args[1].clone().into_array(n)?)
    } else {
        None
    };
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if algo.is_null(i) {
            out.push(None);
            continue;
        }
        let cost: u32 = match &cost_arr {
            Some(arr) => {
                use datafusion::arrow::array::Int64Array;
                let a = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("gen_salt: arg 2 must be Int64".into())
                })?;
                if a.is_null(i) {
                    12
                } else {
                    a.value(i).clamp(4, 31) as u32
                }
            }
            None => 12,
        };
        let s = build_salt(algo.value(i), cost)?;
        out.push(Some(s));
    }
    Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
}

fn build_salt(algo: &str, cost: u32) -> DFResult<String> {
    match algo.to_ascii_lowercase().as_str() {
        "bf" => {
            // 16 random bytes -> 22 chars in bcrypt's base64 alphabet,
            // wrapped in the standard `$2b$NN$...` prefix.
            let mut raw = [0u8; 16];
            getrandom_fill(&mut raw);
            let encoded = bcrypt_base64_encode(&raw);
            Ok(format!("$2b${cost:02}${encoded}"))
        }
        other => Err(DataFusionError::Execution(format!(
            "gen_salt: unsupported algorithm {other:?} (supported: bf)"
        ))),
    }
}

fn bcrypt_base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = String::with_capacity(22);
    let mut bits: u32 = 0;
    let mut nbits: u32 = 0;
    for &b in bytes {
        bits = (bits << 8) | b as u32;
        nbits += 8;
        while nbits >= 6 {
            nbits -= 6;
            let v = ((bits >> nbits) & 0x3f) as usize;
            out.push(ALPHABET[v] as char);
        }
    }
    if nbits > 0 {
        let v = ((bits << (6 - nbits)) & 0x3f) as usize;
        out.push(ALPHABET[v] as char);
    }
    out.truncate(22);
    out
}

/// Fill `buf` with cryptographically random bytes. We delegate to
/// `uuid::Uuid::new_v4()`'s underlying RNG by chaining UUIDs — both crates
/// depend on `getrandom` transitively, and reaching for it directly would
/// add a workspace dep for one call site. Two v4 UUIDs give 32 random
/// bytes, more than the 16 we need here.
fn getrandom_fill(buf: &mut [u8]) {
    let mut idx = 0;
    while idx < buf.len() {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        let take = bytes.len().min(buf.len() - idx);
        buf[idx..idx + take].copy_from_slice(&bytes[..take]);
        idx += take;
    }
}

// ---------------------------------------------------------------------------
// Phase 5.11.A: PG-compat scalar functions.
//
// These four UDFs cover the gaps left by DataFusion's builtins:
//
//  * `mod(a, b)` — present as the `%` operator but missing as a function.
//  * `age(ts1, ts2)` — not implemented in DataFusion at all.
//  * `to_char(ts, fmt)` and `to_timestamp(text, fmt)` — DataFusion ships them
//    using chrono `%Y-%m-%d` style format strings; PG and every PG-emitting
//    ORM use `YYYY-MM-DD HH24:MI:SS`. We register Basin-side UDFs of the
//    same name (DataFusion's registry overwrites by name) that translate
//    PG → chrono before dispatching.
//
// The translation table is intentionally minimal — it covers the directives
// every modern SaaS schema actually emits. Anything outside that set is left
// untouched, which means a chrono-style template (with `%`-prefixed
// directives) round-trips byte-identical so the override is transparent to
// existing chrono-shaped queries.
// ---------------------------------------------------------------------------

/// Translate PG-style date/time format directives to chrono's strftime form.
///
/// Recognised PG directives: `YYYY`, `YY`, `MM`, `DD`, `HH24`, `HH12`, `HH`,
/// `MI`, `SS`, `AM`, `PM`, `Day`, `Mon`, `MS`, `US`. Anything else is passed
/// through verbatim. Chrono `%X` directives are passed through verbatim too,
/// which keeps callers using chrono syntax working unchanged.
fn pg_format_to_chrono(fmt: &str) -> String {
    // Order matters: longest match first so `YYYY` wins over `YY`, and
    // `HH24` over `HH`.
    const REPL: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("Mon", "%b"),
        ("MON", "%b"),
        ("Day", "%A"),
        ("DAY", "%A"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
        ("MS", "%3f"),
        ("US", "%6f"),
        ("AM", "%p"),
        ("PM", "%p"),
        ("am", "%P"),
        ("pm", "%P"),
    ];
    let mut out = String::with_capacity(fmt.len());
    let bytes = fmt.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Pass through chrono `%X` directives unchanged so chrono-style
        // callers remain bit-identical.
        if bytes[i] == b'%' && i + 1 < bytes.len() {
            out.push('%');
            out.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        let mut matched = false;
        for (pg, chrono) in REPL {
            let pg_bytes = pg.as_bytes();
            if i + pg_bytes.len() <= bytes.len() && &bytes[i..i + pg_bytes.len()] == pg_bytes {
                out.push_str(chrono);
                i += pg_bytes.len();
                matched = true;
                break;
            }
        }
        if !matched {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

#[derive(Debug)]
struct ModUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ModUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "mod"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("mod expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let a = args[0].clone().into_array(n)?;
        let b = args[1].clone().into_array(n)?;
        match (a.data_type(), b.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
                let b = b.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut out = Int64Array::builder(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let bv = b.value(i);
                    if bv == 0 {
                        return Err(DataFusionError::Execution("mod: division by zero".into()));
                    }
                    out.append_value(a.value(i) % bv);
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            (DataType::Int32, DataType::Int32) => {
                use datafusion::arrow::array::Int32Array;
                let a = a.as_any().downcast_ref::<Int32Array>().unwrap();
                let b = b.as_any().downcast_ref::<Int32Array>().unwrap();
                let mut out = Int32Array::builder(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    let bv = b.value(i);
                    if bv == 0 {
                        return Err(DataFusionError::Execution("mod: division by zero".into()));
                    }
                    out.append_value(a.value(i) % bv);
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            (DataType::Float64, DataType::Float64) => {
                let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                let b = b.as_any().downcast_ref::<Float64Array>().unwrap();
                let mut out = Float64Array::builder(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) {
                        out.append_null();
                    } else {
                        out.append_value(a.value(i) % b.value(i));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
            (l, r) => exec_err!("mod: unsupported argument types {l:?} % {r:?}"),
        }
    }
}

/// `age(ts1, ts2)` — returns the PG-text rendering of the interval between
/// two timestamps. Format: `"N years M mons D days HH:MM:SS"`, with zero
/// components elided and a sign on the last one if the interval is negative.
/// This matches the default psql output for the `interval` type, which is
/// the rendering ORMs see when they `cast(age(...) AS text)`.
#[derive(Debug)]
struct AgeUdf {
    signature: Signature,
}

impl ScalarUDFImpl for AgeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "age"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("age expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let lhs = args[0].clone().into_array(n)?;
        let rhs = args[1].clone().into_array(n)?;
        let mut out: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(n);
        for i in 0..n {
            let a = ts_array_to_naive(&lhs, i)?;
            let b = ts_array_to_naive(&rhs, i)?;
            match (a, b) {
                (Some(a), Some(b)) => out.push(Some(pg_age_interval(a, b))),
                _ => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            IntervalMonthDayNanoArray::from(out),
        )))
    }
}

fn ts_array_to_naive(arr: &ArrayRef, i: usize) -> DFResult<Option<NaiveDateTime>> {
    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let secs = v.div_euclid(1_000_000_000);
            let ns = v.rem_euclid(1_000_000_000) as u32;
            Ok(Utc.timestamp_opt(secs, ns).single().map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let secs = v.div_euclid(1_000_000);
            let us = v.rem_euclid(1_000_000) as u32;
            Ok(Utc
                .timestamp_opt(secs, us * 1000)
                .single()
                .map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let secs = v.div_euclid(1_000);
            let ms = v.rem_euclid(1_000) as u32;
            Ok(Utc
                .timestamp_opt(secs, ms * 1_000_000)
                .single()
                .map(|d| d.naive_utc()))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            Ok(Utc
                .timestamp_opt(a.value(i), 0)
                .single()
                .map(|d| d.naive_utc()))
        }
        DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            // Date32 stores days since UNIX epoch.
            let days = a.value(i) as i64;
            Ok(Utc
                .timestamp_opt(days * 86_400, 0)
                .single()
                .map(|d| d.naive_utc()))
        }
        other => exec_err!("age: unsupported timestamp type {other:?}"),
    }
}

/// PG `age(ts1, ts2)` calendar walk. Mirrors `timestamp_age` in
/// `src/backend/utils/adt/timestamp.c`: subtract each (y, m, d, hh, mm, ss,
/// us) component, then propagate negatives upward, borrowing days from the
/// **earlier** timestamp's month (so e.g. `age('2024-03-01','2024-01-31')`
/// borrows 31 days from January, yielding 1 month + 1 day, not 1 day + some
/// other figure).
fn pg_age_interval(ts1: NaiveDateTime, ts2: NaiveDateTime) -> IntervalMonthDayNano {
    if ts1 == ts2 {
        return IntervalMonthDayNano::new(0, 0, 0);
    }
    let neg = ts1 < ts2;

    let mut years = ts1.year() - ts2.year();
    let mut months = ts1.month() as i32 - ts2.month() as i32;
    let mut days = ts1.day() as i32 - ts2.day() as i32;
    let mut hours = ts1.hour() as i32 - ts2.hour() as i32;
    let mut mins = ts1.minute() as i32 - ts2.minute() as i32;
    let mut secs = ts1.second() as i32 - ts2.second() as i32;
    // chrono `nanosecond()` is 0..1_000_000_000; PG age preserves
    // microsecond precision but Arrow's MonthDayNano keeps nanoseconds, so
    // carry the full nanosecond residual.
    let mut nanos = ts1.nanosecond() as i64 - ts2.nanosecond() as i64;

    // Flip sign if ts1 < ts2: we walk on absolute components, then re-flip.
    if neg {
        years = -years;
        months = -months;
        days = -days;
        hours = -hours;
        mins = -mins;
        secs = -secs;
        nanos = -nanos;
    }

    // Propagate negatives upward, mirroring PG's `while`-loops.
    while nanos < 0 {
        nanos += 1_000_000_000;
        secs -= 1;
    }
    while secs < 0 {
        secs += 60;
        mins -= 1;
    }
    while mins < 0 {
        mins += 60;
        hours -= 1;
    }
    while hours < 0 {
        hours += 24;
        days -= 1;
    }
    // PG borrows day-count from the earlier timestamp's month when ts1 >=
    // ts2, otherwise from the later timestamp's month. After the sign-flip
    // above, "earlier" is whichever input had the smaller calendar value.
    while days < 0 {
        let (borrow_year, borrow_month) = if neg {
            (ts1.year(), ts1.month())
        } else {
            (ts2.year(), ts2.month())
        };
        days += days_in_month(borrow_year, borrow_month) as i32;
        months -= 1;
    }
    while months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = years * 12 + months;
    let total_nanos = (hours as i64) * 3_600_000_000_000
        + (mins as i64) * 60_000_000_000
        + (secs as i64) * 1_000_000_000
        + nanos;

    if neg {
        IntervalMonthDayNano::new(-total_months, -days, -total_nanos)
    } else {
        IntervalMonthDayNano::new(total_months, days, total_nanos)
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if leap {
                29
            } else {
                28
            }
        }
        _ => 30, // unreachable; guard against bad input.
    }
}

/// PG-format-aware `to_char(timestamp, format)`. Translates PG directives
/// to chrono and renders via `chrono::DateTime::format`.
#[derive(Debug)]
struct ToCharPgUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToCharPgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_char"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("to_char expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let ts = args[0].clone().into_array(n)?;
        let fmt = args[1].clone().into_array(n)?;
        let fmt = fmt
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_char: arg 2 must be Utf8".into()))?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if fmt.is_null(i) {
                out.push(None);
                continue;
            }
            let chrono_fmt = pg_format_to_chrono(fmt.value(i));
            let dt = ts_array_to_naive(&ts, i)?;
            match dt {
                Some(dt) => out.push(Some(dt.format(&chrono_fmt).to_string())),
                None => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

/// PG-format-aware `to_timestamp(text, format)`. Translates PG directives to
/// chrono and parses via `chrono::NaiveDateTime::parse_from_str`. Returns
/// `Timestamp(Nanosecond, None)` to match DataFusion's default `to_timestamp`
/// shape.
#[derive(Debug)]
struct ToTimestampPgUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ToTimestampPgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_timestamp"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("to_timestamp expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let txt = args[0].clone().into_array(n)?;
        let fmt = args[1].clone().into_array(n)?;
        let txt = txt
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_timestamp: arg 1 must be Utf8".into()))?;
        let fmt = fmt
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("to_timestamp: arg 2 must be Utf8".into()))?;
        let mut out: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..n {
            if txt.is_null(i) || fmt.is_null(i) {
                out.push(None);
                continue;
            }
            let chrono_fmt = pg_format_to_chrono(fmt.value(i));
            // Try parsing as a full datetime; fall back to date-only.
            let parsed = NaiveDateTime::parse_from_str(txt.value(i), &chrono_fmt).or_else(|_| {
                chrono::NaiveDate::parse_from_str(txt.value(i), &chrono_fmt)
                    .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
            });
            let parsed = parsed.map_err(|e| {
                DataFusionError::Execution(format!(
                    "to_timestamp: failed to parse {:?} with format {:?} (chrono {:?}): {e}",
                    txt.value(i),
                    fmt.value(i),
                    chrono_fmt
                ))
            })?;
            let dt: DateTime<Utc> = Utc.from_utc_datetime(&parsed);
            out.push(dt.timestamp_nanos_opt());
        }
        let arr = TimestampNanosecondArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_l2_simple() {
        let r = rewrite_vector_operators(
            "SELECT id FROM t ORDER BY embedding <-> '[0.1, 0.2]' LIMIT 5",
        );
        assert_eq!(
            r,
            "SELECT id FROM t ORDER BY l2_distance(embedding, '[0.1, 0.2]') LIMIT 5"
        );
    }

    #[test]
    fn rewrite_dot_negates() {
        let r = rewrite_vector_operators("SELECT a <#> b FROM t");
        assert_eq!(r, "SELECT (- dot_product(a, b)) FROM t");
    }

    #[test]
    fn rewrite_cosine() {
        let r = rewrite_vector_operators("SELECT a <=> b FROM t");
        assert_eq!(r, "SELECT cosine_distance(a, b) FROM t");
    }

    #[test]
    fn rewrite_paren_operand() {
        let r = rewrite_vector_operators("SELECT (a + b) <-> c FROM t");
        assert_eq!(r, "SELECT l2_distance((a + b), c) FROM t");
    }

    // --- auth schema rewriter ---

    #[test]
    fn auth_uid_rewritten() {
        let r = rewrite_auth_schema_functions("SELECT auth.uid()");
        assert_eq!(r, "SELECT auth_uid()");
    }

    #[test]
    fn auth_role_rewritten() {
        let r = rewrite_auth_schema_functions("WHERE auth.role() = 'authenticated'");
        assert_eq!(r, "WHERE auth_role() = 'authenticated'");
    }

    #[test]
    fn auth_jwt_rewritten() {
        let r = rewrite_auth_schema_functions("SELECT auth.jwt()");
        assert_eq!(r, "SELECT auth_jwt()");
    }

    #[test]
    fn auth_rewrite_case_insensitive() {
        let r = rewrite_auth_schema_functions("SELECT AUTH.UID()");
        assert_eq!(r, "SELECT auth_uid()");
    }

    #[test]
    fn auth_rewrite_multiple_calls() {
        let r = rewrite_auth_schema_functions(
            "SELECT auth.uid(), auth.role() FROM t WHERE owner_id = auth.uid()",
        );
        assert_eq!(
            r,
            "SELECT auth_uid(), auth_role() FROM t WHERE owner_id = auth_uid()"
        );
    }

    #[test]
    fn auth_rewrite_leaves_prefix_alone() {
        // `my_auth.uid()` should NOT be rewritten (identifier boundary guard).
        let r = rewrite_auth_schema_functions("SELECT my_auth.uid()");
        assert_eq!(r, "SELECT my_auth.uid()");
    }

    #[test]
    fn auth_rewrite_leaves_suffix_alone() {
        // `auth.uid_column` should NOT be rewritten (post-boundary guard).
        let r = rewrite_auth_schema_functions("SELECT auth.uid_column FROM t");
        assert_eq!(r, "SELECT auth.uid_column FROM t");
    }
}

/// PG-shape `power(x, y)` — always returns `double precision` (Float64).
/// Replaces DataFusion's default `power`, which returns Int64 when both
/// inputs are integer-typed. PG's `power` is the float-only variant; the
/// integer-keeping form is the `^` operator. Callers that downcast
/// `power`'s result to `numeric` or use it in float arithmetic break on
/// the Int64 shape; widening to Float64 unconditionally is the practical
/// fix.
#[derive(Debug)]
struct PowerFloat64Udf {
    signature: Signature,
}

impl ScalarUDFImpl for PowerFloat64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "power"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("power expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let base = args[0].clone().into_array(n)?;
        let exp = args[1].clone().into_array(n)?;
        let base = base
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("power: base did not coerce to Float64".into())
            })?;
        let exp = exp.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            DataFusionError::Execution("power: exponent did not coerce to Float64".into())
        })?;
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            if base.is_null(i) || exp.is_null(i) {
                out.append_null();
            } else {
                out.append_value(base.value(i).powf(exp.value(i)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// PG-shape `extract(second FROM ts)` — returns `double precision` with
/// sub-second precision. DataFusion's `date_part('second', ...)` returns
/// Int32 (whole seconds only); PG returns `numeric` with the fractional
/// part included. Float64 captures microsecond precision losslessly for
/// any realistic timestamp; we don't model PG's `numeric` directly because
/// the workspace arrow bridge has no `numeric` type yet.
///
/// Wired in via `rewrite_extract_second` at the SQL-string layer rather
/// than via DataFusion's `ExprPlanner`. The default planner list has
/// `UserDefinedFunctionPlanner` returning `Planned` for `EXTRACT` before
/// any session-registered planner is consulted, so the string rewrite
/// sidesteps that ordering problem and matches the existing pg_vector
/// operator-rewrite pattern.
#[derive(Debug)]
struct ExtractSecondPgUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ExtractSecondPgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "__basin_extract_second"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "__basin_extract_second expects 1 argument, got {}",
                args.len()
            );
        }
        let n = match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            _ => 1,
        };
        let arr = args[0].clone().into_array(n)?;
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            match ts_array_to_seconds_f64(&arr, i)? {
                Some(v) => out.append_value(v),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `__basin_assert(predicate BOOL, error_text TEXT) -> Int64`. Used by
/// constraint-shaped reactors (5.11.C2) as a planner-friendly stand-in
/// for `CHECK (<predicate>)`. Returns 1 for every row in which the
/// predicate is true; raises `DataFusionError::Execution(error_text)`
/// when any row's predicate is false. The error string is expected to
/// carry the literal `SQLSTATE 23514 check_violation` token so router-
/// side error mapping classifies the failure as a check violation.
///
/// Volatility is `Volatile` to keep the planner from constant-folding a
/// constant-true predicate into the literal `1` (the side-effect of
/// raising on false is the whole point of the UDF).
#[derive(Debug)]
struct BasinAssertUdf {
    signature: Signature,
}

impl ScalarUDFImpl for BasinAssertUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "__basin_assert"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("__basin_assert expects 2 arguments, got {}", args.len());
        }
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let pred = args[0].clone().into_array(n)?;
        let msg = args[1].clone().into_array(n)?;
        let pred = pred
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "__basin_assert: first argument did not coerce to Boolean".into(),
                )
            })?;
        let msg = msg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution(
                "__basin_assert: second argument did not coerce to Utf8".into(),
            )
        })?;
        // A NULL predicate is treated as a check violation, matching PG's
        // `CHECK` semantics where `NULL` is not "satisfied".
        let mut out = Int64Array::builder(n);
        for i in 0..n {
            let ok = !pred.is_null(i) && pred.value(i);
            if !ok {
                let m = if msg.is_null(i) {
                    "SQLSTATE 23514 check_violation".to_string()
                } else {
                    msg.value(i).to_string()
                };
                return exec_err!("{}", m);
            }
            out.append_value(1);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Compute the PG-style `extract(second FROM ts)` value for row `i` of an
/// arrow timestamp array: the integer second-of-minute (0..=59) plus the
/// sub-second fraction expressed in the array's native unit.
fn ts_array_to_seconds_f64(arr: &ArrayRef, i: usize) -> DFResult<Option<f64>> {
    match arr.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let sub_ns = v.rem_euclid(60 * 1_000_000_000);
            Ok(Some(sub_ns as f64 / 1_000_000_000.0))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let sub_us = v.rem_euclid(60 * 1_000_000);
            Ok(Some(sub_us as f64 / 1_000_000.0))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            let v = a.value(i);
            let sub_ms = v.rem_euclid(60 * 1_000);
            Ok(Some(sub_ms as f64 / 1_000.0))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            if a.is_null(i) {
                return Ok(None);
            }
            Ok(Some((a.value(i).rem_euclid(60)) as f64))
        }
        other => exec_err!("__basin_extract_second: unsupported timestamp type {other:?}"),
    }
}

/// SQL-string rewrite for `EXTRACT(SECOND FROM <expr>)` ->
/// `__basin_extract_second(<expr>)`.
///
/// Mirrors the design of [`rewrite_vector_operators`]: a textual rewrite
/// applied before sqlparser sees the SQL. We deliberately don't rewrite
/// other EXTRACT fields — DataFusion's default `date_part` returns Int32
/// for them, which matches PG's textual rendering for whole-number fields
/// and is what existing call sites expect.
///
/// LIMITATIONS — same as the operator rewriter:
///   * does not understand quoted strings (an EXTRACT inside `'...'` will
///     still be rewritten);
///   * matches `SECOND` only, not `SECONDS` / `SEC` synonyms;
///   * matches one EXTRACT(...) per pass; iterates until no match remains.
pub(crate) fn rewrite_extract_second(sql: &str) -> String {
    let mut s = sql.to_string();
    loop {
        let Some(start) = find_extract_second(&s) else {
            break;
        };
        let Some(from_end) = find_extract_second_from_end(&s, start) else {
            break;
        };
        let bytes = s.as_bytes();
        let mut i = from_end;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let expr_start = i;
        let mut depth = 1i32;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                break;
            }
            i += 1;
        }
        if depth != 0 {
            break;
        }
        let expr_end = i;
        let close_paren = i;
        let expr = s[expr_start..expr_end].trim().to_string();
        let replacement = format!("__basin_extract_second({})", expr);
        s.replace_range(start..close_paren + 1, &replacement);
    }
    s
}

/// Locate the next `EXTRACT(SECOND FROM` occurrence (case-insensitive,
/// flexible whitespace). Returns the byte offset of the leading `E`.
fn find_extract_second(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut idx = 0usize;
    while idx + 7 <= lb.len() {
        if &lb[idx..idx + 7] == b"extract" {
            let pre_ok = idx == 0 || {
                let c = bytes[idx - 1];
                !(c.is_ascii_alphanumeric() || c == b'_')
            };
            if pre_ok && find_extract_second_from_end(s, idx).is_some() {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
}

/// Given that `start` points at an `EXTRACT` keyword, verify the rest of
/// the prefix is `(SECOND FROM` and return the byte offset just past `FROM`.
/// Returns `None` if the surrounding tokens don't match.
fn find_extract_second_from_end(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut j = start + 7;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'(' {
        return None;
    }
    let mut k = j + 1;
    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    if k + 6 > lb.len() || &lb[k..k + 6] != b"second" {
        return None;
    }
    let after_field = k + 6;
    let post_ok = after_field >= bytes.len() || {
        let c = bytes[after_field];
        !(c.is_ascii_alphanumeric() || c == b'_')
    };
    if !post_ok {
        return None;
    }
    let mut m = after_field;
    while m < bytes.len() && bytes[m].is_ascii_whitespace() {
        m += 1;
    }
    if m + 4 > lb.len() || &lb[m..m + 4] != b"from" {
        return None;
    }
    let after_from = m + 4;
    let from_post_ok = after_from >= bytes.len() || {
        let c = bytes[after_from];
        !(c.is_ascii_alphanumeric() || c == b'_')
    };
    if !from_post_ok {
        return None;
    }
    Some(after_from)
}

// ---------------------------------------------------------------------------
// Auth schema-dot rewriter: auth.uid() → auth_uid(), etc.
//
// DataFusion's SQL parser does not support schema-qualified function names
// in call position. We rewrite at the raw-SQL string level (before sqlparser
// sees the text) so users can write the Supabase-canonical `auth.uid()` form
// and have it reach the UDFs registered as `auth_uid` etc.
//
// The rewrite is exact-token-sensitive: `my_auth.uid()` or `auth.uidx()` are
// left alone. Only the three canonical names are rewritten.
// ---------------------------------------------------------------------------

/// Rewrite `auth.uid()`, `auth.role()`, and `auth.jwt()` to their
/// underscore-namespaced equivalents that DataFusion can call.
///
/// The rewrite is case-insensitive on the function names to match PG's
/// identifier folding (`AUTH.UID()` → `auth_uid()`). The `auth.` prefix
/// must be preceded by a non-identifier character (or be at the start of
/// the string) so that column names like `my_auth.uid()` aren't touched.
pub(crate) fn rewrite_auth_schema_functions(sql: &str) -> String {
    const TARGETS: &[(&str, &str)] = &[
        ("auth.uid", "auth_uid"),
        ("auth.role", "auth_role"),
        ("auth.jwt", "auth_jwt"),
    ];
    let mut out = sql.to_string();
    for (schema_dot_name, replacement) in TARGETS {
        // Scan `out` left-to-right, rewriting every occurrence that passes
        // the identifier-boundary checks. We rebuild `lower` on each pass
        // so the position arithmetic stays correct after replacements.
        let mut scan_pos = 0usize;
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(rel_found) = lower[scan_pos..].find(schema_dot_name) else {
                break;
            };
            let abs_start = scan_pos + rel_found;
            let abs_end = abs_start + schema_dot_name.len();

            // Identifier-boundary checks.
            let pre_ok = abs_start == 0 || {
                let prev = out.as_bytes()[abs_start - 1];
                !(prev.is_ascii_alphanumeric() || prev == b'_')
            };
            let post_ok = abs_end >= out.len() || {
                let next = out.as_bytes()[abs_end];
                !(next.is_ascii_alphanumeric() || next == b'_')
            };

            if pre_ok && post_ok {
                out.replace_range(abs_start..abs_end, replacement);
                // Advance past the replacement text.
                scan_pos = abs_start + replacement.len();
            } else {
                // Skip past this false match and keep scanning.
                scan_pos = abs_start + 1;
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Auth session UDFs: auth_uid(), auth_role(), auth_jwt()
//
// These are Supabase-compatible session functions. Names use underscores
// rather than dots because DataFusion's SQL parser doesn't support
// schema-qualified function names in function-call position. The SQL
// preprocessor in `sql_functions.rs` rewrites `auth.uid()` →
// `auth_uid()`, etc., so both spellings work at the SQL layer.
//
// All three capture an `Arc<AuthContext>` at session-open time. The
// implementation is purely a read of that captured context — no I/O,
// no locks beyond the Arc.
// ---------------------------------------------------------------------------

use crate::AuthContext;

/// Register `auth_uid()`, `auth_role()`, and `auth_jwt()` on `ctx`.
/// Each UDF captures an `Arc<AuthContext>` at session-open time so that
/// evaluating any of the three functions is a pure read of already-resolved
/// claims — zero per-query I/O or synchronisation overhead.
pub(crate) fn register_auth_udfs(ctx: &SessionContext, auth_context: Arc<AuthContext>) {
    ctx.register_udf(ScalarUDF::from(AuthUidUdf {
        auth_context: auth_context.clone(),
        signature: Signature::nullary(Volatility::Stable),
    }));
    ctx.register_udf(ScalarUDF::from(AuthRoleUdf {
        auth_context: auth_context.clone(),
        signature: Signature::nullary(Volatility::Stable),
    }));
    ctx.register_udf(ScalarUDF::from(AuthJwtUdf {
        auth_context,
        signature: Signature::nullary(Volatility::Stable),
    }));
}

// --- auth_uid() -------------------------------------------------------------

/// `auth_uid() -> Utf8` (nullable). Returns the UUID of the authenticated
/// user as a text string (matching Postgres's UUID wire format), or NULL
/// if the session is unauthenticated.
#[derive(Debug)]
struct AuthUidUdf {
    auth_context: Arc<AuthContext>,
    signature: Signature,
}

impl ScalarUDFImpl for AuthUidUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "auth_uid"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Nullable Utf8: NULL when unauthenticated, UUID string when auth'd.
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        match &self.auth_context.auth_uid {
            Some(uid) => {
                let s = uid.to_string();
                Ok(ColumnarValue::Scalar(
                    datafusion::scalar::ScalarValue::Utf8(Some(s)),
                ))
            }
            None => Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(None),
            )),
        }
    }
}

// --- auth_role() ------------------------------------------------------------

/// `auth_role() -> Utf8`. Returns the session role string: `'authenticated'`,
/// `'anon'`, or `'service_role'`. Never NULL — unauthenticated sessions
/// return `'anon'`.
#[derive(Debug)]
struct AuthRoleUdf {
    auth_context: Arc<AuthContext>,
    signature: Signature,
}

impl ScalarUDFImpl for AuthRoleUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "auth_role"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some(self.auth_context.auth_role.clone())),
        ))
    }
}

// --- auth_jwt() -------------------------------------------------------------

/// `auth_jwt() -> Utf8` (nullable, JSON). Returns the full JWT claims as a
/// JSON text value, or NULL if the session was not established via JWT.
/// The value is a serialised JSON object matching the wire claims layout:
/// `{ "user_id": "...", "email": "...", "roles": [...], "exp": ..., ... }`.
#[derive(Debug)]
struct AuthJwtUdf {
    auth_context: Arc<AuthContext>,
    signature: Signature,
}

impl ScalarUDFImpl for AuthJwtUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "auth_jwt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Return Utf8 (JSON text). DataFusion doesn't have a native JSONB
        // column type; downstream callers that need structured access can
        // cast to Utf8 and use `json_get_*` builtins or extract at the
        // application layer, matching how Supabase surfaces `auth.jwt()`.
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        match &self.auth_context.auth_claims {
            Some(claims) => {
                let json_str = serde_json::to_string(claims).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "auth_jwt: claims serialization failed: {e}"
                    ))
                })?;
                Ok(ColumnarValue::Scalar(
                    datafusion::scalar::ScalarValue::Utf8(Some(json_str)),
                ))
            }
            None => Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(None),
            )),
        }
    }
}

<<<<<<< HEAD
// ---------------------------------------------------------------------------
// JSON operator text rewriter
// ---------------------------------------------------------------------------
//
// Postgres JSON/JSONB operators that DataFusion cannot parse as infix operators
// are rewritten to UDF calls before the SQL reaches sqlparser/DataFusion.
//
// Mapping:
//   j -> 'key'          →  json_get(j, 'key')
//   j ->> 'key'         →  json_get_text(j, 'key')
//   j #> '{a,b}'        →  json_path_extract(j, '{a,b}')
//   j #>> '{a,b}'       →  json_path_extract_text(j, '{a,b}')
//   j @> '{"a":1}'      →  jsonb_contains(j, '{"a":1}')   (JSON only)
//   j <@ '{"a":1}'      →  jsonb_contained_by(j, '{"a":1}')
//   j ? 'key'           →  jsonb_has_key(j, 'key')
//   j ?& '{k1,k2}'      →  jsonb_has_all_keys(j, '{k1,k2}')
//   j ?| '{k1,k2}'      →  jsonb_has_any_key(j, '{k1,k2}')
//   j || '{"b":2}'      →  jsonb_concat(j, '{"b":2}')
//   j @? '$.field'      →  jsonb_path_exists(j, '$.field')
//
// NOTE: `@>` for range types is already handled by DataFusion's range
// operator support. The JSON-specific rewrite is triggered only when the
// RHS looks like a JSON literal (starts with `{`, `[`, `'{"`, etc.).
// This heuristic avoids collisions with range-type `@>`.
//
// LIMITATION: This is a textual rewrite; operators inside string literals
// will also be rewritten. For ORM-generated queries this is acceptable.

/// Rewrite JSON/JSONB infix operators to UDF calls.
pub(crate) fn rewrite_json_operators(sql: &str) -> String {
    // Handle @> specially — only when RHS token looks like a JSON literal
    let mut s = rewrite_json_at_gt(sql);

    // Handle || for JSON concat — only when RHS looks like JSON
    s = rewrite_json_concat_op(&s);

    // @? — jsonpath exists
    s = rewrite_binary_op_to_fn(&s, "@?", "jsonb_path_exists");

    // Operators ordered longest-first to avoid prefix collisions
    for &(op, func) in &[
        ("#>>", "json_path_extract_text"),
        ("#>",  "json_path_extract"),
        ("->>", "json_get_text"),
        ("->",  "json_get"),
        ("?&",  "jsonb_has_all_keys"),
        ("?|",  "jsonb_has_any_key"),
        ("?",   "jsonb_has_key"),
        ("<@",  "jsonb_contained_by"),
    ] {
        s = rewrite_binary_op_to_fn(&s, op, func);
    }
    s
}

/// Rewrite `expr @> json_literal` to `jsonb_contains(expr, json_literal)`.
fn rewrite_json_at_gt(s: &str) -> String {
    let mut out = s.to_string();
    let mut search_from = 0usize;
    loop {
        let Some(rel) = out[search_from..].find("@>") else { break; };
        let op_start = search_from + rel;
        let op_end = op_start + 2;

        // Look ahead past whitespace for RHS token — copy bytes to avoid borrow conflict
        let rhs_looks_json = {
            let bytes = out.as_bytes();
            let mut j = op_end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
            j < bytes.len() && (bytes[j] == b'\'' || bytes[j] == b'{' || bytes[j] == b'[')
        };
        if rhs_looks_json {
            let (lhs_start, lhs_end) = extract_left_operand(&out, op_start);
            let (rhs_start, rhs_end) = extract_right_operand(&out, op_end);
            let lhs = out[lhs_start..lhs_end].to_string();
            let rhs = out[rhs_start..rhs_end].to_string();
            let replacement = format!("jsonb_contains({lhs}, {rhs})");
            out.replace_range(lhs_start..rhs_end, &replacement);
            search_from = lhs_start + replacement.len();
        } else {
            search_from = op_end;
=======
// ── PG aggregate alias rewriter ───────────────────────────────────────────────

/// Rewrite PostgreSQL aggregate function aliases to DataFusion equivalents.
///
/// Mappings applied (case-insensitive, word-boundary-safe):
///   `variance(…)` → `var(…)`       (DF primary name is "var"; aliases include "var_samp")
///   `every(…)`    → `bool_and(…)`  (PG synonym; DF has no alias for this)
///
/// The rewrite fires before DataFusion's SQL planner so users can write
/// standard PostgreSQL aggregate names in their queries.
pub(crate) fn rewrite_pg_agg_aliases(sql: &str) -> String {
    // Each entry: (name_without_paren, replacement_name_with_open_paren)
    const TARGETS: &[(&str, &str)] = &[
        ("variance", "var("),
        ("every", "bool_and("),
    ];
    let mut out = sql.to_string();
    for (from_name, to_with_paren) in TARGETS {
        let mut scan_pos = 0usize;
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(rel_found) = lower[scan_pos..].find(from_name) else {
                break;
            };
            let abs_start = scan_pos + rel_found;
            let abs_end = abs_start + from_name.len();

            // Identifier-boundary check before the name
            let pre_ok = abs_start == 0 || {
                let prev = out.as_bytes()[abs_start - 1];
                !(prev.is_ascii_alphanumeric() || prev == b'_')
            };
            // Identifier-boundary check after the name
            let post_char_ok = abs_end >= out.len() || {
                let next = out.as_bytes()[abs_end];
                // Must be followed by '(' or whitespace (not another identifier char)
                next == b'(' || next == b' ' || next == b'\t' || next == b'\n' || next == b'\r'
            };

            if pre_ok && post_char_ok {
                // Find the first '(' at or after abs_end
                let maybe_paren = out[abs_end..].find('(').map(|p| abs_end + p);
                if let Some(paren_pos) = maybe_paren {
                    // Only rewrite if only whitespace between name and '('
                    let between = &out[abs_end..paren_pos];
                    if between.chars().all(char::is_whitespace) {
                        let replace_end = paren_pos + 1; // consume the '(' too
                        out.replace_range(abs_start..replace_end, to_with_paren);
                        scan_pos = abs_start + to_with_paren.len();
                        continue;
                    }
                }
            }
            scan_pos = abs_start + 1;
>>>>>>> worktree-agent-agent-bulk-pg-aggregates
        }
    }
    out
}
<<<<<<< HEAD

/// Rewrite `expr || expr` to `jsonb_concat(expr, expr)` when RHS looks like JSON.
fn rewrite_json_concat_op(s: &str) -> String {
    let mut out = s.to_string();
    let mut search_from = 0usize;
    loop {
        let Some(rel) = out[search_from..].find("||") else { break; };
        let op_start = search_from + rel;
        let op_end = op_start + 2;
        // Determine if RHS looks like JSON without holding a borrow into `out`
        let rhs_is_json = {
            let bytes = out.as_bytes();
            let mut j = op_end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
            j < bytes.len() && (
                bytes[j] == b'{'
                || bytes[j] == b'['
                || (bytes[j] == b'\'' && j + 1 < bytes.len()
                    && (bytes[j+1] == b'{' || bytes[j+1] == b'[' || bytes[j+1] == b'"'))
            )
        };
        if rhs_is_json {
            let (lhs_start, lhs_end) = extract_left_operand(&out, op_start);
            let (rhs_start, rhs_end) = extract_right_operand(&out, op_end);
            let lhs = out[lhs_start..lhs_end].to_string();
            let rhs = out[rhs_start..rhs_end].to_string();
            let replacement = format!("jsonb_concat({lhs}, {rhs})");
            out.replace_range(lhs_start..rhs_end, &replacement);
            search_from = lhs_start + replacement.len();
        } else {
            search_from = op_end;
        }
    }
    out
}

/// Generic rewriter: replace `lhs OP rhs` with `func(lhs, rhs)`.
fn rewrite_binary_op_to_fn(sql: &str, op: &str, func: &str) -> String {
    let mut s = sql.to_string();
    let mut search_from = 0usize;
    loop {
        let Some(rel) = s[search_from..].find(op) else { break; };
        let op_start = search_from + rel;
        let op_end = op_start + op.len();

        // Boundary guard: previous char must not be alphanumeric/`_`/`#`/`?`/`@`/`<`/`>`
        // (avoids matching inside a longer already-rewritten operator)
        let prev_ok = op_start == 0 || {
            let b = s.as_bytes()[op_start - 1];
            !b.is_ascii_alphanumeric() && b != b'_' && b != b'#' && b != b'?' && b != b'@' && b != b'<' && b != b'>'
        };
        let next_ok = op_end >= s.len() || {
            let b = s.as_bytes()[op_end];
            !b.is_ascii_alphanumeric() && b != b'_' && b != b'>' && b != b'|' && b != b'&'
        };

        if !prev_ok || !next_ok {
            search_from = op_end;
            continue;
        }

        let (lhs_start, lhs_end) = extract_left_operand(&s, op_start);
        let (rhs_start, rhs_end) = extract_right_operand(&s, op_end);
        let lhs = s[lhs_start..lhs_end].to_string();
        let rhs = s[rhs_start..rhs_end].to_string();
        let replacement = format!("{func}({lhs}, {rhs})");
        s.replace_range(lhs_start..rhs_end, &replacement);
        search_from = lhs_start + replacement.len();
    }
    s
}

#[cfg(test)]
mod json_op_rewrite_tests {
    use super::rewrite_json_operators;

    #[test]
    fn test_arrow_op() {
        let r = rewrite_json_operators("SELECT data -> 'key' FROM t");
        assert!(r.contains("json_get(data, 'key')"), "got: {r}");
    }

    #[test]
    fn test_double_arrow_op() {
        let r = rewrite_json_operators("SELECT data ->> 'name' FROM t");
        assert!(r.contains("json_get_text(data, 'name')"), "got: {r}");
    }

    #[test]
    fn test_hash_arrow_op() {
        let r = rewrite_json_operators("SELECT data #> '{a,b}' FROM t");
        assert!(r.contains("json_path_extract(data, '{a,b}')"), "got: {r}");
    }

    #[test]
    fn test_hash_double_arrow_op() {
        let r = rewrite_json_operators("SELECT data #>> '{a,b}' FROM t");
        assert!(r.contains("json_path_extract_text(data, '{a,b}')"), "got: {r}");
    }

    #[test]
    fn test_has_key_op() {
        let r = rewrite_json_operators("SELECT data ? 'foo' FROM t");
        assert!(r.contains("jsonb_has_key(data, 'foo')"), "got: {r}");
    }
}
=======
>>>>>>> worktree-agent-agent-bulk-pg-aggregates
