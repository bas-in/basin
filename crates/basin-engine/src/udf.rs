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
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, StringArray,
};
use datafusion::arrow::datatypes::DataType;
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
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        ),
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
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(CryptUdf {
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Volatile,
        ),
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
        return exec_err!(
            "{} expects 2 arguments, got {}",
            kind.name(),
            args.len()
        );
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
                let array = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{fn_name} {side}: not a StringArray"
                        ))
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
        "hex" => hex::decode(s)
            .map_err(|e| DataFusionError::Execution(format!("decode hex: {e}"))),
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
                        let d2 = bytes
                            .next()
                            .ok_or_else(|| DataFusionError::Execution(
                                "decode escape: truncated octal".into(),
                            ))?;
                        let d3 = bytes
                            .next()
                            .ok_or_else(|| DataFusionError::Execution(
                                "decode escape: truncated octal".into(),
                            ))?;
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
    let cost: u32 = parts[2].parse().map_err(|_| {
        DataFusionError::Execution(format!("crypt: bad cost in salt {salt:?}"))
    })?;
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
    const ALPHABET: &[u8; 64] =
        b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
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
                let a = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
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
    const ALPHABET: &[u8; 64] =
        b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_l2_simple() {
        let r = rewrite_vector_operators("SELECT id FROM t ORDER BY embedding <-> '[0.1, 0.2]' LIMIT 5");
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
}
