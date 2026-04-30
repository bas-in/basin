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

use basin_vector::{cosine_distance as v_cosine, dot_product as v_dot, l2_distance as v_l2};
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::types::parse_vector_literal;

/// Register the three pg_vector-shaped distance UDFs on `ctx`. Idempotent
/// (DataFusion overwrites by name).
pub(crate) fn register_distance_udfs(ctx: &SessionContext) {
    ctx.register_udf(make_udf("l2_distance", DistanceFn::L2));
    ctx.register_udf(make_udf("cosine_distance", DistanceFn::Cosine));
    ctx.register_udf(make_udf("dot_product", DistanceFn::Dot));
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
