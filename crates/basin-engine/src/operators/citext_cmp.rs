//! Case-insensitive comparison operators for `CITEXT` columns (Phase 5.30.C).
//!
//! PostgreSQL `citext` semantics:
//! - `=`, `<>`, `<`, `<=`, `>`, `>=` all compare case-insensitively.
//! - `LIKE` / pattern matching is case-insensitive.
//! - `ORDER BY` on a citext column sorts case-insensitively.
//! - `UNIQUE` constraints treat `'Foo'` and `'foo'` as duplicates.
//!
//! Implementation strategy: case-fold both operands using Unicode lower-case
//! mapping (`to_lowercase()`) before any comparison. This matches PostgreSQL
//! citext's behaviour which delegates to `pg_strncasecmp` (locale-aware, but
//! equivalent to lower() for the default C/POSIX locale Basin targets).

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::DataType;
use basin_common::types::citext::{citext_cmp, citext_eq, citext_like};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

// ─────────────────────────────────────────────────────────────────────────────
// Row-level key extraction (used by UNIQUE enforcement)
// ─────────────────────────────────────────────────────────────────────────────

/// Extract the case-folded UNIQUE key for a citext column at `(arr, row)`.
///
/// Returns `None` if the value is NULL (citext NULL is exempt from UNIQUE
/// checks, matching PG semantics). Returns the lower-cased string otherwise.
pub(crate) fn citext_fold_value(arr: &dyn Array, row: usize) -> Option<String> {
    if arr.is_null(row) {
        return None;
    }
    let sa = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("citext column must be Utf8/StringArray");
    Some(sa.value(row).to_lowercase())
}

// ─────────────────────────────────────────────────────────────────────────────
// DataFusion ScalarUDF implementations
// ─────────────────────────────────────────────────────────────────────────────

fn text_text_bool_sig() -> Signature {
    Signature::new(
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        Volatility::Immutable,
    )
}

/// `citext_eq(a TEXT, b TEXT) -> BOOL` — case-insensitive equality.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextEqUdf {
    signature: Signature,
}

impl CitextEqUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextEqUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_eq" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        apply_binary_bool(&args.args, |a, b| citext_eq(a, b))
    }
}

/// `citext_ne(a TEXT, b TEXT) -> BOOL` — case-insensitive inequality.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextNeUdf {
    signature: Signature,
}

impl CitextNeUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextNeUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_ne" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        apply_binary_bool(&args.args, |a, b| !citext_eq(a, b))
    }
}

/// `citext_lt(a TEXT, b TEXT) -> BOOL` — case-insensitive less-than.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextLtUdf {
    signature: Signature,
}

impl CitextLtUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextLtUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_lt" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use std::cmp::Ordering;
        apply_binary_bool(&args.args, |a, b| citext_cmp(a, b) == Ordering::Less)
    }
}

/// `citext_le(a TEXT, b TEXT) -> BOOL` — case-insensitive less-than-or-equal.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextLeUdf {
    signature: Signature,
}

impl CitextLeUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextLeUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_le" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use std::cmp::Ordering;
        apply_binary_bool(&args.args, |a, b| {
            matches!(citext_cmp(a, b), Ordering::Less | Ordering::Equal)
        })
    }
}

/// `citext_gt(a TEXT, b TEXT) -> BOOL` — case-insensitive greater-than.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextGtUdf {
    signature: Signature,
}

impl CitextGtUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextGtUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_gt" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use std::cmp::Ordering;
        apply_binary_bool(&args.args, |a, b| citext_cmp(a, b) == Ordering::Greater)
    }
}

/// `citext_ge(a TEXT, b TEXT) -> BOOL` — case-insensitive greater-than-or-equal.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextGeUdf {
    signature: Signature,
}

impl CitextGeUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextGeUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_ge" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use std::cmp::Ordering;
        apply_binary_bool(&args.args, |a, b| {
            matches!(citext_cmp(a, b), Ordering::Greater | Ordering::Equal)
        })
    }
}

/// `citext_like(value TEXT, pattern TEXT) -> BOOL` — case-insensitive LIKE.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CitextLikeUdf {
    signature: Signature,
}

impl CitextLikeUdf {
    pub fn new() -> Self {
        Self { signature: text_text_bool_sig() }
    }
}

impl ScalarUDFImpl for CitextLikeUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "citext_like" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> { Ok(DataType::Boolean) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        apply_binary_bool(&args.args, |a, b| citext_like(a, b))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helper
// ─────────────────────────────────────────────────────────────────────────────

/// Apply a binary `(str, str) -> bool` function across two columnar Utf8 args,
/// returning a `BooleanArray` wrapped in `ColumnarValue::Array`.
fn apply_binary_bool(
    args: &[ColumnarValue],
    f: impl Fn(&str, &str) -> bool,
) -> DFResult<ColumnarValue> {
    assert_eq!(args.len(), 2, "citext comparison takes exactly 2 arguments");

    // Determine the batch size from whichever arg is an array.
    let batch_size = args
        .iter()
        .find_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .unwrap_or(1);

    let a = args[0].clone().into_array(batch_size)?;
    let b = args[1].clone().into_array(batch_size)?;

    let sa = a.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal("citext_cmp: arg 0 expected Utf8".into())
    })?;
    let sb = b.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal("citext_cmp: arg 1 expected Utf8".into())
    })?;

    let result: BooleanArray = sa
        .iter()
        .zip(sb.iter())
        .map(|(av, bv)| match (av, bv) {
            (Some(a), Some(b)) => Some(f(a, b)),
            _ => None, // NULL propagation
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Registration helper
// ─────────────────────────────────────────────────────────────────────────────

/// Return all citext comparison UDFs as `Arc<ScalarUDF>` for registration
/// in a DataFusion `SessionContext`.
pub fn citext_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(CitextEqUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextNeUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextLtUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextLeUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextGtUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextGeUdf::new())),
        Arc::new(ScalarUDF::new_from_impl(CitextLikeUdf::new())),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn citext_fold_value_basic() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["Hello", "WORLD", "foo"]));
        assert_eq!(citext_fold_value(&*arr, 0), Some("hello".to_string()));
        assert_eq!(citext_fold_value(&*arr, 1), Some("world".to_string()));
        assert_eq!(citext_fold_value(&*arr, 2), Some("foo".to_string()));
    }

    #[test]
    fn citext_fold_value_null() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![None::<&str>]));
        assert_eq!(citext_fold_value(&*arr, 0), None);
    }
}
