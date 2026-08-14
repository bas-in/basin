//! String and DateTime UDFs filling the gap between DataFusion builtins and
//! PostgreSQL's standard library.
//!
//! ## String UDFs added here
//!
//! | Function | PG signature | Notes |
//! |---|---|---|
//! | `split_part` | `(text, delim text, n int) → text` | 1-indexed; negative n counts from end |
//! | `reverse` | `(text) → text` | reverse Unicode chars |
//! | `format` | `(fmt text, variadic args...) → text` | %s → arg; %I → quoted ident; %L → quoted literal |
//! | `quote_ident` | `(text) → text` | wrap in `"..."`, double internal `"` |
//! | `quote_literal` | `(text) → text` | wrap in `'...'`, double internal `'` |
//! | `quote_nullable` | `(text) → text` | NULL → literal `'NULL'`; else quote_literal |
//! | `regexp_match` | `(string, pattern [, flags]) → text[]` | first match groups as List(Utf8) |
//! | `regexp_matches` | `(string, pattern [, flags]) → text[]` | stub: same as regexp_match for v0.1 |
//! | `regexp_split_to_array` | `(string, pattern [, flags]) → text[]` | split by regex |
//! | `regexp_split_to_table` | `(string, pattern) → text` | stub: returns input unchanged |
//! | `chr` | `(int) → text` | codepoint to char |
//! | `ascii` | `(text) → int` | codepoint of first char |
//! | `translate` | `(string, from text, to text) → text` | per-char translation |
//! | `btrim` | `(text [, chars text]) → text` | trim both sides |
//! | `ltrim` | `(text [, chars text]) → text` | trim left |
//! | `rtrim` | `(text [, chars text]) → text` | trim right |
//! | `convert_from` | `(bytea, encoding text) → text` | UTF-8 decode best-effort |
//! | `convert_to` | `(text, encoding text) → bytea` | UTF-8 encode best-effort |
//!
//! ## DateTime UDFs added here
//!
//! `isfinite(timestamp)` and `isfinite(date)` → bool. Always `true` in v0.1;
//! infinity timestamps are deferred.
//!
//! Note: `make_date`, `make_time`, `make_timestamp`, `make_timestamptz`, and
//! `make_interval` are already registered by `udf::register_pg_compat_udfs`.
//! They are NOT re-registered here.

use std::any::Any;
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, BooleanBuilder, Int32Array, Int64Array, ListArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// Registration entry point
// ---------------------------------------------------------------------------

/// Register all string and datetime gap-filler UDFs on `ctx`. Idempotent.
pub(crate) fn register_string_dt_udfs(ctx: &SessionContext) {
    // ---- split_part ----
    ctx.register_udf(ScalarUDF::from(SplitPartUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- reverse ----
    ctx.register_udf(ScalarUDF::from(SimpleStrUdf {
        name: "reverse".into(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        op: StrOp::Reverse,
    }));

    // ---- format(fmt, args...) ---- overloads for 1–6 total args
    {
        let mut sigs: Vec<TypeSignature> = Vec::new();
        for extra in 0usize..=5 {
            sigs.push(TypeSignature::Exact(vec![DataType::Utf8; 1 + extra]));
        }
        ctx.register_udf(ScalarUDF::from(FormatUdf {
            signature: Signature::one_of(sigs, Volatility::Immutable),
        }));
    }

    // ---- quote_ident ----
    ctx.register_udf(ScalarUDF::from(SimpleStrUdf {
        name: "quote_ident".into(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        op: StrOp::QuoteIdent,
    }));

    // ---- quote_literal ----
    ctx.register_udf(ScalarUDF::from(SimpleStrUdf {
        name: "quote_literal".into(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        op: StrOp::QuoteLiteral,
    }));

    // ---- quote_nullable ----
    ctx.register_udf(ScalarUDF::from(QuoteNullableUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Null]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- regexp_match ----
    ctx.register_udf(ScalarUDF::from(RegexpMatchUdf {
        name: "regexp_match".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- regexp_matches (stub: same as regexp_match for v0.1) ----
    ctx.register_udf(ScalarUDF::from(RegexpMatchUdf {
        name: "regexp_matches".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- regexp_split_to_array ----
    ctx.register_udf(ScalarUDF::from(RegexpSplitToArrayUdf {
        name: "regexp_split_to_array".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- regexp_split_to_table (stub: returns input unchanged) ----
    ctx.register_udf(ScalarUDF::from(RegexpSplitToTableStubUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- chr ----
    ctx.register_udf(ScalarUDF::from(ChrUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- ascii ----
    ctx.register_udf(ScalarUDF::from(AsciiUdf {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    }));

    // ---- translate ----
    ctx.register_udf(ScalarUDF::from(TranslateUdf {
        signature: Signature::exact(
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        ),
    }));

    // ---- btrim ----
    ctx.register_udf(ScalarUDF::from(TrimCharsUdf {
        name: "btrim".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
        mode: TrimMode::Both,
    }));

    // ---- ltrim ----
    ctx.register_udf(ScalarUDF::from(TrimCharsUdf {
        name: "ltrim".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
        mode: TrimMode::Left,
    }));

    // ---- rtrim ----
    ctx.register_udf(ScalarUDF::from(TrimCharsUdf {
        name: "rtrim".into(),
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
        mode: TrimMode::Right,
    }));

    // ---- convert_from(bytea, encoding) → text ----
    ctx.register_udf(ScalarUDF::from(ConvertFromUdf {
        signature: Signature::exact(
            vec![DataType::Binary, DataType::Utf8],
            Volatility::Immutable,
        ),
    }));

    // ---- convert_to(text, encoding) → bytea ----
    ctx.register_udf(ScalarUDF::from(ConvertToUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
    }));

    // ---- isfinite(timestamp | timestamptz | date) → bool ----
    ctx.register_udf(ScalarUDF::from(IsFiniteUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Microsecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Millisecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Second, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("UTC".into()),
                )]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Second,
                    Some("UTC".into()),
                )]),
                TypeSignature::Exact(vec![DataType::Date32]),
            ],
            Volatility::Immutable,
        ),
    }));
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

fn to_str_vec(args: &[ColumnarValue], idx: usize, n: usize) -> DFResult<Vec<Option<String>>> {
    let arr = args[idx].clone().into_array(n)?;
    let strings = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("arg {idx} must be Utf8, got {:?}", arr.data_type()))
    })?;
    Ok((0..n)
        .map(|i| {
            if strings.is_null(i) {
                None
            } else {
                Some(strings.value(i).to_string())
            }
        })
        .collect())
}

fn str_vec_to_array(v: Vec<Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from(
        v.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
    ))
}

// ---------------------------------------------------------------------------
// split_part
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct SplitPartUdf {
    signature: Signature,
}

impl ScalarUDFImpl for SplitPartUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "split_part"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let delims = to_str_vec(args, 1, n)?;
        let parts_arr = args[2].clone().into_array(n)?;

        // Collect positions as i64 regardless of whether arg3 is Int32 or Int64.
        let positions: Vec<Option<i64>> =
            if let Some(arr) = parts_arr.as_any().downcast_ref::<Int32Array>() {
                (0..n)
                    .map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i) as i64)
                        }
                    })
                    .collect()
            } else if let Some(arr) = parts_arr.as_any().downcast_ref::<Int64Array>() {
                (0..n)
                    .map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect()
            } else {
                return exec_err!("split_part: arg 3 must be Int32 or Int64");
            };

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match (&strings[i], &delims[i]) {
                (None, _) | (_, None) => {
                    out.push(None);
                }
                _ if positions[i].is_none() => {
                    out.push(None);
                }
                (Some(s), Some(d)) => {
                    let p = positions[i].unwrap();
                    if p == 0 {
                        return exec_err!("split_part: field position must not be zero");
                    }
                    let pieces: Vec<&str> = s.split(d.as_str()).collect();
                    let result = if p > 0 {
                        pieces
                            .get((p as usize) - 1)
                            .copied()
                            .unwrap_or("")
                            .to_string()
                    } else {
                        let idx = pieces.len() as i64 + p;
                        if idx >= 0 {
                            pieces.get(idx as usize).copied().unwrap_or("").to_string()
                        } else {
                            String::new()
                        }
                    };
                    out.push(Some(result));
                }
            }
        }
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

// ---------------------------------------------------------------------------
// SimpleStrUdf — single-string → string transformations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StrOp {
    Reverse,
    QuoteIdent,
    QuoteLiteral,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SimpleStrUdf {
    name: String,
    signature: Signature,
    op: StrOp,
}

impl ScalarUDFImpl for SimpleStrUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let out: Vec<Option<String>> = strings
            .into_iter()
            .map(|opt| {
                opt.map(|s| match self.op {
                    StrOp::Reverse => s.chars().rev().collect::<String>(),
                    StrOp::QuoteIdent => pg_quote_ident(&s),
                    StrOp::QuoteLiteral => pg_quote_literal(&s),
                })
            })
            .collect();
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

fn pg_quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn pg_quote_literal(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

// ---------------------------------------------------------------------------
// format(fmt, args...)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatUdf {
    signature: Signature,
}

impl ScalarUDFImpl for FormatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "format"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.is_empty() {
            return exec_err!("format() requires at least 1 argument");
        }
        let n = num_rows(args);
        let fmts = to_str_vec(args, 0, n)?;

        // Build str vecs for remaining args
        let rest: Vec<Vec<Option<String>>> = (1..args.len())
            .map(|i| to_str_vec(args, i, n))
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match &fmts[i] {
                None => out.push(None),
                Some(fmt_str) => {
                    let row_args: Vec<Option<&str>> =
                        rest.iter().map(|col| col[i].as_deref()).collect();
                    out.push(Some(apply_format(fmt_str, &row_args)?));
                }
            }
        }
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

fn apply_format(fmt: &str, args: &[Option<&str>]) -> DFResult<String> {
    let mut out = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    let mut arg_idx = 0usize;

    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out.push('%'),
            Some('%') => out.push('%'),
            Some('s') => {
                let v = get_format_arg(args, arg_idx)?;
                out.push_str(v.as_deref().unwrap_or("NULL"));
                arg_idx += 1;
            }
            Some('I') => {
                let v = get_format_arg(args, arg_idx)?;
                out.push_str(&pg_quote_ident(v.as_deref().unwrap_or("")));
                arg_idx += 1;
            }
            Some('L') => {
                let v = get_format_arg(args, arg_idx)?;
                match v {
                    None => out.push_str("NULL"),
                    Some(s) => out.push_str(&pg_quote_literal(s)),
                }
                arg_idx += 1;
            }
            Some(other) => {
                out.push('%');
                out.push(other);
            }
        }
    }
    Ok(out)
}

fn get_format_arg<'a>(args: &[Option<&'a str>], idx: usize) -> DFResult<Option<&'a str>> {
    match args.get(idx) {
        None => exec_err!("format(): not enough arguments for format string"),
        Some(v) => Ok(*v),
    }
}

// ---------------------------------------------------------------------------
// quote_nullable
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct QuoteNullableUdf {
    signature: Signature,
}

impl ScalarUDFImpl for QuoteNullableUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "quote_nullable"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr = args[0].clone().into_array(n)?;
        let out: Vec<Option<String>> = match arr.data_type() {
            DataType::Null => (0..n).map(|_| Some("NULL".to_string())).collect(),
            DataType::Utf8 => {
                let strings = arr.as_any().downcast_ref::<StringArray>().unwrap();
                (0..n)
                    .map(|i| {
                        if strings.is_null(i) {
                            Some("NULL".to_string())
                        } else {
                            Some(pg_quote_literal(strings.value(i)))
                        }
                    })
                    .collect()
            }
            _ => {
                let cast = datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?;
                let strings = cast.as_any().downcast_ref::<StringArray>().unwrap();
                (0..n)
                    .map(|i| {
                        if strings.is_null(i) {
                            Some("NULL".to_string())
                        } else {
                            Some(pg_quote_literal(strings.value(i)))
                        }
                    })
                    .collect()
            }
        };
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

// ---------------------------------------------------------------------------
// regexp_match / regexp_matches  →  List(Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpMatchUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for RegexpMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let patterns = to_str_vec(args, 1, n)?;
        let flags = if args.len() > 2 {
            Some(to_str_vec(args, 2, n)?)
        } else {
            None
        };

        let mut values: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i32> = vec![0i32];
        let mut validity: Vec<bool> = Vec::with_capacity(n);

        for i in 0..n {
            let start = *offsets.last().unwrap();
            match (&strings[i], &patterns[i]) {
                (None, _) | (_, None) => {
                    offsets.push(start);
                    validity.push(false);
                }
                (Some(s), Some(pat)) => {
                    let flag_str = flags.as_ref().and_then(|f| f[i].as_deref()).unwrap_or("");
                    let re = build_regex(pat, flag_str)?;
                    if let Some(caps) = re.captures(s) {
                        let groups: Vec<Option<String>> = if caps.len() == 1 {
                            vec![Some(caps[0].to_string())]
                        } else {
                            (1..caps.len())
                                .map(|g| caps.get(g).map(|m| m.as_str().to_string()))
                                .collect()
                        };
                        let cnt = groups.len() as i32;
                        values.extend(groups);
                        offsets.push(start + cnt);
                        validity.push(true);
                    } else {
                        // No match → SQL NULL (PG semantics: regexp_match returns
                        // NULL when the pattern does not match the input).
                        offsets.push(start);
                        validity.push(false);
                    }
                }
            }
        }

        let values_arr: ArrayRef = Arc::new(StringArray::from(
            values.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ));
        let offsets_buf = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let nulls = datafusion::arrow::buffer::NullBuffer::from(validity);
        let list_arr = ListArray::new(field, offsets_buf, values_arr, Some(nulls));
        Ok(ColumnarValue::Array(Arc::new(list_arr)))
    }
}

// ---------------------------------------------------------------------------
// regexp_split_to_array → List(Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpSplitToArrayUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for RegexpSplitToArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let patterns = to_str_vec(args, 1, n)?;
        let flags = if args.len() > 2 {
            Some(to_str_vec(args, 2, n)?)
        } else {
            None
        };

        let mut values: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i32> = vec![0i32];

        for i in 0..n {
            let start = *offsets.last().unwrap();
            match (&strings[i], &patterns[i]) {
                (None, _) | (_, None) => {
                    offsets.push(start);
                }
                (Some(s), Some(pat)) => {
                    let flag_str = flags.as_ref().and_then(|f| f[i].as_deref()).unwrap_or("");
                    let re = build_regex(pat, flag_str)?;
                    let parts: Vec<Option<String>> =
                        re.split(s).map(|p| Some(p.to_string())).collect();
                    let cnt = parts.len() as i32;
                    values.extend(parts);
                    offsets.push(start + cnt);
                }
            }
        }

        let values_arr: ArrayRef = Arc::new(StringArray::from(
            values.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
        ));
        let offsets_buf = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list_arr = ListArray::new(field, offsets_buf, values_arr, None);
        Ok(ColumnarValue::Array(Arc::new(list_arr)))
    }
}

// ---------------------------------------------------------------------------
// regexp_split_to_table stub → Utf8 (returns input; real SETOF deferred)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpSplitToTableStubUdf {
    signature: Signature,
}

impl ScalarUDFImpl for RegexpSplitToTableStubUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_split_to_table"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        Ok(ColumnarValue::Array(str_vec_to_array(strings)))
    }
}

// ---------------------------------------------------------------------------
// chr(int) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ChrUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ChrUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "chr"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        match arr.data_type() {
            DataType::Int32 => {
                let ints = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..n {
                    if ints.is_null(i) {
                        out.push(None);
                    } else {
                        let cp = ints.value(i) as u32;
                        match char::from_u32(cp) {
                            Some(c) => out.push(Some(c.to_string())),
                            None => return exec_err!("chr: invalid codepoint {cp}"),
                        }
                    }
                }
            }
            DataType::Int64 => {
                let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..n {
                    if ints.is_null(i) {
                        out.push(None);
                    } else {
                        let cp = ints.value(i) as u32;
                        match char::from_u32(cp) {
                            Some(c) => out.push(Some(c.to_string())),
                            None => return exec_err!("chr: invalid codepoint {cp}"),
                        }
                    }
                }
            }
            _ => return exec_err!("chr: expected Int32 or Int64"),
        }
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

// ---------------------------------------------------------------------------
// ascii(text) → int
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsciiUdf {
    signature: Signature,
}

impl ScalarUDFImpl for AsciiUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ascii"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let out: Vec<Option<i32>> = strings
            .iter()
            .map(|opt| {
                opt.as_deref()
                    .map(|s| s.chars().next().map(|c| c as i32).unwrap_or(0))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(out))))
    }
}

// ---------------------------------------------------------------------------
// translate(string, from, to) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct TranslateUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TranslateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "translate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let froms = to_str_vec(args, 1, n)?;
        let tos = to_str_vec(args, 2, n)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            match (&strings[i], &froms[i], &tos[i]) {
                (None, _, _) | (_, None, _) | (_, _, None) => out.push(None),
                (Some(s), Some(from), Some(to)) => {
                    let from_chars: Vec<char> = from.chars().collect();
                    let to_chars: Vec<char> = to.chars().collect();
                    let result: String = s
                        .chars()
                        .filter_map(|c| {
                            if let Some(pos) = from_chars.iter().position(|&f| f == c) {
                                to_chars.get(pos).copied()
                                // If to is shorter than from, delete the char
                            } else {
                                Some(c)
                            }
                        })
                        .collect();
                    out.push(Some(result));
                }
            }
        }
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

// ---------------------------------------------------------------------------
// btrim / ltrim / rtrim with optional chars arg
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TrimMode {
    Both,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrimCharsUdf {
    name: String,
    signature: Signature,
    mode: TrimMode,
}

impl ScalarUDFImpl for TrimCharsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let chars_vec: Option<Vec<Option<String>>> = if args.len() > 1 {
            Some(to_str_vec(args, 1, n)?)
        } else {
            None
        };

        let mode = self.mode;
        let out: Vec<Option<String>> = strings
            .into_iter()
            .enumerate()
            .map(|(i, opt)| {
                opt.map(|s| {
                    let trim_chars: &str = chars_vec
                        .as_ref()
                        .and_then(|cv| cv[i].as_deref())
                        .unwrap_or(" ");
                    trim_string(&s, trim_chars, mode)
                })
            })
            .collect();
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

fn trim_string(s: &str, chars: &str, mode: TrimMode) -> String {
    let char_set: Vec<char> = chars.chars().collect();
    let predicate = |c: char| char_set.contains(&c);
    match mode {
        TrimMode::Both => s.trim_matches(predicate).to_string(),
        TrimMode::Left => s.trim_start_matches(predicate).to_string(),
        TrimMode::Right => s.trim_end_matches(predicate).to_string(),
    }
}

// ---------------------------------------------------------------------------
// convert_from(bytea, encoding) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConvertFromUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ConvertFromUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "convert_from"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let bytes_arr = args[0].clone().into_array(n)?;
        let bytes = bytes_arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("convert_from: arg 1 must be Binary".into())
            })?;

        let out: Vec<Option<String>> = (0..n)
            .map(|i| {
                if bytes.is_null(i) {
                    None
                } else {
                    Some(String::from_utf8_lossy(bytes.value(i)).into_owned())
                }
            })
            .collect();
        Ok(ColumnarValue::Array(str_vec_to_array(out)))
    }
}

// ---------------------------------------------------------------------------
// convert_to(text, encoding) → bytea
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ConvertToUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ConvertToUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "convert_to"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let strings = to_str_vec(args, 0, n)?;
        let out: Vec<Option<Vec<u8>>> = strings
            .into_iter()
            .map(|opt| opt.map(|s| s.into_bytes()))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
            out.iter().map(|o| o.as_deref()),
        ))))
    }
}

// ---------------------------------------------------------------------------
// isfinite(timestamp | timestamptz | date) → bool
// ---------------------------------------------------------------------------

/// `isfinite(x)` — false for the two infinity sentinels, true otherwise.
///
/// This was hardcoded `true`, and Basin genuinely materialises infinities:
/// `datetime_extras::rewrite_infinity_timestamp_casts` turns
/// `'infinity'::timestamp` into `cast_infinity_timestamp('infinity')`, whose
/// UDF stores `i64::MAX` µs (and `i64::MIN` for `-infinity`) — PostgreSQL's own
/// internal sentinels. So `SELECT isfinite('infinity'::timestamp)` came back
/// `t` where PostgreSQL 18.2 answers `f`, on a path Basin can actually reach.
///
/// The sentinel is the *extremal value for the column's own unit*, not a fixed
/// microsecond count: a `Timestamp(Nanosecond, _)` infinity is `i64::MAX`
/// nanoseconds. Comparing against `i64::MAX`/`i64::MIN` per array is therefore
/// unit-independent. No real timestamp lands on either bound — `i64::MAX` µs is
/// year 294247 and `i64::MAX` ns is year 2262, but Arrow cannot represent a
/// value one tick beyond them either, so nothing legitimate is being shadowed
/// that was not already saturating.
///
/// `Date32` is accepted by the signature but has no infinity encoding in Basin
/// (`cast_infinity_timestamp` only produces timestamps), so every date is
/// finite — which is also what PostgreSQL says for every date that is not
/// literally `'infinity'::date`, a spelling Basin does not currently
/// materialise at all.
///
/// NULL in, NULL out, as PostgreSQL: `isfinite(NULL::timestamp)` is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct IsFiniteUdf {
    signature: Signature,
}

impl ScalarUDFImpl for IsFiniteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isfinite"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("isfinite expects 1 argument, got {}", args.len());
        }
        let n = num_rows(args);
        let arr = args[0].clone().into_array(n)?;

        // Every accepted timestamp unit is a distinct Arrow array type over the
        // same i64 tick, so read the ticks out once and decide uniformly.
        let ticks: Option<Vec<i64>> = match arr.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .map(|a| a.values().to_vec()),
            DataType::Timestamp(TimeUnit::Millisecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.values().to_vec()),
            DataType::Timestamp(TimeUnit::Microsecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.values().to_vec()),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map(|a| a.values().to_vec()),
            _ => None,
        };

        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            match &ticks {
                Some(t) => out.append_value(t[i] != i64::MAX && t[i] != i64::MIN),
                // Date32 (and anything else the signature lets through):
                // no infinity representation, so always finite.
                None => out.append_value(true),
            }
        }
        let arr: ArrayRef = Arc::new(out.finish());
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// Process-global compiled-regex cache
// ---------------------------------------------------------------------------

/// Default capacity: 1 024 entries.  Override with `BASIN_ENGINE_REGEX_CACHE_CAP`.
const DEFAULT_REGEX_CACHE_CAP: usize = 1024;

fn regex_cache_cap() -> usize {
    std::env::var("BASIN_ENGINE_REGEX_CACHE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REGEX_CACHE_CAP)
}

type RegexCache = lru::LruCache<u64, Arc<regex::Regex>>;

static REGEX_CACHE: OnceLock<Mutex<RegexCache>> = OnceLock::new();

fn regex_cache() -> &'static Mutex<RegexCache> {
    REGEX_CACHE.get_or_init(|| {
        let cap = std::num::NonZeroUsize::new(regex_cache_cap())
            .unwrap_or(std::num::NonZeroUsize::new(DEFAULT_REGEX_CACHE_CAP).unwrap());
        Mutex::new(lru::LruCache::new(cap))
    })
}

/// Return a cached `Arc<Regex>` for `(pattern, flags)`, compiling on first use.
///
/// The cache key is the xxh3-64 hash of `pattern + '\0' + flags`; a collision
/// (astronomically unlikely) would simply yield a wrong but non-silent result
/// (the regex match would fail or succeed differently), so the tradeoff is fine.
/// `flags` must be the normalised flag string (e.g. `"i"`, `"gi"`, `""`).
pub(crate) fn get_or_compile_regex(pattern: &str, flags: &str) -> DFResult<Arc<regex::Regex>> {
    let key = xxhash_rust::xxh3::xxh3_64(format!("{pattern}\x00{flags}").as_bytes());
    let cache = regex_cache();
    {
        let mut guard = cache.lock().expect("regex cache mutex poisoned");
        if let Some(hit) = guard.get(&key) {
            return Ok(Arc::clone(hit));
        }
    }
    // Compile outside the lock so a slow compile doesn't block other threads.
    let re = Arc::new(compile_regex(pattern, flags)?);
    {
        let mut guard = cache.lock().expect("regex cache mutex poisoned");
        // Another thread may have inserted it while we compiled — that's fine;
        // we just overwrite with an equivalent compiled regex.
        guard.put(key, Arc::clone(&re));
    }
    Ok(re)
}

// ---------------------------------------------------------------------------
// Regex builder helper
// ---------------------------------------------------------------------------

/// Compile `pattern` with `flags` into a `regex::Regex`.  Not cached; callers
/// should prefer `get_or_compile_regex` to avoid per-row compilation cost.
fn compile_regex(pattern: &str, flags: &str) -> DFResult<regex::Regex> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for f in flags.chars() {
        match f {
            'i' => {
                builder.case_insensitive(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            'g' => {} // global handled by caller
            _ => {}   // unknown flags silently ignored
        }
    }
    builder.build().map_err(|e| {
        DataFusionError::Execution(format!("regexp: invalid pattern {pattern:?}: {e}"))
    })
}

fn build_regex(pattern: &str, flags: &str) -> DFResult<regex::Regex> {
    // Delegate to the cached path; clone the Arc out so callers that expect a
    // plain `Regex` by value still compile.  Hot path: lock + LRU probe is
    // O(1) and avoids the expensive RegexBuilder allocation entirely.
    get_or_compile_regex(pattern, flags).map(|arc| (*arc).clone())
}

// ---------------------------------------------------------------------------
// Unit tests for the regex cache
// ---------------------------------------------------------------------------

#[cfg(test)]
mod regex_cache_tests {
    use super::get_or_compile_regex;
    use std::sync::Arc;

    #[test]
    fn regex_cache_hit_returns_same_arc() {
        let a = get_or_compile_regex("[0-9]+", "").unwrap();
        let b = get_or_compile_regex("[0-9]+", "").unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "second lookup must return the cached Arc"
        );
    }

    #[test]
    fn regex_cache_distinct_patterns_distinct() {
        let a = get_or_compile_regex("[a-z]+", "").unwrap();
        let b = get_or_compile_regex("[A-Z]+", "").unwrap();
        assert!(
            !Arc::ptr_eq(&a, &b),
            "distinct patterns must produce distinct cache entries"
        );
    }

    #[test]
    fn regex_cache_case_sensitive_vs_insensitive_distinct() {
        let cs = get_or_compile_regex("hello", "").unwrap();
        let ci = get_or_compile_regex("hello", "i").unwrap();
        assert!(
            !Arc::ptr_eq(&cs, &ci),
            "same pattern with different flags must be distinct cache entries"
        );
        // Verify functional difference: case-insensitive should match "HELLO".
        assert!(!cs.is_match("HELLO"), "case-sensitive must not match HELLO");
        assert!(ci.is_match("HELLO"), "case-insensitive must match HELLO");
    }
}
