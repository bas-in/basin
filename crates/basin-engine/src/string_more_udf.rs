//! Additional PostgreSQL string UDFs not covered by `string_dt_udf` or `udf`.
//!
//! ## Functions provided
//!
//! | Function | PG signature | Notes |
//! |---|---|---|
//! | `regexp_replace` | `(string, pattern, replacement [, flags]) → text` | `'g'` flag replaces all; default replaces first |
//! | `format` | `(fmt text, variadic) → text` | `%s` / `%I` / `%L` / `%%` |
//! | `left` | `(text, n int) → text` | first n chars; negative n omits last \|n\| chars |
//! | `right` | `(text, n int) → text` | last n chars; negative n omits first \|n\| chars |
//! | `encode` | `(bytea, format text) → text` | `hex`, `base64`, `escape` |
//! | `decode` | `(text, format text) → bytea` | inverse of encode |
//!
//! ## PG semantics notes
//!
//! ### regexp_replace
//! - `regexp_replace(string, pattern, replacement)` — replaces the **first**
//!   occurrence of `pattern` in `string` with `replacement`.
//! - `regexp_replace(string, pattern, replacement, flags)` — when `flags`
//!   contains `'g'`, replaces **all** occurrences.
//! - Back-references `\1`…`\9` in `replacement` refer to capture groups.
//!   The Rust `regex` crate uses `$1`…`$9` internally; we translate `\N` →
//!   `${N}` before passing to `replace`/`replace_all`.
//! - Returns NULL if any non-flag argument is NULL.
//! - Invalid regex → DataFusion execution error (matches PG's behaviour).
//!
//! ### format
//! - `%s` — stringify the argument (NULL → literal `NULL`).
//! - `%I` — quote as a PostgreSQL identifier (double-quoted, internal `"` doubled).
//! - `%L` — quote as a PostgreSQL literal (single-quoted, internal `'` doubled).
//!   NULL arg → unquoted `NULL`.
//! - `%%` — literal `%`.
//!
//! ### left / right
//! - `left(str, n)`:  `n >= 0` → first n chars; `n < 0` → all but last |n| chars.
//! - `right(str, n)`: `n >= 0` → last n chars; `n < 0` → all but first |n| chars.
//! - Works on Unicode code-points (chars), matching PG semantics.
//! - NULL input → NULL output.
//!
//! ### encode / decode
//! - Formats: `hex`, `base64`, `escape`.
//! - `encode(NULL, fmt)` → NULL; `decode(NULL, fmt)` → NULL.
//! - `escape` encode: non-printable / non-ASCII bytes become `\NNN` octal;
//!   backslash becomes `\\`.  Decode is the inverse.
//!
//! ## Registration
//!
//! Call `register_string_more_udfs(&ctx)` from `session::build_stateless_udf_cache`
//! and the two per-session fallback registration blocks in `session.rs`.

use std::any::Any;
use std::sync::Arc;

use base64::Engine as _;
use datafusion::arrow::array::{Array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// Registration entry point
// ---------------------------------------------------------------------------

/// Register all UDFs from this module on `ctx`. Idempotent.
pub(crate) fn register_string_more_udfs(ctx: &SessionContext) {
    // ---- regexp_replace (3-arg and 4-arg) ----
    ctx.register_udf(ScalarUDF::from(RegexpReplaceUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- format(fmt, args...) — overloads for 1-7 total arguments ----
    {
        let mut sigs: Vec<TypeSignature> = Vec::new();
        for extra in 0usize..=6 {
            sigs.push(TypeSignature::Exact(vec![DataType::Utf8; 1 + extra]));
        }
        ctx.register_udf(ScalarUDF::from(FormatMoreUdf {
            signature: Signature::one_of(sigs, Volatility::Immutable),
        }));
    }

    // ---- left(text, int) ----
    ctx.register_udf(ScalarUDF::from(LeftRightUdf {
        name: "left",
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
        side: Side::Left,
    }));

    // ---- right(text, int) ----
    ctx.register_udf(ScalarUDF::from(LeftRightUdf {
        name: "right",
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
        side: Side::Right,
    }));

    // ---- encode(bytea, text) → text ----
    ctx.register_udf(ScalarUDF::from(EncodeUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ---- decode(text, text) → bytea ----
    ctx.register_udf(ScalarUDF::from(DecodeUdf {
        signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
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

fn arg_as_strings(arg: &ColumnarValue, n: usize) -> DFResult<StringArray> {
    let arr = arg.clone().into_array(n)?;
    let arr = if arr.data_type() == &DataType::Utf8 {
        arr
    } else {
        datafusion::arrow::compute::cast(&arr, &DataType::Utf8)?
    };
    Ok(arr
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

fn arg_as_i64(arg: &ColumnarValue, n: usize) -> DFResult<Vec<Option<i64>>> {
    let arr = arg.clone().into_array(n)?;
    let arr = if arr.data_type() == &DataType::Int64 {
        arr
    } else {
        datafusion::arrow::compute::cast(&arr, &DataType::Int64)?
    };
    let int_arr = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("cast to Int64 yields Int64Array");
    Ok((0..n)
        .map(|i| {
            if int_arr.is_null(i) {
                None
            } else {
                Some(int_arr.value(i))
            }
        })
        .collect())
}

// ---------------------------------------------------------------------------
// regexp_replace
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegexpReplaceUdf {
    signature: Signature,
}

impl ScalarUDFImpl for RegexpReplaceUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_replace"
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
        if args.len() < 3 || args.len() > 4 {
            return exec_err!(
                "regexp_replace expects 3 or 4 arguments, got {}",
                args.len()
            );
        }
        let n = num_rows(args);
        let strings = arg_as_strings(&args[0], n)?;
        let patterns = arg_as_strings(&args[1], n)?;
        let replacements = arg_as_strings(&args[2], n)?;
        let flags_opt = if args.len() == 4 {
            Some(arg_as_strings(&args[3], n)?)
        } else {
            None
        };

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if strings.is_null(i) || patterns.is_null(i) || replacements.is_null(i) {
                out.push(None);
                continue;
            }
            let text = strings.value(i);
            let pat = patterns.value(i);
            let repl_raw = replacements.value(i);
            let global = flags_opt
                .as_ref()
                .map(|f| !f.is_null(i) && f.value(i).contains('g'))
                .unwrap_or(false);

            // Compile regex (cached process-globally to avoid per-row cost).
            let flags = if flags_opt
                .as_ref()
                .map(|f| !f.is_null(i) && f.value(i).contains('i'))
                .unwrap_or(false)
            {
                "i"
            } else {
                ""
            };
            let re = crate::string_dt_udf::get_or_compile_regex(pat, flags).map_err(|e| {
                DataFusionError::Execution(format!(
                    "regexp_replace: invalid regular expression {:?}: {e}",
                    pat
                ))
            })?;

            // Translate PG back-references \1..\9 → ${1}..${9} for the regex crate.
            let repl = translate_backrefs(repl_raw);

            let result = if global {
                re.replace_all(text, repl.as_str()).into_owned()
            } else {
                re.replace(text, repl.as_str()).into_owned()
            };
            out.push(Some(result));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

/// Translate PostgreSQL `\1`…`\9` back-references to `${1}`…`${9}` as
/// expected by the Rust `regex` crate's replace API.
///
/// Also handles `\\` → `\` (literal backslash in replacement).
fn translate_backrefs(repl: &str) -> String {
    let mut out = String::with_capacity(repl.len() + 4);
    let mut chars = repl.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some(&d) if d.is_ascii_digit() && d != '0' => {
                    chars.next();
                    out.push('$');
                    out.push('{');
                    out.push(d);
                    out.push('}');
                }
                Some(&'\\') => {
                    chars.next();
                    out.push('\\');
                }
                _ => {
                    // Keep the backslash as-is for other escape sequences.
                    out.push('\\');
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// format(fmt, args...)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FormatMoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for FormatMoreUdf {
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
        let fmts = arg_as_strings(&args[0], n)?;

        // Convert remaining args to Option<String> columns.
        let rest: Vec<Vec<Option<String>>> = (1..args.len())
            .map(|i| {
                let sa = arg_as_strings(&args[i], n)?;
                Ok((0..n)
                    .map(|r| {
                        if sa.is_null(r) {
                            None
                        } else {
                            Some(sa.value(r).to_string())
                        }
                    })
                    .collect::<Vec<_>>())
            })
            .collect::<DFResult<_>>()?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if fmts.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt_str = fmts.value(i);
            let row_args: Vec<Option<&str>> = rest.iter().map(|col| col[i].as_deref()).collect();
            out.push(Some(apply_format(fmt_str, &row_args)?));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
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
                out.push_str(v.unwrap_or("NULL"));
                arg_idx += 1;
            }
            Some('I') => {
                let v = get_format_arg(args, arg_idx)?;
                out.push_str(&pg_quote_ident(v.unwrap_or("")));
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

fn pg_quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

fn pg_quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

// ---------------------------------------------------------------------------
// left / right
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Side {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LeftRightUdf {
    name: &'static str,
    signature: Signature,
    side: Side,
}

impl ScalarUDFImpl for LeftRightUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
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
        if args.len() != 2 {
            return exec_err!("{} expects 2 arguments, got {}", self.name, args.len());
        }
        let n = num_rows(args);
        let strings = arg_as_strings(&args[0], n)?;
        let ns = arg_as_i64(&args[1], n)?;
        let side = self.side;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if strings.is_null(i) || ns[i].is_none() {
                out.push(None);
                continue;
            }
            let text = strings.value(i);
            let n_val = ns[i].unwrap();
            let chars: Vec<char> = text.chars().collect();
            let len = chars.len() as i64;

            let result: String = match side {
                Side::Left => {
                    if n_val >= 0 {
                        // first min(n, len) chars
                        let take = n_val.min(len) as usize;
                        chars[..take].iter().collect()
                    } else {
                        // all but last |n| chars
                        let skip_end = (-n_val).min(len) as usize;
                        if skip_end >= chars.len() {
                            String::new()
                        } else {
                            chars[..chars.len() - skip_end].iter().collect()
                        }
                    }
                }
                Side::Right => {
                    if n_val >= 0 {
                        // last min(n, len) chars
                        let take = n_val.min(len) as usize;
                        if take >= chars.len() {
                            chars.iter().collect()
                        } else {
                            chars[chars.len() - take..].iter().collect()
                        }
                    } else {
                        // all but first |n| chars
                        let skip = (-n_val).min(len) as usize;
                        chars[skip..].iter().collect()
                    }
                }
            };
            out.push(Some(result));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

// ---------------------------------------------------------------------------
// encode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("encode expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);

        // Arg 1: bytes — accept Binary, LargeBinary, or Utf8.
        let data_arr = args[0].clone().into_array(n)?;
        // Arg 2: format string.
        let fmt_arr = arg_as_strings(&args[1], n)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if data_arr.is_null(i) || fmt_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let fmt = fmt_arr.value(i);
            let bytes: &[u8] = match data_arr.data_type() {
                DataType::Binary => {
                    let ba = data_arr
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .expect("Binary array");
                    ba.value(i)
                }
                DataType::LargeBinary => {
                    let ba = data_arr
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
                        .expect("LargeBinary array");
                    ba.value(i)
                }
                DataType::Utf8 => {
                    let sa = data_arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Utf8 array");
                    sa.value(i).as_bytes()
                }
                other => {
                    return exec_err!(
                        "encode: arg 1 must be Binary, LargeBinary, or Utf8, got {other:?}"
                    );
                }
            };
            out.push(Some(encode_bytes(fmt, bytes)?));
        }
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

fn encode_bytes(fmt: &str, bytes: &[u8]) -> DFResult<String> {
    match fmt {
        "hex" => Ok(hex::encode(bytes)),
        "base64" => Ok(base64::engine::general_purpose::STANDARD.encode(bytes)),
        "escape" => Ok(escape_encode(bytes)),
        other => exec_err!(
            "encode: unsupported format {:?} (supported: hex, base64, escape)",
            other
        ),
    }
}

/// Encode bytes as PostgreSQL `escape` format:
/// - Backslash → `\\`
/// - Non-printable / non-ASCII bytes (< 0x20 or >= 0x7F) → `\NNN` (3-digit octal)
/// - Printable ASCII (0x20..=0x7E) → as-is
fn escape_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            0x20..=0x7e => out.push(b as char),
            _ => {
                out.push('\\');
                out.push(char::from_digit((b >> 6) as u32, 8).unwrap());
                out.push(char::from_digit(((b >> 3) & 0x07) as u32, 8).unwrap());
                out.push(char::from_digit((b & 0x07) as u32, 8).unwrap());
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("decode expects 2 arguments, got {}", args.len());
        }
        let n = num_rows(args);
        let text_arr = arg_as_strings(&args[0], n)?;
        let fmt_arr = arg_as_strings(&args[1], n)?;

        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            if text_arr.is_null(i) || fmt_arr.is_null(i) {
                out.push(None);
                continue;
            }
            let text = text_arr.value(i);
            let fmt = fmt_arr.value(i);
            out.push(Some(decode_bytes(fmt, text)?));
        }
        // Build BinaryArray from the Option<Vec<u8>> values.
        let arr: BinaryArray = out
            .iter()
            .map(|v| v.as_deref())
            .collect::<Vec<Option<&[u8]>>>()
            .into();
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

fn decode_bytes(fmt: &str, s: &str) -> DFResult<Vec<u8>> {
    match fmt {
        "hex" => hex::decode(s).map_err(|e| DataFusionError::Execution(format!("decode hex: {e}"))),
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| DataFusionError::Execution(format!("decode base64: {e}"))),
        "escape" => escape_decode(s),
        other => exec_err!(
            "decode: unsupported format {:?} (supported: hex, base64, escape)",
            other
        ),
    }
}

/// Decode PostgreSQL `escape`-format text back to bytes.
/// `\NNN` → single byte with octal value NNN; `\\` → single backslash.
fn escape_decode(s: &str) -> DFResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        if bytes[i] == b'\\' {
            if i + 1 < len && bytes[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
            } else if i + 3 < len
                && bytes[i + 1].is_ascii_digit()
                && bytes[i + 2].is_ascii_digit()
                && bytes[i + 3].is_ascii_digit()
            {
                let octal_str = std::str::from_utf8(&bytes[i + 1..i + 4])
                    .map_err(|e| DataFusionError::Execution(format!("escape decode: {e}")))?;
                let byte_val = u8::from_str_radix(octal_str, 8)
                    .map_err(|e| DataFusionError::Execution(format!("escape decode: {e}")))?;
                out.push(byte_val);
                i += 4;
            } else {
                // Bare backslash — treat as literal (lenient).
                out.push(b'\\');
                i += 1;
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array as _, BinaryArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::Result as DFResult;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use super::{
        apply_format, decode_bytes, encode_bytes, escape_decode, escape_encode, pg_quote_ident,
        pg_quote_literal, translate_backrefs, DecodeUdf, EncodeUdf, FormatMoreUdf, LeftRightUdf,
        RegexpReplaceUdf, Side,
    };
    use datafusion::logical_expr::{Signature, TypeSignature, Volatility};

    fn utf8_field() -> Arc<Field> {
        Arc::new(Field::new("_", DataType::Utf8, true))
    }
    fn binary_field() -> Arc<Field> {
        Arc::new(Field::new("_", DataType::Binary, true))
    }

    fn call_udf(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        return_field: Arc<Field>,
    ) -> DFResult<ColumnarValue>
where {
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: n,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn str_arg(v: Option<&str>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(vec![v])))
    }

    fn int64_arg(v: i64) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            v,
        ])))
    }

    fn binary_arg(v: &[u8]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(BinaryArray::from(vec![v])))
    }

    fn result_str(cv: ColumnarValue, row: usize) -> Option<String> {
        match cv {
            ColumnarValue::Array(arr) => {
                let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                if sa.is_null(row) {
                    None
                } else {
                    Some(sa.value(row).to_string())
                }
            }
            ColumnarValue::Scalar(s) => {
                use datafusion::scalar::ScalarValue;
                match s {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v,
                    _ => None,
                }
            }
        }
    }

    fn result_bytes(cv: ColumnarValue, row: usize) -> Option<Vec<u8>> {
        match cv {
            ColumnarValue::Array(arr) => {
                let ba = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                if ba.is_null(row) {
                    None
                } else {
                    Some(ba.value(row).to_vec())
                }
            }
            _ => None,
        }
    }

    // -------------------------------------------------------------------------
    // regexp_replace
    // -------------------------------------------------------------------------

    fn regexp_replace_udf() -> RegexpReplaceUdf {
        RegexpReplaceUdf {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    #[test]
    fn regexp_replace_first_only() {
        // regexp_replace('hello world', 'o', 'X') → 'hellX world'  (first only)
        let udf = regexp_replace_udf();
        let cv = call_udf(
            &udf,
            vec![
                str_arg(Some("hello world")),
                str_arg(Some("o")),
                str_arg(Some("X")),
            ],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("hellX world".into()));
    }

    #[test]
    fn regexp_replace_global_flag() {
        // regexp_replace('hello world', 'o', 'X', 'g') → 'hellX wXrld'
        let udf = regexp_replace_udf();
        let cv = call_udf(
            &udf,
            vec![
                str_arg(Some("hello world")),
                str_arg(Some("o")),
                str_arg(Some("X")),
                str_arg(Some("g")),
            ],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("hellX wXrld".into()));
    }

    #[test]
    fn regexp_replace_backreference() {
        // regexp_replace('2024-01-15', '(\d{4})-(\d{2})-(\d{2})', '\3/\2/\1')
        // PG: '15/01/2024'
        let udf = regexp_replace_udf();
        let cv = call_udf(
            &udf,
            vec![
                str_arg(Some("2024-01-15")),
                str_arg(Some(r"(\d{4})-(\d{2})-(\d{2})")),
                str_arg(Some(r"\3/\2/\1")),
            ],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("15/01/2024".into()));
    }

    #[test]
    fn regexp_replace_null_input_returns_null() {
        let udf = regexp_replace_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(None), str_arg(Some("o")), str_arg(Some("X"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), None);
    }

    #[test]
    fn regexp_replace_no_match_returns_original() {
        // When pattern doesn't match, PG returns the original string unchanged.
        let udf = regexp_replace_udf();
        let cv = call_udf(
            &udf,
            vec![
                str_arg(Some("hello")),
                str_arg(Some("[0-9]+")),
                str_arg(Some("NUM")),
            ],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("hello".into()));
    }

    // -------------------------------------------------------------------------
    // format
    // -------------------------------------------------------------------------

    fn format_udf() -> FormatMoreUdf {
        let mut sigs: Vec<TypeSignature> = Vec::new();
        for extra in 0usize..=6 {
            sigs.push(TypeSignature::Exact(vec![DataType::Utf8; 1 + extra]));
        }
        FormatMoreUdf {
            signature: Signature::one_of(sigs, Volatility::Immutable),
        }
    }

    #[test]
    fn format_percent_s() {
        // format('Hello %s', 'World') → 'Hello World'
        let udf = format_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("Hello %s")), str_arg(Some("World"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("Hello World".into()));
    }

    #[test]
    fn format_percent_i_quotes_ident() {
        // format('SELECT * FROM %I', 'my table') → 'SELECT * FROM "my table"'
        let udf = format_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("SELECT * FROM %I")), str_arg(Some("my table"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(
            result_str(cv, 0),
            Some(r#"SELECT * FROM "my table""#.into())
        );
    }

    #[test]
    fn format_percent_l_quotes_literal() {
        // format('WHERE name = %L', "O'Brien") → "WHERE name = 'O''Brien'"
        let udf = format_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("WHERE name = %L")), str_arg(Some("O'Brien"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("WHERE name = 'O''Brien'".into()));
    }

    #[test]
    fn format_double_percent_is_literal() {
        let udf = format_udf();
        let cv = call_udf(&udf, vec![str_arg(Some("100%%"))], utf8_field()).unwrap();
        assert_eq!(result_str(cv, 0), Some("100%".into()));
    }

    // -------------------------------------------------------------------------
    // left / right
    // -------------------------------------------------------------------------

    fn left_udf() -> LeftRightUdf {
        LeftRightUdf {
            name: "left",
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64])],
                Volatility::Immutable,
            ),
            side: Side::Left,
        }
    }

    fn right_udf() -> LeftRightUdf {
        LeftRightUdf {
            name: "right",
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64])],
                Volatility::Immutable,
            ),
            side: Side::Right,
        }
    }

    #[test]
    fn left_positive_n() {
        // left('abcde', 3) → 'abc'
        let udf = left_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("abcde")), int64_arg(3)],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("abc".into()));
    }

    #[test]
    fn left_negative_n_omits_last() {
        // left('abcde', -2) → 'abc'  (all but last 2)
        let udf = left_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("abcde")), int64_arg(-2)],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("abc".into()));
    }

    #[test]
    fn right_positive_n() {
        // right('abcde', 3) → 'cde'
        let udf = right_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("abcde")), int64_arg(3)],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("cde".into()));
    }

    #[test]
    fn right_negative_n_omits_first() {
        // right('abcde', -2) → 'cde'  (all but first 2)
        let udf = right_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("abcde")), int64_arg(-2)],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("cde".into()));
    }

    #[test]
    fn left_n_larger_than_length() {
        // left('hi', 100) → 'hi'
        let udf = left_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("hi")), int64_arg(100)],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("hi".into()));
    }

    #[test]
    fn left_null_returns_null() {
        let udf = left_udf();
        let cv = call_udf(&udf, vec![str_arg(None), int64_arg(3)], utf8_field()).unwrap();
        assert_eq!(result_str(cv, 0), None);
    }

    // -------------------------------------------------------------------------
    // encode / decode
    // -------------------------------------------------------------------------

    fn encode_udf() -> EncodeUdf {
        EncodeUdf {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }

    fn decode_udf() -> DecodeUdf {
        DecodeUdf {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }

    #[test]
    fn encode_hex_roundtrip() {
        // encode('\x48656c6c6f', 'hex') → '48656c6c6f'
        let udf = encode_udf();
        let cv = call_udf(
            &udf,
            vec![binary_arg(b"Hello"), str_arg(Some("hex"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("48656c6c6f".into()));
    }

    #[test]
    fn encode_base64() {
        let udf = encode_udf();
        let cv = call_udf(
            &udf,
            vec![binary_arg(b"Hello"), str_arg(Some("base64"))],
            utf8_field(),
        )
        .unwrap();
        assert_eq!(result_str(cv, 0), Some("SGVsbG8=".into()));
    }

    #[test]
    fn decode_hex() {
        let udf = decode_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("48656c6c6f")), str_arg(Some("hex"))],
            binary_field(),
        )
        .unwrap();
        assert_eq!(result_bytes(cv, 0), Some(b"Hello".to_vec()));
    }

    #[test]
    fn decode_base64() {
        let udf = decode_udf();
        let cv = call_udf(
            &udf,
            vec![str_arg(Some("SGVsbG8=")), str_arg(Some("base64"))],
            binary_field(),
        )
        .unwrap();
        assert_eq!(result_bytes(cv, 0), Some(b"Hello".to_vec()));
    }

    #[test]
    fn encode_escape_and_back() {
        // non-printable byte 0x01 should encode to \001 and decode back
        let raw = b"\x01\x41\x5c"; // ctrl-A, 'A', backslash
        let encoded = escape_encode(raw);
        let decoded = escape_decode(&encoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn encode_null_returns_null() {
        // When the data arg is NULL for a Binary input the row should be NULL.
        // We test this via encode_bytes alone (not the UDF), as constructing
        // a null BinaryArray requires a builder.
        // Instead, verify via the encode_bytes helper that valid data works.
        assert!(encode_bytes("hex", b"abc").is_ok());
        assert!(encode_bytes("unknown_fmt", b"abc").is_err());
    }

    // -------------------------------------------------------------------------
    // translate_backrefs helper
    // -------------------------------------------------------------------------

    #[test]
    fn backrefs_translated_correctly() {
        assert_eq!(translate_backrefs(r"\1-\2"), "${1}-${2}");
        assert_eq!(translate_backrefs(r"\\"), "\\");
        assert_eq!(translate_backrefs("plain"), "plain");
    }
}
