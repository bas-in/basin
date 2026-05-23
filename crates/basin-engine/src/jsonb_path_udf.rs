//! PG 12+ JSONPath UDFs for Basin's pg-compat surface.
//!
//! Implements the PostgreSQL JSONPath query family:
//! - `jsonb_path_query(target jsonb, path text)` — true SRF (UDTF) returning
//!   one row per match.
//! - `jsonb_path_query_array(target jsonb, path text) -> jsonb` — all matches
//!   wrapped in a JSON array.
//! - `jsonb_path_query_first(target jsonb, path text) -> jsonb` — first match
//!   only, or NULL.
//! - `@?` operator (jsonb @? path_text) → bool — path matches anything?
//!   Rewritten to `jsonb_path_exists(target, path)` (already in jsonb_udf.rs).
//! - `@@` operator (jsonb @@ predicate_text) → bool — predicate evaluates true?
//!   Rewritten to `jsonb_path_match(target, predicate)` by
//!   `rewrite_jsonpath_operators`.
//!
//! # JSONPath subset supported
//!
//! | Syntax                      | Description                                   |
//! | --------------------------- | --------------------------------------------- |
//! | `$`                         | Root element                                  |
//! | `$.key`                     | Object key access                             |
//! | `$.key[0]`                  | Array index (0-based)                         |
//! | `$.key[*]`                  | All array elements                            |
//! | `$.key.*`                   | All object values                             |
//! | `$..key`                    | Recursive descent for key                     |
//! | `$.items[?(@.x > 1)]`       | Filter: numeric comparison                    |
//! | `$.items[?(@.x == "str")]`  | Filter: string equality                       |
//! | `$.items[?(@.x)]`           | Filter: existence check                       |
//! | `$.x > 3` (predicate form)  | Top-level predicate for `@@` / `jsonb_path_match` |
//!
//! # NOT supported (honest rejection)
//!
//! - Lax / strict mode keywords (stripped silently)
//! - `.keyvalue()` method
//! - `.type()`, `.size()` methods
//! - `.double()`, `.ceiling()`, `.floor()`, `.abs()` arithmetic methods
//! - `datetime()` method
//! - Named variables (`$var`)
//! - Negative array indices (`$.a[-1]`)
//! - `to`, `last` inside subscripts
//! - Path arithmetic (`$.a + 1`)
//!
//! Unknown method calls (`.foo()`) produce a plan-time error so the caller
//! knows exactly what's missing rather than silently returning wrong results.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, LargeBinaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err, DataFusionError, Result as DFResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableType,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all JSONPath UDFs (stateless scalars) on `ctx`. Idempotent.
///
/// Note: `jsonb_path_query` UDTF is registered separately by
/// `register_jsonb_path_udtfs` which must be called on every per-session
/// `SessionContext` (UDTFs are not captured by `StatelessUdfCache`).
pub(crate) fn register_jsonb_path_udfs(ctx: &SessionContext) {
    // jsonb_path_query_first(target jsonb, path text) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbPathQueryFirstUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_query_array(target jsonb, path text) -> jsonb
    ctx.register_udf(ScalarUDF::from(JsonbPathQueryArrayUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // jsonb_path_match(target jsonb, predicate text) -> bool
    // Implements the `@@` operator for predicate evaluation.
    ctx.register_udf(ScalarUDF::from(JsonbPathMatchUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
    }));
}

/// Register the `jsonb_path_query` UDTF (table-valued function / true SRF).
/// Must be called per-session context (not cached in StatelessUdfCache).
pub(crate) fn register_jsonb_path_udtfs(ctx: &SessionContext) {
    ctx.register_udtf("jsonb_path_query", Arc::new(JsonbPathQueryTf {}));
}

// ---------------------------------------------------------------------------
// SQL rewrite: @@ operator
// ---------------------------------------------------------------------------

/// Rewrite `expr @@ 'predicate'` to `jsonb_path_match(expr, 'predicate')`.
///
/// The `@?` operator is already handled in `udf::rewrite_json_operators` (it
/// maps to `jsonb_path_exists`).  This function handles `@@` → `jsonb_path_match`.
///
/// Strategy: scan the SQL text token-by-token.  When we encounter the token
/// `@@`, replace `<lhs> @@ <rhs>` with `jsonb_path_match(<lhs>, <rhs>)`.
/// We use the same lightweight state-machine approach as
/// `range_udf::rewrite_range_operators`.
pub(crate) fn rewrite_jsonpath_operators(sql: &str) -> String {
    if !sql.contains("@@") {
        return sql.to_string();
    }
    rewrite_binary_op_to_fn(sql, "@@", "jsonb_path_match")
}

/// Minimal textual binary-operator rewriter: `<lhs> OP <rhs>` →
/// `fn(<lhs>, <rhs>)`.
///
/// Works at the token level, not the AST level.  Handles:
/// - Single-quoted string literals (including escaped quotes).
/// - Parenthesised sub-expressions as LHS or RHS.
/// - Quoted identifiers.
/// - Operator may not appear inside string literals or comments.
///
/// Limitations (acceptable for the `@@` use-case):
/// - Does not handle `/* block comments */`.
/// - LHS extraction is greedy from the nearest preceding comma/paren/keyword.
fn rewrite_binary_op_to_fn(sql: &str, op: &str, func: &str) -> String {
    // We process the string by finding occurrences of ` op ` (with surrounding
    // token boundaries) and reconstructing the expression.
    // For each occurrence we extract the LHS (backwards) and RHS (forwards).

    let bytes = sql.as_bytes();
    let op_bytes = op.as_bytes();
    let n = bytes.len();
    let op_len = op_bytes.len();

    if n < op_len {
        return sql.to_string();
    }

    let mut result = String::with_capacity(n + 64);
    let mut i = 0usize;

    while i < n {
        // Skip string literals
        if bytes[i] == b'\'' {
            result.push('\'');
            i += 1;
            while i < n {
                if bytes[i] == b'\'' {
                    result.push('\'');
                    i += 1;
                    if i < n && bytes[i] == b'\'' {
                        // Escaped single quote ''
                        result.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    result.push(bytes[i] as char);
                    i += 1;
                }
            }
            continue;
        }

        // Skip double-quoted identifiers
        if bytes[i] == b'"' {
            result.push('"');
            i += 1;
            while i < n && bytes[i] != b'"' {
                result.push(bytes[i] as char);
                i += 1;
            }
            if i < n {
                result.push('"');
                i += 1;
            }
            continue;
        }

        // Check for operator match (requires whitespace/boundary around it)
        if i + op_len <= n && &bytes[i..i + op_len] == op_bytes {
            // Verify it's not part of a longer operator token by checking
            // the character before and after.
            let before_ok = i == 0
                || bytes[i - 1].is_ascii_whitespace()
                || bytes[i - 1] == b')'
                || bytes[i - 1] == b'\''
                || bytes[i - 1] == b'"';
            let after_ok = i + op_len >= n
                || bytes[i + op_len].is_ascii_whitespace()
                || bytes[i + op_len] == b'\''
                || bytes[i + op_len] == b'('
                || bytes[i + op_len] == b'"';

            if before_ok && after_ok {
                // Extract LHS from the result buffer (everything up to the last
                // token boundary: comma, opening paren, keyword like AND/OR/WHERE/SET).
                // Clone to release the immutable borrow before mutating result.
                let lhs = extract_lhs_from_result(&result).to_string();
                // Truncate result to remove the lhs we just extracted.
                let trim_len = result.len() - lhs.len();
                result.truncate(trim_len);

                // Skip the operator (and surrounding whitespace).
                i += op_len;
                while i < n && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }

                // Extract RHS: scan forward for a complete token.
                let (rhs, consumed) = extract_rhs(&bytes[i..]);
                i += consumed;

                // Emit: func(lhs, rhs)
                result.push_str(func);
                result.push('(');
                result.push_str(lhs.trim());
                result.push_str(", ");
                result.push_str(rhs.trim());
                result.push(')');
                continue;
            }
        }

        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

/// Extract the LHS of a binary operator from the already-emitted result buffer.
/// Walks backwards to find the start of the operand, stopping at:
/// - unmatched `(` — take everything after it
/// - `,` — take everything after it
/// - SQL keywords: `AND`, `OR`, `NOT`, `WHERE`, `SET`, `ON`, `CASE`, `WHEN`, `THEN`, `ELSE`
/// - start of string
fn extract_lhs_from_result(result: &str) -> &str {
    let trimmed = result.trim_end();
    if trimmed.is_empty() {
        return "";
    }

    // Walk backwards character by character.
    let bytes = trimmed.as_bytes();
    let mut depth = 0i32; // paren depth relative to end
    let mut i = bytes.len();

    while i > 0 {
        i -= 1;
        match bytes[i] {
            b')' => depth += 1,
            b'(' => {
                if depth == 0 {
                    // The LHS starts after this open paren.
                    return &trimmed[i + 1..];
                }
                depth -= 1;
            }
            b',' if depth == 0 => {
                return &trimmed[i + 1..];
            }
            b'\'' => {
                // Walk back through a string literal.
                if depth == 0 && i > 0 {
                    i -= 1;
                    while i > 0 {
                        if bytes[i] == b'\'' {
                            // Potentially the opening quote
                            break;
                        }
                        i -= 1;
                    }
                }
            }
            _ => {
                // Check for keyword boundary (walking backwards in ASCII).
                if depth == 0 && bytes[i].is_ascii_whitespace() {
                    // Peek at the word just before this whitespace.
                    let word_end = i + 1;
                    // Skip trailing whitespace
                    let mut j = i;
                    while j > 0 && bytes[j - 1].is_ascii_whitespace() {
                        j -= 1;
                    }
                    // Find word start
                    let mut wstart = j;
                    while wstart > 0 && bytes[wstart - 1].is_ascii_alphanumeric() {
                        wstart -= 1;
                    }
                    let word = std::str::from_utf8(&bytes[wstart..j]).unwrap_or("");
                    let upper = word.to_ascii_uppercase();
                    if matches!(
                        upper.as_str(),
                        "AND"
                            | "OR"
                            | "NOT"
                            | "WHERE"
                            | "SET"
                            | "ON"
                            | "CASE"
                            | "WHEN"
                            | "THEN"
                            | "ELSE"
                            | "SELECT"
                            | "HAVING"
                    ) {
                        return &trimmed[word_end..];
                    }
                }
            }
        }
    }

    trimmed
}

/// Extract a complete RHS token (single-quoted string, parenthesised expr,
/// or bare identifier/number) from the beginning of `remaining`.
/// Returns `(token_str, bytes_consumed)`.
fn extract_rhs(remaining: &[u8]) -> (String, usize) {
    if remaining.is_empty() {
        return (String::new(), 0);
    }

    let mut i = 0usize;
    let n = remaining.len();

    // Skip leading whitespace.
    while i < n && remaining[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;

    if i >= n {
        return (String::new(), 0);
    }

    match remaining[i] {
        b'\'' => {
            // Single-quoted string literal.
            i += 1;
            while i < n {
                if remaining[i] == b'\'' {
                    i += 1;
                    if i < n && remaining[i] == b'\'' {
                        i += 1; // escaped ''
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        }
        b'(' => {
            // Parenthesised expression.
            let mut depth = 0i32;
            while i < n {
                match remaining[i] {
                    b'(' => {
                        depth += 1;
                        i += 1;
                    }
                    b')' => {
                        depth -= 1;
                        i += 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    b'\'' => {
                        i += 1;
                        while i < n {
                            if remaining[i] == b'\'' {
                                i += 1;
                                if i < n && remaining[i] == b'\'' {
                                    i += 1;
                                } else {
                                    break;
                                }
                            } else {
                                i += 1;
                            }
                        }
                    }
                    _ => i += 1,
                }
            }
        }
        _ => {
            // Bare token: read until whitespace, comma, or operator.
            while i < n {
                match remaining[i] {
                    b' ' | b'\t' | b'\n' | b'\r' | b',' | b')' => break,
                    _ => i += 1,
                }
            }
        }
    }

    let token = std::str::from_utf8(&remaining[start..i])
        .unwrap_or("")
        .to_string();
    (token, i)
}

// ---------------------------------------------------------------------------
// JSONPath parser and evaluator
// ---------------------------------------------------------------------------

/// A single step in a parsed JSONPath expression.
#[derive(Debug, Clone, PartialEq)]
enum PathStep {
    /// `.key` — descend into an object by key name.
    Key(String),
    /// `[n]` — index into an array (0-based).
    Index(usize),
    /// `[*]` — all array elements.
    ArrayWildcard,
    /// `.*` — all object values.
    ObjectWildcard,
    /// `..key` — recursive descent, collect all nested values for a key.
    RecursiveKey(String),
    /// `[?(@.key op value)]` — filter predicate on array elements.
    Filter(FilterPredicate),
}

/// A filter predicate inside `[?(…)]`.
#[derive(Debug, Clone, PartialEq)]
enum FilterPredicate {
    /// `@.key` — existence check (truthy if key exists).
    Exists(Vec<String>),
    /// `@.key OP literal` — comparison.
    Compare {
        path: Vec<String>,
        op: CompareOp,
        value: FilterValue,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterValue {
    Num(f64),
    Str(String),
    Bool(bool),
    Null,
}

/// Top-level predicate form used by `@@` / `jsonb_path_match`.
/// e.g. `$.x > 3` evaluated against the root document.
#[derive(Debug, Clone, PartialEq)]
struct TopLevelPredicate {
    path: Vec<String>,
    op: CompareOp,
    value: FilterValue,
}

/// Parse a JSONPath string into a list of `PathStep`s.
///
/// Returns `Err` if the path contains an unsupported construct.
fn parse_jsonpath(path: &str) -> DFResult<Vec<PathStep>> {
    let path = path.trim();
    // Strip optional lax/strict prefix (we treat both as lax).
    let path = path.strip_prefix("strict").map(str::trim).unwrap_or(path);
    let path = path.strip_prefix("lax").map(str::trim).unwrap_or(path);

    // Must start with `$`.
    if !path.starts_with('$') {
        return plan_err!("JSONPath must start with '$', got: {path}");
    }
    let rest = &path[1..]; // drop '$'

    parse_path_rest(rest)
}

/// Parse everything after the leading `$`.
fn parse_path_rest(mut s: &str) -> DFResult<Vec<PathStep>> {
    let mut steps: Vec<PathStep> = Vec::new();

    loop {
        s = s.trim_start();
        if s.is_empty() {
            break;
        }

        match s.as_bytes()[0] {
            b'.' => {
                s = &s[1..]; // consume '.'
                             // Recursive descent `..`
                if s.starts_with('.') {
                    s = &s[1..]; // consume second '.'
                    let (key, rest) = read_key(s);
                    if key.is_empty() {
                        return plan_err!("Expected key after '..' in JSONPath");
                    }
                    steps.push(PathStep::RecursiveKey(key));
                    s = rest;
                    continue;
                }
                // `.*` — all object values
                if s.starts_with('*') {
                    s = &s[1..];
                    steps.push(PathStep::ObjectWildcard);
                    continue;
                }
                // Unsupported method calls: `.foo()`
                let (key, rest) = read_key(s);
                // Check if followed by '(' — method call
                let trimmed_rest = rest.trim_start();
                if trimmed_rest.starts_with('(') {
                    return plan_err!(
                        "JSONPath method calls are not supported: .{key}(). \
                        Unsupported: .type(), .size(), .double(), .ceiling(), .floor(), \
                        .abs(), .datetime(), .keyvalue()"
                    );
                }
                if key.is_empty() {
                    return plan_err!("Expected key after '.' in JSONPath at: {s}");
                }
                steps.push(PathStep::Key(key));
                s = rest;
            }
            b'[' => {
                let close = s
                    .find(']')
                    .ok_or_else(|| DataFusionError::Plan("Unclosed '[' in JSONPath".to_string()))?;
                let inner = s[1..close].trim();
                s = &s[close + 1..];

                if inner == "*" {
                    steps.push(PathStep::ArrayWildcard);
                } else if inner.starts_with('?') {
                    // Filter predicate: `[?(…)]`
                    let pred_src = inner[1..].trim();
                    if !pred_src.starts_with('(') || !pred_src.ends_with(')') {
                        return plan_err!(
                            "JSONPath filter must be of the form [?(…)], got: [{inner}]"
                        );
                    }
                    let pred_inner = &pred_src[1..pred_src.len() - 1];
                    let pred = parse_filter_predicate(pred_inner)?;
                    steps.push(PathStep::Filter(pred));
                } else if let Ok(idx) = inner.parse::<usize>() {
                    steps.push(PathStep::Index(idx));
                } else {
                    return plan_err!(
                        "Unsupported JSONPath subscript: [{inner}]. \
                        Supported: [0], [*], [?(…)]"
                    );
                }
            }
            _ => {
                return plan_err!("Unexpected character in JSONPath at: {s}");
            }
        }
    }

    Ok(steps)
}

/// Read a bare identifier/key from the beginning of `s`.
/// Returns `(key, remainder)`.
fn read_key(s: &str) -> (String, &str) {
    // Keys can be quoted with double-quotes in JSONPath.
    if s.starts_with('"') {
        // Quoted key — read until closing '"'.
        let mut i = 1;
        let bytes = s.as_bytes();
        while i < bytes.len() && bytes[i] != b'"' {
            i += 1;
        }
        let key = s[1..i].to_string();
        let rest = if i < bytes.len() {
            &s[i + 1..]
        } else {
            &s[i..]
        };
        return (key, rest);
    }
    let end = s
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(s.len());
    (s[..end].to_string(), &s[end..])
}

/// Parse the inner expression of a `[?(…)]` filter.
/// Supports:
/// - `@.key` — existence
/// - `@.key op value` where op ∈ {==, !=, <>, <, <=, >, >=}
/// - value can be a number, single-quoted string, `true`, `false`, `null`
fn parse_filter_predicate(s: &str) -> DFResult<FilterPredicate> {
    let s = s.trim();
    if !s.starts_with('@') {
        return plan_err!("JSONPath filter must start with '@', got: {s}");
    }
    let rest = &s[1..]; // drop '@'

    // Read the path from '@': zero or more `.key` segments.
    let mut path_parts: Vec<String> = Vec::new();
    let mut cur = rest;
    loop {
        let cur_trimmed = cur.trim_start();
        if cur_trimmed.starts_with('.') {
            cur = &cur_trimmed[1..]; // drop '.'
            let (key, r) = read_key(cur);
            if key.is_empty() {
                break;
            }
            path_parts.push(key);
            cur = r;
        } else if cur_trimmed.starts_with('[') {
            // Array subscript inside filter path — handle [0] / [*]
            let close = cur_trimmed
                .find(']')
                .ok_or_else(|| DataFusionError::Plan("Unclosed '[' in filter path".to_string()))?;
            let inner = cur_trimmed[1..close].trim();
            cur = &cur_trimmed[close + 1..];
            if inner == "*" {
                path_parts.push("*".to_string());
            } else if let Ok(idx) = inner.parse::<usize>() {
                path_parts.push(idx.to_string());
            }
        } else {
            break;
        }
    }

    let remaining = cur.trim();

    // If nothing follows the path, it's an existence check.
    if remaining.is_empty() {
        return Ok(FilterPredicate::Exists(path_parts));
    }

    // Parse the comparison operator.
    let (op, after_op) = parse_compare_op(remaining)?;
    let value = parse_filter_value(after_op.trim())?;

    Ok(FilterPredicate::Compare {
        path: path_parts,
        op,
        value,
    })
}

fn parse_compare_op(s: &str) -> DFResult<(CompareOp, &str)> {
    if s.starts_with("==") {
        Ok((CompareOp::Eq, &s[2..]))
    } else if s.starts_with("!=") || s.starts_with("<>") {
        Ok((CompareOp::Ne, &s[2..]))
    } else if s.starts_with("<=") {
        Ok((CompareOp::Le, &s[2..]))
    } else if s.starts_with(">=") {
        Ok((CompareOp::Ge, &s[2..]))
    } else if s.starts_with('<') {
        Ok((CompareOp::Lt, &s[1..]))
    } else if s.starts_with('>') {
        Ok((CompareOp::Gt, &s[1..]))
    } else if s.starts_with('=') {
        // Allow single `=` as alias for `==`.
        Ok((CompareOp::Eq, &s[1..]))
    } else {
        plan_err!("Unknown JSONPath comparison operator at: {s}")
    }
}

fn parse_filter_value(s: &str) -> DFResult<FilterValue> {
    let s = s.trim();
    if s == "null" {
        return Ok(FilterValue::Null);
    }
    if s == "true" {
        return Ok(FilterValue::Bool(true));
    }
    if s == "false" {
        return Ok(FilterValue::Bool(false));
    }
    if s.starts_with('\'') {
        // Single-quoted string.
        if s.len() >= 2 && s.ends_with('\'') {
            return Ok(FilterValue::Str(s[1..s.len() - 1].to_string()));
        }
        return plan_err!("Unterminated string in JSONPath filter: {s}");
    }
    if s.starts_with('"') {
        // Double-quoted string.
        if s.len() >= 2 && s.ends_with('"') {
            return Ok(FilterValue::Str(s[1..s.len() - 1].to_string()));
        }
        return plan_err!("Unterminated string in JSONPath filter: {s}");
    }
    // Try numeric.
    if let Ok(n) = s.parse::<f64>() {
        return Ok(FilterValue::Num(n));
    }
    plan_err!("Unsupported filter value: {s}")
}

// ---------------------------------------------------------------------------
// JSONPath evaluator
// ---------------------------------------------------------------------------

/// Execute `steps` against `root`, returning all matching `Value`s.
pub(crate) fn jsonpath_eval(root: &Value, steps: &[PathStep]) -> Vec<Value> {
    let mut current: Vec<Value> = vec![root.clone()];

    for step in steps {
        let mut next: Vec<Value> = Vec::new();
        match step {
            PathStep::Key(key) => {
                for v in &current {
                    if let Value::Object(map) = v {
                        if let Some(child) = map.get(key.as_str()) {
                            next.push(child.clone());
                        }
                    }
                }
            }
            PathStep::Index(idx) => {
                for v in &current {
                    if let Value::Array(arr) = v {
                        if let Some(elem) = arr.get(*idx) {
                            next.push(elem.clone());
                        }
                    }
                }
            }
            PathStep::ArrayWildcard => {
                for v in &current {
                    if let Value::Array(arr) = v {
                        next.extend(arr.iter().cloned());
                    }
                }
            }
            PathStep::ObjectWildcard => {
                for v in &current {
                    if let Value::Object(map) = v {
                        next.extend(map.values().cloned());
                    }
                }
            }
            PathStep::RecursiveKey(key) => {
                for v in &current {
                    collect_recursive_key(v, key, &mut next);
                }
            }
            PathStep::Filter(pred) => {
                for v in &current {
                    match v {
                        Value::Array(arr) => {
                            for elem in arr {
                                if filter_matches(elem, pred) {
                                    next.push(elem.clone());
                                }
                            }
                        }
                        // Filter on a single object: treat as single-element array.
                        Value::Object(_) => {
                            if filter_matches(v, pred) {
                                next.push(v.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        current = next;
    }

    current
}

/// Recursively collect all values accessible under `key` anywhere in the tree.
fn collect_recursive_key(v: &Value, key: &str, out: &mut Vec<Value>) {
    match v {
        Value::Object(map) => {
            if let Some(child) = map.get(key) {
                out.push(child.clone());
            }
            for child in map.values() {
                collect_recursive_key(child, key, out);
            }
        }
        Value::Array(arr) => {
            for elem in arr {
                collect_recursive_key(elem, key, out);
            }
        }
        _ => {}
    }
}

/// Evaluate a `FilterPredicate` against a candidate `Value`.
fn filter_matches(v: &Value, pred: &FilterPredicate) -> bool {
    match pred {
        FilterPredicate::Exists(path) => {
            // Existence check: navigate `path` from `v` and check non-null result.
            match resolve_filter_path(v, path) {
                Some(Value::Null) => false,
                Some(_) => true,
                None => false,
            }
        }
        FilterPredicate::Compare { path, op, value } => {
            let target = match resolve_filter_path(v, path) {
                Some(t) => t,
                None => return false,
            };
            compare_value(&target, op, value)
        }
    }
}

/// Navigate a simple dot-separated path from a root value.
fn resolve_filter_path<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = root;
    for seg in path {
        match cur {
            Value::Object(map) => {
                cur = map.get(seg.as_str())?;
            }
            Value::Array(arr) => {
                let idx: usize = seg.parse().ok()?;
                cur = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(cur)
}

/// Compare a JSON value against a filter literal.
fn compare_value(v: &Value, op: &CompareOp, expected: &FilterValue) -> bool {
    match (v, expected) {
        (Value::Number(n), FilterValue::Num(f)) => {
            let vf = n.as_f64().unwrap_or(f64::NAN);
            match op {
                CompareOp::Eq => (vf - f).abs() < f64::EPSILON,
                CompareOp::Ne => (vf - f).abs() >= f64::EPSILON,
                CompareOp::Lt => vf < *f,
                CompareOp::Le => vf <= *f,
                CompareOp::Gt => vf > *f,
                CompareOp::Ge => vf >= *f,
            }
        }
        (Value::String(s), FilterValue::Str(expected_s)) => match op {
            CompareOp::Eq => s == expected_s,
            CompareOp::Ne => s != expected_s,
            CompareOp::Lt => s.as_str() < expected_s.as_str(),
            CompareOp::Le => s.as_str() <= expected_s.as_str(),
            CompareOp::Gt => s.as_str() > expected_s.as_str(),
            CompareOp::Ge => s.as_str() >= expected_s.as_str(),
        },
        (Value::Bool(b), FilterValue::Bool(expected_b)) => match op {
            CompareOp::Eq => b == expected_b,
            CompareOp::Ne => b != expected_b,
            _ => false,
        },
        (Value::Null, FilterValue::Null) => matches!(op, CompareOp::Eq),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Top-level predicate parser for `@@` / jsonb_path_match
// ---------------------------------------------------------------------------

/// Parse a top-level predicate expression like `$.x > 3` or `$.name == "Alice"`.
///
/// Returns `Some((path_steps, op, value))` on success, `None` if the
/// expression doesn't match the simple predicate pattern (e.g. if it's
/// a plain path existence check like `$.key`).
fn parse_top_level_predicate(path: &str) -> DFResult<Option<TopLevelPredicate>> {
    let path = path.trim();
    // Strip optional lax/strict.
    let path = path.strip_prefix("strict").map(str::trim).unwrap_or(path);
    let path = path.strip_prefix("lax").map(str::trim).unwrap_or(path);

    if !path.starts_with('$') {
        return plan_err!("JSONPath predicate must start with '$', got: {path}");
    }

    // Find the comparison operator.
    // Strategy: look for `==`, `!=`, `<>`, `<=`, `>=`, `<`, `>` outside of quotes.
    // We need to split the path into `lhs_path OP rhs_value`.

    // Try to find op position by scanning.
    let bytes = path.as_bytes();
    let mut i = 0;
    let n = bytes.len();

    // Find where the path ends and operator begins.
    // Path chars: alphanumeric, _, ., [, ], *, $
    // Operator chars: =, !, <, >
    while i < n {
        match bytes[i] {
            b'\'' | b'"' => {
                // Skip string literals.
                let q = bytes[i];
                i += 1;
                while i < n && bytes[i] != q {
                    i += 1;
                }
                i += 1;
            }
            b'=' | b'!' | b'<' | b'>' => {
                // Found operator start.
                let path_part = path[..i].trim();
                let op_and_rest = &path[i..];
                let (op, after_op) = parse_compare_op(op_and_rest)?;
                let value = parse_filter_value(after_op.trim())?;

                // Parse the path part.
                if !path_part.starts_with('$') {
                    return plan_err!("JSONPath left-hand side must start with '$': {path_part}");
                }
                let lhs_rest = &path_part[1..]; // drop '$'
                                                // Convert path to key segments.
                let steps = parse_path_rest(lhs_rest)?;
                let key_path = steps_to_key_path(&steps)?;

                return Ok(Some(TopLevelPredicate {
                    path: key_path,
                    op,
                    value,
                }));
            }
            _ => i += 1,
        }
    }

    // No operator found — this is an existence predicate, not a comparison.
    // Return None to indicate "just check existence".
    Ok(None)
}

/// Convert a list of PathSteps to a simple Vec<String> key path.
/// Fails if the steps contain anything other than Key steps.
fn steps_to_key_path(steps: &[PathStep]) -> DFResult<Vec<String>> {
    let mut path = Vec::new();
    for step in steps {
        match step {
            PathStep::Key(k) => path.push(k.clone()),
            PathStep::Index(i) => path.push(i.to_string()),
            _ => {
                return plan_err!(
                    "Complex JSONPath in @@ predicate (only simple key paths supported)"
                )
            }
        }
    }
    Ok(path)
}

// ---------------------------------------------------------------------------
// Helper: convert serde_json::Value to LargeBinary JSONB bytes
// ---------------------------------------------------------------------------

fn value_to_jsonb_bytes(v: &Value) -> DFResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| DataFusionError::Execution(format!("JSON serialize: {e}")))
}

/// Typed view over a JSONB-compatible Arrow array, with `as_any` downcast
/// performed **once** up front.  Per-row access via [`TypedJsonbDoc::value`]
/// then takes a single `match` branch — avoiding the per-row vtable lookup
/// that `extract_jsonb_value_from_array` pays in the row loop.
///
/// Local to this module; mirrors `jsonb_udf::TypedJsonbArray` shape.
enum TypedJsonbDoc<'a> {
    LargeBinary(&'a datafusion::arrow::array::LargeBinaryArray),
    BinaryView(&'a datafusion::arrow::array::BinaryViewArray),
    Utf8(&'a StringArray),
    Utf8View(&'a datafusion::arrow::array::StringViewArray),
    LargeUtf8(&'a datafusion::arrow::array::LargeStringArray),
}

impl<'a> TypedJsonbDoc<'a> {
    /// Build once before the row loop.  Matches the union of types accepted
    /// by `extract_jsonb_value_from_array` for behavioural parity.
    fn new(arr: &'a ArrayRef, fn_name: &str) -> DFResult<Self> {
        if let Some(lb) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
        {
            return Ok(TypedJsonbDoc::LargeBinary(lb));
        }
        if let Some(bv) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
        {
            return Ok(TypedJsonbDoc::BinaryView(bv));
        }
        if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
            return Ok(TypedJsonbDoc::Utf8(sa));
        }
        if let Some(sa) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
        {
            return Ok(TypedJsonbDoc::Utf8View(sa));
        }
        if let Some(sa) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
        {
            return Ok(TypedJsonbDoc::LargeUtf8(sa));
        }
        exec_err!("{fn_name}: unsupported input type")
    }

    #[inline]
    fn value(&self, i: usize, fn_name: &str) -> DFResult<Option<Value>> {
        let parse_bytes = |b: &[u8]| {
            serde_json::from_slice(b).map_err(|e| {
                DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}"))
            })
        };
        let parse_str = |s: &str| {
            serde_json::from_str(s).map_err(|e| {
                DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}"))
            })
        };
        match self {
            TypedJsonbDoc::LargeBinary(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_bytes(a.value(i)).map(Some)
                }
            }
            TypedJsonbDoc::BinaryView(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_bytes(a.value(i)).map(Some)
                }
            }
            TypedJsonbDoc::Utf8(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_str(a.value(i)).map(Some)
                }
            }
            TypedJsonbDoc::Utf8View(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_str(a.value(i)).map(Some)
                }
            }
            TypedJsonbDoc::LargeUtf8(a) => {
                if a.is_null(i) {
                    Ok(None)
                } else {
                    parse_str(a.value(i)).map(Some)
                }
            }
        }
    }
}

/// Typed view over a string array (path / predicate argument), downcast once.
/// Per-row `value()` returns `&str` — caller does not need to clone unless
/// the downstream API wants an owned `String`.
enum TypedPathStr<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a datafusion::arrow::array::StringViewArray),
    LargeUtf8(&'a datafusion::arrow::array::LargeStringArray),
}

impl<'a> TypedPathStr<'a> {
    fn new(arr: &'a ArrayRef) -> Option<Self> {
        if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
            return Some(TypedPathStr::Utf8(sa));
        }
        if let Some(sa) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
        {
            return Some(TypedPathStr::Utf8View(sa));
        }
        if let Some(sa) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
        {
            return Some(TypedPathStr::LargeUtf8(sa));
        }
        None
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        match self {
            TypedPathStr::Utf8(a) => a.is_null(i),
            TypedPathStr::Utf8View(a) => a.is_null(i),
            TypedPathStr::LargeUtf8(a) => a.is_null(i),
        }
    }

    #[inline]
    fn value(&self, i: usize) -> &str {
        match self {
            TypedPathStr::Utf8(a) => a.value(i),
            TypedPathStr::Utf8View(a) => a.value(i),
            TypedPathStr::LargeUtf8(a) => a.value(i),
        }
    }
}

/// **Perf note**: pays an `as_any().downcast_ref()` per call.  In a row loop,
/// prefer [`TypedJsonbDoc::new`] once before the loop and
/// [`TypedJsonbDoc::value`] per row.
#[allow(dead_code)]
fn extract_jsonb_value_from_array(
    arr: &ArrayRef,
    i: usize,
    fn_name: &str,
) -> DFResult<Option<Value>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    // LargeBinary
    if let Some(lb) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
    {
        let bytes = lb.value(i);
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}")))?;
        return Ok(Some(v));
    }
    // BinaryView (Arrow view-format JSONB; DataFusion 53 surfaces this for
    // stored columns and any operator that re-buffers JSONB through the
    // view-format layout).
    if let Some(bv) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
    {
        let bytes = bv.value(i);
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}")))?;
        return Ok(Some(v));
    }
    // Utf8
    if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
        let s = sa.value(i);
        let v: Value = serde_json::from_str(s)
            .map_err(|e| DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}")))?;
        return Ok(Some(v));
    }
    // Utf8View
    if let Some(sa) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringViewArray>()
    {
        let s = sa.value(i);
        let v: Value = serde_json::from_str(s)
            .map_err(|e| DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}")))?;
        return Ok(Some(v));
    }
    // LargeUtf8
    if let Some(sa) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
    {
        let s = sa.value(i);
        let v: Value = serde_json::from_str(s)
            .map_err(|e| DataFusionError::Execution(format!("{fn_name}: JSON parse error: {e}")))?;
        return Ok(Some(v));
    }
    exec_err!("{fn_name}: unsupported input type")
}

/// **Perf note**: allocates per call.  In a row loop, prefer
/// [`TypedPathStr::new`] once before the loop.
#[allow(dead_code)]
fn extract_string_from_array(arr: &ArrayRef, i: usize) -> Option<String> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(sa.value(i).to_string());
    }
    if let Some(sa) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringViewArray>()
    {
        return Some(sa.value(i).to_string());
    }
    if let Some(sa) = arr
        .as_any()
        .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
    {
        return Some(sa.value(i).to_string());
    }
    None
}

fn row_count_from_args(args: &[ColumnarValue]) -> usize {
    for a in args {
        if let ColumnarValue::Array(arr) = a {
            return arr.len();
        }
    }
    1
}

// ---------------------------------------------------------------------------
// Scalar UDF: jsonb_path_query_first
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathQueryFirstUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryFirstUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_query_first"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!(
                "jsonb_path_query_first expects 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count_from_args(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        // Hoist downcasts: one match per array, not one per row.
        let doc_typed = TypedJsonbDoc::new(&doc_arr, "jsonb_path_query_first")?;
        let path_typed = TypedPathStr::new(&path_arr);
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

        for i in 0..n {
            let doc = match doc_typed.value(i, "jsonb_path_query_first")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let path_str: &str = match &path_typed {
                Some(p) if !p.is_null(i) => p.value(i),
                _ => {
                    out.push(None);
                    continue;
                }
            };
            let steps = parse_jsonpath(path_str)
                .map_err(|e| DataFusionError::Execution(format!("jsonb_path_query_first: {e}")))?;
            let matches = jsonpath_eval(&doc, &steps);
            match matches.into_iter().next() {
                Some(v) => out.push(Some(value_to_jsonb_bytes(&v)?)),
                None => out.push(None),
            }
        }

        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// Scalar UDF: jsonb_path_query_array
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathQueryArrayUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathQueryArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_query_array"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!(
                "jsonb_path_query_array expects 2 arguments, got {}",
                args.len()
            );
        }
        let n = row_count_from_args(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let path_arr = args[1].clone().into_array(n)?;
        // Hoist downcasts: one match per array, not one per row.
        let doc_typed = TypedJsonbDoc::new(&doc_arr, "jsonb_path_query_array")?;
        let path_typed = TypedPathStr::new(&path_arr);
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

        for i in 0..n {
            let doc = match doc_typed.value(i, "jsonb_path_query_array")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let path_str: &str = match &path_typed {
                Some(p) if !p.is_null(i) => p.value(i),
                _ => {
                    out.push(None);
                    continue;
                }
            };
            let steps = parse_jsonpath(path_str)
                .map_err(|e| DataFusionError::Execution(format!("jsonb_path_query_array: {e}")))?;
            let matches = jsonpath_eval(&doc, &steps);
            let arr = Value::Array(matches);
            out.push(Some(value_to_jsonb_bytes(&arr)?));
        }

        let result = LargeBinaryArray::from_iter(out.iter().map(|o| o.as_deref()));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// Scalar UDF: jsonb_path_match (implements @@ operator)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonbPathMatchUdf {
    signature: Signature,
}

impl ScalarUDFImpl for JsonbPathMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "jsonb_path_match"
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
        if args.len() != 2 {
            return exec_err!("jsonb_path_match expects 2 arguments, got {}", args.len());
        }
        let n = row_count_from_args(args);
        let doc_arr = args[0].clone().into_array(n)?;
        let pred_arr = args[1].clone().into_array(n)?;
        // Hoist downcasts: one match per array, not one per row.
        let doc_typed = TypedJsonbDoc::new(&doc_arr, "jsonb_path_match")?;
        let pred_typed = TypedPathStr::new(&pred_arr);
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);

        for i in 0..n {
            let doc = match doc_typed.value(i, "jsonb_path_match")? {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let pred_str: &str = match &pred_typed {
                Some(p) if !p.is_null(i) => p.value(i),
                _ => {
                    out.push(None);
                    continue;
                }
            };

            // Try to parse as a top-level predicate `$.x op value`.
            match parse_top_level_predicate(pred_str) {
                Ok(Some(pred)) => {
                    let target = resolve_filter_path(&doc, &pred.path);
                    let result = match target {
                        Some(v) => compare_value(v, &pred.op, &pred.value),
                        None => false,
                    };
                    out.push(Some(result));
                }
                Ok(None) => {
                    // No operator found — treat as existence check via jsonpath_eval.
                    let steps = parse_jsonpath(pred_str).map_err(|e| {
                        DataFusionError::Execution(format!("jsonb_path_match: {e}"))
                    })?;
                    let matches = jsonpath_eval(&doc, &steps);
                    out.push(Some(!matches.is_empty()));
                }
                Err(e) => {
                    return Err(DataFusionError::Execution(format!("jsonb_path_match: {e}")));
                }
            }
        }

        let result = BooleanArray::from(out);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// ---------------------------------------------------------------------------
// UDTF: jsonb_path_query (true SRF — returns SETOF jsonb)
// ---------------------------------------------------------------------------

/// DataFusion's UDTF machinery calls `TableFunctionImpl::call()` at plan time
/// with the argument `Expr` list.  For literal JSON arguments we materialise
/// the rows eagerly into a MemTable; for column references we return a
/// plan-time error (SRF over columns requires proper LATERAL support which is
/// a future item — same limitation as `jsonb_array_elements`).
#[derive(Debug)]
struct JsonbPathQueryTf {}

/// Table produced by `jsonb_path_query`.  One column `value` of type
/// `LargeBinary` (JSONB), one row per match.
#[derive(Debug)]
struct JsonbPathQueryTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for JsonbPathQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// Extract JSON text from a literal `Expr` (Utf8 or LargeBinary scalar).
fn json_from_literal_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeBinary(Some(b)), _) => String::from_utf8(b.clone()).ok(),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

impl TableFunctionImpl for JsonbPathQueryTf {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 2 {
            return plan_err!("jsonb_path_query requires exactly 2 arguments (target, path)");
        }

        // Extract target JSON.
        let json_str = match json_from_literal_expr(&args[0]) {
            Some(s) => s,
            None => {
                return plan_err!(
                    "jsonb_path_query: first argument must be a JSON literal. \
                    Column-valued SRF requires LATERAL support (not yet implemented)"
                );
            }
        };

        // Extract path string.
        let path_str = match &args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
            _ => {
                return plan_err!(
                    "jsonb_path_query: second argument (path) must be a string literal"
                );
            }
        };

        // Parse the document.
        let doc: Value = serde_json::from_str(&json_str).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_path_query: JSON parse error: {e}"))
        })?;

        // Parse and evaluate the JSONPath.
        let steps = parse_jsonpath(&path_str)?;
        let matches = jsonpath_eval(&doc, &steps);

        // Build a RecordBatch with one row per match.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::LargeBinary,
            true,
        )]));

        let rows: Vec<Option<Vec<u8>>> =
            matches.iter().map(|v| serde_json::to_vec(v).ok()).collect();

        let val_arr: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            rows.iter().map(|o| o.as_deref()),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![val_arr]).map_err(|e| {
            DataFusionError::Plan(format!("jsonb_path_query: RecordBatch error: {e}"))
        })?;

        Ok(Arc::new(JsonbPathQueryTable {
            schema,
            batches: vec![batch],
        }))
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── parse_jsonpath tests ─────────────────────────────────────────────────

    #[test]
    fn test_parse_root_only() {
        let steps = parse_jsonpath("$").unwrap();
        assert!(steps.is_empty());
    }

    #[test]
    fn test_parse_simple_key() {
        let steps = parse_jsonpath("$.a").unwrap();
        assert_eq!(steps, vec![PathStep::Key("a".to_string())]);
    }

    #[test]
    fn test_parse_nested_keys() {
        let steps = parse_jsonpath("$.a.b.c").unwrap();
        assert_eq!(
            steps,
            vec![
                PathStep::Key("a".to_string()),
                PathStep::Key("b".to_string()),
                PathStep::Key("c".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_array_index() {
        let steps = parse_jsonpath("$.a[0]").unwrap();
        assert_eq!(
            steps,
            vec![PathStep::Key("a".to_string()), PathStep::Index(0)]
        );
    }

    #[test]
    fn test_parse_array_wildcard() {
        let steps = parse_jsonpath("$.a[*]").unwrap();
        assert_eq!(
            steps,
            vec![PathStep::Key("a".to_string()), PathStep::ArrayWildcard]
        );
    }

    #[test]
    fn test_parse_object_wildcard() {
        let steps = parse_jsonpath("$.*").unwrap();
        assert_eq!(steps, vec![PathStep::ObjectWildcard]);
    }

    #[test]
    fn test_parse_recursive_descent() {
        let steps = parse_jsonpath("$..name").unwrap();
        assert_eq!(steps, vec![PathStep::RecursiveKey("name".to_string())]);
    }

    #[test]
    fn test_parse_filter_gt() {
        let steps = parse_jsonpath("$.items[?(@.x > 1)]").unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0], PathStep::Key("items".to_string()));
        if let PathStep::Filter(FilterPredicate::Compare { path, op, value }) = &steps[1] {
            assert_eq!(path, &vec!["x".to_string()]);
            assert_eq!(*op, CompareOp::Gt);
            assert_eq!(*value, FilterValue::Num(1.0));
        } else {
            panic!("Expected Filter(Compare), got: {:?}", steps[1]);
        }
    }

    #[test]
    fn test_parse_filter_eq_string() {
        let steps = parse_jsonpath("$.items[?(@.name == 'alice')]").unwrap();
        assert_eq!(steps.len(), 2);
        if let PathStep::Filter(FilterPredicate::Compare { path, op, value }) = &steps[1] {
            assert_eq!(path, &vec!["name".to_string()]);
            assert_eq!(*op, CompareOp::Eq);
            assert_eq!(*value, FilterValue::Str("alice".to_string()));
        } else {
            panic!("Expected Filter(Compare)");
        }
    }

    #[test]
    fn test_parse_filter_existence() {
        let steps = parse_jsonpath("$.items[?(@.active)]").unwrap();
        assert_eq!(steps.len(), 2);
        if let PathStep::Filter(FilterPredicate::Exists(path)) = &steps[1] {
            assert_eq!(path, &vec!["active".to_string()]);
        } else {
            panic!("Expected Filter(Exists)");
        }
    }

    #[test]
    fn test_parse_method_call_rejected() {
        let err = parse_jsonpath("$.a.type()");
        assert!(err.is_err(), "method calls should be rejected");
    }

    // ── jsonpath_eval tests ──────────────────────────────────────────────────

    #[test]
    fn test_eval_array_wildcard_returns_all_elements() {
        // jsonb_path_query('{"a":[1,2,3]}', '$.a[*]') → 3 rows
        let doc = json!({"a": [1, 2, 3]});
        let steps = parse_jsonpath("$.a[*]").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert_eq!(results, vec![json!(1), json!(2), json!(3)]);
    }

    #[test]
    fn test_eval_array_index() {
        let doc = json!({"a": [10, 20, 30]});
        let steps = parse_jsonpath("$.a[1]").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert_eq!(results, vec![json!(20)]);
    }

    #[test]
    fn test_eval_nested_key() {
        let doc = json!({"a": {"b": {"c": 42}}});
        let steps = parse_jsonpath("$.a.b.c").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert_eq!(results, vec![json!(42)]);
    }

    #[test]
    fn test_eval_recursive_descent() {
        let doc = json!({"a": {"name": "top"}, "b": {"name": "nested"}});
        let steps = parse_jsonpath("$..name").unwrap();
        let mut results = jsonpath_eval(&doc, &steps);
        results.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&json!("top")));
        assert!(results.contains(&json!("nested")));
    }

    #[test]
    fn test_eval_object_wildcard() {
        let doc = json!({"x": 1, "y": 2});
        let steps = parse_jsonpath("$.*").unwrap();
        let mut results = jsonpath_eval(&doc, &steps);
        results.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_eval_filter_gt() {
        let doc = json!({"items": [{"x": 1}, {"x": 3}, {"x": 5}]});
        let steps = parse_jsonpath("$.items[?(@.x > 1)]").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&json!({"x": 3})));
        assert!(results.contains(&json!({"x": 5})));
    }

    #[test]
    fn test_eval_filter_existence() {
        let doc = json!({"items": [{"x": 1}, {"y": 2}, {"x": 3}]});
        let steps = parse_jsonpath("$.items[?(@.x)]").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_eval_missing_key_returns_empty() {
        let doc = json!({"a": 1});
        let steps = parse_jsonpath("$.b").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert!(results.is_empty());
    }

    // ── jsonb_path_query_array helpers ───────────────────────────────────────

    #[test]
    fn test_array_form_wraps_results() {
        // jsonb_path_query_array('{"a":[1,2,3]}', '$.a[*]') → '[1,2,3]'
        let doc = json!({"a": [1, 2, 3]});
        let steps = parse_jsonpath("$.a[*]").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        let arr = Value::Array(results);
        assert_eq!(arr, json!([1, 2, 3]));
    }

    // ── rewrite_jsonpath_operators tests ────────────────────────────────────

    #[test]
    fn test_rewrite_at_at_operator() {
        let sql = "SELECT data @@ '$.x > 3' FROM t";
        let rewritten = rewrite_jsonpath_operators(sql);
        assert!(
            rewritten.contains("jsonb_path_match"),
            "Expected jsonb_path_match in: {rewritten}"
        );
        assert!(
            !rewritten.contains(" @@ "),
            "Expected @@ to be rewritten: {rewritten}"
        );
    }

    #[test]
    fn test_rewrite_no_op_if_no_at_at() {
        let sql = "SELECT a FROM t WHERE b > 1";
        let rewritten = rewrite_jsonpath_operators(sql);
        assert_eq!(rewritten, sql);
    }

    // ── top-level predicate (@@) eval ────────────────────────────────────────

    #[test]
    fn test_top_level_predicate_gt() {
        // '{"x":5}'::jsonb @@ '$.x > 3' → true
        let doc = json!({"x": 5});
        let pred = parse_top_level_predicate("$.x > 3").unwrap().unwrap();
        let target = resolve_filter_path(&doc, &pred.path);
        assert!(compare_value(target.unwrap(), &pred.op, &pred.value));
    }

    #[test]
    fn test_top_level_predicate_gt_false() {
        // '{"x":2}'::jsonb @@ '$.x > 3' → false
        let doc = json!({"x": 2});
        let pred = parse_top_level_predicate("$.x > 3").unwrap().unwrap();
        let target = resolve_filter_path(&doc, &pred.path);
        assert!(!compare_value(target.unwrap(), &pred.op, &pred.value));
    }

    // ── @? existence operator tests ──────────────────────────────────────────

    #[test]
    fn test_path_exists_true() {
        // '{"a":1}'::jsonb @? '$.a' → true (via jsonpath_eval)
        let doc = json!({"a": 1});
        let steps = parse_jsonpath("$.a").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert!(!results.is_empty(), "Expected match for $.a");
    }

    #[test]
    fn test_path_exists_false() {
        // '{"a":1}'::jsonb @? '$.b' → false
        let doc = json!({"a": 1});
        let steps = parse_jsonpath("$.b").unwrap();
        let results = jsonpath_eval(&doc, &steps);
        assert!(results.is_empty(), "Expected no match for $.b");
    }

    #[test]
    fn test_lax_prefix_stripped() {
        let steps = parse_jsonpath("lax $.a[*]").unwrap();
        assert_eq!(
            steps,
            vec![PathStep::Key("a".to_string()), PathStep::ArrayWildcard]
        );
    }

    #[test]
    fn test_strict_prefix_stripped() {
        let steps = parse_jsonpath("strict $.a").unwrap();
        assert_eq!(steps, vec![PathStep::Key("a".to_string())]);
    }
}

// ---------------------------------------------------------------------------
// Microbenchmark (opt-in, env-gated — not part of CI)
// ---------------------------------------------------------------------------
//
// Run: `BASIN_JSONB_BENCH=1 cargo test -p basin-engine --lib --release \
//       jsonb_path_udf::perf_bench -- --nocapture --ignored`
#[cfg(test)]
mod perf_bench {
    use super::*;
    use datafusion::arrow::array::LargeBinaryArray;
    use std::time::Instant;

    #[test]
    #[ignore = "opt-in: BASIN_JSONB_BENCH=1"]
    fn bench_path_query_first_100k() {
        if std::env::var("BASIN_JSONB_BENCH").as_deref() != Ok("1") {
            return;
        }
        const N: usize = 100_000;
        let rows: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                serde_json::to_vec(&serde_json::json!({
                    "user": {"name": format!("u{i}"), "age": i % 100},
                    "tags": ["a", "b", "c"],
                }))
                .unwrap()
            })
            .collect();
        let doc_arr: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            rows.iter().map(|r| Some(r.as_slice())),
        ));
        let path_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            std::iter::repeat("$.user.name").take(N),
        ));

        // SLOW: pre-optimization shape — per-row downcast + String alloc.
        let t0 = Instant::now();
        for i in 0..N {
            let doc = extract_jsonb_value_from_array(&doc_arr, i, "b").unwrap().unwrap();
            let p = extract_string_from_array(&path_arr, i).unwrap();
            let steps = parse_jsonpath(&p).unwrap();
            let matches = jsonpath_eval(&doc, &steps);
            std::hint::black_box(matches.into_iter().next());
        }
        let slow = t0.elapsed();

        // FAST: hoisted downcasts + &str path access.
        let doc_t = TypedJsonbDoc::new(&doc_arr, "b").unwrap();
        let path_t = TypedPathStr::new(&path_arr).unwrap();
        let t1 = Instant::now();
        for i in 0..N {
            let doc = doc_t.value(i, "b").unwrap().unwrap();
            let p: &str = path_t.value(i);
            let steps = parse_jsonpath(p).unwrap();
            let matches = jsonpath_eval(&doc, &steps);
            std::hint::black_box(matches.into_iter().next());
        }
        let fast = t1.elapsed();
        eprintln!(
            "SLOW {:?} ({:.2} M/s) | FAST {:?} ({:.2} M/s) | {:.2}x",
            slow,
            N as f64 / slow.as_secs_f64() / 1e6,
            fast,
            N as f64 / fast.as_secs_f64() / 1e6,
            slow.as_secs_f64() / fast.as_secs_f64()
        );
    }
}
