//! Sequence UDFs (`nextval` / `currval` / `setval`) and the SQL-string
//! rewriter that routes calls to the catalog before sqlparser runs.
//!
//! ## Why a string rewrite, not a real UDF body
//!
//! DataFusion's `ScalarUDFImpl::invoke` is synchronous and runs inside
//! the runtime executor; calling an `async` `Catalog` method from there
//! requires either:
//!
//! * a separate runtime via `spawn_blocking + Runtime::block_on`, which
//!   is heavyweight per call and breaks the per-project blocking-pool
//!   budget the executor already maintains, or
//! * `tokio::task::block_in_place + Handle::block_on`, which panics on
//!   the single-thread runtime that powers `#[tokio::test]`.
//!
//! Pre-execution rewrite sidesteps both. The executor invokes
//! [`rewrite_sequence_calls`] right after the existing
//! `rewrite_extract_second` / `rewrite_vector_operators` passes; calls
//! with literal sequence-name arguments are computed via the catalog
//! (an `async` call) and substituted with the resulting BIGINT literal
//! before sqlparser sees the SQL.
//!
//! The UDF tombstones registered alongside the regular pg-compat
//! catalogue (`NextvalUdf` / `CurrvalUdf` / `SetvalUdf`) only fire when
//! the rewriter couldn't statically resolve the call — typically a
//! dynamic sequence-name argument like `nextval(some_column)`. v0.1
//! refuses that path with a clear execution-time error; v0.2 can
//! upgrade to a true async-bridge UDF if dynamic-name lookup ever
//! becomes load-bearing.

use std::any::Any;
use std::sync::Arc;

use basin_catalog::Catalog;
use basin_common::{BasinError, Result, ProjectId};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use tokio::sync::Mutex;

/// Per-session bookkeeping for sequence UDFs. Tracks the most recent
/// `nextval` value handed out *to this session* so `currval` matches PG
/// semantics ("currval is not yet defined for this session" until the
/// session calls `nextval`).
///
/// Also tracks the globally-last sequence name advanced in this session
/// so `lastval()` can retrieve it (PG: `lastval` returns the last value
/// returned by `nextval` in the current session, regardless of which
/// sequence name was used).
///
/// One instance is owned by each [`crate::session::SessionState`] and
/// shared with the rewriter via the [`SequenceContext`] passed at SQL
/// rewrite time.
#[derive(Default, Debug)]
pub(crate) struct SessionSequenceCache {
    /// `(project, sequence_name) -> last nextval`. Per-session, so two
    /// sessions on the same sequence never confuse each other's
    /// `currval`. Project is included in the key so a future
    /// cross-project session (admin tools, etc.) is sound; in the v0.1
    /// per-project `ProjectSession` shape there's only ever one project
    /// per cache instance.
    inner: Mutex<SessionSequenceCacheInner>,
}

#[derive(Default, Debug)]
struct SessionSequenceCacheInner {
    /// Per-sequence last-value map.
    values: std::collections::HashMap<(ProjectId, String), i64>,
    /// Most recent `(project, seq_name, value)` for `lastval()`.
    last: Option<(ProjectId, String, i64)>,
}

impl SessionSequenceCache {
    pub(crate) async fn record(&self, project: ProjectId, name: &str, value: i64) {
        let mut g = self.inner.lock().await;
        g.values.insert((project, name.to_string()), value);
        g.last = Some((project, name.to_string(), value));
    }

    pub(crate) async fn get(&self, project: ProjectId, name: &str) -> Option<i64> {
        let g = self.inner.lock().await;
        g.values.get(&(project, name.to_string())).copied()
    }

    /// Return the value from the most recent `nextval` call in this session,
    /// across all sequences. PG SQLSTATE 55000 when no `nextval` has been
    /// called yet.
    pub(crate) async fn lastval(&self, project: ProjectId) -> Result<i64> {
        let g = self.inner.lock().await;
        match &g.last {
            Some((t, _, v)) if *t == project => Ok(*v),
            _ => Err(BasinError::Catalog(
                "55000: lastval is not yet defined in this session".into(),
            )),
        }
    }
}

/// Bundle of references the rewriter needs to invoke catalog methods
/// for sequence calls. Cheap to assemble at the start of every query
/// (one `Arc::clone` per field).
pub(crate) struct SequenceContext<'a> {
    pub catalog: &'a Arc<dyn Catalog>,
    pub project: ProjectId,
    pub session_cache: &'a Arc<SessionSequenceCache>,
}

/// Tombstone UDF for `nextval(text) -> bigint`. Always errors on
/// invocation; the rewriter is the load-bearing path. See module docs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct NextvalUdf {
    signature: Signature,
}

impl Default for NextvalUdf {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for NextvalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "nextval"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "nextval: dynamic sequence-name arguments are not supported in v0.1; \
             use a string literal"
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CurrvalUdf {
    signature: Signature,
}

impl Default for CurrvalUdf {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for CurrvalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "currval"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "currval: dynamic sequence-name arguments are not supported in v0.1; \
             use a string literal"
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SetvalUdf {
    signature: Signature,
}

impl Default for SetvalUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for SetvalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "setval"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "setval: dynamic sequence-name arguments are not supported in v0.1; \
             use a string literal"
        )
    }
}

/// Tombstone UDF for `lastval() -> bigint`. Always errors on invocation;
/// the pre-execution rewriter is the load-bearing path (same pattern as
/// `NextvalUdf` / `CurrvalUdf` / `SetvalUdf`). `lastval()` takes no
/// arguments so its signature is a zero-argument volatile scalar.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct LastvalUdf {
    signature: Signature,
}

impl Default for LastvalUdf {
    fn default() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for LastvalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "lastval"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        exec_err!(
            "lastval: call was not resolved by the pre-execution rewriter; \
             this is an internal error — please report it"
        )
    }
}

/// Tag for which kind of sequence call the rewriter saw. The
/// `setval` advance flag is parsed out of the call's third argument
/// (defaulting to `true`) by [`parse_seq_args`] rather than being
/// embedded here, so this enum stays a plain dispatch tag.
#[derive(Copy, Clone, Debug)]
enum SeqCall {
    Nextval,
    Currval,
    Setval,
    /// `lastval()` — no arguments; returns the value from the last
    /// `nextval` call in this session, regardless of which sequence.
    Lastval,
}

/// Rewrite every `nextval('seq')`, `currval('seq')`, and `setval('seq',
/// n[, advance])` call in `sql` to a literal BIGINT, dispatching the
/// underlying state mutation through `ctx.catalog`. Idempotent on input
/// SQL with no sequence calls. Returns the rewritten SQL.
///
/// The rewriter is intentionally conservative:
///
/// * Only literal string-name arguments are supported. Dynamic forms
///   like `nextval(my_col)` fall through to the tombstone UDF, which
///   surfaces a clean execution-time error.
/// * The match is case-insensitive on the function name and tolerates
///   surrounding whitespace.
/// * String-literal interiors are not skipped — a call-shaped substring
///   inside `'...'` will be rewritten. Same caveat as
///   `rewrite_extract_second` / `rewrite_vector_operators`.
pub(crate) async fn rewrite_sequence_calls(sql: &str, ctx: &SequenceContext<'_>) -> Result<String> {
    let mut s = sql.to_string();
    loop {
        let Some(call) = find_first_sequence_call(&s) else {
            break;
        };
        let (kind, call_start, call_end, args_text) = call;
        let parsed = match parse_seq_args(&args_text, kind) {
            Some(p) => p,
            None => {
                // Couldn't parse as literal-only; leave it for the
                // tombstone UDF (whose error message points the user at
                // the v0.1 limitation).
                break;
            }
        };
        let value = dispatch_sequence_call(ctx, kind, &parsed).await?;
        // Emit a plain integer literal rather than `<n>::bigint`. The
        // ::bigint cast survived rewrite and broke the INSERT-default
        // path's `coerce_i64`, which only recognises bare Number /
        // UnaryOp(Minus, Number). The downstream planner re-infers the
        // type from context (parameter type, column type) so the cast
        // is unnecessary.
        let replacement = if value < 0 {
            // Wrap negatives in parens so the surrounding `replace_range`
            // doesn't accidentally fuse a `-` with an adjacent operator
            // (e.g. `a-nextval(...)` becoming `a--5` instead of `a-(-5)`).
            format!("({value})")
        } else {
            value.to_string()
        };
        s.replace_range(call_start..call_end, &replacement);
    }
    Ok(s)
}

/// Parsed literal-only sequence-call arguments.
struct ParsedSeqArgs {
    name: String,
    value: Option<i64>,
    advance: Option<bool>,
}

/// Parse `'seq'[, n[, true|false]]` into [`ParsedSeqArgs`]. Returns
/// `None` if any argument is non-literal (e.g. an identifier, function
/// call, or arithmetic expression) so the caller can fall back to the
/// UDF path.
fn parse_seq_args(args_text: &str, kind: SeqCall) -> Option<ParsedSeqArgs> {
    let parts = split_top_level_args(args_text);
    let parts: Vec<&str> = parts.iter().map(|s| s.trim()).collect();
    // `lastval()` takes no arguments.
    if matches!(kind, SeqCall::Lastval) {
        return Some(ParsedSeqArgs {
            name: String::new(),
            value: None,
            advance: None,
        });
    }
    let want = match kind {
        SeqCall::Nextval | SeqCall::Currval => 1,
        SeqCall::Setval => {
            // Either 2 (default advance=true) or 3 (explicit).
            if parts.len() != 2 && parts.len() != 3 {
                return None;
            }
            parts.len()
        }
        SeqCall::Lastval => unreachable!("handled above"),
    };
    if parts.len() != want {
        return None;
    }
    let name = parse_string_literal(parts[0])?;
    let value = match kind {
        SeqCall::Setval => Some(parse_int_literal(parts[1])?),
        _ => None,
    };
    let advance = if let SeqCall::Setval = kind {
        if parts.len() == 3 {
            Some(parse_bool_literal(parts[2])?)
        } else {
            Some(true)
        }
    } else {
        None
    };
    Some(ParsedSeqArgs {
        name,
        value,
        advance,
    })
}

/// Strip outer single quotes (PG-style) and unescape doubled-up `''`
/// pairs into a single literal `'`. Returns `None` for anything that
/// isn't a string literal.
fn parse_string_literal(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\'' || bytes[bytes.len() - 1] != b'\'' {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    Some(inner.replace("''", "'"))
}

fn parse_int_literal(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

fn parse_bool_literal(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "t" => Some(true),
        "false" | "f" => Some(false),
        _ => None,
    }
}

/// Split a function-call argument list on top-level commas, ignoring
/// commas nested inside parentheses or string literals.
fn split_top_level_args(args: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0;
    let bytes = args.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_str => in_str = true,
            b'\'' if in_str => {
                // doubled-up '' is an escaped quote; stay in-string.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 1;
                } else {
                    in_str = false;
                }
            }
            b'(' if !in_str => depth += 1,
            b')' if !in_str => depth -= 1,
            b',' if !in_str && depth == 0 => {
                out.push(args[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(args[start..].to_string());
    out
}

async fn dispatch_sequence_call(
    ctx: &SequenceContext<'_>,
    kind: SeqCall,
    parsed: &ParsedSeqArgs,
) -> Result<i64> {
    match kind {
        SeqCall::Nextval => {
            let v = ctx.catalog.nextval(&ctx.project, &parsed.name).await?;
            ctx.session_cache.record(ctx.project, &parsed.name, v).await;
            Ok(v)
        }
        SeqCall::Currval => {
            // PG semantics: per-session cache, not catalog-wide. If the
            // session hasn't called `nextval` for this sequence yet,
            // surface the PG-style "object_not_in_prerequisite_state"
            // error mapped through `BasinError::Catalog` (the router's
            // SQLSTATE table maps Catalog -> 55000-class behaviour).
            match ctx.session_cache.get(ctx.project, &parsed.name).await {
                Some(v) => Ok(v),
                None => Err(BasinError::Catalog(format!(
                    "55000: currval of sequence {:?} is not yet defined in this session",
                    parsed.name
                ))),
            }
        }
        SeqCall::Setval => {
            let value = parsed.value.expect("setval requires value");
            let advance = parsed.advance.expect("setval has advance flag");
            let v = ctx
                .catalog
                .setval(&ctx.project, &parsed.name, value, advance)
                .await?;
            // Per PG: setval(seq, n, true) updates the session's
            // "last value" (advance=true); setval(seq, n, false) does
            // *not* arm currval. Mirror that.
            if advance {
                ctx.session_cache.record(ctx.project, &parsed.name, v).await;
            }
            Ok(v)
        }
        SeqCall::Lastval => ctx.session_cache.lastval(ctx.project).await,
    }
}

/// Locate the next `nextval(...)` / `currval(...)` / `setval(...)` call
/// in `s`. Returns `(kind, call_start, call_end, args_text)` where the
/// byte range `[call_start, call_end)` covers the entire call expression
/// (including the trailing `)`), and `args_text` is the contents
/// between the parentheses.
fn find_first_sequence_call(s: &str) -> Option<(SeqCall, usize, usize, String)> {
    let lower = s.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let bytes = s.as_bytes();
    let mut idx = 0usize;
    while idx < lb.len() {
        let (kw_len, kind) = if matches_keyword(lb, idx, b"nextval") {
            (7, SeqCall::Nextval)
        } else if matches_keyword(lb, idx, b"currval") {
            (7, SeqCall::Currval)
        } else if matches_keyword(lb, idx, b"setval") {
            (6, SeqCall::Setval)
        } else if matches_keyword(lb, idx, b"lastval") {
            (7, SeqCall::Lastval)
        } else {
            idx += 1;
            continue;
        };
        // Identifier-boundary check on both sides.
        let pre_ok = idx == 0 || {
            let c = bytes[idx - 1];
            !(c.is_ascii_alphanumeric() || c == b'_')
        };
        if !pre_ok {
            idx += 1;
            continue;
        }
        let mut j = idx + kw_len;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j >= bytes.len() || bytes[j] != b'(' {
            idx += 1;
            continue;
        }
        let args_start = j + 1;
        let mut depth = 1i32;
        let mut in_str = false;
        let mut k = args_start;
        while k < bytes.len() && depth > 0 {
            match bytes[k] {
                b'\'' if !in_str => in_str = true,
                b'\'' if in_str => {
                    if k + 1 < bytes.len() && bytes[k + 1] == b'\'' {
                        k += 1;
                    } else {
                        in_str = false;
                    }
                }
                b'(' if !in_str => depth += 1,
                b')' if !in_str => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                break;
            }
            k += 1;
        }
        if depth != 0 {
            return None;
        }
        let args_end = k;
        let call_end = k + 1;
        let args_text = s[args_start..args_end].to_string();
        return Some((kind, idx, call_end, args_text));
    }
    None
}

fn matches_keyword(lower: &[u8], idx: usize, kw: &[u8]) -> bool {
    idx + kw.len() <= lower.len() && &lower[idx..idx + kw.len()] == kw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_string_literal_basic() {
        assert_eq!(parse_string_literal("'foo'"), Some("foo".into()));
        assert_eq!(parse_string_literal("'a''b'"), Some("a'b".into()));
        assert_eq!(parse_string_literal("foo"), None);
    }

    #[test]
    fn split_args_handles_strings_and_nesting() {
        let parts = split_top_level_args("'foo', 1, true");
        assert_eq!(parts, vec!["'foo'", " 1", " true"]);
        let parts = split_top_level_args("'a, b', 2");
        assert_eq!(parts, vec!["'a, b'", " 2"]);
        let parts = split_top_level_args("foo(1, 2), 3");
        assert_eq!(parts, vec!["foo(1, 2)", " 3"]);
    }

    #[test]
    fn find_first_call_locates_nextval() {
        let s = "SELECT nextval('foo')";
        let (kind, start, end, args) = find_first_sequence_call(s).expect("should find call");
        assert!(matches!(kind, SeqCall::Nextval));
        assert_eq!(&s[start..end], "nextval('foo')");
        assert_eq!(args, "'foo'");
    }

    #[test]
    fn find_first_call_skips_identifier_prefix_match() {
        // `mynextval(...)` should not match.
        let s = "SELECT mynextval('foo')";
        assert!(find_first_sequence_call(s).is_none());
    }

    #[test]
    fn find_first_call_setval_three_args() {
        let s = "SELECT setval('foo', 100, false)";
        let (kind, _, _, args) = find_first_sequence_call(s).unwrap();
        assert!(matches!(kind, SeqCall::Setval));
        assert_eq!(args, "'foo', 100, false");
    }
}
