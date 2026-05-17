//! INET / CIDR scalar UDFs and operator pre-pass rewriter.
//!
//! ## Storage model
//! Both `INET` and `CIDR` columns are stored as Arrow `Utf8` with a
//! `BASIN_TYPE=INET` / `BASIN_TYPE=CIDR` field metadata marker (see
//! `crate::types`). INSERT-time validation lives in `crate::dml`.
//!
//! ## UDFs registered here
//!
//! | Function name         | Signature           | Returns  | PG equivalent |
//! |-----------------------|---------------------|----------|---------------|
//! | `inet_contained_by`   | (text, text) → bool | BOOLEAN  | `A << B`      |
//! | `inet_contains`       | (text, text) → bool | BOOLEAN  | `A >> B`      |
//!
//! Both functions accept any combination of IPv4 / IPv6 host address
//! (`192.168.1.1`) and network notation (`192.168.0.0/16`).
//!
//! ## Operator rewriter (`rewrite_inet_operators`)
//!
//! The pre-pass textual rewriter translates PG network infix operators:
//!
//! | PG operator | Rewrites to                     |
//! |-------------|---------------------------------|
//! | `A << B`    | `inet_contained_by(A, B)`       |
//! | `A >> B`    | `inet_contains(A, B)`           |
//!
//! The rewriter only fires when the range-operator rewriter has already
//! passed over the SQL (the two rewriters compose: range `<<`/`>>` fire
//! first via `range_udf::rewrite_range_operators` and consume occurrences
//! where at least one operand looks like a range constructor; our rewriter
//! then handles what's left).

use std::net::IpAddr;
use std::sync::Arc;

use arrow_array::array::BooleanArray;
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use datafusion::prelude::SessionContext;

// ── InetContainedBy: A << B ──────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct InetContainedByUdf {
    signature: Signature,
}

impl datafusion::logical_expr::ScalarUDFImpl for InetContainedByUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "inet_contained_by"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        invoke_inet_op(&args.args, false)
    }
}

// ── InetContains: A >> B ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct InetContainsUdf {
    signature: Signature,
}

impl datafusion::logical_expr::ScalarUDFImpl for InetContainsUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "inet_contains"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        invoke_inet_op(&args.args, true)
    }
}

// ── Shared evaluation logic ──────────────────────────────────────────────────

/// Evaluate `inet_contained_by(a, b)` (if `contains=false`) or
/// `inet_contains(a, b)` (if `contains=true`).
///
/// `inet_contained_by(a, b)` returns true when address `a` falls inside
/// network `b`. `inet_contains(a, b)` is the inverse: network `a` contains
/// address `b`. Semantically:
///
/// - `a << b`  → `inet_contained_by(a, b)` → a's address bits match b's
///   network prefix.
/// - `a >> b`  → `inet_contains(a, b)` → b's address bits match a's network
///   prefix.
///
/// NULL inputs propagate NULL (SQL three-valued logic).
fn invoke_inet_op(args: &[ColumnarValue], contains: bool) -> DFResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "inet_contained_by / inet_contains: expected 2 args, got {}",
            args.len()
        )));
    }
    let n = columnar_len(&args[0], &args[1]);
    let lefts = columnar_to_strings(&args[0], n)?;
    let rights = columnar_to_strings(&args[1], n)?;

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let (lhs, rhs) = (&lefts[i], &rights[i]);
        match (lhs.as_deref(), rhs.as_deref()) {
            (None, _) | (_, None) => out.push(None),
            (Some(l), Some(r)) => {
                let result = if contains {
                    // `a >> b`: a (network) contains b (address/network)
                    inet_contains_impl(l, r)?
                } else {
                    // `a << b`: a (address/network) is contained by b (network)
                    inet_contains_impl(r, l)?
                };
                out.push(Some(result));
            }
        }
    }

    let arr = BooleanArray::from(out);
    Ok(ColumnarValue::Array(Arc::new(arr)))
}

/// Returns `true` when `network` (CIDR or host-with-prefix) contains `addr`.
///
/// Parse rules:
/// - Both arguments accept `host`, `host/prefix` forms.
/// - When `network` has no prefix, it matches only the exact address.
/// - IPv4 and IPv6 are matched independently; mismatched families → false.
fn inet_contains_impl(network: &str, addr: &str) -> DFResult<bool> {
    let (net_ip, net_bits) = parse_ip_prefix(network)?;
    let (addr_ip, addr_bits) = parse_ip_prefix(addr)?;

    // Family mismatch → not contained.
    let result = match (net_ip, addr_ip) {
        (IpAddr::V4(net_v4), IpAddr::V4(addr_v4)) => {
            let net_bits = net_bits.unwrap_or(32);
            let addr_bits = addr_bits.unwrap_or(32);
            // `network` contains `addr` when:
            //   1. net_bits <= addr_bits (network is at least as broad as addr)
            //   2. The host bits of both, masked to net_bits, are equal.
            if net_bits > addr_bits {
                false
            } else {
                let net_n = u32::from(net_v4);
                let addr_n = u32::from(addr_v4);
                let mask = if net_bits == 0 { 0u32 } else { !0u32 << (32 - net_bits) };
                (net_n & mask) == (addr_n & mask)
            }
        }
        (IpAddr::V6(net_v6), IpAddr::V6(addr_v6)) => {
            let net_bits = net_bits.unwrap_or(128);
            let addr_bits = addr_bits.unwrap_or(128);
            if net_bits > addr_bits {
                false
            } else {
                let net_n = u128::from(net_v6);
                let addr_n = u128::from(addr_v6);
                let mask: u128 = if net_bits == 0 {
                    0u128
                } else {
                    !0u128 << (128 - net_bits)
                };
                (net_n & mask) == (addr_n & mask)
            }
        }
        _ => false, // IPv4 vs IPv6 → never contained
    };
    Ok(result)
}

/// Parse `"host"` or `"host/prefix"` into `(IpAddr, Option<prefix_len>)`.
fn parse_ip_prefix(s: &str) -> DFResult<(IpAddr, Option<u8>)> {
    let s = s.trim();
    if let Some(slash) = s.rfind('/') {
        let host = &s[..slash];
        let prefix_str = &s[slash + 1..];
        let ip = host.trim().parse::<IpAddr>().map_err(|_| {
            DataFusionError::Execution(format!(
                "inet_contained_by / inet_contains: invalid IP address {host:?}"
            ))
        })?;
        let prefix: u8 = prefix_str.trim().parse().map_err(|_| {
            DataFusionError::Execution(format!(
                "inet_contained_by / inet_contains: invalid prefix length {prefix_str:?}"
            ))
        })?;
        Ok((ip, Some(prefix)))
    } else {
        let ip = s.parse::<IpAddr>().map_err(|_| {
            DataFusionError::Execution(format!(
                "inet_contained_by / inet_contains: invalid IP address {s:?}"
            ))
        })?;
        Ok((ip, None))
    }
}

// ── ColumnarValue helpers ────────────────────────────────────────────────────

fn columnar_len(a: &ColumnarValue, b: &ColumnarValue) -> usize {
    match (a, b) {
        (ColumnarValue::Array(arr), _) => arr.len(),
        (_, ColumnarValue::Array(arr)) => arr.len(),
        _ => 1,
    }
}

fn columnar_to_strings(cv: &ColumnarValue, n: usize) -> DFResult<Vec<Option<String>>> {
    use arrow_array::{Array, StringArray};
    use datafusion::arrow::compute::cast;
    match cv {
        ColumnarValue::Scalar(sv) => {
            let s = match sv {
                datafusion::scalar::ScalarValue::Utf8(v)
                | datafusion::scalar::ScalarValue::LargeUtf8(v) => v.clone(),
                _ => None,
            };
            Ok(vec![s; n])
        }
        ColumnarValue::Array(arr) => {
            let arr = match arr.data_type() {
                DataType::Utf8 => arr.clone(),
                _ => cast(arr.as_ref(), &DataType::Utf8).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "inet_contained_by / inet_contains: cannot cast argument to text: {e}"
                    ))
                })?,
            };
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "inet_contained_by / inet_contains: argument must be text".into(),
                    )
                })?;
            let mut out = Vec::with_capacity(n);
            for i in 0..str_arr.len() {
                if str_arr.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(str_arr.value(i).to_string()));
                }
            }
            Ok(out)
        }
    }
}

// ── Registration ─────────────────────────────────────────────────────────────

/// Register `inet_contained_by` and `inet_contains` into `ctx`.
/// Called from `session::build_stateless_udf_cache`.
pub(crate) fn register_inet_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(InetContainedByUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(InetContainsUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}

// ── Operator pre-pass rewriter ───────────────────────────────────────────────

/// Rewrite PG network infix operators to UDF calls before sqlparser sees the
/// SQL.
///
/// | PG operator | Rewrites to                   |
/// |-------------|-------------------------------|
/// | `A << B`    | `inet_contained_by(A, B)`     |
/// | `A >> B`    | `inet_contains(A, B)`         |
///
/// This rewriter runs AFTER `range_udf::rewrite_range_operators` in
/// `executor.rs`, which already consumed any `<<`/`>>` whose operands look
/// like range constructors. What remains here are network-address operands
/// (column refs, string literals, cast expressions).
///
/// The heuristic to distinguish from bitwise shift (`x << 3`):
/// - At least one operand must look like a network address: a single-quoted
///   string literal (IP address / CIDR), a `::inet` / `::cidr` cast, or a
///   known network-column reference heuristic.
/// - A purely numeric RHS (`<<` 3) is left alone.
///
/// Best-effort: mis-detection is extremely rare for real SQL workloads
/// (bitwise shift on inet columns is not a PG pattern).
pub(crate) fn rewrite_inet_operators(sql: &str) -> String {
    let mut s = sql.to_string();
    // Process `>>` before `<<` to avoid ambiguity when both appear (rare).
    s = rewrite_inet_op_once(s, ">>", "inet_contains");
    s = rewrite_inet_op_once(s, "<<", "inet_contained_by");
    s
}

fn rewrite_inet_op_once(sql: String, op: &str, func: &str) -> String {
    let mut s = sql;
    let mut search_from = 0usize;
    loop {
        let bytes = s.as_bytes();
        let Some(rel) = find_op_outside_strings_from(&s[search_from..], op) else {
            break;
        };
        let op_start = search_from + rel;
        let op_end = op_start + op.len();

        let (lhs_start, lhs_end) = inet_extract_left(&s, op_start);
        let (rhs_start, rhs_end) = inet_extract_right(&s, op_end);
        let lhs = s[lhs_start..lhs_end].trim().to_string();
        let rhs = s[rhs_start..rhs_end].trim().to_string();

        // Only rewrite if neither operand looks like a plain integer. The PG
        // network operators `<<` / `>>` never take a bare integer literal as
        // an operand (operands are IP/CIDR strings, casts, or column refs),
        // so a plain-integer side means this is a bitwise shift (`x << 3`)
        // which must be left untouched.
        if is_plain_integer(&rhs) || is_plain_integer(&lhs) {
            search_from = op_end;
            continue;
        }

        // Skip if either operand is empty (unresolvable parse position).
        if lhs.is_empty() || rhs.is_empty() {
            search_from = op_end;
            continue;
        }

        let replacement = format!("{func}({lhs}, {rhs})");
        // Safety: lhs_start / rhs_end are valid byte offsets into `s`.
        let _ = &bytes[lhs_start..rhs_end]; // bounds check assertion
        s.replace_range(lhs_start..rhs_end, &replacement);
        search_from = lhs_start + replacement.len();
    }
    s
}

/// Scan `s` for the first occurrence of `op` (e.g. `"<<"`) that is
/// outside a single-quoted string literal and is not part of a longer op
/// (`<<` inside `<<=`, though PG SQL doesn't have `<<=`, it's defensive).
fn find_op_outside_strings_from(s: &str, op: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let op_bytes = op.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        // Skip single-quoted string literals.
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Skip `--` line comments.
        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Check for op match.
        if i + op_bytes.len() <= bytes.len() && &bytes[i..i + op_bytes.len()] == op_bytes {
            // For `<<`: ensure it's not `<<=` or `<<<`.
            // For `>>`: ensure it's not `>>=` or `>>>`.
            let after = i + op_bytes.len();
            if after < bytes.len()
                && (bytes[after] == b'=' || bytes[after] == op_bytes[op_bytes.len() - 1])
            {
                i += 1;
                continue;
            }
            // Also ensure it's not preceded by `<` (making `<<<`) or `>`.
            if i > 0 && bytes[i - 1] == op_bytes[0] {
                i += 1;
                continue;
            }
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Walk left from `end` to extract the LHS operand (identifier, quoted string,
/// function call, or parenthesised expression).
fn inet_extract_left(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    // Skip trailing whitespace before operator.
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    if i == 0 {
        return (0, operand_end);
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
        // Capture function name before `(`.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else if last == b'\'' {
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'\'' {
                break;
            }
        }
    } else {
        // Identifier / column ref (possibly schema-qualified).
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.'
                || bytes[i - 1] == b'"')
        {
            i -= 1;
        }
    }
    (i, operand_end)
}

/// Walk right from `start` (first byte after operator whitespace) to extract
/// the RHS operand.
fn inet_extract_right(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    if i >= bytes.len() {
        return (operand_start, operand_start);
    }
    if bytes[i] == b'\'' {
        i += 1;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                i += 1;
                break;
            }
            i += 1;
        }
        // Absorb an optional `::inet` / `::cidr` cast suffix.
        if i + 2 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            i += 2;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
        }
    } else if bytes[i] == b'(' {
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
    } else {
        // Identifier / number (column ref, nextval, etc.).
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric()
                || bytes[i] == b'_'
                || bytes[i] == b'.'
                || bytes[i] == b'"')
        {
            i += 1;
        }
        // Absorb optional cast `::inet` / `::cidr` / `::text`.
        if i + 2 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            i += 2;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
        }
    }
    (operand_start, i)
}

/// Return `true` when `expr` is clearly just a decimal integer literal (e.g.
/// `3`, `128`). These are the RHS of bitwise-shift expressions that we must
/// NOT rewrite.
fn is_plain_integer(expr: &str) -> bool {
    let t = expr.trim();
    !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit())
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── inet_contains_impl unit tests ────────────────────────────────────

    /// 10.0.0.0/8 contains 10.1.2.3
    #[test]
    fn ipv4_network_contains_host() {
        assert!(inet_contains_impl("10.0.0.0/8", "10.1.2.3").unwrap());
    }

    /// 10.0.0.0/8 does NOT contain 192.168.1.1
    #[test]
    fn ipv4_network_does_not_contain_host() {
        assert!(!inet_contains_impl("10.0.0.0/8", "192.168.1.1").unwrap());
    }

    /// 192.168.1.0/24 contains 192.168.1.100
    #[test]
    fn ipv4_slash24_contains() {
        assert!(inet_contains_impl("192.168.1.0/24", "192.168.1.100").unwrap());
    }

    /// 192.168.1.0/24 does NOT contain 192.168.2.1
    #[test]
    fn ipv4_slash24_does_not_contain_other_subnet() {
        assert!(!inet_contains_impl("192.168.1.0/24", "192.168.2.1").unwrap());
    }

    /// Network contains itself.
    #[test]
    fn ipv4_network_contains_itself() {
        assert!(inet_contains_impl("10.0.0.0/8", "10.0.0.0/8").unwrap());
    }

    /// /32 contains only the exact host.
    #[test]
    fn ipv4_slash32_exact_match() {
        assert!(inet_contains_impl("192.168.1.5/32", "192.168.1.5").unwrap());
        assert!(!inet_contains_impl("192.168.1.5/32", "192.168.1.6").unwrap());
    }

    /// IPv6: 2001:db8::/32 contains 2001:db8::1
    #[test]
    fn ipv6_network_contains_host() {
        assert!(inet_contains_impl("2001:db8::/32", "2001:db8::1").unwrap());
    }

    /// IPv6: 2001:db8::/32 does NOT contain 2001:dc8::1
    #[test]
    fn ipv6_network_does_not_contain_host() {
        assert!(!inet_contains_impl("2001:db8::/32", "2001:dc8::1").unwrap());
    }

    /// IPv4 vs IPv6 family mismatch → false.
    #[test]
    fn ipv4_v6_family_mismatch() {
        assert!(!inet_contains_impl("10.0.0.0/8", "::1").unwrap());
        assert!(!inet_contains_impl("::1/128", "10.0.0.1").unwrap());
    }

    // ── Text roundtrip test: INET / CIDR stored as Utf8 text ─────────────

    /// Verify that the text representation stored in the column (the literal
    /// exactly as the user wrote it) round-trips through the UDF without
    /// mutation. The storage layer is type-agnostic; the text IS the value.
    #[test]
    fn inet_text_is_preserved_as_stored() {
        let addr = "192.168.1.1";
        let net = "192.168.0.0/16";
        // inet_contained_by(addr, net) should be true and not alter inputs.
        assert!(inet_contains_impl(net, addr).unwrap());
    }

    // ── Operator rewriter tests ───────────────────────────────────────────

    #[test]
    fn rewrite_contained_by_literal() {
        let sql = "SELECT * FROM t WHERE ip << '10.0.0.0/8'";
        let out = rewrite_inet_operators(sql);
        assert!(
            out.contains("inet_contained_by"),
            "expected rewrite, got: {out}"
        );
        assert!(!out.contains(" << "), "op should be gone, got: {out}");
    }

    #[test]
    fn rewrite_contains_literal() {
        let sql = "SELECT * FROM t WHERE net >> '10.5.0.1'";
        let out = rewrite_inet_operators(sql);
        assert!(out.contains("inet_contains"), "expected rewrite, got: {out}");
        assert!(!out.contains(" >> "), "op should be gone, got: {out}");
    }

    #[test]
    fn rewrite_with_cast_suffix() {
        let sql = "SELECT '10.0.0.0/8'::inet >> '10.1.2.3'::inet";
        let out = rewrite_inet_operators(sql);
        assert!(out.contains("inet_contains"), "expected rewrite, got: {out}");
    }

    #[test]
    fn integer_bit_shift_not_rewritten() {
        // `x << 3` is bitwise shift on an integer column — must NOT be rewritten.
        let sql = "SELECT x << 3 FROM t";
        let out = rewrite_inet_operators(sql);
        // Should be unchanged (or at worst contain the original operator form).
        assert!(!out.contains("inet_contained_by"), "should not rewrite int shift: {out}");
    }

    #[test]
    fn string_inside_literal_not_rewritten() {
        // Operator characters inside a string literal must not trigger rewrite.
        let sql = "SELECT * FROM t WHERE comment = 'A << B'";
        let out = rewrite_inet_operators(sql);
        assert_eq!(out, sql, "string literal contents must not be rewritten");
    }

    #[test]
    fn both_operators_in_one_query() {
        let sql = "SELECT (net >> '10.0.0.1') AND (ip << '10.0.0.0/8') FROM t";
        let out = rewrite_inet_operators(sql);
        assert!(out.contains("inet_contains"), "got: {out}");
        assert!(out.contains("inet_contained_by"), "got: {out}");
    }
}
