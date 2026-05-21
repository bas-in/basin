//! Range type utilities shared across Basin crates.
//!
//! Basin represents range columns (`int4range`, `int8range`, `numrange`,
//! `tsrange`, `tstzrange`, `daterange`) as Arrow `Utf8` fields with a
//! `BASIN_TYPE=<SUBTYPE>` metadata marker (defined in `basin-engine::types`).
//! The physical storage format is a compact JSON string:
//!
//! ```text
//! {"l":<lower>,"u":<upper>,"li":<bool>,"ui":<bool>}
//! ```
//!
//! where `li`/`ui` are inclusive-lower / inclusive-upper flags, and a
//! JSON `null` value on `l` or `u` means negative/positive infinity.
//! An empty range is represented as `{"empty":true}`.
//!
//! ## Canonical form
//!
//! PostgreSQL canonicalizes ranges over *discrete* types (`int4range`,
//! `int8range`, `daterange`) to the *half-open* form `[lo, hi)` by
//! adjusting the upper bound:
//!   - `[1,9]` → `[1,10)` (inclusive upper → next integer)
//!   - `(0,9]` → `[1,10)` (exclusive lower → lower+1, then inclusive upper)
//!   - `(0,10)` → `[1,10)` (both exclusive → normalize lower)
//!
//! Continuous types (`numrange`, `tsrange`, `tstzrange`) are NOT
//! canonicalized; they preserve the user-supplied inclusivity flags.
//!
//! ## Equality semantics
//!
//! Two range values are equal when their canonical forms produce the same
//! lower bound, upper bound, and inclusivity flags. For discrete ranges
//! this means `[1,9]::int4range = '[1,10)'::int4range` is `true`.

use std::cmp::Ordering;
use std::fmt;

// ── Sub-type tag ──────────────────────────────────────────────────────────────

/// The element sub-type of a range column. Controls canonicalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSubtype {
    /// `int4range` — 32-bit integer bounds (discrete).
    Int4,
    /// `int8range` — 64-bit integer bounds (discrete).
    Int8,
    /// `numrange` — arbitrary-precision numeric bounds (continuous).
    Num,
    /// `tsrange` — timestamp without time zone (continuous).
    Ts,
    /// `tstzrange` — timestamp with time zone (continuous).
    Tstz,
    /// `daterange` — calendar date (discrete, step = 1 day).
    Date,
}

impl RangeSubtype {
    /// Whether this sub-type has a discrete (integer) element domain.
    /// Discrete ranges are canonicalized to the half-open `[lo, hi)` form.
    pub fn is_discrete(self) -> bool {
        matches!(self, RangeSubtype::Int4 | RangeSubtype::Int8 | RangeSubtype::Date)
    }
}

// ── Bound values ─────────────────────────────────────────────────────────────

/// A single range bound — either a finite value (as a string, preserving the
/// original text) or positive/negative infinity.
///
/// Internally the engine compares bounds numerically (via `bound_as_f64`), so
/// storing text is lossless for the round-trip while still supporting the
/// comparison operations the operator UDFs need.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    /// Negative (lower) or positive (upper) infinity — no limit.
    Infinite,
    /// A finite bound value, stored as the original text.
    Finite(String),
}

impl Bound {
    /// Return the numeric value of a finite bound for comparison purposes.
    /// Returns `None` for infinite bounds.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Bound::Infinite => None,
            Bound::Finite(s) => s.parse::<f64>().ok(),
        }
    }

    /// Return `true` iff this is an infinite bound.
    pub fn is_infinite(&self) -> bool {
        matches!(self, Bound::Infinite)
    }
}

// ── Range value ───────────────────────────────────────────────────────────────

/// Parsed representation of a Basin range value.
///
/// This struct is the canonical in-memory form used by operator UDFs and
/// the canonicalization path. It owns its bound strings so it can be passed
/// across async boundaries without lifetime constraints.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeValue {
    /// Lower bound (or infinite).
    pub lower: Bound,
    /// Upper bound (or infinite).
    pub upper: Bound,
    /// Whether the lower bound is inclusive.
    pub lower_inc: bool,
    /// Whether the upper bound is inclusive.
    pub upper_inc: bool,
    /// Whether this is the empty range.
    pub empty: bool,
}

impl RangeValue {
    /// Construct the empty range.
    pub fn empty() -> Self {
        RangeValue {
            lower: Bound::Infinite,
            upper: Bound::Infinite,
            lower_inc: false,
            upper_inc: false,
            empty: true,
        }
    }

    /// Construct a finite range from its components.
    pub fn new(lower: Bound, upper: Bound, lower_inc: bool, upper_inc: bool) -> Self {
        RangeValue { lower, upper, lower_inc, upper_inc, empty: false }
    }

    /// Serialize to the Basin JSON storage format.
    ///
    /// Format: `{"l":<val>,"u":<val>,"li":<bool>,"ui":<bool>}` where `<val>`
    /// is a JSON number for numeric bounds or `null` for infinite bounds.
    /// The empty range serializes as `{"empty":true}`.
    pub fn to_json_string(&self) -> String {
        if self.empty {
            return r#"{"empty":true}"#.to_string();
        }
        let l_val = bound_to_json_value(&self.lower);
        let u_val = bound_to_json_value(&self.upper);
        format!(
            r#"{{"l":{l_val},"u":{u_val},"li":{li},"ui":{ui}}}"#,
            li = self.lower_inc,
            ui = self.upper_inc,
        )
    }

    /// Parse from the Basin JSON storage format. Returns `None` on parse failure.
    pub fn from_json_str(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed == "empty" {
            return Some(RangeValue::empty());
        }
        let v: serde_json::Value = serde_json::from_str(trimmed).ok()?;
        if v.get("empty").and_then(|e| e.as_bool()).unwrap_or(false) {
            return Some(RangeValue::empty());
        }
        let lower = json_value_to_bound(v.get("l")?);
        let upper = json_value_to_bound(v.get("u")?);
        let lower_inc = v.get("li").and_then(|b| b.as_bool()).unwrap_or(true);
        let upper_inc = v.get("ui").and_then(|b| b.as_bool()).unwrap_or(false);
        Some(RangeValue { lower, upper, lower_inc, upper_inc, empty: false })
    }

    /// Parse from the PG text representation `[lo,hi)` / `(lo,hi]` / `empty`.
    ///
    /// Supports all four bracket combinations:
    ///   `[lo,hi)`, `[lo,hi]`, `(lo,hi)`, `(lo,hi]`
    /// Infinite bounds are expressed as empty strings:
    ///   `(,10)` — lower infinite
    ///   `[1,)` — upper infinite
    ///   `(,)`  — all-infinite (covers everything)
    pub fn from_pg_text(s: &str) -> Option<Self> {
        let trimmed = s.trim();
        if trimmed.eq_ignore_ascii_case("empty") {
            return Some(RangeValue::empty());
        }
        let bytes = trimmed.as_bytes();
        if bytes.len() < 3 {
            return None;
        }
        let lower_inc = match bytes[0] {
            b'[' => true,
            b'(' => false,
            _ => return None,
        };
        let upper_inc = match bytes[bytes.len() - 1] {
            b']' => true,
            b')' => false,
            _ => return None,
        };
        // Find the comma separating lower and upper bounds.
        let inner = &trimmed[1..trimmed.len() - 1];
        // The comma might be inside a quoted string (for ts/tstz ranges).
        // We do a simple find of the *last* comma at depth 0.
        let comma_pos = find_bound_comma(inner)?;
        let lo_str = inner[..comma_pos].trim();
        let hi_str = inner[comma_pos + 1..].trim();
        let lower = if lo_str.is_empty() {
            Bound::Infinite
        } else {
            Bound::Finite(lo_str.to_string())
        };
        let upper = if hi_str.is_empty() {
            Bound::Infinite
        } else {
            Bound::Finite(hi_str.to_string())
        };
        Some(RangeValue { lower, upper, lower_inc, upper_inc, empty: false })
    }

    /// Canonicalize this range for a given sub-type.
    ///
    /// For discrete subtypes (`Int4`, `Int8`, `Date`) this normalizes the
    /// range to the half-open `[lo, hi)` form (adjusting bounds by 1 step
    /// when necessary). For continuous subtypes the value is returned unchanged.
    ///
    /// Returns the empty range if normalization produces an empty range.
    pub fn canonicalize(mut self, subtype: RangeSubtype) -> Self {
        if self.empty {
            return self;
        }
        if !subtype.is_discrete() {
            return self;
        }
        match subtype {
            RangeSubtype::Date => self.canonicalize_date(),
            _ => self.canonicalize_integer(),
        }
    }

    /// Canonicalize integer (Int4/Int8) ranges to the half-open `[lo, hi)` form.
    fn canonicalize_integer(mut self) -> Self {
        // Normalize lower bound to inclusive: (lo, ...) → [lo+1, ...)
        if !self.lower_inc {
            if let Bound::Finite(ref lo) = self.lower.clone() {
                if let Some(n) = parse_discrete_i64(lo) {
                    self.lower = Bound::Finite((n + 1).to_string());
                    self.lower_inc = true;
                }
            }
        }
        // Normalize upper bound to exclusive: (..., hi] → [..., hi+1)
        if self.upper_inc {
            if let Bound::Finite(ref hi) = self.upper.clone() {
                if let Some(n) = parse_discrete_i64(hi) {
                    self.upper = Bound::Finite((n + 1).to_string());
                    self.upper_inc = false;
                }
            }
        }
        // Check if the range became empty.
        if let (Bound::Finite(ref lo), Bound::Finite(ref hi)) =
            (&self.lower.clone(), &self.upper.clone())
        {
            if let (Some(l), Some(h)) = (parse_discrete_i64(lo), parse_discrete_i64(hi)) {
                if l >= h {
                    return RangeValue::empty();
                }
            }
        }
        self
    }

    /// Canonicalize daterange bounds to the half-open `[lo, hi)` form.
    ///
    /// Date strings are in ISO 8601 form `YYYY-MM-DD`. We increment/decrement
    /// by one calendar day. For simplicity we use an embedded day-arithmetic
    /// helper that does not require any external date library.
    fn canonicalize_date(mut self) -> Self {
        // Normalize exclusive lower to inclusive: (lo_date, ...) → [lo_date+1day, ...)
        if !self.lower_inc {
            if let Bound::Finite(ref lo) = self.lower.clone() {
                if let Some(next) = date_add_one_day(lo.trim()) {
                    self.lower = Bound::Finite(next);
                    self.lower_inc = true;
                }
            }
        }
        // Normalize inclusive upper to exclusive: (..., hi_date] → [..., hi_date+1day)
        if self.upper_inc {
            if let Bound::Finite(ref hi) = self.upper.clone() {
                if let Some(next) = date_add_one_day(hi.trim()) {
                    self.upper = Bound::Finite(next);
                    self.upper_inc = false;
                }
            }
        }
        // Check emptiness: if lo >= hi as dates.
        if let (Bound::Finite(ref lo), Bound::Finite(ref hi)) =
            (&self.lower.clone(), &self.upper.clone())
        {
            if lo.trim() >= hi.trim() {
                return RangeValue::empty();
            }
        }
        self
    }

    /// Semantic equality: two ranges are equal when their canonical bounds
    /// compare equal numerically (for finite bounds) and their inclusivity
    /// flags match.
    ///
    /// For discrete types the caller should canonicalize first; this method
    /// compares the values as-is.
    pub fn semantic_eq(&self, other: &Self) -> bool {
        if self.empty && other.empty {
            return true;
        }
        if self.empty != other.empty {
            return false;
        }
        self.lower_inc == other.lower_inc
            && self.upper_inc == other.upper_inc
            && bounds_eq(&self.lower, &other.lower)
            && bounds_eq(&self.upper, &other.upper)
    }
}

impl fmt::Display for RangeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.empty {
            return write!(f, "empty");
        }
        let lo_bracket = if self.lower_inc { '[' } else { '(' };
        let hi_bracket = if self.upper_inc { ']' } else { ')' };
        let lo = match &self.lower {
            Bound::Infinite => String::new(),
            Bound::Finite(s) => s.clone(),
        };
        let hi = match &self.upper {
            Bound::Infinite => String::new(),
            Bound::Finite(s) => s.clone(),
        };
        write!(f, "{lo_bracket}{lo},{hi}{hi_bracket}")
    }
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

/// Parse a discrete (integer) bound from a string. Returns `None` if the
/// string is not a valid integer (e.g. it's a timestamp string for tsrange).
pub fn parse_discrete_i64(s: &str) -> Option<i64> {
    // Strip optional surrounding quotes (some date formats use them).
    let s = s.trim_matches('"').trim();
    s.parse::<i64>().ok()
}

/// Increment an ISO 8601 date string (`YYYY-MM-DD`) by one calendar day.
///
/// Returns the next calendar day as a `String` in `YYYY-MM-DD` form, or
/// `None` if the input cannot be parsed as a valid date.
///
/// This is a minimal self-contained implementation that handles the month /
/// year boundary rollover without requiring a date library in `basin-common`.
pub fn date_add_one_day(s: &str) -> Option<String> {
    // Accept `YYYY-MM-DD` or `YYYY-M-D` variants.
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;

    if month < 1 || month > 12 || day < 1 {
        return None;
    }

    // Days in each month (non-leap year).
    let days_in_month = |y: i32, m: u32| -> u32 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                    29
                } else {
                    28
                }
            }
            _ => 0,
        }
    };

    let dim = days_in_month(year, month);
    if day > dim {
        return None;
    }

    let (ny, nm, nd) = if day < dim {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    };

    Some(format!("{ny:04}-{nm:02}-{nd:02}"))
}

fn find_bound_comma(inner: &str) -> Option<usize> {
    // For range text like `1,10` or `2024-01-01,2024-01-31` or
    // `2024-01-01 00:00:00,2024-01-02 00:00:00`: find the first unquoted comma
    // that is at nesting depth 0 (no parentheses, no brackets).
    let mut depth = 0i32;
    let mut in_quote = false;
    for (i, ch) in inner.char_indices() {
        match ch {
            '\'' | '"' => in_quote = !in_quote,
            '(' | '[' if !in_quote => depth += 1,
            ')' | ']' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

fn bound_to_json_value(b: &Bound) -> String {
    match b {
        Bound::Infinite => "null".to_string(),
        Bound::Finite(s) => {
            // Try integer first, then float, then quote as string.
            if let Ok(n) = s.parse::<i64>() {
                return n.to_string();
            }
            if let Ok(f) = s.parse::<f64>() {
                if let Some(jn) = serde_json::Number::from_f64(f) {
                    return jn.to_string();
                }
            }
            // Fall back to a JSON string (for timestamps, dates).
            serde_json::Value::String(s.clone()).to_string()
        }
    }
}

fn json_value_to_bound(v: &serde_json::Value) -> Bound {
    match v {
        serde_json::Value::Null => Bound::Infinite,
        serde_json::Value::Number(n) => Bound::Finite(n.to_string()),
        serde_json::Value::String(s) => Bound::Finite(s.clone()),
        other => Bound::Finite(other.to_string()),
    }
}

fn bounds_eq(a: &Bound, b: &Bound) -> bool {
    match (a, b) {
        (Bound::Infinite, Bound::Infinite) => true,
        (Bound::Finite(sa), Bound::Finite(sb)) => {
            // Try numeric comparison first; fall back to string equality.
            match (sa.parse::<f64>(), sb.parse::<f64>()) {
                (Ok(fa), Ok(fb)) => (fa - fb).abs() < 1e-12,
                _ => sa == sb,
            }
        }
        _ => false,
    }
}

/// Compare two bound values numerically. Used by operator UDFs.
pub fn bound_cmp(a: &Bound, b: &Bound) -> Option<Ordering> {
    match (a, b) {
        (Bound::Infinite, Bound::Infinite) => Some(Ordering::Equal),
        (Bound::Infinite, _) => Some(Ordering::Less), // -inf < anything
        (_, Bound::Infinite) => Some(Ordering::Greater),
        (Bound::Finite(sa), Bound::Finite(sb)) => {
            let fa: f64 = sa.parse().ok()?;
            let fb: f64 = sb.parse().ok()?;
            fa.partial_cmp(&fb)
        }
    }
}

// ── Interval index entry ──────────────────────────────────────────────────────

/// A half-open interval `[lo, hi)` for use in the interval index.
/// Both bounds are finite `f64` values; infinite-bound ranges are excluded
/// from the interval index (they always match, so they're not prunable).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInterval {
    pub lo: f64,
    pub hi: f64,
}

impl IndexInterval {
    /// Attempt to create an `IndexInterval` from a `RangeValue`.
    /// Returns `None` for empty ranges, infinite-bound ranges, or
    /// ranges whose bounds are not parseable as f64.
    pub fn from_range(r: &RangeValue) -> Option<Self> {
        if r.empty {
            return None;
        }
        let lo = match &r.lower {
            Bound::Finite(s) => {
                let mut v: f64 = s.parse().ok()?;
                if !r.lower_inc {
                    v = v.next_up(); // exclusive lower → step just inside
                }
                v
            }
            Bound::Infinite => return None,
        };
        let hi = match &r.upper {
            Bound::Finite(s) => {
                let mut v: f64 = s.parse().ok()?;
                if r.upper_inc {
                    v = v.next_up(); // inclusive upper → step just outside
                }
                v
            }
            Bound::Infinite => return None,
        };
        if lo >= hi {
            return None;
        }
        Some(IndexInterval { lo, hi })
    }

    /// Return `true` if this interval contains the point `p`
    /// (i.e., `lo <= p < hi`).
    pub fn contains_point(&self, p: f64) -> bool {
        p >= self.lo && p < self.hi
    }

    /// Return `true` if this interval overlaps `other`
    /// (i.e., `lo < other.hi && other.lo < hi`).
    pub fn overlaps(&self, other: &Self) -> bool {
        self.lo < other.hi && other.lo < self.hi
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── from_pg_text ──────────────────────────────────────────────────────────

    #[test]
    fn parse_half_open() {
        let r = RangeValue::from_pg_text("[1,10)").unwrap();
        assert_eq!(r.lower, Bound::Finite("1".to_string()));
        assert_eq!(r.upper, Bound::Finite("10".to_string()));
        assert!(r.lower_inc);
        assert!(!r.upper_inc);
        assert!(!r.empty);
    }

    #[test]
    fn parse_closed() {
        let r = RangeValue::from_pg_text("[1,9]").unwrap();
        assert!(r.lower_inc);
        assert!(r.upper_inc);
        assert_eq!(r.lower, Bound::Finite("1".to_string()));
        assert_eq!(r.upper, Bound::Finite("9".to_string()));
    }

    #[test]
    fn parse_empty() {
        let r = RangeValue::from_pg_text("empty").unwrap();
        assert!(r.empty);
    }

    #[test]
    fn parse_infinite_lower() {
        let r = RangeValue::from_pg_text("(,10)").unwrap();
        assert_eq!(r.lower, Bound::Infinite);
        assert_eq!(r.upper, Bound::Finite("10".to_string()));
    }

    #[test]
    fn parse_infinite_upper() {
        let r = RangeValue::from_pg_text("[5,)").unwrap();
        assert_eq!(r.lower, Bound::Finite("5".to_string()));
        assert_eq!(r.upper, Bound::Infinite);
    }

    // ── canonicalize ──────────────────────────────────────────────────────────

    #[test]
    fn canonicalize_int4_closed_becomes_half_open() {
        // [1,9] → [1,10)
        let r = RangeValue::from_pg_text("[1,9]").unwrap();
        let c = r.canonicalize(RangeSubtype::Int4);
        assert!(c.lower_inc);
        assert!(!c.upper_inc);
        assert_eq!(c.lower, Bound::Finite("1".to_string()));
        assert_eq!(c.upper, Bound::Finite("10".to_string()));
    }

    #[test]
    fn canonicalize_int4_exclusive_lower() {
        // (0,10) → [1,10)
        let r = RangeValue::from_pg_text("(0,10)").unwrap();
        let c = r.canonicalize(RangeSubtype::Int4);
        assert!(c.lower_inc);
        assert!(!c.upper_inc);
        assert_eq!(c.lower, Bound::Finite("1".to_string()));
        assert_eq!(c.upper, Bound::Finite("10".to_string()));
    }

    #[test]
    fn canonicalize_int4_already_canonical() {
        // [1,10) stays [1,10)
        let r = RangeValue::from_pg_text("[1,10)").unwrap();
        let c = r.canonicalize(RangeSubtype::Int4);
        assert_eq!(c.lower, Bound::Finite("1".to_string()));
        assert_eq!(c.upper, Bound::Finite("10".to_string()));
        assert!(c.lower_inc);
        assert!(!c.upper_inc);
    }

    #[test]
    fn canonicalize_continuous_unchanged() {
        // numrange [1.5,2.5) stays [1.5,2.5)
        let r = RangeValue::from_pg_text("[1.5,2.5)").unwrap();
        let c = r.canonicalize(RangeSubtype::Num);
        assert!(c.lower_inc);
        assert!(!c.upper_inc);
        assert_eq!(c.lower, Bound::Finite("1.5".to_string()));
        assert_eq!(c.upper, Bound::Finite("2.5".to_string()));
    }

    // ── semantic_eq ───────────────────────────────────────────────────────────

    #[test]
    fn semantic_eq_after_canonicalize() {
        // [1,9]::int4range = [1,10)::int4range after canonicalization.
        let a = RangeValue::from_pg_text("[1,9]").unwrap().canonicalize(RangeSubtype::Int4);
        let b = RangeValue::from_pg_text("[1,10)").unwrap().canonicalize(RangeSubtype::Int4);
        assert!(a.semantic_eq(&b));
    }

    #[test]
    fn semantic_eq_different_ranges() {
        let a = RangeValue::from_pg_text("[1,5)").unwrap().canonicalize(RangeSubtype::Int4);
        let b = RangeValue::from_pg_text("[1,10)").unwrap().canonicalize(RangeSubtype::Int4);
        assert!(!a.semantic_eq(&b));
    }

    // ── to_json_string / from_json_str roundtrip ──────────────────────────────

    #[test]
    fn json_roundtrip_finite() {
        let r = RangeValue::new(
            Bound::Finite("1".to_string()),
            Bound::Finite("10".to_string()),
            true,
            false,
        );
        let json = r.to_json_string();
        let r2 = RangeValue::from_json_str(&json).unwrap();
        assert!(r.semantic_eq(&r2));
    }

    #[test]
    fn json_roundtrip_empty() {
        let r = RangeValue::empty();
        let json = r.to_json_string();
        let r2 = RangeValue::from_json_str(&json).unwrap();
        assert!(r2.empty);
    }

    // ── IndexInterval ─────────────────────────────────────────────────────────

    #[test]
    fn interval_contains_point() {
        let iv = IndexInterval { lo: 1.0, hi: 10.0 };
        assert!(iv.contains_point(1.0));
        assert!(iv.contains_point(5.0));
        assert!(!iv.contains_point(10.0)); // exclusive upper
        assert!(!iv.contains_point(0.0));
    }

    #[test]
    fn interval_overlaps() {
        let a = IndexInterval { lo: 1.0, hi: 5.0 };
        let b = IndexInterval { lo: 3.0, hi: 8.0 };
        let c = IndexInterval { lo: 5.0, hi: 10.0 };
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c)); // [1,5) and [5,10) are adjacent — no overlap
    }

    #[test]
    fn interval_from_range_half_open() {
        let r = RangeValue::from_pg_text("[1,10)").unwrap();
        let iv = IndexInterval::from_range(&r).unwrap();
        assert!((iv.lo - 1.0).abs() < 1e-12);
        assert!((iv.hi - 10.0).abs() < 1e-12);
    }
}
