//! Type modifiers — the `(n)` in `varchar(n)`, the `(p,s)` in `numeric(p,s)`,
//! the `(p)` in `timestamp(p)`.
//!
//! A typmod is a single `i32` whose meaning is **per type**: there is no
//! generic decoder. `-1` universally means "unspecified". Everything else is
//! encoded by the type's own rules, and this module reproduces Postgres's
//! encodings exactly, because the value travels on the wire in
//! `RowDescription` and clients decode it with those same rules.
//!
//! Basin does not currently enforce or report typmods
//! (`docs/migration/df-removal/12-pg-type-fidelity.md`), which is why
//! introspecting a Basin `varchar(10)` reports no length. This module is the
//! first half of fixing that; enforcement on write is the second.

/// Postgres's `VARHDRSZ` — the 4-byte varlena header length, which is added
/// into every varlena typmod so that a typmod of `-1` (unspecified) can never
/// collide with a legitimately small length.
const VARHDRSZ: i32 = 4;

/// The sentinel for "no modifier specified".
pub const UNSPECIFIED: i32 = -1;

// ─── varchar(n) / char(n) ───────────────────────────────────────────────────

/// Encode `varchar(n)` / `bpchar(n)`, where `n` is a length in **characters**.
///
/// Postgres stores `n + VARHDRSZ`. Returns [`UNSPECIFIED`] for a non-positive
/// `n`, matching Postgres's rejection of `varchar(0)`.
pub fn encode_varlena_len(n: i32) -> i32 {
    if n <= 0 {
        UNSPECIFIED
    } else {
        n + VARHDRSZ
    }
}

/// Decode a `varchar` / `bpchar` typmod back to its declared length.
pub fn decode_varlena_len(typmod: i32) -> Option<i32> {
    if typmod < VARHDRSZ {
        None
    } else {
        Some(typmod - VARHDRSZ)
    }
}

// ─── numeric(p, s) ──────────────────────────────────────────────────────────

/// Maximum `NUMERIC` precision Postgres accepts in a type modifier.
///
/// Note this bounds the *declared* precision only. An undecorated `NUMERIC`
/// column is unconstrained and holds values far larger — which is exactly where
/// Basin's `Decimal128` physical representation cannot follow. See
/// [`crate::physical`] and doc 12.
pub const NUMERIC_MAX_PRECISION: i32 = 1000;

/// Encode `numeric(precision, scale)`.
///
/// Reproduces Postgres's `make_numeric_typmod`:
/// `((precision << 16) | (scale & 0x7ff)) + VARHDRSZ`.
///
/// The `& 0x7ff` is not a truncation bug — Postgres 15 and later allow a
/// negative scale (down to -1000), and stores it in 11 bits recovered by the
/// bias trick in [`decode_numeric`]. Encoding and decoding must agree on it.
pub fn encode_numeric(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
}

/// Decode a `numeric` typmod into `(precision, scale)`.
///
/// Reproduces Postgres's macros:
/// - precision: `(((t) - VARHDRSZ) >> 16) & 0xffff`
/// - scale:     `((((t) - VARHDRSZ) & 0x7ff) ^ 1024) - 1024`
///
/// The `^ 1024 - 1024` recovers the sign of an 11-bit scale.
pub fn decode_numeric(typmod: i32) -> Option<(i32, i32)> {
    if typmod < VARHDRSZ {
        return None;
    }
    let t = typmod - VARHDRSZ;
    let precision = (t >> 16) & 0xffff;
    let scale = ((t & 0x7ff) ^ 1024) - 1024;
    Some((precision, scale))
}

// ─── time / timestamp / interval precision ──────────────────────────────────

/// Maximum fractional-seconds precision Postgres supports. Postgres stores
/// microseconds, so 6 digits is the hard ceiling.
pub const MAX_TIME_PRECISION: i32 = 6;

/// Encode `timestamp(p)` / `timestamptz(p)` / `time(p)` / `timetz(p)`.
///
/// Unlike varlena types these carry **no** `VARHDRSZ` offset — the typmod is
/// the precision itself. Getting this wrong by adding 4 is a classic bug, so
/// it is called out here and pinned by a test below.
pub fn encode_time_precision(p: i32) -> i32 {
    if !(0..=MAX_TIME_PRECISION).contains(&p) {
        UNSPECIFIED
    } else {
        p
    }
}

/// Decode a time/timestamp typmod back to its fractional-seconds precision.
pub fn decode_time_precision(typmod: i32) -> Option<i32> {
    if (0..=MAX_TIME_PRECISION).contains(&typmod) {
        Some(typmod)
    } else {
        None
    }
}

// ─── interval ────────────────────────────────────────────────────────────
//
// `interval` is the odd one out in this family: its typmod is not just a
// precision. Postgres packs it as
// `INTERVAL_TYPMOD(prec, range) = ((range & 0x7FFF) << 16) | (prec & 0xFFFF)`
// (see `INTERVAL_TYPMOD` / `INTERVAL_RANGE` / `INTERVAL_PRECISION` in
// Postgres's `datatype/timestamp.h`).
//
// `range` is a bitmask of which fields the declaration restricted (`YEAR TO
// MONTH`, `DAY TO SECOND`, a single field like `HOUR`, ...); an interval
// declared with no `... TO ...` clause — including a bare `interval(p)` —
// gets `INTERVAL_FULL_RANGE`, meaning "no field restriction". `prec` is the
// fractional-second precision, or -1 (all-ones in the low 16 bits) if
// unspecified.
//
// Verified against live PostgreSQL 18.2:
//   interval(3)                -> typmod 2147418115 (range=FULL,          prec=3)
//   interval year to month     -> typmod 458751      (range=YEAR|MONTH,   prec=unspecified)
//   interval day to second(3)  -> typmod 470286339    (range=DAY..SECOND, prec=3)
//   interval hour               -> typmod 67174399     (range=HOUR,        prec=unspecified)
//   interval (bare)             -> typmod -1

// Field bit positions, from Postgres's `datetime.h` `#define`s for the
// DecodeUnits enum (`MONTH = 1`, `YEAR = 2`, `DAY = 3`, `HOUR = 10`,
// `MINUTE = 11`, `SECOND = 12`); the range mask for a field is `1 << field`.
const FIELD_MONTH: i32 = 1 << 1;
const FIELD_YEAR: i32 = 1 << 2;
const FIELD_DAY: i32 = 1 << 3;
const FIELD_HOUR: i32 = 1 << 10;
const FIELD_MINUTE: i32 = 1 << 11;
const FIELD_SECOND: i32 = 1 << 12;

/// "No field restriction" — a bare `interval` or `interval(p)` with no `...
/// TO ...` clause. Postgres calls this `INTERVAL_FULL_RANGE`.
pub const INTERVAL_RANGE_FULL: i32 = 0x7FFF;

pub const INTERVAL_RANGE_YEAR: i32 = FIELD_YEAR;
pub const INTERVAL_RANGE_MONTH: i32 = FIELD_MONTH;
pub const INTERVAL_RANGE_DAY: i32 = FIELD_DAY;
pub const INTERVAL_RANGE_HOUR: i32 = FIELD_HOUR;
pub const INTERVAL_RANGE_MINUTE: i32 = FIELD_MINUTE;
pub const INTERVAL_RANGE_SECOND: i32 = FIELD_SECOND;
pub const INTERVAL_RANGE_YEAR_TO_MONTH: i32 = FIELD_YEAR | FIELD_MONTH;
pub const INTERVAL_RANGE_DAY_TO_HOUR: i32 = FIELD_DAY | FIELD_HOUR;
pub const INTERVAL_RANGE_DAY_TO_MINUTE: i32 = FIELD_DAY | FIELD_HOUR | FIELD_MINUTE;
pub const INTERVAL_RANGE_DAY_TO_SECOND: i32 = FIELD_DAY | FIELD_HOUR | FIELD_MINUTE | FIELD_SECOND;
pub const INTERVAL_RANGE_HOUR_TO_MINUTE: i32 = FIELD_HOUR | FIELD_MINUTE;
pub const INTERVAL_RANGE_HOUR_TO_SECOND: i32 = FIELD_HOUR | FIELD_MINUTE | FIELD_SECOND;
pub const INTERVAL_RANGE_MINUTE_TO_SECOND: i32 = FIELD_MINUTE | FIELD_SECOND;

/// Encode an `interval` typmod from a field-range mask (one of the
/// `INTERVAL_RANGE_*` constants above) and an optional fractional-second
/// precision.
///
/// `precision: None`, or `Some(p)` outside `0..=MAX_TIME_PRECISION`, encodes
/// as "unspecified" — Postgres represents that as -1 sign-extended into the
/// low 16 bits, not as 0, so it is NOT the same bit pattern as
/// `precision(0)`.
///
/// A bare `interval` column (no parens, no `TO` clause at all) never calls
/// Postgres's `intervaltypmodin` — like every other type, "no modifier
/// written" is just the universal typmod sentinel [`UNSPECIFIED`] (-1), a
/// raw all-ones bit pattern. That is distinct from what the packing formula
/// below would produce for `(INTERVAL_RANGE_FULL, None)` — `0x7FFF0000 |
/// 0x0000FFFF = 0x7FFFFFFF`, not `-1` — because `INTERVAL_RANGE_FULL` is
/// only 15 bits (bit 15 of the range half is always 0), whereas `-1`'s
/// range half has every bit, including bit 15, set. Both decode to the same
/// `(range, precision)` tuple, so it is harmless to special-case the
/// no-restriction/no-precision pair to the sentinel Postgres actually uses.
pub fn encode_interval(range: i32, precision: Option<i32>) -> i32 {
    if range & 0x7FFF == INTERVAL_RANGE_FULL && precision.is_none() {
        return UNSPECIFIED;
    }
    let prec_bits = match precision {
        Some(p) if (0..=MAX_TIME_PRECISION).contains(&p) => p & 0xFFFF,
        _ => -1i32 & 0xFFFF, // 0xFFFF: "unspecified", per INTERVAL_TYPMOD
    };
    ((range & 0x7FFF) << 16) | prec_bits
}

/// Decode an `interval` typmod into `(range_mask, precision)`.
///
/// The low 16 bits are sign-extended back to recover -1 ("unspecified") as
/// `None` rather than as the small positive precision `65535 & 0xFFFF` would
/// otherwise imply.
pub fn decode_interval(typmod: i32) -> (i32, Option<i32>) {
    let range = (typmod >> 16) & 0x7FFF;
    // Sign-extend the low 16 bits: shift left into the top of the word, then
    // arithmetic-shift back down. Rust's `>>` on i32 is arithmetic.
    let precision = (typmod << 16) >> 16;
    let precision = if precision == UNSPECIFIED {
        None
    } else {
        Some(precision)
    };
    (range, precision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varchar_length_round_trips_with_varhdrsz() {
        // varchar(10) is typmod 14 in Postgres, not 10.
        assert_eq!(encode_varlena_len(10), 14);
        assert_eq!(decode_varlena_len(14), Some(10));
        assert_eq!(encode_varlena_len(255), 259);
        assert_eq!(decode_varlena_len(259), Some(255));
    }

    #[test]
    fn unspecified_varchar_decodes_to_none() {
        assert_eq!(decode_varlena_len(UNSPECIFIED), None);
        assert_eq!(encode_varlena_len(0), UNSPECIFIED);
        assert_eq!(encode_varlena_len(-3), UNSPECIFIED);
    }

    #[test]
    fn numeric_round_trips() {
        for (p, s) in [(10, 2), (38, 10), (1000, 0), (5, 5), (18, 0)] {
            let t = encode_numeric(p, s);
            assert_eq!(
                decode_numeric(t),
                Some((p, s)),
                "numeric({p},{s}) typmod {t}"
            );
        }
    }

    /// Postgres 15+ permits a negative scale. The 11-bit bias encoding is the
    /// only reason that works, so it gets its own test.
    #[test]
    fn numeric_negative_scale_round_trips() {
        for (p, s) in [(10, -2), (20, -100), (5, -1000)] {
            assert_eq!(decode_numeric(encode_numeric(p, s)), Some((p, s)));
        }
    }

    #[test]
    fn numeric_typmod_matches_postgres_encoding() {
        // numeric(10,2) in Postgres is typmod 655366:
        //   ((10 << 16) | 2) + 4 = 655360 + 2 + 4
        assert_eq!(encode_numeric(10, 2), 655_366);
    }

    /// Time precision carries no VARHDRSZ. If someone "unifies" the encoders
    /// this test fails, which is the point.
    #[test]
    fn time_precision_has_no_varhdrsz_offset() {
        assert_eq!(encode_time_precision(3), 3);
        assert_eq!(decode_time_precision(3), Some(3));
        assert_eq!(encode_time_precision(0), 0);
        assert_eq!(encode_time_precision(6), 6);
    }

    #[test]
    fn time_precision_beyond_microseconds_is_unspecified() {
        assert_eq!(encode_time_precision(7), UNSPECIFIED);
        assert_eq!(encode_time_precision(-1), UNSPECIFIED);
        assert_eq!(decode_time_precision(9), None);
    }

    /// Pinned against live PostgreSQL 18.2's `pg_attribute.atttypmod` for
    /// columns declared with each interval spelling (see module doc for the
    /// full `psql` transcript). Unlike time/timestamp, interval's typmod is
    /// NOT a bare precision — verifying that was the point of this check.
    #[test]
    fn interval_typmod_matches_postgres_encoding() {
        // `interval(3)`: no field restriction, precision 3.
        assert_eq!(
            encode_interval(INTERVAL_RANGE_FULL, Some(3)),
            2_147_418_115
        );
        // `interval year to month`: no precision.
        assert_eq!(
            encode_interval(INTERVAL_RANGE_YEAR_TO_MONTH, None),
            458_751
        );
        // `interval day to second(3)`.
        assert_eq!(
            encode_interval(INTERVAL_RANGE_DAY_TO_SECOND, Some(3)),
            470_286_339
        );
        // `interval hour`: single-field range, no precision.
        assert_eq!(encode_interval(INTERVAL_RANGE_HOUR, None), 67_174_399);
        // bare `interval`: no field restriction, no precision.
        assert_eq!(encode_interval(INTERVAL_RANGE_FULL, None), UNSPECIFIED);
    }

    #[test]
    fn interval_typmod_round_trips() {
        for (range, precision) in [
            (INTERVAL_RANGE_FULL, Some(3)),
            (INTERVAL_RANGE_YEAR_TO_MONTH, None),
            (INTERVAL_RANGE_DAY_TO_SECOND, Some(3)),
            (INTERVAL_RANGE_HOUR, None),
            (INTERVAL_RANGE_FULL, None),
            (INTERVAL_RANGE_MINUTE_TO_SECOND, Some(0)),
            (INTERVAL_RANGE_DAY_TO_HOUR, Some(6)),
        ] {
            let t = encode_interval(range, precision);
            assert_eq!(
                decode_interval(t),
                (range, precision),
                "interval typmod {t}"
            );
        }
    }

    /// A bare `interval` (typmod -1) must decode identically to the
    /// general-purpose formula, not via a special case — the whole point of
    /// `INTERVAL_TYPMOD`'s design is that -1 already means "full range,
    /// unspecified precision" without special-casing.
    #[test]
    fn interval_unspecified_typmod_is_full_range_no_precision() {
        assert_eq!(decode_interval(UNSPECIFIED), (INTERVAL_RANGE_FULL, None));
    }
}
