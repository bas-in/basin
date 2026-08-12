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
}
