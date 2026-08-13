//! Postgres's cast system — `pg_cast`, and the three contexts in which a cast
//! may be applied.
//!
//! # Why three categories and not one
//!
//! The single most important fact about Postgres casts is that "can this value
//! become that type?" is **not** a yes/no question. It depends on *where in the
//! query* the coercion is being attempted:
//!
//! ```text
//!   SELECT int8col + int4col       -- int4 -> int8 happens silently
//!   INSERT INTO t(int4col) VALUES (int8expr)   -- int8 -> int4 happens silently
//!   SELECT int4col + int8col::int4 -- int8 -> int4 requires you to ask
//! ```
//!
//! Postgres encodes this as `pg_cast.castcontext`, one of `'i'`, `'a'`, `'e'`
//! ([`CastKind`]), checked against the syntactic position's own requirement
//! ([`CastContext`]). Collapsing the two into a single "is castable" predicate
//! is how engines end up silently narrowing an `int8` to an `int4` inside an
//! expression — which Postgres refuses to do, because the result would depend
//! on data rather than on types.
//!
//! # Where these values come from
//!
//! The table in [`cast_kind`] reproduces `pg_cast` — every entry below was
//! checked against a live PostgreSQL 18 `pg_cast` join, not recalled from
//! memory:
//!
//! ```sql
//! SELECT s.typname, t.typname, c.castcontext, c.castmethod
//!   FROM pg_cast c
//!   JOIN pg_type s ON s.oid = c.castsource
//!   JOIN pg_type t ON t.oid = c.casttarget;
//! ```
//!
//! Anything this crate could *not* confirm that way would be marked
//! `UNVERIFIED` in a comment rather than presented as fact. Re-run the query
//! above before editing the table; several entries are counter-intuitive and
//! guessing produces exactly the fidelity bugs this crate exists to remove.
//!
//! Two rules are *not* rows in `pg_cast` and are easy to miss:
//!
//! 1. **I/O coercion.** `parse_coerce.c`'s `find_coercion_pathway` falls back
//!    to the type's text input/output functions when no `pg_cast` row exists:
//!    any type may go *to* a string type at assignment level, and *from* a
//!    string type at explicit level. That is why `uuid`, `bytea` and `jsonb`
//!    need no rows here yet still cast to `text`.
//! 2. **`unknown`.** `can_coerce_type` short-circuits on an `unknown` input
//!    before it ever consults `pg_cast`, which is how an untyped literal takes
//!    its type from context. See [`cast_kind`] and `oid::UNKNOWN`.
//!
//! # What is deliberately absent
//!
//! Function and operator overload resolution (`func_select_candidate`, the
//! preferred-type rules, `select_common_type`) is a separate increment. This
//! module answers "is this cast legal here", nothing more. Callers doing
//! overload resolution will *use* it; it does not do their job for them.

use crate::{oid, Oid, PgType};

/// How aggressively Postgres will apply a cast — `pg_cast.castcontext`.
///
/// The variants are ordered from most to least automatic, and that ordering is
/// load-bearing: a cast is permitted in a context iff its kind is *no more
/// demanding* than the context allows. See [`CastKind::allowed_in`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CastKind {
    /// `'i'` — applied automatically anywhere, including in the middle of an
    /// expression. Reserved for casts that cannot lose information, e.g.
    /// `int4 -> int8`, `int4 -> float8`.
    Implicit,
    /// `'a'` — applied automatically only when storing into a target of known
    /// type: `INSERT`/`UPDATE` column targets, and function/`RETURNS` result
    /// coercion. `int8 -> int4` lives here: Postgres is willing to narrow when
    /// you have *named* the destination column, and unwilling to guess inside
    /// an expression.
    Assignment,
    /// `'e'` — only via `CAST(x AS t)` or `x::t`. The user has taken
    /// responsibility for a conversion that may fail or surprise, e.g.
    /// `text -> int4`.
    Explicit,
}

impl CastKind {
    /// The `pg_cast.castcontext` character.
    ///
    /// `pg_catalog` must report this verbatim — clients and ORMs read
    /// `pg_cast` to decide what they may emit without an explicit cast — so the
    /// encoding is part of Basin's external surface, not an internal detail.
    #[inline]
    pub const fn as_char(self) -> char {
        match self {
            CastKind::Implicit => 'i',
            CastKind::Assignment => 'a',
            CastKind::Explicit => 'e',
        }
    }

    /// Parse a `pg_cast.castcontext` character.
    #[inline]
    pub const fn from_char(c: char) -> Option<CastKind> {
        Some(match c {
            'i' => CastKind::Implicit,
            'a' => CastKind::Assignment,
            'e' => CastKind::Explicit,
            _ => return None,
        })
    }

    /// Whether a cast of this kind may be applied in `context`.
    ///
    /// This is Postgres's `ccontext >= pg_cast.castcontext` comparison from
    /// `find_coercion_pathway`, spelled out: an implicit cast is allowed
    /// everywhere, an assignment cast in assignment and explicit positions, an
    /// explicit cast only where the user wrote one.
    #[inline]
    pub const fn allowed_in(self, context: CastContext) -> bool {
        self.strictness() <= context.permissiveness()
    }

    /// Rank used by [`allowed_in`](CastKind::allowed_in). Lower is more
    /// automatic.
    #[inline]
    const fn strictness(self) -> u8 {
        match self {
            CastKind::Implicit => 0,
            CastKind::Assignment => 1,
            CastKind::Explicit => 2,
        }
    }
}

/// The syntactic position a coercion is being attempted in — Postgres's
/// `CoercionContext`.
///
/// Note this is a property of the *query*, not of the types: the same pair of
/// types may coerce here and not there. Callers must pass the real context;
/// defaulting to [`CastContext::Explicit`] "to be permissive" reintroduces
/// exactly the silent narrowing the three-category system exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CastContext {
    /// An ordinary expression position: an operand of `+`, a `WHERE` clause, a
    /// `UNION` arm. Only implicit casts apply.
    Implicit,
    /// Storing into a target whose type is already fixed: `INSERT`/`UPDATE`
    /// column lists, and coercing a function body's result to its declared
    /// return type.
    Assignment,
    /// The user wrote `CAST(x AS t)` or `x::t`.
    Explicit,
}

impl CastContext {
    /// Rank used by [`CastKind::allowed_in`]. Higher permits more.
    #[inline]
    const fn permissiveness(self) -> u8 {
        match self {
            CastContext::Implicit => 0,
            CastContext::Assignment => 1,
            CastContext::Explicit => 2,
        }
    }
}

/// The kind of cast Postgres has from `from` to `to`, or `None` if there is no
/// cast at all in any context.
///
/// Reproduces `pg_cast` plus the two out-of-table rules described in the module
/// docs. The order of the checks below matters and mirrors
/// `can_coerce_type` / `find_coercion_pathway`:
///
/// 1. identical types,
/// 2. `unknown` source (before `pg_cast` is consulted at all),
/// 3. array-to-array, which delegates to the element types,
/// 4. the `pg_cast` table,
/// 5. the string-category I/O fallback, which is *last* so that a real
///    `pg_cast` row always wins over it.
pub fn cast_kind(from: Oid, to: Oid) -> Option<CastKind> {
    // A type is trivially, freely itself. Note this covers `varchar(10) ->
    // varchar(5)`: differing typmods are *not* a cast in Postgres, they are a
    // separate length-coercion step (`coerce_type_typmod`) applied after the
    // type is settled. Enforcing the length is a storage concern, not this
    // module's. See `can_coerce` for the same point at the PgType level.
    if from == to {
        return Some(CastKind::Implicit);
    }

    // `unknown` is an untyped literal that has not been resolved yet, and it
    // becomes whatever the context needs — `SELECT 'x' = col` types the literal
    // from the column, not the column from the literal. Postgres does this in
    // `can_coerce_type` with a bare `if (inputTypeId == UNKNOWNOID) continue;`,
    // *before* looking in pg_cast, which is why there are no `unknown` rows
    // there. Effectively implicit, in every context.
    if from == oid::UNKNOWN {
        return Some(CastKind::Implicit);
    }

    // The converse is not true: nothing coerces *to* `unknown`. A resolved type
    // never becomes unresolved, and a plan that asks for this has a bug in its
    // type resolution rather than a missing cast.
    if to == oid::UNKNOWN {
        return None;
    }

    // Pseudo-types that never carry a value. Guarded here so the string
    // fallback below cannot invent a `void -> text` cast.
    if is_pseudo(from) || is_pseudo(to) {
        return None;
    }

    // An array coerces exactly as far as its elements do (Postgres's
    // COERCION_PATH_ARRAYCOERCE, which builds an ArrayCoerceExpr around the
    // per-element cast). `int4[] -> int8[]` is therefore implicit, and
    // `int8[] -> int4[]` assignment, with no rows in pg_cast for either.
    if let (Some(from_elem), Some(to_elem)) = (oid::element_of(from), oid::element_of(to)) {
        return cast_kind(from_elem, to_elem);
    }

    // Mixing an array with a non-array is never a cast — except via the string
    // fallback below (`int4[] -> text` is a legitimate assignment cast, and is
    // how an array reaches a text column). Fall through rather than returning.
    if let Some(kind) = builtin_pg_cast(from, to) {
        return Some(kind);
    }

    // The I/O fallback from `find_coercion_pathway`:
    //
    //   if (ccontext >= COERCION_ASSIGNMENT && TypeCategory(target) == STRING)
    //       result = COERCION_PATH_COERCEVIAIO;
    //   else if (ccontext >= COERCION_EXPLICIT && TypeCategory(source) == STRING)
    //       result = COERCION_PATH_COERCEVIAIO;
    //
    // Postgres's own comment calls the asymmetry "a compromise to preserve
    // something of the pre-8.3 behavior that many types had implicit (yipes!)
    // casts to text". It is the reason `INSERT INTO t(txt) VALUES (5)` works
    // while `SELECT 5 = txtcol` does not.
    if is_string_category(to) {
        return Some(CastKind::Assignment);
    }
    if is_string_category(from) {
        return Some(CastKind::Explicit);
    }

    None
}

/// Whether `from` may be coerced to `to` in the given syntactic position.
///
/// The whole point of the `context` argument is that this is *not* a property
/// of the type pair alone:
///
/// ```
/// use basin_pgtype::PgType;
/// use basin_pgtype::cast::{CastContext, can_coerce};
///
/// // Narrowing is fine when you have named the destination column...
/// assert!(can_coerce(PgType::INT8, PgType::INT4, CastContext::Assignment));
/// // ...and refused in the middle of an expression.
/// assert!(!can_coerce(PgType::INT8, PgType::INT4, CastContext::Implicit));
/// ```
///
/// Typmods are ignored. `varchar(10) -> varchar(5)` is a legal coercion whose
/// *runtime* behaviour (error, or blank-truncation for trailing spaces) is
/// decided when the value is stored, not when the plan is typed — Postgres
/// separates `coerce_type` from `coerce_type_typmod` for exactly this reason.
pub fn can_coerce(from: PgType, to: PgType, context: CastContext) -> bool {
    match cast_kind(from.oid, to.oid) {
        Some(kind) => kind.allowed_in(context),
        None => false,
    }
}

/// Whether `oid` is one of Postgres's `TYPCATEGORY_STRING` types.
///
/// The membership is what drives the I/O fallback in [`cast_kind`], so it
/// follows `pg_type.typcategory = 'S'` exactly: `text`, `varchar`, `bpchar`,
/// `name`. Notably `xml` and `json` are category `'U'` and are *not* string
/// types, despite being textual — adding them here would silently make
/// `int4 -> xml` an assignment cast.
///
/// The internal single-byte `"char"` (oid 18) does **not** belong here either,
/// despite looking like a fourth string type: `SELECT typname, typcategory
/// FROM pg_type WHERE typname = 'char'` reports category `'Z'`, verified
/// against a live PostgreSQL 18. Including it here was a bug this crate
/// shipped with: it made the I/O fallback silently invent casts pg_cast does
/// not have — `bool -> "char"`, `int8 -> "char"`, `jsonb -> "char"`, etc. all
/// error in real Postgres (`cannot cast type ... to "char"`), and would
/// wrongly succeed as an assignment cast if `"char"` were treated as string
/// category here. `"char"`'s handful of genuine casts (to/from
/// text/varchar/bpchar/int4) are real `pg_cast` rows and are tabulated
/// explicitly in [`builtin_pg_cast`] instead.
#[inline]
pub fn is_string_category(oid: Oid) -> bool {
    matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME)
}

/// Pseudo-types with no values, which therefore have no casts.
#[inline]
fn is_pseudo(oid: Oid) -> bool {
    matches!(oid, oid::UNKNOWN | oid::VOID | oid::RECORD)
}

/// The builtin `pg_cast` rows Basin reproduces.
///
/// Only pairs with a genuine `pg_cast` entry belong here. Pairs served by the
/// string I/O fallback (`uuid -> text`, `text -> uuid`, `jsonb -> text`, …)
/// must **not** be tabulated, or the fallback silently stops being tested.
fn builtin_pg_cast(from: Oid, to: Oid) -> Option<CastKind> {
    use CastKind::{Assignment, Explicit, Implicit};

    Some(match (from, to) {
        // ─── The numeric tower ──────────────────────────────────────────────
        //
        // Widening is implicit; narrowing is assignment-only. That single line
        // is the reason `SELECT int8col + 1` keeps 64 bits, and the reason
        // `SELECT int4col = int8col` promotes rather than truncating.
        //
        // The tower is NOT the tidy chain int2 < int4 < int8 < numeric < float4
        // < float8 that it is usually described as. Two real asymmetries:
        //
        //   * numeric -> float4/float8 is IMPLICIT even though it loses
        //     precision, because float8 is the *preferred* type of the numeric
        //     category and Postgres leans on that for operator resolution.
        //   * float4/float8 -> numeric is only ASSIGNMENT, i.e. the reverse
        //     direction is not the mirror image.
        //
        // Both are checked in `numeric_tower_is_not_symmetric` below.
        (oid::INT2, oid::INT4) => Implicit,
        (oid::INT2, oid::INT8) => Implicit,
        (oid::INT2, oid::FLOAT4) => Implicit,
        (oid::INT2, oid::FLOAT8) => Implicit,
        (oid::INT2, oid::NUMERIC) => Implicit,

        (oid::INT4, oid::INT8) => Implicit,
        (oid::INT4, oid::FLOAT4) => Implicit,
        (oid::INT4, oid::FLOAT8) => Implicit,
        (oid::INT4, oid::NUMERIC) => Implicit,
        (oid::INT4, oid::INT2) => Assignment,

        (oid::INT8, oid::FLOAT4) => Implicit,
        (oid::INT8, oid::FLOAT8) => Implicit,
        (oid::INT8, oid::NUMERIC) => Implicit,
        (oid::INT8, oid::INT2) => Assignment,
        (oid::INT8, oid::INT4) => Assignment,

        (oid::FLOAT4, oid::FLOAT8) => Implicit,
        (oid::FLOAT4, oid::INT2) => Assignment,
        (oid::FLOAT4, oid::INT4) => Assignment,
        (oid::FLOAT4, oid::INT8) => Assignment,
        (oid::FLOAT4, oid::NUMERIC) => Assignment,

        (oid::FLOAT8, oid::FLOAT4) => Assignment,
        (oid::FLOAT8, oid::INT2) => Assignment,
        (oid::FLOAT8, oid::INT4) => Assignment,
        (oid::FLOAT8, oid::INT8) => Assignment,
        (oid::FLOAT8, oid::NUMERIC) => Assignment,

        (oid::NUMERIC, oid::FLOAT4) => Implicit,
        (oid::NUMERIC, oid::FLOAT8) => Implicit,
        (oid::NUMERIC, oid::INT2) => Assignment,
        (oid::NUMERIC, oid::INT4) => Assignment,
        (oid::NUMERIC, oid::INT8) => Assignment,

        // ─── oid ────────────────────────────────────────────────────────────
        //
        // `oid` is unsigned 32-bit, so int4 -> oid is not lossless in general
        // (a negative int4 wraps); pg_cast still calls it implicit, and does it
        // binary-coercibly. Basin reproduces the wart rather than correcting it.
        // Going the other way is assignment.
        (oid::INT2, oid::OID) => Implicit,
        (oid::INT4, oid::OID) => Implicit,
        (oid::INT8, oid::OID) => Implicit,
        (oid::OID, oid::INT4) => Assignment,
        (oid::OID, oid::INT8) => Assignment,

        // ─── bool ───────────────────────────────────────────────────────────
        //
        // Explicit in both directions. `WHERE int4col` is an error in Postgres
        // (unlike MySQL/SQLite), and this row is why.
        (oid::BOOL, oid::INT4) => Explicit,
        (oid::INT4, oid::BOOL) => Explicit,

        // ─── bytea ──────────────────────────────────────────────────────────
        //
        // The integer <-> bytea casts reinterpret the machine representation
        // and are explicit in both directions. Note this is *not* the
        // string-ish `'\x0a'::bytea` route: bytea is category 'U', so
        // text -> bytea takes the I/O fallback instead.
        (oid::INT2, oid::BYTEA) => Explicit,
        (oid::INT4, oid::BYTEA) => Explicit,
        (oid::INT8, oid::BYTEA) => Explicit,
        (oid::BYTEA, oid::INT2) => Explicit,
        (oid::BYTEA, oid::INT4) => Explicit,
        (oid::BYTEA, oid::INT8) => Explicit,

        // ─── string <-> string ──────────────────────────────────────────────
        //
        // text, varchar and bpchar are mutually implicit: they are the same
        // physical string, and pg_cast marks the pairs binary-coercible (except
        // bpchar -> text/varchar, which runs a function to strip the blank
        // padding — a different castmethod, same castcontext 'i').
        (oid::TEXT, oid::VARCHAR) => Implicit,
        (oid::TEXT, oid::BPCHAR) => Implicit,
        (oid::VARCHAR, oid::TEXT) => Implicit,
        (oid::VARCHAR, oid::BPCHAR) => Implicit,
        (oid::BPCHAR, oid::TEXT) => Implicit,
        (oid::BPCHAR, oid::VARCHAR) => Implicit,

        // `name` is the 63-byte identifier type used across pg_catalog, and its
        // casts run *backwards* from intuition. text/varchar/bpchar -> name is
        // IMPLICIT even though it truncates at 63 bytes, because pg_catalog
        // comparisons like `relname = 'some_table'` have to work without a
        // cast. name -> bpchar/varchar is only 'a', which is why those two
        // directions are absent here and left to the I/O fallback (same answer,
        // Assignment, arrived at by the general rule).
        (oid::NAME, oid::TEXT) => Implicit,
        (oid::TEXT, oid::NAME) => Implicit,
        (oid::VARCHAR, oid::NAME) => Implicit,
        (oid::BPCHAR, oid::NAME) => Implicit,

        // `"char"` — the internal single-byte type (oid 18), NOT char(n).
        // Its typcategory is 'Z', not 'S' (verified live), so it gets NO help
        // from the string I/O fallback in `cast_kind` — every one of its casts
        // must be a real, tabulated `pg_cast` row, or the pair has no cast at
        // all. `int8 -> "char"`, `bool -> "char"`, `jsonb -> "char"`, etc. are
        // not typos of omission: Postgres genuinely has no cast for them
        // (`ERROR: cannot cast type bigint to "char"`), confirmed live.
        //
        // Widening it to text is implicit; the rest of its six real casts
        // (to/from bpchar, varchar, and the int4 pair) are 'a'/'e' and must be
        // tabulated explicitly, since — unlike text/varchar/bpchar/name — this
        // type gets no free ride from `is_string_category`.
        (oid::CHAR, oid::TEXT) => Implicit,
        (oid::CHAR, oid::BPCHAR) => Assignment,
        (oid::CHAR, oid::VARCHAR) => Assignment,
        (oid::CHAR, oid::INT4) => Explicit,
        (oid::TEXT, oid::CHAR) => Assignment,
        (oid::BPCHAR, oid::CHAR) => Assignment,
        (oid::VARCHAR, oid::CHAR) => Assignment,
        (oid::INT4, oid::CHAR) => Explicit,

        // ─── date / time ────────────────────────────────────────────────────
        //
        // Widening a date to a timestamp adds midnight and loses nothing;
        // going back drops the time-of-day, hence assignment. timestamp ->
        // timestamptz is implicit and *not* value-preserving in the way it
        // looks: it interprets the value in the session TimeZone.
        (oid::DATE, oid::TIMESTAMP) => Implicit,
        (oid::DATE, oid::TIMESTAMPTZ) => Implicit,
        (oid::TIMESTAMP, oid::TIMESTAMPTZ) => Implicit,
        (oid::TIMESTAMPTZ, oid::TIMESTAMP) => Assignment,
        (oid::TIMESTAMP, oid::DATE) => Assignment,
        (oid::TIMESTAMPTZ, oid::DATE) => Assignment,
        (oid::TIMESTAMP, oid::TIME) => Assignment,
        (oid::TIMESTAMPTZ, oid::TIME) => Assignment,
        (oid::TIMESTAMPTZ, oid::TIMETZ) => Assignment,
        (oid::TIME, oid::INTERVAL) => Implicit,
        (oid::INTERVAL, oid::TIME) => Assignment,
        (oid::TIME, oid::TIMETZ) => Implicit,
        (oid::TIMETZ, oid::TIME) => Assignment,

        // ─── json ───────────────────────────────────────────────────────────
        //
        // json <-> jsonb is 'a' in BOTH directions (castmethod 'i', i.e. via
        // the types' I/O functions). Neither type is string-category, so the
        // fallback in `cast_kind` does not reach them and these rows are the
        // only reason the conversion exists at all. The symmetry is real, and
        // it is not free: json -> jsonb reparses and normalises (key order and
        // duplicate keys are lost), while jsonb -> json re-serialises.
        (oid::JSON, oid::JSONB) => Assignment,
        (oid::JSONB, oid::JSON) => Assignment,

        // Extracting a scalar from jsonb is explicit. There is no matching set
        // of rows for `json`, so `json -> int4` has no cast at all — an
        // asymmetry in Postgres, not a gap here.
        (oid::JSONB, oid::BOOL) => Explicit,
        (oid::JSONB, oid::INT2) => Explicit,
        (oid::JSONB, oid::INT4) => Explicit,
        (oid::JSONB, oid::INT8) => Explicit,
        (oid::JSONB, oid::FLOAT4) => Explicit,
        (oid::JSONB, oid::FLOAT8) => Explicit,
        (oid::JSONB, oid::NUMERIC) => Explicit,

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ordering *is* the feature. If someone reorders the variants or
    /// "simplifies" `allowed_in` into equality, this fails.
    #[test]
    fn cast_kinds_are_permitted_in_progressively_wider_contexts() {
        use CastContext as C;
        use CastKind as K;

        // Implicit: everywhere.
        assert!(K::Implicit.allowed_in(C::Implicit));
        assert!(K::Implicit.allowed_in(C::Assignment));
        assert!(K::Implicit.allowed_in(C::Explicit));

        // Assignment: not in a bare expression.
        assert!(!K::Assignment.allowed_in(C::Implicit));
        assert!(K::Assignment.allowed_in(C::Assignment));
        assert!(K::Assignment.allowed_in(C::Explicit));

        // Explicit: only when the user asked.
        assert!(!K::Explicit.allowed_in(C::Implicit));
        assert!(!K::Explicit.allowed_in(C::Assignment));
        assert!(K::Explicit.allowed_in(C::Explicit));
    }

    /// pg_catalog reports these characters verbatim; clients switch on them.
    #[test]
    fn castcontext_chars_match_pg_cast() {
        assert_eq!(CastKind::Implicit.as_char(), 'i');
        assert_eq!(CastKind::Assignment.as_char(), 'a');
        assert_eq!(CastKind::Explicit.as_char(), 'e');
        for k in [CastKind::Implicit, CastKind::Assignment, CastKind::Explicit] {
            assert_eq!(CastKind::from_char(k.as_char()), Some(k));
        }
        assert_eq!(CastKind::from_char('x'), None);
    }

    /// The headline asymmetry: Postgres will widen an int4 anywhere and will
    /// narrow an int8 only into a named destination. An engine that gets this
    /// wrong silently truncates values in the middle of expressions.
    #[test]
    fn int4_widens_implicitly_but_int8_narrows_only_on_assignment() {
        assert_eq!(cast_kind(oid::INT4, oid::INT8), Some(CastKind::Implicit));
        assert_eq!(cast_kind(oid::INT8, oid::INT4), Some(CastKind::Assignment));

        // ...which means, in query terms:
        assert!(can_coerce(
            PgType::INT4,
            PgType::INT8,
            CastContext::Implicit
        ));
        assert!(
            !can_coerce(PgType::INT8, PgType::INT4, CastContext::Implicit),
            "narrowing int8 to int4 inside an expression must be refused"
        );
        assert!(can_coerce(
            PgType::INT8,
            PgType::INT4,
            CastContext::Assignment
        ));
        assert!(can_coerce(
            PgType::INT8,
            PgType::INT4,
            CastContext::Explicit
        ));
    }

    #[test]
    fn numeric_tower_widens_implicitly() {
        for (from, to) in [
            (oid::INT2, oid::INT4),
            (oid::INT2, oid::INT8),
            (oid::INT4, oid::INT8),
            (oid::INT2, oid::NUMERIC),
            (oid::INT4, oid::NUMERIC),
            (oid::INT8, oid::NUMERIC),
            (oid::INT4, oid::FLOAT4),
            (oid::INT4, oid::FLOAT8),
            (oid::INT8, oid::FLOAT8),
            (oid::FLOAT4, oid::FLOAT8),
        ] {
            assert_eq!(
                cast_kind(from, to),
                Some(CastKind::Implicit),
                "{from} -> {to} should be an implicit widening"
            );
        }
    }

    #[test]
    fn numeric_tower_narrows_only_on_assignment() {
        for (from, to) in [
            (oid::INT4, oid::INT2),
            (oid::INT8, oid::INT4),
            (oid::INT8, oid::INT2),
            (oid::FLOAT8, oid::FLOAT4),
            (oid::FLOAT8, oid::INT4),
            (oid::FLOAT4, oid::INT4),
            (oid::NUMERIC, oid::INT4),
            (oid::NUMERIC, oid::INT8),
        ] {
            assert_eq!(
                cast_kind(from, to),
                Some(CastKind::Assignment),
                "{from} -> {to} should be assignment-only"
            );
        }
    }

    /// The tower is usually described as a chain, and it is not one. These two
    /// pairs are the counterexamples, and someone "tidying" the table into a
    /// rank comparison will break exactly here.
    #[test]
    fn numeric_tower_is_not_symmetric() {
        // Loses precision, still implicit — float8 is the preferred type of the
        // numeric category.
        assert_eq!(
            cast_kind(oid::NUMERIC, oid::FLOAT8),
            Some(CastKind::Implicit)
        );
        assert_eq!(
            cast_kind(oid::NUMERIC, oid::FLOAT4),
            Some(CastKind::Implicit)
        );
        // The reverse is not the mirror image.
        assert_eq!(
            cast_kind(oid::FLOAT8, oid::NUMERIC),
            Some(CastKind::Assignment)
        );
        assert_eq!(
            cast_kind(oid::FLOAT4, oid::NUMERIC),
            Some(CastKind::Assignment)
        );
    }

    /// Parsing a string into a number is a runtime failure waiting to happen,
    /// so Postgres makes you write the cast.
    #[test]
    fn text_to_number_is_explicit_only() {
        for from in [oid::TEXT, oid::VARCHAR, oid::BPCHAR] {
            for to in [oid::INT2, oid::INT4, oid::INT8, oid::FLOAT8, oid::NUMERIC] {
                assert_eq!(
                    cast_kind(from, to),
                    Some(CastKind::Explicit),
                    "{from} -> {to} must require an explicit cast"
                );
            }
        }

        assert!(!can_coerce(
            PgType::TEXT,
            PgType::INT4,
            CastContext::Implicit
        ));
        assert!(
            !can_coerce(PgType::TEXT, PgType::INT4, CastContext::Assignment),
            "INSERT INTO t(int4col) VALUES (textcol) is an error in Postgres"
        );
        assert!(can_coerce(
            PgType::TEXT,
            PgType::INT4,
            CastContext::Explicit
        ));
    }

    /// The other half of the I/O fallback, and the asymmetry Postgres's own
    /// source calls a compromise: everything reaches a string type on
    /// assignment.
    #[test]
    fn anything_reaches_a_string_type_on_assignment() {
        for from in [
            oid::INT4,
            oid::INT8,
            oid::FLOAT8,
            oid::NUMERIC,
            oid::BOOL,
            oid::UUID,
            oid::JSONB,
            oid::BYTEA,
            oid::TIMESTAMPTZ,
            oid::INT4_ARRAY,
        ] {
            for to in [oid::TEXT, oid::VARCHAR, oid::BPCHAR] {
                assert_eq!(
                    cast_kind(from, to),
                    Some(CastKind::Assignment),
                    "{from} -> {to} should be an assignment cast via I/O"
                );
            }
        }

        // But NOT in an expression: `SELECT textcol = 5` is a type error, and
        // it is this line that makes it one.
        assert!(!can_coerce(
            PgType::INT4,
            PgType::TEXT,
            CastContext::Implicit
        ));
    }

    /// `unknown` is how an untyped literal survives until something tells it
    /// what it is. It must reach every real type, in the strictest context.
    #[test]
    fn unknown_coerces_to_anything_implicitly() {
        for to in [
            oid::BOOL,
            oid::INT2,
            oid::INT4,
            oid::INT8,
            oid::FLOAT4,
            oid::FLOAT8,
            oid::NUMERIC,
            oid::TEXT,
            oid::VARCHAR,
            oid::BYTEA,
            oid::DATE,
            oid::TIMESTAMPTZ,
            oid::UUID,
            oid::JSONB,
            oid::INT4_ARRAY,
        ] {
            assert_eq!(
                cast_kind(oid::UNKNOWN, to),
                Some(CastKind::Implicit),
                "unknown -> {to} must be implicit"
            );
            assert!(
                can_coerce(PgType::UNKNOWN, PgType::new(to), CastContext::Implicit),
                "SELECT 'lit' = col must be able to type the literal from {to}"
            );
        }
    }

    /// The rule is one-directional. A resolved type never becomes unresolved,
    /// and a planner asking for that has lost track of its own types.
    #[test]
    fn nothing_coerces_to_unknown() {
        assert_eq!(cast_kind(oid::INT4, oid::UNKNOWN), None);
        assert_eq!(cast_kind(oid::TEXT, oid::UNKNOWN), None);
        // Identity is still identity.
        assert_eq!(
            cast_kind(oid::UNKNOWN, oid::UNKNOWN),
            Some(CastKind::Implicit)
        );
    }

    #[test]
    fn same_type_is_always_coercible() {
        for t in [
            PgType::BOOL,
            PgType::INT4,
            PgType::TEXT,
            PgType::UUID,
            PgType::TIMESTAMPTZ,
            PgType::new(oid::INT4_ARRAY),
        ] {
            assert_eq!(cast_kind(t.oid, t.oid), Some(CastKind::Implicit));
            assert!(can_coerce(t, t, CastContext::Implicit));
        }
    }

    /// A differing typmod is a length coercion, not a cast. varchar(10) and
    /// varchar(5) are the same *type*; whether the value fits is decided when
    /// it is stored.
    #[test]
    fn typmod_does_not_affect_coercibility() {
        assert!(can_coerce(
            PgType::varchar(10),
            PgType::varchar(5),
            CastContext::Implicit
        ));
        assert!(can_coerce(
            PgType::numeric(20, 4),
            PgType::numeric(5, 0),
            CastContext::Implicit
        ));
        // And a typmod cannot smuggle in a cast that does not exist.
        assert!(!can_coerce(
            PgType::varchar(10),
            PgType::INT4,
            CastContext::Assignment
        ));
    }

    /// Arrays inherit their elements' cast kind, so the narrowing rule holds
    /// one level down too.
    #[test]
    fn arrays_coerce_exactly_as_far_as_their_elements() {
        assert_eq!(
            cast_kind(oid::INT4_ARRAY, oid::INT8_ARRAY),
            Some(CastKind::Implicit)
        );
        assert_eq!(
            cast_kind(oid::INT8_ARRAY, oid::INT4_ARRAY),
            Some(CastKind::Assignment)
        );
        assert_eq!(
            cast_kind(oid::TEXT_ARRAY, oid::INT4_ARRAY),
            Some(CastKind::Explicit)
        );
        // An element pair with no cast gives an array pair with no cast.
        assert_eq!(cast_kind(oid::BOOL_ARRAY, oid::DATE_ARRAY), None);
        // And an array is not its element type.
        assert_eq!(cast_kind(oid::INT4_ARRAY, oid::INT4), None);
        assert_eq!(cast_kind(oid::INT4, oid::INT4_ARRAY), None);
    }

    /// `WHERE int4col` is an error in Postgres, unlike in MySQL or SQLite.
    #[test]
    fn bool_and_int4_convert_only_explicitly() {
        assert_eq!(cast_kind(oid::INT4, oid::BOOL), Some(CastKind::Explicit));
        assert_eq!(cast_kind(oid::BOOL, oid::INT4), Some(CastKind::Explicit));
        assert!(!can_coerce(
            PgType::INT4,
            PgType::BOOL,
            CastContext::Assignment
        ));
    }

    #[test]
    fn date_widens_to_timestamp_but_not_back() {
        assert_eq!(
            cast_kind(oid::DATE, oid::TIMESTAMP),
            Some(CastKind::Implicit)
        );
        assert_eq!(
            cast_kind(oid::TIMESTAMP, oid::TIMESTAMPTZ),
            Some(CastKind::Implicit)
        );
        assert_eq!(
            cast_kind(oid::TIMESTAMP, oid::DATE),
            Some(CastKind::Assignment)
        );
        assert_eq!(
            cast_kind(oid::TIMESTAMPTZ, oid::TIMESTAMP),
            Some(CastKind::Assignment)
        );
    }

    /// Unrelated types have no cast at all, in any context — the function
    /// returns None rather than a permissive Explicit.
    #[test]
    fn unrelated_types_have_no_cast() {
        for (from, to) in [
            (oid::BOOL, oid::DATE),
            (oid::UUID, oid::INT8),
            (oid::JSONB, oid::TIMESTAMP),
            (oid::BYTEA, oid::TIMESTAMP),
            // jsonb -> int4 exists but json -> int4 does not. That asymmetry is
            // Postgres's, and pinning it stops someone "completing" the pair.
            (oid::JSON, oid::INT4),
        ] {
            assert_eq!(cast_kind(from, to), None, "{from} -> {to}");
            assert!(!can_coerce(
                PgType::new(from),
                PgType::new(to),
                CastContext::Explicit
            ));
        }
    }

    /// Pseudo-types carry no value, so the string fallback must not invent a
    /// `void -> text` cast for them.
    #[test]
    fn pseudo_types_have_no_casts() {
        assert_eq!(cast_kind(oid::VOID, oid::TEXT), None);
        assert_eq!(cast_kind(oid::RECORD, oid::TEXT), None);
        assert_eq!(cast_kind(oid::TEXT, oid::VOID), None);
    }

    /// `xml` and `json` are textual but are category 'U', not 'S'. If someone
    /// adds them to `is_string_category`, `int4 -> xml` silently becomes a
    /// legal assignment cast.
    #[test]
    fn xml_and_json_are_not_string_category() {
        assert!(!is_string_category(oid::XML));
        assert!(!is_string_category(oid::JSON));
        assert!(!is_string_category(oid::JSONB));
        assert_eq!(cast_kind(oid::INT4, oid::XML), None);
        assert_eq!(cast_kind(oid::INT4, oid::JSONB), None);
        // Text is still string category, so text -> xml goes via I/O.
        assert_eq!(cast_kind(oid::TEXT, oid::XML), Some(CastKind::Explicit));
    }

    /// The string types are mutually implicit — they are one physical string
    /// wearing three logical hats.
    #[test]
    fn text_varchar_and_bpchar_are_mutually_implicit() {
        for a in [oid::TEXT, oid::VARCHAR, oid::BPCHAR] {
            for b in [oid::TEXT, oid::VARCHAR, oid::BPCHAR] {
                assert_eq!(
                    cast_kind(a, b),
                    Some(CastKind::Implicit),
                    "{a} -> {b} should be implicit"
                );
            }
        }
        assert_eq!(cast_kind(oid::NAME, oid::TEXT), Some(CastKind::Implicit));
    }

    /// `name` runs backwards from intuition: coercing *into* the 63-byte
    /// identifier type is implicit (pg_catalog comparisons depend on it) while
    /// coercing out of it to varchar/bpchar is only assignment. Verified against
    /// a live pg_cast; without this test someone will "fix" the direction.
    #[test]
    fn casts_into_name_are_implicit_and_out_of_name_are_not() {
        for from in [oid::TEXT, oid::VARCHAR, oid::BPCHAR] {
            assert_eq!(
                cast_kind(from, oid::NAME),
                Some(CastKind::Implicit),
                "{from} -> name is implicit despite truncating at 63 bytes"
            );
        }
        assert_eq!(cast_kind(oid::NAME, oid::TEXT), Some(CastKind::Implicit));
        assert_eq!(
            cast_kind(oid::NAME, oid::VARCHAR),
            Some(CastKind::Assignment)
        );
        assert_eq!(
            cast_kind(oid::NAME, oid::BPCHAR),
            Some(CastKind::Assignment)
        );
    }

    /// json <-> jsonb is assignment in *both* directions, and it is the only
    /// route between them — neither type is string-category, so the I/O
    /// fallback never fires for this pair.
    #[test]
    fn json_and_jsonb_convert_symmetrically_at_assignment_level() {
        assert_eq!(cast_kind(oid::JSON, oid::JSONB), Some(CastKind::Assignment));
        assert_eq!(cast_kind(oid::JSONB, oid::JSON), Some(CastKind::Assignment));
        assert!(!can_coerce(
            PgType::JSONB,
            PgType::new(oid::JSON),
            CastContext::Implicit
        ));
        // Pulling a scalar out of jsonb needs the user to ask.
        assert_eq!(cast_kind(oid::JSONB, oid::INT4), Some(CastKind::Explicit));
        assert_eq!(cast_kind(oid::JSONB, oid::BOOL), Some(CastKind::Explicit));
    }

    /// A real pg_cast row must beat the string I/O fallback. `int4 -> "char"`
    /// is `'e'` in pg_cast, tabulated explicitly because `"char"` is category
    /// `'Z'` and gets no help at all from the fallback (see
    /// [`char_has_no_string_category_fallback`] for the general case). If the
    /// table lookup is ever moved after (or dropped in favour of) a fallback,
    /// this is what catches it.
    #[test]
    fn a_pg_cast_row_overrides_the_string_io_fallback() {
        assert_eq!(cast_kind(oid::INT4, oid::CHAR), Some(CastKind::Explicit));
        assert!(
            !can_coerce(
                PgType::INT4,
                PgType::new(oid::CHAR),
                CastContext::Assignment
            ),
            "int4 must not reach a \"char\" column without an explicit cast"
        );
        // ...while an int4 does reach text/varchar on assignment, via the
        // fallback, because those have no pg_cast row from int4.
        assert!(can_coerce(
            PgType::INT4,
            PgType::TEXT,
            CastContext::Assignment
        ));
    }

    /// `"char"` (oid 18) is `pg_type.typcategory = 'Z'`, not `'S'` — verified
    /// against a live PostgreSQL 18 (`SELECT typname, typcategory FROM
    /// pg_type WHERE typname = 'char'`). It must NOT be a member of
    /// `is_string_category`, or the I/O fallback invents casts pg_cast does
    /// not have. This was a real bug in this file: with `"char"` wrongly
    /// treated as string category, `bool -> "char"`, `int8 -> "char"`, and
    /// every other unrelated type paired with `"char"` silently became a
    /// legal assignment cast, when Postgres rejects all of them outright
    /// (`ERROR: cannot cast type ... to "char"`).
    #[test]
    fn char_has_no_string_category_fallback() {
        assert!(!is_string_category(oid::CHAR));

        // None of these have a pg_cast row, and "char" gets no fallback, so
        // there must be no cast in either direction — not even explicit.
        for other in [
            oid::BOOL,
            oid::BYTEA,
            oid::INT2,
            oid::INT8,
            oid::OID,
            oid::JSON,
            oid::XML,
            oid::FLOAT4,
            oid::FLOAT8,
            oid::NUMERIC,
            oid::DATE,
            oid::TIME,
            oid::TIMESTAMP,
            oid::TIMESTAMPTZ,
            oid::INTERVAL,
            oid::TIMETZ,
            oid::UUID,
            oid::JSONB,
        ] {
            assert_eq!(
                cast_kind(other, oid::CHAR),
                None,
                "{other} -> \"char\" must not exist"
            );
            assert_eq!(
                cast_kind(oid::CHAR, other),
                None,
                "\"char\" -> {other} must not exist"
            );
        }
    }

    /// `"char"`'s six real casts to/from the genuine string types, tabulated
    /// explicitly since the fallback cannot serve them. `"char" -> text` is
    /// implicit; the rest — both directions against bpchar and varchar — are
    /// assignment. Verified against a live pg_cast.
    #[test]
    fn char_casts_to_real_string_types_are_tabulated_explicitly() {
        assert_eq!(cast_kind(oid::CHAR, oid::TEXT), Some(CastKind::Implicit));
        for other in [oid::BPCHAR, oid::VARCHAR] {
            assert_eq!(
                cast_kind(oid::CHAR, other),
                Some(CastKind::Assignment),
                "\"char\" -> {other}"
            );
        }
        for from in [oid::TEXT, oid::BPCHAR, oid::VARCHAR] {
            assert_eq!(
                cast_kind(from, oid::CHAR),
                Some(CastKind::Assignment),
                "{from} -> \"char\""
            );
        }
    }

    /// `name` <-> `"char"` has no `pg_cast` row at all — both directions are
    /// served by the fallback, and the two directions land in *different*
    /// contexts because the fallback checks source and target independently:
    /// `name -> "char"` only qualifies via `is_string_category(name)` (source
    /// check, explicit), while `"char" -> name` qualifies via
    /// `is_string_category(name)` on the *target* side (assignment). Verified
    /// live: `INSERT` into a `"char"` column from a `name` value is rejected,
    /// but `INSERT` into a `name` column from a `"char"` value succeeds.
    #[test]
    fn name_and_char_are_asymmetric_with_no_pg_cast_row() {
        assert_eq!(cast_kind(oid::NAME, oid::CHAR), Some(CastKind::Explicit));
        assert!(
            !can_coerce(
                PgType::new(oid::NAME),
                PgType::new(oid::CHAR),
                CastContext::Assignment
            ),
            "INSERT INTO t(charcol) VALUES (namecol) is an error in Postgres"
        );

        assert_eq!(cast_kind(oid::CHAR, oid::NAME), Some(CastKind::Assignment));
        assert!(
            can_coerce(
                PgType::new(oid::CHAR),
                PgType::new(oid::NAME),
                CastContext::Assignment
            ),
            "INSERT INTO t(namecol) VALUES (charcol) is legal in Postgres"
        );
    }

    /// bytea is category 'U', not 'S'. The integer<->bytea reinterpretations are
    /// explicit both ways, and text -> bytea takes the fallback rather than one
    /// of these rows.
    #[test]
    fn integer_and_bytea_reinterpret_only_explicitly() {
        for i in [oid::INT2, oid::INT4, oid::INT8] {
            assert_eq!(cast_kind(i, oid::BYTEA), Some(CastKind::Explicit));
            assert_eq!(cast_kind(oid::BYTEA, i), Some(CastKind::Explicit));
        }
        assert_eq!(cast_kind(oid::TEXT, oid::BYTEA), Some(CastKind::Explicit));
        assert_eq!(cast_kind(oid::BYTEA, oid::TEXT), Some(CastKind::Assignment));
    }
}
