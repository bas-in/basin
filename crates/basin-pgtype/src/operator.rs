//! Postgres's builtin operator table — `pg_catalog.pg_operator` — and
//! resolution of an operator name plus argument types to the operator it
//! means.
//!
//! # Why this exists
//!
//! `crates/basin-engine/src/pg_operators.rs` currently turns `~`, `@>`, `<@`,
//! `&&`, `#>>`, `@@` and friends into function calls by rewriting the *SQL
//! text* before it is parsed. Its own header admits this breaks on
//! dollar-quoted strings, comments and quoted identifiers, because there is no
//! operator catalog underneath it to look these up in properly — string
//! rewriting is the only tool available without one. This module is that
//! catalog. Once a planner can call [`resolve`], the rewriting pass has
//! nothing left to do.
//!
//! It is also, verbatim, `pg_catalog.pg_operator`: the same table serves
//! wire-protocol/catalog fidelity (`\do`, JDBC/psycopg introspection, ORM
//! codegen) and the planner's own operator resolution at once. See
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md`.
//!
//! # Where these values come from
//!
//! Every row was checked against a live PostgreSQL 18 `pg_operator`, not
//! recalled from memory — the cast table in [`crate::cast`] caught five wrong
//! entries this way and operators are no less error-prone. The query:
//!
//! ```sql
//! SELECT o.oid, o.oprname, lt.typname, rt.typname, rest.typname
//!   FROM pg_operator o
//!   LEFT JOIN pg_type lt ON lt.oid = o.oprleft
//!   LEFT JOIN pg_type rt ON rt.oid = o.oprright
//!   JOIN pg_type rest ON rest.oid = o.oprresult
//!  WHERE o.oprnamespace = 11
//!  ORDER BY o.oid;
//! ```
//!
//! (namespace 11 is `pg_catalog`; `oprleft` is NULL for a prefix operator,
//! which is why the join is a `LEFT JOIN`). Re-run it — filtered to the
//! operator names and types below — before editing this table.
//!
//! # What is deliberately absent
//!
//! Postgres ships roughly 800 builtin operators; this module does not attempt
//! all of them. It covers the categories that unblock retiring
//! `pg_operators.rs`'s textual rewriting: comparison and arithmetic over the
//! scalar types, text concatenation and pattern matching, and the JSONB/array
//! operators that file currently rewrites by hand. Anything else resolves to
//! `None` today, same as it would if the name were simply misspelled.
//!
//! Overload resolution here is also narrower than Postgres's full algorithm
//! (`oper.c`'s `oper_select_candidate`): [`resolve`] takes the first operator
//! whose declared arguments the caller's arguments implicitly coerce to,
//! rather than Postgres's tie-breaking over *all* implicit candidates by
//! "most exact matches" / preferred-type / unknown-literal rules. For the
//! non-overloaded builtin comparison and arithmetic operators below —
//! Postgres never defines two competing implicit paths from the same input
//! types to the same operator name — first-match and best-match agree.
//!
//! Cross-type comparisons and arithmetic (`int4 = int8`, `int2 + int4`, …)
//! *are* tabulated as their own rows below, because Postgres really does mint
//! a separate operator oid for many of these pairs — `int4 = int8` is oid 15,
//! not a cast of one side onto the other (its mirror image, `int8 = int4`, is
//! the *different* oid 416 — easy to get backwards, and initially gotten
//! backwards in this module, without checking a live server). Confirmed live
//! with the query at the top of this module restricted to `lt.typname <>
//! rt.typname` and to `int2`/`int4`/`int8`/`float4`/`float8`/`numeric`:
//!
//! ```sql
//! SELECT o.oid, o.oprname, lt.typname, rt.typname, rest.typname
//!   FROM pg_operator o
//!   JOIN pg_type lt ON lt.oid = o.oprleft
//!   JOIN pg_type rt ON rt.oid = o.oprright
//!   JOIN pg_type rest ON rest.oid = o.oprresult
//!  WHERE o.oprnamespace = 11
//!    AND lt.typname <> rt.typname
//!    AND lt.typname IN ('int2','int4','int8','float4','float8','numeric')
//!    AND rt.typname IN ('int2','int4','int8','float4','float8','numeric')
//!  ORDER BY o.oprname, o.oid;
//! ```
//!
//! Run live, for the ten operators covered here (`= <> < <= > >= + - * /`)
//! this returns cross-type rows only *within* the integer family (`int2` ×
//! `int4`, `int2` × `int8`, `int4` × `int8`, both directions) and *within*
//! the float family (`float4` × `float8`) — 80 rows in total (48 comparison,
//! 32 arithmetic), all tabulated below. The same unfiltered query also
//! surfaces `<<`/`>>` bit-shift rows sharing these integer types (e.g. oid
//! 1878, `int2 << int4`); those are out of scope for this module (comparison
//! and arithmetic only, per the module docs above) and are not included.
//!
//! It returns **no** rows pairing `numeric` with any other type, and **no**
//! rows pairing an integer type with a float type (`int4 = float8` and
//! friends are absent). Postgres itself has no native operator for those
//! pairs and reaches them only by implicitly casting one side — exactly
//! [`resolve`]'s coercion fallback via [`crate::cast`], which this table
//! still leans on for every pair not listed above. `int4 = float8` resolves
//! this way, by widening the int4 to float8 and landing on the `float8 =
//! float8` row. `numeric` mixed with an integer type also falls back this
//! way, but — because [`resolve`]'s fallback is first-match, not
//! best-match, and every integer type implicitly reaches `float4` while
//! `numeric` only reaches `int2`/`int4`/`int8` via an *assignment* (not
//! implicit) cast — it lands on `float4 = float4`, not `numeric = numeric`,
//! same as it did before this table grew any cross-type rows. That is a
//! preexisting quirk of the fallback's tie-breaking, not something this
//! change introduces or is scoped to fix; the coercion tests below pin the
//! current (surprising but unchanged) behavior rather than paper over it.
//! None of this is a gap to close later by tabulating more rows — there is
//! no such native Postgres row to copy for `numeric` mixed with anything.
//!
//! # Array and JSONB path operators are genuinely polymorphic
//!
//! `@>`, `<@` and `&&` on arrays are declared in Postgres on the pseudo-type
//! `anyarray`; `||` on arrays is declared on `anycompatiblearray`. There is
//! **one** physical operator OID shared by every concrete array type — Postgres
//! does not mint a fresh oid per element type the way it does for, say,
//! `int4 = int4` vs `int8 = int8`. Confirmed live:
//!
//! ```sql
//! SELECT oprname, oprleft::regtype, oprright::regtype
//!   FROM pg_operator WHERE oid IN (2751, 2752, 2750, 375);
//! -- ||  | anycompatiblearray | anycompatiblearray
//! -- &&  | anyarray           | anyarray
//! -- @>  | anyarray           | anyarray
//! -- <@  | anyarray           | anyarray
//! ```
//!
//! [`OperatorSig`] has no room for a polymorphic pseudo-type, so below each of
//! these appears as several *monomorphized* rows — `int4[] @> int4[]`,
//! `text[] @> text[]`, and so on — that all legitimately carry the same real
//! oid, because that is the oid Postgres itself resolves to no matter which
//! concrete array type you use. This is not guessing an oid per type; it is
//! the one true oid, repeated. Any array type not given a row here (e.g.
//! `uuid[] @> uuid[]`) simply is not covered yet, not covered wrongly.
//!
//! `#>` and `#>>` (JSONB path extraction) take a `text[]` path argument;
//! that argument is `text[]` regardless of what is inside the JSONB, so those
//! two rows are not an instance of this polymorphism problem.

use crate::{
    cast::{cast_kind, CastKind},
    oid, Oid,
};

/// One row of `pg_catalog.pg_operator`: an operator name plus the argument
/// and result types for one specific overload.
///
/// `left` is `None` for a prefix operator (Postgres's `oprkind = 'l'`,
/// `oprleft = 0`) — e.g. unary `-5`. Every operator in Postgres has a right
/// operand; there is no such thing as a postfix-only builtin left in modern
/// Postgres, so `right` is a plain [`Oid`] rather than `Option<Oid>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorSig {
    /// `pg_operator.oid`. Stable across releases, part of the same wire/catalog
    /// contract as the type OIDs in [`crate::oid`].
    pub oid: Oid,
    /// `pg_operator.oprname`, in Postgres's own internal spelling — `~~` for
    /// `LIKE`, not the SQL keyword. The parser desugars `a LIKE b` to `a ~~ b`
    /// before this table is ever consulted, same as Postgres itself does.
    pub name: &'static str,
    /// `pg_operator.oprleft`, or `None` for a prefix operator.
    pub left: Option<Oid>,
    /// `pg_operator.oprright`.
    pub right: Oid,
    /// `pg_operator.oprresult`.
    pub result: Oid,
}

impl OperatorSig {
    const fn binary(
        oid: u32,
        name: &'static str,
        left: Oid,
        right: Oid,
        result: Oid,
    ) -> OperatorSig {
        OperatorSig {
            oid: Oid(oid),
            name,
            left: Some(left),
            right,
            result,
        }
    }

    const fn prefix(oid: u32, name: &'static str, right: Oid, result: Oid) -> OperatorSig {
        OperatorSig {
            oid: Oid(oid),
            name,
            left: None,
            right,
            result,
        }
    }
}

/// The builtin operators this crate knows about. See the module docs for
/// scope and the query used to verify every row.
pub static OPERATORS: &[OperatorSig] = &[
    // ─── Comparison ─────────────────────────────────────────────────────────
    //
    // `=`, `<>`, `<`, `<=`, `>`, `>=`, one row per type, always returning
    // `bool`. The real cross-type rows Postgres has among int2/int4/int8 and
    // float4/float8 are tabulated separately below, after this block — see
    // the module docs for the query that found them and why `numeric` and
    // int/float pairs are not among them.
    OperatorSig::binary(91, "=", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(85, "<>", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(58, "<", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(1694, "<=", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(59, ">", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(1695, ">=", oid::BOOL, oid::BOOL, oid::BOOL),
    OperatorSig::binary(94, "=", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(519, "<>", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(95, "<", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(522, "<=", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(520, ">", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(524, ">=", oid::INT2, oid::INT2, oid::BOOL),
    OperatorSig::binary(96, "=", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(518, "<>", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(97, "<", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(523, "<=", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(521, ">", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(525, ">=", oid::INT4, oid::INT4, oid::BOOL),
    OperatorSig::binary(410, "=", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(411, "<>", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(412, "<", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(414, "<=", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(413, ">", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(415, ">=", oid::INT8, oid::INT8, oid::BOOL),
    OperatorSig::binary(620, "=", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(621, "<>", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(622, "<", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(624, "<=", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(623, ">", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(625, ">=", oid::FLOAT4, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(670, "=", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(671, "<>", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(672, "<", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(673, "<=", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(674, ">", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(675, ">=", oid::FLOAT8, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1752, "=", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(1753, "<>", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(1754, "<", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(1755, "<=", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(1756, ">", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(1757, ">=", oid::NUMERIC, oid::NUMERIC, oid::BOOL),
    OperatorSig::binary(98, "=", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(531, "<>", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(664, "<", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(665, "<=", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(666, ">", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(667, ">=", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(1093, "=", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(1094, "<>", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(1095, "<", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(1096, "<=", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(1097, ">", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(1098, ">=", oid::DATE, oid::DATE, oid::BOOL),
    OperatorSig::binary(2060, "=", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2061, "<>", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2062, "<", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2063, "<=", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2064, ">", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2065, ">=", oid::TIMESTAMP, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(1320, "=", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(1321, "<>", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(1322, "<", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(1323, "<=", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(1324, ">", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(1325, ">=", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2972, "=", oid::UUID, oid::UUID, oid::BOOL),
    OperatorSig::binary(2973, "<>", oid::UUID, oid::UUID, oid::BOOL),
    OperatorSig::binary(2974, "<", oid::UUID, oid::UUID, oid::BOOL),
    OperatorSig::binary(2976, "<=", oid::UUID, oid::UUID, oid::BOOL),
    OperatorSig::binary(2975, ">", oid::UUID, oid::UUID, oid::BOOL),
    OperatorSig::binary(2977, ">=", oid::UUID, oid::UUID, oid::BOOL),
    // `bpchar` (blank-padded `char(n)`) gets its own comparison oids, unlike
    // `varchar` — confirmed live: `varchar` has **zero** native operator rows
    // at all (`SELECT ... WHERE oprleft = 'varchar'::regtype OR oprright =
    // 'varchar'::regtype` returns nothing), so `varchar = varchar` only ever
    // resolves through [`resolve`]'s coercion fallback (`VARCHAR -> TEXT` is
    // implicit, per `crate::cast`) onto the `text = text` row above. That is
    // not a gap to fill; there is no native row to copy.
    OperatorSig::binary(1054, "=", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    OperatorSig::binary(1057, "<>", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    OperatorSig::binary(1058, "<", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    OperatorSig::binary(1059, "<=", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    OperatorSig::binary(1060, ">", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    OperatorSig::binary(1061, ">=", oid::BPCHAR, oid::BPCHAR, oid::BOOL),
    // `bytea` comparison. Confirmed live alongside `||` (bytea concatenation,
    // tabulated in the text/pattern-matching section below) and the byte
    // pattern-match operators `~~`/`!~~`, which are out of scope per the
    // module docs (comparison and arithmetic only).
    OperatorSig::binary(1955, "=", oid::BYTEA, oid::BYTEA, oid::BOOL),
    OperatorSig::binary(1956, "<>", oid::BYTEA, oid::BYTEA, oid::BOOL),
    OperatorSig::binary(1957, "<", oid::BYTEA, oid::BYTEA, oid::BOOL),
    OperatorSig::binary(1958, "<=", oid::BYTEA, oid::BYTEA, oid::BOOL),
    OperatorSig::binary(1959, ">", oid::BYTEA, oid::BYTEA, oid::BOOL),
    OperatorSig::binary(1960, ">=", oid::BYTEA, oid::BYTEA, oid::BOOL),
    // `time`, `interval` and `timetz` own-type comparison. Each has real,
    // distinct oids in the live catalog (checked individually, not inferred
    // from the `date`/`timestamp`/`timestamptz` pattern above).
    OperatorSig::binary(1108, "=", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1109, "<>", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1110, "<", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1111, "<=", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1112, ">", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1113, ">=", oid::TIME, oid::TIME, oid::BOOL),
    OperatorSig::binary(1330, "=", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1331, "<>", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1332, "<", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1333, "<=", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1334, ">", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1335, ">=", oid::INTERVAL, oid::INTERVAL, oid::BOOL),
    OperatorSig::binary(1550, "=", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    OperatorSig::binary(1551, "<>", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    OperatorSig::binary(1552, "<", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    OperatorSig::binary(1553, "<=", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    OperatorSig::binary(1554, ">", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    OperatorSig::binary(1555, ">=", oid::TIMETZ, oid::TIMETZ, oid::BOOL),
    // ─── Cross-type date/timestamp/timestamptz comparison ──────────────────
    //
    // Same asymmetric-oid pattern as the int2/int4/int8/float4/float8 block
    // below, and just as easy to get backwards: `date = timestamp` (oid 2347)
    // is NOT the same oid as `timestamp = date` (oid 2373). Confirmed live:
    //
    // ```sql
    // SELECT o.oid, o.oprname, lt.typname, rt.typname, ot.typname
    //   FROM pg_operator o
    //   JOIN pg_type lt ON lt.oid = o.oprleft
    //   JOIN pg_type rt ON rt.oid = o.oprright
    //   JOIN pg_type ot ON ot.oid = o.oprresult
    //  WHERE o.oprname IN ('=','<>','<','<=','>','>=')
    //    AND lt.typname IN ('date','timestamp','timestamptz')
    //    AND rt.typname IN ('date','timestamp','timestamptz')
    //    AND lt.typname <> rt.typname
    //  ORDER BY o.oprname, lt.typname, rt.typname;
    // ```
    //
    // 36 rows: 6 operators × 3 unordered type pairs × 2 directions.
    OperatorSig::binary(2347, "=", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2373, "=", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2360, "=", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2386, "=", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2536, "=", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2542, "=", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2350, "<>", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2376, "<>", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2363, "<>", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2389, "<>", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2539, "<>", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2545, "<>", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2345, "<", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2371, "<", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2358, "<", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2384, "<", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2534, "<", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2540, "<", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2346, "<=", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2372, "<=", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2359, "<=", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2385, "<=", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2535, "<=", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2541, "<=", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2349, ">", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2375, ">", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2362, ">", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2388, ">", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2538, ">", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2544, ">", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2348, ">=", oid::DATE, oid::TIMESTAMP, oid::BOOL),
    OperatorSig::binary(2374, ">=", oid::TIMESTAMP, oid::DATE, oid::BOOL),
    OperatorSig::binary(2361, ">=", oid::DATE, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2387, ">=", oid::TIMESTAMPTZ, oid::DATE, oid::BOOL),
    OperatorSig::binary(2537, ">=", oid::TIMESTAMP, oid::TIMESTAMPTZ, oid::BOOL),
    OperatorSig::binary(2543, ">=", oid::TIMESTAMPTZ, oid::TIMESTAMP, oid::BOOL),
    // ─── Cross-type comparison ──────────────────────────────────────────────
    //
    // Real Postgres OIDs for `int2`/`int4`/`int8` compared pairwise, and
    // `float4` compared with `float8` — see the module docs for the query
    // that produced these and why `numeric` and int/float pairs are not
    // among them (Postgres has no such native rows; those still resolve via
    // `resolve`'s coercion fallback).
    OperatorSig::binary(532, "=", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(533, "=", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1862, "=", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1868, "=", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(15, "=", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(416, "=", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1120, "=", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1130, "=", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(538, "<>", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(539, "<>", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1863, "<>", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1869, "<>", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(36, "<>", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(417, "<>", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1121, "<>", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1131, "<>", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(534, "<", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(535, "<", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1864, "<", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1870, "<", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(37, "<", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(418, "<", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1122, "<", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1132, "<", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(540, "<=", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(541, "<=", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1866, "<=", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1872, "<=", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(80, "<=", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(420, "<=", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1124, "<=", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1134, "<=", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(536, ">", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(537, ">", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1865, ">", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1871, ">", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(76, ">", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(419, ">", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1123, ">", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1133, ">", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    OperatorSig::binary(542, ">=", oid::INT2, oid::INT4, oid::BOOL),
    OperatorSig::binary(543, ">=", oid::INT4, oid::INT2, oid::BOOL),
    OperatorSig::binary(1867, ">=", oid::INT2, oid::INT8, oid::BOOL),
    OperatorSig::binary(1873, ">=", oid::INT8, oid::INT2, oid::BOOL),
    OperatorSig::binary(82, ">=", oid::INT4, oid::INT8, oid::BOOL),
    OperatorSig::binary(430, ">=", oid::INT8, oid::INT4, oid::BOOL),
    OperatorSig::binary(1125, ">=", oid::FLOAT4, oid::FLOAT8, oid::BOOL),
    OperatorSig::binary(1135, ">=", oid::FLOAT8, oid::FLOAT4, oid::BOOL),
    // ─── Arithmetic ─────────────────────────────────────────────────────────
    //
    // `+ - * /` for every numeric type; `%` (modulo) only for the integer
    // types and numeric — Postgres has no float modulo operator at all, which
    // is why there is no float4/float8 row here (not an omission). The real
    // cross-type rows Postgres has among int2/int4/int8/float4/float8 are
    // tabulated separately below, after this block — see the module docs.
    OperatorSig::binary(550, "+", oid::INT2, oid::INT2, oid::INT2),
    OperatorSig::binary(554, "-", oid::INT2, oid::INT2, oid::INT2),
    OperatorSig::binary(526, "*", oid::INT2, oid::INT2, oid::INT2),
    OperatorSig::binary(527, "/", oid::INT2, oid::INT2, oid::INT2),
    OperatorSig::binary(529, "%", oid::INT2, oid::INT2, oid::INT2),
    OperatorSig::binary(551, "+", oid::INT4, oid::INT4, oid::INT4),
    OperatorSig::binary(555, "-", oid::INT4, oid::INT4, oid::INT4),
    OperatorSig::binary(514, "*", oid::INT4, oid::INT4, oid::INT4),
    OperatorSig::binary(528, "/", oid::INT4, oid::INT4, oid::INT4),
    OperatorSig::binary(530, "%", oid::INT4, oid::INT4, oid::INT4),
    OperatorSig::binary(684, "+", oid::INT8, oid::INT8, oid::INT8),
    OperatorSig::binary(685, "-", oid::INT8, oid::INT8, oid::INT8),
    OperatorSig::binary(686, "*", oid::INT8, oid::INT8, oid::INT8),
    OperatorSig::binary(687, "/", oid::INT8, oid::INT8, oid::INT8),
    OperatorSig::binary(439, "%", oid::INT8, oid::INT8, oid::INT8),
    OperatorSig::binary(586, "+", oid::FLOAT4, oid::FLOAT4, oid::FLOAT4),
    OperatorSig::binary(587, "-", oid::FLOAT4, oid::FLOAT4, oid::FLOAT4),
    OperatorSig::binary(589, "*", oid::FLOAT4, oid::FLOAT4, oid::FLOAT4),
    OperatorSig::binary(588, "/", oid::FLOAT4, oid::FLOAT4, oid::FLOAT4),
    OperatorSig::binary(591, "+", oid::FLOAT8, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(592, "-", oid::FLOAT8, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(594, "*", oid::FLOAT8, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(593, "/", oid::FLOAT8, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(1758, "+", oid::NUMERIC, oid::NUMERIC, oid::NUMERIC),
    OperatorSig::binary(1759, "-", oid::NUMERIC, oid::NUMERIC, oid::NUMERIC),
    OperatorSig::binary(1760, "*", oid::NUMERIC, oid::NUMERIC, oid::NUMERIC),
    OperatorSig::binary(1761, "/", oid::NUMERIC, oid::NUMERIC, oid::NUMERIC),
    OperatorSig::binary(1762, "%", oid::NUMERIC, oid::NUMERIC, oid::NUMERIC),
    // ─── Cross-type arithmetic ──────────────────────────────────────────────
    //
    // Result type follows Postgres's own promotion: the wider of the two
    // operand types, confirmed per-row against the live catalog (see the
    // module docs for the query).
    OperatorSig::binary(552, "+", oid::INT2, oid::INT4, oid::INT4),
    OperatorSig::binary(553, "+", oid::INT4, oid::INT2, oid::INT4),
    OperatorSig::binary(822, "+", oid::INT2, oid::INT8, oid::INT8),
    OperatorSig::binary(818, "+", oid::INT8, oid::INT2, oid::INT8),
    OperatorSig::binary(692, "+", oid::INT4, oid::INT8, oid::INT8),
    OperatorSig::binary(688, "+", oid::INT8, oid::INT4, oid::INT8),
    OperatorSig::binary(1116, "+", oid::FLOAT4, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(1126, "+", oid::FLOAT8, oid::FLOAT4, oid::FLOAT8),
    OperatorSig::binary(556, "-", oid::INT2, oid::INT4, oid::INT4),
    OperatorSig::binary(557, "-", oid::INT4, oid::INT2, oid::INT4),
    OperatorSig::binary(823, "-", oid::INT2, oid::INT8, oid::INT8),
    OperatorSig::binary(819, "-", oid::INT8, oid::INT2, oid::INT8),
    OperatorSig::binary(693, "-", oid::INT4, oid::INT8, oid::INT8),
    OperatorSig::binary(689, "-", oid::INT8, oid::INT4, oid::INT8),
    OperatorSig::binary(1117, "-", oid::FLOAT4, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(1127, "-", oid::FLOAT8, oid::FLOAT4, oid::FLOAT8),
    OperatorSig::binary(544, "*", oid::INT2, oid::INT4, oid::INT4),
    OperatorSig::binary(545, "*", oid::INT4, oid::INT2, oid::INT4),
    OperatorSig::binary(824, "*", oid::INT2, oid::INT8, oid::INT8),
    OperatorSig::binary(820, "*", oid::INT8, oid::INT2, oid::INT8),
    OperatorSig::binary(694, "*", oid::INT4, oid::INT8, oid::INT8),
    OperatorSig::binary(690, "*", oid::INT8, oid::INT4, oid::INT8),
    OperatorSig::binary(1119, "*", oid::FLOAT4, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(1129, "*", oid::FLOAT8, oid::FLOAT4, oid::FLOAT8),
    OperatorSig::binary(546, "/", oid::INT2, oid::INT4, oid::INT4),
    OperatorSig::binary(547, "/", oid::INT4, oid::INT2, oid::INT4),
    OperatorSig::binary(825, "/", oid::INT2, oid::INT8, oid::INT8),
    OperatorSig::binary(821, "/", oid::INT8, oid::INT2, oid::INT8),
    OperatorSig::binary(695, "/", oid::INT4, oid::INT8, oid::INT8),
    OperatorSig::binary(691, "/", oid::INT8, oid::INT4, oid::INT8),
    OperatorSig::binary(1118, "/", oid::FLOAT4, oid::FLOAT8, oid::FLOAT8),
    OperatorSig::binary(1128, "/", oid::FLOAT8, oid::FLOAT4, oid::FLOAT8),
    // ─── Date/time arithmetic ───────────────────────────────────────────────
    //
    // Confirmed live by oid, one query rather than one row at a time (see the
    // `SELECT ... WHERE o.oid IN (...)` form used throughout this module's
    // development). Two things worth flagging because they are *not* what a
    // "tidy" table would predict:
    //
    //   * `date + interval` and `interval + date` do NOT return `date` — they
    //     return `timestamp`, because an `interval` can carry a sub-day
    //     component the `date` type cannot hold. Writing a `date` result type
    //     here would be a silent precision-losing bug baked into the catalog
    //     row itself.
    //   * `date - date` returns `int4` (a day count), not `interval` — the
    //     one place in this block where subtracting two like-typed values
    //     does not produce a duration type.
    //
    // `time`/`timestamp`/`timestamptz` each follow the more predictable
    // pattern (`T + interval -> T`, `T - interval -> T`, `T - T -> interval`).
    OperatorSig::binary(1100, "+", oid::DATE, oid::INT4, oid::DATE),
    OperatorSig::binary(2555, "+", oid::INT4, oid::DATE, oid::DATE),
    OperatorSig::binary(1101, "-", oid::DATE, oid::INT4, oid::DATE),
    OperatorSig::binary(1099, "-", oid::DATE, oid::DATE, oid::INT4),
    OperatorSig::binary(1076, "+", oid::DATE, oid::INTERVAL, oid::TIMESTAMP),
    OperatorSig::binary(2551, "+", oid::INTERVAL, oid::DATE, oid::TIMESTAMP),
    OperatorSig::binary(1077, "-", oid::DATE, oid::INTERVAL, oid::TIMESTAMP),
    OperatorSig::binary(1800, "+", oid::TIME, oid::INTERVAL, oid::TIME),
    OperatorSig::binary(1849, "+", oid::INTERVAL, oid::TIME, oid::TIME),
    OperatorSig::binary(1801, "-", oid::TIME, oid::INTERVAL, oid::TIME),
    OperatorSig::binary(1399, "-", oid::TIME, oid::TIME, oid::INTERVAL),
    OperatorSig::binary(2066, "+", oid::TIMESTAMP, oid::INTERVAL, oid::TIMESTAMP),
    OperatorSig::binary(2553, "+", oid::INTERVAL, oid::TIMESTAMP, oid::TIMESTAMP),
    OperatorSig::binary(2068, "-", oid::TIMESTAMP, oid::INTERVAL, oid::TIMESTAMP),
    OperatorSig::binary(2067, "-", oid::TIMESTAMP, oid::TIMESTAMP, oid::INTERVAL),
    OperatorSig::binary(1327, "+", oid::TIMESTAMPTZ, oid::INTERVAL, oid::TIMESTAMPTZ),
    OperatorSig::binary(2554, "+", oid::INTERVAL, oid::TIMESTAMPTZ, oid::TIMESTAMPTZ),
    OperatorSig::binary(1329, "-", oid::TIMESTAMPTZ, oid::INTERVAL, oid::TIMESTAMPTZ),
    OperatorSig::binary(1328, "-", oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::INTERVAL),
    OperatorSig::binary(1337, "+", oid::INTERVAL, oid::INTERVAL, oid::INTERVAL),
    OperatorSig::binary(1338, "-", oid::INTERVAL, oid::INTERVAL, oid::INTERVAL),
    OperatorSig::binary(1583, "*", oid::INTERVAL, oid::FLOAT8, oid::INTERVAL),
    OperatorSig::binary(1585, "/", oid::INTERVAL, oid::FLOAT8, oid::INTERVAL),
    // Unary minus (`oprkind = 'l'`, prefix). Exercises the `left: None`
    // half of `OperatorSig` — negation is Postgres's only common builtin
    // prefix operator over these types.
    OperatorSig::prefix(559, "-", oid::INT2, oid::INT2),
    OperatorSig::prefix(558, "-", oid::INT4, oid::INT4),
    OperatorSig::prefix(484, "-", oid::INT8, oid::INT8),
    OperatorSig::prefix(584, "-", oid::FLOAT4, oid::FLOAT4),
    OperatorSig::prefix(585, "-", oid::FLOAT8, oid::FLOAT8),
    OperatorSig::prefix(1751, "-", oid::NUMERIC, oid::NUMERIC),
    OperatorSig::prefix(1336, "-", oid::INTERVAL, oid::INTERVAL),
    // ─── Text concatenation and pattern matching ───────────────────────────
    OperatorSig::binary(654, "||", oid::TEXT, oid::TEXT, oid::TEXT),
    // `bytea` concatenation — real oid 2018, confirmed alongside the `bytea`
    // comparison rows above. Byte-string equivalent of `text || text`.
    OperatorSig::binary(2018, "||", oid::BYTEA, oid::BYTEA, oid::BYTEA),
    // POSIX regex, case-sensitive / case-insensitive, and negated forms.
    OperatorSig::binary(641, "~", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(1228, "~*", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(642, "!~", oid::TEXT, oid::TEXT, oid::BOOL),
    OperatorSig::binary(1229, "!~*", oid::TEXT, oid::TEXT, oid::BOOL),
    // `LIKE` desugars to `~~` before it reaches this table — see the `name`
    // field docs on `OperatorSig`.
    OperatorSig::binary(1209, "~~", oid::TEXT, oid::TEXT, oid::BOOL),
    // ─── JSONB ──────────────────────────────────────────────────────────────
    //
    // `->` and `->>` differ ONLY in result type (jsonb vs text) and are the
    // classic source of bugs when hand-rewritten as text, per the module
    // docs — `resolve_dash_gt_yields_jsonb_not_text` below pins the
    // distinction. Each has both a text-key and an integer-index overload
    // (`obj -> 'k'` vs `arr -> 0`).
    OperatorSig::binary(3211, "->", oid::JSONB, oid::TEXT, oid::JSONB),
    OperatorSig::binary(3212, "->", oid::JSONB, oid::INT4, oid::JSONB),
    OperatorSig::binary(3477, "->>", oid::JSONB, oid::TEXT, oid::TEXT),
    OperatorSig::binary(3481, "->>", oid::JSONB, oid::INT4, oid::TEXT),
    // Path variants take a `text[]` path, regardless of what the JSONB
    // contains.
    OperatorSig::binary(3213, "#>", oid::JSONB, oid::TEXT_ARRAY, oid::JSONB),
    OperatorSig::binary(3206, "#>>", oid::JSONB, oid::TEXT_ARRAY, oid::TEXT),
    OperatorSig::binary(3246, "@>", oid::JSONB, oid::JSONB, oid::BOOL),
    OperatorSig::binary(3250, "<@", oid::JSONB, oid::JSONB, oid::BOOL),
    OperatorSig::binary(3247, "?", oid::JSONB, oid::TEXT, oid::BOOL),
    OperatorSig::binary(3248, "?|", oid::JSONB, oid::TEXT_ARRAY, oid::BOOL),
    OperatorSig::binary(3249, "?&", oid::JSONB, oid::TEXT_ARRAY, oid::BOOL),
    // ─── Array ──────────────────────────────────────────────────────────────
    //
    // `@>`, `<@`, `&&` (`anyarray`) and `||` (`anycompatiblearray`) are truly
    // polymorphic in Postgres — one physical oid serves every concrete array
    // type. The rows below monomorphize that one oid at a handful of concrete
    // element types; see the module docs for why that is faithful rather than
    // invented, and why the set of covered element types is incomplete by
    // construction.
    OperatorSig::binary(2751, "@>", oid::INT4_ARRAY, oid::INT4_ARRAY, oid::BOOL),
    OperatorSig::binary(2751, "@>", oid::TEXT_ARRAY, oid::TEXT_ARRAY, oid::BOOL),
    OperatorSig::binary(2752, "<@", oid::INT4_ARRAY, oid::INT4_ARRAY, oid::BOOL),
    OperatorSig::binary(2752, "<@", oid::TEXT_ARRAY, oid::TEXT_ARRAY, oid::BOOL),
    OperatorSig::binary(2750, "&&", oid::INT4_ARRAY, oid::INT4_ARRAY, oid::BOOL),
    OperatorSig::binary(2750, "&&", oid::TEXT_ARRAY, oid::TEXT_ARRAY, oid::BOOL),
    OperatorSig::binary(375, "||", oid::INT4_ARRAY, oid::INT4_ARRAY, oid::INT4_ARRAY),
    OperatorSig::binary(375, "||", oid::TEXT_ARRAY, oid::TEXT_ARRAY, oid::TEXT_ARRAY),
];

/// Resolve an operator invocation to the [`OperatorSig`] Postgres would pick.
///
/// `left` is `None` for a prefix use (`-x`); `right` is always given, since
/// every builtin operator has a right operand.
///
/// Mirrors Postgres's two-phase approach in `oper.c`: first look for an exact
/// type match, then look for a candidate reachable by *implicit* coercion of
/// the given argument types (`can_coerce_type` with `COERCION_IMPLICIT`,
/// which is exactly [`CastKind::Implicit`] as reported by
/// [`crate::cast::cast_kind`]). An operator whose match requires an
/// assignment-only or explicit-only cast is never chosen implicitly — that
/// mirrors Postgres refusing to silently pick `int4 = int4` for
/// `int8col = 5000000000`, forcing the wider `int8 = int8` match instead. See
/// the module docs for the narrower-than-Postgres tie-breaking this performs
/// among multiple implicit candidates.
pub fn resolve(name: &str, left: Option<Oid>, right: Oid) -> Option<&'static OperatorSig> {
    if let Some(op) = OPERATORS
        .iter()
        .find(|op| op.name == name && op.left == left && op.right == right)
    {
        return Some(op);
    }

    OPERATORS.iter().find(|op| {
        if op.name != name {
            return false;
        }
        let left_matches = match (op.left, left) {
            (None, None) => true,
            (Some(candidate), Some(given)) => {
                cast_kind(given, candidate) == Some(CastKind::Implicit)
            }
            // A prefix operator cannot be reached by supplying a left operand,
            // and a binary operator cannot be reached by omitting one.
            _ => false,
        };
        left_matches && cast_kind(right, op.right) == Some(CastKind::Implicit)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bread-and-butter case: identical types match their own row
    /// without touching the coercion fallback at all.
    #[test]
    fn exact_match_resolves_directly() {
        let op = resolve("=", Some(oid::INT4), oid::INT4).expect("int4 = int4 must resolve");
        assert_eq!(op.oid, Oid(96));
        assert_eq!(op.result, oid::BOOL);
    }

    /// `int4 = int8` has its own real row now (oid 15, confirmed live — see
    /// the module docs; oid 416 is the *reverse* pair, `int8 = int4`, easy
    /// to get backwards without checking a live server), and must resolve to
    /// *that* exact row rather than by implicitly widening the int4 side
    /// onto the `int8 = int8` row (oid 410). Landing on 410 would still
    /// produce the right boolean answer but the wrong plan, exactly the
    /// divergence from Postgres this table now closes.
    #[test]
    fn cross_type_comparison_resolves_to_its_own_real_oid() {
        let op = resolve("=", Some(oid::INT4), oid::INT8).expect("int4 = int8 must resolve");
        assert_eq!(op.oid, Oid(15), "must be int4 = int8's own oid, not int8 = int8 (410) nor the reverse pair int8 = int4 (416)");
        assert_eq!(op.left, Some(oid::INT4));
        assert_eq!(op.right, oid::INT8);
        assert_eq!(op.result, oid::BOOL);

        let reverse = resolve("=", Some(oid::INT8), oid::INT4).expect("int8 = int4 must resolve");
        assert_eq!(
            reverse.oid,
            Oid(416),
            "the reverse pair has its own distinct oid"
        );
    }

    /// `date = timestamp` (oid 2347) and `timestamp = date` (oid 2373) are
    /// the same class of trap as `int4 = int8` / `int8 = int4` above, and
    /// were verified the same way — direct query by oid against a live
    /// server, not inferred from the int/float pattern. Also covers
    /// `date`/`timestamptz` and `timestamp`/`timestamptz`, the other two
    /// asymmetric pairs added in the same pass.
    #[test]
    fn date_timestamp_cross_type_comparison_is_asymmetric() {
        let date_eq_ts = resolve("=", Some(oid::DATE), oid::TIMESTAMP).unwrap();
        let ts_eq_date = resolve("=", Some(oid::TIMESTAMP), oid::DATE).unwrap();
        assert_eq!(date_eq_ts.oid, Oid(2347));
        assert_eq!(ts_eq_date.oid, Oid(2373));
        assert_ne!(date_eq_ts.oid, ts_eq_date.oid);

        let date_eq_tstz = resolve("=", Some(oid::DATE), oid::TIMESTAMPTZ).unwrap();
        let tstz_eq_date = resolve("=", Some(oid::TIMESTAMPTZ), oid::DATE).unwrap();
        assert_eq!(date_eq_tstz.oid, Oid(2360));
        assert_eq!(tstz_eq_date.oid, Oid(2386));
        assert_ne!(date_eq_tstz.oid, tstz_eq_date.oid);

        let ts_eq_tstz = resolve("=", Some(oid::TIMESTAMP), oid::TIMESTAMPTZ).unwrap();
        let tstz_eq_ts = resolve("=", Some(oid::TIMESTAMPTZ), oid::TIMESTAMP).unwrap();
        assert_eq!(ts_eq_tstz.oid, Oid(2536));
        assert_eq!(tstz_eq_ts.oid, Oid(2542));
        assert_ne!(ts_eq_tstz.oid, tstz_eq_ts.oid);
    }

    /// `date + int4` and `int4 + date` both yield `date` but are two
    /// different catalog oids (1100 vs 2555) — a pair that would be easy to
    /// collapse into "one commutative row" without checking the live
    /// catalog. Same trap, arithmetic flavor rather than comparison.
    #[test]
    fn date_plus_int4_is_asymmetric_by_oid() {
        let date_plus_int = resolve("+", Some(oid::DATE), oid::INT4).unwrap();
        let int_plus_date = resolve("+", Some(oid::INT4), oid::DATE).unwrap();
        assert_eq!(date_plus_int.oid, Oid(1100));
        assert_eq!(int_plus_date.oid, Oid(2555));
        assert_ne!(date_plus_int.oid, int_plus_date.oid);
        assert_eq!(date_plus_int.result, oid::DATE);
        assert_eq!(int_plus_date.result, oid::DATE);
    }

    /// `date + interval` and `interval + date` are the sharper version of the
    /// same trap: not only do they carry different oids (1076 vs 2551), the
    /// result is `timestamp`, not `date` — an `interval` can hold a sub-day
    /// component a `date` cannot represent, so Postgres widens the result
    /// type rather than truncating it. A table that "simplified" this to
    /// `date + interval -> date` would be a silent precision-loss bug.
    #[test]
    fn date_plus_interval_widens_to_timestamp_not_date() {
        let date_plus_iv = resolve("+", Some(oid::DATE), oid::INTERVAL).unwrap();
        let iv_plus_date = resolve("+", Some(oid::INTERVAL), oid::DATE).unwrap();
        assert_eq!(date_plus_iv.oid, Oid(1076));
        assert_eq!(iv_plus_date.oid, Oid(2551));
        assert_ne!(date_plus_iv.oid, iv_plus_date.oid);
        assert_eq!(date_plus_iv.result, oid::TIMESTAMP);
        assert_eq!(iv_plus_date.result, oid::TIMESTAMP);
    }

    /// `date - date` is the one place in the date/time arithmetic block where
    /// subtracting two like-typed values does not produce `interval` — it
    /// produces a plain `int4` day count. `time - time`, `timestamp -
    /// timestamp` and `timestamptz - timestamptz` all go to `interval`
    /// instead, so this is the odd one out, not the pattern.
    #[test]
    fn date_minus_date_is_int4_not_interval() {
        let diff = resolve("-", Some(oid::DATE), oid::DATE).unwrap();
        assert_eq!(diff.oid, Oid(1099));
        assert_eq!(diff.result, oid::INT4);

        let time_diff = resolve("-", Some(oid::TIME), oid::TIME).unwrap();
        assert_eq!(time_diff.result, oid::INTERVAL);
        let ts_diff = resolve("-", Some(oid::TIMESTAMP), oid::TIMESTAMP).unwrap();
        assert_eq!(ts_diff.result, oid::INTERVAL);
    }

    /// `bpchar` (`char(n)`) has real comparison oids of its own; `varchar`
    /// has none at all (confirmed live — see the module docs) and reaches
    /// comparison only via the coercion fallback onto `text = text`.
    #[test]
    fn bpchar_has_its_own_comparison_oid() {
        let op = resolve("=", Some(oid::BPCHAR), oid::BPCHAR).expect("bpchar = bpchar");
        assert_eq!(op.oid, Oid(1054));
        assert_eq!(op.result, oid::BOOL);
    }

    /// `bytea` comparison and concatenation, previously entirely absent from
    /// this table.
    #[test]
    fn bytea_comparison_and_concat_resolve() {
        let eq = resolve("=", Some(oid::BYTEA), oid::BYTEA).expect("bytea = bytea");
        assert_eq!(eq.oid, Oid(1955));
        assert_eq!(eq.result, oid::BOOL);

        let concat = resolve("||", Some(oid::BYTEA), oid::BYTEA).expect("bytea || bytea");
        assert_eq!(concat.oid, Oid(2018));
        assert_eq!(concat.result, oid::BYTEA);
    }

    /// Unary minus on `interval` — the one prefix operator added in this
    /// pass, exercising the same `left: None` path as the numeric prefixes.
    #[test]
    fn unary_minus_resolves_for_interval() {
        let neg = resolve("-", None, oid::INTERVAL).expect("unary minus on interval");
        assert_eq!(neg.oid, Oid(1336));
        assert_eq!(neg.result, oid::INTERVAL);
    }

    /// Every new cross-type row's declared result type matches what the live
    /// server reports for that exact oid (see the module docs for the
    /// query): comparisons always `bool`, arithmetic the wider operand type.
    #[test]
    fn cross_type_rows_have_the_servers_result_type() {
        // Comparison: always bool, both directions.
        assert_eq!(
            resolve("=", Some(oid::INT2), oid::INT4).unwrap().result,
            oid::BOOL
        );
        assert_eq!(
            resolve("<", Some(oid::FLOAT8), oid::FLOAT4).unwrap().result,
            oid::BOOL
        );
        // Arithmetic: result widens to the bigger operand type, per row,
        // matching the live catalog exactly (not just "some numeric type").
        let int2_plus_int8 = resolve("+", Some(oid::INT2), oid::INT8).unwrap();
        assert_eq!(int2_plus_int8.oid, Oid(822));
        assert_eq!(int2_plus_int8.result, oid::INT8);

        let int8_times_int4 = resolve("*", Some(oid::INT8), oid::INT4).unwrap();
        assert_eq!(int8_times_int4.oid, Oid(690));
        assert_eq!(int8_times_int4.result, oid::INT8);

        let float4_div_float8 = resolve("/", Some(oid::FLOAT4), oid::FLOAT8).unwrap();
        assert_eq!(float4_div_float8.oid, Oid(1118));
        assert_eq!(float4_div_float8.result, oid::FLOAT8);

        let int4_minus_int2 = resolve("-", Some(oid::INT4), oid::INT2).unwrap();
        assert_eq!(int4_minus_int2.oid, Oid(557));
        assert_eq!(int4_minus_int2.result, oid::INT4);
    }

    /// `numeric` and int/float pairs have no native cross-type row in
    /// Postgres (confirmed live — see the module docs), so these must still
    /// resolve via `resolve`'s implicit-coercion fallback exactly as before
    /// this table grew the int2/int4/int8/float4/float8 rows above. This
    /// guards against the fallback silently breaking for the pairs that
    /// still depend on it.
    #[test]
    fn untabulated_cross_type_pairs_still_resolve_via_coercion_fallback() {
        let numeric_eq_float8 =
            resolve("=", Some(oid::NUMERIC), oid::FLOAT8).expect("numeric = float8 must resolve");
        assert_eq!(
            numeric_eq_float8.oid,
            Oid(670),
            "must land on the float8 = float8 row by widening"
        );

        let int4_plus_float8 =
            resolve("+", Some(oid::INT4), oid::FLOAT8).expect("int4 + float8 must resolve");
        assert_eq!(
            int4_plus_float8.oid,
            Oid(591),
            "must land on the float8 + float8 row by widening"
        );
    }

    /// `text = int4` has no implicit path in either direction — text only
    /// reaches a number via an *explicit* cast (see
    /// `crate::cast::text_to_number_is_explicit_only`) — so this must not
    /// silently resolve to something. A hand-rolled resolver "helpfully"
    /// treating any pg_cast entry as fair game would get this wrong.
    #[test]
    fn no_implicit_path_means_no_resolution() {
        assert_eq!(resolve("=", Some(oid::TEXT), oid::INT4), None);
        assert_eq!(resolve("=", Some(oid::INT4), oid::TEXT), None);
    }

    /// `~` must resolve to a real catalog operator rather than requiring a
    /// caller to fall back to string-rewriting it into a function call — the
    /// entire reason this module exists.
    #[test]
    fn regex_match_resolves_to_a_real_operator() {
        let op = resolve("~", Some(oid::TEXT), oid::TEXT).expect("~ on text must resolve");
        assert_eq!(op.oid, Oid(641));
        assert_eq!(op.result, oid::BOOL);
    }

    /// `LIKE` reaches this table as `~~`, its Postgres-internal spelling.
    #[test]
    fn like_resolves_under_its_internal_name() {
        let op = resolve("~~", Some(oid::TEXT), oid::TEXT).expect("~~ (LIKE) on text must resolve");
        assert_eq!(op.oid, Oid(1209));
    }

    /// `->` and `->>` are a classic confusion in hand-written JSONB rewriting:
    /// one returns another jsonb value, the other returns text. Mixing them
    /// up is a silent type bug, not a crash, which is exactly why it needs a
    /// pinning test rather than just a working example.
    #[test]
    fn jsonb_arrow_and_double_arrow_have_different_result_types() {
        let arrow = resolve("->", Some(oid::JSONB), oid::TEXT).expect("jsonb -> text must resolve");
        let double_arrow =
            resolve("->>", Some(oid::JSONB), oid::TEXT).expect("jsonb ->> text must resolve");
        assert_eq!(arrow.result, oid::JSONB, "-> must stay inside jsonb");
        assert_eq!(double_arrow.result, oid::TEXT, "->> must unwrap to text");
        assert_ne!(arrow.oid, double_arrow.oid);
    }

    /// The integer-index overloads of `->`/`->>` (array element access) are
    /// distinct rows from the text-key overloads (object key access).
    #[test]
    fn jsonb_arrow_has_separate_int_and_text_overloads() {
        let by_key = resolve("->", Some(oid::JSONB), oid::TEXT).unwrap();
        let by_index = resolve("->", Some(oid::JSONB), oid::INT4).unwrap();
        assert_ne!(by_key.oid, by_index.oid);
        assert_eq!(by_key.result, oid::JSONB);
        assert_eq!(by_index.result, oid::JSONB);
    }

    /// `#>` and `#>>` take a `text[]` path irrespective of the JSONB payload,
    /// and split result type the same way `->`/`->>` do.
    #[test]
    fn jsonb_path_operators_take_a_text_array_path() {
        let path = resolve("#>", Some(oid::JSONB), oid::TEXT_ARRAY).expect("#> must resolve");
        let path_text =
            resolve("#>>", Some(oid::JSONB), oid::TEXT_ARRAY).expect("#>> must resolve");
        assert_eq!(path.result, oid::JSONB);
        assert_eq!(path_text.result, oid::TEXT);
    }

    /// JSONB containment: `@>` and `<@` are mirror images (same operands,
    /// opposite direction), both distinct catalog rows.
    #[test]
    fn jsonb_containment_operators_resolve() {
        let contains = resolve("@>", Some(oid::JSONB), oid::JSONB).unwrap();
        let contained_by = resolve("<@", Some(oid::JSONB), oid::JSONB).unwrap();
        assert_ne!(contains.oid, contained_by.oid);
        assert_eq!(contains.result, oid::BOOL);
        assert_eq!(contained_by.result, oid::BOOL);
    }

    /// Array containment/overlap share Postgres's own genuinely polymorphic
    /// oids across element types — see the module docs. Both the int4[] and
    /// text[] instantiations must resolve to the *same* oid, because in real
    /// Postgres they are the same operator.
    #[test]
    fn array_operators_share_one_oid_across_element_types() {
        let int_contains = resolve("@>", Some(oid::INT4_ARRAY), oid::INT4_ARRAY).unwrap();
        let text_contains = resolve("@>", Some(oid::TEXT_ARRAY), oid::TEXT_ARRAY).unwrap();
        assert_eq!(int_contains.oid, text_contains.oid);
        assert_eq!(int_contains.oid, Oid(2751));

        let overlap = resolve("&&", Some(oid::INT4_ARRAY), oid::INT4_ARRAY).unwrap();
        assert_eq!(overlap.oid, Oid(2750));

        let concat = resolve("||", Some(oid::INT4_ARRAY), oid::INT4_ARRAY).unwrap();
        assert_eq!(concat.result, oid::INT4_ARRAY);
    }

    /// Prefix (unary) operators are looked up with `left: None`, and must not
    /// be reachable by supplying a left operand or vice versa.
    #[test]
    fn unary_minus_is_a_prefix_operator() {
        let neg = resolve("-", None, oid::INT4).expect("unary minus on int4 must resolve");
        assert_eq!(neg.oid, Oid(558));
        assert_eq!(neg.left, None);
        assert_eq!(neg.result, oid::INT4);

        // The binary minus (int4 - int4) is a different row and must not be
        // returned for a prefix lookup, nor the prefix row for a binary one.
        let sub =
            resolve("-", Some(oid::INT4), oid::INT4).expect("binary minus on int4 must resolve");
        assert_ne!(neg.oid, sub.oid);
    }

    /// Arithmetic has no float modulo in Postgres at all — confirmed absent
    /// from the live catalog, not merely untabulated here.
    #[test]
    fn float_has_no_modulo_operator() {
        assert_eq!(resolve("%", Some(oid::FLOAT8), oid::FLOAT8), None);
        assert_eq!(resolve("%", Some(oid::FLOAT4), oid::FLOAT4), None);
    }

    /// An operator that plain does not exist under any name must resolve to
    /// `None`, not panic or silently pick something close.
    #[test]
    fn unknown_operator_name_does_not_resolve() {
        assert_eq!(resolve("<=>", Some(oid::INT4), oid::INT4), None);
    }

    /// Pins the total row count so an accidental deletion (or an accidental
    /// duplicate) shows up as a failing test rather than silently shrinking
    /// (or bloating) the table. 211 pre-existing rows (see the module docs
    /// for that count's own derivation) plus 91 rows added in a later pass —
    /// 6 `bpchar` comparisons, 7 `bytea` (6 comparison + `||`), 18 own-type
    /// comparisons for `time`/`interval`/`timetz`, 36 cross-type
    /// `date`/`timestamp`/`timestamptz` comparisons, and 24 date/time
    /// arithmetic rows (including unary minus on `interval`) — is 302. If
    /// this legitimately changes, recount by hand against a live server
    /// before updating the number here.
    #[test]
    fn total_operator_row_count_is_pinned() {
        assert_eq!(
            OPERATORS.len(),
            302,
            "row count changed — update this pin only after confirming the new rows against a live server"
        );
    }

    /// Every row's declared result is a real, representable type — guards
    /// against a copy-paste leaving a result OID from the wrong row.
    #[test]
    fn every_operator_has_a_named_result_type() {
        for op in OPERATORS {
            assert!(
                oid::type_name(op.result).is_some() || oid::is_array(op.result),
                "operator {} ({}) has an unrecognized result oid {}",
                op.name,
                op.oid,
                op.result
            );
        }
    }
}
