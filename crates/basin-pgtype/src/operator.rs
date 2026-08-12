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
//! Cross-type comparisons and arithmetic (`int4 = int8`, `int2 + int4`, …) are
//! deliberately *not* tabulated as their own rows, even though Postgres has
//! real operator OIDs for many of them (`int4 = int8` is oid 416, not a cast
//! of one side). This table has one row per operator name per matching
//! argument type, and leans on [`resolve`]'s implicit-coercion fallback via
//! [`crate::cast`] to reach the cross-type cases — `int4 = int8` resolves by
//! widening the `int4` to `int8` and matching the `int8 = int8` row (oid 410).
//! This is a real, admitted divergence from Postgres's own plan (which would
//! pick oid 416 directly) but produces the same boolean answer, and keeping a
//! single coercion path is far less error-prone than tabulating the full
//! matrix. If Basin ever needs to report the *specific* cross-type operator
//! oid Postgres would have chosen (`pg_operator` introspection, `EXPLAIN`
//! parity), that is a follow-up, not a change to this rule.
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
    // `bool`. Cross-type comparisons (`int4 = int8`) are handled by
    // `resolve`'s implicit-coercion fallback rather than tabulated — see the
    // module docs.
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
    // ─── Arithmetic ─────────────────────────────────────────────────────────
    //
    // `+ - * /` for every numeric type; `%` (modulo) only for the integer
    // types and numeric — Postgres has no float modulo operator at all, which
    // is why there is no float4/float8 row here (not an omission).
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
    // Unary minus (`oprkind = 'l'`, prefix). Exercises the `left: None`
    // half of `OperatorSig` — negation is Postgres's only common builtin
    // prefix operator over these types.
    OperatorSig::prefix(559, "-", oid::INT2, oid::INT2),
    OperatorSig::prefix(558, "-", oid::INT4, oid::INT4),
    OperatorSig::prefix(484, "-", oid::INT8, oid::INT8),
    OperatorSig::prefix(584, "-", oid::FLOAT4, oid::FLOAT4),
    OperatorSig::prefix(585, "-", oid::FLOAT8, oid::FLOAT8),
    OperatorSig::prefix(1751, "-", oid::NUMERIC, oid::NUMERIC),
    // ─── Text concatenation and pattern matching ───────────────────────────
    OperatorSig::binary(654, "||", oid::TEXT, oid::TEXT, oid::TEXT),
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

    /// `int4 = int8` has no row of its own in this table (see the module
    /// docs on why cross-type rows are not tabulated); it must still resolve,
    /// by implicitly widening the int4 side to int8 and landing on the
    /// `int8 = int8` row.
    #[test]
    fn cross_type_comparison_resolves_via_implicit_widening() {
        let op =
            resolve("=", Some(oid::INT4), oid::INT8).expect("int4 = int8 must resolve by widening");
        assert_eq!(op.oid, Oid(410), "should land on the int8 = int8 row");
        assert_eq!(op.left, Some(oid::INT8));
        assert_eq!(op.right, oid::INT8);
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
