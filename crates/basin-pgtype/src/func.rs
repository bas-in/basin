//! Postgres's builtin function catalog — `pg_catalog.pg_proc` — and
//! resolution of a function name plus argument types to the overload it
//! means.
//!
//! # Why this exists
//!
//! [`crate::operator`] does this for operators, replacing textual rewriting
//! (`crates/basin-engine/src/pg_operators.rs`) with a real `pg_operator`
//! table and resolution by argument type. This module is the same idea for
//! *functions*: it backs `pg_proc`, and it is what lets the planner resolve
//! `lower(x)`, `count(*)`, `sum(x)` and friends by name and argument types
//! instead of guessing from the parsed call site.
//!
//! It is also, verbatim, `pg_catalog.pg_proc`: the same table serves
//! wire-protocol/catalog fidelity (`\df`, JDBC/psycopg introspection, ORM
//! codegen, `pg_dump`) and the planner's own function resolution at once. See
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md`.
//!
//! # Where these values come from
//!
//! Every row was checked against a live PostgreSQL 18 `pg_proc`, not recalled
//! from memory — the cast table in [`crate::cast`] caught five wrong entries
//! this way, [`crate::operator`] caught a wrong operator OID, and this module
//! caught the multi-SRF row-count rule below. The query:
//!
//! ```sql
//! SELECT p.oid, p.proname, pg_get_function_identity_arguments(p.oid),
//!        t.typname, p.prokind
//!   FROM pg_proc p
//!   JOIN pg_type t ON t.oid = p.prorettype
//!  WHERE p.pronamespace = 11 AND p.prokind IN ('f', 'a', 'w')
//!  ORDER BY p.proname, p.oid;
//! ```
//!
//! (namespace 11 is `pg_catalog`; `prokind` is `f` for a plain function, `a`
//! for an aggregate, `w` for a window function — `p` for a procedure never
//! appears here because procedures have no return type to join against).
//! Re-run it — filtered to the function names below — before editing this
//! table.
//!
//! # What is deliberately absent
//!
//! Postgres ships well over a thousand builtin functions; this module covers
//! the categories needed to unblock planner resolution and `pg_proc`
//! fidelity for common queries: string, math, date/time, aggregate, window
//! and set-returning functions. Anything else resolves to `None` today, same
//! as it would if the name were simply misspelled.
//!
//! `ceiling` (the alias for `ceil`), `date_trunc`'s three-argument
//! (timezone-name) overload, and `age(xid)` (transaction-id wraparound, not a
//! date/time function despite the name) are real `pg_proc` rows this table
//! does not include. Not covered wrongly, just not covered yet.
//!
//! # Polymorphic functions are monomorphized, like the array operators
//!
//! Several of these functions are declared in Postgres on pseudo-types —
//! `array_agg(anynonarray)`, `unnest(anyarray)`, `lag`/`lead`/`first_value`/
//! `last_value`/`nth_value(anyelement, ...)` — the same way `@>`/`<@`/`&&` are
//! declared on `anyarray` in [`crate::operator`]. There is **one** physical
//! function OID shared by every concrete instantiation; Postgres does not
//! mint a fresh oid per element type. [`FuncSig`] has no room for a
//! polymorphic pseudo-type or a return type that depends on the argument, so
//! below each of these appears as several *monomorphized* rows — e.g.
//! `array_agg(int4) -> int4[]` and `array_agg(text) -> text[]` — that
//! legitimately carry the same real oid (2335), because that is the oid
//! Postgres itself resolves to no matter which concrete type is aggregated.
//! `int4` and `text` are chosen as one representative numeric and one
//! representative string instantiation; any other concrete type is simply not
//! covered yet, not covered wrongly. See the module docs on
//! [`crate::operator::OPERATORS`] for the same rule spelled out for arrays.
//!
//! `concat` is different in kind: it is declared `VARIADIC "any"` (oid 3058,
//! `pg_get_function_identity_arguments` confirms exactly one `"any"`-typed
//! parameter, repeated per call). This table monomorphizes it at the
//! two-argument call shape (`concat(any, any) -> text`), which is faithful to
//! how each individual slot behaves — a `"any"`-typed slot accepts a value of
//! *any* type with no coercion, confirmed live via `SELECT concat('a', 3)` —
//! but does not attempt every arity; `concat()` (zero args) is confirmed live
//! to be a *syntax error* in Postgres (`concat` requires at least one
//! argument), and this table does not cover three-or-more-argument calls
//! either. A real variadic-arity mechanism is a follow-up, not a change to
//! this rule.
//!
//! `count(*)` and `count(x)` are two different real rows (oids 2803 and
//! 2147, confirmed live via `pg_get_function_identity_arguments`), not one
//! function with an optional argument — conflating them is a correctness bug
//! (`count(*)` counts rows including NULLs; `count(x)` does not), which is
//! why [`FuncSig`] represents `count(*)` as the *zero-argument* row rather
//! than reusing `count(x)`'s row with an empty/placeholder argument.
//!
//! `avg(smallint)` and `avg(integer)` are deliberately **not** tabulated,
//! unlike every other aggregate here. Confirmed live: `avg(smallint)`,
//! `avg(integer)` and `avg(bigint)` all return `numeric` — unlike `sum`, none
//! of `avg`'s integer overloads changes return type by width. Leaving the
//! narrower two out and relying on [`resolve`]'s implicit-coercion fallback
//! to reach `avg(bigint)` (oid 2100) is therefore lossless: the same real oid
//! Postgres would pick for `avg(int8)` is the oid Basin lands on for
//! `avg(int2)`/`avg(int4)` too, they just aren't the specific oids Postgres
//! would pick for those narrower inputs. This is the same documented
//! divergence [`crate::operator`] takes for `int4 = int8` (see its module
//! docs) — narrower-than-Postgres tabulation, same answer, and a real,
//! admitted gap if Basin ever needs to report the *specific* oid Postgres
//! would have chosen.
//!
//! # The multi-SRF row-count rule (confirmed live)
//!
//! `crates/basin-plan/src/lib.rs`'s `LogicalPlan::ProjectSet` doc comment
//! describes multiple set-returning functions in one target list as
//! expanding to the *least common multiple* of their row counts, with
//! shorter ones cycling. That was Postgres's behavior before version 10.
//! Confirmed live against PostgreSQL 18:
//!
//! ```sql
//! SELECT generate_series(1, 2), generate_series(1, 3);
//! --  1 | 1
//! --  2 | 2
//! --    | 3
//! ```
//!
//! Three rows, not six (`lcm(2, 3)`): Postgres 10 changed this to the
//! *greater* of the two row counts, padding the shorter output with `NULL`.
//! [`FuncKind::SetReturning`] exists so a caller building `ProjectSet` can
//! find this out (`is_srf`) rather than guessing; getting the row-count rule
//! itself right is that caller's job, not this module's — recorded here
//! because this is where the live check happened.

use crate::{
    cast::{cast_kind, CastKind},
    oid, Oid,
};

/// Postgres's `"any"` pseudo-type (`pg_type.oid` 2276): a parameter that
/// accepts a value of literally any type, with no coercion applied. Used by
/// `count(any)` and, monomorphized to two slots, by `concat` — see the
/// module docs.
const PSEUDO_ANY: Oid = Oid(2276);

/// What kind of `pg_proc` entry a [`FuncSig`] is — `pg_proc.prokind`, spelled
/// out rather than left as Postgres's single character, because callers
/// building a plan need to branch on this (an aggregate needs a `GROUP BY`
/// node, a window function a `Window` node, a set-returning function a
/// `ProjectSet` node) and a stringly-typed char invites typos a `match` can't
/// catch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuncKind {
    /// `prokind = 'f'` and not set-returning: an ordinary function called
    /// once per row, returning one value. `lower`, `abs`, `now`.
    Scalar,
    /// `prokind = 'a'`: consumes one row per group, produces one value per
    /// group. `count`, `sum`, `array_agg`.
    Aggregate,
    /// `prokind = 'w'`: like an aggregate, but sees the whole window frame
    /// and does not collapse rows. `row_number`, `lag`, `rank`.
    Window,
    /// `prokind = 'f'` with `proretset = true`: called once, produces zero or
    /// more rows. `generate_series`, `unnest`. Note this is a real
    /// distinction Postgres makes *within* `prokind = 'f'` — `pg_proc.prokind`
    /// alone does not separate a set-returning function from a scalar one,
    /// which is exactly why [`FuncKind`] does not just mirror the raw
    /// character. See the module docs' note on `proretset` in the query.
    SetReturning,
}

/// One row of `pg_catalog.pg_proc`: a function name plus the argument and
/// result types for one specific overload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuncSig {
    /// `pg_proc.oid`. Stable across releases, part of the same wire/catalog
    /// contract as the type OIDs in [`crate::oid`] and the operator OIDs in
    /// [`crate::operator`].
    pub oid: Oid,
    /// `pg_proc.proname`.
    pub name: &'static str,
    /// `pg_proc.proargtypes`, one entry per declared parameter. Empty for a
    /// niladic function (`now()`, `count(*)`, `row_number()`).
    pub args: &'static [Oid],
    /// `pg_proc.prorettype`. For a set-returning function this is the row
    /// type of one output row, not a "set of" wrapper — Postgres itself
    /// stores it the same way and marks set-returning separately via
    /// `proretset`, which is what [`FuncKind::SetReturning`] carries here.
    pub ret: Oid,
    /// Not part of `pg_proc` as a single column (it is derived from
    /// `prokind` plus `proretset`), but the whole reason this struct exists
    /// rather than a bare tuple — see [`FuncKind`].
    pub kind: FuncKind,
}

impl FuncSig {
    const fn new(
        oid: u32,
        name: &'static str,
        args: &'static [Oid],
        ret: Oid,
        kind: FuncKind,
    ) -> FuncSig {
        FuncSig {
            oid: Oid(oid),
            name,
            args,
            ret,
            kind,
        }
    }
}

/// The builtin functions this crate knows about. See the module docs for
/// scope, the query used to verify every row, and the monomorphization rule
/// for polymorphic functions.
pub static FUNCS: &[FuncSig] = &[
    // ─── String ─────────────────────────────────────────────────────────────
    FuncSig::new(870, "lower", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(871, "upper", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(1317, "length", &[oid::TEXT], oid::INT4, FuncKind::Scalar),
    FuncSig::new(
        877,
        "substr",
        &[oid::TEXT, oid::INT4, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        883,
        "substr",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // `TRIM(BOTH/LEADING/TRAILING chars FROM s)` desugars to
    // btrim/ltrim/rtrim before it reaches this table — Postgres's own parser
    // does the same desugaring, there is no `pg_proc` row literally named
    // `trim`.
    FuncSig::new(
        884,
        "btrim",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(885, "btrim", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(
        875,
        "ltrim",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(881, "ltrim", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(
        876,
        "rtrim",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(882, "rtrim", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(
        2087,
        "replace",
        &[oid::TEXT, oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        868,
        "strpos",
        &[oid::TEXT, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    // Monomorphized to the two-argument call shape of `VARIADIC "any"` — see
    // the module docs.
    FuncSig::new(
        3058,
        "concat",
        &[PSEUDO_ANY, PSEUDO_ANY],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // ─── Math ───────────────────────────────────────────────────────────────
    FuncSig::new(1398, "abs", &[oid::INT2], oid::INT2, FuncKind::Scalar),
    FuncSig::new(1397, "abs", &[oid::INT4], oid::INT4, FuncKind::Scalar),
    FuncSig::new(1396, "abs", &[oid::INT8], oid::INT8, FuncKind::Scalar),
    FuncSig::new(1394, "abs", &[oid::FLOAT4], oid::FLOAT4, FuncKind::Scalar),
    FuncSig::new(1395, "abs", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1705, "abs", &[oid::NUMERIC], oid::NUMERIC, FuncKind::Scalar),
    FuncSig::new(1342, "round", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1708,
        "round",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1707,
        "round",
        &[oid::NUMERIC, oid::INT4],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1711,
        "ceil",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(2308, "ceil", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1712,
        "floor",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(2309, "floor", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1344, "sqrt", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1730,
        "sqrt",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1368,
        "power",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2169,
        "power",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        940,
        "mod",
        &[oid::INT2, oid::INT2],
        oid::INT2,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        941,
        "mod",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        947,
        "mod",
        &[oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1728,
        "mod",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // ─── Date/time ──────────────────────────────────────────────────────────
    FuncSig::new(1299, "now", &[], oid::TIMESTAMPTZ, FuncKind::Scalar),
    FuncSig::new(
        1171,
        "date_part",
        &[oid::TEXT, oid::TIMESTAMPTZ],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2021,
        "date_part",
        &[oid::TEXT, oid::TIMESTAMP],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1384,
        "date_part",
        &[oid::TEXT, oid::DATE],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1172,
        "date_part",
        &[oid::TEXT, oid::INTERVAL],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1385,
        "date_part",
        &[oid::TEXT, oid::TIME],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1273,
        "date_part",
        &[oid::TEXT, oid::TIMETZ],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1217,
        "date_trunc",
        &[oid::TEXT, oid::TIMESTAMPTZ],
        oid::TIMESTAMPTZ,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2020,
        "date_trunc",
        &[oid::TEXT, oid::TIMESTAMP],
        oid::TIMESTAMP,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1218,
        "date_trunc",
        &[oid::TEXT, oid::INTERVAL],
        oid::INTERVAL,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1199,
        "age",
        &[oid::TIMESTAMPTZ, oid::TIMESTAMPTZ],
        oid::INTERVAL,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1386,
        "age",
        &[oid::TIMESTAMPTZ],
        oid::INTERVAL,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2058,
        "age",
        &[oid::TIMESTAMP, oid::TIMESTAMP],
        oid::INTERVAL,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2059,
        "age",
        &[oid::TIMESTAMP],
        oid::INTERVAL,
        FuncKind::Scalar,
    ),
    // ─── Aggregates ─────────────────────────────────────────────────────────
    //
    // `count(*)` and `count(x)` are different real oids — see the module
    // docs. `count(*)` is the zero-argument row.
    FuncSig::new(2803, "count", &[], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(2147, "count", &[PSEUDO_ANY], oid::INT8, FuncKind::Aggregate),
    // `sum` widens by exactly one step for the fixed-width integers
    // (int2/int4 -> int8) and then jumps to `numeric` for int8, which is the
    // surprising one: `sum(bigint)` cannot stay bigint without risking
    // overflow across the group, so Postgres returns `numeric` instead. Each
    // is a genuinely different oid with a genuinely different return type,
    // confirmed live — see `sum_int4_and_int8_are_different_oids_and_return_types`.
    FuncSig::new(2109, "sum", &[oid::INT2], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(2108, "sum", &[oid::INT4], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(2107, "sum", &[oid::INT8], oid::NUMERIC, FuncKind::Aggregate),
    FuncSig::new(
        2110,
        "sum",
        &[oid::FLOAT4],
        oid::FLOAT4,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2111,
        "sum",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2114,
        "sum",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2113,
        "sum",
        &[oid::INTERVAL],
        oid::INTERVAL,
        FuncKind::Aggregate,
    ),
    FuncSig::new(2133, "min", &[oid::INT2], oid::INT2, FuncKind::Aggregate),
    FuncSig::new(2132, "min", &[oid::INT4], oid::INT4, FuncKind::Aggregate),
    FuncSig::new(2131, "min", &[oid::INT8], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(
        2135,
        "min",
        &[oid::FLOAT4],
        oid::FLOAT4,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2136,
        "min",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2146,
        "min",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(2145, "min", &[oid::TEXT], oid::TEXT, FuncKind::Aggregate),
    FuncSig::new(2138, "min", &[oid::DATE], oid::DATE, FuncKind::Aggregate),
    FuncSig::new(
        2142,
        "min",
        &[oid::TIMESTAMP],
        oid::TIMESTAMP,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2143,
        "min",
        &[oid::TIMESTAMPTZ],
        oid::TIMESTAMPTZ,
        FuncKind::Aggregate,
    ),
    FuncSig::new(2117, "max", &[oid::INT2], oid::INT2, FuncKind::Aggregate),
    FuncSig::new(2116, "max", &[oid::INT4], oid::INT4, FuncKind::Aggregate),
    FuncSig::new(2115, "max", &[oid::INT8], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(
        2119,
        "max",
        &[oid::FLOAT4],
        oid::FLOAT4,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2120,
        "max",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2130,
        "max",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(2129, "max", &[oid::TEXT], oid::TEXT, FuncKind::Aggregate),
    FuncSig::new(2122, "max", &[oid::DATE], oid::DATE, FuncKind::Aggregate),
    FuncSig::new(
        2126,
        "max",
        &[oid::TIMESTAMP],
        oid::TIMESTAMP,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2127,
        "max",
        &[oid::TIMESTAMPTZ],
        oid::TIMESTAMPTZ,
        FuncKind::Aggregate,
    ),
    // `avg(int2)`/`avg(int4)` are deliberately absent — see the module docs.
    FuncSig::new(2100, "avg", &[oid::INT8], oid::NUMERIC, FuncKind::Aggregate),
    FuncSig::new(
        2104,
        "avg",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2105,
        "avg",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2103,
        "avg",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2106,
        "avg",
        &[oid::INTERVAL],
        oid::INTERVAL,
        FuncKind::Aggregate,
    ),
    // `array_agg(anynonarray) -> anyarray`, oid 2335, monomorphized at two
    // representative element types — see the module docs.
    FuncSig::new(
        2335,
        "array_agg",
        &[oid::INT4],
        oid::INT4_ARRAY,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2335,
        "array_agg",
        &[oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        3538,
        "string_agg",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        3545,
        "string_agg",
        &[oid::BYTEA, oid::BYTEA],
        oid::BYTEA,
        FuncKind::Aggregate,
    ),
    // ─── Window ─────────────────────────────────────────────────────────────
    FuncSig::new(3100, "row_number", &[], oid::INT8, FuncKind::Window),
    FuncSig::new(3101, "rank", &[], oid::INT8, FuncKind::Window),
    FuncSig::new(3102, "dense_rank", &[], oid::INT8, FuncKind::Window),
    // `lag`/`lead`/`first_value`/`last_value`/`nth_value` are declared on
    // `anyelement` (`anycompatible` for `lag`/`lead`'s three-argument default
    // form, not covered here); monomorphized at `int4` and `text` — see the
    // module docs.
    FuncSig::new(3106, "lag", &[oid::INT4], oid::INT4, FuncKind::Window),
    FuncSig::new(3106, "lag", &[oid::TEXT], oid::TEXT, FuncKind::Window),
    FuncSig::new(
        3107,
        "lag",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Window,
    ),
    FuncSig::new(
        3107,
        "lag",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Window,
    ),
    FuncSig::new(3109, "lead", &[oid::INT4], oid::INT4, FuncKind::Window),
    FuncSig::new(3109, "lead", &[oid::TEXT], oid::TEXT, FuncKind::Window),
    FuncSig::new(
        3110,
        "lead",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Window,
    ),
    FuncSig::new(
        3110,
        "lead",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Window,
    ),
    FuncSig::new(
        3112,
        "first_value",
        &[oid::INT4],
        oid::INT4,
        FuncKind::Window,
    ),
    FuncSig::new(
        3112,
        "first_value",
        &[oid::TEXT],
        oid::TEXT,
        FuncKind::Window,
    ),
    FuncSig::new(
        3113,
        "last_value",
        &[oid::INT4],
        oid::INT4,
        FuncKind::Window,
    ),
    FuncSig::new(
        3113,
        "last_value",
        &[oid::TEXT],
        oid::TEXT,
        FuncKind::Window,
    ),
    FuncSig::new(
        3114,
        "nth_value",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Window,
    ),
    FuncSig::new(
        3114,
        "nth_value",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Window,
    ),
    // ─── Set-returning ──────────────────────────────────────────────────────
    //
    // `generate_series`'s two-arg and three-arg (explicit step) forms are
    // different real oids per integer width, not the same oid with an
    // optional parameter — see `generate_series_has_distinct_two_and_three_arg_oids`.
    FuncSig::new(
        1067,
        "generate_series",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        1066,
        "generate_series",
        &[oid::INT4, oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        1069,
        "generate_series",
        &[oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        1068,
        "generate_series",
        &[oid::INT8, oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        3260,
        "generate_series",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        3259,
        "generate_series",
        &[oid::NUMERIC, oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        938,
        "generate_series",
        &[oid::TIMESTAMP, oid::TIMESTAMP, oid::INTERVAL],
        oid::TIMESTAMP,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        939,
        "generate_series",
        &[oid::TIMESTAMPTZ, oid::TIMESTAMPTZ, oid::INTERVAL],
        oid::TIMESTAMPTZ,
        FuncKind::SetReturning,
    ),
    // `unnest(anyarray) -> anyelement`, oid 2331, monomorphized at two
    // representative element types — see the module docs.
    FuncSig::new(
        2331,
        "unnest",
        &[oid::INT4_ARRAY],
        oid::INT4,
        FuncKind::SetReturning,
    ),
    FuncSig::new(
        2331,
        "unnest",
        &[oid::TEXT_ARRAY],
        oid::TEXT,
        FuncKind::SetReturning,
    ),
];

/// Whether `declared` (one entry from a [`FuncSig::args`]) accepts `given`
/// without any coercion at all.
///
/// True for a literal type match, and true for [`PSEUDO_ANY`] against any
/// concrete type — Postgres's `"any"` pseudo-type binds to its argument's own
/// type with no cast applied, so from a coercion standpoint it behaves like
/// an automatic exact match rather than like a candidate for the implicit
/// fallback below.
#[inline]
fn arg_matches_exact(declared: Oid, given: Oid) -> bool {
    declared == given || declared == PSEUDO_ANY
}

/// Whether every entry of `declared` accepts the matching entry of `given`,
/// under `matches`. Both must be checked for equal length first — a
/// three-argument declaration can never match a two-argument call, pseudo
/// types included.
fn args_match(declared: &[Oid], given: &[Oid], matches: impl Fn(Oid, Oid) -> bool) -> bool {
    declared.len() == given.len() && declared.iter().zip(given).all(|(&d, &g)| matches(d, g))
}

/// Resolve a function call to the [`FuncSig`] Postgres would pick.
///
/// Mirrors [`crate::operator::resolve`]'s two-phase approach, which itself
/// mirrors Postgres's `func_select_candidate` / `oper_select_candidate`:
/// first look for a candidate every argument matches without coercion (an
/// exact type match, or Postgres's `"any"` pseudo-type — see
/// [`arg_matches_exact`]), then fall back to a candidate reachable by
/// *implicit* coercion of the given argument types
/// ([`crate::cast::cast_kind`] `== Some(CastKind::Implicit)`). A function
/// whose match requires an assignment-only or explicit-only cast is never
/// chosen implicitly, same as for operators — see
/// `lower_text_resolves_but_lower_int4_does_not` below, which pins that
/// `int4 -> text` is assignment-only (Postgres's string I/O fallback), not
/// implicit.
///
/// Like [`crate::operator::resolve`], this is narrower than Postgres's full
/// overload resolution (no "most exact matches" / preferred-type
/// tie-breaking among several implicit candidates): the first-in-table match
/// wins. For the functions tabulated here that is not an observable
/// difference — see the module docs for the one case (`avg`) where it is a
/// deliberate, documented one.
pub fn resolve(name: &str, args: &[Oid]) -> Option<&'static FuncSig> {
    if let Some(f) = FUNCS
        .iter()
        .find(|f| f.name == name && args_match(f.args, args, arg_matches_exact))
    {
        return Some(f);
    }

    FUNCS.iter().find(|f| {
        f.name == name
            && args_match(f.args, args, |declared, given| {
                arg_matches_exact(declared, given)
                    || cast_kind(given, declared) == Some(CastKind::Implicit)
            })
    })
}

/// Whether `oid` names a builtin aggregate function (`pg_proc.prokind = 'a'`).
///
/// Takes the function's `pg_proc.oid`, not its name — `count`, `sum`, `min`
/// and `max` each have several oids (one per overload), and every one of
/// them is an aggregate, so this is really "is this specific overload an
/// aggregate" rather than "is this name ever an aggregate".
pub fn is_aggregate(oid: Oid) -> bool {
    FUNCS
        .iter()
        .any(|f| f.oid == oid && f.kind == FuncKind::Aggregate)
}

/// Whether `oid` names a builtin window function (`pg_proc.prokind = 'w'`).
pub fn is_window(oid: Oid) -> bool {
    FUNCS
        .iter()
        .any(|f| f.oid == oid && f.kind == FuncKind::Window)
}

/// Whether `oid` names a builtin set-returning function (`proretset = true`).
pub fn is_srf(oid: Oid) -> bool {
    FUNCS
        .iter()
        .any(|f| f.oid == oid && f.kind == FuncKind::SetReturning)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `count(*)` is the *zero-argument* row (oid 2803, confirmed live via
    /// `pg_get_function_identity_arguments`), not `count(x)` (oid 2147) called
    /// with some placeholder. Conflating them is a correctness bug: `count(*)`
    /// counts every row including ones with NULL columns, `count(x)` does not.
    #[test]
    fn count_star_is_a_distinct_zero_arg_row_from_count_of_an_argument() {
        let star = resolve("count", &[]).expect("count(*) must resolve");
        assert_eq!(star.oid, Oid(2803));
        assert!(star.args.is_empty());
        assert_eq!(star.ret, oid::INT8);

        let of_x = resolve("count", &[oid::INT4]).expect("count(x) must resolve");
        assert_eq!(of_x.oid, Oid(2147));
        assert_ne!(
            star.oid, of_x.oid,
            "count(*) and count(x) must not collapse"
        );
    }

    /// `count(any)` resolving for an `int4` argument must not be mistaken for
    /// `int4` being tabulated directly — it is Postgres's `"any"` pseudo-type
    /// accepting an arbitrary concrete type. A `text` argument must resolve to
    /// the very same row, unchanged.
    #[test]
    fn count_of_x_accepts_any_concrete_argument_type() {
        let int_count = resolve("count", &[oid::INT4]).unwrap();
        let text_count = resolve("count", &[oid::TEXT]).unwrap();
        assert_eq!(int_count.oid, Oid(2147));
        assert_eq!(int_count.oid, text_count.oid);
    }

    /// `sum(int4)` and `sum(int8)` are different real oids with different
    /// return types — `sum(int4)` stays `int8`, but `sum(int8)` widens all the
    /// way to `numeric` because Postgres cannot promise a group of bigints
    /// sums without overflowing 64 bits. Confirmed live; this is the kind of
    /// widening that surprises people who assume `sum` just "adds a digit".
    #[test]
    fn sum_int4_and_int8_are_different_oids_and_return_types() {
        let sum_int4 = resolve("sum", &[oid::INT4]).expect("sum(int4) must resolve");
        let sum_int8 = resolve("sum", &[oid::INT8]).expect("sum(int8) must resolve");
        assert_ne!(sum_int4.oid, sum_int8.oid);
        assert_eq!(sum_int4.ret, oid::INT8, "sum(int4) stays int8");
        assert_eq!(
            sum_int8.ret,
            oid::NUMERIC,
            "sum(int8) widens to numeric, not int8"
        );
    }

    /// The bread-and-butter case: `lower(text)` matches its own row exactly.
    /// `lower(int4)` must NOT resolve — there is no implicit int-to-text
    /// path. Postgres's string I/O fallback makes `int4 -> text` an
    /// *assignment*-only cast (`SELECT CAST(5 AS text)` needs no explicit
    /// syntax at an assignment target, but `lower(5)` is not one), never
    /// implicit, so a hand-rolled resolver that treated "has some cast" as
    /// "may call this function" would wrongly accept `lower(5)`.
    #[test]
    fn lower_text_resolves_but_lower_int4_does_not() {
        let lower = resolve("lower", &[oid::TEXT]).expect("lower(text) must resolve");
        assert_eq!(lower.oid, Oid(870));
        assert_eq!(lower.ret, oid::TEXT);

        assert_eq!(
            cast_kind(oid::INT4, oid::TEXT),
            Some(CastKind::Assignment),
            "precondition: int4 -> text must be assignment-only, not implicit"
        );
        assert_eq!(resolve("lower", &[oid::INT4]), None);
    }

    /// `avg(int2)`/`avg(int4)` are deliberately not tabulated — see the
    /// module docs. An `int4` argument must still resolve `avg`, by
    /// implicitly widening to `int8` and landing on the `avg(int8)` row. This
    /// is lossless here specifically because `avg(int2)`, `avg(int4)` and
    /// `avg(int8)` all return `numeric` in real Postgres (confirmed live) —
    /// unlike `sum`, no return-type information is lost by skipping the
    /// narrower rows.
    #[test]
    fn int4_arg_resolves_bigint_signature_via_implicit_widening() {
        let avg = resolve("avg", &[oid::INT4]).expect("avg(int4) must resolve by widening");
        assert_eq!(avg.oid, Oid(2100), "should land on the avg(int8) row");
        assert_eq!(avg.args, &[oid::INT8]);
        assert_eq!(avg.ret, oid::NUMERIC);
    }

    /// `generate_series` has distinct two-argument and three-argument
    /// (explicit step) forms with different oids, for both `int4` and `int8`
    /// — not one function with an optional parameter.
    #[test]
    fn generate_series_has_distinct_two_and_three_arg_oids() {
        let two = resolve("generate_series", &[oid::INT4, oid::INT4])
            .expect("generate_series(int4, int4) must resolve");
        let three = resolve("generate_series", &[oid::INT4, oid::INT4, oid::INT4])
            .expect("generate_series(int4, int4, int4) must resolve");
        assert_eq!(two.oid, Oid(1067));
        assert_eq!(three.oid, Oid(1066));
        assert_ne!(two.oid, three.oid);

        let two8 = resolve("generate_series", &[oid::INT8, oid::INT8]).unwrap();
        let three8 = resolve("generate_series", &[oid::INT8, oid::INT8, oid::INT8]).unwrap();
        assert_eq!(two8.oid, Oid(1069));
        assert_eq!(three8.oid, Oid(1068));
        assert_ne!(
            two.oid, two8.oid,
            "int4 and int8 generate_series are different oids too"
        );
    }

    /// Window functions must be classified as `Window`, not `Scalar` — a
    /// caller building a plan needs this to decide whether a call belongs in
    /// a `Window` node.
    #[test]
    fn window_functions_are_classified_as_window_not_scalar() {
        let row_number = resolve("row_number", &[]).expect("row_number() must resolve");
        assert_eq!(row_number.kind, FuncKind::Window);
        assert_ne!(row_number.kind, FuncKind::Scalar);
        assert!(is_window(row_number.oid));
        assert!(!is_aggregate(row_number.oid));
        assert!(!is_srf(row_number.oid));

        for name in ["rank", "dense_rank"] {
            let f = resolve(name, &[]).unwrap_or_else(|| panic!("{name}() must resolve"));
            assert_eq!(f.kind, FuncKind::Window, "{name} must be Window");
        }

        let lag = resolve("lag", &[oid::INT4]).expect("lag(int4) must resolve");
        assert_eq!(lag.kind, FuncKind::Window);
        assert!(is_window(lag.oid));
    }

    /// Aggregates are classified as `Aggregate`, and `is_aggregate` agrees
    /// with `resolve`'s classification rather than being a separately
    /// maintained list that can drift from it.
    #[test]
    fn aggregates_are_classified_as_aggregate() {
        let count_star = resolve("count", &[]).unwrap();
        assert_eq!(count_star.kind, FuncKind::Aggregate);
        assert!(is_aggregate(count_star.oid));
        assert!(!is_window(count_star.oid));
        assert!(!is_srf(count_star.oid));

        let array_agg = resolve("array_agg", &[oid::INT4]).expect("array_agg(int4) must resolve");
        assert_eq!(array_agg.kind, FuncKind::Aggregate);
        assert_eq!(array_agg.ret, oid::INT4_ARRAY);
    }

    /// `array_agg` is genuinely polymorphic in real Postgres (`anynonarray`):
    /// the `int4` and `text` instantiations tabulated here must share the
    /// *same* oid, because in real Postgres they are the same function.
    #[test]
    fn array_agg_shares_one_oid_across_element_types() {
        let int_agg = resolve("array_agg", &[oid::INT4]).unwrap();
        let text_agg = resolve("array_agg", &[oid::TEXT]).unwrap();
        assert_eq!(int_agg.oid, text_agg.oid);
        assert_eq!(int_agg.oid, Oid(2335));
        assert_eq!(int_agg.ret, oid::INT4_ARRAY);
        assert_eq!(text_agg.ret, oid::TEXT_ARRAY);
    }

    /// Set-returning functions are classified `SetReturning`, distinct from
    /// both `Scalar` and `Aggregate` — `generate_series` and `unnest` are
    /// ordinary (`prokind = 'f'`) functions in `pg_proc`, and the
    /// set-returning-ness lives in `proretset`, not `prokind`. A resolver
    /// that only looked at `prokind` would misclassify these as `Scalar`.
    #[test]
    fn set_returning_functions_are_neither_scalar_nor_aggregate() {
        let series = resolve("generate_series", &[oid::INT4, oid::INT4]).unwrap();
        assert_eq!(series.kind, FuncKind::SetReturning);
        assert!(is_srf(series.oid));
        assert!(!is_aggregate(series.oid));
        assert!(!is_window(series.oid));

        let unnest = resolve("unnest", &[oid::INT4_ARRAY]).expect("unnest(int4[]) must resolve");
        assert_eq!(unnest.kind, FuncKind::SetReturning);
        assert_eq!(unnest.ret, oid::INT4);
    }

    /// `unnest` is genuinely polymorphic (`anyarray -> anyelement`) in real
    /// Postgres; the `int4[]` and `text[]` instantiations share one oid.
    #[test]
    fn unnest_shares_one_oid_across_element_types() {
        let int_unnest = resolve("unnest", &[oid::INT4_ARRAY]).unwrap();
        let text_unnest = resolve("unnest", &[oid::TEXT_ARRAY]).unwrap();
        assert_eq!(int_unnest.oid, text_unnest.oid);
        assert_eq!(int_unnest.oid, Oid(2331));
    }

    /// `date_part` has a different oid per second argument type, all
    /// returning `float8`; `date_trunc` mirrors it but preserves the input's
    /// timestamp-ness in its return type instead of collapsing to a scalar.
    #[test]
    fn date_part_and_date_trunc_have_one_row_per_argument_type() {
        let part_ts = resolve("date_part", &[oid::TEXT, oid::TIMESTAMPTZ]).unwrap();
        let part_date = resolve("date_part", &[oid::TEXT, oid::DATE]).unwrap();
        assert_ne!(part_ts.oid, part_date.oid);
        assert_eq!(part_ts.ret, oid::FLOAT8);
        assert_eq!(part_date.ret, oid::FLOAT8);

        let trunc_tz = resolve("date_trunc", &[oid::TEXT, oid::TIMESTAMPTZ]).unwrap();
        let trunc_ts = resolve("date_trunc", &[oid::TEXT, oid::TIMESTAMP]).unwrap();
        assert_eq!(trunc_tz.ret, oid::TIMESTAMPTZ, "tz in, tz out");
        assert_eq!(trunc_ts.ret, oid::TIMESTAMP, "no tz in, no tz out");
    }

    /// `age` has both a two-argument and a one-argument (implicitly compared
    /// to `now()`) form, for both `timestamp` and `timestamptz`.
    #[test]
    fn age_has_one_and_two_argument_forms() {
        let two_arg = resolve("age", &[oid::TIMESTAMPTZ, oid::TIMESTAMPTZ]).unwrap();
        let one_arg = resolve("age", &[oid::TIMESTAMPTZ]).unwrap();
        assert_ne!(two_arg.oid, one_arg.oid);
        assert_eq!(two_arg.ret, oid::INTERVAL);
        assert_eq!(one_arg.ret, oid::INTERVAL);
    }

    /// `concat` is monomorphized at the two-argument call shape (see the
    /// module docs); each slot independently accepts any type, so a
    /// heterogeneous call (`text`, `int4`) must resolve just as readily as a
    /// homogeneous one.
    #[test]
    fn concat_accepts_heterogeneous_argument_types() {
        let same = resolve("concat", &[oid::TEXT, oid::TEXT]).expect("concat(text, text)");
        let mixed = resolve("concat", &[oid::TEXT, oid::INT4]).expect("concat(text, int4)");
        assert_eq!(same.oid, Oid(3058));
        assert_eq!(mixed.oid, Oid(3058));
        assert_eq!(mixed.ret, oid::TEXT);
    }

    /// `round` has a one-argument and a two-argument (explicit scale) form
    /// over `numeric`, plus a separate one-argument `float8` form — three
    /// different real oids, not defaults of one function.
    #[test]
    fn round_has_distinct_arities_and_a_separate_float_overload() {
        let numeric_one = resolve("round", &[oid::NUMERIC]).unwrap();
        let numeric_two = resolve("round", &[oid::NUMERIC, oid::INT4]).unwrap();
        let float_one = resolve("round", &[oid::FLOAT8]).unwrap();
        assert_eq!(numeric_one.oid, Oid(1708));
        assert_eq!(numeric_two.oid, Oid(1707));
        assert_eq!(float_one.oid, Oid(1342));
        assert_ne!(numeric_one.oid, float_one.oid);
    }

    /// An argument-count mismatch must not resolve, even when the name and
    /// element types are otherwise right — a two-argument call must not
    /// silently match a one-argument row or vice versa.
    #[test]
    fn wrong_arg_count_does_not_resolve() {
        assert_eq!(resolve("lower", &[]), None);
        assert_eq!(resolve("lower", &[oid::TEXT, oid::TEXT]), None);
        assert_eq!(resolve("now", &[oid::INT4]), None);
    }

    /// A function that plain does not exist under any name must resolve to
    /// `None`, not panic or silently pick something close.
    #[test]
    fn unknown_function_name_does_not_resolve() {
        assert_eq!(resolve("frobnicate", &[oid::TEXT]), None);
    }

    /// Every row's declared result is a real, representable type — guards
    /// against a copy-paste leaving a result OID from the wrong row.
    #[test]
    fn every_function_has_a_named_or_array_result_type() {
        for f in FUNCS {
            assert!(
                oid::type_name(f.ret).is_some() || oid::is_array(f.ret),
                "function {} ({}) has an unrecognized result oid {}",
                f.name,
                f.oid,
                f.ret
            );
        }
    }

    /// `is_aggregate`/`is_window`/`is_srf` must agree with each `FuncSig`'s
    /// own `kind` for every row in the table — a mismatch here would mean the
    /// classification helpers and the table itself can disagree.
    #[test]
    fn classification_helpers_agree_with_every_row() {
        for f in FUNCS {
            match f.kind {
                FuncKind::Aggregate => {
                    assert!(is_aggregate(f.oid));
                    assert!(!is_window(f.oid));
                    assert!(!is_srf(f.oid));
                }
                FuncKind::Window => {
                    assert!(is_window(f.oid));
                    assert!(!is_aggregate(f.oid));
                    assert!(!is_srf(f.oid));
                }
                FuncKind::SetReturning => {
                    assert!(is_srf(f.oid));
                    assert!(!is_aggregate(f.oid));
                    assert!(!is_window(f.oid));
                }
                FuncKind::Scalar => {
                    assert!(!is_aggregate(f.oid));
                    assert!(!is_window(f.oid));
                    assert!(!is_srf(f.oid));
                }
            }
        }
    }
}
