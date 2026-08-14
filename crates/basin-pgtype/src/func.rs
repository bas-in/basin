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
//! fidelity for common queries: string, math, date/time, array, regex,
//! hashing, aggregate, window and set-returning functions. Anything else
//! resolves to `None` today, same as it would if the name were simply
//! misspelled.
//!
//! Scope grew once for a specific, measured reason. Commit `c9bd3a68`
//! enumerated the 79 `pg_catalog` functions Basin answers *only* because
//! DataFusion's builtin registry answers them, and found that 73 of the 79
//! were not merely unimplemented but **unresolvable**: [`resolve`] returned
//! `None`, so the owned planner could not even build the call and an
//! implementation in `crates/basin-exec/src/eval.rs` would have been
//! unreachable code. The catalog row has to land before the implementation.
//! Every overload of those 79 names that this crate's type vocabulary can
//! represent is therefore tabulated below — see the `The DataFusion orphans`
//! block, and the two categories under "What is deliberately absent" that it
//! could not represent.
//!
//! `age(xid)` (transaction-id wraparound, not a date/time function despite
//! the name), `trunc(macaddr)`/`trunc(macaddr8)` (network-address functions
//! sharing the `trunc` name, not math) and the `bytea` overloads of
//! `btrim`/`ltrim`/`rtrim` are real `pg_proc` rows this table does not
//! include. Not covered wrongly, just not covered yet.
//!
//! Two categories are absent for reasons stronger than "not yet", and both
//! were established while adding the DataFusion-orphan rows below:
//!
//!   * **Anything typed `bit`/`bit varying` (`pg_type` oid 1560).**
//!     [`crate::oid`] has no constant for it and [`crate::physical`] no
//!     mapping, so a row would have to name a type this crate cannot
//!     represent. That excludes `bit_length(bit)` (1812),
//!     `octet_length(bit)` (1682), `position(bit, bit)` (1698),
//!     `substring(bit, ...)` (1680, 1699), `overlay(bit, ...)` (3030, 3031)
//!     and the `bit` overloads of the `bit_and`/`bit_or`/`bit_xor`
//!     aggregates (2242, 2243, 6167) — 10 real rows, named here so their
//!     absence is a decision rather than an oversight.
//!   * **Ordered-set and hypothetical-set aggregates.** `percentile_cont`
//!     (3974, 3976, 3980, 3982, `pg_aggregate.aggkind = 'o'`) is spelled
//!     `percentile_cont(f) WITHIN GROUP (ORDER BY x)`, and the aggregate
//!     forms of `cume_dist`/`percent_rank` (3990, 3988, `aggkind = 'h'`) are
//!     spelled `cume_dist(x) WITHIN GROUP (ORDER BY y)`. [`FuncKind`] has no
//!     variant for either, and tabulating them as plain
//!     [`FuncKind::Aggregate`] would assert they are callable as ordinary
//!     two-argument aggregates — which is exactly the "same name, different
//!     function" trap DataFusion falls into for `percentile_cont`. Six real
//!     rows, deliberately omitted rather than misrepresented; representing
//!     them needs an `aggkind` this struct does not carry.
//!
//! `ceiling` — the SQL-standard-named alias of `ceil` — IS covered, as a
//! genuinely separate `pg_proc` oid per argument type, not folded into
//! `ceil`'s rows.
//!
//! A block of math rows added for `pg_proc`/planner-resolution coverage
//! (trig, `ln`/`log`/`exp`/`cbrt`, `degrees`/`radians`, `sign`, `trunc`,
//! `ceiling`, `pi`) has, as of this writing, no backing implementation in
//! `crates/basin-exec/src/eval.rs` — see that sub-block's own comment for
//! why resolving them here is still safe today rather than a regression.
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
//! Which concrete types get a row is a real coverage decision, not a
//! formality — an element type with no row is a type the function cannot be
//! called on. `array_agg` and the five `anyelement` window functions
//! (`lag`, `lead`, `first_value`, `last_value`, `nth_value`) therefore carry
//! **five** instantiations each: `int4`, `text`, `int8`, `float8` and
//! `numeric` — the four numeric widths a column in a real table actually
//! has, plus one representative string. The `int8` row is load-bearing
//! rather than aspirational: before it existed, `lag(id)` over a `bigint`
//! column resolved only because a bare column arrives typed `unknown` and
//! [`resolve`]'s implicit-coercion pass dropped it into the `int4` row.
//! Giving columns their real types — which is the correct direction — would
//! have turned that accident into `None`. `int4` is deliberately still
//! listed first in each group so the `unknown` path lands exactly where it
//! did before.
//!
//! Every other family here (the `anycompatible` array functions, `unnest`)
//! remains at the original `int4`/`text` pair; any concrete type without a
//! row is simply not covered yet, not covered wrongly. See the module docs
//! on [`crate::operator::OPERATORS`] for the same rule spelled out for
//! arrays.
//!
//! The array family added with the DataFusion-orphan block (`array_append`,
//! `array_cat`, `array_length`, `array_ndims`, `array_position`,
//! `array_positions`, `array_prepend`, `array_remove`, `array_replace`,
//! `array_reverse`, `array_sort`, `array_to_string`, `cardinality`) follows
//! exactly this rule, and is declared on the *other* polymorphic family:
//! `anycompatible` (2277's sibling, oid 5077) and `anycompatiblearray` (5078),
//! not `anyelement`/`anyarray` — a real distinction that governs how Postgres
//! unifies the argument types, and one this table does not flatten.
//!
//! **The monomorphization must never escape this crate.** It is a resolution
//! aid, not a claim about the catalog:
//! `crates/basin-pgcatalog/src/pg_proc.rs`'s `SignatureOverride` restores the
//! real polymorphic `proargtypes`/`prorettype` for every one of these oids, so
//! `pg_proc` reports `array_cat(anycompatiblearray, anycompatiblearray)`, not
//! `array_cat(int4[], int4[])`. Commit `c09b783b` found 17 cells where that
//! had gone wrong for `lag`/`lead`/`nth_value` — a driver reading parameter
//! types off `pg_proc` would have refused every non-`int4` call — and
//! `catalog_fidelity` now re-checks all of them against a live server on every
//! run. Adding a polymorphic row here without its `SignatureOverride` there is
//! a bug that harness will catch; do not silence it by monomorphizing the
//! catalog.
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
    // `concat_ws(text, VARIADIC "any") -> text` (oid 3059,
    // `pg_get_function_identity_arguments` confirms `text, VARIADIC "any"`):
    // the separator is a real fixed `text` parameter, only the values being
    // joined are the `"any"`-typed variadic part. Monomorphized at the
    // three-argument call shape (separator plus two values), the same
    // convention `concat` uses above — see the module docs.
    //
    // Not backed by `crates/basin-exec/src/eval.rs` yet (only `concat`
    // itself is) — see the "Math — trig/log/exp/power" comment below for why
    // resolving it here is still safe today.
    FuncSig::new(
        3059,
        "concat_ws",
        &[oid::TEXT, PSEUDO_ANY, PSEUDO_ANY],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // `substring(text, int, int)`/`substring(text, int)` (oids 936/937) are
    // Postgres's SQL-standard-named byte/character-offset substring
    // functions — confirmed live to have the *same* behaviour as
    // `substr`/oids 877/883 above but a genuinely different `pg_proc` oid,
    // not an alias row. `SUBSTRING(x FROM y FOR z)` desugars to this, same as
    // `substr(x, y, z)` called directly.
    //
    // `substring(text, text)`/`substring(text, text, text)` (oids 2073/2074,
    // POSIX-regex extraction) are deliberately not tabulated — a different
    // feature (needs a regex engine), not covered wrongly, just not covered
    // yet.
    //
    // Not backed by `eval.rs` yet — see below.
    FuncSig::new(
        936,
        "substring",
        &[oid::TEXT, oid::INT4, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        937,
        "substring",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // `position(text, text) -> int4` (oid 849): same argument order and
    // semantics as `strpos` above (confirmed live:
    // `pg_catalog.position('hello', 'lo')` and `strpos('hello', 'lo')` both
    // return 4) — `POSITION(substring IN string)` desugars to this call, but
    // it is a genuinely separate `pg_proc` oid from `strpos`, not a rename.
    //
    // Not backed by `eval.rs` yet — see below.
    FuncSig::new(
        849,
        "position",
        &[oid::TEXT, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    // `split_part(text, text, int) -> text` (oid 2088): splits the first
    // argument on the second and returns the `n`-th (1-indexed) field.
    //
    // Not backed by `eval.rs` yet — see below.
    FuncSig::new(
        2088,
        "split_part",
        &[oid::TEXT, oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // `left(text, integer) -> text` (oid 3060) and
    // `right(text, integer) -> text` (oid 3061), both confirmed live against
    // PostgreSQL 18.2 (`prokind = 'f'`, `prosrc` `text_left`/`text_right`).
    //
    // Unlike most of the rows around them these are *not* awaiting an
    // implementation: `crates/basin-exec/src/eval.rs` has answered both oids
    // since commit `5fedc616`, including `right`'s `INT_MIN` overflow
    // behaviour. The implementation was unreachable because these two rows
    // were missing — [`resolve`] returned `None`, so a call site could never
    // be lowered to the oid `eval.rs` was already matching on. The row is
    // what connects them.
    FuncSig::new(
        3060,
        "left",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3061,
        "right",
        &[oid::TEXT, oid::INT4],
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
    // ─── Math — trig, log/exp/power, and friends ───────────────────────────
    //
    // `basin-exec::eval` implements NONE of the rows in this sub-block today
    // (only `abs`/`round`/`ceil`/`floor` above, plus the pre-existing
    // `sqrt`/`power`/`mod` rows, which were already unbacked before this
    // sub-block was added — see docs/migration/df-removal/17-udf-rehosting.md).
    // Resolving them here is still useful, not a regression: when
    // `owned_engine`'s `RealFunctions::resolve` (`crates/basin-engine/src/
    // owned_engine.rs`) picks one of these oids and `eval_scalar_fn`'s match
    // falls through to its `other =>` arm, that `ExecError` is caught by
    // `try_execute` and turned into a `Fallback::Exec`, which makes the
    // *whole statement* fall back to DataFusion — not a user-visible error
    // (see `owned_engine::try_execute`/`Fallback`). The owned-engine bridge
    // is also behind `BASIN_OWNED_ENGINE`, default OFF. So today, adding
    // these rows changes nothing observable; it is `pg_proc` catalog/
    // resolution groundwork for `basin-exec` to grow real implementations
    // against, per the standing goal of not depending on DataFusion. Do not
    // read a row's presence here as "this function runs on Basin's own
    // executor" — check `eval.rs` for that.
    FuncSig::new(1601, "acos", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1600, "asin", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1602, "atan", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1603,
        "atan2",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    // Cube root. Confirmed live: `cbrt(27) = 3`.
    FuncSig::new(1345, "cbrt", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    // `ceiling` is the SQL-standard-named alias of `ceil` — confirmed live to
    // be a genuinely separate `pg_proc` oid per argument type (2167/2320),
    // not the same row under two names.
    FuncSig::new(
        2167,
        "ceiling",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2320,
        "ceiling",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(1605, "cos", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    // Radians -> degrees. Confirmed live: `degrees(pi()) = 180`.
    FuncSig::new(
        1608,
        "degrees",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(1347, "exp", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1732, "exp", &[oid::NUMERIC], oid::NUMERIC, FuncKind::Scalar),
    // Natural log — distinct from `log`, which is base 10 (one-arg form) or
    // an explicit base (two-arg numeric form), below.
    FuncSig::new(1341, "ln", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1734, "ln", &[oid::NUMERIC], oid::NUMERIC, FuncKind::Scalar),
    // `log(float8)` is base-10 log; there is no two-argument
    // `log(float8, float8)` overload in real Postgres — only `numeric` has
    // an explicit-base form. Confirmed live via the `pg_proc` query: exactly
    // three `log` rows exist (1340, 1736, 1741), not four.
    FuncSig::new(1340, "log", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    // `log(b numeric, x numeric)` — base first, then the value. Confirmed
    // live: `log(2, 8) = 3` (log base 2 of 8), not `log(8, 2)`.
    FuncSig::new(
        1736,
        "log",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(1741, "log", &[oid::NUMERIC], oid::NUMERIC, FuncKind::Scalar),
    FuncSig::new(1610, "pi", &[], oid::FLOAT8, FuncKind::Scalar),
    // Degrees -> radians. Confirmed live: `radians(180) = pi()`.
    FuncSig::new(
        1609,
        "radians",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    // `sign` only has `numeric`/`float8` overloads in real Postgres — no
    // fixed-width-integer rows the way `abs` has. An integer argument
    // resolves via [`resolve`]'s implicit-coercion fallback to
    // `sign(numeric)`. Confirmed live: `sign(-5) = -1`.
    FuncSig::new(
        1706,
        "sign",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(2310, "sign", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1604, "sin", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1606, "tan", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    // `trunc` truncates toward zero (unlike `floor`, which always rounds
    // down) — confirmed live: `trunc(3.7) = 3` but `trunc(-3.7) = -3`, not
    // `-4`.
    FuncSig::new(1343, "trunc", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1709,
        "trunc",
        &[oid::NUMERIC, oid::INT4],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1710,
        "trunc",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // `trunc(macaddr)`/`trunc(macaddr8)` (oids 753/4112) are real `pg_proc`
    // rows under the same name but are network-address functions (zero the
    // host portion of a MAC address), not math — deliberately not tabulated
    // here.
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
    // `extract` is NOT `date_part` with a cast, and the difference is a
    // return type, not a rendering: every row below returns **`numeric`**
    // (2 above the `float8` `date_part` returns). Measured on a live
    // PostgreSQL 18.2 rather than assumed — the two disagree in the digits:
    //
    // ```text
    // extract  (epoch  from '4024-03-15 12:34:56.654321+02'::timestamptz)
    //   = 64824402896.654321   -- numeric, every microsecond kept
    // date_part('epoch',       '4024-03-15 12:34:56.654321+02'::timestamptz)
    //   = 64824402896.65432    -- float8, the last digit gone
    //
    // extract  (second from '2024-03-15 12:34:56.789123'::timestamp)
    //   = 56.789123
    // date_part('second',   '2024-03-15 12:34:56.789123'::timestamp)
    //   = 56.789123000000004   -- float8 cannot represent it exactly
    // ```
    //
    // The pair also disagrees on which units each accepts, because
    // `date_part(text, date)` (1384) is a SQL wrapper that casts its
    // argument to `timestamp` first (`prosqlbody` reads `RETURN
    // date_part($1, ($2)::timestamp without time zone)`, read off the
    // server) while `extract(text, date)` (6199) is the C function
    // `extract_date`, which has no such cast. So `date_part('hour', DATE
    // '2024-03-15')` is `0`, but `extract(hour FROM DATE '2024-03-15')` is
    // an *error* — "unit \"hour\" not supported for type date". Both
    // measured; `crates/basin-exec/src/eval.rs` implements both behaviours
    // separately for that reason.
    //
    // Row order mirrors the `date_part` block directly above, deliberately:
    // a call whose arguments are *all* `unknown` ties in [`resolve`]'s
    // stage-1 pass and lands on whichever row comes first, and these two
    // sibling functions landing on the *same* argument type is less
    // surprising than them disagreeing. This order is no longer what decides
    // `extract(YEAR FROM <date column>)` — a bare column carries its catalog
    // type into resolution since `798b5e9c`, and the known argument elects
    // 6199 whatever the order — but it still decides the genuinely
    // all-`unknown` call, which a live server answers with ERROR 42725
    // rather than a row. See [`resolve`]'s own docs.
    FuncSig::new(
        6203,
        "extract",
        &[oid::TEXT, oid::TIMESTAMPTZ],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6202,
        "extract",
        &[oid::TEXT, oid::TIMESTAMP],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6199,
        "extract",
        &[oid::TEXT, oid::DATE],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6204,
        "extract",
        &[oid::TEXT, oid::INTERVAL],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6200,
        "extract",
        &[oid::TEXT, oid::TIME],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6201,
        "extract",
        &[oid::TEXT, oid::TIMETZ],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // `to_char`. **Postgres has no `to_char(date, text)` row** — confirmed by
    // enumerating every `to_char` in `pg_catalog` on a live PostgreSQL 18.2:
    //
    // ```text
    // 1768 to_char(interval, text)      1773 to_char(int4, text)
    // 1770 to_char(timestamptz, text)   1774 to_char(int8, text)
    // 1772 to_char(numeric, text)       1775 to_char(float4, text)
    // 2049 to_char(timestamp, text)     1776 to_char(float8, text)
    // ```
    //
    // `to_char(day, 'YYYY-MM-DD')` on a `date` column therefore resolves here
    // to **2049**, `to_char(timestamp, text)`, by the implicit `date ->
    // timestamp` cast (`pg_cast` reports `castcontext = 'i'` for that pair,
    // also read off the server). A live PostgreSQL 18.2 picks **1770**,
    // `to_char(timestamptz, text)`, instead — measured by deparsing a view
    // over a `date` column, which prints `to_char((d)::timestamp with time
    // zone, 'YYYY'::text)`. Neither row is an exact match, so the choice
    // falls to stage 2 of `func_select_candidate` (preferred type within the
    // input's own category), and `timestamptz` is the preferred type of the
    // date/time category; [`resolve`] implements stage 1 only and keeps the
    // first tabulated row. Named as a known divergence, with its cause, so it
    // is not rediscovered as a mystery: it is visible for the format fields
    // that a session timezone can move, and closing it means implementing
    // that stage, not reordering this table.
    //
    // Inventing a `(date, text)` row here would
    // report an oid no PostgreSQL has and make `\df to_char` disagree with
    // every real server, so there is none — `crates/basin-exec/src/eval.rs`
    // performs the same widening PostgreSQL's inserted cast would, at
    // evaluation time.
    //
    // Only the two temporal overloads are tabulated. The numeric/integer
    // `to_char`s (1772-1776) take an entirely different format-pattern
    // language (`9`, `0`, `D`, `G`, `PR`, `RN`, ...) that shares nothing with
    // the date/time templates below, and `to_char(interval, text)` (1768) a
    // third; none is implemented, and a row without an implementation on a
    // *name that now resolves* turns what is currently a clean "no such
    // function" into a call that resolves and then fails at execution. Named
    // here so their absence is a decision rather than an oversight.
    // 2049 comes first so that a call whose first argument is `unknown` lands
    // on the overload PostgreSQL picks for a `timestamp` column — confirmed
    // live, `to_char(ts, 'YYYY')` deparses with `ts` uncast — rather than on
    // the `timestamptz` one. For a `date` column the live server picks 1770
    // instead, per the divergence recorded above. Same first-in-table
    // ordering dependence as the `extract` block above: [`resolve`]'s
    // stage-1 pass only discriminates when an argument's type is *known*,
    // and neither of these rows matches a `date` or an `unknown` exactly.
    FuncSig::new(
        2049,
        "to_char",
        &[oid::TIMESTAMP, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1770,
        "to_char",
        &[oid::TIMESTAMPTZ, oid::TEXT],
        oid::TEXT,
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
    // `array_agg(anynonarray) -> anyarray`, oid 2335, monomorphized at five
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
        2335,
        "array_agg",
        &[oid::INT8],
        oid::INT8_ARRAY,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2335,
        "array_agg",
        &[oid::FLOAT8],
        oid::FLOAT8_ARRAY,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2335,
        "array_agg",
        &[oid::NUMERIC],
        oid::NUMERIC_ARRAY,
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
    // form, not covered here); monomorphized at `int4`, `text`, `int8`,
    // `float8` and `numeric` — see the module docs.
    //
    // The three numeric widths past `int4` are not decoration. Until they
    // landed, `lag(id)` on a `bigint` column resolved *only* by accident: a
    // bare column reached [`resolve`] typed `unknown`, missed the exact pass,
    // and fell into the `int4` row through [`cast_kind`]'s implicit
    // `unknown -> int4`. Any work that gives a column its real type — which
    // is the correct direction — turns that accident into a `None` and the
    // query into a fallback, which is exactly what an attempt at it measured
    // and then reverted. `int4` stays first in each group so the `unknown`
    // path keeps landing where it always has; the wider rows are what make
    // the *typed* path work at all.
    FuncSig::new(3106, "lag", &[oid::INT4], oid::INT4, FuncKind::Window),
    FuncSig::new(3106, "lag", &[oid::TEXT], oid::TEXT, FuncKind::Window),
    FuncSig::new(3106, "lag", &[oid::INT8], oid::INT8, FuncKind::Window),
    FuncSig::new(3106, "lag", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Window),
    FuncSig::new(3106, "lag", &[oid::NUMERIC], oid::NUMERIC, FuncKind::Window),
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
    FuncSig::new(
        3107,
        "lag",
        &[oid::INT8, oid::INT4],
        oid::INT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3107,
        "lag",
        &[oid::FLOAT8, oid::INT4],
        oid::FLOAT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3107,
        "lag",
        &[oid::NUMERIC, oid::INT4],
        oid::NUMERIC,
        FuncKind::Window,
    ),
    FuncSig::new(3109, "lead", &[oid::INT4], oid::INT4, FuncKind::Window),
    FuncSig::new(3109, "lead", &[oid::TEXT], oid::TEXT, FuncKind::Window),
    FuncSig::new(3109, "lead", &[oid::INT8], oid::INT8, FuncKind::Window),
    FuncSig::new(3109, "lead", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Window),
    FuncSig::new(
        3109,
        "lead",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Window,
    ),
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
        3110,
        "lead",
        &[oid::INT8, oid::INT4],
        oid::INT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3110,
        "lead",
        &[oid::FLOAT8, oid::INT4],
        oid::FLOAT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3110,
        "lead",
        &[oid::NUMERIC, oid::INT4],
        oid::NUMERIC,
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
        3112,
        "first_value",
        &[oid::INT8],
        oid::INT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3112,
        "first_value",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3112,
        "first_value",
        &[oid::NUMERIC],
        oid::NUMERIC,
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
        3113,
        "last_value",
        &[oid::INT8],
        oid::INT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3113,
        "last_value",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3113,
        "last_value",
        &[oid::NUMERIC],
        oid::NUMERIC,
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
    FuncSig::new(
        3114,
        "nth_value",
        &[oid::INT8, oid::INT4],
        oid::INT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3114,
        "nth_value",
        &[oid::FLOAT8, oid::INT4],
        oid::FLOAT8,
        FuncKind::Window,
    ),
    FuncSig::new(
        3114,
        "nth_value",
        &[oid::NUMERIC, oid::INT4],
        oid::NUMERIC,
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
    // ─────────────────────────────────────────────────────────────────────
    // The DataFusion orphans
    // ─────────────────────────────────────────────────────────────────────
    //
    // Every row below is a `pg_catalog` function Basin answers today ONLY
    // because DataFusion's builtin registry answers it — see commit
    // `c9bd3a68` and `crates/basin-exec/tests/orphan_functions.rs`, which is
    // the acceptance criterion for their removal. Until a row exists here,
    // [`resolve`] returns `None` and the owned planner cannot even *build*
    // the call, so no amount of work in `eval.rs` would be reachable: the
    // catalog row has to land first.
    //
    // As with the math sub-block above, a row's presence here does NOT mean
    // `basin-exec` implements the function. It means the planner can name it.
    //
    // Every oid, argument type and result type below was read off a live
    // PostgreSQL 18.2 `pg_proc` by the query in the module docs and emitted
    // mechanically — none was recalled.
    // ─── Math — hyperbolics, cot, log10, pow
    FuncSig::new(2466, "acosh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(2465, "asinh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(2467, "atanh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(2463, "cosh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(2462, "sinh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(2464, "tanh", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1607, "cot", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(1194, "log10", &[oid::FLOAT8], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        1481,
        "log10",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1346,
        "pow",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1738,
        "pow",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // ─── Integer math — factorial, gcd, lcm
    FuncSig::new(
        1376,
        "factorial",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5044,
        "gcd",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5045,
        "gcd",
        &[oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5048,
        "gcd",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5046,
        "lcm",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5047,
        "lcm",
        &[oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        5049,
        "lcm",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // ─── String — length family
    FuncSig::new(
        1811,
        "bit_length",
        &[oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1810,
        "bit_length",
        &[oid::BYTEA],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1381,
        "char_length",
        &[oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1372,
        "char_length",
        &[oid::BPCHAR],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1369,
        "character_length",
        &[oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1367,
        "character_length",
        &[oid::BPCHAR],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1374,
        "octet_length",
        &[oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        720,
        "octet_length",
        &[oid::BYTEA],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1375,
        "octet_length",
        &[oid::BPCHAR],
        oid::INT4,
        FuncKind::Scalar,
    ),
    // ─── String — case, padding, repetition
    FuncSig::new(872, "initcap", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(
        879,
        "lpad",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        873,
        "lpad",
        &[oid::TEXT, oid::INT4, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        880,
        "rpad",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        874,
        "rpad",
        &[oid::TEXT, oid::INT4, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1622,
        "repeat",
        &[oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    // ─── String — hashing
    FuncSig::new(2311, "md5", &[oid::TEXT], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(2321, "md5", &[oid::BYTEA], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(3419, "sha224", &[oid::BYTEA], oid::BYTEA, FuncKind::Scalar),
    FuncSig::new(3420, "sha256", &[oid::BYTEA], oid::BYTEA, FuncKind::Scalar),
    FuncSig::new(3421, "sha384", &[oid::BYTEA], oid::BYTEA, FuncKind::Scalar),
    FuncSig::new(3422, "sha512", &[oid::BYTEA], oid::BYTEA, FuncKind::Scalar),
    // ─── String — overlay, starts_with, to_hex
    FuncSig::new(
        1405,
        "overlay",
        &[oid::TEXT, oid::TEXT, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1404,
        "overlay",
        &[oid::TEXT, oid::TEXT, oid::INT4, oid::INT4],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        752,
        "overlay",
        &[oid::BYTEA, oid::BYTEA, oid::INT4],
        oid::BYTEA,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        749,
        "overlay",
        &[oid::BYTEA, oid::BYTEA, oid::INT4, oid::INT4],
        oid::BYTEA,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3696,
        "starts_with",
        &[oid::TEXT, oid::TEXT],
        oid::BOOL,
        FuncKind::Scalar,
    ),
    FuncSig::new(2089, "to_hex", &[oid::INT4], oid::TEXT, FuncKind::Scalar),
    FuncSig::new(2090, "to_hex", &[oid::INT8], oid::TEXT, FuncKind::Scalar),
    // ─── String — substring/position overloads
    FuncSig::new(
        2073,
        "substring",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2074,
        "substring",
        &[oid::TEXT, oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2013,
        "substring",
        &[oid::BYTEA, oid::INT4],
        oid::BYTEA,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2012,
        "substring",
        &[oid::BYTEA, oid::INT4, oid::INT4],
        oid::BYTEA,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2014,
        "position",
        &[oid::BYTEA, oid::BYTEA],
        oid::INT4,
        FuncKind::Scalar,
    ),
    // ─── Regex
    FuncSig::new(
        6254,
        "regexp_count",
        &[oid::TEXT, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6255,
        "regexp_count",
        &[oid::TEXT, oid::TEXT, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6256,
        "regexp_count",
        &[oid::TEXT, oid::TEXT, oid::INT4, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6257,
        "regexp_instr",
        &[oid::TEXT, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6258,
        "regexp_instr",
        &[oid::TEXT, oid::TEXT, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6259,
        "regexp_instr",
        &[oid::TEXT, oid::TEXT, oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6260,
        "regexp_instr",
        &[oid::TEXT, oid::TEXT, oid::INT4, oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6261,
        "regexp_instr",
        &[
            oid::TEXT,
            oid::TEXT,
            oid::INT4,
            oid::INT4,
            oid::INT4,
            oid::TEXT,
        ],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6262,
        "regexp_instr",
        &[
            oid::TEXT,
            oid::TEXT,
            oid::INT4,
            oid::INT4,
            oid::INT4,
            oid::TEXT,
            oid::INT4,
        ],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6263,
        "regexp_like",
        &[oid::TEXT, oid::TEXT],
        oid::BOOL,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6264,
        "regexp_like",
        &[oid::TEXT, oid::TEXT, oid::TEXT],
        oid::BOOL,
        FuncKind::Scalar,
    ),
    // ─── Date/time
    FuncSig::new(
        3846,
        "make_date",
        &[oid::INT4, oid::INT4, oid::INT4],
        oid::DATE,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        1284,
        "date_trunc",
        &[oid::TEXT, oid::TIMESTAMPTZ, oid::TEXT],
        oid::TIMESTAMPTZ,
        FuncKind::Scalar,
    ),
    // ─── Volatile
    FuncSig::new(1598, "random", &[], oid::FLOAT8, FuncKind::Scalar),
    FuncSig::new(
        6339,
        "random",
        &[oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6340,
        "random",
        &[oid::INT8, oid::INT8],
        oid::INT8,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6341,
        "random",
        &[oid::NUMERIC, oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Scalar,
    ),
    // ─── string_to_array
    FuncSig::new(
        394,
        "string_to_array",
        &[oid::TEXT, oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        376,
        "string_to_array",
        &[oid::TEXT, oid::TEXT, oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    // ─── Aggregates — bitwise and boolean
    FuncSig::new(
        2236,
        "bit_and",
        &[oid::INT2],
        oid::INT2,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2238,
        "bit_and",
        &[oid::INT4],
        oid::INT4,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2240,
        "bit_and",
        &[oid::INT8],
        oid::INT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(2237, "bit_or", &[oid::INT2], oid::INT2, FuncKind::Aggregate),
    FuncSig::new(2239, "bit_or", &[oid::INT4], oid::INT4, FuncKind::Aggregate),
    FuncSig::new(2241, "bit_or", &[oid::INT8], oid::INT8, FuncKind::Aggregate),
    FuncSig::new(
        6164,
        "bit_xor",
        &[oid::INT2],
        oid::INT2,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        6165,
        "bit_xor",
        &[oid::INT4],
        oid::INT4,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        6166,
        "bit_xor",
        &[oid::INT8],
        oid::INT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2517,
        "bool_and",
        &[oid::BOOL],
        oid::BOOL,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2518,
        "bool_or",
        &[oid::BOOL],
        oid::BOOL,
        FuncKind::Aggregate,
    ),
    // `every(boolean) -> boolean`, oid 2519: the SQL-standard spelling of
    // `bool_and`, and a genuinely *separate* `pg_proc` row (confirmed live —
    // 2517 and 2519 are two distinct oids, both `prokind = 'a'`, both one
    // `boolean` argument), not an alias resolved to 2517. `bool_or` has no
    // such twin; SQL never standardised a `some`/`any` aggregate spelling
    // that Postgres could tabulate, because `ANY` is already a reserved
    // quantifier.
    //
    // `crates/basin-exec/src/build.rs`'s `agg_func_of` already maps 2519 to
    // the same `AggFunc::BoolAnd` as 2517 (commit `aad32271`); this row is
    // what lets a call site reach it.
    FuncSig::new(2519, "every", &[oid::BOOL], oid::BOOL, FuncKind::Aggregate),
    // ─── Aggregates — statistical
    FuncSig::new(
        2829,
        "corr",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2827,
        "covar_pop",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2828,
        "covar_samp",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2818,
        "regr_count",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::INT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2819,
        "regr_sxx",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2820,
        "regr_syy",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2821,
        "regr_sxy",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2822,
        "regr_avgx",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2823,
        "regr_avgy",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2824,
        "regr_r2",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2825,
        "regr_slope",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2826,
        "regr_intercept",
        &[oid::FLOAT8, oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    // ─── Aggregates — stddev/variance
    FuncSig::new(
        2154,
        "stddev",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2155,
        "stddev",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2156,
        "stddev",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2157,
        "stddev",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2158,
        "stddev",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2159,
        "stddev",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2724,
        "stddev_pop",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2725,
        "stddev_pop",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2726,
        "stddev_pop",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2727,
        "stddev_pop",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2728,
        "stddev_pop",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2729,
        "stddev_pop",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2712,
        "stddev_samp",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2713,
        "stddev_samp",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2714,
        "stddev_samp",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2715,
        "stddev_samp",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2716,
        "stddev_samp",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2717,
        "stddev_samp",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2718,
        "var_pop",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2719,
        "var_pop",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2720,
        "var_pop",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2721,
        "var_pop",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2722,
        "var_pop",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2723,
        "var_pop",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2641,
        "var_samp",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2642,
        "var_samp",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2643,
        "var_samp",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2644,
        "var_samp",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2645,
        "var_samp",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2646,
        "var_samp",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    // `variance` — the historical spelling of `var_samp`, and six real
    // `pg_proc` oids of its own (2148-2153, confirmed live), not an alias
    // resolved to `var_samp`'s 2641-2646. Same argument widths, same return
    // types, same sample-variance answer; a different oid per width, exactly
    // like `stddev` alongside `stddev_samp` above.
    //
    // The return types repeat the surprise the `stddev` block records: the
    // integer widths all widen to `numeric` while `real` and `double
    // precision` stay `float8`.
    //
    // `crates/basin-exec/src/build.rs`'s `agg_func_of` maps all six to
    // `AggFunc::Variance(VarKind::VarSamp)` (commit `aad32271`), so these
    // rows do not merely stop a fallback — they reach an implementation that
    // is bit-exact with PostgreSQL where the fallback path was not.
    FuncSig::new(
        2148,
        "variance",
        &[oid::INT8],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2149,
        "variance",
        &[oid::INT4],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2150,
        "variance",
        &[oid::INT2],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2151,
        "variance",
        &[oid::FLOAT4],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2152,
        "variance",
        &[oid::FLOAT8],
        oid::FLOAT8,
        FuncKind::Aggregate,
    ),
    FuncSig::new(
        2153,
        "variance",
        &[oid::NUMERIC],
        oid::NUMERIC,
        FuncKind::Aggregate,
    ),
    // ─── Window
    FuncSig::new(3103, "percent_rank", &[], oid::FLOAT8, FuncKind::Window),
    FuncSig::new(3104, "cume_dist", &[], oid::FLOAT8, FuncKind::Window),
    FuncSig::new(3105, "ntile", &[oid::INT4], oid::INT4, FuncKind::Window),
    // ─── Arrays ─────────────────────────────────────────────────────────────
    //
    // Every row in this block is a *monomorphization* of a genuinely
    // polymorphic real `pg_proc` row, at the two representative element types
    // `unnest` also uses (`int4` and `text`) — see the module docs. This
    // family stays at the pair; `array_agg` and the `anyelement` window
    // functions went wider for a measured reason recorded there. Postgres mints one physical oid per function, not
    // one per element type, so both rows of a pair legitimately carry the same
    // oid. `crates/basin-pgcatalog/src/pg_proc.rs`'s `SignatureOverride`
    // restores the real `anyarray`/`anycompatible`/`anycompatiblearray`
    // signature for the catalog view, so nothing outside this table ever sees
    // the concrete instantiation.
    FuncSig::new(
        378,
        "array_append",
        &[oid::INT4_ARRAY, oid::INT4],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        378,
        "array_append",
        &[oid::TEXT_ARRAY, oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        379,
        "array_prepend",
        &[oid::INT4, oid::INT4_ARRAY],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        379,
        "array_prepend",
        &[oid::TEXT, oid::TEXT_ARRAY],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        383,
        "array_cat",
        &[oid::INT4_ARRAY, oid::INT4_ARRAY],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        383,
        "array_cat",
        &[oid::TEXT_ARRAY, oid::TEXT_ARRAY],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3167,
        "array_remove",
        &[oid::INT4_ARRAY, oid::INT4],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3167,
        "array_remove",
        &[oid::TEXT_ARRAY, oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3168,
        "array_replace",
        &[oid::INT4_ARRAY, oid::INT4, oid::INT4],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3168,
        "array_replace",
        &[oid::TEXT_ARRAY, oid::TEXT, oid::TEXT],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    // `array_position`'s result is a plain `int4`, not the element type — the
    // polymorphism is entirely in its arguments.
    FuncSig::new(
        3277,
        "array_position",
        &[oid::INT4_ARRAY, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3277,
        "array_position",
        &[oid::TEXT_ARRAY, oid::TEXT],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3278,
        "array_position",
        &[oid::INT4_ARRAY, oid::INT4, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3278,
        "array_position",
        &[oid::TEXT_ARRAY, oid::TEXT, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    // Likewise `array_positions`: `int4[]` whatever the element type is.
    FuncSig::new(
        3279,
        "array_positions",
        &[oid::INT4_ARRAY, oid::INT4],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3279,
        "array_positions",
        &[oid::TEXT_ARRAY, oid::TEXT],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    // `array_length(a, 1)` is NULL for an empty array while `cardinality(a)`
    // is 0 — confirmed live, and the reason these two are separate rows rather
    // than one shared helper.
    FuncSig::new(
        2176,
        "array_length",
        &[oid::INT4_ARRAY, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        2176,
        "array_length",
        &[oid::TEXT_ARRAY, oid::INT4],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        748,
        "array_ndims",
        &[oid::INT4_ARRAY],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        748,
        "array_ndims",
        &[oid::TEXT_ARRAY],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3179,
        "cardinality",
        &[oid::INT4_ARRAY],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        3179,
        "cardinality",
        &[oid::TEXT_ARRAY],
        oid::INT4,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6381,
        "array_reverse",
        &[oid::INT4_ARRAY],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6381,
        "array_reverse",
        &[oid::TEXT_ARRAY],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6388,
        "array_sort",
        &[oid::INT4_ARRAY],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6388,
        "array_sort",
        &[oid::TEXT_ARRAY],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6389,
        "array_sort",
        &[oid::INT4_ARRAY, oid::BOOL],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6389,
        "array_sort",
        &[oid::TEXT_ARRAY, oid::BOOL],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6390,
        "array_sort",
        &[oid::INT4_ARRAY, oid::BOOL, oid::BOOL],
        oid::INT4_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        6390,
        "array_sort",
        &[oid::TEXT_ARRAY, oid::BOOL, oid::BOOL],
        oid::TEXT_ARRAY,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        395,
        "array_to_string",
        &[oid::INT4_ARRAY, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        395,
        "array_to_string",
        &[oid::TEXT_ARRAY, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        384,
        "array_to_string",
        &[oid::INT4_ARRAY, oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
    ),
    FuncSig::new(
        384,
        "array_to_string",
        &[oid::TEXT_ARRAY, oid::TEXT, oid::TEXT],
        oid::TEXT,
        FuncKind::Scalar,
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

/// How many of `declared`'s parameters the call matches *exactly*, counting
/// only the arguments whose given type is actually known.
///
/// This is Postgres's first elimination stage in `func_select_candidate`
/// (`parse_func.c`), transcribed:
///
/// ```c
/// if (input_base_typeids[i] != UNKNOWNOID &&
///     current_typeids[i] == input_base_typeids[i])
///     nmatch++;
/// ```
///
/// An `unknown` argument — a bare string literal, which Postgres types
/// `unknown` too — contributes to *no* candidate's score, so it cannot vote;
/// only the arguments whose type the caller actually knows do. That is the
/// whole reason `date_part('month', day)` picks `date_part(text, date)`: the
/// literal abstains and the `date` argument elects the `date` overload.
///
/// [`PSEUDO_ANY`] deliberately does **not** count, even though
/// [`arg_matches_exact`] accepts it. Postgres compares raw oids here, and
/// `"any"` is never equal to a concrete input type, so a polymorphic
/// candidate scores zero at that position and loses to a candidate that
/// names the type — which is exactly Postgres's own preference order.
fn exact_match_count(declared: &[Oid], given: &[Oid]) -> usize {
    declared
        .iter()
        .zip(given)
        .filter(|(&d, &g)| g != oid::UNKNOWN && d == g)
        .count()
}

/// Resolve a function call to the [`FuncSig`] Postgres would pick.
///
/// Three passes, the first two shared with [`crate::operator::resolve`]:
///
/// 1. **Exact.** A candidate every argument matches without coercion (a
///    literal type match, or Postgres's `"any"` pseudo-type — see
///    [`arg_matches_exact`]). Postgres's own fast path, before
///    `func_select_candidate` is ever reached.
/// 2. **Implicit candidates.** Every candidate reachable by *implicit*
///    coercion of the given argument types ([`crate::cast::cast_kind`]
///    `== Some(CastKind::Implicit)`) — Postgres's `func_match_argtypes`. A
///    function whose match needs an assignment-only or explicit-only cast is
///    never chosen implicitly, same as for operators — see
///    `lower_text_resolves_but_lower_int4_does_not` below, which pins that
///    `int4 -> text` is assignment-only (Postgres's string I/O fallback), not
///    implicit.
/// 3. **Most exact matches on the known arguments** ([`exact_match_count`]),
///    keeping only the candidates that tie for the best score. This is
///    **stage 1 of four** in Postgres's `func_select_candidate`.
///
/// # Which stages of `func_select_candidate` are *not* implemented
///
/// Postgres runs three further eliminations when stage 1 leaves more than one
/// candidate. None of them is implemented here:
///
///   * **Stage 2 — preferred type in the *same* category.** Re-scores the
///     known arguments counting a match when the declared type is either
///     exact *or* the preferred type of the input's own type category
///     (`IsPreferredType`; cross-category conversions to a preferred type
///     score nothing).
///   * **Stage 3 — resolving the `unknown` slots by category.** For each
///     `unknown` argument position, take the category of the candidates'
///     declared types there — `STRING` always wins a conflict, since untyped
///     literals look like strings; otherwise all candidates must agree or the
///     stage fails. Then drop the candidates that take the wrong category,
///     and, if any candidate takes the category's *preferred* type, drop the
///     ones that do not. This is why `abs('5')` is `double precision` live
///     and not ambiguous: `float8` is the numeric category's preferred type.
///   * **Stage 4 — the "last gasp".** If some arguments are known and all the
///     known ones share one type, assume the `unknown` ones are that type too
///     and take the match if that is now unique.
///
/// Implementing 2 and 3 needs `pg_type.typcategory` / `typispreferred`, which
/// this crate does not tabulate today. Where they would run, this function
/// instead keeps the **first row in [`FUNCS`]** among the stage-1 survivors —
/// the historical behaviour, and the reason table order is load-bearing (the
/// module docs' `avg` note, and [`crate::operator`]'s `^` note, both depend
/// on it). Measured over every argument vector drawable from the table's own
/// declared types plus `unknown` (1,202 vectors): of the 78 `FUNCS` names
/// carrying more than one signature, 47 have at least one vector where stage
/// 1 still leaves two or more oids tied — almost always the all-`unknown`
/// call. That set is the size of the remaining gap.
///
/// # Ambiguity
///
/// When every stage fails, Postgres's `func_select_candidate` returns NULL
/// and `ParseFuncOrColumn` raises SQLSTATE **42725**:
///
/// ```text
/// ERROR:  function date_part(unknown, unknown) is not unique
/// HINT:  Could not choose a best candidate function. You might need to add
///        explicit type casts.
/// ```
///
/// (verified live: `SELECT date_part('month','2020-01-01')` is that error).
/// This function cannot report it, because a tie *here* is not evidence of a
/// tie *there* — stages 2 to 4 are missing, and they resolve most ties rather
/// than failing. Reporting ambiguity at the end of stage 1 would refuse calls
/// the real server answers, so a residual tie takes the first-in-table row
/// instead, and a genuinely ambiguous call gets an answer where Postgres
/// gets an error. That is the one direction in which this is still wrong,
/// and closing it means implementing stages 2 to 4, not tightening this one.
pub fn resolve(name: &str, args: &[Oid]) -> Option<&'static FuncSig> {
    if let Some(f) = FUNCS
        .iter()
        .find(|f| f.name == name && args_match(f.args, args, arg_matches_exact))
    {
        return Some(f);
    }

    let implicitly_matches = |f: &&FuncSig| {
        f.name == name
            && args_match(f.args, args, |declared, given| {
                arg_matches_exact(declared, given)
                    || cast_kind(given, declared) == Some(CastKind::Implicit)
            })
    };

    let best = FUNCS
        .iter()
        .filter(implicitly_matches)
        .map(|f| exact_match_count(f.args, args))
        .max()?;

    FUNCS
        .iter()
        .filter(implicitly_matches)
        .find(|f| exact_match_count(f.args, args) == best)
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

    /// `extract` is not `date_part` under another name: it has its own six
    /// oids (6199-6204, one per argument type, exactly like `date_part`) and
    /// every one of them returns **`numeric`**, where `date_part` returns
    /// `float8`. Conflating the two loses microseconds — see this module's
    /// `extract` block for the measured pair of answers that differ.
    #[test]
    fn extract_returns_numeric_where_date_part_returns_float8() {
        let cases = [
            (oid::TIMESTAMPTZ, 6203u32),
            (oid::TIMESTAMP, 6202),
            (oid::DATE, 6199),
            (oid::INTERVAL, 6204),
            (oid::TIME, 6200),
            (oid::TIMETZ, 6201),
        ];
        for (arg, want_oid) in cases {
            let ex = resolve("extract", &[oid::TEXT, arg])
                .unwrap_or_else(|| panic!("extract(text, {arg}) must resolve"));
            assert_eq!(ex.oid, Oid(want_oid));
            assert_eq!(ex.ret, oid::NUMERIC, "extract always returns numeric");

            let part = resolve("date_part", &[oid::TEXT, arg]).unwrap();
            assert_ne!(ex.oid, part.oid, "different functions, different oids");
            assert_eq!(part.ret, oid::FLOAT8, "date_part always returns float8");
        }
    }

    /// **PostgreSQL has no `to_char(date, text)`.** A `date` argument reaches
    /// `to_char(timestamp, text)` (2049) through the implicit `date ->
    /// timestamp` cast, which is what a live server does too — inventing a
    /// `(date, text)` row would report an oid no server has.
    #[test]
    fn to_char_of_a_date_resolves_to_the_timestamp_overload() {
        let by_cast = resolve("to_char", &[oid::DATE, oid::TEXT]).expect("to_char(date, text)");
        assert_eq!(by_cast.oid, Oid(2049));
        assert_eq!(by_cast.args, &[oid::TIMESTAMP, oid::TEXT]);
        assert_eq!(by_cast.ret, oid::TEXT);

        assert!(
            !FUNCS
                .iter()
                .any(|f| f.name == "to_char" && f.args == [oid::DATE, oid::TEXT]),
            "no row may claim a (date, text) overload PostgreSQL does not have"
        );

        let tstz =
            resolve("to_char", &[oid::TIMESTAMPTZ, oid::TEXT]).expect("to_char(timestamptz, text)");
        assert_eq!(tstz.oid, Oid(1770));
    }

    /// A call whose arguments are all `unknown` ties [`resolve`]'s stage-1
    /// pass — nothing is known, so nothing can discriminate — and lands on
    /// the first matching row, which is why the row order in the
    /// `extract`/`to_char` blocks is load-bearing and pinned here. `extract`
    /// mirrors `date_part`'s own first row deliberately; `to_char` leads with
    /// the `timestamp` overload, the one a real server picks for a
    /// `timestamp` column (for a `date` column it picks the `timestamptz`
    /// row, a stage-2 divergence recorded on the table itself).
    #[test]
    fn unknown_arguments_land_on_the_documented_first_row() {
        let ex = resolve("extract", &[oid::UNKNOWN, oid::UNKNOWN]).unwrap();
        let part = resolve("date_part", &[oid::UNKNOWN, oid::UNKNOWN]).unwrap();
        assert_eq!(ex.args, part.args, "extract follows date_part's first row");
        assert_eq!(ex.oid, Oid(6203));
        assert_eq!(part.oid, Oid(1171));

        let tc = resolve("to_char", &[oid::UNKNOWN, oid::UNKNOWN]).unwrap();
        assert_eq!(tc.oid, Oid(2049));
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

    /// `substring(text, int, int)`/`substring(text, int)` (oids 936/937) are
    /// a distinct `pg_proc` entry from `substr` (oids 877/883) — same
    /// behaviour, different real oid, confirmed live — so they must not
    /// collapse onto `substr`'s rows.
    #[test]
    fn substring_is_a_distinct_oid_from_substr() {
        let sub3 = resolve("substring", &[oid::TEXT, oid::INT4, oid::INT4])
            .expect("substring(text, int, int) must resolve");
        let sub2 = resolve("substring", &[oid::TEXT, oid::INT4])
            .expect("substring(text, int) must resolve");
        assert_eq!(sub3.oid, Oid(936));
        assert_eq!(sub2.oid, Oid(937));

        let substr3 = resolve("substr", &[oid::TEXT, oid::INT4, oid::INT4]).unwrap();
        let substr2 = resolve("substr", &[oid::TEXT, oid::INT4]).unwrap();
        assert_ne!(
            sub3.oid, substr3.oid,
            "substring and substr must not share an oid"
        );
        assert_ne!(sub2.oid, substr2.oid);
    }

    /// `position(text, text)` (oid 849) is a real, separate `pg_proc` row
    /// from `strpos` (oid 868) even though both share the same
    /// (string, substring) argument order and return the same value —
    /// confirmed live: `position('hello', 'lo')` and `strpos('hello', 'lo')`
    /// both return 4.
    #[test]
    fn position_is_a_distinct_oid_from_strpos_with_the_same_argument_order() {
        let position = resolve("position", &[oid::TEXT, oid::TEXT])
            .expect("position(text, text) must resolve");
        let strpos = resolve("strpos", &[oid::TEXT, oid::TEXT]).unwrap();
        assert_eq!(position.oid, Oid(849));
        assert_ne!(position.oid, strpos.oid);
        assert_eq!(position.ret, oid::INT4);
    }

    /// `split_part(text, text, int)` resolves and returns `text`.
    #[test]
    fn split_part_resolves() {
        let f = resolve("split_part", &[oid::TEXT, oid::TEXT, oid::INT4])
            .expect("split_part(text, text, int) must resolve");
        assert_eq!(f.oid, Oid(2088));
        assert_eq!(f.ret, oid::TEXT);
    }

    /// `concat_ws` is monomorphized at (text, any, any) — the separator slot
    /// is a real fixed `text` parameter (unlike `concat`'s all-`"any"`
    /// slots), so a non-text separator must not resolve while the value
    /// slots still accept heterogeneous types.
    #[test]
    fn concat_ws_has_a_fixed_text_separator_and_any_typed_values() {
        let f = resolve("concat_ws", &[oid::TEXT, oid::TEXT, oid::INT4])
            .expect("concat_ws(text, text, int4) must resolve");
        assert_eq!(f.oid, Oid(3059));
        assert_eq!(f.ret, oid::TEXT);
    }

    /// `ceiling` is the SQL-standard-named alias of `ceil` — same behaviour,
    /// but a genuinely different real oid per argument type, confirmed live.
    /// Must not collapse onto `ceil`'s rows.
    #[test]
    fn ceiling_is_a_distinct_oid_per_type_from_ceil() {
        let ceiling_numeric = resolve("ceiling", &[oid::NUMERIC]).unwrap();
        let ceiling_float = resolve("ceiling", &[oid::FLOAT8]).unwrap();
        let ceil_numeric = resolve("ceil", &[oid::NUMERIC]).unwrap();
        let ceil_float = resolve("ceil", &[oid::FLOAT8]).unwrap();
        assert_eq!(ceiling_numeric.oid, Oid(2167));
        assert_eq!(ceiling_float.oid, Oid(2320));
        assert_ne!(ceiling_numeric.oid, ceil_numeric.oid);
        assert_ne!(ceiling_float.oid, ceil_float.oid);
    }

    /// `log` has three real oids, not four: a one-argument `float8` form
    /// (base 10), a one-argument `numeric` form (base 10), and a two-argument
    /// `numeric` form with an explicit base — there is no two-argument
    /// `float8` overload in real Postgres. The two-argument form takes
    /// `(base, x)`, confirmed live via `log(2, 8) = 3`, not `(x, base)`.
    #[test]
    fn log_has_three_oids_base_first_in_the_two_arg_form() {
        let log_float = resolve("log", &[oid::FLOAT8]).expect("log(float8) must resolve");
        let log_numeric = resolve("log", &[oid::NUMERIC]).expect("log(numeric) must resolve");
        let log_base = resolve("log", &[oid::NUMERIC, oid::NUMERIC])
            .expect("log(numeric, numeric) must resolve");
        assert_eq!(log_float.oid, Oid(1340));
        assert_eq!(log_numeric.oid, Oid(1741));
        assert_eq!(log_base.oid, Oid(1736));
        assert_ne!(log_float.oid, log_numeric.oid);
        assert_ne!(log_numeric.oid, log_base.oid);
        assert_eq!(
            resolve("log", &[oid::FLOAT8, oid::FLOAT8]),
            None,
            "log(float8, float8) is not a real Postgres overload"
        );
    }

    /// `ln` (natural log) is a distinct oid per type from `log` (base 10 in
    /// the one-argument form) — different function, not an alias.
    #[test]
    fn ln_is_distinct_from_log() {
        let ln_float = resolve("ln", &[oid::FLOAT8]).unwrap();
        let log_float = resolve("log", &[oid::FLOAT8]).unwrap();
        assert_ne!(ln_float.oid, log_float.oid);
        assert_eq!(ln_float.oid, Oid(1341));
    }

    /// `sign` has no fixed-width-integer overloads in real Postgres (unlike
    /// `abs`) — an `int4` argument must reach `sign(numeric)` via implicit
    /// widening, the same pattern `avg` uses (see the module docs).
    #[test]
    fn sign_has_no_integer_overload_and_widens_via_implicit_cast() {
        let sign_numeric = resolve("sign", &[oid::NUMERIC]).unwrap();
        let sign_float = resolve("sign", &[oid::FLOAT8]).unwrap();
        assert_eq!(sign_numeric.oid, Oid(1706));
        assert_eq!(sign_float.oid, Oid(2310));
        assert_ne!(sign_numeric.oid, sign_float.oid);

        let from_int4 = resolve("sign", &[oid::INT4]).expect("sign(int4) must widen to numeric");
        assert_eq!(from_int4.oid, Oid(1706), "should land on sign(numeric)");
    }

    /// `trunc` has a `float8` one-argument form plus `numeric` one- and
    /// two-argument (explicit scale) forms — three different real oids, the
    /// same "distinct arities, distinct oids" shape `round` already has.
    #[test]
    fn trunc_has_distinct_arities_and_a_separate_float_overload() {
        let numeric_one = resolve("trunc", &[oid::NUMERIC]).unwrap();
        let numeric_two = resolve("trunc", &[oid::NUMERIC, oid::INT4]).unwrap();
        let float_one = resolve("trunc", &[oid::FLOAT8]).unwrap();
        assert_eq!(numeric_one.oid, Oid(1710));
        assert_eq!(numeric_two.oid, Oid(1709));
        assert_eq!(float_one.oid, Oid(1343));
        assert_ne!(numeric_one.oid, float_one.oid);
    }

    /// `atan2` takes two `float8` arguments — pinning the arity distinguishes
    /// it from the single-argument `atan`, `asin`, `acos`, `sin`, `cos`,
    /// `tan` family it sits alongside.
    #[test]
    fn atan2_is_the_two_argument_form() {
        let atan2 = resolve("atan2", &[oid::FLOAT8, oid::FLOAT8]).expect("atan2 must resolve");
        assert_eq!(atan2.oid, Oid(1603));
        assert_eq!(atan2.ret, oid::FLOAT8);

        for name in ["atan", "asin", "acos", "sin", "cos", "tan"] {
            let f = resolve(name, &[oid::FLOAT8])
                .unwrap_or_else(|| panic!("{name}(float8) must resolve"));
            assert_eq!(f.kind, FuncKind::Scalar);
            assert_eq!(f.ret, oid::FLOAT8);
        }
    }

    /// `pi()` is niladic, like `now()`.
    #[test]
    fn pi_is_niladic() {
        let pi = resolve("pi", &[]).expect("pi() must resolve");
        assert_eq!(pi.oid, Oid(1610));
        assert_eq!(pi.ret, oid::FLOAT8);
        assert_eq!(resolve("pi", &[oid::FLOAT8]), None);
    }

    /// `cbrt`, `exp`, `degrees`, `radians` each resolve for `float8`; `exp`
    /// additionally has a `numeric` overload (`ln` mirrors it).
    #[test]
    fn remaining_math_family_rows_resolve() {
        for name in ["cbrt", "exp", "degrees", "radians"] {
            resolve(name, &[oid::FLOAT8]).unwrap_or_else(|| panic!("{name}(float8) must resolve"));
        }
        let exp_numeric = resolve("exp", &[oid::NUMERIC]).expect("exp(numeric) must resolve");
        assert_eq!(exp_numeric.oid, Oid(1732));
        let ln_numeric = resolve("ln", &[oid::NUMERIC]).expect("ln(numeric) must resolve");
        assert_eq!(ln_numeric.oid, Oid(1734));
    }

    // ─── The DataFusion orphans ─────────────────────────────────────────

    /// The whole point of the orphan block: every one of these was
    /// `UNRESOLVABLE` before it landed, meaning the owned planner could not
    /// build the call at all. Each OID here was read off a live PostgreSQL
    /// 18.2 `pg_proc`. A wrong OID is worse than a missing row — it resolves
    /// to the wrong function — so they are pinned individually rather than
    /// merely asserted non-`None`.
    #[test]
    fn orphan_scalar_overloads_resolve_to_their_real_oids() {
        let cases: &[(&str, &[Oid], u32)] = &[
            ("acosh", &[oid::FLOAT8], 2466),
            ("asinh", &[oid::FLOAT8], 2465),
            ("atanh", &[oid::FLOAT8], 2467),
            ("cosh", &[oid::FLOAT8], 2463),
            ("sinh", &[oid::FLOAT8], 2462),
            ("tanh", &[oid::FLOAT8], 2464),
            ("cot", &[oid::FLOAT8], 1607),
            ("log10", &[oid::FLOAT8], 1194),
            ("log10", &[oid::NUMERIC], 1481),
            ("pow", &[oid::FLOAT8, oid::FLOAT8], 1346),
            ("pow", &[oid::NUMERIC, oid::NUMERIC], 1738),
            ("factorial", &[oid::INT8], 1376),
            ("gcd", &[oid::INT4, oid::INT4], 5044),
            ("gcd", &[oid::INT8, oid::INT8], 5045),
            ("lcm", &[oid::INT4, oid::INT4], 5046),
            ("lcm", &[oid::INT8, oid::INT8], 5047),
            ("initcap", &[oid::TEXT], 872),
            ("md5", &[oid::TEXT], 2311),
            ("md5", &[oid::BYTEA], 2321),
            ("octet_length", &[oid::TEXT], 1374),
            ("octet_length", &[oid::BYTEA], 720),
            ("bit_length", &[oid::TEXT], 1811),
            ("char_length", &[oid::TEXT], 1381),
            ("character_length", &[oid::TEXT], 1369),
            ("repeat", &[oid::TEXT, oid::INT4], 1622),
            ("lpad", &[oid::TEXT, oid::INT4], 879),
            ("lpad", &[oid::TEXT, oid::INT4, oid::TEXT], 873),
            ("rpad", &[oid::TEXT, oid::INT4], 880),
            ("rpad", &[oid::TEXT, oid::INT4, oid::TEXT], 874),
            ("starts_with", &[oid::TEXT, oid::TEXT], 3696),
            ("string_to_array", &[oid::TEXT, oid::TEXT], 394),
            ("string_to_array", &[oid::TEXT, oid::TEXT, oid::TEXT], 376),
            ("to_hex", &[oid::INT4], 2089),
            ("to_hex", &[oid::INT8], 2090),
            ("regexp_count", &[oid::TEXT, oid::TEXT], 6254),
            ("regexp_instr", &[oid::TEXT, oid::TEXT], 6257),
            ("regexp_like", &[oid::TEXT, oid::TEXT], 6263),
            ("sha224", &[oid::BYTEA], 3419),
            ("sha256", &[oid::BYTEA], 3420),
            ("sha384", &[oid::BYTEA], 3421),
            ("sha512", &[oid::BYTEA], 3422),
            ("overlay", &[oid::TEXT, oid::TEXT, oid::INT4], 1405),
            (
                "overlay",
                &[oid::TEXT, oid::TEXT, oid::INT4, oid::INT4],
                1404,
            ),
            ("make_date", &[oid::INT4, oid::INT4, oid::INT4], 3846),
            ("random", &[], 1598),
        ];
        for (name, args, want) in cases {
            let f = resolve(name, args)
                .unwrap_or_else(|| panic!("{name}({args:?}) must resolve — it is an orphan row"));
            assert_eq!(
                f.oid,
                Oid(*want),
                "{name}({args:?}) resolved to the wrong oid"
            );
        }
    }

    /// `pow`/`log10` are separate real `pg_proc` oids from `power`/`log`, not
    /// aliases pointing at the same row — confirmed live. Collapsing them
    /// would make `pg_proc` report a function Postgres does not have there.
    #[test]
    fn pow_and_log10_are_distinct_oids_from_power_and_log() {
        let pow = resolve("pow", &[oid::FLOAT8, oid::FLOAT8]).unwrap();
        let power = resolve("power", &[oid::FLOAT8, oid::FLOAT8]).unwrap();
        assert_eq!(pow.oid, Oid(1346));
        assert_ne!(pow.oid, power.oid, "pow and power are different oids");

        let log10 = resolve("log10", &[oid::FLOAT8]).unwrap();
        let log = resolve("log", &[oid::FLOAT8]).unwrap();
        assert_eq!(log10.oid, Oid(1194));
        assert_ne!(log10.oid, log.oid, "log10 and log are different oids");
    }

    /// The polymorphic array family shares one oid across element types, the
    /// same invariant `array_agg`/`unnest` already have — and every one of
    /// them monomorphizes to a *different* concrete result type while keeping
    /// the single real oid.
    #[test]
    fn polymorphic_array_functions_share_one_oid_across_element_types() {
        let int_cat = resolve("array_cat", &[oid::INT4_ARRAY, oid::INT4_ARRAY])
            .expect("array_cat(int4[], int4[]) must resolve");
        let text_cat = resolve("array_cat", &[oid::TEXT_ARRAY, oid::TEXT_ARRAY]).unwrap();
        assert_eq!(int_cat.oid, Oid(383));
        assert_eq!(
            int_cat.oid, text_cat.oid,
            "one real oid, two instantiations"
        );
        assert_eq!(int_cat.ret, oid::INT4_ARRAY);
        assert_eq!(text_cat.ret, oid::TEXT_ARRAY);

        let append = resolve("array_append", &[oid::TEXT_ARRAY, oid::TEXT]).unwrap();
        assert_eq!(append.oid, Oid(378));
        assert_eq!(append.ret, oid::TEXT_ARRAY);

        // `array_prepend` takes (element, array) — the reverse of
        // `array_append`. Getting the order wrong is a silent wrong answer,
        // not a resolution failure, so it is pinned.
        let prepend = resolve("array_prepend", &[oid::INT4, oid::INT4_ARRAY]).unwrap();
        assert_eq!(prepend.oid, Oid(379));
        assert_eq!(
            resolve("array_prepend", &[oid::INT4_ARRAY, oid::INT4]),
            None,
            "array_prepend(array, element) is not a real signature"
        );
    }

    /// `array_length`, `cardinality` and `array_ndims` return a plain
    /// `integer` whatever the element type, and `array_positions` a plain
    /// `integer[]` — their polymorphism is entirely in the arguments. A row
    /// that made the result follow the element type would be wrong, and is
    /// exactly the mistake `pg_proc`'s `SignatureOverride` exists to prevent
    /// in the other direction.
    #[test]
    fn array_measurement_functions_return_integers_not_the_element_type() {
        for (name, args, oid_want) in [
            ("array_length", vec![oid::TEXT_ARRAY, oid::INT4], 2176u32),
            ("cardinality", vec![oid::TEXT_ARRAY], 3179),
            ("array_ndims", vec![oid::TEXT_ARRAY], 748),
        ] {
            let f = resolve(name, &args).unwrap_or_else(|| panic!("{name} must resolve"));
            assert_eq!(f.oid, Oid(oid_want));
            assert_eq!(f.ret, oid::INT4, "{name} returns integer, not the element");
        }

        let positions = resolve("array_positions", &[oid::TEXT_ARRAY, oid::TEXT]).unwrap();
        assert_eq!(positions.oid, Oid(3279));
        assert_eq!(positions.ret, oid::INT4_ARRAY, "positions, not elements");
    }

    /// The statistical aggregates are `Aggregate`, not `Scalar`, and their
    /// return types are the surprising part: `stddev(int4)` widens to
    /// `numeric` while `stddev(float8)` stays `float8`, and `regr_count`
    /// returns `bigint` while every other `regr_*` returns `float8`.
    /// Confirmed live.
    #[test]
    fn statistical_aggregates_carry_their_real_return_types() {
        let s_int = resolve("stddev", &[oid::INT4]).expect("stddev(int4) must resolve");
        assert_eq!(s_int.oid, Oid(2155));
        assert_eq!(s_int.ret, oid::NUMERIC);
        assert_eq!(s_int.kind, FuncKind::Aggregate);

        let s_float = resolve("stddev", &[oid::FLOAT8]).unwrap();
        assert_eq!(s_float.oid, Oid(2158));
        assert_eq!(s_float.ret, oid::FLOAT8);

        let count = resolve("regr_count", &[oid::FLOAT8, oid::FLOAT8]).unwrap();
        assert_eq!(count.oid, Oid(2818));
        assert_eq!(count.ret, oid::INT8, "regr_count is bigint, not float8");
        for name in ["regr_slope", "regr_r2", "regr_sxx", "corr", "covar_pop"] {
            let f = resolve(name, &[oid::FLOAT8, oid::FLOAT8])
                .unwrap_or_else(|| panic!("{name} must resolve"));
            assert_eq!(f.ret, oid::FLOAT8, "{name} returns float8");
            assert!(is_aggregate(f.oid), "{name} must be an aggregate");
        }
    }

    /// `bit_and`/`bit_or`/`bit_xor` have one oid per integer width, each
    /// returning that same width — not one polymorphic row and not a single
    /// `int4` row that everything widens into.
    #[test]
    fn bitwise_aggregates_have_one_oid_per_integer_width() {
        for (arg, want) in [(oid::INT2, 2236u32), (oid::INT4, 2238), (oid::INT8, 2240)] {
            let f = resolve("bit_and", &[arg]).expect("bit_and must resolve");
            assert_eq!(f.oid, Oid(want));
            assert_eq!(f.ret, arg, "bit_and preserves its argument's width");
            assert!(is_aggregate(f.oid));
        }
        assert_eq!(resolve("bit_or", &[oid::INT4]).unwrap().oid, Oid(2239));
        assert_eq!(resolve("bit_xor", &[oid::INT4]).unwrap().oid, Oid(6165));
    }

    /// `percent_rank`/`cume_dist` are niladic *window* functions here (oids
    /// 3103/3104). Their same-named `pg_proc` rows 3988/3990 are
    /// hypothetical-set *aggregates* — a different function that this table
    /// deliberately does not carry (see the module docs), so a one-argument
    /// call must not resolve to something that would run as a plain
    /// aggregate.
    #[test]
    fn percent_rank_and_cume_dist_are_the_window_rows_not_the_aggregate_ones() {
        for (name, want) in [("percent_rank", 3103u32), ("cume_dist", 3104)] {
            let f = resolve(name, &[]).unwrap_or_else(|| panic!("{name}() must resolve"));
            assert_eq!(f.oid, Oid(want));
            assert_eq!(f.kind, FuncKind::Window);
            assert!(is_window(f.oid));
            assert!(!is_aggregate(f.oid));
            assert_eq!(
                resolve(name, &[oid::INT4]),
                None,
                "{name}(x) is the hypothetical-set aggregate, which is not tabulated"
            );
        }

        let ntile = resolve("ntile", &[oid::INT4]).expect("ntile(int) must resolve");
        assert_eq!(ntile.oid, Oid(3105));
        assert_eq!(ntile.kind, FuncKind::Window);
    }

    /// `percentile_cont` is an ordered-set aggregate
    /// (`percentile_cont(f) WITHIN GROUP (ORDER BY x)`); [`FuncKind`] cannot
    /// express that, so it is absent rather than misrepresented as a plain
    /// two-argument aggregate. This pins the *absence*, because the failure
    /// mode of getting it wrong is silent: a call would resolve and then run
    /// a different function than Postgres would.
    #[test]
    fn percentile_cont_is_absent_rather_than_a_plain_two_arg_aggregate() {
        assert_eq!(
            resolve("percentile_cont", &[oid::FLOAT8, oid::FLOAT8]),
            None
        );
        assert!(
            !FUNCS.iter().any(|f| f.name == "percentile_cont"),
            "no percentile_cont row may exist until FuncKind can carry aggkind"
        );
    }

    /// Adding the orphan rows must not have moved an existing resolution.
    /// `substring` gains a `(text, text)` regex overload and two `bytea`
    /// ones; `position` gains a `bytea` one; `date_trunc` gains its
    /// three-argument timezone-name form. None of them may shadow the row
    /// that was already there.
    #[test]
    fn new_overloads_do_not_shadow_the_existing_rows_of_the_same_name() {
        assert_eq!(
            resolve("substring", &[oid::TEXT, oid::INT4]).unwrap().oid,
            Oid(937)
        );
        assert_eq!(
            resolve("substring", &[oid::TEXT, oid::INT4, oid::INT4])
                .unwrap()
                .oid,
            Oid(936)
        );
        assert_eq!(
            resolve("substring", &[oid::TEXT, oid::TEXT]).unwrap().oid,
            Oid(2073),
            "the regex overload is a different row, not a replacement"
        );
        assert_eq!(
            resolve("substring", &[oid::BYTEA, oid::INT4]).unwrap().oid,
            Oid(2013)
        );

        assert_eq!(
            resolve("position", &[oid::TEXT, oid::TEXT]).unwrap().oid,
            Oid(849)
        );
        assert_eq!(
            resolve("position", &[oid::BYTEA, oid::BYTEA]).unwrap().oid,
            Oid(2014)
        );

        assert_eq!(
            resolve("date_trunc", &[oid::TEXT, oid::TIMESTAMPTZ])
                .unwrap()
                .oid,
            Oid(1217)
        );
        assert_eq!(
            resolve("date_trunc", &[oid::TEXT, oid::TIMESTAMPTZ, oid::TEXT])
                .unwrap()
                .oid,
            Oid(1284),
            "the three-argument timezone-name form is now covered"
        );
    }

    /// No row may name a type this crate cannot represent — the guard that
    /// keeps the `bit`-typed overloads named in the module docs from being
    /// added by reflex. [`every_function_has_a_named_or_array_result_type`]
    /// covers results; this covers arguments, which nothing checked before.
    #[test]
    fn every_function_argument_is_a_named_or_array_type() {
        for f in FUNCS {
            for (i, arg) in f.args.iter().enumerate() {
                assert!(
                    oid::type_name(*arg).is_some() || oid::is_array(*arg) || *arg == PSEUDO_ANY,
                    "function {} ({}) argument {i} has an unrecognized oid {}",
                    f.name,
                    f.oid,
                    arg
                );
            }
        }
    }

    /// A single oid must never carry two different `FuncKind`s or two
    /// different names across its monomorphized rows — `pg_proc` reports one
    /// row per oid (the first), so a disagreement here would make the catalog
    /// row depend on table order.
    #[test]
    fn rows_sharing_an_oid_agree_on_name_and_kind() {
        for f in FUNCS {
            for g in FUNCS.iter().filter(|g| g.oid == f.oid) {
                assert_eq!(f.name, g.name, "oid {} has two names", f.oid);
                assert_eq!(f.kind, g.kind, "oid {} has two kinds", f.oid);
            }
        }
    }

    // ─── Rows whose implementation landed before the row ────────────────

    /// `left` and `right` are the inverse of the "no implementation yet"
    /// math block: `crates/basin-exec/src/eval.rs` has answered oids 3060
    /// and 3061 since commit `5fedc616`, but with no row here [`resolve`]
    /// returned `None`, so no call site could ever be lowered to them. Both
    /// oids read off a live PostgreSQL 18.2.
    #[test]
    fn left_and_right_resolve_to_their_real_oids() {
        let left = resolve("left", &[oid::TEXT, oid::INT4]).expect("left(text, int4)");
        assert_eq!(left.oid, Oid(3060));
        assert_eq!(left.ret, oid::TEXT);
        assert_eq!(left.kind, FuncKind::Scalar);

        let right = resolve("right", &[oid::TEXT, oid::INT4]).expect("right(text, int4)");
        assert_eq!(right.oid, Oid(3061));
        assert_eq!(right.ret, oid::TEXT);
        assert_eq!(right.kind, FuncKind::Scalar);

        assert_ne!(left.oid, right.oid);
    }

    /// `variance` is six real oids of its own, not an alias of `var_samp`'s
    /// 2641-2646. Pinned individually because resolving `variance(float8)`
    /// to a `var_samp` oid would be exactly the "same name, different
    /// function" trap the module docs warn about — and because the six are
    /// what `crates/basin-exec/src/build.rs` matches on.
    ///
    /// The return types are the surprising part and repeat `stddev`'s: the
    /// three integer widths widen to `numeric`, `float4`/`float8` stay
    /// `float8`.
    #[test]
    fn variance_has_six_oids_distinct_from_var_samp() {
        let cases: &[(Oid, u32, Oid)] = &[
            (oid::INT8, 2148, oid::NUMERIC),
            (oid::INT4, 2149, oid::NUMERIC),
            (oid::INT2, 2150, oid::NUMERIC),
            (oid::FLOAT4, 2151, oid::FLOAT8),
            (oid::FLOAT8, 2152, oid::FLOAT8),
            (oid::NUMERIC, 2153, oid::NUMERIC),
        ];
        for &(arg, want_oid, want_ret) in cases {
            let f = resolve("variance", &[arg])
                .unwrap_or_else(|| panic!("variance({arg}) must resolve"));
            assert_eq!(f.oid, Oid(want_oid), "variance({arg})");
            assert_eq!(f.ret, want_ret, "variance({arg}) return type");
            assert_eq!(f.kind, FuncKind::Aggregate);

            let same_width = resolve("var_samp", &[arg]).unwrap();
            assert_ne!(
                f.oid, same_width.oid,
                "variance({arg}) and var_samp({arg}) are different pg_proc rows"
            );
            assert_eq!(f.ret, same_width.ret, "but the same answer and type");
        }
    }

    /// `every(boolean)` is the SQL-standard spelling of `bool_and` and a
    /// separate real oid (2519), which `basin-exec` maps onto the same
    /// accumulator as 2517. `bool_or` has no such twin.
    #[test]
    fn every_is_a_separate_oid_from_bool_and() {
        let every = resolve("every", &[oid::BOOL]).expect("every(bool)");
        let bool_and = resolve("bool_and", &[oid::BOOL]).unwrap();
        assert_eq!(every.oid, Oid(2519));
        assert_eq!(bool_and.oid, Oid(2517));
        assert_ne!(every.oid, bool_and.oid);
        assert_eq!(every.ret, oid::BOOL);
        assert_eq!(every.kind, FuncKind::Aggregate);
    }

    /// The polymorphic monomorphizations must cover the numeric widths a
    /// real column actually has, not just `int4`. Before the `int8` rows
    /// landed, `array_agg(id)`/`lag(id)` over a `bigint` column resolved
    /// only through the `unknown -> int4` implicit cast a bare column gets;
    /// the moment a caller supplies the real type, `int4` no longer matches
    /// and the call becomes unresolvable. Every row still reports the one
    /// real polymorphic oid.
    #[test]
    fn polymorphic_rows_cover_int8_float8_and_numeric() {
        let one_arg: &[(&str, u32)] = &[
            ("array_agg", 2335),
            ("lag", 3106),
            ("lead", 3109),
            ("first_value", 3112),
            ("last_value", 3113),
        ];
        for &(name, want_oid) in one_arg {
            for arg in [oid::INT4, oid::TEXT, oid::INT8, oid::FLOAT8, oid::NUMERIC] {
                let f =
                    resolve(name, &[arg]).unwrap_or_else(|| panic!("{name}({arg}) must resolve"));
                assert_eq!(f.oid, Oid(want_oid), "{name}({arg}) must keep the real oid");
            }
        }

        let two_arg: &[(&str, u32)] = &[("lag", 3107), ("lead", 3110), ("nth_value", 3114)];
        for &(name, want_oid) in two_arg {
            for arg in [oid::INT4, oid::TEXT, oid::INT8, oid::FLOAT8, oid::NUMERIC] {
                let f = resolve(name, &[arg, oid::INT4])
                    .unwrap_or_else(|| panic!("{name}({arg}, int4) must resolve"));
                assert_eq!(f.oid, Oid(want_oid), "{name}({arg}, int4)");
            }
        }
    }

    /// The element type must follow the argument type, not be pinned to
    /// `int4[]` — `array_agg(bigint)` returning `int4[]` would be a wrong
    /// answer about the result's wire type, not merely a narrow one.
    #[test]
    fn array_agg_result_element_type_follows_its_argument() {
        let cases: &[(Oid, Oid)] = &[
            (oid::INT4, oid::INT4_ARRAY),
            (oid::TEXT, oid::TEXT_ARRAY),
            (oid::INT8, oid::INT8_ARRAY),
            (oid::FLOAT8, oid::FLOAT8_ARRAY),
            (oid::NUMERIC, oid::NUMERIC_ARRAY),
        ];
        for &(arg, want) in cases {
            let f = resolve("array_agg", &[arg]).unwrap();
            assert_eq!(f.ret, want, "array_agg({arg})");
            assert_eq!(oid::element_of(f.ret), Some(arg));
        }
    }

    /// The accident the wider rows exist to make unnecessary must keep
    /// working while it is still load-bearing: a bare column arrives typed
    /// `unknown`, and `int4` is first in every monomorphized group so the
    /// implicit-coercion pass still lands there rather than on a row added
    /// later.
    #[test]
    fn unknown_argument_still_lands_on_the_int4_monomorphization() {
        for name in ["array_agg", "lag", "lead", "first_value", "last_value"] {
            let f = resolve(name, &[oid::UNKNOWN])
                .unwrap_or_else(|| panic!("{name}(unknown) must still resolve"));
            assert_eq!(
                f.args,
                &[oid::INT4],
                "{name}(unknown) must still pick the int4 row"
            );
        }
    }

    /// A bare `'month'` is `unknown` — Postgres types it that way too — so
    /// the all-exact pass fails as a whole and every date/time overload
    /// survives implicit coercion. Postgres's `func_select_candidate` then
    /// discards the candidates with fewer exact matches on the arguments
    /// whose type *is* known, which leaves exactly the overload named after
    /// the second argument's type. Without that stage the implicit pass takes
    /// whichever row is first in [`FUNCS`], and `timestamptz` sits ahead of
    /// `date` — which is how `date_part('month', day)` on a `date` column
    /// resolved to 1171 and fed a `Date32` to a `timestamptz` evaluator.
    ///
    /// Every expectation below is the live server's own answer, read off the
    /// deparse of a view over a table with one column of each type
    /// (`pg_get_viewdef` prints the argument casts Postgres inserted, so the
    /// overload it chose is visible): `date_part('month'::text, d)` with `d`
    /// *uncast* is `date_part(text, date)`, oid 1384.
    #[test]
    fn a_known_argument_elects_its_own_overload_against_an_unknown_literal() {
        let cases: &[(&str, Oid, u32)] = &[
            ("date_part", oid::DATE, 1384),
            ("date_part", oid::TIME, 1385),
            ("date_part", oid::TIMESTAMP, 2021),
            ("extract", oid::DATE, 6199),
            ("extract", oid::TIME, 6200),
            ("extract", oid::TIMESTAMP, 6202),
        ];
        for &(name, arg, want) in cases {
            let f = resolve(name, &[oid::UNKNOWN, arg])
                .unwrap_or_else(|| panic!("{name}(unknown, {arg}) must resolve"));
            assert_eq!(
                f.oid,
                Oid(want),
                "{name}(unknown, {arg}) must pick the overload the known argument names"
            );
            assert_eq!(f.args[1], arg, "and it must take that type uncoerced");
        }

        // The timestamptz overloads stay reachable when that IS the argument
        // type — this stage narrows the choice, it does not delete rows.
        assert_eq!(
            resolve("date_part", &[oid::UNKNOWN, oid::TIMESTAMPTZ])
                .unwrap()
                .oid,
            Oid(1171)
        );
        assert_eq!(
            resolve("extract", &[oid::UNKNOWN, oid::TIMESTAMPTZ])
                .unwrap()
                .oid,
            Oid(6203)
        );
    }

    /// The same stage, where it changes the *result* type rather than only
    /// the oid — so it is not a cosmetic difference in a catalog column.
    ///
    /// `date_trunc('day', ts)` on a `timestamp` column is
    /// `date_trunc(text, timestamp) -> timestamp` (oid 2020), not
    /// `date_trunc(text, timestamptz) -> timestamptz` (oid 1217): confirmed
    /// live, `pg_typeof(date_trunc('day', now()::timestamp))` is `timestamp
    /// without time zone`. `power('2', n)` on a `numeric` column is
    /// `power(numeric, numeric) -> numeric` (2169), not the `float8` row
    /// (1368) — `pg_typeof(power('2', 3::numeric))` is `numeric` live, and
    /// answering that in `float8` would round a numeric result through binary
    /// floating point.
    #[test]
    fn the_elected_overload_carries_its_own_return_type() {
        let dt = resolve("date_trunc", &[oid::UNKNOWN, oid::TIMESTAMP]).unwrap();
        assert_eq!(dt.oid, Oid(2020));
        assert_eq!(dt.ret, oid::TIMESTAMP);

        for (name, want) in [("pow", 1738u32), ("power", 2169)] {
            let left = resolve(name, &[oid::UNKNOWN, oid::NUMERIC]).unwrap();
            let right = resolve(name, &[oid::NUMERIC, oid::UNKNOWN]).unwrap();
            assert_eq!(left.oid, Oid(want), "{name}(unknown, numeric)");
            assert_eq!(right.oid, Oid(want), "{name}(numeric, unknown)");
            assert_eq!(left.ret, oid::NUMERIC);
        }

        // `age` picks the timestamp pair (2058) over the timestamptz pair
        // (1199) for the same reason, in either argument position: live,
        // `age('2020-01-01', ts)` deparses with the literal typed
        // `timestamp without time zone` and `ts` uncast.
        assert_eq!(
            resolve("age", &[oid::UNKNOWN, oid::TIMESTAMP]).unwrap().oid,
            Oid(2058)
        );
        assert_eq!(
            resolve("age", &[oid::TIMESTAMP, oid::UNKNOWN]).unwrap().oid,
            Oid(2058)
        );
    }

    /// The stage counts only arguments whose type is *known*, so an
    /// all-`unknown` call is a tie at zero and nothing moves — the
    /// first-in-table row still wins. This is load-bearing in both
    /// directions: it is what keeps `avg(int4)` on the `avg(int8)` row and
    /// the polymorphic monomorphizations on their `int4` rows, and it is
    /// also where this resolver is still *unlike* Postgres, which runs three
    /// further stages here (see [`resolve`]). `abs('5')` is the cheapest
    /// example: live it is `abs(float8)` (1395) by the preferred-type rule of
    /// stage 3, here it is `abs(int2)` (1398) because that row is first.
    /// Pinned so the divergence is visible rather than latent.
    #[test]
    fn an_all_unknown_call_ties_and_keeps_the_first_row() {
        assert_eq!(resolve("avg", &[oid::INT4]).unwrap().oid, Oid(2100));
        assert_eq!(resolve("abs", &[oid::UNKNOWN]).unwrap().oid, Oid(1398));
        assert_eq!(
            resolve("date_part", &[oid::UNKNOWN, oid::UNKNOWN])
                .unwrap()
                .oid,
            Oid(1171),
            "live this call is ERROR 42725 'function date_part(unknown, unknown) \
             is not unique'; stages 2-4 are unimplemented, so it still gets an answer"
        );
    }

    /// `count(any)` must keep resolving even though [`exact_match_count`]
    /// scores the `"any"` pseudo-type zero: the all-exact pass answers it
    /// before the counting stage is ever reached, and a candidate that is
    /// alone after implicit coercion wins its tie by default.
    #[test]
    fn the_any_pseudo_type_is_unaffected_by_the_counting_stage() {
        for arg in [oid::INT4, oid::TEXT, oid::DATE, oid::UNKNOWN] {
            assert_eq!(
                resolve("count", &[arg]).unwrap().oid,
                Oid(2147),
                "count({arg}) must stay on the any row"
            );
        }
    }
}
