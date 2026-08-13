//! `pg_catalog.pg_proc`, a view over `basin-pgtype`'s own function catalog.
//!
//! Like [`crate::pg_type`], [`crate::pg_operator`] and [`crate::pg_cast`],
//! this needs no [`CatalogSource`]: every builtin function Basin resolves
//! lives in `crates/basin-pgtype/src/func.rs`'s `FUNCS` table, already
//! checked against a live PostgreSQL 18 `pg_proc` (see that module's own docs
//! for the verifying query). This relation is that table, reshaped into
//! `pg_proc`'s column set.
//!
//! # Why this table, not a private map
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 puts `pg_operator`
//! and `pg_cast` at priority 1 for the same reason `func.rs`'s own module
//! docs give for itself: the owned planner must resolve `lower(x)`, `count(*)`,
//! `sum(x)` and friends by name and argument types against *some* table, and
//! `basin_pgtype::func::FUNCS` already is that table. `pg_proc` is the same
//! table under its real catalog name — this file costs nothing beyond itself
//! and delivers `\df` / driver introspection for free.
//!
//! # Column audit against live PostgreSQL 18.2 (see crate-level task docs)
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//! WHERE attrelid = 'pg_catalog.pg_proc'::regclass AND attnum > 0
//! ORDER BY attnum;
//! ```
//!
//! reports **30** columns. This relation previously implemented 7 of them
//! (`oid`, `proname`, `pronamespace`, `prorettype`, `pronargs`, `proargtypes`,
//! `prokind`), all in the wrong `attnum` position — `prokind` (real attnum
//! 10) was declared last, and `prorettype`/`pronargs`/`proargtypes` (19, 17,
//! 20) were declared before it. Fixed here, along with adding every other
//! column `basin_pgtype::func::FuncSig` can honestly support. Real `attnum`
//! order, with source and evidence:
//!
//! - `oid`, `proname`, `pronamespace` (1–3): `FuncSig::oid`/`name`, and
//!   [`crate::PG_CATALOG_NAMESPACE`] (every function here is a builtin,
//!   confirmed `pronamespace = 11` live for the whole sample below).
//!
//! The sample is **all 278 distinct oids `FUNCS` declares**. It was 135 until
//! the DataFusion-orphan rows landed (see `func.rs`'s docs and commit
//! `c9bd3a68`); the counts below are the current, re-measured ones, and two
//! columns that were uniform across the smaller sample — `proleakproof` and
//! `proparallel` — turned out not to be. That is the recurring lesson of this
//! file: "uniform across the rows we happen to have" is not "uniform".
//!
//! - `proowner` (4), `oid`, `NOT NULL`: not a `FuncSig` field, but confirmed
//!   live to be `10` (the bootstrap superuser) for every one of the 278 oids —
//!   `SELECT DISTINCT proowner FROM pg_proc WHERE oid IN (<all 278 FUNCS
//!   oids>)` returns exactly one row, `10`. Added as that literal.
//! - `prolang` (5), `oid`, `NOT NULL`: not a `FuncSig` field. Live-verified
//!   `12` (`internal`) for 266 of the 278 oids; the other 12 — `lpad`/`rpad`
//!   two-argument (879, 880), `date_part` (1384), `age` (1386, 2059),
//!   `log10(numeric)` (1481), `round` (1708), `trunc` (1710), `log` (1741),
//!   `bit_length` (1810, 1811), `substring(text, text, text)` (2074) — are
//!   real SQL-language wrappers (`prolang = 14`, `prosqlbody` non-`NULL`)
//!   around another builtin, a distinction `FuncSig` has no field for. Carried
//!   as an explicit exception set by [`prolang`].
//! - `procost` (6), `real`, `NOT NULL`: live-verified `1` for all 278 oids.
//!   Added as that literal.
//! - `prorows` (7), `real`, `NOT NULL`: live-verified `0` for every
//!   non-set-returning row and `1000` for `generate_series`'s eight oids —
//!   Postgres's own default row estimate for a set-returning function with
//!   no `ROWS` clause. `unnest` (2331) is the one exception in the sample
//!   (`100`, an explicit `ROWS` in its real definition), and [`prorows`]
//!   carries it as such. None of the orphan rows is set-returning.
//! - `provariadic` (8), `oid`, `NOT NULL`: live-verified `0` for 276 of 278
//!   oids; `concat` (3058) and `concat_ws` (3059) are real
//!   `VARIADIC "any"` functions (`provariadic = 2276`, the `"any"` pseudo-
//!   type) that `func.rs`'s own docs say are monomorphized to a fixed arity
//!   rather than modeled as variadic (see that module's docs on `concat`).
//!   Carried by [`provariadic`], and their real one-variadic-slot signature
//!   by [`SignatureOverride`]. The two other real `VARIADIC "any"` rows in
//!   this territory — the hypothetical-set aggregates `cume_dist`/
//!   `percent_rank` (3990, 3988) — are absent from `FUNCS` entirely; see
//!   `func.rs`'s docs on why an ordered-/hypothetical-set aggregate cannot be
//!   represented by [`FuncKind`].
//! - `prosupport` (9), `regproc`, `NOT NULL`: not a `FuncSig` field, and not
//!   uniform — live-verified non-zero (a real planner support function, e.g.
//!   `count`'s `int8inc_support`, `row_number`'s
//!   `window_row_number_support`, `starts_with`'s `text_starts_with_support`)
//!   for 20 of the 278 oids. `0` ("no support function") is Postgres's own
//!   default when a `CREATE FUNCTION` omits `SUPPORT`, and is right for the
//!   other 258 — represented as `oid` per this crate's `regproc`-as-`oid`
//!   convention (see [`crate::pg_cast`]'s `castfunc`). The 20 real oids are
//!   tabulated in [`PROSUPPORT`], which `catalog_fidelity` re-verifies against
//!   a live server every run.
//! - `prokind` (10): unchanged logic (see [`prokind`]), moved into its real
//!   position. 90 aggregates, 13 window functions, the rest `'f'`.
//! - `prosecdef` (11), `boolean`, `NOT NULL`: live-verified `false` for all
//!   278 oids (no builtin here runs with definer's rights). Added as that
//!   literal.
//! - `proleakproof` (12), `boolean`, `NOT NULL`: **not** uniform, though it
//!   was across the original 135. Live-verified `true` for 7 of the 278 —
//!   `md5` (2311, 2321), `sha224`/`sha256`/`sha384`/`sha512` (3419–3422) and
//!   `starts_with` (3696), all of which leak nothing about their arguments
//!   through an error or a timing channel, so the planner may push them below
//!   a security barrier. Carried by [`proleakproof`] with the seven in
//!   [`LEAKPROOF_OIDS`]. Reporting `false` for these would make a
//!   security-barrier view refuse a pushdown real Postgres permits.
//! - `proisstrict` (13), `boolean`, `NOT NULL`: not a `FuncSig` field, but
//!   fully determined once the exceptions are enumerated rather than
//!   guessed. Live-verified across all 278 oids: `false` for every aggregate
//!   (90/90), and `true` for every other row except eighteen —
//!   `concat`/`concat_ws` (which return `''`, not `NULL`, for a `NULL`
//!   argument), the niladic window functions `row_number`/`rank`/
//!   `dense_rank`/`percent_rank`/`cume_dist`, and the array/string functions
//!   for which a `NULL` argument is *meaningful* rather than absorbing
//!   (`array_append`/`array_prepend`/`array_cat`/`array_remove`/
//!   `array_replace`/`array_position`/`array_positions`, and
//!   `string_to_array`/`array_to_string`'s null-string forms).
//!   [`proisstrict`] implements exactly that, with the eighteen in
//!   [`NOT_STRICT_OIDS`]. An earlier version of this file omitted the column
//!   on the grounds that window functions "split roughly evenly with no rule
//!   `FuncSig` can derive"; the split is real, but naming the exceptions is a
//!   rule, and `catalog_fidelity` checks it every run.
//! - `proretset` (14), `boolean`, `NOT NULL`: real data, not a placeholder —
//!   `f.kind == FuncKind::SetReturning`, exactly what that variant's own doc
//!   comment says it exists to carry (see `func.rs`). 9 of 278.
//! - `provolatile` (15), `"char"`, `NOT NULL`: not a `FuncSig` field.
//!   Live-verified `'i'` (immutable) for 264 of 278 oids. Ten are `'s'`
//!   (stable): `array_to_string` (384, 395 — they call the element type's
//!   output function, which can depend on session settings),
//!   `generate_series` on `timestamptz` (939), `date_part` (1171),
//!   `date_trunc` (1217), `now` (1299), `age` (1386, 2059),
//!   `concat`/`concat_ws` (3058, 3059). Four are `'v'` (volatile): every
//!   `random` overload (1598, 6339, 6340, 6341). Carried as two explicit
//!   exception sets by [`provolatile`].
//! - `proparallel` (16), `"char"`, `NOT NULL`: also not uniform any more.
//!   Live-verified `'s'` (parallel-safe) for 274 of 278; the four `random`
//!   overloads are `'r'` (parallel-*restricted*), because they advance a
//!   per-backend PRNG state a parallel worker cannot share. Carried by
//!   [`proparallel`]. `'r'` and `'v'` are different claims about the same
//!   function and are stored in different columns — `now()` is `'s'`/stable,
//!   `random()` is `'r'`/volatile — so neither may be derived from the other.
//! - `pronargdefaults` (18), `smallint`, `NOT NULL`: not a `FuncSig` field
//!   (it has no notion of a default-valued argument), but live-verified `0`
//!   for all 278 oids — real, not a guess, for every row this crate can
//!   currently construct. Added as that literal.
//! - `pronargs` (17), `prorettype` (19), `proargtypes` (20): from
//!   `FuncSig::args`/`ret`, **except** for the twenty-eight oids whose `FUNCS`
//!   rows are monomorphizations of a genuinely polymorphic or variadic real
//!   function, which report the real row's signature — see
//!   [`SignatureOverride`], and the note below on how that bug was found.
//!   `proargtypes` is `oidvector`, distinct from `proallargtypes`'s `oid[]`
//!   at 21 — see "how vector/array columns are represented" below.
//! - `proallargtypes` (21, `oid[]`), `proargmodes` (22, `"char"[]`),
//!   `proargnames` (23, `text[]`) — all nullable, and `NULL` for the
//!   overwhelming majority of the sample (`FuncSig` has no field for any of
//!   the three). The exceptions are real and enumerable:
//!   `concat`/`concat_ws` (3058, 3059) have non-`NULL`
//!   `proallargtypes`/`proargmodes` (their variadic slot, per `provariadic`
//!   above — carried by [`SignatureOverride`]), and 19 oids have non-`NULL`
//!   `proargnames` — `string_agg`, `make_date`, the `regexp_*` family,
//!   `random(min, max)` and `array_sort`'s two- and three-argument forms,
//!   carried by [`PROARGNAMES`].
//! - `proargdefaults` (24, `pg_node_tree`), `protrftypes` (25, `oid[]`) —
//!   both nullable; live-verified `NULL` for **all** 278 oids (no default-
//!   valued argument and no `TRANSFORM` in this sample). Added as
//!   always-`NULL`, correct for every row.
//! - `prosrc` (26), `text`, `NOT NULL`: from [`PROSRC`]. `FuncSig::name` is
//!   the SQL-visible `proname`, not the internal symbol `prosrc` names, and
//!   the two differ for 250 of the 278 rows — `count`'s is
//!   `aggregate_dummy`, `row_number`'s is `window_row_number`,
//!   `generate_series`'s is `generate_series_int4`, `cardinality`'s is
//!   `array_cardinality`. An earlier version omitted the column on the
//!   grounds that the only alternative to `FuncSig::name` was "a fabricated
//!   per-row string". There is a third option, which is what every other
//!   transcribed column in this crate does: read the real values off a live
//!   server, tabulate them, and let `catalog_fidelity`'s row oracle re-check
//!   every one on every run. The twelve SQL-language wrappers named under
//!   `prolang` report the **empty string** here, which is the real value
//!   PostgreSQL 14+ stores for a function whose body lives in `prosqlbody` —
//!   not a placeholder.
//! - `probin` (27), `text`, nullable: live-verified `NULL` for all 278 oids
//!   (every `FUNCS` function is `internal`- or SQL-language, never
//!   `C`-language). Added as always-`NULL`, correct for every row.
//! - `prosqlbody` (28), `pg_node_tree`, nullable: **not added.**
//!   Live-verified non-`NULL` for exactly the 12 SQL-language wrapper oids
//!   named under `prolang` above, `NULL` for the other 266. Each of those
//!   values is a ~1.4 KB serialized `Query` node tree in PostgreSQL 18's
//!   internal `nodeToString` format — a structure Basin cannot produce, has
//!   no reader for, and which changes shape between major Postgres releases.
//!   Transcribing them would be exactly the fabrication
//!   [`crate::pg_attrdef`] refuses for `adbin`, and an always-`NULL` column
//!   is a documented wrong answer for 12 of 278 rows that nothing detected
//!   until `catalog_fidelity`'s row oracle did. Omitted rather than either.
//! - `proconfig` (29, `text[]`), `proacl` (30, `aclitem[]`) — both nullable;
//!   live-verified `NULL` for all 278 oids (no `SET` clause, and default
//!   privileges throughout — nothing in this sample has had `GRANT`/`REVOKE`
//!   applied). Added as always-`NULL`, correct for every row.
//!
//! ## Positional consequence of the one omission
//!
//! `prosqlbody` (real attnum 28) is the only column this relation does not
//! implement at all. Attnums 29–30 (`proconfig`, `proacl`) therefore sit one
//! slot earlier here than in a real `pg_proc`. Everything at or below attnum
//! 27 is in its real position. A caller reading by name is unaffected; a
//! caller reading by position past attnum 27 is not looking at what it
//! thinks it is.
//!
//! ## How the signature bug was found
//!
//! `catalog_fidelity`'s `diff_static_rows` — the row-content oracle this
//! relation had no equivalent of until it was written — diffs all 28 columns
//! of all 135 rows against a live server on every run (278 rows today). On
//! its first run it
//! reported 49 disagreements: the 14 real `prosupport` oids, `unnest`'s
//! `prorows`, `string_agg`'s `proargnames`, the two variadic rows'
//! arity/modes, the 6 `prosqlbody` bodies, and — the one that mattered most
//! — 17 cells across nine oids where this relation was reporting a
//! *monomorphized* signature (`lag(integer) -> integer`) for a real row that
//! is polymorphic (`lag(anyelement) -> anyelement`). Everything but
//! `prosqlbody` is fixed above; nothing on that list was visible to the
//! by-hand audit that wrote the original column set. It is now 29 columns
//! of all rows the oracle checks, `prosrc` included.
//!
//! ## How `oidvector`/`oid[]`/`"char"[]`/`text[]`/`pg_node_tree` columns are represented
//!
//! `basin-pgtype`'s `oid` module has `OIDVECTOR` (30), `INT2VECTOR` (22),
//! `PG_NODE_TREE` (194) and `ANYARRAY` (2277) constants, but its `physical()`
//! mapping (`lib.rs`) has no match arm for any of them — the same gap
//! [`crate::pg_index`]'s module docs note for `indkey`. This relation
//! sidesteps it the same way: `proargtypes`/`proallargtypes`/`protrftypes`
//! are a hand-built Arrow `List<UInt32>` (values in argument order for the
//! first, real Postgres `oid` array semantics for the other two — `oidvector`
//! and `oid[]` are different real types that happen to share a physical
//! representation here), `proargmodes`/`proargnames`/`proconfig`/`proacl`
//! are `List<Utf8>`, and `proargdefaults`/`prosqlbody` (both `pg_node_tree`,
//! a text-encoded serialized expression tree per `oid.rs`'s own docs) are
//! plain nullable `Utf8`. `provolatile`/`proparallel`/`prokind` are
//! Postgres's single-byte `"char"` type, distinct from both `text` and
//! `bpchar` — represented as single-character `Utf8`, matching
//! [`crate::pg_class`]'s `relkind` precedent, not as a numeric byte.
//!
//! # `oid` is deduplicated to match a real primary key
//!
//! `pg_proc.oid` is a primary key in real Postgres, but `FUNCS` is not one
//! row per oid — `func.rs`'s own module docs explain that several
//! polymorphic functions (`array_agg`, `unnest`, `lag`/`lead`/`first_value`/
//! `last_value`/`nth_value`) are monomorphized into multiple rows sharing one
//! real oid, the same way [`crate::pg_operator`] handles the polymorphic
//! array operators. This relation keeps only the first `FUNCS` row for each
//! oid and drops the rest, for the same reason: reporting two rows under one
//! primary key would violate the real table's own constraint and break "a
//! predicate on oid returns exactly one row". `basin_pgtype::func::resolve`
//! is unaffected, since it consults the full `FUNCS` table directly.
//!
//! # Row coverage is `FUNCS`'s own scope, not all of real `pg_proc`
//!
//! Postgres ships well over a thousand builtin functions; `FUNCS` covers
//! string, math, date/time, array, regex, hashing, aggregate, window and
//! set-returning functions in the categories needed to unblock planner
//! resolution, and the real `pg_proc` rows it explicitly does not include
//! (`age(xid)`, `avg(int2)`/`avg(int4)`'s own dedicated oids, everything
//! typed `bit`, and the ordered-/hypothetical-set aggregates) are simply
//! absent from this relation too — not covered wrongly, and in the last two
//! cases not coverable in `FuncSig`'s current shape. See `func.rs`'s own
//! module docs for the full scope statement and the reasons.

use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    builder::{ListBuilder, StringBuilder, UInt32Builder},
    BooleanArray, Float32Array, Int16Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::{
    func::{FuncKind, FuncSig, FUNCS},
    Oid,
};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// `pg_proc.prokind` for this function — `'a'`/`'w'` for aggregate/window,
/// `'f'` for both scalar and set-returning (real Postgres collapses the
/// latter two as well — see the module docs).
fn prokind(kind: FuncKind) -> char {
    match kind {
        FuncKind::Aggregate => 'a',
        FuncKind::Window => 'w',
        FuncKind::Scalar | FuncKind::SetReturning => 'f',
    }
}

/// The 12 real SQL-language wrapper oids in `FUNCS` — `lpad`/`rpad`'s
/// two-argument forms, `date_part`, `age` (both overloads), `log10(numeric)`,
/// `bit_length` (both overloads), `round`, `trunc`, `log`,
/// `substring(text, text, text)` — confirmed live to have `prolang = 14` and a
/// non-`NULL` `prosqlbody`, unlike every other row's `prolang = 12`
/// (`internal`). See the module docs.
const SQL_LANGUAGE_WRAPPER_OIDS: [u32; 12] = [
    879, 880, 1384, 1386, 1481, 1708, 1710, 1741, 1810, 1811, 2059, 2074,
];

/// `pg_proc.prolang` for this function — see [`SQL_LANGUAGE_WRAPPER_OIDS`].
fn prolang(f: &FuncSig) -> Oid {
    if SQL_LANGUAGE_WRAPPER_OIDS.contains(&f.oid.get()) {
        Oid(14)
    } else {
        Oid(12)
    }
}

/// The 10 real `provolatile = 's'` (stable) oids in `FUNCS`. `array_to_string`
/// (384, 395) joins the original 8 because it calls the element type's output
/// function, which may depend on session settings. See the module docs.
const STABLE_VOLATILITY_OIDS: [u32; 10] = [384, 395, 939, 1171, 1217, 1299, 1386, 2059, 3058, 3059];

/// The 4 real `provolatile = 'v'` (volatile) oids in `FUNCS` — every `random`
/// overload, and nothing else. Confirmed live: `random`'s four oids are the
/// only rows in this table that are not `'i'` or `'s'`.
const VOLATILE_OIDS: [u32; 4] = [1598, 6339, 6340, 6341];

/// `pg_proc.provolatile` for this function — see [`STABLE_VOLATILITY_OIDS`]
/// and [`VOLATILE_OIDS`]. Every other row is `'i'` (immutable).
fn provolatile(f: &FuncSig) -> char {
    if VOLATILE_OIDS.contains(&f.oid.get()) {
        'v'
    } else if STABLE_VOLATILITY_OIDS.contains(&f.oid.get()) {
        's'
    } else {
        'i'
    }
}

/// The 4 real `proparallel = 'r'` (parallel-restricted) oids in `FUNCS` —
/// the `random` overloads again, for a different reason: they advance a
/// per-backend PRNG state a parallel worker cannot share. Every other row is
/// `'s'` (parallel-safe), live-verified.
const RESTRICTED_PARALLEL_OIDS: [u32; 4] = [1598, 6339, 6340, 6341];

/// `pg_proc.proparallel` for this function — see
/// [`RESTRICTED_PARALLEL_OIDS`].
fn proparallel(f: &FuncSig) -> char {
    if RESTRICTED_PARALLEL_OIDS.contains(&f.oid.get()) {
        'r'
    } else {
        's'
    }
}

/// The 7 real `proleakproof = true` oids in `FUNCS` — the hash functions
/// (`md5`'s two overloads, `sha224`/`sha256`/`sha384`/`sha512`) and
/// `starts_with`. Live-verified as the only rows here that leak nothing about
/// their arguments through an error message or a timing channel, so the
/// planner may push them below a security barrier. Every other row is
/// `false`.
const LEAKPROOF_OIDS: [u32; 7] = [2311, 2321, 3419, 3420, 3421, 3422, 3696];

/// `pg_proc.proleakproof` for this function — see [`LEAKPROOF_OIDS`].
fn proleakproof(f: &FuncSig) -> bool {
    LEAKPROOF_OIDS.contains(&f.oid.get())
}

/// `concat` (3058) and `concat_ws` (3059) — the only real `VARIADIC "any"`
/// functions in `FUNCS`; see the module docs.
const VARIADIC_ANY_OIDS: [u32; 2] = [3058, 3059];

/// `pg_proc.provariadic` for this function — `"any"`'s oid (2276) for the
/// two real variadic functions `FUNCS` monomorphizes to fixed arity, `0`
/// (not variadic) otherwise. See the module docs.
fn provariadic(f: &FuncSig) -> Oid {
    if VARIADIC_ANY_OIDS.contains(&f.oid.get()) {
        Oid(2276)
    } else {
        Oid::INVALID
    }
}

/// The 14 real planner *support functions* among the oids `FUNCS` covers —
/// see the module docs on `prosupport`. Every other row's `prosupport` is
/// `0` (no support function), which is Postgres's own default when a
/// `CREATE FUNCTION` omits `SUPPORT`.
///
/// These are real, fixed oids assigned by Postgres's catalog bootstrap, the
/// same kind of value [`crate::pg_am`]'s `amhandler` carries, and every one
/// is re-checked against a live server by `catalog_fidelity`'s
/// `diff_static_rows` on each run.
const PROSUPPORT: &[(u32, u32)] = &[
    (378, 6378),  // array_append(anycompatiblearray,…) -> array_append_support
    (379, 6379),  // array_prepend(anycompatible,…)  -> array_prepend_support
    (938, 6354),  // generate_series(timestamp,...)  -> generate_series_timestamp_support
    (939, 6354),  // generate_series(timestamptz,...)-> generate_series_timestamp_support
    (1066, 3994), // generate_series(int4,int4,int4) -> generate_series_int4_support
    (1067, 3994), // generate_series(int4,int4)      -> generate_series_int4_support
    (1068, 3995), // generate_series(int8,int8,int8) -> generate_series_int8_support
    (1069, 3995), // generate_series(int8,int8)      -> generate_series_int8_support
    (2147, 6236), // count(any)                      -> int8inc_support
    (2331, 3996), // unnest(anyarray)                -> array_unnest_support
    (2803, 6236), // count(*)                        -> int8inc_support
    (3100, 6233), // row_number()                    -> window_row_number_support
    (3101, 6234), // rank()                          -> window_rank_support
    (3102, 6235), // dense_rank()                    -> window_dense_rank_support
    (3103, 6306), // percent_rank()                  -> window_percent_rank_support
    (3104, 6307), // cume_dist()                     -> window_cume_dist_support
    (3105, 6308), // ntile(integer)                  -> window_ntile_support
    (3259, 6357), // generate_series(numeric,...)    -> generate_series_numeric_support
    (3260, 6357), // generate_series(numeric,...)    -> generate_series_numeric_support
    (3696, 6242), // starts_with(text, text)         -> text_starts_with_support
];

/// `pg_proc.prosupport` for this function — see [`PROSUPPORT`].
fn prosupport(f: &FuncSig) -> Oid {
    PROSUPPORT
        .iter()
        .find(|(oid, _)| *oid == f.oid.get())
        .map(|(_, support)| Oid(*support))
        .unwrap_or(Oid::INVALID)
}

/// The eighteen oids among `FUNCS`'s non-aggregates that are **not** `STRICT`
/// — see the module docs on `proisstrict`. Every aggregate is non-strict and
/// every other row is strict, live-verified across all 278 oids.
const NOT_STRICT_OIDS: [u32; 18] = [
    376,  // string_to_array(text, text, text) — a NULL null-string is meaningful
    378,  // array_append   — appending NULL to an array is not NULL
    379,  // array_prepend  — likewise
    383,  // array_cat      — cat(NULL, a) is a, not NULL
    384,  // array_to_string(anyarray, text, text) — NULL null-string is meaningful
    394,  // string_to_array(text, text) — a NULL delimiter splits into characters
    3058, // concat      — VARIADIC "any", returns '' rather than NULL
    3059, // concat_ws   — likewise
    3100, // row_number()
    3101, // rank()
    3102, // dense_rank()
    3103, // percent_rank()
    3104, // cume_dist()
    3167, // array_remove   — removing NULL is a real operation
    3168, // array_replace  — replacing with/of NULL likewise
    3277, // array_position — searching for a NULL element returns its index
    3278, // array_position(…, start)
    3279, // array_positions
];

/// `pg_proc.proisstrict` for this function — see [`NOT_STRICT_OIDS`].
fn proisstrict(f: &FuncSig) -> bool {
    f.kind != FuncKind::Aggregate && !NOT_STRICT_OIDS.contains(&f.oid.get())
}

/// The real argument signature of a `FUNCS` oid whose row in that table is a
/// *monomorphization* of a genuinely polymorphic or variadic Postgres
/// function.
///
/// `func.rs` deliberately expands `anyelement`/`anyarray`/`anynonarray`
/// signatures into one row per concrete element type, and expands
/// `VARIADIC "any"` into a fixed arity, because that is what
/// [`basin_pgtype::func::resolve`] needs to match a call site. Those
/// expansions all share the one real `pg_proc.oid`, so this relation — which
/// reports one row per oid — must report the *real* row's signature, not
/// whichever monomorphization happened to be first in `FUNCS`.
///
/// Reporting the monomorphization is not a harmless approximation: it tells a
/// client that `pg_proc` oid 3106 is `lag(integer) -> integer` when the real
/// row is `lag(anyelement) -> anyelement`, so a driver resolving a prepared
/// statement's parameter types off this relation would refuse every non-`int4`
/// `lag`. Every entry below was found by `catalog_fidelity`'s
/// `diff_static_rows` and is re-verified against a live server on every run.
struct SignatureOverride {
    oid: u32,
    /// The real `prorettype`, where the monomorphization replaced a
    /// polymorphic pseudo-type with a concrete one.
    prorettype: Option<u32>,
    /// The real `proargtypes`. `pronargs` follows from its length, which is
    /// how the two `VARIADIC "any"` rows below get their real arity back (a
    /// real variadic function declares *one* variadic slot, however many
    /// arguments a call passes into it).
    proargtypes: &'static [u32],
    /// `Some` for a real `VARIADIC` function, whose `proargmodes` (`'v'` for
    /// the variadic slot, `'i'` for the rest) and `proallargtypes` are
    /// non-`NULL`; `None` for every other row.
    proargmodes: Option<&'static [char]>,
}

static SIGNATURE_OVERRIDES: &[SignatureOverride] = &[
    // `VARIADIC "any"` — see `provariadic` above and `func.rs`'s docs on why
    // `FUNCS` monomorphizes these to a fixed arity.
    SignatureOverride {
        oid: 3058, // concat(VARIADIC "any")
        prorettype: None,
        proargtypes: &[2276],
        proargmodes: Some(&['v']),
    },
    SignatureOverride {
        oid: 3059, // concat_ws(text, VARIADIC "any")
        prorettype: None,
        proargtypes: &[25, 2276],
        proargmodes: Some(&['i', 'v']),
    },
    // Polymorphic. 2283 = `anyelement`, 2277 = `anyarray`,
    // 2776 = `anynonarray`.
    SignatureOverride {
        oid: 2331, // unnest(anyarray) -> SETOF anyelement
        prorettype: Some(2283),
        proargtypes: &[2277],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 2335, // array_agg(anynonarray) -> anyarray
        prorettype: Some(2277),
        proargtypes: &[2776],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3106, // lag(anyelement)
        prorettype: Some(2283),
        proargtypes: &[2283],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3107, // lag(anyelement, integer)
        prorettype: Some(2283),
        proargtypes: &[2283, 23],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3109, // lead(anyelement)
        prorettype: Some(2283),
        proargtypes: &[2283],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3110, // lead(anyelement, integer)
        prorettype: Some(2283),
        proargtypes: &[2283, 23],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3112, // first_value(anyelement)
        prorettype: Some(2283),
        proargtypes: &[2283],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3113, // last_value(anyelement)
        prorettype: Some(2283),
        proargtypes: &[2283],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3114, // nth_value(anyelement, integer)
        prorettype: Some(2283),
        proargtypes: &[2283, 23],
        proargmodes: None,
    },
    // The array family, monomorphized in `FUNCS` at `int4`/`text` the same
    // way `array_agg`/`unnest` are (see `func.rs`'s docs). 5077 =
    // `anycompatible`, 5078 = `anycompatiblearray` — a *different* pseudo-type
    // family from `anyelement`/`anyarray` (2283/2277), which Postgres uses for
    // the older rows above and which resolve independently per argument. They
    // are transcribed as the live server has them, not collapsed onto one
    // another.
    //
    // `prorettype: None` where the real result type is already concrete:
    // `array_length`/`array_ndims`/`cardinality`/`array_position` return
    // `integer`, `array_positions` returns `integer[]` and `array_to_string`
    // returns `text` no matter what they are given, so only their *arguments*
    // are polymorphic.
    SignatureOverride {
        oid: 378, // array_append(anycompatiblearray, anycompatible)
        prorettype: Some(5078),
        proargtypes: &[5078, 5077],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 379, // array_prepend(anycompatible, anycompatiblearray)
        prorettype: Some(5078),
        proargtypes: &[5077, 5078],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 383, // array_cat(anycompatiblearray, anycompatiblearray)
        prorettype: Some(5078),
        proargtypes: &[5078, 5078],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 384, // array_to_string(anyarray, text, text) -> text
        prorettype: None,
        proargtypes: &[2277, 25, 25],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 395, // array_to_string(anyarray, text) -> text
        prorettype: None,
        proargtypes: &[2277, 25],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 748, // array_ndims(anyarray) -> integer
        prorettype: None,
        proargtypes: &[2277],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 2176, // array_length(anyarray, integer) -> integer
        prorettype: None,
        proargtypes: &[2277, 23],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3167, // array_remove(anycompatiblearray, anycompatible)
        prorettype: Some(5078),
        proargtypes: &[5078, 5077],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3168, // array_replace(anycompatiblearray, anycompatible, anycompatible)
        prorettype: Some(5078),
        proargtypes: &[5078, 5077, 5077],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3179, // cardinality(anyarray) -> integer
        prorettype: None,
        proargtypes: &[2277],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3277, // array_position(anycompatiblearray, anycompatible) -> integer
        prorettype: None,
        proargtypes: &[5078, 5077],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3278, // array_position(anycompatiblearray, anycompatible, integer) -> integer
        prorettype: None,
        proargtypes: &[5078, 5077, 23],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 3279, // array_positions(anycompatiblearray, anycompatible) -> integer[]
        prorettype: None,
        proargtypes: &[5078, 5077],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 6381, // array_reverse(anyarray) -> anyarray
        prorettype: Some(2277),
        proargtypes: &[2277],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 6388, // array_sort(anyarray) -> anyarray
        prorettype: Some(2277),
        proargtypes: &[2277],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 6389, // array_sort(anyarray, boolean) -> anyarray
        prorettype: Some(2277),
        proargtypes: &[2277, 16],
        proargmodes: None,
    },
    SignatureOverride {
        oid: 6390, // array_sort(anyarray, boolean, boolean) -> anyarray
        prorettype: Some(2277),
        proargtypes: &[2277, 16, 16],
        proargmodes: None,
    },
];

/// The [`SignatureOverride`] for this function, if it has one.
fn signature_override(f: &FuncSig) -> Option<&'static SignatureOverride> {
    SIGNATURE_OVERRIDES.iter().find(|s| s.oid == f.oid.get())
}

/// `pg_proc.prorettype` for this function — the real row's, which differs
/// from [`FuncSig::ret`] for the polymorphic rows. See
/// [`SignatureOverride`].
fn prorettype(f: &FuncSig) -> Oid {
    match signature_override(f).and_then(|s| s.prorettype) {
        Some(ret) => Oid(ret),
        None => f.ret,
    }
}

/// `pg_proc.proargtypes` for this function — the real row's. See
/// [`SignatureOverride`].
fn proargtypes(f: &FuncSig) -> Vec<u32> {
    match signature_override(f) {
        Some(s) => s.proargtypes.to_vec(),
        None => f.args.iter().map(|a| a.get()).collect(),
    }
}

/// `pg_proc.prosrc` for every oid `FUNCS` covers — the internal symbol the
/// function's implementation is looked up by, which is *not* the same as
/// `proname` for 250 of the 278 rows.
///
/// Real, fixed values assigned by PostgreSQL's catalog bootstrap, the same
/// class of transcribed data as [`crate::pg_am`]'s `amhandler` and
/// [`crate::pg_operator`]'s `oprcode`, and re-verified against a live server
/// by `catalog_fidelity`'s `diff_static_rows` on every run. Basin implements
/// none of these C functions; the value describes the *function*, not Basin's
/// implementation of it, exactly as `typinput` does in [`crate::pg_type`].
///
/// A comment names `proname` where it differs from `prosrc`. The empty
/// strings are the six SQL-language wrappers (see `prolang`), for which
/// PostgreSQL 14+ genuinely stores an empty `prosrc`.
const PROSRC: &[(u32, &str)] = &[
    (376, "text_to_array_null"), // string_to_array
    (378, "array_append"),
    (379, "array_prepend"),
    (383, "array_cat"),
    (384, "array_to_text_null"), // array_to_string
    (394, "text_to_array"),      // string_to_array
    (395, "array_to_text"),      // array_to_string
    (720, "byteaoctetlen"),      // octet_length
    (748, "array_ndims"),
    (749, "byteaoverlay"),        // overlay
    (752, "byteaoverlay_no_len"), // overlay
    (849, "textpos"),             // position
    (868, "textpos"),             // strpos
    (870, "lower"),
    (871, "upper"),
    (872, "initcap"),
    (873, "lpad"),
    (874, "rpad"),
    (875, "ltrim"),
    (876, "rtrim"),
    (877, "text_substr"),        // substr
    (879, ""),                   // lpad
    (880, ""),                   // rpad
    (881, "ltrim1"),             // ltrim
    (882, "rtrim1"),             // rtrim
    (883, "text_substr_no_len"), // substr
    (884, "btrim"),
    (885, "btrim1"),                      // btrim
    (936, "text_substr"),                 // substring
    (937, "text_substr_no_len"),          // substring
    (938, "generate_series_timestamp"),   // generate_series
    (939, "generate_series_timestamptz"), // generate_series
    (940, "int2mod"),                     // mod
    (941, "int4mod"),                     // mod
    (947, "int8mod"),                     // mod
    (1066, "generate_series_step_int4"),  // generate_series
    (1067, "generate_series_int4"),       // generate_series
    (1068, "generate_series_step_int8"),  // generate_series
    (1069, "generate_series_int8"),       // generate_series
    (1171, "timestamptz_part"),           // date_part
    (1172, "interval_part"),              // date_part
    (1194, "dlog10"),                     // log10
    (1199, "timestamptz_age"),            // age
    (1217, "timestamptz_trunc"),          // date_trunc
    (1218, "interval_trunc"),             // date_trunc
    (1273, "timetz_part"),                // date_part
    (1284, "timestamptz_trunc_zone"),     // date_trunc
    (1299, "now"),
    (1317, "textlen"),            // length
    (1340, "dlog10"),             // log
    (1341, "dlog1"),              // ln
    (1342, "dround"),             // round
    (1343, "dtrunc"),             // trunc
    (1344, "dsqrt"),              // sqrt
    (1345, "dcbrt"),              // cbrt
    (1346, "dpow"),               // pow
    (1347, "dexp"),               // exp
    (1367, "bpcharlen"),          // character_length
    (1368, "dpow"),               // power
    (1369, "textlen"),            // character_length
    (1372, "bpcharlen"),          // char_length
    (1374, "textoctetlen"),       // octet_length
    (1375, "bpcharoctetlen"),     // octet_length
    (1376, "numeric_fac"),        // factorial
    (1381, "textlen"),            // char_length
    (1384, ""),                   // date_part
    (1385, "time_part"),          // date_part
    (1386, ""),                   // age
    (1394, "float4abs"),          // abs
    (1395, "float8abs"),          // abs
    (1396, "int8abs"),            // abs
    (1397, "int4abs"),            // abs
    (1398, "int2abs"),            // abs
    (1404, "textoverlay"),        // overlay
    (1405, "textoverlay_no_len"), // overlay
    (1481, ""),                   // log10
    (1598, "drandom"),            // random
    (1600, "dasin"),              // asin
    (1601, "dacos"),              // acos
    (1602, "datan"),              // atan
    (1603, "datan2"),             // atan2
    (1604, "dsin"),               // sin
    (1605, "dcos"),               // cos
    (1606, "dtan"),               // tan
    (1607, "dcot"),               // cot
    (1608, "degrees"),
    (1609, "radians"),
    (1610, "dpi"), // pi
    (1622, "repeat"),
    (1705, "numeric_abs"),         // abs
    (1706, "numeric_sign"),        // sign
    (1707, "numeric_round"),       // round
    (1708, ""),                    // round
    (1709, "numeric_trunc"),       // trunc
    (1710, ""),                    // trunc
    (1711, "numeric_ceil"),        // ceil
    (1712, "numeric_floor"),       // floor
    (1728, "numeric_mod"),         // mod
    (1730, "numeric_sqrt"),        // sqrt
    (1732, "numeric_exp"),         // exp
    (1734, "numeric_ln"),          // ln
    (1736, "numeric_log"),         // log
    (1738, "numeric_power"),       // pow
    (1741, ""),                    // log
    (1810, ""),                    // bit_length
    (1811, ""),                    // bit_length
    (2012, "bytea_substr"),        // substring
    (2013, "bytea_substr_no_len"), // substring
    (2014, "byteapos"),            // position
    (2020, "timestamp_trunc"),     // date_trunc
    (2021, "timestamp_part"),      // date_part
    (2058, "timestamp_age"),       // age
    (2059, ""),                    // age
    (2073, "textregexsubstr"),     // substring
    (2074, ""),                    // substring
    (2087, "replace_text"),        // replace
    (2088, "split_part"),
    (2089, "to_hex32"),        // to_hex
    (2090, "to_hex64"),        // to_hex
    (2100, "aggregate_dummy"), // avg
    (2103, "aggregate_dummy"), // avg
    (2104, "aggregate_dummy"), // avg
    (2105, "aggregate_dummy"), // avg
    (2106, "aggregate_dummy"), // avg
    (2107, "aggregate_dummy"), // sum
    (2108, "aggregate_dummy"), // sum
    (2109, "aggregate_dummy"), // sum
    (2110, "aggregate_dummy"), // sum
    (2111, "aggregate_dummy"), // sum
    (2113, "aggregate_dummy"), // sum
    (2114, "aggregate_dummy"), // sum
    (2115, "aggregate_dummy"), // max
    (2116, "aggregate_dummy"), // max
    (2117, "aggregate_dummy"), // max
    (2119, "aggregate_dummy"), // max
    (2120, "aggregate_dummy"), // max
    (2122, "aggregate_dummy"), // max
    (2126, "aggregate_dummy"), // max
    (2127, "aggregate_dummy"), // max
    (2129, "aggregate_dummy"), // max
    (2130, "aggregate_dummy"), // max
    (2131, "aggregate_dummy"), // min
    (2132, "aggregate_dummy"), // min
    (2133, "aggregate_dummy"), // min
    (2135, "aggregate_dummy"), // min
    (2136, "aggregate_dummy"), // min
    (2138, "aggregate_dummy"), // min
    (2142, "aggregate_dummy"), // min
    (2143, "aggregate_dummy"), // min
    (2145, "aggregate_dummy"), // min
    (2146, "aggregate_dummy"), // min
    (2147, "aggregate_dummy"), // count
    (2154, "aggregate_dummy"), // stddev
    (2155, "aggregate_dummy"), // stddev
    (2156, "aggregate_dummy"), // stddev
    (2157, "aggregate_dummy"), // stddev
    (2158, "aggregate_dummy"), // stddev
    (2159, "aggregate_dummy"), // stddev
    (2167, "numeric_ceil"),    // ceiling
    (2169, "numeric_power"),   // power
    (2176, "array_length"),
    (2236, "aggregate_dummy"),         // bit_and
    (2237, "aggregate_dummy"),         // bit_or
    (2238, "aggregate_dummy"),         // bit_and
    (2239, "aggregate_dummy"),         // bit_or
    (2240, "aggregate_dummy"),         // bit_and
    (2241, "aggregate_dummy"),         // bit_or
    (2308, "dceil"),                   // ceil
    (2309, "dfloor"),                  // floor
    (2310, "dsign"),                   // sign
    (2311, "md5_text"),                // md5
    (2320, "dceil"),                   // ceiling
    (2321, "md5_bytea"),               // md5
    (2331, "array_unnest"),            // unnest
    (2335, "aggregate_dummy"),         // array_agg
    (2462, "dsinh"),                   // sinh
    (2463, "dcosh"),                   // cosh
    (2464, "dtanh"),                   // tanh
    (2465, "dasinh"),                  // asinh
    (2466, "dacosh"),                  // acosh
    (2467, "datanh"),                  // atanh
    (2517, "aggregate_dummy"),         // bool_and
    (2518, "aggregate_dummy"),         // bool_or
    (2641, "aggregate_dummy"),         // var_samp
    (2642, "aggregate_dummy"),         // var_samp
    (2643, "aggregate_dummy"),         // var_samp
    (2644, "aggregate_dummy"),         // var_samp
    (2645, "aggregate_dummy"),         // var_samp
    (2646, "aggregate_dummy"),         // var_samp
    (2712, "aggregate_dummy"),         // stddev_samp
    (2713, "aggregate_dummy"),         // stddev_samp
    (2714, "aggregate_dummy"),         // stddev_samp
    (2715, "aggregate_dummy"),         // stddev_samp
    (2716, "aggregate_dummy"),         // stddev_samp
    (2717, "aggregate_dummy"),         // stddev_samp
    (2718, "aggregate_dummy"),         // var_pop
    (2719, "aggregate_dummy"),         // var_pop
    (2720, "aggregate_dummy"),         // var_pop
    (2721, "aggregate_dummy"),         // var_pop
    (2722, "aggregate_dummy"),         // var_pop
    (2723, "aggregate_dummy"),         // var_pop
    (2724, "aggregate_dummy"),         // stddev_pop
    (2725, "aggregate_dummy"),         // stddev_pop
    (2726, "aggregate_dummy"),         // stddev_pop
    (2727, "aggregate_dummy"),         // stddev_pop
    (2728, "aggregate_dummy"),         // stddev_pop
    (2729, "aggregate_dummy"),         // stddev_pop
    (2803, "aggregate_dummy"),         // count
    (2818, "aggregate_dummy"),         // regr_count
    (2819, "aggregate_dummy"),         // regr_sxx
    (2820, "aggregate_dummy"),         // regr_syy
    (2821, "aggregate_dummy"),         // regr_sxy
    (2822, "aggregate_dummy"),         // regr_avgx
    (2823, "aggregate_dummy"),         // regr_avgy
    (2824, "aggregate_dummy"),         // regr_r2
    (2825, "aggregate_dummy"),         // regr_slope
    (2826, "aggregate_dummy"),         // regr_intercept
    (2827, "aggregate_dummy"),         // covar_pop
    (2828, "aggregate_dummy"),         // covar_samp
    (2829, "aggregate_dummy"),         // corr
    (3058, "text_concat"),             // concat
    (3059, "text_concat_ws"),          // concat_ws
    (3100, "window_row_number"),       // row_number
    (3101, "window_rank"),             // rank
    (3102, "window_dense_rank"),       // dense_rank
    (3103, "window_percent_rank"),     // percent_rank
    (3104, "window_cume_dist"),        // cume_dist
    (3105, "window_ntile"),            // ntile
    (3106, "window_lag"),              // lag
    (3107, "window_lag_with_offset"),  // lag
    (3109, "window_lead"),             // lead
    (3110, "window_lead_with_offset"), // lead
    (3112, "window_first_value"),      // first_value
    (3113, "window_last_value"),       // last_value
    (3114, "window_nth_value"),        // nth_value
    (3167, "array_remove"),
    (3168, "array_replace"),
    (3179, "array_cardinality"),            // cardinality
    (3259, "generate_series_step_numeric"), // generate_series
    (3260, "generate_series_numeric"),      // generate_series
    (3277, "array_position"),
    (3278, "array_position_start"), // array_position
    (3279, "array_positions"),
    (3419, "sha224_bytea"),     // sha224
    (3420, "sha256_bytea"),     // sha256
    (3421, "sha384_bytea"),     // sha384
    (3422, "sha512_bytea"),     // sha512
    (3538, "aggregate_dummy"),  // string_agg
    (3545, "aggregate_dummy"),  // string_agg
    (3696, "text_starts_with"), // starts_with
    (3846, "make_date"),
    (5044, "int4gcd"),               // gcd
    (5045, "int8gcd"),               // gcd
    (5046, "int4lcm"),               // lcm
    (5047, "int8lcm"),               // lcm
    (5048, "numeric_gcd"),           // gcd
    (5049, "numeric_lcm"),           // lcm
    (6164, "aggregate_dummy"),       // bit_xor
    (6165, "aggregate_dummy"),       // bit_xor
    (6166, "aggregate_dummy"),       // bit_xor
    (6254, "regexp_count_no_start"), // regexp_count
    (6255, "regexp_count_no_flags"), // regexp_count
    (6256, "regexp_count"),
    (6257, "regexp_instr_no_start"),     // regexp_instr
    (6258, "regexp_instr_no_n"),         // regexp_instr
    (6259, "regexp_instr_no_endoption"), // regexp_instr
    (6260, "regexp_instr_no_flags"),     // regexp_instr
    (6261, "regexp_instr_no_subexpr"),   // regexp_instr
    (6262, "regexp_instr"),
    (6263, "regexp_like_no_flags"), // regexp_like
    (6264, "regexp_like"),
    (6339, "int4random"),     // random
    (6340, "int8random"),     // random
    (6341, "numeric_random"), // random
    (6381, "array_reverse"),
    (6388, "array_sort"),
    (6389, "array_sort_order"),             // array_sort
    (6390, "array_sort_order_nulls_first"), // array_sort
];

/// `pg_proc.prosrc` for this function — see [`PROSRC`]. An oid with no entry
/// falls back to `proname`, which is right for the 9 rows where the two
/// coincide and detectably wrong (against the live oracle) otherwise.
fn prosrc(f: &FuncSig) -> &'static str {
    PROSRC
        .iter()
        .find(|(oid, _)| *oid == f.oid.get())
        .map(|(_, src)| *src)
        .unwrap_or(f.name)
}

/// The nineteen `FUNCS` oids whose real `pg_proc` row carries argument
/// *names* — `string_agg(value, delimiter)`, `make_date(year, month, day)`,
/// the `regexp_*` family, `random(min, max)` and `array_sort`'s two- and
/// three-argument forms. Live-verified as the only non-`NULL` `proargnames`
/// among the 278 oids `FUNCS` covers; `FuncSig` has no field for an argument
/// name.
///
/// Case is load-bearing: `regexp_instr`'s fourth parameter is named `"N"`,
/// uppercase and quoted, in real `pg_proc`.
const PROARGNAMES: &[(u32, &[&str])] = &[
    (3538, &["value", "delimiter"]),
    (3545, &["value", "delimiter"]),
    (3846, &["year", "month", "day"]),
    (6254, &["string", "pattern"]),
    (6255, &["string", "pattern", "start"]),
    (6256, &["string", "pattern", "start", "flags"]),
    (6257, &["string", "pattern"]),
    (6258, &["string", "pattern", "start"]),
    (6259, &["string", "pattern", "start", "N"]),
    (6260, &["string", "pattern", "start", "N", "endoption"]),
    (
        6261,
        &["string", "pattern", "start", "N", "endoption", "flags"],
    ),
    (
        6262,
        &[
            "string",
            "pattern",
            "start",
            "N",
            "endoption",
            "flags",
            "subexpr",
        ],
    ),
    (6263, &["string", "pattern"]),
    (6264, &["string", "pattern", "flags"]),
    (6339, &["min", "max"]),
    (6340, &["min", "max"]),
    (6341, &["min", "max"]),
    (6389, &["array", "descending"]),
    (6390, &["array", "descending", "nulls_first"]),
];

/// `pg_proc.proargnames` for this function — see [`PROARGNAMES`].
fn proargnames(f: &FuncSig) -> Option<&'static [&'static str]> {
    PROARGNAMES
        .iter()
        .find(|(oid, _)| *oid == f.oid.get())
        .map(|(_, names)| *names)
}

/// `pg_proc.proretset` for this function — real data, not a placeholder:
/// exactly what [`FuncKind::SetReturning`] exists to carry (see `func.rs`).
fn proretset(f: &FuncSig) -> bool {
    f.kind == FuncKind::SetReturning
}

/// `pg_proc.prorows` for this function — Postgres's own default row
/// estimate for a set-returning function with no explicit `ROWS` clause
/// (`1000`), `0` for a row-at-a-time function, and `100` for `unnest`
/// (oid 2331), whose real definition carries an explicit `ROWS 100`.
/// Live-verified across all 278 oids: `unnest` is the only exception.
fn prorows(f: &FuncSig) -> f32 {
    if f.oid.get() == 2331 {
        100.0
    } else if proretset(f) {
        1000.0
    } else {
        0.0
    }
}

/// This function's value for `column`, or `None` if `column` is not one of
/// this relation's scalar-`Value`-representable columns — the list-typed
/// columns (`proargtypes`, `proallargtypes`, `proargmodes`, `proargnames`,
/// `protrftypes`, `proconfig`, `proacl`), the always-`NULL` nullable scalars
/// (`proargdefaults`, `probin`, `prosqlbody`), and `procost`/`prorows`
/// (`real`, and [`Value`] has no floating-point variant) are all handled by
/// the caller instead, and a predicate naming any of them is rejected by the
/// schema check in `scan` only if it is not a real column at all.
fn value(f: &FuncSig, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(f.oid),
        "proname" => Value::Text(f.name.to_string()),
        "pronamespace" => Value::Oid(crate::PG_CATALOG_NAMESPACE),
        "proowner" => Value::Oid(Oid(10)),
        "prolang" => Value::Oid(prolang(f)),
        "provariadic" => Value::Oid(provariadic(f)),
        "prosupport" => Value::Oid(prosupport(f)),
        "prokind" => Value::Text(prokind(f.kind).to_string()),
        "prosecdef" => Value::Bool(false),
        "proleakproof" => Value::Bool(proleakproof(f)),
        "proisstrict" => Value::Bool(proisstrict(f)),
        "proretset" => Value::Bool(proretset(f)),
        "provolatile" => Value::Text(provolatile(f).to_string()),
        "proparallel" => Value::Text(proparallel(f).to_string()),
        "pronargs" => Value::Int(proargtypes(f).len() as i64),
        "pronargdefaults" => Value::Int(0),
        "prorettype" => Value::Oid(prorettype(f)),
        "prosrc" => Value::Text(prosrc(f).to_string()),
        _ => return None,
    })
}

/// `FUNCS`, keeping only the first row for each oid — see the module docs on
/// why a real `pg_proc` cannot report the polymorphic functions' several
/// monomorphizations as separate rows.
fn deduplicated_by_oid() -> Vec<&'static FuncSig> {
    let mut seen = HashSet::new();
    FUNCS.iter().filter(|f| seen.insert(f.oid)).collect()
}

/// `pg_catalog.pg_proc`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgProc;

impl PgProc {
    fn arrow_schema() -> SchemaRef {
        let oid_list = || DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
        let text_list = || DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", DataType::UInt32, false),
            Field::new("proowner", DataType::UInt32, false),
            Field::new("prolang", DataType::UInt32, false),
            Field::new("procost", DataType::Float32, false),
            Field::new("prorows", DataType::Float32, false),
            Field::new("provariadic", DataType::UInt32, false),
            Field::new("prosupport", DataType::UInt32, false),
            Field::new("prokind", DataType::Utf8, false),
            Field::new("prosecdef", DataType::Boolean, false),
            Field::new("proleakproof", DataType::Boolean, false),
            Field::new("proisstrict", DataType::Boolean, false),
            Field::new("proretset", DataType::Boolean, false),
            Field::new("provolatile", DataType::Utf8, false),
            Field::new("proparallel", DataType::Utf8, false),
            Field::new("pronargs", DataType::Int16, false),
            Field::new("pronargdefaults", DataType::Int16, false),
            Field::new("prorettype", DataType::UInt32, false),
            Field::new("proargtypes", oid_list(), false),
            Field::new("proallargtypes", oid_list(), true),
            Field::new("proargmodes", text_list(), true),
            Field::new("proargnames", text_list(), true),
            Field::new("proargdefaults", DataType::Utf8, true),
            Field::new("protrftypes", oid_list(), true),
            Field::new("prosrc", DataType::Utf8, false),
            Field::new("probin", DataType::Utf8, true),
            // Real attnum 28, `prosqlbody`, is omitted here — see the module
            // docs. Every column below this point sits one `attnum` earlier
            // than its real position.
            Field::new("proconfig", text_list(), true),
            Field::new("proacl", text_list(), true),
        ]))
    }
}

impl crate::SystemView for PgProc {
    fn name(&self) -> &str {
        "pg_proc"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        _catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_proc",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&FuncSig> = deduplicated_by_oid()
            .into_iter()
            .filter(|f| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(f, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let pronames: StringArray = rows.iter().map(|r| Some(r.name)).collect();
        let pronamespaces: UInt32Array = rows
            .iter()
            .map(|_| crate::PG_CATALOG_NAMESPACE.get())
            .collect();
        // Placeholder / derived defaults — see the module docs for why each
        // is either a real value confirmed live for every `FUNCS` row, or a
        // documented default confirmed live wrong for a small, enumerable
        // set of rows.
        let proowners: UInt32Array = rows.iter().map(|_| 10).collect();
        let prolangs: UInt32Array = rows.iter().map(|r| prolang(r).get()).collect();
        let procosts: Float32Array = rows.iter().map(|_| 1.0).collect();
        let prorowss: Float32Array = rows.iter().map(|r| prorows(r)).collect();
        let provariadics: UInt32Array = rows.iter().map(|r| provariadic(r).get()).collect();
        let prosupports: UInt32Array = rows.iter().map(|r| prosupport(r).get()).collect();
        let prokinds: StringArray = rows
            .iter()
            .map(|r| Some(prokind(r.kind).to_string()))
            .collect();
        let prosecdefs: BooleanArray = rows.iter().map(|_| false).collect();
        let proleakproofs: BooleanArray = rows.iter().map(|r| proleakproof(r)).collect();
        let proisstricts: BooleanArray = rows.iter().map(|r| proisstrict(r)).collect();
        let proretsets: BooleanArray = rows.iter().map(|r| proretset(r)).collect();
        let provolatiles: StringArray = rows
            .iter()
            .map(|r| Some(provolatile(r).to_string()))
            .collect();
        let proparallels: StringArray = rows
            .iter()
            .map(|r| Some(proparallel(r).to_string()))
            .collect();
        let pronargs: Int16Array = rows.iter().map(|r| proargtypes(r).len() as i16).collect();
        let pronargdefaults: Int16Array = rows.iter().map(|_| 0).collect();
        let prorettypes: UInt32Array = rows.iter().map(|r| prorettype(r).get()).collect();

        let mut proargtypes_builder = ListBuilder::new(UInt32Builder::new());
        for r in &rows {
            for arg in proargtypes(r) {
                proargtypes_builder.values().append_value(arg);
            }
            proargtypes_builder.append(true);
        }
        let proargtypes_array: ListArray = proargtypes_builder.finish();

        // `proallargtypes`/`proargmodes` are non-`NULL` only for a real
        // `VARIADIC` function, and `proargnames` only for `string_agg` — see
        // `SignatureOverride` and `PROARGNAMES`. `protrftypes`,
        // `proargdefaults`, `probin`, `proconfig` and `proacl` are `NULL` for
        // every one of the 278 oids `FUNCS` covers, live-verified.
        let mut proallargtypes_builder = ListBuilder::new(UInt32Builder::new());
        let mut proargmodes_builder = ListBuilder::new(StringBuilder::new());
        let mut proargnames_builder = ListBuilder::new(StringBuilder::new());
        let mut protrftypes_builder = ListBuilder::new(UInt32Builder::new());
        let mut proconfig_builder = ListBuilder::new(StringBuilder::new());
        let mut proacl_builder = ListBuilder::new(StringBuilder::new());
        for r in &rows {
            match signature_override(r).and_then(|s| s.proargmodes) {
                Some(modes) => {
                    for arg in proargtypes(r) {
                        proallargtypes_builder.values().append_value(arg);
                    }
                    proallargtypes_builder.append(true);
                    for m in modes {
                        proargmodes_builder.values().append_value(m.to_string());
                    }
                    proargmodes_builder.append(true);
                }
                None => {
                    proallargtypes_builder.append_null();
                    proargmodes_builder.append_null();
                }
            }
            match proargnames(r) {
                Some(names) => {
                    for n in names {
                        proargnames_builder.values().append_value(n);
                    }
                    proargnames_builder.append(true);
                }
                None => proargnames_builder.append_null(),
            }
            protrftypes_builder.append_null();
            proconfig_builder.append_null();
            proacl_builder.append_null();
        }
        let proallargtypes: ListArray = proallargtypes_builder.finish();
        let proargmodes: ListArray = proargmodes_builder.finish();
        let proargnames_array: ListArray = proargnames_builder.finish();
        let protrftypes: ListArray = protrftypes_builder.finish();
        let proconfig: ListArray = proconfig_builder.finish();
        let proacl: ListArray = proacl_builder.finish();

        let proargdefaults = StringArray::from(vec![None::<&str>; rows.len()]);
        let prosrcs: StringArray = rows.iter().map(|r| Some(prosrc(r))).collect();
        let probin = StringArray::from(vec![None::<&str>; rows.len()]);

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(pronames),
                Arc::new(pronamespaces),
                Arc::new(proowners),
                Arc::new(prolangs),
                Arc::new(procosts),
                Arc::new(prorowss),
                Arc::new(provariadics),
                Arc::new(prosupports),
                Arc::new(prokinds),
                Arc::new(prosecdefs),
                Arc::new(proleakproofs),
                Arc::new(proisstricts),
                Arc::new(proretsets),
                Arc::new(provolatiles),
                Arc::new(proparallels),
                Arc::new(pronargs),
                Arc::new(pronargdefaults),
                Arc::new(prorettypes),
                Arc::new(proargtypes_array),
                Arc::new(proallargtypes),
                Arc::new(proargmodes),
                Arc::new(proargnames_array),
                Arc::new(proargdefaults),
                Arc::new(protrftypes),
                Arc::new(prosrcs),
                Arc::new(probin),
                Arc::new(proconfig),
                Arc::new(proacl),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use basin_pgtype::Oid;

    use super::*;
    use crate::{mock::MockCatalog, SystemView};

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    fn col_i16(batch: &RecordBatch, name: &str) -> Vec<i16> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_bool(batch: &RecordBatch, name: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|b| b.unwrap())
            .collect()
    }

    fn col_f32(batch: &RecordBatch, name: &str) -> Vec<f32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn proargtypes_row(batch: &RecordBatch, i: usize) -> Vec<u32> {
        let list = batch
            .column(batch.schema().index_of("proargtypes").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list.value(i);
        values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn row_for(batch: &RecordBatch, oid: u32) -> usize {
        col_u32(batch, "oid")
            .into_iter()
            .position(|o| o == oid)
            .unwrap_or_else(|| panic!("no pg_proc row for oid {oid}"))
    }

    #[test]
    fn name_is_pg_proc() {
        assert_eq!(PgProc.name(), "pg_proc");
    }

    /// Pins the exact column set, order, Arrow type and nullability of
    /// `pg_proc` against live PostgreSQL 18.2's `attnum` order, so a future
    /// edit cannot silently reorder or rename a column out from under
    /// positional readers (`psql \df`, and every ORM's function
    /// introspection, both read this positionally as well as by name).
    /// Verified live via:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_proc'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    /// ```
    ///
    /// See the module docs for why `prosqlbody` (28) is the one real column
    /// absent here, and the positional consequence.
    #[test]
    fn schema_matches_live_postgres_column_order_and_types() {
        let schema = PgProc.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();

        let oid_list = DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
        let text_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("proname", DataType::Utf8, false),
                ("pronamespace", DataType::UInt32, false),
                ("proowner", DataType::UInt32, false),
                ("prolang", DataType::UInt32, false),
                ("procost", DataType::Float32, false),
                ("prorows", DataType::Float32, false),
                ("provariadic", DataType::UInt32, false),
                ("prosupport", DataType::UInt32, false),
                ("prokind", DataType::Utf8, false),
                ("prosecdef", DataType::Boolean, false),
                ("proleakproof", DataType::Boolean, false),
                ("proisstrict", DataType::Boolean, false),
                ("proretset", DataType::Boolean, false),
                ("provolatile", DataType::Utf8, false),
                ("proparallel", DataType::Utf8, false),
                ("pronargs", DataType::Int16, false),
                ("pronargdefaults", DataType::Int16, false),
                ("prorettype", DataType::UInt32, false),
                ("proargtypes", oid_list.clone(), false),
                ("proallargtypes", oid_list.clone(), true),
                ("proargmodes", text_list.clone(), true),
                ("proargnames", text_list.clone(), true),
                ("proargdefaults", DataType::Utf8, true),
                ("protrftypes", oid_list, true),
                ("prosrc", DataType::Utf8, false),
                ("probin", DataType::Utf8, true),
                ("proconfig", text_list.clone(), true),
                ("proacl", text_list, true),
            ]
        );
    }

    /// `count(*)` (oid 2803) is the zero-argument aggregate row; `row_number`
    /// (oid 3100) is a zero-argument window row. Confirmed live — both
    /// `pronargs = 0` and `proargtypes` empty, `prokind` distinguishing them.
    #[test]
    fn zero_arg_rows_report_empty_proargtypes_and_correct_prokind() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();

        let i = row_for(&batch, 2803);
        assert_eq!(col_str(&batch, "proname")[i], "count");
        assert_eq!(col_i16(&batch, "pronargs")[i], 0);
        assert!(proargtypes_row(&batch, i).is_empty());
        assert_eq!(col_str(&batch, "prokind")[i], "a");

        let i = row_for(&batch, 3100);
        assert_eq!(col_str(&batch, "proname")[i], "row_number");
        assert_eq!(col_i16(&batch, "pronargs")[i], 0);
        assert_eq!(col_str(&batch, "prokind")[i], "w");
    }

    /// `prokind` must actually distinguish `'a'` aggregates from `'w'` window
    /// functions from `'f'` plain functions — the core claim this column
    /// exists to let a client (or the planner) branch on.
    #[test]
    fn prokind_distinguishes_aggregate_window_and_plain() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();

        // sum(int4), oid 2108: aggregate.
        let i = row_for(&batch, 2108);
        assert_eq!(col_str(&batch, "prokind")[i], "a");

        // lag(int4), oid 3106: window.
        let i = row_for(&batch, 3106);
        assert_eq!(col_str(&batch, "prokind")[i], "w");

        // lower(text), oid 870: plain scalar.
        let i = row_for(&batch, 870);
        assert_eq!(col_str(&batch, "prokind")[i], "f");
    }

    /// `count(x)` (oid 2147) has one declared argument, Postgres's `"any"`
    /// pseudo-type (oid 2276) — confirmed live via `proargtypes`.
    #[test]
    fn count_of_x_reports_the_any_pseudo_type_argument() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 2147);
        assert_eq!(col_i16(&batch, "pronargs")[i], 1);
        assert_eq!(proargtypes_row(&batch, i), vec![2276]);
    }

    /// `generate_series(int4, int4)` (oid 1067) has two `int4` arguments and
    /// returns `int4` — confirmed live.
    #[test]
    fn generate_series_reports_its_two_int4_arguments() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 1067);
        assert_eq!(col_str(&batch, "proname")[i], "generate_series");
        assert_eq!(proargtypes_row(&batch, i), vec![23, 23]);
        assert_eq!(col_u32(&batch, "prorettype")[i], 23);
        // Set-returning, but prokind still collapses to 'f' — see the module
        // docs on why that matches real Postgres.
        assert_eq!(col_str(&batch, "prokind")[i], "f");
        // proretset and prorows are real, derived data — confirmed live:
        // generate_series is set-returning with the default 1000-row
        // estimate.
        assert!(col_bool(&batch, "proretset")[i]);
        assert_eq!(col_f32(&batch, "prorows")[i], 1000.0);
    }

    /// `lower(text)` (oid 870) is not set-returning — `proretset = false` and
    /// `prorows = 0`, confirmed live.
    #[test]
    fn scalar_function_reports_false_proretset_and_zero_prorows() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 870);
        assert!(!col_bool(&batch, "proretset")[i]);
        assert_eq!(col_f32(&batch, "prorows")[i], 0.0);
    }

    /// The polymorphic functions (`array_agg`, `unnest`, `lag`/`lead`/...)
    /// share one real oid across their monomorphized instantiations in
    /// `FUNCS`; this relation must report that oid exactly once.
    #[test]
    fn polymorphic_function_oid_appears_exactly_once() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let count = col_u32(&batch, "oid")
            .into_iter()
            .filter(|&o| o == 2335)
            .count();
        assert_eq!(
            count, 1,
            "oid 2335 (array_agg) must be deduplicated to one row"
        );
    }

    /// Every row lives in `pg_catalog` (namespace 11), confirmed live.
    #[test]
    fn every_row_is_in_pg_catalog_namespace() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        for ns in col_u32(&batch, "pronamespace") {
            assert_eq!(ns, 11);
        }
    }

    /// `proowner` and `prosecdef` are uniform real values across every `FUNCS`
    /// row — confirmed live.
    ///
    /// `proleakproof` and `proparallel` used to be asserted uniform here too.
    /// They are not, and never were: the claim held only because `FUNCS`
    /// covered 135 oids that happened to agree. Adding the DataFusion-orphan
    /// rows brought in seven genuinely leakproof functions and four
    /// parallel-restricted ones, so both columns now have real, enumerated
    /// exceptions —
    /// [`leakproof_and_parallel_report_their_real_per_function_values`] pins
    /// them, and `catalog_fidelity` re-checks every row of both columns
    /// against a live server on every run.
    #[test]
    fn uniform_columns_report_their_confirmed_live_values() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        for owner in col_u32(&batch, "proowner") {
            assert_eq!(owner, 10);
        }
        for secdef in col_bool(&batch, "prosecdef") {
            assert!(!secdef);
        }
    }

    /// `proleakproof` and `proparallel` are per-function, not uniform.
    /// Confirmed live: `md5` (2311) and `starts_with` (3696) are leakproof
    /// while `lower` (870) is not, and every `random` overload (1598) is
    /// parallel-*restricted* (`'r'`) because it advances a per-backend PRNG a
    /// worker cannot share, while `lower` is parallel-safe (`'s'`).
    #[test]
    fn leakproof_and_parallel_report_their_real_per_function_values() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let leakproof = col_bool(&batch, "proleakproof");
        assert!(leakproof[row_for(&batch, 2311)], "md5(text) is leakproof");
        assert!(leakproof[row_for(&batch, 3696)], "starts_with is leakproof");
        assert!(!leakproof[row_for(&batch, 870)], "lower is not");

        let parallel = col_str(&batch, "proparallel");
        assert_eq!(parallel[row_for(&batch, 1598)], "r", "random() is 'r'");
        assert_eq!(parallel[row_for(&batch, 6339)], "r");
        assert_eq!(parallel[row_for(&batch, 870)], "s", "lower is 's'");

        // `random()` is also the only `provolatile = 'v'` shape in this
        // table — 'v' and 'r' are different columns saying different things,
        // and conflating them would make `now()` (stable, parallel-safe) look
        // like `random()`.
        assert_eq!(col_str(&batch, "provolatile")[row_for(&batch, 1598)], "v");
        assert_eq!(col_str(&batch, "provolatile")[row_for(&batch, 1299)], "s");
    }

    /// `now()` (oid 1299) is one of the 8 real `provolatile = 's'`
    /// exceptions — `lower` (870) is `'i'` like the vast majority of rows.
    /// Confirmed live.
    #[test]
    fn provolatile_distinguishes_the_known_stable_exceptions() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(col_str(&batch, "provolatile")[row_for(&batch, 1299)], "s");
        assert_eq!(col_str(&batch, "provolatile")[row_for(&batch, 870)], "i");
    }

    /// `concat` (oid 3058) is one of the 2 real `provariadic` exceptions
    /// (`"any"`'s oid, 2276) — `lower` (870) is `0` like the vast majority of
    /// rows. Confirmed live.
    #[test]
    fn provariadic_distinguishes_the_known_variadic_exceptions() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(col_u32(&batch, "provariadic")[row_for(&batch, 3058)], 2276);
        assert_eq!(col_u32(&batch, "provariadic")[row_for(&batch, 870)], 0);
    }

    /// `date_part` (oid 1384) is one of the 6 real `prolang = 14` SQL-wrapper
    /// exceptions — `lower` (870) is `12` (`internal`) like the vast
    /// majority of rows. Confirmed live.
    #[test]
    fn prolang_distinguishes_the_known_sql_wrapper_exceptions() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(col_u32(&batch, "prolang")[row_for(&batch, 1384)], 14);
        assert_eq!(col_u32(&batch, "prolang")[row_for(&batch, 870)], 12);
    }

    /// The entire point of this crate: a predicate on `oid` narrows to
    /// exactly one row, mirroring `pg_proc.oid` being a real primary key.
    #[test]
    fn pushed_oid_predicate_narrows_to_exactly_one_row() {
        let full = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(full.num_rows() > 1, "sanity: pg_proc has more than one row");

        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(2803))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "proname"), vec!["count".to_string()]);
    }

    /// A predicate matching nothing returns zero rows, not everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(999_999))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column this relation does not have at all is an
    /// error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_proc",
                column: "nope".to_string(),
            }
        );
    }

    /// `proargtypes` is a real column of this relation's schema, but it is
    /// list-typed and [`value`] has no scalar [`Value`] to report for it — so
    /// a predicate naming it is not an `UnknownColumn` error (the column
    /// exists), but matches no row at all, the same way any predicate
    /// against a column a given row cannot produce a value for behaves (see
    /// [`Predicate::matches`]'s own docs on `actual: None`).
    #[test]
    fn predicate_on_proargtypes_matches_no_rows_rather_than_erroring() {
        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("proargtypes", 1i64)])
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// `IN` pushdown on `proname` narrows to exactly the named functions.
    #[test]
    fn pushed_in_predicate_narrows_by_name() {
        let filtered = PgProc
            .scan(
                &MockCatalog::new(),
                &[Predicate::in_list(
                    "proname",
                    [
                        Value::Text("lower".to_string()),
                        Value::Text("upper".to_string()),
                    ],
                )],
            )
            .unwrap();
        let mut names = col_str(&filtered, "proname");
        names.sort();
        assert_eq!(names, vec!["lower".to_string(), "upper".to_string()]);
    }
}
