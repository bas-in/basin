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
//! - `proowner` (4), `oid`, `NOT NULL`: not a `FuncSig` field, but confirmed
//!   live to be `10` (the bootstrap superuser) for **every one of the 135
//!   oids `FUNCS` currently declares** —
//!   `SELECT DISTINCT proowner FROM pg_proc WHERE oid IN (<all 135 FUNCS
//!   oids>)` returns exactly one row, `10`. Added as that literal.
//! - `prolang` (5), `oid`, `NOT NULL`: not a `FuncSig` field. Live-verified
//!   `12` (`internal`) for 129 of the 135 oids; the other 6 — `date_part`
//!   (1384), `age` (1386, 2059), `round` (1708), `trunc` (1710), `log`
//!   (1741) — are real SQL-language wrappers (`prolang = 14`, `prosqlbody`
//!   non-`NULL`) around another builtin, a distinction `FuncSig` has no field
//!   for. Added as `12`, wrong for those 6.
//! - `procost` (6), `real`, `NOT NULL`: live-verified `1` for all 135 oids.
//!   Added as that literal.
//! - `prorows` (7), `real`, `NOT NULL`: live-verified `0` for every
//!   non-set-returning row and `1000` for `generate_series`'s six oids —
//!   Postgres's own default row estimate for a set-returning function with
//!   no `ROWS` clause. `unnest` (2331) is the one exception in the sample
//!   (`100`, an explicit `ROWS` in its real definition); `FuncSig` has no
//!   field to distinguish it from `generate_series`. Added as
//!   `1000.0`/`0.0` from [`FuncKind::SetReturning`], wrong only for
//!   `unnest`.
//! - `provariadic` (8), `oid`, `NOT NULL`: live-verified `0` for 133 of 135
//!   oids; `concat` (3058) and `concat_ws` (3059) are real
//!   `VARIADIC "any"` functions (`provariadic = 2276`, the `"any"` pseudo-
//!   type) that `func.rs`'s own docs say are monomorphized to a fixed arity
//!   rather than modeled as variadic (see that module's docs on `concat`).
//!   Added as `0`, wrong for those 2.
//! - `prosupport` (9), `regproc`, `NOT NULL`: not a `FuncSig` field, and not
//!   uniform — live-verified non-zero (a real planner support function, e.g.
//!   `count`'s `int8inc_support`, `row_number`'s
//!   `window_row_number_support`) for 14 of the 135 oids, all aggregates,
//!   window functions or SRFs. `0` ("no support function") is Postgres's own
//!   default when a `CREATE FUNCTION` omits `SUPPORT`, and matches 121 of
//!   135 (~90%) of the sample — represented here as `oid` per this crate's
//!   `regproc`-as-`oid` convention (see [`crate::pg_cast`]'s `castfunc`).
//!   Added as `0`, wrong for that ~10%.
//! - `prokind` (10): unchanged logic (see [`prokind`]), moved into its real
//!   position.
//! - `prosecdef` (11), `proleakproof` (12), both `boolean`, `NOT NULL`:
//!   live-verified `false` for all 135 oids (no builtin here runs with
//!   definer's rights or claims to be leakproof). Added as that literal.
//! - `proisstrict` (13), `boolean`, `NOT NULL`: **not added.** Not a
//!   `FuncSig` field, and unlike the columns above, not close to uniform:
//!   live-verified `false` for every aggregate (37/37) and `true` for
//!   nearly every plain/SRF row (86/88), but window functions split roughly
//!   evenly (7 strict, 3 not) with no rule `FuncSig` can derive — `lag`/
//!   `lead` are strict, `row_number`/`rank`/`ntile` are not, and nothing
//!   in [`FuncKind`] distinguishes them. Any single default would be wrong
//!   for a large, undocumentable share of rows rather than a small
//!   enumerable one, so this crate's "confirmed live, or omitted" line is
//!   drawn here. See "positional consequence" below.
//! - `proretset` (14), `boolean`, `NOT NULL`: real data, not a placeholder —
//!   `f.kind == FuncKind::SetReturning`, exactly what that variant's own doc
//!   comment says it exists to carry (see `func.rs`).
//! - `provolatile` (15), `"char"`, `NOT NULL`: not a `FuncSig` field.
//!   Live-verified `'i'` (immutable) for 127 of 135 oids; the 8 exceptions
//!   are `'s'` (stable): `generate_series` on `timestamp`/`timestamptz`
//!   (oid 939), `date_part` (1171), `date_trunc` (1217), `now` (1299), `age`
//!   (1386, 2059), `concat`/`concat_ws` (3058, 3059) — all genuinely
//!   time-zone- or session-dependent. Added as `'i'`, wrong for those 8.
//! - `proparallel` (16), `"char"`, `NOT NULL`: live-verified `'s'`
//!   (parallel-safe) for all 135 oids. Added as that literal.
//! - `pronargs` (17), `smallint`, `NOT NULL`: unchanged (`FuncSig::args.len()
//!   as i16`), moved into its real position.
//! - `pronargdefaults` (18), `smallint`, `NOT NULL`: not a `FuncSig` field
//!   (it has no notion of a default-valued argument), but live-verified `0`
//!   for all 135 oids — real, not a guess, for every row this crate can
//!   currently construct. Added as that literal.
//! - `prorettype` (19), `proargtypes` (20): unchanged (`FuncSig::ret`,
//!   `FuncSig::args`), moved into their real positions. `proargtypes` is
//!   `oidvector`, distinct from `proallargtypes`'s `oid[]` at 21 — see
//!   "how vector/array columns are represented" below.
//! - `proallargtypes` (21, `oid[]`), `proargmodes` (22, `"char"[]`),
//!   `proargnames` (23, `text[]`) — all nullable, and `NULL` for the
//!   overwhelming majority of the sample (132–133 of 135: `FuncSig` has no
//!   field for any of the three). The exceptions are real and enumerable:
//!   `concat`/`concat_ws` (3058, 3059) have non-`NULL`
//!   `proallargtypes`/`proargmodes` (their variadic slot, per `provariadic`
//!   above), and `string_agg` (3538, 3545) has non-`NULL` `proargnames`
//!   (`{value,delimiter}`). Added as always-`NULL`, wrong for those 4 rows.
//! - `proargdefaults` (24, `pg_node_tree`), `protrftypes` (25, `oid[]`) —
//!   both nullable; live-verified `NULL` for **all** 135 oids (no default-
//!   valued argument and no `TRANSFORM` in this sample). Added as
//!   always-`NULL`, correct for every row.
//! - `prosrc` (26), `text`, `NOT NULL`: **not added.** `FuncSig::name` is
//!   the SQL-visible `proname`, not the internal C symbol `prosrc` actually
//!   names, and the two differ for a large share of the table — `count`'s
//!   is `aggregate_dummy`, `row_number`'s is `window_row_number`,
//!   `generate_series`'s is `generate_series_int4`, live-verified. Because
//!   this column is `NOT NULL`, "no data" cannot mean `NULL` the way it does
//!   for `probin` below; the only other option is a fabricated per-row
//!   string, which the crate's own task rules this file was written under
//!   forbid outright. Omitted. See "positional consequence" below.
//! - `probin` (27), `text`, nullable: live-verified `NULL` for all 135 oids
//!   (every `FUNCS` function is `internal`-language, never `C`-language).
//!   Added as always-`NULL`, correct for every row.
//! - `prosqlbody` (28), `pg_node_tree`, nullable: live-verified non-`NULL`
//!   for exactly the 6 SQL-language wrapper oids named under `prolang`
//!   above, `NULL` for the other 129. Added as always-`NULL`, wrong for
//!   those 6 — the same 6, and the same reason, as `prolang`.
//! - `proconfig` (29, `text[]`), `proacl` (30, `aclitem[]`) — both nullable;
//!   live-verified `NULL` for all 135 oids (no `SET` clause, and default
//!   privileges throughout — nothing in this sample has had `GRANT`/`REVOKE`
//!   applied). Added as always-`NULL`, correct for every row.
//!
//! ## Positional consequence of the two omissions
//!
//! `proisstrict` (real attnum 13) and `prosrc` (real attnum 26) are the only
//! columns this relation does not implement at all. Everything between them,
//! attnums 14–25, sits one slot earlier here than in a real `pg_proc`
//! (`proretset` is Basin attnum 13, not 14; `protrftypes` is Basin attnum 24,
//! not 25). Everything after `prosrc`, attnums 27–30, sits two slots earlier
//! (`probin` is Basin attnum 25, not 27; `proacl` is Basin attnum 28, not
//! 30). A caller reading by name is unaffected; a caller reading by position
//! past attnum 12 is not looking at what it thinks it is.
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
//! string, math, date/time, aggregate, window and set-returning functions in
//! the categories needed to unblock planner resolution, and several real
//! `pg_proc` rows it explicitly does not include (`ceiling`, `date_trunc`'s
//! three-argument timezone-name overload, `age(xid)`, `avg(int2)`/
//! `avg(int4)`'s own dedicated oids) are simply absent from this relation
//! too — not covered wrongly, not covered yet. See `func.rs`'s own module
//! docs for the full scope statement.

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

/// The 6 real SQL-language wrapper oids in `FUNCS` — `date_part`, `age`
/// (both overloads), `round`, `trunc`, `log` — confirmed live to have
/// `prolang = 14` and a non-`NULL` `prosqlbody`, unlike every other row's
/// `prolang = 12` (`internal`). See the module docs.
const SQL_LANGUAGE_WRAPPER_OIDS: [u32; 6] = [1384, 1386, 1708, 1710, 1741, 2059];

/// `pg_proc.prolang` for this function — see [`SQL_LANGUAGE_WRAPPER_OIDS`].
fn prolang(f: &FuncSig) -> Oid {
    if SQL_LANGUAGE_WRAPPER_OIDS.contains(&f.oid.get()) {
        Oid(14)
    } else {
        Oid(12)
    }
}

/// The 8 real `provolatile = 's'` (stable) oids in `FUNCS` — every other row
/// is `'i'` (immutable). See the module docs.
const STABLE_VOLATILITY_OIDS: [u32; 8] = [939, 1171, 1217, 1299, 1386, 2059, 3058, 3059];

/// `pg_proc.provolatile` for this function — see [`STABLE_VOLATILITY_OIDS`].
fn provolatile(f: &FuncSig) -> char {
    if STABLE_VOLATILITY_OIDS.contains(&f.oid.get()) {
        's'
    } else {
        'i'
    }
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

/// `pg_proc.proretset` for this function — real data, not a placeholder:
/// exactly what [`FuncKind::SetReturning`] exists to carry (see `func.rs`).
fn proretset(f: &FuncSig) -> bool {
    f.kind == FuncKind::SetReturning
}

/// `pg_proc.prorows` for this function — Postgres's own default row
/// estimate for a set-returning function with no explicit `ROWS` clause
/// (`1000`), `0` for a row-at-a-time function. See the module docs on the
/// one known exception (`unnest`, real value `100`).
fn prorows(f: &FuncSig) -> f32 {
    if proretset(f) {
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
        "prosupport" => Value::Oid(Oid::INVALID),
        "prokind" => Value::Text(prokind(f.kind).to_string()),
        "prosecdef" => Value::Bool(false),
        "proleakproof" => Value::Bool(false),
        "proretset" => Value::Bool(proretset(f)),
        "provolatile" => Value::Text(provolatile(f).to_string()),
        "proparallel" => Value::Text("s".to_string()),
        "pronargs" => Value::Int(f.args.len() as i64),
        "pronargdefaults" => Value::Int(0),
        "prorettype" => Value::Oid(f.ret),
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
            // Real attnum 13, `proisstrict`, is omitted here — not close to
            // uniform across `FUNCS` and not derivable from `FuncSig`; see
            // the module docs. Every column below this point therefore sits
            // one `attnum` earlier than its real position, and two earlier
            // from `probin` (25) onward, once `prosrc` (real attnum 26) is
            // also skipped below.
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
            // Real attnum 26, `prosrc`, is omitted here — it is `NOT NULL`
            // in real Postgres, but `FuncSig::name` is the SQL-visible
            // `proname`, not the internal C symbol `prosrc` actually names,
            // and the two disagree for a large share of `FUNCS`; see the
            // module docs. Every column below this point sits two `attnum`s
            // earlier than its real position.
            Field::new("probin", DataType::Utf8, true),
            Field::new("prosqlbody", DataType::Utf8, true),
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
        let prosupports: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let prokinds: StringArray = rows
            .iter()
            .map(|r| Some(prokind(r.kind).to_string()))
            .collect();
        let prosecdefs: BooleanArray = rows.iter().map(|_| false).collect();
        let proleakproofs: BooleanArray = rows.iter().map(|_| false).collect();
        let proretsets: BooleanArray = rows.iter().map(|r| proretset(r)).collect();
        let provolatiles: StringArray = rows
            .iter()
            .map(|r| Some(provolatile(r).to_string()))
            .collect();
        let proparallels: StringArray = rows.iter().map(|_| Some("s".to_string())).collect();
        let pronargs: Int16Array = rows.iter().map(|r| r.args.len() as i16).collect();
        let pronargdefaults: Int16Array = rows.iter().map(|_| 0).collect();
        let prorettypes: UInt32Array = rows.iter().map(|r| r.ret.get()).collect();

        let mut proargtypes_builder = ListBuilder::new(UInt32Builder::new());
        for r in &rows {
            for arg in r.args {
                proargtypes_builder.values().append_value(arg.get());
            }
            proargtypes_builder.append(true);
        }
        let proargtypes: ListArray = proargtypes_builder.finish();

        // Always-`NULL` placeholders — real for every `FUNCS` row except the
        // small, enumerable exceptions the module docs name (`concat`/
        // `concat_ws` for `proallargtypes`/`proargmodes`, `string_agg` for
        // `proargnames`, the 6 SQL-language wrapper oids for `prosqlbody`).
        let mut proallargtypes_builder = ListBuilder::new(UInt32Builder::new());
        let mut proargmodes_builder = ListBuilder::new(StringBuilder::new());
        let mut proargnames_builder = ListBuilder::new(StringBuilder::new());
        let mut protrftypes_builder = ListBuilder::new(UInt32Builder::new());
        let mut proconfig_builder = ListBuilder::new(StringBuilder::new());
        let mut proacl_builder = ListBuilder::new(StringBuilder::new());
        for _ in &rows {
            proallargtypes_builder.append_null();
            proargmodes_builder.append_null();
            proargnames_builder.append_null();
            protrftypes_builder.append_null();
            proconfig_builder.append_null();
            proacl_builder.append_null();
        }
        let proallargtypes: ListArray = proallargtypes_builder.finish();
        let proargmodes: ListArray = proargmodes_builder.finish();
        let proargnames: ListArray = proargnames_builder.finish();
        let protrftypes: ListArray = protrftypes_builder.finish();
        let proconfig: ListArray = proconfig_builder.finish();
        let proacl: ListArray = proacl_builder.finish();

        let proargdefaults = StringArray::from(vec![None::<&str>; rows.len()]);
        let probin = StringArray::from(vec![None::<&str>; rows.len()]);
        let prosqlbody = StringArray::from(vec![None::<&str>; rows.len()]);

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
                Arc::new(proretsets),
                Arc::new(provolatiles),
                Arc::new(proparallels),
                Arc::new(pronargs),
                Arc::new(pronargdefaults),
                Arc::new(prorettypes),
                Arc::new(proargtypes),
                Arc::new(proallargtypes),
                Arc::new(proargmodes),
                Arc::new(proargnames),
                Arc::new(proargdefaults),
                Arc::new(protrftypes),
                Arc::new(probin),
                Arc::new(prosqlbody),
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
    /// See the module docs for why `proisstrict` (13) and `prosrc` (26) are
    /// the two real columns absent here, and the positional consequence of
    /// each.
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
                ("probin", DataType::Utf8, true),
                ("prosqlbody", DataType::Utf8, true),
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

    /// `proowner`, `prosecdef`, `proleakproof` and `proparallel` are uniform
    /// real values across every `FUNCS` row — confirmed live.
    #[test]
    fn uniform_columns_report_their_confirmed_live_values() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        for owner in col_u32(&batch, "proowner") {
            assert_eq!(owner, 10);
        }
        for secdef in col_bool(&batch, "prosecdef") {
            assert!(!secdef);
        }
        for leakproof in col_bool(&batch, "proleakproof") {
            assert!(!leakproof);
        }
        for parallel in col_str(&batch, "proparallel") {
            assert_eq!(parallel, "s");
        }
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
