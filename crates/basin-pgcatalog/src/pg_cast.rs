//! `pg_catalog.pg_cast`, a view over `basin-pgtype`'s own cast matrix.
//!
//! Like [`crate::pg_type`] and [`crate::pg_operator`], this needs no
//! [`CatalogSource`]: every builtin cast Basin knows about is already encoded
//! in `crates/basin-pgtype/src/cast.rs`'s `cast_kind`, whose own module docs
//! record the live PostgreSQL 18 `pg_cast` join used to verify it. This
//! relation reports that same cast matrix under `pg_cast`'s column names.
//!
//! # Why this table, not a private map
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 ranks `pg_cast`
//! alongside `pg_operator` as priority 1, for the same reason: the owned
//! planner must decide whether an implicit/assignment/explicit coercion is
//! legal by consulting *some* table, and `basin_pgtype::cast` already is that
//! table internally. Exposing it as `pg_catalog.pg_cast` costs this file and
//! delivers `\dC` / driver introspection for free.
//!
//! # Where the row list comes from, and what is deliberately not derived
//!
//! `cast_kind`'s internal `builtin_pg_cast` match table is private and has no
//! `castmethod` column — Postgres's cast matrix carries a fact `cast_kind`
//! never needed: *how* the conversion runs (`'f'` a real function, `'b'`
//! binary-coercible with no function at all, `'i'` via the types' own I/O
//! routines). [`CAST_ROWS`] below is therefore this module's own static list
//! of `(source, target, castmethod)` triples — the exact pair set
//! `cast.rs`'s `builtin_pg_cast` covers, re-declared here because that table
//! is private, plus the one new fact it doesn't carry. `castcontext` is
//! **not** re-declared: every row calls the real, public
//! [`basin_pgtype::cast::cast_kind`] at scan time and reports whatever it
//! returns, so `pg_cast.castcontext` can never drift from the function the
//! planner itself calls to decide coercion legality. A test
//! ([`tests::every_row_agrees_with_cast_kind`]) pins that no row here names a
//! pair `cast_kind` disagrees with.
//!
//! `castmethod` was checked against a live PostgreSQL 18, pair by pair:
//!
//! ```sql
//! SELECT s.typname, t.typname, c.castfunc, c.castcontext, c.castmethod
//!   FROM pg_cast c
//!   JOIN pg_type s ON s.oid = c.castsource
//!   JOIN pg_type t ON t.oid = c.casttarget
//!  WHERE s.typname IN (<every builtin scalar type name>)
//!    AND t.typname IN (<same set>)
//!  ORDER BY s.typname, t.typname;
//! ```
//!
//! Re-run it, filtered to the pairs in [`CAST_ROWS`], before editing the
//! table. Most rows are `'f'`; a handful are binary-coercible (`int4 <->
//! oid`, and `text`/`varchar`/`bpchar` cross-casts among themselves — real
//! `pg_cast` rows, not the I/O fallback, despite all being string types), and
//! `json <-> jsonb` is `'i'`, confirmed live.
//!
//! # What is deliberately absent — three separate gaps, stated honestly
//!
//! 1. **The string I/O fallback is not tabulated, matching `cast.rs`.**
//!    `cast_kind`'s own docs explain that most types reach a string type on
//!    assignment (and a string type reaches most types on explicit cast) via
//!    Postgres's I/O-function fallback rather than a real `pg_cast` row —
//!    `int4 -> text`, `uuid -> text`, `numeric -> text` have **no row** in a
//!    real `pg_cast` either (confirmed live: zero rows returned for
//!    `castsource = 'int4'::regtype AND casttarget = 'text'::regtype`). This
//!    relation correctly has no row for those pairs, matching the real
//!    server. One real exception exists and is *not* reproduced here: `bool
//!    -> text` (oid confirmed live) has a genuine dedicated `pg_cast` row in
//!    Postgres (`castfunc = text(boolean)`, context `'a'`) rather than using
//!    the fallback — `cast.rs` still gets `bool -> text`'s *context* right via
//!    the fallback path, so nothing is observably wrong, but this relation
//!    will report zero rows for that pair where a real server reports one.
//! 2. **`castfunc` is the real function oid**, from [`CASTFUNC`]. It was
//!    previously always `0`, documented as a gap on the grounds that Basin
//!    executes these conversions directly in Rust rather than through a
//!    `pg_proc`-registered function, so there was "no real oid to report".
//!    That reasoning was wrong in the same way it would have been wrong for
//!    [`crate::pg_am`]'s `amhandler` or [`crate::pg_type`]'s `typinput`:
//!    `castfunc` is a property of the *cast*, fixed by Postgres's own catalog
//!    bootstrap and identical on every installation, not a statement about
//!    how Basin implements it. `0` is what a real server reports for a
//!    `'b'`-method (binary-coercible) row and *never* for an `'f'`-method
//!    row, so the old placeholder was telling clients that 69 of this
//!    relation's function-based casts had no implementing function at all —
//!    a claim `catalog_fidelity`'s row oracle flagged the first time it ran
//!    against `pg_cast`. The oids Basin reports point at `pg_proc` rows
//!    [`crate::pg_proc`] mostly does not have, which is the same admitted
//!    gap `pg_am` and `pg_type` already carry and is documented there.
//! 3. **Row coverage mirrors `cast.rs`'s own scope**, not all 269 rows a real
//!    PostgreSQL 18 `pg_cast` has (`SELECT count(*) FROM pg_cast`, confirmed
//!    live). `cast.rs`'s own docs describe its scope as the builtin scalar
//!    cast matrix; array-to-array and `unknown` coercions are real, correct
//!    answers from `cast_kind` but are not `pg_cast` rows in real Postgres
//!    either (arrays inherit their element cast, `unknown` is resolved before
//!    `pg_cast` is ever consulted — see `cast.rs`'s module docs), so their
//!    absence here matches the server, not just `cast.rs`.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::{cast::cast_kind, oid, Oid};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// `(castsource, casttarget, oid, castmethod)`. `castcontext` is deliberately
/// not a field here — see the module docs on why it is derived from
/// [`cast_kind`] at scan time instead of duplicated by hand.
///
/// `oid` is `pg_cast`'s own row oid (real `attnum` 1, ahead of
/// `castsource`) — live-verified per pair below rather than assigned by this
/// crate, the same reasoning [`crate::pg_am`] gives for its handler oids:
/// `pg_cast` rows have no explicit oid in Postgres's bootstrap data, so
/// `genbki` auto-numbers them deterministically from the fixed order its own
/// `pg_cast.dat` lists them in — the same number on every standard
/// PostgreSQL 18.2 build, not a value this crate invented.
type CastPair = (Oid, Oid, Oid, char);

/// The builtin `pg_cast` rows this relation reports. Mirrors exactly the pair
/// set `basin_pgtype::cast`'s private `builtin_pg_cast` table covers — see the
/// module docs for the live query used to check every `castmethod`. The `oid`
/// field (third of the tuple) was checked with:
///
/// ```sql
/// SELECT s.typname, t.typname, c.oid
///   FROM pg_cast c
///   JOIN pg_type s ON s.oid = c.castsource
///   JOIN pg_type t ON t.oid = c.casttarget
///  WHERE s.typname IN (<every builtin scalar type name>)
///    AND t.typname IN (<same set>)
///  ORDER BY s.typname, t.typname;
/// ```
static CAST_ROWS: &[CastPair] = &[
    // ─── The numeric tower ──────────────────────────────────────────────────
    (oid::INT2, oid::INT4, Oid(10006), 'f'),
    (oid::INT2, oid::INT8, Oid(10005), 'f'),
    (oid::INT2, oid::FLOAT4, Oid(10007), 'f'),
    (oid::INT2, oid::FLOAT8, Oid(10008), 'f'),
    (oid::INT2, oid::NUMERIC, Oid(10009), 'f'),
    (oid::INT4, oid::INT8, Oid(10010), 'f'),
    (oid::INT4, oid::FLOAT4, Oid(10012), 'f'),
    (oid::INT4, oid::FLOAT8, Oid(10013), 'f'),
    (oid::INT4, oid::NUMERIC, Oid(10014), 'f'),
    (oid::INT4, oid::INT2, Oid(10011), 'f'),
    (oid::INT8, oid::FLOAT4, Oid(10002), 'f'),
    (oid::INT8, oid::FLOAT8, Oid(10003), 'f'),
    (oid::INT8, oid::NUMERIC, Oid(10004), 'f'),
    (oid::INT8, oid::INT2, Oid(10000), 'f'),
    (oid::INT8, oid::INT4, Oid(10001), 'f'),
    (oid::FLOAT4, oid::FLOAT8, Oid(10018), 'f'),
    (oid::FLOAT4, oid::INT2, Oid(10016), 'f'),
    (oid::FLOAT4, oid::INT4, Oid(10017), 'f'),
    (oid::FLOAT4, oid::INT8, Oid(10015), 'f'),
    (oid::FLOAT4, oid::NUMERIC, Oid(10019), 'f'),
    (oid::FLOAT8, oid::FLOAT4, Oid(10023), 'f'),
    (oid::FLOAT8, oid::INT2, Oid(10021), 'f'),
    (oid::FLOAT8, oid::INT4, Oid(10022), 'f'),
    (oid::FLOAT8, oid::INT8, Oid(10020), 'f'),
    (oid::FLOAT8, oid::NUMERIC, Oid(10024), 'f'),
    (oid::NUMERIC, oid::FLOAT4, Oid(10028), 'f'),
    (oid::NUMERIC, oid::FLOAT8, Oid(10029), 'f'),
    (oid::NUMERIC, oid::INT2, Oid(10026), 'f'),
    (oid::NUMERIC, oid::INT4, Oid(10027), 'f'),
    (oid::NUMERIC, oid::INT8, Oid(10025), 'f'),
    // ─── oid ────────────────────────────────────────────────────────────────
    //
    // int4 <-> oid are binary-coercible (no function at all) in both
    // directions; int2 -> oid and oid -> int8 still run a real function.
    // Confirmed live — this asymmetry is real, not a copy-paste.
    (oid::INT2, oid::OID, Oid(10038), 'f'),
    (oid::INT4, oid::OID, Oid(10039), 'b'),
    (oid::INT8, oid::OID, Oid(10037), 'f'),
    (oid::OID, oid::INT4, Oid(10041), 'b'),
    (oid::OID, oid::INT8, Oid(10040), 'f'),
    // ─── bool ───────────────────────────────────────────────────────────────
    (oid::BOOL, oid::INT4, Oid(10035), 'f'),
    (oid::INT4, oid::BOOL, Oid(10034), 'f'),
    // ─── bytea ──────────────────────────────────────────────────────────────
    (oid::INT2, oid::BYTEA, Oid(10143), 'f'),
    (oid::INT4, oid::BYTEA, Oid(10144), 'f'),
    (oid::INT8, oid::BYTEA, Oid(10145), 'f'),
    (oid::BYTEA, oid::INT2, Oid(10146), 'f'),
    (oid::BYTEA, oid::INT4, Oid(10147), 'f'),
    (oid::BYTEA, oid::INT8, Oid(10148), 'f'),
    // ─── string <-> string ──────────────────────────────────────────────────
    //
    // text/varchar and text/bpchar are binary-coercible; bpchar -> text/
    // varchar runs a real function that strips blank padding. Confirmed live
    // — this is the one place `castmethod` alone (not `castcontext`) exposes
    // an asymmetry `cast.rs`'s `CastKind` cannot represent.
    (oid::TEXT, oid::VARCHAR, Oid(10126), 'b'),
    (oid::TEXT, oid::BPCHAR, Oid(10125), 'b'),
    (oid::VARCHAR, oid::TEXT, Oid(10129), 'b'),
    (oid::VARCHAR, oid::BPCHAR, Oid(10130), 'b'),
    (oid::BPCHAR, oid::TEXT, Oid(10127), 'f'),
    (oid::BPCHAR, oid::VARCHAR, Oid(10128), 'f'),
    // ─── name ───────────────────────────────────────────────────────────────
    (oid::NAME, oid::TEXT, Oid(10134), 'f'),
    (oid::TEXT, oid::NAME, Oid(10140), 'f'),
    (oid::VARCHAR, oid::NAME, Oid(10142), 'f'),
    (oid::BPCHAR, oid::NAME, Oid(10141), 'f'),
    // ─── "char" ─────────────────────────────────────────────────────────────
    (oid::CHAR, oid::TEXT, Oid(10131), 'f'),
    (oid::INT4, oid::CHAR, Oid(10150), 'f'),
    // ─── date / time ────────────────────────────────────────────────────────
    (oid::DATE, oid::TIMESTAMP, Oid(10158), 'f'),
    (oid::DATE, oid::TIMESTAMPTZ, Oid(10159), 'f'),
    (oid::TIMESTAMP, oid::TIMESTAMPTZ, Oid(10164), 'f'),
    (oid::TIMESTAMPTZ, oid::TIMESTAMP, Oid(10167), 'f'),
    (oid::TIMESTAMP, oid::DATE, Oid(10162), 'f'),
    (oid::TIMESTAMPTZ, oid::DATE, Oid(10165), 'f'),
    (oid::TIMESTAMP, oid::TIME, Oid(10163), 'f'),
    (oid::TIMESTAMPTZ, oid::TIME, Oid(10166), 'f'),
    (oid::TIMESTAMPTZ, oid::TIMETZ, Oid(10168), 'f'),
    (oid::TIME, oid::INTERVAL, Oid(10160), 'f'),
    (oid::INTERVAL, oid::TIME, Oid(10169), 'f'),
    (oid::TIME, oid::TIMETZ, Oid(10161), 'f'),
    (oid::TIMETZ, oid::TIME, Oid(10170), 'f'),
    // ─── json ───────────────────────────────────────────────────────────────
    //
    // json <-> jsonb runs through each type's own I/O routines, not a
    // dedicated function — the only 'i'-method pair in this table.
    (oid::JSON, oid::JSONB, Oid(10220), 'i'),
    (oid::JSONB, oid::JSON, Oid(10221), 'i'),
    (oid::JSONB, oid::BOOL, Oid(10222), 'f'),
    (oid::JSONB, oid::INT2, Oid(10224), 'f'),
    (oid::JSONB, oid::INT4, Oid(10225), 'f'),
    (oid::JSONB, oid::INT8, Oid(10226), 'f'),
    (oid::JSONB, oid::FLOAT4, Oid(10227), 'f'),
    (oid::JSONB, oid::FLOAT8, Oid(10228), 'f'),
    (oid::JSONB, oid::NUMERIC, Oid(10223), 'f'),
];

/// One resolved `pg_cast` row: a [`CastPair`] plus the `castcontext`
/// [`cast_kind`] reports for it.
struct CastRow {
    oid: Oid,
    source: Oid,
    target: Oid,
    castcontext: char,
    castmethod: char,
}

fn resolved_rows() -> Vec<CastRow> {
    CAST_ROWS
        .iter()
        .map(|&(source, target, oid, castmethod)| {
            let castcontext = cast_kind(source, target)
                .unwrap_or_else(|| {
                    panic!(
                        "pg_cast row ({source}, {target}) has no cast_kind — CAST_ROWS and \
                         cast.rs's builtin_pg_cast have drifted apart"
                    )
                })
                .as_char();
            CastRow {
                oid,
                source,
                target,
                castcontext,
                castmethod,
            }
        })
        .collect()
}

/// `pg_cast.castfunc` for every function-based (`castmethod = 'f'`) cast this
/// relation reports, keyed by the cast's own `pg_cast.oid`.
///
/// These are real, fixed oids assigned by PostgreSQL's catalog bootstrap
/// (`genbki`) — the same class of value [`crate::pg_am`]'s `amhandler` and
/// [`crate::pg_type`]'s `typinput` carry, and identical on every
/// installation. Every entry was produced by, and is re-verified on every
/// run against a live server by, `catalog_fidelity`'s `diff_static_rows`.
///
/// A cast absent from this table has `castfunc = 0`, which is the real value
/// for a `'b'`-method (binary-coercible) or `'i'` (inout) row.
const CASTFUNC: &[(u32, u32)] = &[
    (10000, 714),  // bigint -> smallint (int2)
    (10001, 480),  // bigint -> integer (int4)
    (10002, 652),  // bigint -> real (float4)
    (10003, 482),  // bigint -> double precision (float8)
    (10004, 1781), // bigint -> numeric (numeric)
    (10005, 754),  // smallint -> bigint (int8)
    (10006, 313),  // smallint -> integer (int4)
    (10007, 236),  // smallint -> real (float4)
    (10008, 235),  // smallint -> double precision (float8)
    (10009, 1782), // smallint -> numeric (numeric)
    (10010, 481),  // integer -> bigint (int8)
    (10011, 314),  // integer -> smallint (int2)
    (10012, 318),  // integer -> real (float4)
    (10013, 316),  // integer -> double precision (float8)
    (10014, 1740), // integer -> numeric (numeric)
    (10015, 653),  // real -> bigint (int8)
    (10016, 238),  // real -> smallint (int2)
    (10017, 319),  // real -> integer (int4)
    (10018, 311),  // real -> double precision (float8)
    (10019, 1742), // real -> numeric (numeric)
    (10020, 483),  // double precision -> bigint (int8)
    (10021, 237),  // double precision -> smallint (int2)
    (10022, 317),  // double precision -> integer (int4)
    (10023, 312),  // double precision -> real (float4)
    (10024, 1743), // double precision -> numeric (numeric)
    (10025, 1779), // numeric -> bigint (int8)
    (10026, 1783), // numeric -> smallint (int2)
    (10027, 1744), // numeric -> integer (int4)
    (10028, 1745), // numeric -> real (float4)
    (10029, 1746), // numeric -> double precision (float8)
    (10034, 2557), // integer -> boolean (bool)
    (10035, 2558), // boolean -> integer (int4)
    (10037, 1287), // bigint -> oid (oid)
    (10038, 313),  // smallint -> oid (int4)
    (10040, 1288), // oid -> bigint (int8)
    (10127, 401),  // character -> text (text)
    (10128, 401),  // character -> character varying (text)
    (10131, 946),  // "char" -> text (text)
    (10134, 406),  // name -> text (text)
    (10140, 407),  // text -> name (name)
    (10141, 409),  // character -> name (name)
    (10142, 1400), // character varying -> name (name)
    (10143, 6367), // smallint -> bytea (bytea)
    (10144, 6368), // integer -> bytea (bytea)
    (10145, 6369), // bigint -> bytea (bytea)
    (10146, 6370), // bytea -> smallint (int2)
    (10147, 6371), // bytea -> integer (int4)
    (10148, 6372), // bytea -> bigint (int8)
    (10150, 78),   // integer -> "char" (char)
    (10158, 2024), // date -> timestamp without time zone (timestamp)
    (10159, 1174), // date -> timestamp with time zone (timestamptz)
    (10160, 1370), // time without time zone -> interval (interval)
    (10161, 2047), // time without time zone -> time with time zone (timetz)
    (10162, 2029), // timestamp without time zone -> date (date)
    (10163, 1316), // timestamp without time zone -> time without time zone (time)
    (10164, 2028), // timestamp without time zone -> timestamp with time zone (timestamptz)
    (10165, 1178), // timestamp with time zone -> date (date)
    (10166, 2019), // timestamp with time zone -> time without time zone (time)
    (10167, 2027), // timestamp with time zone -> timestamp without time zone (timestamp)
    (10168, 1388), // timestamp with time zone -> time with time zone (timetz)
    (10169, 1419), // interval -> time without time zone (time)
    (10170, 2046), // time with time zone -> time without time zone (time)
    (10222, 3556), // jsonb -> boolean (bool)
    (10223, 3449), // jsonb -> numeric (numeric)
    (10224, 3450), // jsonb -> smallint (int2)
    (10225, 3451), // jsonb -> integer (int4)
    (10226, 3452), // jsonb -> bigint (int8)
    (10227, 3453), // jsonb -> real (float4)
    (10228, 2580), // jsonb -> double precision (float8)
];

/// `pg_cast.castfunc` for the cast whose `pg_cast.oid` is `oid` — see
/// [`CASTFUNC`].
fn castfunc(oid: Oid) -> Oid {
    CASTFUNC
        .iter()
        .find(|(cast_oid, _)| *cast_oid == oid.get())
        .map(|(_, func)| Oid(*func))
        .unwrap_or(Oid::INVALID)
}

/// This row's value for `column`, or `None` if `column` is not one of this
/// relation's columns.
fn value(row: &CastRow, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(row.oid),
        "castsource" => Value::Oid(row.source),
        "casttarget" => Value::Oid(row.target),
        "castfunc" => Value::Oid(castfunc(row.oid)),
        "castcontext" => Value::Text(row.castcontext.to_string()),
        "castmethod" => Value::Text(row.castmethod.to_string()),
        _ => return None,
    })
}

/// `pg_catalog.pg_cast`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgCast;

impl PgCast {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("castsource", DataType::UInt32, false),
            Field::new("casttarget", DataType::UInt32, false),
            Field::new("castfunc", DataType::UInt32, false),
            Field::new("castcontext", DataType::Utf8, false),
            Field::new("castmethod", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgCast {
    fn name(&self) -> &str {
        "pg_cast"
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
                    relation: "pg_cast",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<CastRow> = resolved_rows()
            .into_iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(r, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let sources: UInt32Array = rows.iter().map(|r| r.source.get()).collect();
        let targets: UInt32Array = rows.iter().map(|r| r.target.get()).collect();
        let funcs: UInt32Array = rows.iter().map(|r| castfunc(r.oid).get()).collect();
        let contexts: StringArray = rows
            .iter()
            .map(|r| Some(r.castcontext.to_string()))
            .collect();
        let methods: StringArray = rows
            .iter()
            .map(|r| Some(r.castmethod.to_string()))
            .collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(sources),
                Arc::new(targets),
                Arc::new(funcs),
                Arc::new(contexts),
                Arc::new(methods),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

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

    fn row_for(batch: &RecordBatch, source: u32, target: u32) -> usize {
        let sources = col_u32(batch, "castsource");
        let targets = col_u32(batch, "casttarget");
        sources
            .iter()
            .zip(targets.iter())
            .position(|(&s, &t)| s == source && t == target)
            .unwrap_or_else(|| panic!("no pg_cast row for {source} -> {target}"))
    }

    #[test]
    fn name_is_pg_cast() {
        assert_eq!(PgCast.name(), "pg_cast");
    }

    #[test]
    fn schema_matches_the_documented_column_set() {
        let schema = PgCast.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "castsource",
                "casttarget",
                "castfunc",
                "castcontext",
                "castmethod",
            ]
        );
    }

    /// Pins that `oid` (real `attnum` 1) is not nullable and comes before
    /// every other column — a future edit cannot silently drop it back to
    /// the wrong position the way this relation previously had it missing
    /// entirely. See module docs on where the live oids come from.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgCast.schema();
        let got: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", false),
                ("castsource", false),
                ("casttarget", false),
                ("castfunc", false),
                ("castcontext", false),
                ("castmethod", false),
            ]
        );
    }

    /// Every row's `oid` is nonzero and unique — real `pg_cast.oid` is a
    /// primary key, and a row left with `Oid::INVALID` (`0`) would be
    /// indistinguishable from one whose oid was never set.
    #[test]
    fn every_row_has_a_unique_nonzero_oid() {
        let batch = PgCast.scan(&MockCatalog::new(), &[]).unwrap();
        let oids = col_u32(&batch, "oid");
        assert!(oids.iter().all(|&o| o != 0));
        let unique: std::collections::HashSet<u32> = oids.iter().copied().collect();
        assert_eq!(unique.len(), oids.len());
    }

    /// No two rows in [`CAST_ROWS`] disagree with what `cast_kind` itself
    /// says, and every row's `cast_kind` call actually succeeds — pins that
    /// [`resolved_rows`] never silently drops a row via its `unwrap_or_else`
    /// panic path, and that `pg_cast.castcontext` cannot drift from the
    /// function the planner calls to decide coercion legality.
    #[test]
    fn every_row_agrees_with_cast_kind() {
        for &(source, target, _, _) in CAST_ROWS {
            let kind = cast_kind(source, target)
                .unwrap_or_else(|| panic!("{source} -> {target} has no cast_kind"));
            let batch = PgCast.scan(&MockCatalog::new(), &[]).unwrap();
            let i = row_for(&batch, source.get(), target.get());
            assert_eq!(
                col_str(&batch, "castcontext")[i],
                kind.as_char().to_string()
            );
        }
    }

    /// The three `castcontext` categories `cast.rs`'s module docs describe
    /// all actually appear: implicit widening, assignment-only narrowing, and
    /// explicit-only conversion.
    #[test]
    fn castcontext_covers_all_three_categories() {
        let batch = PgCast.scan(&MockCatalog::new(), &[]).unwrap();

        // int2 -> int4: implicit widening.
        let i = row_for(&batch, oid::INT2.get(), oid::INT4.get());
        assert_eq!(col_str(&batch, "castcontext")[i], "i");

        // int8 -> int4: assignment-only narrowing.
        let i = row_for(&batch, oid::INT8.get(), oid::INT4.get());
        assert_eq!(col_str(&batch, "castcontext")[i], "a");

        // int4 -> bool: explicit-only.
        let i = row_for(&batch, oid::INT4.get(), oid::BOOL.get());
        assert_eq!(col_str(&batch, "castcontext")[i], "e");
    }

    /// `int4 <-> oid` are binary-coercible in real Postgres — no cast
    /// function at all — while `int2 -> oid` still runs one. Confirmed live;
    /// pins that `castmethod` is not just a constant `'f'` for every row.
    #[test]
    fn castmethod_distinguishes_binary_coercible_from_function_casts() {
        let batch = PgCast.scan(&MockCatalog::new(), &[]).unwrap();

        let i = row_for(&batch, oid::INT4.get(), oid::OID.get());
        assert_eq!(col_str(&batch, "castmethod")[i], "b");

        let i = row_for(&batch, oid::INT2.get(), oid::OID.get());
        assert_eq!(col_str(&batch, "castmethod")[i], "f");

        let i = row_for(&batch, oid::JSON.get(), oid::JSONB.get());
        assert_eq!(col_str(&batch, "castmethod")[i], "i");
    }

    /// The entire point of this crate: a predicate on `castsource` AND
    /// `casttarget` together narrows to exactly one row, mirroring
    /// `(castsource, casttarget)` being a real unique constraint.
    #[test]
    fn pushed_predicates_on_source_and_target_narrow_to_one_row() {
        let full = PgCast.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(full.num_rows() > 1, "sanity: pg_cast has more than one row");

        let filtered = PgCast
            .scan(
                &MockCatalog::new(),
                &[
                    Predicate::eq("castsource", oid::INT2),
                    Predicate::eq("casttarget", oid::INT4),
                ],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "castcontext"), vec!["i".to_string()]);
    }

    /// Every `'f'`-method (function-based) cast reports a real, non-zero
    /// `castfunc`, and every `'b'`/`'i'`-method one reports `0` — exactly
    /// what a live server does. See the module docs; `catalog_fidelity`
    /// checks the individual oids against the server.
    #[test]
    fn castfunc_is_nonzero_exactly_for_function_based_casts() {
        let batch = PgCast.scan(&MockCatalog::new(), &[]).unwrap();
        let methods = col_str(&batch, "castmethod");
        let funcs = col_u32(&batch, "castfunc");
        assert_eq!(methods.len(), funcs.len());
        let mut function_based = 0;
        for (method, func) in methods.iter().zip(&funcs) {
            if method == "f" {
                assert_ne!(*func, 0, "an 'f'-method cast must name a function");
                function_based += 1;
            } else {
                assert_eq!(*func, 0, "a '{method}'-method cast has no function");
            }
        }
        assert_eq!(
            function_based,
            CASTFUNC.len(),
            "every CASTFUNC entry belongs to a row this relation reports, and              every 'f'-method row has an entry"
        );
    }

    /// A predicate matching nothing returns zero rows, not everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgCast
            .scan(
                &MockCatalog::new(),
                &[Predicate::eq("castsource", Oid(999_999))],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column this relation does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgCast
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_cast",
                column: "nope".to_string(),
            }
        );
    }
}
