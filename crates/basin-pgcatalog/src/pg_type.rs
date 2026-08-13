//! `pg_catalog.pg_type`, fully determined by `basin-pgtype`'s own OID table.
//!
//! This is the first relation implemented in this crate, and it needs no
//! [`CatalogSource`] at all: every builtin type Basin represents has a fixed,
//! well-known OID (`crates/basin-pgtype/src/oid.rs`), and `pg_type`'s row for
//! a builtin is static data, not a catalog query. User-defined types (a
//! `CREATE TYPE`, or a table's implicit row type) are out of scope for this
//! increment — see the crate docs.
//!
//! # Where these values come from
//!
//! Every row below was checked against a live PostgreSQL 18 `pg_type`, not
//! recalled from memory — this project has repeatedly found recall wrong and
//! the server right (see `crates/basin-pgtype/src/operator.rs`'s module docs
//! for the precedent). The query, which is also what
//! `tests/catalog_fidelity.rs`'s `diff_static_rows` re-runs on every
//! live-server test run and diffs cell by cell:
//!
//! ```sql
//! SELECT oid, typname, typnamespace, typowner, typlen, typbyval, typtype,
//!        typcategory, typispreferred, typisdefined, typdelim, typrelid,
//!        typsubscript, typelem, typarray, typinput, typoutput, typreceive,
//!        typsend, typmodin, typmodout, typanalyze, typalign, typstorage,
//!        typnotnull, typbasetype, typtypmod, typndims, typcollation,
//!        typdefaultbin, typdefault, typacl
//!   FROM pg_type
//!  WHERE oid IN (16,17,18,19,20,21,23,25,26,114,142,700,701,1042,1043,1082,
//!                1083,1114,1184,1186,1266,1700,2950,3802,705,2278,2249,
//!                1000,1001,1002,1003,1005,1007,1009,1014,1015,1016,1021,
//!                1022,1028,1182,1183,1115,1185,1187,1231,199,3807,2951)
//!  ORDER BY oid;
//! ```
//!
//! (the `IN` list is exactly the builtin scalar, pseudo, and array OIDs
//! `basin-pgtype::oid` names constants for). Re-run this before editing
//! [`TYPES`] — or better, just run `catalog_fidelity` with `PG_DIFF_TEST_DSN`
//! set, which does it for you and fails on any disagreement.
//!
//! Two OIDs appear as `typarray`/`typelem` values below without a matching
//! named constant in `basin-pgtype::oid`, because Basin does not represent a
//! `pg_type` row for them (they are not builtins this crate reports as their
//! own row, only as another row's `typelem`/`typarray` field): `xml`'s
//! `typarray` is `143`, and `record`'s `typarray` is `2287`. `timetz`'s
//! `typarray` (`1270`) is the same situation. These are real Postgres OIDs,
//! confirmed live, not invented — they are simply not (yet) rows of their
//! own here.
//!
//! # The function-oid columns point at functions Basin does not implement
//!
//! `typinput`/`typoutput`/`typreceive`/`typsend`/`typmodin`/`typmodout`/
//! `typanalyze`/`typsubscript` are `regproc`s — `pg_proc.oid`s. The values
//! here are the *real* oids PostgreSQL's own catalog bootstrap (`genbki`)
//! assigns, fixed across every installation and verified live for all 49
//! rows, exactly like [`crate::pg_am`]'s `amhandler`. Basin implements none
//! of those C functions and [`crate::pg_proc`] has no row for any of them, so
//! a join from `pg_type` to `pg_proc` on these columns finds nothing here
//! where a real server would find the handler. That is a real, admitted gap —
//! but reporting `0` instead would be worse: `0` means "this type has no
//! input function", which is false of every one of these types and would tell
//! a client the type is unusable. Same call [`crate::pg_am`] already made.
//!
//! # The columns that are uniform across all 49 rows
//!
//! `typowner` (`10`), `typisdefined` (`true`), `typdelim` (`','`),
//! `typrelid` (`0`), `typnotnull` (`false`), `typbasetype` (`0`),
//! `typtypmod` (`-1`), `typndims` (`0`), and the three nullable columns
//! `typdefaultbin`/`typdefault`/`typacl` (all `NULL`) take the same value for
//! every row Basin reports — live-verified, not assumed (the query above
//! returns exactly one distinct value for each). They are module constants
//! rather than per-row fields so that the per-row table stays readable, and
//! `catalog_fidelity`'s `diff_static_rows` checks them per row anyway.
//!
//! `typnamespace` is [`crate::PG_CATALOG_NAMESPACE`] (`11`) for the same
//! reason: every type here is a builtin.

use std::sync::Arc;

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::{oid, Oid};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// `pg_type.typowner` — the bootstrap superuser, for every builtin. Live-
/// verified as the only distinct value across all 49 rows.
const BUILTIN_TYPE_OWNER: Oid = Oid(10);
/// `pg_type.typisdefined` — a shell type (`CREATE TYPE t;` with no body) would
/// report `false`; Basin has no shell types, and every builtin is defined.
const TYPISDEFINED: bool = true;
/// `pg_type.typdelim` — the character that separates values in this type's
/// external array representation. `,` for every type Basin reports (only
/// `box` uses `;` in stock Postgres, and Basin has no `box`).
const TYPDELIM: char = ',';
/// `pg_type.typrelid` — the `pg_class.oid` of the composite type's relation.
/// `0` (not a composite type) for every row Basin reports; Basin's `pg_type`
/// is builtins-only and has no composite/row types.
const TYPRELID: Oid = Oid::INVALID;
/// `pg_type.typnotnull` — only ever `true` for a domain with a `NOT NULL`
/// constraint. Basin has no domains.
const TYPNOTNULL: bool = false;
/// `pg_type.typbasetype` — the domain's underlying type. `0` (not a domain)
/// for every row Basin reports.
const TYPBASETYPE: Oid = Oid::INVALID;
/// `pg_type.typtypmod` — the domain's declared typmod. `-1` (none) for every
/// row Basin reports; only domains ever carry one.
const TYPTYPMOD: i32 = -1;
/// `pg_type.typndims` — the domain's declared array dimension count. `0`
/// (not an array domain) for every row Basin reports.
const TYPNDIMS: i32 = 0;

/// One row of `pg_type`, carrying every column this relation reports that is
/// not one of the module-constant-valued ones documented above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypeRow {
    oid: Oid,
    typname: &'static str,
    typlen: i16,
    typbyval: bool,
    typtype: char,
    typcategory: char,
    typispreferred: bool,
    typsubscript: Oid,
    typelem: Oid,
    typarray: Oid,
    typinput: Oid,
    typoutput: Oid,
    typreceive: Oid,
    typsend: Oid,
    typmodin: Oid,
    typmodout: Oid,
    typanalyze: Oid,
    typalign: char,
    typstorage: char,
    typcollation: Oid,
}

impl TypeRow {
    /// A builtin scalar or pseudo-type row.
    ///
    /// The four I/O function oids are required parameters rather than
    /// defaulted, because `0` is never the real value for any of them — every
    /// Postgres type has an input, output, receive and send function, so a
    /// defaulted `0` would be a claim ("this type cannot be parsed or
    /// serialized") that is false of every row here.
    ///
    /// The remaining columns default to the value that is correct for the
    /// plurality of rows and are overridden by the `const fn` setters below
    /// where the real value differs: not byval, not preferred, no subscript
    /// handler, no typmod I/O, no custom analyze function, `'i'` alignment,
    /// `'p'` (plain) storage, no collation. Nothing here is trusted on the
    /// strength of that default alone — `catalog_fidelity`'s
    /// `diff_static_rows` diffs all 32 columns of all 49 rows against a live
    /// server on every run with `PG_DIFF_TEST_DSN` set, so a wrong default
    /// cannot survive one.
    #[allow(clippy::too_many_arguments)]
    const fn new(
        oid: Oid,
        typname: &'static str,
        typlen: i16,
        typtype: char,
        typcategory: char,
        typelem: Oid,
        typarray: Oid,
        typinput: Oid,
        typoutput: Oid,
        typreceive: Oid,
        typsend: Oid,
    ) -> TypeRow {
        TypeRow {
            oid,
            typname,
            typlen,
            typbyval: false,
            typtype,
            typcategory,
            typispreferred: false,
            typsubscript: Oid::INVALID,
            typelem,
            typarray,
            typinput,
            typoutput,
            typreceive,
            typsend,
            typmodin: Oid::INVALID,
            typmodout: Oid::INVALID,
            typanalyze: Oid::INVALID,
            typalign: 'i',
            typstorage: 'p',
            typcollation: Oid::INVALID,
        }
    }

    /// A builtin array (`_t`) row. Live-verified: every one of the 22 array
    /// rows Basin reports shares `typlen = -1`, `typbyval = false`,
    /// `typtype = 'b'`, `typcategory = 'A'`, `typispreferred = false`,
    /// `typsubscript = 6179` (`array_subscript_handler`), `typarray = 0`
    /// (Postgres does not nest array *types* — see
    /// `basin-pgtype::oid::array_of`'s own docs), `typinput = 750`
    /// (`array_in`), `typoutput = 751` (`array_out`), `typreceive = 2400`
    /// (`array_recv`), `typsend = 2401` (`array_send`), `typanalyze = 3816`
    /// (`array_typanalyze`) and `typstorage = 'x'` (extended). Only
    /// `typelem`, the typmod I/O pair, `typalign` and `typcollation` vary,
    /// and each of those four is inherited from the element type.
    const fn array(
        oid: Oid,
        typname: &'static str,
        typelem: Oid,
        typmodin: Oid,
        typmodout: Oid,
        typalign: char,
        typcollation: Oid,
    ) -> TypeRow {
        TypeRow {
            oid,
            typname,
            typlen: -1,
            typbyval: false,
            typtype: 'b',
            typcategory: 'A',
            typispreferred: false,
            typsubscript: Oid(6179),
            typelem,
            typarray: Oid::INVALID,
            typinput: Oid(750),
            typoutput: Oid(751),
            typreceive: Oid(2400),
            typsend: Oid(2401),
            typmodin,
            typmodout,
            typanalyze: Oid(3816),
            typalign,
            typstorage: 'x',
            typcollation,
        }
    }

    /// `typbyval = true` — the type is passed by value, not by reference.
    const fn byval(mut self) -> TypeRow {
        self.typbyval = true;
        self
    }

    /// `typispreferred = true` — the type is its category's preferred target
    /// for implicit coercion.
    const fn preferred(mut self) -> TypeRow {
        self.typispreferred = true;
        self
    }

    /// `typalign` — `'c'` (char), `'s'` (int2), `'i'` (int4) or `'d'`
    /// (double) storage alignment.
    const fn align(mut self, typalign: char) -> TypeRow {
        self.typalign = typalign;
        self
    }

    /// `typstorage` — `'p'` (plain), `'e'` (external), `'m'` (main) or `'x'`
    /// (extended) TOAST strategy.
    const fn storage(mut self, typstorage: char) -> TypeRow {
        self.typstorage = typstorage;
        self
    }

    /// `typcollation` — `100` (`default`) or `950` (`C`) for the collatable
    /// types; `0` for everything else.
    const fn collation(mut self, typcollation: Oid) -> TypeRow {
        self.typcollation = typcollation;
        self
    }

    /// `typsubscript` — the subscripting handler function's oid, for the
    /// types that support `x[i]`.
    const fn subscript(mut self, typsubscript: Oid) -> TypeRow {
        self.typsubscript = typsubscript;
        self
    }

    /// `typmodin`/`typmodout` — the typmod parse/print functions, for the
    /// types that take a modifier (`varchar(n)`, `numeric(p,s)`,
    /// `timestamp(p)`, ...).
    const fn typmod_io(mut self, typmodin: Oid, typmodout: Oid) -> TypeRow {
        self.typmodin = typmodin;
        self.typmodout = typmodout;
        self
    }

    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's scalar-`Value`-representable columns. The three
    /// always-`NULL` columns (`typdefaultbin`, `typdefault`, `typacl`) have
    /// no scalar value and are handled by the caller.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "oid" => Value::Oid(self.oid),
            "typname" => Value::Text(self.typname.to_string()),
            "typnamespace" => Value::Oid(crate::PG_CATALOG_NAMESPACE),
            "typowner" => Value::Oid(BUILTIN_TYPE_OWNER),
            "typlen" => Value::Int(self.typlen as i64),
            "typbyval" => Value::Bool(self.typbyval),
            "typtype" => Value::Text(self.typtype.to_string()),
            "typcategory" => Value::Text(self.typcategory.to_string()),
            "typispreferred" => Value::Bool(self.typispreferred),
            "typisdefined" => Value::Bool(TYPISDEFINED),
            "typdelim" => Value::Text(TYPDELIM.to_string()),
            "typrelid" => Value::Oid(TYPRELID),
            "typsubscript" => Value::Oid(self.typsubscript),
            "typelem" => Value::Oid(self.typelem),
            "typarray" => Value::Oid(self.typarray),
            "typinput" => Value::Oid(self.typinput),
            "typoutput" => Value::Oid(self.typoutput),
            "typreceive" => Value::Oid(self.typreceive),
            "typsend" => Value::Oid(self.typsend),
            "typmodin" => Value::Oid(self.typmodin),
            "typmodout" => Value::Oid(self.typmodout),
            "typanalyze" => Value::Oid(self.typanalyze),
            "typalign" => Value::Text(self.typalign.to_string()),
            "typstorage" => Value::Text(self.typstorage.to_string()),
            "typnotnull" => Value::Bool(TYPNOTNULL),
            "typbasetype" => Value::Oid(TYPBASETYPE),
            "typtypmod" => Value::Int(TYPTYPMOD as i64),
            "typndims" => Value::Int(TYPNDIMS as i64),
            "typcollation" => Value::Oid(self.typcollation),
            _ => return None,
        })
    }
}

/// Every builtin `pg_type` row Basin can currently represent. See the module
/// docs for the query used to verify each one, and `catalog_fidelity`'s
/// `diff_static_rows` for the check that re-runs it.
static TYPES: &[TypeRow] = &[
    // ─── Scalar builtins ────────────────────────────────────────────
    TypeRow::new(
        oid::BOOL,
        "bool",
        1,
        'b',
        'B',
        Oid::INVALID,
        oid::BOOL_ARRAY,
        Oid(1242),
        Oid(1243),
        Oid(2436),
        Oid(2437),
    )
    .byval()
    .preferred()
    .align('c'),
    TypeRow::new(
        oid::BYTEA,
        "bytea",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::BYTEA_ARRAY,
        Oid(1244),
        Oid(31),
        Oid(2412),
        Oid(2413),
    )
    .storage('x'),
    TypeRow::new(
        oid::CHAR,
        "char",
        1,
        'b',
        'Z',
        Oid::INVALID,
        oid::CHAR_ARRAY,
        Oid(1245),
        Oid(33),
        Oid(2434),
        Oid(2435),
    )
    .byval()
    .align('c'),
    TypeRow::new(
        oid::NAME,
        "name",
        64,
        'b',
        'S',
        oid::CHAR,
        oid::NAME_ARRAY,
        Oid(34),
        Oid(35),
        Oid(2422),
        Oid(2423),
    )
    .align('c')
    .collation(Oid(950))
    .subscript(Oid(6180)),
    TypeRow::new(
        oid::INT8,
        "int8",
        8,
        'b',
        'N',
        Oid::INVALID,
        oid::INT8_ARRAY,
        Oid(460),
        Oid(461),
        Oid(2408),
        Oid(2409),
    )
    .byval()
    .align('d'),
    TypeRow::new(
        oid::INT2,
        "int2",
        2,
        'b',
        'N',
        Oid::INVALID,
        oid::INT2_ARRAY,
        Oid(38),
        Oid(39),
        Oid(2404),
        Oid(2405),
    )
    .byval()
    .align('s'),
    TypeRow::new(
        oid::INT4,
        "int4",
        4,
        'b',
        'N',
        Oid::INVALID,
        oid::INT4_ARRAY,
        Oid(42),
        Oid(43),
        Oid(2406),
        Oid(2407),
    )
    .byval(),
    TypeRow::new(
        oid::TEXT,
        "text",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::TEXT_ARRAY,
        Oid(46),
        Oid(47),
        Oid(2414),
        Oid(2415),
    )
    .preferred()
    .storage('x')
    .collation(Oid(100)),
    TypeRow::new(
        oid::OID,
        "oid",
        4,
        'b',
        'N',
        Oid::INVALID,
        oid::OID_ARRAY,
        Oid(1798),
        Oid(1799),
        Oid(2418),
        Oid(2419),
    )
    .byval()
    .preferred(),
    TypeRow::new(
        oid::JSON,
        "json",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::JSON_ARRAY,
        Oid(321),
        Oid(322),
        Oid(323),
        Oid(324),
    )
    .storage('x'),
    TypeRow::new(
        oid::XML,
        "xml",
        -1,
        'b',
        'U',
        Oid::INVALID,
        Oid(143),
        Oid(2893),
        Oid(2894),
        Oid(2898),
        Oid(2899),
    )
    .storage('x'),
    TypeRow::new(
        oid::FLOAT4,
        "float4",
        4,
        'b',
        'N',
        Oid::INVALID,
        oid::FLOAT4_ARRAY,
        Oid(200),
        Oid(201),
        Oid(2424),
        Oid(2425),
    )
    .byval(),
    TypeRow::new(
        oid::FLOAT8,
        "float8",
        8,
        'b',
        'N',
        Oid::INVALID,
        oid::FLOAT8_ARRAY,
        Oid(214),
        Oid(215),
        Oid(2426),
        Oid(2427),
    )
    .byval()
    .preferred()
    .align('d'),
    TypeRow::new(
        oid::BPCHAR,
        "bpchar",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::BPCHAR_ARRAY,
        Oid(1044),
        Oid(1045),
        Oid(2430),
        Oid(2431),
    )
    .storage('x')
    .collation(Oid(100))
    .typmod_io(Oid(2913), Oid(2914)),
    TypeRow::new(
        oid::VARCHAR,
        "varchar",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::VARCHAR_ARRAY,
        Oid(1046),
        Oid(1047),
        Oid(2432),
        Oid(2433),
    )
    .storage('x')
    .collation(Oid(100))
    .typmod_io(Oid(2915), Oid(2916)),
    TypeRow::new(
        oid::DATE,
        "date",
        4,
        'b',
        'D',
        Oid::INVALID,
        oid::DATE_ARRAY,
        Oid(1084),
        Oid(1085),
        Oid(2468),
        Oid(2469),
    )
    .byval(),
    TypeRow::new(
        oid::TIME,
        "time",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIME_ARRAY,
        Oid(1143),
        Oid(1144),
        Oid(2470),
        Oid(2471),
    )
    .byval()
    .align('d')
    .typmod_io(Oid(2909), Oid(2910)),
    TypeRow::new(
        oid::TIMESTAMP,
        "timestamp",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIMESTAMP_ARRAY,
        Oid(1312),
        Oid(1313),
        Oid(2474),
        Oid(2475),
    )
    .byval()
    .align('d')
    .typmod_io(Oid(2905), Oid(2906)),
    TypeRow::new(
        oid::TIMESTAMPTZ,
        "timestamptz",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIMESTAMPTZ_ARRAY,
        Oid(1150),
        Oid(1151),
        Oid(2476),
        Oid(2477),
    )
    .byval()
    .preferred()
    .align('d')
    .typmod_io(Oid(2907), Oid(2908)),
    TypeRow::new(
        oid::INTERVAL,
        "interval",
        16,
        'b',
        'T',
        Oid::INVALID,
        oid::INTERVAL_ARRAY,
        Oid(1160),
        Oid(1161),
        Oid(2478),
        Oid(2479),
    )
    .preferred()
    .align('d')
    .typmod_io(Oid(2903), Oid(2904)),
    TypeRow::new(
        oid::TIMETZ,
        "timetz",
        12,
        'b',
        'D',
        Oid::INVALID,
        Oid(1270),
        Oid(1350),
        Oid(1351),
        Oid(2472),
        Oid(2473),
    )
    .align('d')
    .typmod_io(Oid(2911), Oid(2912)),
    TypeRow::new(
        oid::NUMERIC,
        "numeric",
        -1,
        'b',
        'N',
        Oid::INVALID,
        oid::NUMERIC_ARRAY,
        Oid(1701),
        Oid(1702),
        Oid(2460),
        Oid(2461),
    )
    .storage('m')
    .typmod_io(Oid(2917), Oid(2918)),
    TypeRow::new(
        oid::UUID,
        "uuid",
        16,
        'b',
        'U',
        Oid::INVALID,
        oid::UUID_ARRAY,
        Oid(2952),
        Oid(2953),
        Oid(2961),
        Oid(2962),
    )
    .align('c'),
    TypeRow::new(
        oid::JSONB,
        "jsonb",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::JSONB_ARRAY,
        Oid(3806),
        Oid(3804),
        Oid(3805),
        Oid(3803),
    )
    .storage('x')
    .subscript(Oid(6098)),
    // ─── Pseudo-types ───────────────────────────────────────────────
    TypeRow::new(
        oid::UNKNOWN,
        "unknown",
        -2,
        'p',
        'X',
        Oid::INVALID,
        Oid::INVALID,
        Oid(109),
        Oid(110),
        Oid(2416),
        Oid(2417),
    )
    .align('c'),
    TypeRow::new(
        oid::VOID,
        "void",
        4,
        'p',
        'P',
        Oid::INVALID,
        Oid::INVALID,
        Oid(2298),
        Oid(2299),
        Oid(3120),
        Oid(3121),
    )
    .byval(),
    TypeRow::new(
        oid::RECORD,
        "record",
        -1,
        'p',
        'P',
        Oid::INVALID,
        Oid(2287),
        Oid(2290),
        Oid(2291),
        Oid(2402),
        Oid(2403),
    )
    .align('d')
    .storage('x'),
    // ─── Array builtins ─────────────────────────────────────────────
    TypeRow::array(
        oid::BOOL_ARRAY,
        "_bool",
        oid::BOOL,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::BYTEA_ARRAY,
        "_bytea",
        oid::BYTEA,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::CHAR_ARRAY,
        "_char",
        oid::CHAR,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::NAME_ARRAY,
        "_name",
        oid::NAME,
        Oid(0),
        Oid(0),
        'i',
        Oid(950),
    ),
    TypeRow::array(
        oid::INT2_ARRAY,
        "_int2",
        oid::INT2,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::INT4_ARRAY,
        "_int4",
        oid::INT4,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::TEXT_ARRAY,
        "_text",
        oid::TEXT,
        Oid(0),
        Oid(0),
        'i',
        Oid(100),
    ),
    TypeRow::array(
        oid::BPCHAR_ARRAY,
        "_bpchar",
        oid::BPCHAR,
        Oid(2913),
        Oid(2914),
        'i',
        Oid(100),
    ),
    TypeRow::array(
        oid::VARCHAR_ARRAY,
        "_varchar",
        oid::VARCHAR,
        Oid(2915),
        Oid(2916),
        'i',
        Oid(100),
    ),
    TypeRow::array(
        oid::INT8_ARRAY,
        "_int8",
        oid::INT8,
        Oid(0),
        Oid(0),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::FLOAT4_ARRAY,
        "_float4",
        oid::FLOAT4,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::FLOAT8_ARRAY,
        "_float8",
        oid::FLOAT8,
        Oid(0),
        Oid(0),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::OID_ARRAY,
        "_oid",
        oid::OID,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::DATE_ARRAY,
        "_date",
        oid::DATE,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::TIME_ARRAY,
        "_time",
        oid::TIME,
        Oid(2909),
        Oid(2910),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::TIMESTAMP_ARRAY,
        "_timestamp",
        oid::TIMESTAMP,
        Oid(2905),
        Oid(2906),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::TIMESTAMPTZ_ARRAY,
        "_timestamptz",
        oid::TIMESTAMPTZ,
        Oid(2907),
        Oid(2908),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::INTERVAL_ARRAY,
        "_interval",
        oid::INTERVAL,
        Oid(2903),
        Oid(2904),
        'd',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::NUMERIC_ARRAY,
        "_numeric",
        oid::NUMERIC,
        Oid(2917),
        Oid(2918),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::JSON_ARRAY,
        "_json",
        oid::JSON,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::JSONB_ARRAY,
        "_jsonb",
        oid::JSONB,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
    TypeRow::array(
        oid::UUID_ARRAY,
        "_uuid",
        oid::UUID,
        Oid(0),
        Oid(0),
        'i',
        Oid::INVALID,
    ),
];

/// The `pg_type` columns real Postgres copies into a new column's
/// `pg_attribute` row at column-creation time — see [`crate::pg_attribute`],
/// which is the only caller.
///
/// This is not a convenience: it is how the real catalog works.
/// `pg_attribute.attlen`/`attbyval`/`attalign`/`attstorage` are documented as
/// copies of the type's `typlen`/`typbyval`/`typalign`/`typstorage`, and
/// `attcollation` of its `typcollation`, taken at `CREATE TABLE` time. Basin
/// derives them the same way rather than inventing per-column values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttributeTypeInfo {
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: char,
    pub attstorage: char,
    pub attcollation: Oid,
    /// `typcategory == 'A'`. Real Postgres records `attndims = 1` for a
    /// column declared `t[]` and `0` for a non-array column (live-verified —
    /// see [`crate::pg_attribute`]'s module docs).
    pub is_array: bool,
}

/// The [`AttributeTypeInfo`] for `oid`, or `None` for an oid [`TYPES`] has no
/// row for.
pub(crate) fn attribute_type_info(oid: Oid) -> Option<AttributeTypeInfo> {
    TYPES
        .iter()
        .find(|r| r.oid == oid)
        .map(|r| AttributeTypeInfo {
            attlen: r.typlen,
            attbyval: r.typbyval,
            attalign: r.typalign,
            attstorage: r.typstorage,
            attcollation: r.typcollation,
            is_array: r.typcategory == 'A',
        })
}

/// `pg_catalog.pg_type`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgType;

impl PgType {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::UInt32, false),
            Field::new("typowner", DataType::UInt32, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typispreferred", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, false),
            Field::new("typrelid", DataType::UInt32, false),
            Field::new("typsubscript", DataType::UInt32, false),
            Field::new("typelem", DataType::UInt32, false),
            Field::new("typarray", DataType::UInt32, false),
            Field::new("typinput", DataType::UInt32, false),
            Field::new("typoutput", DataType::UInt32, false),
            Field::new("typreceive", DataType::UInt32, false),
            Field::new("typsend", DataType::UInt32, false),
            Field::new("typmodin", DataType::UInt32, false),
            Field::new("typmodout", DataType::UInt32, false),
            Field::new("typanalyze", DataType::UInt32, false),
            Field::new("typalign", DataType::Utf8, false),
            Field::new("typstorage", DataType::Utf8, false),
            Field::new("typnotnull", DataType::Boolean, false),
            Field::new("typbasetype", DataType::UInt32, false),
            Field::new("typtypmod", DataType::Int32, false),
            Field::new("typndims", DataType::Int32, false),
            Field::new("typcollation", DataType::UInt32, false),
            // `pg_node_tree` and `text`, both nullable and both always `NULL`
            // here: only a domain with a `DEFAULT` ever carries either, and
            // Basin has no domains.
            Field::new("typdefaultbin", DataType::Utf8, true),
            Field::new("typdefault", DataType::Utf8, true),
            // `aclitem[]`, nullable and always `NULL` — `NULL` is what a real
            // server reports for a type whose privileges have never been
            // `GRANT`ed or `REVOKE`d away from the built-in default, which is
            // every type here. Basin has no privilege system at all.
            Field::new(
                "typacl",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]))
    }
}

impl crate::SystemView for PgType {
    fn name(&self) -> &str {
        "pg_type"
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
                    relation: "pg_type",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&TypeRow> = TYPES
            .iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let n = rows.len();
        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let typnames: StringArray = rows.iter().map(|r| Some(r.typname)).collect();
        let typnamespaces: UInt32Array = rows
            .iter()
            .map(|_| crate::PG_CATALOG_NAMESPACE.get())
            .collect();
        let typowners: UInt32Array = rows.iter().map(|_| BUILTIN_TYPE_OWNER.get()).collect();
        let typlens: Int16Array = rows.iter().map(|r| r.typlen).collect();
        let typbyvals: BooleanArray = rows.iter().map(|r| r.typbyval).collect();
        let typtypes: StringArray = rows.iter().map(|r| Some(r.typtype.to_string())).collect();
        let typcategories: StringArray = rows
            .iter()
            .map(|r| Some(r.typcategory.to_string()))
            .collect();
        let typispreferreds: BooleanArray = rows.iter().map(|r| r.typispreferred).collect();
        let typisdefineds: BooleanArray = rows.iter().map(|_| TYPISDEFINED).collect();
        let typdelims: StringArray = rows.iter().map(|_| Some(TYPDELIM.to_string())).collect();
        let typrelids: UInt32Array = rows.iter().map(|_| TYPRELID.get()).collect();
        let typsubscripts: UInt32Array = rows.iter().map(|r| r.typsubscript.get()).collect();
        let typelems: UInt32Array = rows.iter().map(|r| r.typelem.get()).collect();
        let typarrays: UInt32Array = rows.iter().map(|r| r.typarray.get()).collect();
        let typinputs: UInt32Array = rows.iter().map(|r| r.typinput.get()).collect();
        let typoutputs: UInt32Array = rows.iter().map(|r| r.typoutput.get()).collect();
        let typreceives: UInt32Array = rows.iter().map(|r| r.typreceive.get()).collect();
        let typsends: UInt32Array = rows.iter().map(|r| r.typsend.get()).collect();
        let typmodins: UInt32Array = rows.iter().map(|r| r.typmodin.get()).collect();
        let typmodouts: UInt32Array = rows.iter().map(|r| r.typmodout.get()).collect();
        let typanalyzes: UInt32Array = rows.iter().map(|r| r.typanalyze.get()).collect();
        let typaligns: StringArray = rows.iter().map(|r| Some(r.typalign.to_string())).collect();
        let typstorages: StringArray = rows
            .iter()
            .map(|r| Some(r.typstorage.to_string()))
            .collect();
        let typnotnulls: BooleanArray = rows.iter().map(|_| TYPNOTNULL).collect();
        let typbasetypes: UInt32Array = rows.iter().map(|_| TYPBASETYPE.get()).collect();
        let typtypmods: Int32Array = rows.iter().map(|_| TYPTYPMOD).collect();
        let typndimss: Int32Array = rows.iter().map(|_| TYPNDIMS).collect();
        let typcollations: UInt32Array = rows.iter().map(|r| r.typcollation.get()).collect();
        let typdefaultbins = StringArray::from(vec![None::<&str>; n]);
        let typdefaults = StringArray::from(vec![None::<&str>; n]);
        let typacls: ListArray = {
            let mut b = ListBuilder::new(StringBuilder::new());
            for _ in 0..n {
                b.append(false);
            }
            b.finish()
        };

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(typnames),
                Arc::new(typnamespaces),
                Arc::new(typowners),
                Arc::new(typlens),
                Arc::new(typbyvals),
                Arc::new(typtypes),
                Arc::new(typcategories),
                Arc::new(typispreferreds),
                Arc::new(typisdefineds),
                Arc::new(typdelims),
                Arc::new(typrelids),
                Arc::new(typsubscripts),
                Arc::new(typelems),
                Arc::new(typarrays),
                Arc::new(typinputs),
                Arc::new(typoutputs),
                Arc::new(typreceives),
                Arc::new(typsends),
                Arc::new(typmodins),
                Arc::new(typmodouts),
                Arc::new(typanalyzes),
                Arc::new(typaligns),
                Arc::new(typstorages),
                Arc::new(typnotnulls),
                Arc::new(typbasetypes),
                Arc::new(typtypmods),
                Arc::new(typndimss),
                Arc::new(typcollations),
                Arc::new(typdefaultbins),
                Arc::new(typdefaults),
                Arc::new(typacls),
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

    fn col_i16(batch: &RecordBatch, name: &str) -> Vec<i16> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn row_for(batch: &RecordBatch, oid: u32) -> usize {
        col_u32(batch, "oid")
            .into_iter()
            .position(|o| o == oid)
            .unwrap_or_else(|| panic!("no pg_type row for oid {oid}"))
    }

    /// The well-known builtins report the exact `typlen`/`typtype`/
    /// `typcategory` a live Postgres 18 reports — see the module docs for the
    /// verifying query.
    #[test]
    fn well_known_builtins_have_correct_typlen_and_typcategory() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();

        let cases: &[(u32, &str, i16, char, char)] = &[
            (16, "bool", 1, 'b', 'B'),
            (23, "int4", 4, 'b', 'N'),
            (20, "int8", 8, 'b', 'N'),
            (25, "text", -1, 'b', 'S'),
            (1700, "numeric", -1, 'b', 'N'),
            (2950, "uuid", 16, 'b', 'U'),
            (3802, "jsonb", -1, 'b', 'U'),
            (705, "unknown", -2, 'p', 'X'),
            // "char" (18) is a single byte, distinct from bpchar (1042).
            (18, "char", 1, 'b', 'Z'),
        ];

        for &(oid, name, typlen, typtype, typcategory) in cases {
            let i = row_for(&batch, oid);
            assert_eq!(col_str(&batch, "typname")[i], name, "typname for oid {oid}");
            assert_eq!(col_i16(&batch, "typlen")[i], typlen, "typlen for oid {oid}");
            assert_eq!(
                col_str(&batch, "typtype")[i],
                typtype.to_string(),
                "typtype for oid {oid}"
            );
            assert_eq!(
                col_str(&batch, "typcategory")[i],
                typcategory.to_string(),
                "typcategory for oid {oid}"
            );
        }
    }

    /// The physical-representation columns real Postgres copies into
    /// `pg_attribute` — spot-checked here against the live values recorded in
    /// the module docs, and checked in full by `catalog_fidelity`.
    #[test]
    fn physical_representation_columns_match_live_postgres() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();

        // (oid, typbyval, typalign, typstorage, typcollation)
        let cases: &[(u32, bool, &str, &str, u32)] = &[
            (23, true, "i", "p", 0),      // int4
            (20, true, "d", "p", 0),      // int8
            (21, true, "s", "p", 0),      // int2
            (16, true, "c", "p", 0),      // bool
            (25, false, "i", "x", 100),   // text — collatable, default collation
            (19, false, "c", "p", 950),   // name — collatable, C collation
            (1700, false, "i", "m", 0),   // numeric — 'm' (main), not 'x'
            (1009, false, "i", "x", 100), // _text — inherits text's collation
            (1007, false, "i", "x", 0),   // _int4
        ];

        for &(oid, byval, align, storage, collation) in cases {
            let i = row_for(&batch, oid);
            assert_eq!(col_bool(&batch, "typbyval")[i], byval, "typbyval {oid}");
            assert_eq!(col_str(&batch, "typalign")[i], align, "typalign {oid}");
            assert_eq!(
                col_str(&batch, "typstorage")[i],
                storage,
                "typstorage {oid}"
            );
            assert_eq!(
                col_u32(&batch, "typcollation")[i],
                collation,
                "typcollation {oid}"
            );
        }
    }

    /// The four I/O function oids are never `0` — every real type has all
    /// four, so a `0` here would be a claim the type cannot be parsed or
    /// serialized. See the module docs.
    #[test]
    fn every_row_has_all_four_io_functions() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();
        for col in ["typinput", "typoutput", "typreceive", "typsend"] {
            for (i, v) in col_u32(&batch, col).into_iter().enumerate() {
                assert_ne!(v, 0, "{col} is 0 for {}", col_str(&batch, "typname")[i]);
            }
        }
    }

    /// The columns the module docs record as uniform across all 49 rows
    /// really are uniform — a guard on the module-constant representation.
    #[test]
    fn the_uniform_columns_are_uniform() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(col_u32(&batch, "typowner").iter().all(|&v| v == 10));
        assert!(col_u32(&batch, "typrelid").iter().all(|&v| v == 0));
        assert!(col_u32(&batch, "typbasetype").iter().all(|&v| v == 0));
        assert!(col_bool(&batch, "typisdefined").iter().all(|&v| v));
        assert!(col_bool(&batch, "typnotnull").iter().all(|&v| !v));
        assert!(col_str(&batch, "typdelim").iter().all(|v| v == ","));
    }

    /// The three nullable columns are `NULL` for every row, not empty
    /// strings or empty lists — see the module docs on why `NULL` is the
    /// correct value rather than a stand-in.
    #[test]
    fn the_three_nullable_columns_are_null_for_every_row() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();
        for name in ["typdefaultbin", "typdefault", "typacl"] {
            let c = batch.column(batch.schema().index_of(name).unwrap());
            assert_eq!(c.null_count(), c.len(), "{name} must be NULL everywhere");
        }
    }

    /// Every array row's `typelem` points back at its scalar, and the scalar's
    /// `typarray` points forward at the array — the round trip
    /// `basin-pgtype::oid::array_of`/`element_of` also pin, now reported
    /// through the catalog relation itself.
    #[test]
    fn array_and_scalar_rows_reference_each_other() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();

        let int4 = row_for(&batch, 23);
        assert_eq!(col_u32(&batch, "typarray")[int4], 1007, "int4.typarray");

        let int4_array = row_for(&batch, 1007);
        assert_eq!(col_str(&batch, "typname")[int4_array], "_int4");
        assert_eq!(col_u32(&batch, "typelem")[int4_array], 23, "_int4.typelem");
        assert_eq!(
            col_u32(&batch, "typarray")[int4_array],
            0,
            "arrays do not nest"
        );
    }

    /// Every row lives in `pg_catalog` (namespace 11), confirmed live.
    #[test]
    fn every_row_is_in_pg_catalog_namespace() {
        let batch = PgType.scan(&MockCatalog::new(), &[]).unwrap();
        for ns in col_u32(&batch, "typnamespace") {
            assert_eq!(ns, 11);
        }
    }

    /// [`attribute_type_info`] reports exactly what the row carries — the
    /// contract [`crate::pg_attribute`] relies on to derive `attlen`,
    /// `attbyval`, `attalign`, `attstorage`, `attcollation` and `attndims`
    /// the same way real Postgres does.
    #[test]
    fn attribute_type_info_mirrors_the_row() {
        let text = attribute_type_info(oid::TEXT).expect("text has a pg_type row");
        assert_eq!(
            text,
            AttributeTypeInfo {
                attlen: -1,
                attbyval: false,
                attalign: 'i',
                attstorage: 'x',
                attcollation: Oid(100),
                is_array: false,
            }
        );

        let text_array = attribute_type_info(oid::TEXT_ARRAY).expect("_text has a pg_type row");
        assert!(text_array.is_array, "_text is typcategory 'A'");
        assert_eq!(text_array.attcollation, Oid(100), "inherited from text");

        assert_eq!(attribute_type_info(Oid(999_999)), None);
    }

    /// The entire point of this crate: a predicate on `oid` must actually
    /// narrow the row set, not be silently discarded the way today's
    /// `TableProvider::scan()` discards `_filters` (see the crate docs and
    /// doc 11 §1).
    #[test]
    fn pushed_oid_predicate_narrows_the_result() {
        let full = PgType.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(full.num_rows() > 1, "sanity: pg_type has more than one row");

        let filtered = PgType
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(23))])
            .unwrap();

        assert_eq!(
            filtered.num_rows(),
            1,
            "oid = 23 must match exactly one row"
        );
        assert_eq!(col_u32(&filtered, "oid"), vec![23]);
        assert_eq!(col_str(&filtered, "typname"), vec!["int4".to_string()]);
    }

    /// `IN` pushdown narrows to exactly the named set, in whatever order the
    /// relation's own row order produces them.
    #[test]
    fn pushed_in_predicate_narrows_to_the_named_set() {
        let filtered = PgType
            .scan(
                &MockCatalog::new(),
                &[Predicate::in_list(
                    "oid",
                    [Value::Oid(Oid(16)), Value::Oid(Oid(25))],
                )],
            )
            .unwrap();

        let mut oids = col_u32(&filtered, "oid");
        oids.sort_unstable();
        assert_eq!(oids, vec![16, 25]);
    }

    /// A newly added column is pushable too, not just the original eight.
    #[test]
    fn pushed_predicate_on_a_newly_added_column_narrows() {
        let byval = PgType
            .scan(&MockCatalog::new(), &[Predicate::eq("typbyval", true)])
            .unwrap();
        assert!(byval.num_rows() > 0);
        assert!(col_bool(&byval, "typbyval").iter().all(|&v| v));
        assert!(
            byval.num_rows() < TYPES.len(),
            "typbyval = true must not match every row"
        );
    }

    /// A predicate on a real column that matches nothing must return zero
    /// rows, not fall back to returning everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgType
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(999_999))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column `pg_type` does not have is an error, not a
    /// silent no-op — see [`crate::Error::UnknownColumn`]'s docs.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgType
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_type",
                column: "nope".to_string(),
            }
        );
    }

    /// Two predicates combine with AND, same as a real `WHERE a = x AND b = y`.
    #[test]
    fn multiple_predicates_combine_with_and() {
        let filtered = PgType
            .scan(
                &MockCatalog::new(),
                &[
                    Predicate::eq("typcategory", "A"),
                    Predicate::eq("typelem", Oid(23)),
                ],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "typname"), vec!["_int4".to_string()]);
    }

    /// Pins the exact column set, order, and Arrow type of `pg_type` against
    /// live PostgreSQL 18.2's `attnum` order, so a future edit cannot
    /// silently reorder or rename a column out from under positional readers.
    /// Verified live via:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_type'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    /// ```
    ///
    /// which reports all **32** columns in exactly this order. This relation
    /// now implements every one of them, so — unlike `pg_index` and
    /// `pg_operator`, which still subset — position here is faithful as well
    /// as name.
    #[test]
    fn schema_matches_live_postgres_column_order_and_types() {
        let list_of_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let schema = PgType.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("typname", DataType::Utf8, false),
                ("typnamespace", DataType::UInt32, false),
                ("typowner", DataType::UInt32, false),
                ("typlen", DataType::Int16, false),
                ("typbyval", DataType::Boolean, false),
                ("typtype", DataType::Utf8, false),
                ("typcategory", DataType::Utf8, false),
                ("typispreferred", DataType::Boolean, false),
                ("typisdefined", DataType::Boolean, false),
                ("typdelim", DataType::Utf8, false),
                ("typrelid", DataType::UInt32, false),
                ("typsubscript", DataType::UInt32, false),
                ("typelem", DataType::UInt32, false),
                ("typarray", DataType::UInt32, false),
                ("typinput", DataType::UInt32, false),
                ("typoutput", DataType::UInt32, false),
                ("typreceive", DataType::UInt32, false),
                ("typsend", DataType::UInt32, false),
                ("typmodin", DataType::UInt32, false),
                ("typmodout", DataType::UInt32, false),
                ("typanalyze", DataType::UInt32, false),
                ("typalign", DataType::Utf8, false),
                ("typstorage", DataType::Utf8, false),
                ("typnotnull", DataType::Boolean, false),
                ("typbasetype", DataType::UInt32, false),
                ("typtypmod", DataType::Int32, false),
                ("typndims", DataType::Int32, false),
                ("typcollation", DataType::UInt32, false),
                ("typdefaultbin", DataType::Utf8, true),
                ("typdefault", DataType::Utf8, true),
                ("typacl", list_of_utf8, true),
            ]
        );
    }

    #[test]
    fn name_is_pg_type() {
        assert_eq!(PgType.name(), "pg_type");
    }
}
