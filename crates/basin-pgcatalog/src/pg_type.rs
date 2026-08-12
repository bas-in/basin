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
//! for the precedent). The queries:
//!
//! ```sql
//! \d pg_type
//!
//! SELECT oid, typname, typlen, typtype, typcategory, typelem, typarray, typnamespace
//!   FROM pg_type
//!  WHERE oid IN (16,17,18,19,20,21,23,25,26,114,142,700,701,1042,1043,1082,
//!                1083,1114,1184,1186,1266,1700,2950,3802,705,2278,2249,
//!                1000,1001,1002,1003,1005,1007,1009,1014,1015,1016,1021,
//!                1022,1028,1182,1183,1115,1185,1187,1231,199,3807,2951)
//!  ORDER BY oid;
//! ```
//!
//! (the `IN` list is exactly the builtin scalar, pseudo, and array OIDs
//! `basin-pgtype::oid` names constants for). `typnamespace` was `11`
//! (`pg_catalog`) for every one of the 49 rows returned. Re-run this before
//! editing [`TYPES`].
//!
//! Two OIDs appear as `typarray`/`typelem` values below without a matching
//! named constant in `basin-pgtype::oid`, because Basin does not represent a
//! `pg_type` row for them (they are not builtins this crate reports as their
//! own row, only as another row's `typelem`/`typarray` field): `xml`'s
//! `typarray` is `143`, and `record`'s `typarray` is `2287`. `timetz`'s
//! `typarray` (`1270`) is the same situation. These are real Postgres OIDs,
//! confirmed live, not invented — they are simply not (yet) rows of their
//! own here.

use std::sync::Arc;

use arrow_array::{Int16Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::{oid, Oid};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// One row of `pg_type`, restricted to the columns
/// `docs/migration/df-removal/11-pg-catalog-fidelity.md` calls out as
/// mattering in practice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypeRow {
    oid: Oid,
    typname: &'static str,
    typnamespace: Oid,
    typlen: i16,
    typtype: char,
    typcategory: char,
    typelem: Oid,
    typarray: Oid,
}

impl TypeRow {
    const fn new(
        oid: Oid,
        typname: &'static str,
        typlen: i16,
        typtype: char,
        typcategory: char,
        typelem: Oid,
        typarray: Oid,
    ) -> TypeRow {
        TypeRow {
            oid,
            typname,
            typnamespace: crate::PG_CATALOG_NAMESPACE,
            typlen,
            typtype,
            typcategory,
            typelem,
            typarray,
        }
    }

    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "oid" => Value::Oid(self.oid),
            "typname" => Value::Text(self.typname.to_string()),
            "typnamespace" => Value::Oid(self.typnamespace),
            "typlen" => Value::Int(self.typlen as i64),
            "typtype" => Value::Text(self.typtype.to_string()),
            "typcategory" => Value::Text(self.typcategory.to_string()),
            "typelem" => Value::Oid(self.typelem),
            "typarray" => Value::Oid(self.typarray),
            _ => return None,
        })
    }
}

/// Every builtin `pg_type` row Basin can currently represent. See the module
/// docs for the query used to verify each one.
static TYPES: &[TypeRow] = &[
    // ─── Scalar builtins ────────────────────────────────────────────────────
    TypeRow::new(
        oid::BOOL,
        "bool",
        1,
        'b',
        'B',
        Oid::INVALID,
        oid::BOOL_ARRAY,
    ),
    TypeRow::new(
        oid::BYTEA,
        "bytea",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::BYTEA_ARRAY,
    ),
    // `"char"` — see basin-pgtype::oid::CHAR's own docs on why this is not `bpchar`.
    TypeRow::new(
        oid::CHAR,
        "char",
        1,
        'b',
        'Z',
        Oid::INVALID,
        oid::CHAR_ARRAY,
    ),
    TypeRow::new(oid::NAME, "name", 64, 'b', 'S', oid::CHAR, oid::NAME_ARRAY),
    TypeRow::new(
        oid::INT8,
        "int8",
        8,
        'b',
        'N',
        Oid::INVALID,
        oid::INT8_ARRAY,
    ),
    TypeRow::new(
        oid::INT2,
        "int2",
        2,
        'b',
        'N',
        Oid::INVALID,
        oid::INT2_ARRAY,
    ),
    TypeRow::new(
        oid::INT4,
        "int4",
        4,
        'b',
        'N',
        Oid::INVALID,
        oid::INT4_ARRAY,
    ),
    TypeRow::new(
        oid::TEXT,
        "text",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::TEXT_ARRAY,
    ),
    TypeRow::new(oid::OID, "oid", 4, 'b', 'N', Oid::INVALID, oid::OID_ARRAY),
    TypeRow::new(
        oid::JSON,
        "json",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::JSON_ARRAY,
    ),
    // No `XML_ARRAY` constant in basin-pgtype yet; 143 is `_xml`'s real oid.
    TypeRow::new(oid::XML, "xml", -1, 'b', 'U', Oid::INVALID, Oid(143)),
    TypeRow::new(
        oid::FLOAT4,
        "float4",
        4,
        'b',
        'N',
        Oid::INVALID,
        oid::FLOAT4_ARRAY,
    ),
    TypeRow::new(
        oid::FLOAT8,
        "float8",
        8,
        'b',
        'N',
        Oid::INVALID,
        oid::FLOAT8_ARRAY,
    ),
    TypeRow::new(
        oid::BPCHAR,
        "bpchar",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::BPCHAR_ARRAY,
    ),
    TypeRow::new(
        oid::VARCHAR,
        "varchar",
        -1,
        'b',
        'S',
        Oid::INVALID,
        oid::VARCHAR_ARRAY,
    ),
    TypeRow::new(
        oid::DATE,
        "date",
        4,
        'b',
        'D',
        Oid::INVALID,
        oid::DATE_ARRAY,
    ),
    TypeRow::new(
        oid::TIME,
        "time",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIME_ARRAY,
    ),
    TypeRow::new(
        oid::TIMESTAMP,
        "timestamp",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIMESTAMP_ARRAY,
    ),
    TypeRow::new(
        oid::TIMESTAMPTZ,
        "timestamptz",
        8,
        'b',
        'D',
        Oid::INVALID,
        oid::TIMESTAMPTZ_ARRAY,
    ),
    TypeRow::new(
        oid::INTERVAL,
        "interval",
        16,
        'b',
        'T',
        Oid::INVALID,
        oid::INTERVAL_ARRAY,
    ),
    // No `TIMETZ_ARRAY` constant in basin-pgtype yet; 1270 is `_timetz`'s real oid.
    TypeRow::new(oid::TIMETZ, "timetz", 12, 'b', 'D', Oid::INVALID, Oid(1270)),
    TypeRow::new(
        oid::NUMERIC,
        "numeric",
        -1,
        'b',
        'N',
        Oid::INVALID,
        oid::NUMERIC_ARRAY,
    ),
    TypeRow::new(
        oid::UUID,
        "uuid",
        16,
        'b',
        'U',
        Oid::INVALID,
        oid::UUID_ARRAY,
    ),
    TypeRow::new(
        oid::JSONB,
        "jsonb",
        -1,
        'b',
        'U',
        Oid::INVALID,
        oid::JSONB_ARRAY,
    ),
    // ─── Pseudo-types ───────────────────────────────────────────────────────
    TypeRow::new(
        oid::UNKNOWN,
        "unknown",
        -2,
        'p',
        'X',
        Oid::INVALID,
        Oid::INVALID,
    ),
    TypeRow::new(oid::VOID, "void", 4, 'p', 'P', Oid::INVALID, Oid::INVALID),
    // No `RECORD_ARRAY` constant in basin-pgtype yet; 2287 is `_record`'s real oid.
    TypeRow::new(oid::RECORD, "record", -1, 'p', 'P', Oid::INVALID, Oid(2287)),
    // ─── Array builtins ─────────────────────────────────────────────────────
    //
    // Every array row has typlen -1, typtype 'b', typcategory 'A', and
    // typarray 0 (Postgres does not nest array *types* — see
    // basin-pgtype::oid::array_of's own docs).
    TypeRow::new(
        oid::BOOL_ARRAY,
        "_bool",
        -1,
        'b',
        'A',
        oid::BOOL,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::BYTEA_ARRAY,
        "_bytea",
        -1,
        'b',
        'A',
        oid::BYTEA,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::CHAR_ARRAY,
        "_char",
        -1,
        'b',
        'A',
        oid::CHAR,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::NAME_ARRAY,
        "_name",
        -1,
        'b',
        'A',
        oid::NAME,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::INT2_ARRAY,
        "_int2",
        -1,
        'b',
        'A',
        oid::INT2,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::INT4_ARRAY,
        "_int4",
        -1,
        'b',
        'A',
        oid::INT4,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::TEXT_ARRAY,
        "_text",
        -1,
        'b',
        'A',
        oid::TEXT,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::BPCHAR_ARRAY,
        "_bpchar",
        -1,
        'b',
        'A',
        oid::BPCHAR,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::VARCHAR_ARRAY,
        "_varchar",
        -1,
        'b',
        'A',
        oid::VARCHAR,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::INT8_ARRAY,
        "_int8",
        -1,
        'b',
        'A',
        oid::INT8,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::FLOAT4_ARRAY,
        "_float4",
        -1,
        'b',
        'A',
        oid::FLOAT4,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::FLOAT8_ARRAY,
        "_float8",
        -1,
        'b',
        'A',
        oid::FLOAT8,
        Oid::INVALID,
    ),
    TypeRow::new(oid::OID_ARRAY, "_oid", -1, 'b', 'A', oid::OID, Oid::INVALID),
    TypeRow::new(
        oid::DATE_ARRAY,
        "_date",
        -1,
        'b',
        'A',
        oid::DATE,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::TIME_ARRAY,
        "_time",
        -1,
        'b',
        'A',
        oid::TIME,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::TIMESTAMP_ARRAY,
        "_timestamp",
        -1,
        'b',
        'A',
        oid::TIMESTAMP,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::TIMESTAMPTZ_ARRAY,
        "_timestamptz",
        -1,
        'b',
        'A',
        oid::TIMESTAMPTZ,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::INTERVAL_ARRAY,
        "_interval",
        -1,
        'b',
        'A',
        oid::INTERVAL,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::NUMERIC_ARRAY,
        "_numeric",
        -1,
        'b',
        'A',
        oid::NUMERIC,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::JSON_ARRAY,
        "_json",
        -1,
        'b',
        'A',
        oid::JSON,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::JSONB_ARRAY,
        "_jsonb",
        -1,
        'b',
        'A',
        oid::JSONB,
        Oid::INVALID,
    ),
    TypeRow::new(
        oid::UUID_ARRAY,
        "_uuid",
        -1,
        'b',
        'A',
        oid::UUID,
        Oid::INVALID,
    ),
];

/// The `typlen` a builtin's own `pg_type` row reports, for callers (namely
/// [`crate::pg_attribute`]) that need to copy it into `pg_attribute.attlen`
/// the way real Postgres does at column-creation time. `None` for an oid
/// [`TYPES`] has no row for.
pub(crate) fn typlen(oid: Oid) -> Option<i16> {
    TYPES.iter().find(|r| r.oid == oid).map(|r| r.typlen)
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
            Field::new("typlen", DataType::Int16, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typelem", DataType::UInt32, false),
            Field::new("typarray", DataType::UInt32, false),
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

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let typnames: arrow_array::StringArray = rows.iter().map(|r| Some(r.typname)).collect();
        let typnamespaces: UInt32Array = rows.iter().map(|r| r.typnamespace.get()).collect();
        let typlens: Int16Array = rows.iter().map(|r| r.typlen).collect();
        let typtypes: arrow_array::StringArray =
            rows.iter().map(|r| Some(r.typtype.to_string())).collect();
        let typcategories: arrow_array::StringArray = rows
            .iter()
            .map(|r| Some(r.typcategory.to_string()))
            .collect();
        let typelems: UInt32Array = rows.iter().map(|r| r.typelem.get()).collect();
        let typarrays: UInt32Array = rows.iter().map(|r| r.typarray.get()).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(typnames),
                Arc::new(typnamespaces),
                Arc::new(typlens),
                Arc::new(typtypes),
                Arc::new(typcategories),
                Arc::new(typelems),
                Arc::new(typarrays),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int16Array, StringArray, UInt32Array};

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

    #[test]
    fn schema_matches_the_documented_column_set() {
        let schema = PgType.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "typname",
                "typnamespace",
                "typlen",
                "typtype",
                "typcategory",
                "typelem",
                "typarray",
            ]
        );
    }

    #[test]
    fn name_is_pg_type() {
        assert_eq!(PgType.name(), "pg_type");
    }
}
