//! `pg_catalog.pg_am` — the access methods a table or index can be built on
//! (`heap`, `btree`, `hash`, ...). `\dA`, `CREATE INDEX ... USING <am>`
//! resolution, and `pg_class.relam` all resolve against this relation.
//!
//! Unlike every other relation in this crate so far except [`crate::pg_type`],
//! this one needs no [`CatalogSource`] at all: Basin does not let a user
//! register a new access method (there is no `CREATE ACCESS METHOD`), so the
//! full row set is exactly Postgres's own builtins, which are as fixed
//! across installations as `pg_type`'s builtin OIDs are.
//!
//! # Where the column layout and rows come from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//!   WHERE attrelid = 'pg_catalog.pg_am'::regclass AND attnum > 0
//!   ORDER BY attnum;
//! -- attname  | atttypid | attnum | attnotnull
//! -- oid      | oid      |      1 | t
//! -- amname   | name     |      2 | t
//! -- amhandler| regproc  |      3 | t
//! -- amtype   | "char"   |      4 | t
//!
//! SELECT oid, amname, amhandler, amtype FROM pg_am ORDER BY oid;
//! -- oid  | amname | amhandler            | amtype
//! --    2 | heap   | heap_tableam_handler | t
//! --  403 | btree  | bthandler            | i
//! --  405 | hash   | hashhandler          | i
//! --  783 | gist   | gisthandler          | i
//! -- 2742 | gin    | ginhandler           | i
//! -- 3580 | brin   | brinhandler          | i
//! -- 4000 | spgist | spghandler           | i
//! ```
//!
//! (This particular server also has `ivfflat`/`hnsw` rows from the `pgvector`
//! extension at oids `7575059`/`7575061` — extension-installed access
//! methods, not part of stock Postgres. Basin has no extension mechanism that
//! could add an access method, so those two are intentionally not modeled
//! here; the seven above are exactly stock PostgreSQL 18's built-in set.)
//!
//! # `amhandler` (`regproc`) — real, fixed oids, verified live
//!
//! `regproc` is `pg_proc.oid`. Each handler function below is a built-in C
//! function with a fixed oid assigned by Postgres's own catalog bootstrap
//! (`genbki`), the same way builtin `pg_type` oids are fixed — confirmed
//! live:
//!
//! ```sql
//! SELECT amname, amhandler::oid, amhandler::text FROM pg_am
//!   WHERE oid IN (2,403,405,783,2742,3580,4000) ORDER BY oid;
//! -- amname | amhandler | amhandler
//! -- heap   |         3 | heap_tableam_handler
//! -- btree  |       330 | bthandler
//! -- hash   |       331 | hashhandler
//! -- gist   |       332 | gisthandler
//! -- gin    |       333 | ginhandler
//! -- brin   |       335 | brinhandler
//! -- spgist |       334 | spghandler
//! ```
//!
//! These are not `basin-pgtype::oid` constants — `basin-pgtype` tracks *type*
//! oids, not function oids, and none of these handler functions are
//! SQL-callable or otherwise represented in `basin-pgtype::func`. They are
//! declared as local constants in this file instead, the same way
//! [`crate::pg_depend`] declares `PG_CLASS_OID`/`PG_NAMESPACE_OID` locally
//! rather than reaching into another crate for oids only this relation needs.
//! Unlike [`crate::pg_cast`]'s `castfunc` (always `0` — a genuine gap, no
//! ground truth available), these values are real and verified, not a
//! documented absence.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// One row of `pg_am`.
struct AmRow {
    oid: Oid,
    amname: &'static str,
    amhandler: Oid,
    amtype: char,
}

impl AmRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "oid" => Value::Oid(self.oid),
            "amname" => Value::Text(self.amname.to_string()),
            "amhandler" => Value::Oid(self.amhandler),
            "amtype" => Value::Text(self.amtype.to_string()),
            _ => return None,
        })
    }
}

// Handler function oids — real, fixed, verified live. See the module docs.
const HEAP_TABLEAM_HANDLER: Oid = Oid(3);
const BTHANDLER: Oid = Oid(330);
const HASHHANDLER: Oid = Oid(331);
const GISTHANDLER: Oid = Oid(332);
const GINHANDLER: Oid = Oid(333);
const SPGHANDLER: Oid = Oid(334);
const BRINHANDLER: Oid = Oid(335);

/// Every stock PostgreSQL 18 access method. See the module docs for the
/// live query that produced this exact set, order, and oids.
static ROWS: &[AmRow] = &[
    AmRow {
        oid: Oid(2),
        amname: "heap",
        amhandler: HEAP_TABLEAM_HANDLER,
        amtype: 't',
    },
    AmRow {
        oid: Oid(403),
        amname: "btree",
        amhandler: BTHANDLER,
        amtype: 'i',
    },
    AmRow {
        oid: Oid(405),
        amname: "hash",
        amhandler: HASHHANDLER,
        amtype: 'i',
    },
    AmRow {
        oid: Oid(783),
        amname: "gist",
        amhandler: GISTHANDLER,
        amtype: 'i',
    },
    AmRow {
        oid: Oid(2742),
        amname: "gin",
        amhandler: GINHANDLER,
        amtype: 'i',
    },
    AmRow {
        oid: Oid(3580),
        amname: "brin",
        amhandler: BRINHANDLER,
        amtype: 'i',
    },
    AmRow {
        oid: Oid(4000),
        amname: "spgist",
        amhandler: SPGHANDLER,
        amtype: 'i',
    },
];

/// `pg_catalog.pg_am`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgAm;

impl PgAm {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("amname", DataType::Utf8, false),
            Field::new("amhandler", DataType::UInt32, false),
            Field::new("amtype", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgAm {
    fn name(&self) -> &str {
        "pg_am"
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
                    relation: "pg_am",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&AmRow> = ROWS
            .iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let amnames: StringArray = rows.iter().map(|r| Some(r.amname)).collect();
        let amhandlers: UInt32Array = rows.iter().map(|r| r.amhandler.get()).collect();
        let amtypes: StringArray = rows.iter().map(|r| Some(r.amtype.to_string())).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(amnames),
                Arc::new(amhandlers),
                Arc::new(amtypes),
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

    #[test]
    fn name_is_pg_am() {
        assert_eq!(PgAm.name(), "pg_am");
    }

    /// The exact column layout live Postgres reports, in order.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgAm.schema();
        let fields: Vec<(&str, &DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("oid", &DataType::UInt32, false),
                ("amname", &DataType::Utf8, false),
                ("amhandler", &DataType::UInt32, false),
                ("amtype", &DataType::Utf8, false),
            ]
        );
    }

    /// This relation needs no catalog at all — an empty `MockCatalog` still
    /// reports the full builtin set, matching live Postgres's own `pg_am`
    /// (populated at `initdb` time, not by user DDL).
    #[test]
    fn empty_catalog_still_yields_all_seven_builtin_access_methods() {
        let batch = PgAm.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 7);
        let mut names = col_str(&batch, "amname");
        names.sort();
        assert_eq!(
            names,
            vec!["brin", "btree", "gin", "gist", "hash", "heap", "spgist"]
        );
    }

    /// Verified live: `heap`'s handler is `heap_tableam_handler` (oid 3) and
    /// its `amtype` is `'t'` (table), not `'i'` (index) like every other row.
    #[test]
    fn heap_is_the_only_table_access_method() {
        let batch = PgAm
            .scan(&MockCatalog::new(), &[Predicate::eq("amname", "heap")])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_u32(&batch, "oid"), vec![2]);
        assert_eq!(col_u32(&batch, "amhandler"), vec![3]);
        assert_eq!(col_str(&batch, "amtype"), vec!["t".to_string()]);
    }

    /// The entire point of this crate: a pushed `oid` predicate narrows to
    /// exactly one access method, applied inside `scan` rather than after
    /// it. Verified live: `btree`'s oid is 403, handler `bthandler` is 330.
    #[test]
    fn pushed_oid_predicate_narrows_to_one_access_method() {
        let full = PgAm.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(full.num_rows(), 7);

        let filtered = PgAm
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(403))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "amname"), vec!["btree".to_string()]);
        assert_eq!(col_u32(&filtered, "amhandler"), vec![330]);
        assert_eq!(col_str(&filtered, "amtype"), vec!["i".to_string()]);
    }

    /// A predicate naming a column `pg_am` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgAm
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_am",
                column: "nope".to_string(),
            }
        );
    }
}
