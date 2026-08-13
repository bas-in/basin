//! `pg_catalog.pg_attrdef` — column default expressions.
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` calls this and
//! [`crate::pg_attribute`] the largest user-visible win for the least code:
//! **`pg_dump` cannot round-trip a schema without `pg_attrdef`** — a dumped
//! table loses every `DEFAULT`.
//!
//! # `adbin` is source text, not a parse tree
//!
//! Real Postgres's `adbin` column is `pg_node_tree` — an internal
//! `nodeToString` serialization of the default's parsed expression, which
//! tools never read directly; they call `pg_get_expr(adbin, adrelid)` (a
//! server-side function this crate does not implement) to get the default's
//! source text back, e.g. `'anon'::character varying` or `nextval(...)`.
//!
//! This relation reports that source text **directly in `adbin`** rather
//! than a byte-compatible parse tree, because:
//! - Basin has no `nodeToString` serializer and fabricating one that no
//!   client of ours actually parses would be pure surface area for no
//!   benefit — a fake parse tree is worse than an honest gap.
//! - The two consumers this crate exists to serve — `pg_dump`-shaped
//!   round-tripping and ORM introspection — both want the *source text*, not
//!   the parse tree; real `pg_dump` itself only ever consumes `adbin` through
//!   `pg_get_expr`.
//!
//! **If a real client sends literal `SELECT adbin FROM pg_attrdef` and
//! expects a `pg_node_tree`-shaped string, this is a deliberate, documented
//! deviation, not a bug.**
//!
//! # Where these values come from
//!
//! Checked against a live PostgreSQL 18 (see [`crate::pg_attribute`]'s module
//! docs for the table this reuses):
//!
//! ```sql
//! \d pg_attrdef
//!
//! SELECT oid, adrelid, adnum, adbin, pg_get_expr(adbin, adrelid) AS src
//!   FROM pg_attrdef
//!  WHERE adrelid = 'attr_test'::regclass
//!  ORDER BY adnum;
//! ```
//!
//! Confirmed live: `adnum` matches the defaulted column's `attnum` in
//! `pg_attribute` (e.g. `name`'s default is `adnum = 2`, the same as
//! `name`'s `attnum`); `adbin` for a generated column (`full_name GENERATED
//! ALWAYS AS (...) STORED`) is also a `pg_attrdef` row, with `adbin`'s
//! `pg_get_expr` output the generation expression (`(name)::text || bio`),
//! not a `DEFAULT` in the SQL-clause sense — real Postgres stores both kinds
//! in the same relation, and this crate reproduces that: whatever the
//! catalog's [`crate::catalog_source::ColumnDefaultInfo`] reports (a
//! `DEFAULT` clause or a generation expression) becomes a row here without
//! distinguishing the two, matching real `pg_attrdef`'s own lack of a column
//! for it (a consumer tells them apart via `pg_attribute.attgenerated`).

use std::sync::Arc;

use arrow_array::{Int16Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// One row of `pg_attrdef`, straight off
/// [`crate::catalog_source::ColumnDefaultInfo`].
struct AttrDefRow {
    oid: Oid,
    adrelid: Oid,
    adnum: i16,
    adbin: String,
}

impl AttrDefRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "oid" => Value::Oid(self.oid),
            "adrelid" => Value::Oid(self.adrelid),
            "adnum" => Value::Int(self.adnum as i64),
            "adbin" => Value::Text(self.adbin.clone()),
            _ => return None,
        })
    }
}

/// `pg_catalog.pg_attrdef`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgAttrDef;

impl PgAttrDef {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("adrelid", DataType::UInt32, false),
            Field::new("adnum", DataType::Int16, false),
            // Source text, not a `pg_node_tree` blob — see the module docs.
            Field::new("adbin", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgAttrDef {
    fn name(&self) -> &str {
        "pg_attrdef"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_attrdef",
                    column: p.column().to_string(),
                });
            }
        }

        let mut rows: Vec<AttrDefRow> = Vec::new();
        for table in catalog.tables() {
            for d in catalog.column_defaults(table.oid) {
                rows.push(AttrDefRow {
                    oid: d.oid,
                    adrelid: d.table_oid,
                    adnum: d.attnum,
                    adbin: d.expr_src,
                });
            }
        }

        let rows: Vec<AttrDefRow> = rows
            .into_iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let adrelids: UInt32Array = rows.iter().map(|r| r.adrelid.get()).collect();
        let adnums: Int16Array = rows.iter().map(|r| r.adnum).collect();
        let adbins: StringArray = rows.iter().map(|r| Some(r.adbin.as_str())).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(adrelids),
                Arc::new(adnums),
                Arc::new(adbins),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int16Array, StringArray, UInt32Array};

    use super::*;
    use crate::{
        catalog_source::{ColumnDefaultInfo, ColumnInfo, RelKind, TableInfo},
        mock::MockCatalog,
        SystemView,
    };

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
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

    fn plain_column(table_oid: Oid, name: &str, attnum: i16) -> ColumnInfo {
        ColumnInfo {
            table_oid,
            name: name.to_string(),
            attnum,
            type_oid: Oid(25),
            not_null: false,
            atttypmod: -1,
            attisdropped: false,
            attidentity: None,
            attgenerated: None,
        }
    }

    /// Two tables, one of which (`widgets`) has two defaulted columns and one
    /// plain column; `gadgets` has no defaults at all.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_table(TableInfo {
                oid: Oid(16390),
                name: "gadgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_column(plain_column(Oid(16385), "id", 1))
            .with_column(plain_column(Oid(16385), "name", 2))
            .with_column(plain_column(Oid(16385), "bio", 3))
            .with_column(plain_column(Oid(16390), "widget_id", 1))
            .with_default(ColumnDefaultInfo {
                oid: Oid(20001),
                table_oid: Oid(16385),
                attnum: 1,
                expr_src: "nextval('widgets_id_seq'::regclass)".to_string(),
            })
            .with_default(ColumnDefaultInfo {
                oid: Oid(20002),
                table_oid: Oid(16385),
                attnum: 2,
                expr_src: "'anon'::character varying".to_string(),
            })
    }

    /// A predicate on `adrelid` must narrow to exactly one table's defaults —
    /// the core claim of this crate, applied to `pg_attrdef`.
    #[test]
    fn pushed_adrelid_predicate_narrows_to_one_tables_defaults() {
        let full = PgAttrDef.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 2, "sanity: two defaults total");

        let filtered = PgAttrDef
            .scan(&catalog(), &[Predicate::eq("adrelid", Oid(16385))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        for relid in col_u32(&filtered, "adrelid") {
            assert_eq!(relid, 16385);
        }

        let gadgets = PgAttrDef
            .scan(&catalog(), &[Predicate::eq("adrelid", Oid(16390))])
            .unwrap();
        assert_eq!(gadgets.num_rows(), 0, "gadgets has no defaulted columns");
    }

    /// `adnum` matches the defaulted column's `attnum`.
    #[test]
    fn adnum_matches_the_defaulted_columns_attnum() {
        let batch = PgAttrDef
            .scan(&catalog(), &[Predicate::eq("adnum", 2i64)])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            col_str(&batch, "adbin"),
            vec!["'anon'::character varying".to_string()]
        );
    }

    /// `adbin` reports the default's source text, not a fabricated
    /// `pg_node_tree` blob — the documented, deliberate deviation this
    /// module's docs explain.
    #[test]
    fn adbin_reports_source_text() {
        let batch = PgAttrDef
            .scan(&catalog(), &[Predicate::eq("adnum", 1i64)])
            .unwrap();
        assert_eq!(
            col_str(&batch, "adbin"),
            vec!["nextval('widgets_id_seq'::regclass)".to_string()]
        );
    }

    #[test]
    fn adnum_is_one_based() {
        let batch = PgAttrDef.scan(&catalog(), &[]).unwrap();
        let mut nums = col_i16(&batch, "adnum");
        nums.sort_unstable();
        assert_eq!(nums, vec![1, 2]);
    }

    /// A predicate naming a column `pg_attrdef` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgAttrDef
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_attrdef",
                column: "nope".to_string(),
            }
        );
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgAttrDef.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn name_is_pg_attrdef() {
        assert_eq!(PgAttrDef.name(), "pg_attrdef");
    }

    /// Pins the exact column order, names, types and nullability against a
    /// live PostgreSQL 18.2's `pg_attribute` for `pg_catalog.pg_attrdef`:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_attrdef'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    ///
    ///  attname |   atttypid   | attnum | attnotnull
    /// ---------+--------------+--------+------------
    ///  oid     | oid          |      1 | t
    ///  adrelid | oid          |      2 | t
    ///  adnum   | smallint     |      3 | t
    ///  adbin   | pg_node_tree |      4 | t
    /// ```
    ///
    /// `adbin` is `Utf8` here rather than `pg_node_tree`-shaped — a
    /// deliberate, documented deviation (source text instead of a
    /// `nodeToString` blob), not a positional bug.
    #[test]
    fn schema_matches_live_postgres_18_2_column_order() {
        let schema = PgAttrDef.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("adrelid", DataType::UInt32, false),
                ("adnum", DataType::Int16, false),
                ("adbin", DataType::Utf8, false),
            ]
        );
    }
}
