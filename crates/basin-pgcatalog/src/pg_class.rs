//! `pg_catalog.pg_class` — relations (tables, indexes, views, sequences, ...).
//!
//! Column set is restricted to what this increment's [`CatalogSource`] can
//! actually supply: `reltype`, `relam`, storage statistics (`relpages`,
//! `reltuples`), and ACLs are real `pg_class` columns Postgres reports but are
//! not attempted here. `relnatts` and `relhasindex` are *derived* — via
//! [`CatalogSource::columns`] and [`CatalogSource::indexes`] respectively —
//! rather than stored directly, which is deliberate: it exercises those two
//! [`CatalogSource`] methods and proves the trait's shape is usable, not just
//! declared.
//!
//! Live-verified column set and types (`\d pg_class`): `oid` oid, `relname`
//! name, `relnamespace` oid, `relowner` oid, `relkind` "char", `relnatts`
//! smallint, `relhasindex` boolean.

use std::sync::Arc;

use arrow_array::{BooleanArray, Int16Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, TableInfo},
    error::Error,
    predicate::{Predicate, Value},
};

/// `pg_catalog.pg_class`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgClass;

impl PgClass {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relnamespace", DataType::UInt32, false),
            Field::new("relowner", DataType::UInt32, false),
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relnatts", DataType::Int16, false),
            Field::new("relhasindex", DataType::Boolean, false),
        ]))
    }
}

/// A [`TableInfo`] plus the columns of [`pg_class`](self) that must be
/// derived from the rest of the catalog rather than read off `TableInfo`
/// directly.
struct ClassRow<'a> {
    table: &'a TableInfo,
    relnatts: i16,
    relhasindex: bool,
}

fn row_value(row: &ClassRow, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(row.table.oid),
        "relname" => Value::Text(row.table.name.clone()),
        "relnamespace" => Value::Oid(row.table.namespace),
        "relowner" => Value::Oid(row.table.owner),
        "relkind" => Value::Text(row.table.kind.as_char().to_string()),
        "relnatts" => Value::Int(row.relnatts as i64),
        "relhasindex" => Value::Bool(row.relhasindex),
        _ => return None,
    })
}

impl crate::SystemView for PgClass {
    fn name(&self) -> &str {
        "pg_class"
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
                    relation: "pg_class",
                    column: p.column().to_string(),
                });
            }
        }

        let tables = catalog.tables();
        let rows: Vec<ClassRow> = tables
            .iter()
            .map(|table| ClassRow {
                table,
                relnatts: catalog.columns(table.oid).len() as i16,
                relhasindex: !catalog.indexes(table.oid).is_empty(),
            })
            .filter(|row| {
                pushed
                    .iter()
                    .all(|p| p.matches(row_value(row, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.table.oid.get()).collect();
        let relnames: StringArray = rows.iter().map(|r| Some(r.table.name.as_str())).collect();
        let relnamespaces: UInt32Array = rows.iter().map(|r| r.table.namespace.get()).collect();
        let relowners: UInt32Array = rows.iter().map(|r| r.table.owner.get()).collect();
        let relkinds: StringArray = rows
            .iter()
            .map(|r| Some(r.table.kind.as_char().to_string()))
            .collect();
        let relnatts: Int16Array = rows.iter().map(|r| r.relnatts).collect();
        let relhasindexes: BooleanArray = rows.iter().map(|r| r.relhasindex).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(relnames),
                Arc::new(relnamespaces),
                Arc::new(relowners),
                Arc::new(relkinds),
                Arc::new(relnatts),
                Arc::new(relhasindexes),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, BooleanArray, Int16Array};

    use super::*;
    use crate::{
        catalog_source::{ColumnInfo, IndexInfo, RelKind},
        mock::MockCatalog,
        Oid, SystemView,
    };

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
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "id".to_string(),
                attnum: 1,
                type_oid: Oid(23),
                not_null: true,
                atttypmod: -1,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "name".to_string(),
                attnum: 2,
                type_oid: Oid(25),
                not_null: false,
                atttypmod: -1,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_index(IndexInfo {
                oid: Oid(16389),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                is_unique: true,
                is_primary: true,
                column_attnums: vec![1],
            })
    }

    fn names(batch: &RecordBatch) -> Vec<String> {
        batch
            .column(batch.schema().index_of("relname").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    #[test]
    fn returns_every_table_the_mock_holds() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let mut n = names(&batch);
        n.sort();
        assert_eq!(n, vec!["gadgets".to_string(), "widgets".to_string()]);
    }

    /// `relnatts` is derived from `CatalogSource::columns`, not stored on
    /// `TableInfo` — this is the whole reason `pg_class` needs a catalog
    /// while `pg_type` does not.
    #[test]
    fn relnatts_is_derived_from_catalog_columns() {
        let batch = PgClass
            .scan(&catalog(), &[Predicate::eq("oid", Oid(16385))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let relnatts = batch
            .column(batch.schema().index_of("relnatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(relnatts.value(0), 2, "widgets has two columns in the mock");

        let gadgets = PgClass
            .scan(&catalog(), &[Predicate::eq("oid", Oid(16390))])
            .unwrap();
        let relnatts = gadgets
            .column(gadgets.schema().index_of("relnatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(
            relnatts.value(0),
            0,
            "gadgets has no columns registered in the mock"
        );
    }

    /// `relhasindex` is likewise derived, from `CatalogSource::indexes`.
    #[test]
    fn relhasindex_is_derived_from_catalog_indexes() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        let by_name: Vec<(String, bool)> = names(&batch)
            .into_iter()
            .zip(
                batch
                    .column(batch.schema().index_of("relhasindex").unwrap())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|b| b.unwrap()),
            )
            .collect();

        assert!(by_name.contains(&("widgets".to_string(), true)));
        assert!(by_name.contains(&("gadgets".to_string(), false)));
    }

    /// A pushed `relkind` predicate narrows on a derived-from-`TableInfo`
    /// (not derived-from-catalog-query) column.
    #[test]
    fn pushed_relkind_predicate_narrows() {
        let batch = PgClass
            .scan(&catalog(), &[Predicate::eq("relkind", "r")])
            .unwrap();
        assert_eq!(batch.num_rows(), 2, "both mock tables are ordinary tables");
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgClass.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
