//! `pg_catalog.pg_namespace` — schemas.
//!
//! Unlike [`crate::pg_type`], this relation is entirely catalog-driven: every
//! row comes from [`CatalogSource::namespaces`]. Columns are restricted to
//! what psql's `\d`-family and ORM introspection actually read; `nspacl` is
//! not reported (same omission `basin-pgtype`'s builtins-only OID table makes
//! for `pg_type`, and for the same reason — it is not needed yet, not
//! forgotten).
//!
//! Live-verified column set and types:
//!
//! ```sql
//! \d pg_namespace
//! -- oid | oid, nspname | name, nspowner | oid, nspacl | aclitem[]
//! ```

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, NamespaceInfo},
    error::Error,
    predicate::{Predicate, Value},
};

/// `pg_catalog.pg_namespace`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgNamespace;

impl PgNamespace {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::UInt32, false),
        ]))
    }
}

fn row_value(ns: &NamespaceInfo, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(ns.oid),
        "nspname" => Value::Text(ns.name.clone()),
        "nspowner" => Value::Oid(ns.owner),
        _ => return None,
    })
}

impl crate::SystemView for PgNamespace {
    fn name(&self) -> &str {
        "pg_namespace"
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
                    relation: "pg_namespace",
                    column: p.column().to_string(),
                });
            }
        }

        let namespaces = catalog.namespaces();
        let rows: Vec<&NamespaceInfo> = namespaces
            .iter()
            .filter(|ns| {
                pushed
                    .iter()
                    .all(|p| p.matches(row_value(ns, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|ns| ns.oid.get()).collect();
        let names: StringArray = rows.iter().map(|ns| Some(ns.name.as_str())).collect();
        let owners: UInt32Array = rows.iter().map(|ns| ns.owner.get()).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(oids), Arc::new(names), Arc::new(owners)],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{mock::MockCatalog, Oid, SystemView};

    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_namespace(NamespaceInfo {
                oid: Oid(11),
                name: "pg_catalog".to_string(),
                owner: Oid(10),
            })
            .with_namespace(NamespaceInfo {
                oid: Oid(16384),
                name: "public".to_string(),
                owner: Oid(10),
            })
    }

    fn names(batch: &RecordBatch) -> Vec<String> {
        batch
            .column(batch.schema().index_of("nspname").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    /// With no predicate, every namespace the mock holds comes back.
    #[test]
    fn returns_every_namespace_the_mock_holds() {
        let batch = PgNamespace.scan(&catalog(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let mut ns = names(&batch);
        ns.sort();
        assert_eq!(ns, vec!["pg_catalog".to_string(), "public".to_string()]);
    }

    /// A pushed `oid` predicate narrows to the matching namespace only.
    #[test]
    fn pushed_oid_predicate_narrows_to_one_namespace() {
        let batch = PgNamespace
            .scan(&catalog(), &[Predicate::eq("oid", Oid(16384))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(names(&batch), vec!["public".to_string()]);
    }

    /// A pushed `nspname` predicate works the same way, on a text column.
    #[test]
    fn pushed_nspname_predicate_narrows_by_name() {
        let batch = PgNamespace
            .scan(&catalog(), &[Predicate::eq("nspname", "pg_catalog")])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(names(&batch), vec!["pg_catalog".to_string()]);
    }

    #[test]
    fn empty_catalog_yields_zero_rows_not_an_error() {
        let batch = PgNamespace.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Pins the exact column set, order, and Arrow type of `pg_namespace`
    /// against live PostgreSQL 18.2's `attnum` order, so a future edit
    /// cannot silently reorder or rename a column out from under positional
    /// readers. Verified live via:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_namespace'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    /// ```
    ///
    /// which reports `oid` (1), `nspname` (2), `nspowner` (3), `nspacl` (4,
    /// not reported here) — this module's order already matched; this test
    /// is the audit's record of that, and a guard against regression.
    #[test]
    fn schema_matches_live_postgres_column_order_and_types() {
        let schema = PgNamespace.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("nspname", DataType::Utf8, false),
                ("nspowner", DataType::UInt32, false),
            ]
        );
    }
}
