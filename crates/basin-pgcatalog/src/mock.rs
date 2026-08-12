//! An in-memory [`CatalogSource`] for tests.
//!
//! Not behind `#[cfg(test)]`: downstream crates that eventually wire a real
//! `basin-catalog`-backed [`CatalogSource`] will still want a mock for their
//! own relation tests, the same way this crate uses one for [`crate::pg_class`]
//! and [`crate::pg_namespace`].

use crate::{
    catalog_source::{ColumnInfo, ConstraintInfo, IndexInfo, NamespaceInfo, TableInfo},
    CatalogSource, Oid,
};

/// A [`CatalogSource`] backed by plain `Vec`s, built with the `with_*`
/// methods.
#[derive(Debug, Clone, Default)]
pub struct MockCatalog {
    namespaces: Vec<NamespaceInfo>,
    tables: Vec<TableInfo>,
    columns: Vec<ColumnInfo>,
    indexes: Vec<IndexInfo>,
    constraints: Vec<ConstraintInfo>,
}

impl MockCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespace(mut self, ns: NamespaceInfo) -> Self {
        self.namespaces.push(ns);
        self
    }

    pub fn with_table(mut self, table: TableInfo) -> Self {
        self.tables.push(table);
        self
    }

    pub fn with_column(mut self, column: ColumnInfo) -> Self {
        self.columns.push(column);
        self
    }

    pub fn with_index(mut self, index: IndexInfo) -> Self {
        self.indexes.push(index);
        self
    }

    pub fn with_constraint(mut self, constraint: ConstraintInfo) -> Self {
        self.constraints.push(constraint);
        self
    }
}

impl CatalogSource for MockCatalog {
    fn namespaces(&self) -> Vec<NamespaceInfo> {
        self.namespaces.clone()
    }

    fn tables(&self) -> Vec<TableInfo> {
        self.tables.clone()
    }

    fn columns(&self, table_oid: Oid) -> Vec<ColumnInfo> {
        self.columns
            .iter()
            .filter(|c| c.table_oid == table_oid)
            .cloned()
            .collect()
    }

    fn indexes(&self, table_oid: Oid) -> Vec<IndexInfo> {
        self.indexes
            .iter()
            .filter(|i| i.table_oid == table_oid)
            .cloned()
            .collect()
    }

    fn constraints(&self, table_oid: Oid) -> Vec<ConstraintInfo> {
        self.constraints
            .iter()
            .filter(|c| c.table_oid == table_oid)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_source::RelKind;

    #[test]
    fn columns_are_scoped_to_their_table() {
        let catalog = MockCatalog::new()
            .with_column(ColumnInfo {
                table_oid: Oid(100),
                name: "id".to_string(),
                attnum: 1,
                type_oid: Oid(23),
                not_null: true,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(200),
                name: "other".to_string(),
                attnum: 1,
                type_oid: Oid(25),
                not_null: false,
            });

        let cols = catalog.columns(Oid(100));
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn tables_and_namespaces_hold_what_was_given() {
        let catalog = MockCatalog::new()
            .with_namespace(NamespaceInfo {
                oid: Oid(16384),
                name: "public".to_string(),
                owner: Oid(10),
            })
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            });

        assert_eq!(catalog.namespaces().len(), 1);
        assert_eq!(catalog.tables().len(), 1);
        assert_eq!(catalog.tables()[0].name, "widgets");
    }
}
