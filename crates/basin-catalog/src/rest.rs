//! Iceberg REST catalog client — Phase 1 stub.
//!
//! This intentionally `unimplemented!()`s every method. The point of having
//! it now is to lock in the [`crate::Catalog`] trait shape against a real
//! REST catalog target (Lakekeeper, Tabular, Polaris, …). Phase 2/3 fills it
//! in against the published [Iceberg REST OpenAPI
//! spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! The struct is constructable so callers can already write code against
//! `Box<dyn Catalog>` today; touching any method panics.

#![allow(dead_code)]

use async_trait::async_trait;
use basin_common::{Result, TableName, TenantId};

use crate::metadata::{DataFileRef, TableMetadata};
use crate::snapshot::{Snapshot, SnapshotId};
use crate::Catalog;

/// Stub HTTP client for an Iceberg REST catalog (e.g. Lakekeeper).
pub struct RestCatalog {
    base_url: String,
    /// Optional bearer token. Real implementation will rotate via the
    /// catalog's OAuth2 endpoint.
    bearer: Option<String>,
}

impl RestCatalog {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            bearer: None,
        }
    }

    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    async fn create_namespace(&self, _tenant: &TenantId) -> Result<()> {
        unimplemented!("RestCatalog::create_namespace lands in Phase 2")
    }

    async fn create_table(
        &self,
        _tenant: &TenantId,
        _table: &TableName,
        _schema: &arrow_schema::Schema,
    ) -> Result<TableMetadata> {
        unimplemented!("RestCatalog::create_table lands in Phase 2")
    }

    async fn load_table(
        &self,
        _tenant: &TenantId,
        _table: &TableName,
    ) -> Result<TableMetadata> {
        unimplemented!("RestCatalog::load_table lands in Phase 2")
    }

    async fn drop_table(&self, _tenant: &TenantId, _table: &TableName) -> Result<()> {
        unimplemented!("RestCatalog::drop_table lands in Phase 2")
    }

    async fn list_tables(&self, _tenant: &TenantId) -> Result<Vec<TableName>> {
        unimplemented!("RestCatalog::list_tables lands in Phase 2")
    }

    async fn append_data_files(
        &self,
        _tenant: &TenantId,
        _table: &TableName,
        _expected_snapshot: SnapshotId,
        _files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        unimplemented!("RestCatalog::append_data_files lands in Phase 2")
    }

    async fn list_snapshots(
        &self,
        _tenant: &TenantId,
        _table: &TableName,
    ) -> Result<Vec<Snapshot>> {
        unimplemented!("RestCatalog::list_snapshots lands in Phase 2")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructable() {
        // We can build the stub even though every method panics.
        let _ = RestCatalog::new("https://catalog.example/iceberg/v1")
            .with_bearer("dev-token");
    }
}
