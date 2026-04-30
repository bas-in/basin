//! `basin-catalog` — typed Iceberg-style catalog client used by the rest of
//! Basin.
//!
//! We do **not** implement the catalog server here. In production we plan to
//! deploy [Lakekeeper](https://github.com/lakekeeper/lakekeeper) (or any other
//! Iceberg REST catalog implementation) and talk to it over HTTP. This crate
//! is the Rust-side abstraction the storage, shard, router, and analytical
//! layers depend on, plus an in-memory implementation good enough for unit
//! tests and the Phase 1 integration test.
//!
//! Phase 1 ships three implementations behind a single [`Catalog`] trait:
//!
//! 1. [`InMemoryCatalog`] — async-safe, volatile. Default for unit tests and
//!    the original Phase 1 integration test.
//! 2. [`PostgresCatalog`] — durable, backed by a Postgres schema. The
//!    production path: every operation persists, restart preserves all
//!    table metadata and snapshot history.
//! 3. [`RestCatalog`] — stub locking in the API shape; implemented in a later
//!    phase against a real Iceberg REST catalog (Lakekeeper et al).
//!
//! Tenant scoping is part of the API: every method takes a [`TenantId`].
//! There is intentionally no method that lists or enumerates across tenants;
//! a future cross-tenant admin path will be a separate, capability-gated
//! interface.

#![forbid(unsafe_code)]

mod in_memory;
mod metadata;
mod postgres;
mod rest;
mod snapshot;

use async_trait::async_trait;
use basin_common::{Result, TableName, TenantId};

pub use in_memory::InMemoryCatalog;
pub use metadata::{DataFileRef, TableMetadata};
pub use postgres::PostgresCatalog;
pub use rest::RestCatalog;
pub use snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};

/// Iceberg-style catalog client. Every operation is tenant-scoped.
///
/// Implementations must be `Send + Sync` so a single instance can be cloned
/// (or wrapped in [`std::sync::Arc`]) and shared across the shard owner,
/// the router, and the analytical pool.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Idempotently create the namespace for `tenant`. Catalog implementations
    /// model a tenant as an Iceberg namespace (e.g. `tenants.<ulid>`).
    async fn create_namespace(&self, tenant: &TenantId) -> Result<()>;

    /// Create a new empty table. Returns the freshly minted metadata, including
    /// the genesis snapshot (id 0, no data files).
    async fn create_table(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: &arrow_schema::Schema,
    ) -> Result<TableMetadata>;

    /// Load the current metadata for `(tenant, table)`. Returns
    /// [`basin_common::BasinError::NotFound`] if the table does not exist.
    async fn load_table(&self, tenant: &TenantId, table: &TableName) -> Result<TableMetadata>;

    /// Drop the table. Does not delete the underlying object-store data; that
    /// is the storage layer's responsibility (and may be deferred for
    /// time-travel / point-in-time-restore in Phase 6).
    async fn drop_table(&self, tenant: &TenantId, table: &TableName) -> Result<()>;

    /// Tables visible to `tenant`. By construction this is *only* `tenant`'s
    /// tables — there is no cross-tenant variant in this trait.
    async fn list_tables(&self, tenant: &TenantId) -> Result<Vec<TableName>>;

    /// Atomic commit: append `files` to the table, producing a new snapshot.
    ///
    /// Optimistic concurrency: the caller passes the snapshot id it last saw.
    /// If the table has advanced since then, the call returns
    /// [`basin_common::BasinError::CommitConflict`] and the caller must
    /// reload and retry.
    async fn append_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata>;

    /// Snapshots in commit order (oldest first). Used by point-in-time-restore
    /// and the analytical reader.
    async fn list_snapshots(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>>;
}
