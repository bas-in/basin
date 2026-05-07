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
pub use metadata::{
    ColumnStats, CvDef, DataFileRef, PartitionSpec, Policy, PolicyCommand, TableMetadata,
};
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

    /// Drop every catalog row owned by `tenant`: tables, snapshots, and the
    /// namespace itself. Does **not** delete the underlying object-store
    /// bytes; that is the storage layer's job. The default implementation
    /// iterates `list_tables` and calls `drop_table` on each; backends that
    /// can issue a single statement (Postgres `DELETE … WHERE tenant_id = …`)
    /// override for one round-trip instead of N.
    ///
    /// Idempotent: calling on a tenant with no tables / no namespace row is
    /// a no-op (returns Ok). The engine's tenant-deletion path calls this
    /// last, after the storage bytes are gone.
    async fn drop_namespace(&self, tenant: &TenantId) -> Result<()> {
        let tables = self.list_tables(tenant).await?;
        for table in tables {
            // Drop best-effort: a NotFound from a racing dropper is fine.
            match self.drop_table(tenant, &table).await {
                Ok(()) => {}
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Enumerate every data file currently catalogued for `tenant`. The
    /// returned set is the union of `DataFileRef`s recorded across every
    /// snapshot of every table the tenant owns; in the snapshot-delta model
    /// this is a *superset* of the live file set (paths swapped out by
    /// `replace_data_files` may still appear). For deletion that's fine —
    /// `DeleteObjects` is idempotent on already-deleted keys. Higher-level
    /// readers should keep using `list_data_files`, which goes through the
    /// LIST RPC and sees only physically-present files.
    ///
    /// Used by `Storage::delete_tenant` to skip the LIST RPC for the bulk
    /// of the bytes; a follow-up LIST mops up files the catalog doesn't
    /// know about (HNSW segments, orphans).
    ///
    /// Default impl: iterate `list_tables` then `load_table` per table.
    /// Postgres can do this in a single SELECT joining the snapshots table.
    async fn list_tenant_data_files(&self, tenant: &TenantId) -> Result<Vec<DataFileRef>> {
        let tables = self.list_tables(tenant).await?;
        let mut out: Vec<DataFileRef> = Vec::new();
        for table in tables {
            match self.load_table(tenant, &table).await {
                Ok(meta) => {
                    for snap in &meta.snapshots {
                        out.extend(snap.data_files.iter().cloned());
                    }
                }
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

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

    /// Atomically replace the data file set: the resulting snapshot's data
    /// files = (parent.data_files − removed_paths) ∪ added_files.
    ///
    /// Used by copy-on-write UPDATE / DELETE: the engine reads the affected
    /// data files, writes their replacements, and commits the swap in one
    /// catalog round trip. Optimistic concurrency on `expected_snapshot` —
    /// same contract as [`append_data_files`](Catalog::append_data_files).
    /// Returns [`basin_common::BasinError::CommitConflict`] if the snapshot
    /// has advanced. If `removed_paths` references a path that isn't in the
    /// parent snapshot's data file set, the call returns
    /// [`basin_common::BasinError::Catalog`] (caller bug — the path list
    /// must come from a prior `load_table`).
    async fn replace_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata>;

    /// Snapshots in commit order (oldest first). Used by point-in-time-restore
    /// and the analytical reader.
    async fn list_snapshots(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>>;

    /// Set the partitioning declared for `(tenant, table)`. The engine calls
    /// this after a `CREATE TABLE … PARTITION BY …` so subsequent INSERTs
    /// see the spec via [`load_table`](Catalog::load_table).
    ///
    /// Idempotent: calling repeatedly with the same spec is a no-op. Calling
    /// with a different spec on an existing table replaces the spec; the
    /// engine itself rejects mid-life partition changes — this method is the
    /// catalog's "store whatever you're told" trapdoor.
    async fn set_partition_spec(
        &self,
        tenant: &TenantId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()>;

    /// Replace the full RLS state for `(tenant, table)`. The engine collapses
    /// `ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY` and every
    /// `CREATE/ALTER/DROP POLICY` into one of these calls so the catalog
    /// has a single, atomic write surface for the policy set. Default impl
    /// is a no-op so existing catalog implementations stay buildable; the
    /// in-memory and Postgres backends override this.
    async fn set_rls_state(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let _ = (tenant, table, rls_enabled, policies);
        Ok(())
    }

    /// Set the tiered-storage age policy for `(tenant, table)`. The compactor
    /// reads this on its periodic sweep. Passing `cold_after_seconds = None`
    /// disables the policy (the default). Default impl is a no-op so the
    /// stub `RestCatalog` and any future backends stay buildable; the
    /// in-memory and Postgres implementations override this.
    async fn set_tier_policy(
        &self,
        tenant: &TenantId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let _ = (tenant, table, cold_after_seconds, cold_age_column);
        Ok(())
    }

    /// Configure the per-table bloom-filter column set. The writer consults
    /// this on each `write_batch` call to decide which columns get a native
    /// Parquet bloom filter section in the file footer; the reader uses the
    /// same metadata implicitly (no extra catalog round-trip — the bloom
    /// filter is in the Parquet file). Passing an empty `columns` vector
    /// disables bloom filters for the table (the default for back-compat).
    /// Default impl is a no-op so the stub `RestCatalog` and any future
    /// backends stay buildable; the in-memory and Postgres implementations
    /// override this.
    async fn set_bloom_filter_columns(
        &self,
        tenant: &TenantId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let _ = (tenant, table, columns);
        Ok(())
    }

    /// Override the writer's `max_row_group_size` for `(tenant, table)`.
    /// `Some(n)` makes the next write group rows in chunks of `n`;
    /// `None` (the default) restores the writer-global default. The
    /// engine consults this on every INSERT and passes the value via
    /// [`basin_storage::WriteOptions::max_row_group_size`]. Default impl
    /// is a no-op so the stub `RestCatalog` and any future backends stay
    /// buildable; the in-memory and Postgres implementations override
    /// this. See `tests/integration/tests/viability_row_group_sizing.rs`.
    async fn set_row_group_rows(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let _ = (tenant, table, rows);
        Ok(())
    }

    /// Replace the table's Arrow [`Schema`]. Used by `ALTER TABLE … ADD
    /// COLUMN` (and any future evolutionary DDL — DROP COLUMN, type
    /// widening, etc.). The catalog records the new schema; existing data
    /// files keep their original Parquet schemas and the reader handles the
    /// resulting null-padded columns transparently via Arrow's schema
    /// projection.
    ///
    /// The catalog is intentionally permissive — it stores whatever the
    /// engine hands it and trusts the engine to validate that the new
    /// schema is a *legal* evolution (additive, types compatible). Default
    /// impl is a no-op so the stub `RestCatalog` and any future backends
    /// stay buildable.
    async fn set_schema(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: arrow_schema::Schema,
    ) -> Result<()> {
        let _ = (tenant, table, schema);
        Ok(())
    }

    /// Replace the continuous-aggregate definition (or clear it with `None`)
    /// for `(tenant, table)`. The `basin-cv` refresher writes back a fresh
    /// `CvDef` each tick (with updated `last_refreshed_at_unix_ms` /
    /// `last_bucket_max_unix_ms`). Default impl is a no-op so the stub
    /// `RestCatalog` and any future backend stay buildable; the in-memory
    /// and Postgres backends override this. See the `basin-cv` crate docs
    /// for the refresh / read-path semantics.
    async fn set_continuous_aggregate(
        &self,
        tenant: &TenantId,
        table: &TableName,
        def: Option<metadata::CvDef>,
    ) -> Result<()> {
        let _ = (tenant, table, def);
        Ok(())
    }
}
