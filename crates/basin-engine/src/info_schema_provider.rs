//! Engine-side routing for `information_schema.tables` and
//! `pg_catalog.pg_class` (Phase 5.11.M, follow-up to the catalog starter).
//!
//! The catalog half — `basin_catalog::info_schema::InfoSchemaQuery::tables`
//! and `pg_class` — already produces tenant-filtered Arrow `RecordBatch`es.
//! This module is the DataFusion glue: a pair of [`TableProvider`]s that
//! plug into a `SessionContext` so a tenant SELECT statement against
//! `information_schema.tables` (or `pg_catalog.pg_class`) lands on the same
//! catalog code path as the Rust API.
//!
//! ## Per-tenant cost discipline
//!
//! Both providers hold `Arc<dyn Catalog>` + `TenantId` only — that's the
//! cheap per-tenant primitive on top of the shared catalog handle. We do
//! **not** cache `RecordBatch`es: each `scan()` call re-runs
//! `InfoSchemaQuery` against the live catalog state. This costs one
//! `list_tables` + one `load_table` per row per query, which is O(rows
//! visible to this tenant) — a few microseconds for tenants with O(10)
//! tables and bounded by the per-tenant table count, never by the global
//! pool. Caching is a v0.2 optimisation (it'd need invalidation hooks on
//! every CREATE/DROP/ALTER, which is more complexity than the current
//! cost justifies).
//!
//! ## Predicate / projection pushdown
//!
//! Not implemented in v0.1. We hand DataFusion the full `RecordBatch`
//! built from `InfoSchemaQuery::*`, and let DataFusion filter / project
//! on top. For the v0.1 shape (≤ thousands of tables per tenant) this is
//! cheap enough; the overhead is a transient ~32 KB Arrow buffer and a
//! linear scan inside DataFusion's filter operator. v0.2 can wire
//! [`TableProvider::supports_filters_pushdown`] and a custom `ExecutionPlan`
//! that prunes the per-table `load_table` calls when the predicate
//! restricts the result set (e.g. `WHERE table_name = 'orders'`).
//!
//! ## Arrow version bridge
//!
//! `InfoSchemaQuery` returns workspace-Arrow (v54) batches. DataFusion 44
//! speaks bundled Arrow (v53). We translate at the boundary using
//! [`crate::convert::schema_ws_to_df`] + [`crate::convert::batch_ws_to_df`],
//! the same bridge `generated_cols.rs` uses to feed workspace batches into
//! DataFusion's planner.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog};
use basin_common::TenantId;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::convert::{batch_ws_to_df, schema_ws_to_df};

/// `TableProvider` for `information_schema.tables` filtered to the calling
/// tenant. Each `scan()` call rebuilds the row set from
/// [`InfoSchemaQuery::tables`] so newly created / dropped tables become
/// visible without any per-session cache invalidation step.
pub(crate) struct InfoSchemaTablesProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    /// Cached df-arrow schema. Built once at construction time so
    /// `schema()` (which DataFusion calls synchronously during planning)
    /// doesn't have to do the ws→df conversion on every plan.
    schema: DfSchemaRef,
}

// `dyn Catalog` doesn't carry a `Debug` bound (the trait is intentionally
// unconstrained on Debug to keep test doubles simple), so derive(Debug)
// can't see through the `Arc<dyn Catalog>`. DataFusion's `TableProvider`
// requires `Debug` though, so we hand-roll one that prints the tenant and
// elides the catalog handle.
impl std::fmt::Debug for InfoSchemaTablesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaTablesProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaTablesProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::tables_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTablesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        // We surface as a base table; DataFusion's `TableType::View` would
        // imply a stored LogicalPlan, which we don't have.
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::tables(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        // MemoryExec wants `&[Vec<RecordBatch>]` — one inner Vec per
        // partition. We always emit a single partition (the catalog scan
        // is cheap enough that splitting wouldn't help).
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.columns` filtered to the
/// calling tenant. One row per (table, column) the tenant owns.
/// Mirrors [`InfoSchemaTablesProvider`] in shape and per-tenant cost
/// discipline (`Arc<dyn Catalog>` + `TenantId`, no caching).
pub(crate) struct InfoSchemaColumnsProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaColumnsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaColumnsProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaColumnsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::columns_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for InfoSchemaColumnsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::columns(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_attribute` filtered to the calling
/// tenant. One row per (table, column) the tenant owns. `attrelid`
/// shares its hashing scheme with `pg_class.oid` so a JOIN against
/// `pg_class` is direct.
pub(crate) struct PgAttributeProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgAttributeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAttributeProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgAttributeProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_attribute_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgAttributeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_attribute(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_namespace` filtered to the calling
/// tenant. v0.1 emits exactly one row (`"public"`) — single-schema-per
/// -tenant is the v0.1 invariant.
pub(crate) struct PgNamespaceProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgNamespaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgNamespaceProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgNamespaceProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_namespace_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgNamespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_namespace(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_class` filtered to the calling
/// tenant. Mirrors [`InfoSchemaTablesProvider`].
pub(crate) struct PgClassProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgClassProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgClassProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgClassProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_class_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgClassProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_class(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_proc` filtered to the calling
/// tenant. One row per user-defined function (prokind='f') and procedure
/// (prokind='p'). Mirrors [`InfoSchemaTablesProvider`] in shape and
/// per-tenant cost discipline (`Arc<dyn Catalog>` + `TenantId`, no
/// caching).
pub(crate) struct PgProcProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgProcProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgProcProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgProcProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_proc_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgProcProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_proc(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.routines` filtered to the
/// calling tenant. One row per user-defined function and procedure.
/// Mirrors [`PgProcProvider`] in shape; the columns differ
/// (SQL-standard vs. PG-flavoured) but the data sources are the same.
pub(crate) struct RoutinesProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for RoutinesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutinesProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl RoutinesProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::routines_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for RoutinesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::routines(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_index` filtered to the calling
/// tenant. Always emits zero rows in v0.1 (no user-defined indexes —
/// Phase 5.7 B1 secondary indexes are queued). Mirrors
/// [`PgProcProvider`] in shape so the v0.2 expansion (one row per
/// declared index) is a non-breaking row-builder change.
pub(crate) struct PgIndexProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgIndexProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgIndexProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_index_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgIndexProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_index(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_constraint` filtered to the calling
/// tenant. Always emits zero rows in v0.1 (no FK / explicit PK / CHECK /
/// UNIQUE constraint surfaces yet). Mirrors [`PgProcProvider`].
pub(crate) struct PgConstraintProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgConstraintProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConstraintProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgConstraintProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_constraint_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgConstraintProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_constraint(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.views` filtered to the calling
/// tenant. One row per continuous materialized view (5.11.D2); plain
/// `CREATE VIEW` is not in v0.1, so a tenant with no matviews sees zero
/// rows. Mirrors [`InfoSchemaTablesProvider`].
pub(crate) struct InfoSchemaViewsProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaViewsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaViewsProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaViewsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::views_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for InfoSchemaViewsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::views(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.schemata` filtered to the
/// calling tenant. v0.1 emits exactly one row (`"public"`).
pub(crate) struct InfoSchemaSchemataProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaSchemataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaSchemataProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaSchemataProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::schemata_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for InfoSchemaSchemataProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::schemata(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.table_constraints` filtered
/// to the calling tenant. One row per declared constraint; v0.1 only
/// emits `NOT NULL` rows. Same per-tenant cost discipline as
/// [`InfoSchemaTablesProvider`] (`Arc<dyn Catalog>` + `TenantId`, no
/// caching).
pub(crate) struct TableConstraintsProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for TableConstraintsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableConstraintsProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl TableConstraintsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::table_constraints_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for TableConstraintsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch =
            InfoSchemaQuery::table_constraints(self.catalog.as_ref(), &self.tenant)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.key_column_usage` filtered to
/// the calling tenant. Always empty in v0.1 (no PK / UNIQUE / FK
/// surfaces yet). Mirrors [`TableConstraintsProvider`].
pub(crate) struct KeyColumnUsageProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for KeyColumnUsageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyColumnUsageProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl KeyColumnUsageProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::key_column_usage_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for KeyColumnUsageProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::key_column_usage(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `information_schema.referential_constraints`
/// filtered to the calling tenant. Always empty in v0.1 (FOREIGN KEY
/// queued). Mirrors [`TableConstraintsProvider`].
pub(crate) struct ReferentialConstraintsProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for ReferentialConstraintsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferentialConstraintsProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl ReferentialConstraintsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::referential_constraints_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for ReferentialConstraintsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch =
            InfoSchemaQuery::referential_constraints(self.catalog.as_ref(), &self.tenant)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_type` filtered to the calling
/// tenant. Static row set listing the PG built-in types Basin's pgwire
/// layer advertises; pgAdmin's column-detail query JOINs against this
/// view to render type names. Mirrors [`PgClassProvider`].
pub(crate) struct PgTypeProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgTypeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTypeProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgTypeProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_type_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgTypeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_type(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_depend` filtered to the calling
/// tenant. Surfaces continuous-matview → source-table edges and
/// function → arg/return type edges. Mirrors [`PgTypeProvider`].
pub(crate) struct PgDependProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgDependProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDependProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgDependProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_depend_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgDependProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_depend(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// `TableProvider` for `pg_catalog.pg_authid` filtered to the calling
/// tenant. Always emits exactly one row — the calling tenant rendered as
/// a PG-style role; cross-tenant role enumeration is intentionally
/// absent. Mirrors [`PgNamespaceProvider`].
pub(crate) struct PgAuthidProvider {
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgAuthidProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAuthidProvider")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl PgAuthidProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, tenant: TenantId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_authid_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            tenant,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgAuthidProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = InfoSchemaQuery::pg_authid(self.catalog.as_ref(), &self.tenant)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch = batch_ws_to_df(&ws_batch)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemoryExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// Register `information_schema.tables` and `pg_catalog.pg_class` with the
/// session's default catalog (`datafusion`). Must be called once per
/// session, before the listing tables are pre-registered, so the schemas
/// exist by the time any user-facing SELECT runs.
///
/// Conflict note: DataFusion has its own optional built-in
/// `information_schema` provider behind the `information_schema = true`
/// session config flag. Basin sessions deliberately do **not** enable
/// that flag (see `crate::session::open`); when the flag is off, the
/// standard schema-resolution path consults the catalog's registered
/// schemas, which is where the providers below land. If a future
/// configuration ever flips that flag on, DataFusion's built-in provider
/// would shadow the Basin one for unqualified `information_schema.tables`
/// resolution; the load-bearing safety here is "Basin sessions never set
/// `with_information_schema(true)`".
pub(crate) fn register_info_schema_providers(
    ctx: &datafusion::prelude::SessionContext,
    catalog: Arc<dyn Catalog>,
    tenant: TenantId,
) -> DfResult<()> {
    use datafusion::catalog_common::memory::MemorySchemaProvider;

    // The default catalog name is `datafusion` (see DataFusion's
    // `ConfigOptions::default_catalog`). It's created automatically by
    // `SessionContext::new` because `create_default_catalog_and_schema`
    // defaults to true; we look it up rather than rebuild it so the
    // already-registered `public` schema (which `register_listing_table`
    // populates) is preserved.
    let catalog_name = ctx
        .state()
        .config_options()
        .catalog
        .default_catalog
        .clone();
    let df_catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "default catalog {catalog_name:?} not registered on session"
        ))
    })?;

    // information_schema schema with the `tables` + `columns` + `routines`
    // providers.
    let info_schema = Arc::new(MemorySchemaProvider::new());
    let tables_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaTablesProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table("tables".to_string(), tables_provider)?;
    let columns_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaColumnsProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table("columns".to_string(), columns_provider)?;
    let routines_provider: Arc<dyn TableProvider> =
        Arc::new(RoutinesProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table("routines".to_string(), routines_provider)?;
    df_catalog.register_schema("information_schema", info_schema.clone())?;

    // pg_catalog schema with `pg_class` + `pg_attribute` + `pg_namespace`
    // + `pg_proc`.
    let pg_catalog_schema = Arc::new(MemorySchemaProvider::new());
    let pg_class_provider: Arc<dyn TableProvider> =
        Arc::new(PgClassProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_class".to_string(), pg_class_provider)?;
    let pg_attribute_provider: Arc<dyn TableProvider> =
        Arc::new(PgAttributeProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_attribute".to_string(), pg_attribute_provider)?;
    let pg_namespace_provider: Arc<dyn TableProvider> =
        Arc::new(PgNamespaceProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_namespace".to_string(), pg_namespace_provider)?;
    let pg_proc_provider: Arc<dyn TableProvider> =
        Arc::new(PgProcProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_proc".to_string(), pg_proc_provider)?;
    df_catalog.register_schema("pg_catalog", pg_catalog_schema.clone())?;

    // Phase 5.11.M Tier 3 expansion: pg_index, pg_constraint,
    // information_schema.views, information_schema.schemata. Registered
    // at the tail so concurrent agent extensions (table_constraints /
    // key_column_usage / referential_constraints) merge cleanly.
    let pg_index_provider: Arc<dyn TableProvider> =
        Arc::new(PgIndexProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_index".to_string(), pg_index_provider)?;
    let pg_constraint_provider: Arc<dyn TableProvider> =
        Arc::new(PgConstraintProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_constraint".to_string(), pg_constraint_provider)?;
    let views_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaViewsProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table("views".to_string(), views_provider)?;
    let schemata_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaSchemataProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table("schemata".to_string(), schemata_provider)?;

    // Phase 5.11.M Tier 3 (constraint introspection):
    // information_schema.table_constraints, .key_column_usage,
    // .referential_constraints. Registered last so concurrent-agent
    // additions above merge cleanly.
    let table_constraints_provider: Arc<dyn TableProvider> =
        Arc::new(TableConstraintsProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table(
        "table_constraints".to_string(),
        table_constraints_provider,
    )?;
    let key_column_usage_provider: Arc<dyn TableProvider> =
        Arc::new(KeyColumnUsageProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table(
        "key_column_usage".to_string(),
        key_column_usage_provider,
    )?;
    let referential_constraints_provider: Arc<dyn TableProvider> =
        Arc::new(ReferentialConstraintsProvider::new(catalog.clone(), tenant)?);
    info_schema.register_table(
        "referential_constraints".to_string(),
        referential_constraints_provider,
    )?;

    // Phase 5.11.M Tier 3 (type introspection): pg_catalog.pg_type. Static
    // row set listing PG built-ins Basin's pgwire layer advertises;
    // pgAdmin's column-detail query joins against this view to resolve
    // type names from `pg_attribute.atttypid`.
    let pg_type_provider: Arc<dyn TableProvider> =
        Arc::new(PgTypeProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_type".to_string(), pg_type_provider)?;

    // Phase 5.11.M tail: pg_catalog.pg_depend + pg_catalog.pg_authid.
    // Lower-priority catalog completeness — admin scripts / pg_dump
    // resolution paths land here. Tail-appended so concurrent agents
    // adding more views earlier in the chain merge cleanly.
    let pg_depend_provider: Arc<dyn TableProvider> =
        Arc::new(PgDependProvider::new(catalog.clone(), tenant)?);
    pg_catalog_schema.register_table("pg_depend".to_string(), pg_depend_provider)?;
    let pg_authid_provider: Arc<dyn TableProvider> =
        Arc::new(PgAuthidProvider::new(catalog, tenant)?);
    pg_catalog_schema.register_table("pg_authid".to_string(), pg_authid_provider)?;

    Ok(())
}
