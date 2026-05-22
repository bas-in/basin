//! Engine-side routing for `information_schema.tables` and
//! `pg_catalog.pg_class` (Phase 5.11.M, follow-up to the catalog starter).
//!
//! The catalog half — `basin_catalog::info_schema::InfoSchemaQuery::tables`
//! and `pg_class` — already produces project-filtered Arrow `RecordBatch`es.
//! This module is the DataFusion glue: a pair of [`TableProvider`]s that
//! plug into a `SessionContext` so a project SELECT statement against
//! `information_schema.tables` (or `pg_catalog.pg_class`) lands on the same
//! catalog code path as the Rust API.
//!
//! ## Per-project cost discipline
//!
//! Both providers hold `Arc<dyn Catalog>` + `ProjectId` only — that's the
//! cheap per-project primitive on top of the shared catalog handle. We do
//! **not** cache `RecordBatch`es: each `scan()` call re-runs
//! `InfoSchemaQuery` against the live catalog state. This costs one
//! `list_tables` + one `load_table` per row per query, which is O(rows
//! visible to this project) — a few microseconds for projects with O(10)
//! tables and bounded by the per-project table count, never by the global
//! pool. Caching is a v0.2 optimisation (it'd need invalidation hooks on
//! every CREATE/DROP/ALTER, which is more complexity than the current
//! cost justifies).
//!
//! ## Predicate / projection pushdown
//!
//! Not implemented in v0.1. We hand DataFusion the full `RecordBatch`
//! built from `InfoSchemaQuery::*`, and let DataFusion filter / project
//! on top. For the v0.1 shape (≤ thousands of tables per project) this is
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

use arrow_array::{ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog};
use basin_common::ProjectId;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::connection_registry::ConnectionRegistry;
use crate::convert::{batch_ws_to_df, schema_ws_to_df};

/// `TableProvider` for `information_schema.tables` filtered to the calling
/// project. Each `scan()` call rebuilds the row set from
/// [`InfoSchemaQuery::tables`] so newly created / dropped tables become
/// visible without any per-session cache invalidation step.
pub(crate) struct InfoSchemaTablesProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    /// Cached df-arrow schema. Built once at construction time so
    /// `schema()` (which DataFusion calls synchronously during planning)
    /// doesn't have to do the ws→df conversion on every plan.
    schema: DfSchemaRef,
}

// `dyn Catalog` doesn't carry a `Debug` bound (the trait is intentionally
// unconstrained on Debug to keep test doubles simple), so derive(Debug)
// can't see through the `Arc<dyn Catalog>`. DataFusion's `TableProvider`
// requires `Debug` though, so we hand-roll one that prints the project and
// elides the catalog handle.
impl std::fmt::Debug for InfoSchemaTablesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaTablesProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaTablesProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::tables_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::tables(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        // MemoryExec wants `&[Vec<RecordBatch>]` — one inner Vec per
        // partition. We always emit a single partition (the catalog scan
        // is cheap enough that splitting wouldn't help).
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.columns` filtered to the
/// calling project. One row per (table, column) the project owns.
/// Mirrors [`InfoSchemaTablesProvider`] in shape and per-project cost
/// discipline (`Arc<dyn Catalog>` + `ProjectId`, no caching).
pub(crate) struct InfoSchemaColumnsProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaColumnsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaColumnsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaColumnsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::columns_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
            schema: Arc::new(df_schema),
        })
    }
}

/// Map an Arrow [`DataType`] to a `(data_type, udt_name)` pair matching PG's
/// `information_schema.columns` values. Used when synthesising column metadata
/// for pg_catalog system views whose schemas are defined in Arrow terms.
fn arrow_to_pg_type(dt: &DataType) -> (&'static str, &'static str) {
    match dt {
        DataType::Boolean => ("boolean", "bool"),
        DataType::Int16 => ("smallint", "int2"),
        DataType::Int32 => ("integer", "int4"),
        DataType::Int64 | DataType::UInt64 | DataType::UInt32 => ("bigint", "int8"),
        DataType::Float32 => ("real", "float4"),
        DataType::Float64 => ("double precision", "float8"),
        DataType::Utf8 | DataType::LargeUtf8 => ("text", "text"),
        DataType::Binary | DataType::LargeBinary => ("bytea", "bytea"),
        DataType::Date32 => ("date", "date"),
        DataType::Timestamp(_, Some(_)) => ("timestamp with time zone", "timestamptz"),
        DataType::Timestamp(_, None) => ("timestamp without time zone", "timestamp"),
        _ => ("text", "text"),
    }
}

/// Build an `information_schema.columns` `RecordBatch` for a set of named
/// pg_catalog system views. Each `(view_name, Arrow schema)` pair contributes
/// one row per field. The resulting batch is concatenated with the user-table
/// batch so that tools querying `information_schema.columns WHERE
/// table_schema = 'pg_catalog'` see the system view column set.
fn pg_catalog_view_columns_batch(
    views: &[(&str, Arc<arrow_schema::Schema>)],
) -> Result<arrow_array::RecordBatch, arrow_schema::ArrowError> {
    let mut table_catalogs: Vec<&str> = Vec::new();
    let mut table_schemas: Vec<&str> = Vec::new();
    let mut table_names: Vec<String> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    let mut ordinals: Vec<i32> = Vec::new();
    let mut defaults: Vec<Option<&str>> = Vec::new();
    let mut is_nullables: Vec<&str> = Vec::new();
    let mut data_types: Vec<&str> = Vec::new();
    let mut udt_names: Vec<&str> = Vec::new();

    for (view_name, schema) in views {
        for (idx, field) in schema.fields().iter().enumerate() {
            table_catalogs.push("basin");
            table_schemas.push("pg_catalog");
            table_names.push(view_name.to_string());
            column_names.push(field.name().clone());
            ordinals.push((idx as i32) + 1);
            defaults.push(None);
            is_nullables.push(if field.is_nullable() { "YES" } else { "NO" });
            let (dt, udt) = arrow_to_pg_type(field.data_type());
            data_types.push(dt);
            udt_names.push(udt);
        }
    }

    // Build using the same columns_schema layout.
    let ws_schema = InfoSchemaQuery::columns_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(table_catalogs)),
        Arc::new(StringArray::from(table_schemas)),
        Arc::new(StringArray::from(table_names)),
        Arc::new(StringArray::from(column_names)),
        Arc::new(Int32Array::from(ordinals)),
        Arc::new(StringArray::from(defaults)),
        Arc::new(StringArray::from(is_nullables)),
        Arc::new(StringArray::from(data_types)),
        Arc::new(StringArray::from(udt_names)),
    ];
    arrow_array::RecordBatch::try_new(ws_schema, columns)
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
        // 1. User tables in the public schema.
        let user_batch = InfoSchemaQuery::columns(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // 2. Known pg_catalog system views — their column metadata exposed via
        //    information_schema.columns so that monitoring tools and ORMs that
        //    query `WHERE table_schema = 'pg_catalog' AND table_name = '…'`
        //    receive the canonical column set (Phase 5.23.C/D).
        let pg_views: &[(&str, Arc<arrow_schema::Schema>)] = &[
            ("pg_stat_activity", InfoSchemaQuery::pg_stat_activity_schema()),
            ("pg_locks", InfoSchemaQuery::pg_locks_schema()),
            ("pg_class", InfoSchemaQuery::pg_class_schema()),
            ("pg_attribute", InfoSchemaQuery::pg_attribute_schema()),
            ("pg_namespace", InfoSchemaQuery::pg_namespace_schema()),
            ("pg_type", InfoSchemaQuery::pg_type_schema()),
            ("pg_proc", InfoSchemaQuery::pg_proc_schema()),
            ("pg_index", InfoSchemaQuery::pg_index_schema()),
            ("pg_constraint", InfoSchemaQuery::pg_constraint_schema()),
            ("pg_depend", InfoSchemaQuery::pg_depend_schema()),
            ("pg_authid", InfoSchemaQuery::pg_authid_schema()),
            ("pg_database", InfoSchemaQuery::pg_database_schema()),
            ("pg_roles", InfoSchemaQuery::pg_roles_schema()),
            ("pg_views", InfoSchemaQuery::pg_views_schema()),
            ("pg_indexes", InfoSchemaQuery::pg_indexes_schema()),
            ("pg_tables", InfoSchemaQuery::pg_tables_schema()),
            ("pg_settings", InfoSchemaQuery::pg_settings_schema()),
            ("pg_description", InfoSchemaQuery::pg_description_schema()),
            ("pg_stat_user_tables", InfoSchemaQuery::pg_stat_user_tables_schema()),
            ("pg_stat_user_indexes", InfoSchemaQuery::pg_stat_user_indexes_schema()),
            ("pg_stat_database", InfoSchemaQuery::pg_stat_database_schema()),
        ];

        let pg_catalog_batch = pg_catalog_view_columns_batch(pg_views)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // 3. Concatenate: user-table rows first, then pg_catalog view rows.
        //    Both batches share the same ws-Arrow schema (columns_schema).
        let combined = arrow_select::concat::concat_batches(
            &InfoSchemaQuery::columns_schema(),
            &[user_batch, pg_catalog_batch],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let df_batch =
            batch_ws_to_df(&combined).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_attribute` filtered to the calling
/// project. One row per (table, column) the project owns. `attrelid`
/// shares its hashing scheme with `pg_class.oid` so a JOIN against
/// `pg_class` is direct.
pub(crate) struct PgAttributeProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgAttributeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAttributeProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgAttributeProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_attribute_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_attribute(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_namespace` filtered to the calling
/// project. v0.1 emits exactly one row (`"public"`) — single-schema-per
/// -project is the v0.1 invariant.
pub(crate) struct PgNamespaceProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgNamespaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgNamespaceProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgNamespaceProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_namespace_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_namespace(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_class` filtered to the calling
/// project. Mirrors [`InfoSchemaTablesProvider`].
pub(crate) struct PgClassProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgClassProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgClassProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgClassProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_class_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_class(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_proc` filtered to the calling
/// project. One row per user-defined function (prokind='f') and procedure
/// (prokind='p'). Mirrors [`InfoSchemaTablesProvider`] in shape and
/// per-project cost discipline (`Arc<dyn Catalog>` + `ProjectId`, no
/// caching).
pub(crate) struct PgProcProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgProcProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgProcProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgProcProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_proc_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_proc(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.routines` filtered to the
/// calling project. One row per user-defined function and procedure.
/// Mirrors [`PgProcProvider`] in shape; the columns differ
/// (SQL-standard vs. PG-flavoured) but the data sources are the same.
pub(crate) struct RoutinesProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for RoutinesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutinesProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl RoutinesProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::routines_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::routines(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_index` filtered to the calling
/// project. Always emits zero rows in v0.1 (no user-defined indexes —
/// Phase 5.7 B1 secondary indexes are queued). Mirrors
/// [`PgProcProvider`] in shape so the v0.2 expansion (one row per
/// declared index) is a non-breaking row-builder change.
pub(crate) struct PgIndexProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgIndexProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgIndexProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_index_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_index(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_constraint` filtered to the calling
/// project. Always emits zero rows in v0.1 (no FK / explicit PK / CHECK /
/// UNIQUE constraint surfaces yet). Mirrors [`PgProcProvider`].
pub(crate) struct PgConstraintProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgConstraintProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConstraintProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgConstraintProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_constraint_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_constraint(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.views` filtered to the calling
/// project. One row per continuous materialized view (5.11.D2); plain
/// `CREATE VIEW` is not in v0.1, so a project with no matviews sees zero
/// rows. Mirrors [`InfoSchemaTablesProvider`].
pub(crate) struct InfoSchemaViewsProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaViewsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaViewsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaViewsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::views_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::views(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_views` filtered to the calling project.
/// Surfaces both plain views (`CREATE VIEW`) and continuous materialized views.
pub(crate) struct PgViewsProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgViewsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgViewsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgViewsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_views_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
            schema: Arc::new(df_schema),
        })
    }
}

#[async_trait]
impl TableProvider for PgViewsProvider {
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
        let ws_batch = InfoSchemaQuery::pg_views(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.schemata` filtered to the
/// calling project. v0.1 emits exactly one row (`"public"`).
pub(crate) struct InfoSchemaSchemataProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for InfoSchemaSchemataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaSchemataProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl InfoSchemaSchemataProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::schemata_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::schemata(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.table_constraints` filtered
/// to the calling project. One row per declared constraint; v0.1 only
/// emits `NOT NULL` rows. Same per-project cost discipline as
/// [`InfoSchemaTablesProvider`] (`Arc<dyn Catalog>` + `ProjectId`, no
/// caching).
pub(crate) struct TableConstraintsProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for TableConstraintsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableConstraintsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl TableConstraintsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::table_constraints_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::table_constraints(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.key_column_usage` filtered to
/// the calling project. Always empty in v0.1 (no PK / UNIQUE / FK
/// surfaces yet). Mirrors [`TableConstraintsProvider`].
pub(crate) struct KeyColumnUsageProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for KeyColumnUsageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyColumnUsageProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl KeyColumnUsageProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::key_column_usage_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::key_column_usage(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `information_schema.referential_constraints`
/// filtered to the calling project. Always empty in v0.1 (FOREIGN KEY
/// queued). Mirrors [`TableConstraintsProvider`].
pub(crate) struct ReferentialConstraintsProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for ReferentialConstraintsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferentialConstraintsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl ReferentialConstraintsProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::referential_constraints_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
            InfoSchemaQuery::referential_constraints(self.catalog.as_ref(), &self.project)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_type` filtered to the calling
/// project. Static row set listing the PG built-in types Basin's pgwire
/// layer advertises; pgAdmin's column-detail query JOINs against this
/// view to render type names. Mirrors [`PgClassProvider`].
pub(crate) struct PgTypeProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgTypeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTypeProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgTypeProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_type_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_type(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_depend` filtered to the calling
/// project. Surfaces continuous-matview → source-table edges and
/// function → arg/return type edges. Mirrors [`PgTypeProvider`].
pub(crate) struct PgDependProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgDependProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDependProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgDependProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_depend_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_depend(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// `TableProvider` for `pg_catalog.pg_authid` filtered to the calling
/// project. Always emits exactly one row — the calling project rendered as
/// a PG-style role; cross-project role enumeration is intentionally
/// absent. Mirrors [`PgNamespaceProvider`].
pub(crate) struct PgAuthidProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
}

impl std::fmt::Debug for PgAuthidProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAuthidProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgAuthidProvider {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_authid_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
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
        let ws_batch = InfoSchemaQuery::pg_authid(self.catalog.as_ref(), &self.project)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

// ---------------------------------------------------------------------------
// Macro to reduce boilerplate for simple catalog-view providers
// ---------------------------------------------------------------------------

/// Generate a simple `TableProvider` struct that calls one `InfoSchemaQuery`
/// method pair (`${name}_schema` / `${name}`).
macro_rules! simple_provider {
    ($struct_name:ident, $query_schema:ident, $query_fn:ident, $debug_name:literal) => {
        pub(crate) struct $struct_name {
            catalog: Arc<dyn Catalog>,
            project: ProjectId,
            schema: DfSchemaRef,
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct($debug_name)
                    .field("project", &self.project)
                    .finish_non_exhaustive()
            }
        }

        impl $struct_name {
            pub(crate) fn new(catalog: Arc<dyn Catalog>, project: ProjectId) -> DfResult<Self> {
                let ws_schema = InfoSchemaQuery::$query_schema();
                let df_schema = schema_ws_to_df(ws_schema.as_ref())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(Self {
                    catalog,
                    project,
                    schema: Arc::new(df_schema),
                })
            }
        }

        #[async_trait]
        impl TableProvider for $struct_name {
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
                let ws_batch = InfoSchemaQuery::$query_fn(self.catalog.as_ref(), &self.project)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let df_batch = batch_ws_to_df(&ws_batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let partitions = vec![vec![df_batch]];
                let exec = MemorySourceConfig::try_new_exec(
                    &partitions,
                    Arc::clone(&self.schema),
                    projection.cloned(),
                )?;
                Ok(exec)
            }
        }
    };
}

// pg_catalog new views
simple_provider!(
    PgDatabaseProvider,
    pg_database_schema,
    pg_database,
    "PgDatabaseProvider"
);
simple_provider!(
    PgRolesProvider,
    pg_roles_schema,
    pg_roles,
    "PgRolesProvider"
);
simple_provider!(
    PgIndexesProvider,
    pg_indexes_schema,
    pg_indexes,
    "PgIndexesProvider"
);
simple_provider!(
    PgTablesProvider,
    pg_tables_schema,
    pg_tables,
    "PgTablesProvider"
);
simple_provider!(
    PgSettingsProvider,
    pg_settings_schema,
    pg_settings,
    "PgSettingsProvider"
);
simple_provider!(
    PgExtensionProvider,
    pg_extension_schema,
    pg_extension,
    "PgExtensionProvider"
);
simple_provider!(
    PgDescriptionProvider,
    pg_description_schema,
    pg_description,
    "PgDescriptionProvider"
);
simple_provider!(
    PgStatUserTablesProvider,
    pg_stat_user_tables_schema,
    pg_stat_user_tables,
    "PgStatUserTablesProvider"
);
simple_provider!(
    PgStatUserIndexesProvider,
    pg_stat_user_indexes_schema,
    pg_stat_user_indexes,
    "PgStatUserIndexesProvider"
);
simple_provider!(
    PgLocksProvider,
    pg_locks_schema,
    pg_locks,
    "PgLocksProvider"
);
simple_provider!(
    PgStatActivityProvider,
    pg_stat_activity_schema,
    pg_stat_activity,
    "PgStatActivityProvider"
);

// information_schema new views
simple_provider!(
    CheckConstraintsProvider,
    check_constraints_schema,
    check_constraints,
    "CheckConstraintsProvider"
);
simple_provider!(
    TriggersProvider,
    triggers_schema,
    triggers,
    "TriggersProvider"
);
simple_provider!(
    SequencesProvider,
    sequences_schema,
    sequences,
    "SequencesProvider"
);
simple_provider!(DomainsProvider, domains_schema, domains, "DomainsProvider");
simple_provider!(
    ParametersProvider,
    parameters_schema,
    parameters,
    "ParametersProvider"
);
simple_provider!(
    RoleTableGrantsProvider,
    role_table_grants_schema,
    role_table_grants,
    "RoleTableGrantsProvider"
);
simple_provider!(
    UserDefinedTypesProvider,
    user_defined_types_schema,
    user_defined_types,
    "UserDefinedTypesProvider"
);
simple_provider!(
    ColumnDomainUsageProvider,
    column_domain_usage_schema,
    column_domain_usage,
    "ColumnDomainUsageProvider"
);
simple_provider!(
    ColumnUdtUsageProvider,
    column_udt_usage_schema,
    column_udt_usage,
    "ColumnUdtUsageProvider"
);
// New pg_stat_* views
simple_provider!(
    PgStatDatabaseProvider,
    pg_stat_database_schema,
    pg_stat_database,
    "PgStatDatabaseProvider"
);
simple_provider!(
    PgStatBgwriterProvider,
    pg_stat_bgwriter_schema,
    pg_stat_bgwriter,
    "PgStatBgwriterProvider"
);
simple_provider!(
    PgStatReplicationProvider,
    pg_stat_replication_schema,
    pg_stat_replication,
    "PgStatReplicationProvider"
);
simple_provider!(
    PgStatArchiverProvider,
    pg_stat_archiver_schema,
    pg_stat_archiver,
    "PgStatArchiverProvider"
);
simple_provider!(
    PgStatWalReceiverProvider,
    pg_stat_wal_receiver_schema,
    pg_stat_wal_receiver,
    "PgStatWalReceiverProvider"
);
simple_provider!(
    PgStatSubscriptionProvider,
    pg_stat_subscription_schema,
    pg_stat_subscription,
    "PgStatSubscriptionProvider"
);
simple_provider!(
    PgStatUserFunctionsProvider,
    pg_stat_user_functions_schema,
    pg_stat_user_functions,
    "PgStatUserFunctionsProvider"
);
simple_provider!(
    PgStatProgressVacuumProvider,
    pg_stat_progress_vacuum_schema,
    pg_stat_progress_vacuum,
    "PgStatProgressVacuumProvider"
);
simple_provider!(
    PgStatProgressCreateIndexProvider,
    pg_stat_progress_create_index_schema,
    pg_stat_progress_create_index,
    "PgStatProgressCreateIndexProvider"
);
simple_provider!(
    PgStatProgressAnalyzeProvider,
    pg_stat_progress_analyze_schema,
    pg_stat_progress_analyze,
    "PgStatProgressAnalyzeProvider"
);
// information_schema bulk views
simple_provider!(
    UsagePrivilegesProvider,
    usage_privileges_schema,
    usage_privileges,
    "UsagePrivilegesProvider"
);
simple_provider!(
    TablePrivilegesProvider,
    table_privileges_schema,
    table_privileges,
    "TablePrivilegesProvider"
);
simple_provider!(
    ColumnPrivilegesProvider,
    column_privileges_schema,
    column_privileges,
    "ColumnPrivilegesProvider"
);
simple_provider!(
    RoleColumnGrantsProvider,
    role_column_grants_schema,
    role_column_grants,
    "RoleColumnGrantsProvider"
);
simple_provider!(
    RoleRoutineGrantsProvider,
    role_routine_grants_schema,
    role_routine_grants,
    "RoleRoutineGrantsProvider"
);
simple_provider!(
    ApplicableRolesProvider,
    applicable_roles_schema,
    applicable_roles,
    "ApplicableRolesProvider"
);
simple_provider!(
    EnabledRolesProvider,
    enabled_roles_schema,
    enabled_roles,
    "EnabledRolesProvider"
);
// FDW stubs
simple_provider!(
    ForeignDataWrappersProvider,
    foreign_data_wrappers_schema,
    foreign_data_wrappers,
    "ForeignDataWrappersProvider"
);
simple_provider!(
    ForeignDataWrapperOptionsProvider,
    foreign_data_wrapper_options_schema,
    foreign_data_wrapper_options,
    "ForeignDataWrapperOptionsProvider"
);
simple_provider!(
    ForeignServersProvider,
    foreign_servers_schema,
    foreign_servers,
    "ForeignServersProvider"
);
simple_provider!(
    ForeignServerOptionsProvider,
    foreign_server_options_schema,
    foreign_server_options,
    "ForeignServerOptionsProvider"
);
simple_provider!(
    ForeignTablesProvider,
    foreign_tables_schema,
    foreign_tables,
    "ForeignTablesProvider"
);
simple_provider!(
    ForeignTableOptionsProvider,
    foreign_table_options_schema,
    foreign_table_options,
    "ForeignTableOptionsProvider"
);
simple_provider!(
    UserMappingsProvider,
    user_mappings_schema,
    user_mappings,
    "UserMappingsProvider"
);
simple_provider!(
    UserMappingOptionsProvider,
    user_mapping_options_schema,
    user_mapping_options,
    "UserMappingOptionsProvider"
);

// ---------------------------------------------------------------------------
// Phase 5.23.C: live pg_stat_activity provider
// ---------------------------------------------------------------------------

/// `TableProvider` for `pg_catalog.pg_stat_activity` backed by the process-wide
/// [`ConnectionRegistry`]. Each `scan()` call snapshots the live session list
/// for the calling project, so the view always reflects currently-active sessions
/// without any per-query caching overhead.
pub(crate) struct PgStatActivityLiveProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
    registry: ConnectionRegistry,
}

impl std::fmt::Debug for PgStatActivityLiveProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStatActivityLiveProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgStatActivityLiveProvider {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        project: ProjectId,
        registry: ConnectionRegistry,
    ) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_stat_activity_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
            schema: Arc::new(df_schema),
            registry,
        })
    }
}

#[async_trait]
impl TableProvider for PgStatActivityLiveProvider {
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
        // Snapshot active sessions from the connection registry.
        let sessions = self.registry.snapshot(&self.project);

        // Build the pg_stat_activity batch. If no sessions are registered
        // (e.g. during engine unit tests that don't wire the registry),
        // fall back to the catalog stub which provides a single synthetic row.
        let ws_batch = if sessions.is_empty() {
            InfoSchemaQuery::pg_stat_activity(self.catalog.as_ref(), &self.project)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            let ws_schema = InfoSchemaQuery::pg_stat_activity_schema();
            let n = sessions.len();
            // Pre-allocate owned strings to avoid temporary lifetime issues.
            let project_str = self.project.to_string();
            let app_names: Vec<String> =
                sessions.iter().map(|s| s.application_name.clone()).collect();
            let states: Vec<String> = sessions.iter().map(|s| s.state.clone()).collect();
            let queries: Vec<Option<String>> =
                sessions.iter().map(|s| s.query.clone()).collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![None::<i64>; n])), // datid (synthetic)
                Arc::new(StringArray::from(vec![Some("basin"); n])),
                Arc::new(Int32Array::from(sessions.iter().map(|s| s.pid).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(vec![None::<i64>; n])), // usesysid (synthetic)
                Arc::new(StringArray::from(vec![Some(project_str.as_str()); n])),
                Arc::new(StringArray::from(
                    app_names
                        .iter()
                        .map(|a| Some(a.as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![None::<&str>; n])), // client_addr
                Arc::new(StringArray::from(vec![None::<&str>; n])), // client_hostname
                Arc::new(Int32Array::from(vec![None::<i32>; n])),   // client_port
                Arc::new(StringArray::from(vec![None::<&str>; n])), // backend_start
                Arc::new(StringArray::from(vec![None::<&str>; n])), // xact_start
                Arc::new(StringArray::from(
                    sessions
                        .iter()
                        .map(|s| s.query_start.map(|_| "now"))
                        .collect::<Vec<_>>(),
                )), // query_start
                Arc::new(StringArray::from(vec![None::<&str>; n])), // state_change
                Arc::new(StringArray::from(vec![None::<&str>; n])), // wait_event_type
                Arc::new(StringArray::from(vec![None::<&str>; n])), // wait_event
                Arc::new(StringArray::from(
                    states
                        .iter()
                        .map(|s| Some(s.as_str()))
                        .collect::<Vec<_>>(),
                )), // state
                Arc::new(Int64Array::from(vec![None::<i64>; n])), // backend_xid
                Arc::new(Int64Array::from(vec![None::<i64>; n])), // backend_xmin
                Arc::new(StringArray::from(
                    queries
                        .iter()
                        .map(|q| q.as_deref())
                        .collect::<Vec<_>>(),
                )), // query
                Arc::new(StringArray::from(vec![Some("client backend"); n])), // backend_type
            ];
            arrow_array::RecordBatch::try_new(ws_schema, columns)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

// ---------------------------------------------------------------------------
// Phase 5.23.D: live pg_locks provider
// ---------------------------------------------------------------------------

/// `TableProvider` for `pg_catalog.pg_locks` backed by the process-wide
/// [`basin_shard::LockRegistry`]. Each `scan()` call snapshots the current
/// lock state for the calling project so that `pg_locks` reflects real held
/// and waiting locks rather than a static empty stub.
///
/// ## Composition with `lock_wait.rs`
///
/// [`basin_shard::lock_wait::bounded_lock_wait`] drives the async acquisition
/// loop (retries + deadline). `PgLocksLiveProvider` is the observer: it reads
/// what `LockRegistry` records after acquisition succeeds or while a waiter
/// is parked, without duplicating the wait-loop logic.
pub(crate) struct PgLocksLiveProvider {
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    schema: DfSchemaRef,
    registry: basin_shard::LockRegistry,
}

impl std::fmt::Debug for PgLocksLiveProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgLocksLiveProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl PgLocksLiveProvider {
    pub(crate) fn new(
        catalog: Arc<dyn Catalog>,
        project: ProjectId,
        registry: basin_shard::LockRegistry,
    ) -> DfResult<Self> {
        let ws_schema = InfoSchemaQuery::pg_locks_schema();
        let df_schema = schema_ws_to_df(ws_schema.as_ref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            catalog,
            project,
            schema: Arc::new(df_schema),
            registry,
        })
    }
}

#[async_trait]
impl TableProvider for PgLocksLiveProvider {
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
        // Snapshot live lock entries for this project.
        let entries = self.registry.snapshot(&self.project);

        let ws_batch = if entries.is_empty() {
            // Fall back to the catalog stub (empty batch) when no locks are
            // registered — preserves the schema even for idle sessions.
            InfoSchemaQuery::pg_locks(self.catalog.as_ref(), &self.project)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            let ws_schema = InfoSchemaQuery::pg_locks_schema();
            let n = entries.len();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(
                    entries
                        .iter()
                        .map(|e| Some(e.locktype.as_str()))
                        .collect::<Vec<_>>(),
                )), // locktype
                Arc::new(Int64Array::from(
                    entries.iter().map(|e| e.database).collect::<Vec<_>>(),
                )), // database
                Arc::new(Int64Array::from(
                    entries.iter().map(|e| e.relation).collect::<Vec<_>>(),
                )), // relation
                Arc::new(Int32Array::from(vec![None::<i32>; n])),   // page
                Arc::new(Int16Array::from(vec![None::<i16>; n])),   // tuple
                Arc::new(StringArray::from(
                    entries
                        .iter()
                        .map(|e| e.virtualxid.as_deref())
                        .collect::<Vec<_>>(),
                )), // virtualxid
                Arc::new(Int64Array::from(vec![None::<i64>; n])),   // transactionid
                Arc::new(Int64Array::from(vec![None::<i64>; n])),   // classid
                Arc::new(Int64Array::from(vec![None::<i64>; n])),   // objid
                Arc::new(Int16Array::from(vec![None::<i16>; n])),   // objsubid
                Arc::new(StringArray::from(
                    entries
                        .iter()
                        .map(|e| format!("1/{}", e.pid))
                        .collect::<Vec<String>>(),
                )), // virtualtransaction
                Arc::new(Int32Array::from(
                    entries.iter().map(|e| Some(e.pid)).collect::<Vec<_>>(),
                )), // pid
                Arc::new(StringArray::from(
                    entries
                        .iter()
                        .map(|e| Some(e.mode.as_str()))
                        .collect::<Vec<_>>(),
                )), // mode
                Arc::new(BooleanArray::from(
                    entries
                        .iter()
                        .map(|e| Some(e.granted))
                        .collect::<Vec<_>>(),
                )), // granted
                Arc::new(BooleanArray::from(vec![Some(true); n])), // fastpath
            ];
            arrow_array::RecordBatch::try_new(ws_schema, columns)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let df_batch =
            batch_ws_to_df(&ws_batch).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partitions = vec![vec![df_batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
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
    project: ProjectId,
    lock_registry: basin_shard::LockRegistry,
    connection_registry: ConnectionRegistry,
) -> DfResult<()> {
    use datafusion::catalog::MemorySchemaProvider;

    // The default catalog name is `datafusion` (see DataFusion's
    // `ConfigOptions::default_catalog`). It's created automatically by
    // `SessionContext::new` because `create_default_catalog_and_schema`
    // defaults to true; we look it up rather than rebuild it so the
    // already-registered `public` schema (which `register_listing_table`
    // populates) is preserved.
    let catalog_name = ctx.state().config_options().catalog.default_catalog.clone();
    let df_catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "default catalog {catalog_name:?} not registered on session"
        ))
    })?;

    // information_schema schema with the `tables` + `columns` + `routines`
    // providers.
    let info_schema = Arc::new(MemorySchemaProvider::new());
    let tables_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaTablesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("tables".to_string(), tables_provider)?;
    let columns_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaColumnsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("columns".to_string(), columns_provider)?;
    let routines_provider: Arc<dyn TableProvider> =
        Arc::new(RoutinesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("routines".to_string(), routines_provider)?;
    df_catalog.register_schema("information_schema", info_schema.clone())?;

    // pg_catalog schema with `pg_class` + `pg_attribute` + `pg_namespace`
    // + `pg_proc`.
    let pg_catalog_schema = Arc::new(MemorySchemaProvider::new());
    let pg_class_provider: Arc<dyn TableProvider> =
        Arc::new(PgClassProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_class".to_string(), pg_class_provider)?;
    let pg_attribute_provider: Arc<dyn TableProvider> =
        Arc::new(PgAttributeProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_attribute".to_string(), pg_attribute_provider)?;
    let pg_namespace_provider: Arc<dyn TableProvider> =
        Arc::new(PgNamespaceProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_namespace".to_string(), pg_namespace_provider)?;
    let pg_proc_provider: Arc<dyn TableProvider> =
        Arc::new(PgProcProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_proc".to_string(), pg_proc_provider)?;
    df_catalog.register_schema("pg_catalog", pg_catalog_schema.clone())?;

    // Phase 5.11.M Tier 3 expansion: pg_index, pg_constraint,
    // information_schema.views, information_schema.schemata. Registered
    // at the tail so concurrent agent extensions (table_constraints /
    // key_column_usage / referential_constraints) merge cleanly.
    let pg_index_provider: Arc<dyn TableProvider> =
        Arc::new(PgIndexProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_index".to_string(), pg_index_provider)?;
    let pg_constraint_provider: Arc<dyn TableProvider> =
        Arc::new(PgConstraintProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_constraint".to_string(), pg_constraint_provider)?;
    let views_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaViewsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("views".to_string(), views_provider)?;
    let schemata_provider: Arc<dyn TableProvider> =
        Arc::new(InfoSchemaSchemataProvider::new(catalog.clone(), project)?);
    info_schema.register_table("schemata".to_string(), schemata_provider)?;

    // Phase 5.11.M Tier 3 (constraint introspection):
    // information_schema.table_constraints, .key_column_usage,
    // .referential_constraints. Registered last so concurrent-agent
    // additions above merge cleanly.
    let table_constraints_provider: Arc<dyn TableProvider> =
        Arc::new(TableConstraintsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("table_constraints".to_string(), table_constraints_provider)?;
    let key_column_usage_provider: Arc<dyn TableProvider> =
        Arc::new(KeyColumnUsageProvider::new(catalog.clone(), project)?);
    info_schema.register_table("key_column_usage".to_string(), key_column_usage_provider)?;
    let referential_constraints_provider: Arc<dyn TableProvider> = Arc::new(
        ReferentialConstraintsProvider::new(catalog.clone(), project)?,
    );
    info_schema.register_table(
        "referential_constraints".to_string(),
        referential_constraints_provider,
    )?;

    // Phase 5.11.M Tier 3 (type introspection): pg_catalog.pg_type. Static
    // row set listing PG built-ins Basin's pgwire layer advertises;
    // pgAdmin's column-detail query joins against this view to resolve
    // type names from `pg_attribute.atttypid`.
    let pg_type_provider: Arc<dyn TableProvider> =
        Arc::new(PgTypeProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_type".to_string(), pg_type_provider)?;

    // Phase 5.11.M tail: pg_catalog.pg_depend + pg_catalog.pg_authid.
    // Lower-priority catalog completeness — admin scripts / pg_dump
    // resolution paths land here. Tail-appended so concurrent agents
    // adding more views earlier in the chain merge cleanly.
    let pg_depend_provider: Arc<dyn TableProvider> =
        Arc::new(PgDependProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_depend".to_string(), pg_depend_provider)?;
    let pg_authid_provider: Arc<dyn TableProvider> =
        Arc::new(PgAuthidProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_authid".to_string(), pg_authid_provider)?;

    // Bulk catalog expansion: pg_catalog additions
    // Bulk catalog expansion: pg_catalog base views (agent-bulk-catalog-views pattern)
    let pg_database_provider: Arc<dyn TableProvider> =
        Arc::new(PgDatabaseProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_database".to_string(), pg_database_provider)?;
    let pg_roles_provider: Arc<dyn TableProvider> =
        Arc::new(PgRolesProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_roles".to_string(), pg_roles_provider)?;
    let pg_views_provider: Arc<dyn TableProvider> =
        Arc::new(PgViewsProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_views".to_string(), pg_views_provider)?;
    let pg_indexes_provider: Arc<dyn TableProvider> =
        Arc::new(PgIndexesProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_indexes".to_string(), pg_indexes_provider)?;
    let pg_tables_provider: Arc<dyn TableProvider> =
        Arc::new(PgTablesProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_tables".to_string(), pg_tables_provider)?;
    let pg_settings_provider: Arc<dyn TableProvider> =
        Arc::new(PgSettingsProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_settings".to_string(), pg_settings_provider)?;
    let pg_extension_provider: Arc<dyn TableProvider> =
        Arc::new(PgExtensionProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_extension".to_string(), pg_extension_provider)?;
    let pg_description_provider: Arc<dyn TableProvider> =
        Arc::new(PgDescriptionProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_description".to_string(), pg_description_provider)?;
    let pg_stat_user_tables_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatUserTablesProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_user_tables".to_string(),
        pg_stat_user_tables_provider,
    )?;
    let pg_stat_user_indexes_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatUserIndexesProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_user_indexes".to_string(),
        pg_stat_user_indexes_provider,
    )?;
    // Phase 5.23.D: use the live provider backed by LockRegistry so that
    // `pg_locks` reflects real held/waiting locks for the calling project.
    let pg_locks_provider: Arc<dyn TableProvider> =
        Arc::new(PgLocksLiveProvider::new(catalog.clone(), project, lock_registry)?);
    pg_catalog_schema.register_table("pg_locks".to_string(), pg_locks_provider)?;
    // Phase 5.23.C: use the live provider backed by ConnectionRegistry so that
    // `pg_stat_activity` reflects the current session list for the project.
    let pg_stat_activity_provider: Arc<dyn TableProvider> = Arc::new(
        PgStatActivityLiveProvider::new(catalog.clone(), project, connection_registry)?,
    );
    pg_catalog_schema.register_table("pg_stat_activity".to_string(), pg_stat_activity_provider)?;

    // Bulk catalog expansion: information_schema additions
    // New pg_stat_* views (agent-bulk-catalog-extras)
    let pg_stat_database_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatDatabaseProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_stat_database".to_string(), pg_stat_database_provider)?;
    let pg_stat_bgwriter_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatBgwriterProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_stat_bgwriter".to_string(), pg_stat_bgwriter_provider)?;
    let pg_stat_replication_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatReplicationProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_replication".to_string(),
        pg_stat_replication_provider,
    )?;
    let pg_stat_archiver_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatArchiverProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table("pg_stat_archiver".to_string(), pg_stat_archiver_provider)?;
    let pg_stat_wal_receiver_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatWalReceiverProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_wal_receiver".to_string(),
        pg_stat_wal_receiver_provider,
    )?;
    let pg_stat_subscription_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatSubscriptionProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_subscription".to_string(),
        pg_stat_subscription_provider,
    )?;
    let pg_stat_user_functions_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatUserFunctionsProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_user_functions".to_string(),
        pg_stat_user_functions_provider,
    )?;
    let pg_stat_progress_vacuum_provider: Arc<dyn TableProvider> =
        Arc::new(PgStatProgressVacuumProvider::new(catalog.clone(), project)?);
    pg_catalog_schema.register_table(
        "pg_stat_progress_vacuum".to_string(),
        pg_stat_progress_vacuum_provider,
    )?;
    let pg_stat_progress_create_index_provider: Arc<dyn TableProvider> = Arc::new(
        PgStatProgressCreateIndexProvider::new(catalog.clone(), project)?,
    );
    pg_catalog_schema.register_table(
        "pg_stat_progress_create_index".to_string(),
        pg_stat_progress_create_index_provider,
    )?;
    let pg_stat_progress_analyze_provider: Arc<dyn TableProvider> = Arc::new(
        PgStatProgressAnalyzeProvider::new(catalog.clone(), project)?,
    );
    pg_catalog_schema.register_table(
        "pg_stat_progress_analyze".to_string(),
        pg_stat_progress_analyze_provider,
    )?;

    // information_schema additions: check_constraints, triggers
    let check_constraints_provider: Arc<dyn TableProvider> =
        Arc::new(CheckConstraintsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("check_constraints".to_string(), check_constraints_provider)?;
    let triggers_provider: Arc<dyn TableProvider> =
        Arc::new(TriggersProvider::new(catalog.clone(), project)?);
    info_schema.register_table("triggers".to_string(), triggers_provider)?;
    let sequences_provider: Arc<dyn TableProvider> =
        Arc::new(SequencesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("sequences".to_string(), sequences_provider)?;
    let domains_provider: Arc<dyn TableProvider> =
        Arc::new(DomainsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("domains".to_string(), domains_provider)?;
    let parameters_provider: Arc<dyn TableProvider> =
        Arc::new(ParametersProvider::new(catalog.clone(), project)?);
    info_schema.register_table("parameters".to_string(), parameters_provider)?;
    let role_table_grants_provider: Arc<dyn TableProvider> =
        Arc::new(RoleTableGrantsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("role_table_grants".to_string(), role_table_grants_provider)?;
    let user_defined_types_provider: Arc<dyn TableProvider> =
        Arc::new(UserDefinedTypesProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "user_defined_types".to_string(),
        user_defined_types_provider,
    )?;
    let column_domain_usage_provider: Arc<dyn TableProvider> =
        Arc::new(ColumnDomainUsageProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "column_domain_usage".to_string(),
        column_domain_usage_provider,
    )?;
    let column_udt_usage_provider: Arc<dyn TableProvider> =
        Arc::new(ColumnUdtUsageProvider::new(catalog.clone(), project)?);
    info_schema.register_table("column_udt_usage".to_string(), column_udt_usage_provider)?;

    // information_schema privilege views
    let usage_privileges_provider: Arc<dyn TableProvider> =
        Arc::new(UsagePrivilegesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("usage_privileges".to_string(), usage_privileges_provider)?;
    let table_privileges_provider: Arc<dyn TableProvider> =
        Arc::new(TablePrivilegesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("table_privileges".to_string(), table_privileges_provider)?;
    let column_privileges_provider: Arc<dyn TableProvider> =
        Arc::new(ColumnPrivilegesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("column_privileges".to_string(), column_privileges_provider)?;
    let role_column_grants_provider: Arc<dyn TableProvider> =
        Arc::new(RoleColumnGrantsProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "role_column_grants".to_string(),
        role_column_grants_provider,
    )?;
    let role_routine_grants_provider: Arc<dyn TableProvider> =
        Arc::new(RoleRoutineGrantsProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "role_routine_grants".to_string(),
        role_routine_grants_provider,
    )?;
    let applicable_roles_provider: Arc<dyn TableProvider> =
        Arc::new(ApplicableRolesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("applicable_roles".to_string(), applicable_roles_provider)?;
    let enabled_roles_provider: Arc<dyn TableProvider> =
        Arc::new(EnabledRolesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("enabled_roles".to_string(), enabled_roles_provider)?;

    // information_schema FDW stubs
    let foreign_data_wrappers_provider: Arc<dyn TableProvider> =
        Arc::new(ForeignDataWrappersProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "foreign_data_wrappers".to_string(),
        foreign_data_wrappers_provider,
    )?;
    let foreign_data_wrapper_options_provider: Arc<dyn TableProvider> = Arc::new(
        ForeignDataWrapperOptionsProvider::new(catalog.clone(), project)?,
    );
    info_schema.register_table(
        "foreign_data_wrapper_options".to_string(),
        foreign_data_wrapper_options_provider,
    )?;
    let foreign_servers_provider: Arc<dyn TableProvider> =
        Arc::new(ForeignServersProvider::new(catalog.clone(), project)?);
    info_schema.register_table("foreign_servers".to_string(), foreign_servers_provider)?;
    let foreign_server_options_provider: Arc<dyn TableProvider> =
        Arc::new(ForeignServerOptionsProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "foreign_server_options".to_string(),
        foreign_server_options_provider,
    )?;
    let foreign_tables_provider: Arc<dyn TableProvider> =
        Arc::new(ForeignTablesProvider::new(catalog.clone(), project)?);
    info_schema.register_table("foreign_tables".to_string(), foreign_tables_provider)?;
    let foreign_table_options_provider: Arc<dyn TableProvider> =
        Arc::new(ForeignTableOptionsProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "foreign_table_options".to_string(),
        foreign_table_options_provider,
    )?;
    let user_mappings_provider: Arc<dyn TableProvider> =
        Arc::new(UserMappingsProvider::new(catalog.clone(), project)?);
    info_schema.register_table("user_mappings".to_string(), user_mappings_provider)?;
    let user_mapping_options_provider: Arc<dyn TableProvider> =
        Arc::new(UserMappingOptionsProvider::new(catalog.clone(), project)?);
    info_schema.register_table(
        "user_mapping_options".to_string(),
        user_mapping_options_provider,
    )?;

    Ok(())
}

/// Phase 5.21.B/E — register CDC-related virtual table providers in the
/// `pg_catalog` schema of the given session context.
///
/// Registers:
/// - `pg_replication_slots` — live view of active logical replication slots.
/// - `pg_publication` — logical replication publication list.
/// - `pg_publication_tables` — per-publication table membership.
///
/// Called from `session::open` after `register_info_schema_providers` so the
/// pg_catalog schema already exists and we can look it up by name.
pub(crate) fn register_cdc_providers(
    ctx: &datafusion::prelude::SessionContext,
    slot_registry: std::sync::Arc<basin_catalog::SlotRegistry>,
    publication_registry: std::sync::Arc<basin_catalog::PublicationRegistry>,
    project: ProjectId,
) -> DfResult<()> {
    let catalog_name = ctx.state().config_options().catalog.default_catalog.clone();
    let df_catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "default catalog {catalog_name:?} not registered on session"
        ))
    })?;

    // Ensure pg_catalog schema exists (it is created by
    // register_info_schema_providers but may not be present in test stubs).
    let pg_catalog_schema = if let Some(s) = df_catalog.schema("pg_catalog") {
        s
    } else {
        let s = std::sync::Arc::new(datafusion::catalog::MemorySchemaProvider::new());
        df_catalog.register_schema("pg_catalog", s.clone())?;
        s
    };

    let repl_slots: std::sync::Arc<dyn TableProvider> = std::sync::Arc::new(
        crate::replication::slot_udf::PgReplicationSlotsProvider::new(slot_registry, project),
    );
    // Ignore already-exists errors (idempotent registration).
    let _ = pg_catalog_schema
        .register_table("pg_replication_slots".to_string(), repl_slots);

    let pub_provider: std::sync::Arc<dyn TableProvider> = std::sync::Arc::new(
        crate::replication::slot_udf::PgPublicationProvider::new(
            publication_registry.clone(),
            project,
        ),
    );
    let _ = pg_catalog_schema.register_table("pg_publication".to_string(), pub_provider);

    let pub_tables_provider: std::sync::Arc<dyn TableProvider> = std::sync::Arc::new(
        crate::replication::slot_udf::PgPublicationTablesProvider::new(
            publication_registry,
            project,
        ),
    );
    let _ = pg_catalog_schema
        .register_table("pg_publication_tables".to_string(), pub_tables_provider);

    Ok(())
}
