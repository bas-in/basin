//! Basin-local wrapper around DataFusion's [`ParquetFormat`] that applies the
//! INTERVAL storage disguise's read-side inverse.
//!
//! ## Why a wrapper is needed at all
//!
//! Parquet cannot store Arrow's `Interval(MonthDayNano)` — arrow-rs 58.4.0
//! refuses it on write ("Attempting to write an Arrow interval type
//! MonthDayNano to parquet") and has it NYI on read. basin-storage therefore
//! stores interval columns as `LargeBinary`, 16 bytes per row
//! (`months|days|nanos`, little-endian) on Parquet exactly as it does on
//! Vortex.
//!
//! That makes Parquet different from the ADR-0024 UUID and the POINT
//! disguises, which are Vortex-only workarounds (Parquet has a native
//! `FixedSizeBinary(N)`) and so never needed a Parquet-side wrapper. For
//! INTERVAL the catalog says `Interval(MonthDayNano)` and the file says
//! `LargeBinary`, so an unwrapped `ParquetFormat` fails the scan with:
//!
//! ```text
//! Cannot cast column 'iv' from 'LargeBinary' (physical data type)
//! to 'Interval(MonthDayNano)' (logical data type)
//! ```
//!
//! The fix mirrors `BasinVortexFormat`'s, using the shared helpers in
//! [`crate::interval_storage`]:
//!
//! 1. [`BasinParquetFormat::file_source`] rewrites interval fields in the
//!    file schema to `LargeBinary` — the type the Parquet reader actually
//!    emits — so no cast is attempted, and
//! 2. [`BasinParquetFormat::create_physical_plan`] stacks an
//!    `IntervalRestoreExec` on the plan, which decodes the blobs back to
//!    `Interval(MonthDayNano)`.
//!
//! Every other `FileFormat` method is an exact pass-through, so tables
//! without an interval column behave byte-identically to the bare
//! `ParquetFormat` they used before this wrapper existed.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::common::Statistics;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileMeta;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::TableSchema;
use object_store::{ObjectMeta, ObjectStore};

/// Wraps [`ParquetFormat`] to undo the INTERVAL-as-`LargeBinary` storage
/// disguise on the DataFusion read path.
pub(crate) struct BasinParquetFormat {
    inner: Arc<ParquetFormat>,
}

impl BasinParquetFormat {
    pub(crate) fn new(inner: Arc<ParquetFormat>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for BasinParquetFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasinParquetFormat")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl FileFormat for BasinParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<FileMeta> {
        self.inner
            .infer_stats_and_ordering(state, store, table_schema, object)
            .await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    /// Claim the PHYSICAL type for interval columns (`LargeBinary` — what
    /// basin-storage wrote and what the Parquet reader emits) so DataFusion's
    /// schema adapter never attempts the unsupported
    /// `LargeBinary` → `Interval(MonthDayNano)` cast. `create_physical_plan`
    /// stacks the restore exec that converts back.
    ///
    /// Tables with no interval column take the untouched `table_schema`
    /// straight to the inner format.
    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let file_schema = table_schema.file_schema().clone();
        if !crate::interval_storage::schema_has_interval_native(file_schema.as_ref()) {
            return self.inner.file_source(table_schema);
        }
        let physical = crate::interval_storage::swap_interval_to_physical(
            file_schema.as_ref(),
            DataType::LargeBinary,
        );
        let physical_table_schema = TableSchema::new(
            Arc::new(physical),
            table_schema.table_partition_cols().clone(),
        );
        self.inner.file_source(physical_table_schema)
    }

    /// Delegate to `ParquetFormat`, then stack `IntervalRestoreExec` when the
    /// resulting plan carries binary-family `BASIN_TYPE=INTERVAL` columns.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.create_physical_plan(state, conf).await?;
        if crate::interval_storage::schema_has_interval_binary(plan.schema().as_ref()) {
            return Ok(Arc::new(crate::interval_storage::IntervalRestoreExec::new(
                plan,
            )));
        }
        Ok(plan)
    }
}
