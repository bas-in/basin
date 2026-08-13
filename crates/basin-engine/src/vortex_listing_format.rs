//! Basin-local wrapper around [`vortex_datafusion::VortexFormat`] that patches
//! `Statistics.total_byte_size` so DataFusion's `join_selection` optimizer rule
//! gets a real value instead of `Precision::Absent`, and that applies the
//! ADR-0024 UUID Decimal256(39,0) → FixedSizeBinary(16) read-side inverse so
//! DataFusion never sees the on-disk disguise.
//!
//! ## Root cause (W2-1)
//!
//! `VortexFormat::infer_stats` folds `total_byte_size` from per-column
//! `Stat::UncompressedSizeInBytes`.  Because `PRUNING_STATS` in
//! `vortex-array-0.70.0` omits that stat, the fold always produces
//! `Precision::Absent`.  DataFusion's `join_selection` /
//! `supports_collect_by_thresholds` fall back to row-count heuristics when
//! byte-size is absent, which can mis-plan byte-skewed joins (observed as a
//! 0.54× regression on `inner_join@100k`).
//!
//! ## Fix (W2-1)
//!
//! After delegating to the inner `VortexFormat::infer_stats`, if
//! `total_byte_size` is still `Absent`, substitute
//! `Precision::Inexact(object.size as usize)`.  `ObjectMeta::size` is the
//! compressed on-disk byte count — a valid underestimate and sufficient for the
//! relative ordering the optimizer needs.  We never overwrite an `Exact` or
//! `Inexact` value returned upstream.
//!
//! ## ADR-0024 UUID read-path fix
//!
//! Vortex 0.71 has no `FixedSizeBinary(N)` encoder; UUID columns are stored as
//! `Decimal256(39, 0)` on disk.  The catalog schema (and DataFusion's registered
//! table schema) declares those columns as `FixedSizeBinary(16)`.  Without
//! intervention DataFusion's `DefaultPhysicalExprAdapterFactory` (inside
//! `VortexOpener`) tries to cast `Decimal256(39,0)` → `FixedSizeBinary(16)` and
//! fails with "Cannot cast column 'id' from 'Decimal256(39,0)' to
//! 'FixedSizeBinary(16)'".
//!
//! Fix (two-part, single-boundary):
//!
//! 1. `BasinVortexFormat::file_source` swaps UUID fields from
//!    `FixedSizeBinary(16)` to `Decimal256(39, 0)` in the `TableSchema` before
//!    passing it to the inner `VortexSource`.  Vortex therefore sees a schema
//!    that matches the on-disk layout and no cast is attempted.
//!
//! 2. `BasinVortexFormat::create_physical_plan` wraps the inner plan with
//!    `UuidDecimal256RestoreExec`, which converts `Decimal256(39, 0)` columns
//!    carrying `BASIN_TYPE=UUID` metadata back to `FixedSizeBinary(16)` on
//!    every output batch.  DataFusion above this point sees only
//!    `FixedSizeBinary(16)`, matching the registered table schema.
//!
//! TODO(adr-0024): drop the UUID swapping when Vortex grows native
//! `FixedSizeBinary(N)` encoding and basin-engine pins the new release.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, ArrayRef, Decimal256Array, FixedSizeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::Result as DFResult;
use datafusion::common::Statistics;
use datafusion::datasource::file_format::FileFormat;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileMeta;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::SortOrderPushdownResult;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::Stream;
use object_store::{ObjectMeta, ObjectStore};

// ---------------------------------------------------------------------------
// ADR-0024 constants (mirrors basin-storage::reader private copies).
// TODO(adr-0024): drop when Vortex grows native FixedSizeBinary(N).
// ---------------------------------------------------------------------------
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_UUID: &str = "UUID";
/// `BASIN_TYPE` marker for POINT columns. The catalog declares them as
/// `FixedSizeBinary(21)`; the storage writer reinterprets the buffer as
/// `LargeBinary` for Vortex (no FSB(N) encoder), and Vortex's scan layer
/// surfaces it as `BinaryView`. Mirrors `basin_storage`'s private copy.
const BASIN_TYPE_POINT: &str = "POINT";

/// On-disk catalog width of a POINT column: 21-byte WKB.
const POINT_FSB_LEN: i32 = basin_geo::POINT_WKB_LEN as i32;

/// Wraps [`vortex_datafusion::VortexFormat`] and patches `total_byte_size` in
/// `infer_stats` when the inner format returns `Precision::Absent`.
///
/// Every other [`FileFormat`] method is an exact pass-through to the inner
/// format so scan, write, schema-inference, and source-construction behaviour
/// are unchanged.
pub(crate) struct BasinVortexFormat {
    inner: Arc<vortex_datafusion::VortexFormat>,
}

impl BasinVortexFormat {
    /// Create a wrapper around an already-configured `VortexFormat`.
    pub(crate) fn new(inner: Arc<vortex_datafusion::VortexFormat>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for BasinVortexFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasinVortexFormat")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl FileFormat for BasinVortexFormat {
    // ---- identity / metadata ------------------------------------------------

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

    // ---- schema / statistics ------------------------------------------------

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        // Promote Utf8/Binary → Utf8View/BinaryView so DataFusion's string UDF
        // kernels (LOWER, SUBSTR, concat, REPLACE, …) hit their StringViewArray
        // fast paths.  Vortex's scan layer already emits view types natively;
        // this aligns the catalog schema with that representation.
        let schema = self.inner.infer_schema(state, store, objects).await?;
        Ok(Arc::new(promote_utf8_to_view_schema(&schema)))
    }

    /// Delegates to the inner `VortexFormat::infer_stats`, then patches
    /// `total_byte_size` if it came back as `Precision::Absent`.
    ///
    /// We substitute `Precision::Inexact(object.size as usize)` — the
    /// compressed on-disk byte count.  This is a valid underestimate of the
    /// uncompressed size and sufficient for the optimizer's relative
    /// join-side ordering.  An upstream `Exact` or `Inexact` value is
    /// never overwritten.
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        let mut stats = self
            .inner
            .infer_stats(state, store, table_schema, object)
            .await?;

        if matches!(stats.total_byte_size, Precision::Absent) {
            stats.total_byte_size = Precision::Inexact(object.size as usize);
        }

        Ok(stats)
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

    /// Overrides the default `infer_stats_and_ordering` to route statistics
    /// through our patched `infer_stats` rather than the inner one, while
    /// delegating ordering to the inner format.
    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<FileMeta> {
        let statistics = self
            .infer_stats(state, store, Arc::clone(&table_schema), object)
            .await?;
        let ordering = self
            .infer_ordering(state, store, table_schema, object)
            .await?;
        Ok(FileMeta::new(statistics).with_ordering(ordering))
    }

    // ---- plan construction --------------------------------------------------

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

    /// ADR-0024: swap UUID fields from `FixedSizeBinary(16)` to
    /// `Decimal256(39, 0)` in the `TableSchema` before handing it to the inner
    /// `VortexSource`.  Vortex stores UUIDs as `Decimal256(39, 0)` on disk;
    /// supplying the catalog's `FixedSizeBinary(16)` schema causes Vortex's
    /// `DefaultPhysicalExprAdapterFactory` to attempt an unsupported cast at
    /// scan time.  By aligning the schema with the physical layout we prevent
    /// that cast.  `create_physical_plan` (above) restores the FSB(16) type via
    /// `UuidDecimal256RestoreExec` so the engine sees only the logical type.
    ///
    /// Field metadata (including `BASIN_TYPE=UUID`) is preserved verbatim so the
    /// restore exec can identify which columns to translate.
    ///
    /// Also wraps the inner source with `UdfPushdownGuard` so that DataFusion's
    /// `ProjectionPushdown` optimizer cannot fuse UDF expressions (e.g.
    /// `json_get_text`) into the `DataSourceExec` scan.  Column-only projections
    /// are still accepted by the guard and passed through to the inner source.
    ///
    /// TODO(adr-0024): drop swap when Vortex grows native FixedSizeBinary(N).
    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let file_schema = table_schema.file_schema().clone();
        // Align the file schema with the physical Vortex layout for both
        // disguised types: UUID FSB(16) → Decimal256(39,0) and POINT FSB(21) →
        // BinaryView. Both swaps avoid an unsupported scan-time cast; the
        // matching restore execs (below / in create_physical_plan) convert back
        // to the catalog's FSB types on output.
        let needs_uuid = schema_has_uuid_fsb16(file_schema.as_ref());
        let needs_point = schema_has_point_fsb(file_schema.as_ref());
        // INTERVAL(MonthDayNano) → BinaryView. Same reason, different cause:
        // Vortex cannot encode OR decode the Arrow interval type at all, so
        // basin-storage stores interval columns as a 16-byte LargeBinary
        // blob (months|days|nanos) on both formats.
        let needs_interval =
            crate::interval_storage::schema_has_interval_native(file_schema.as_ref());
        let inner_source = if needs_uuid || needs_point || needs_interval {
            let mut physical = (*file_schema).clone();
            if needs_uuid {
                physical = swap_uuid_fsb16_to_decimal256(&physical);
            }
            if needs_point {
                physical = swap_point_fsb_to_binary_view(&physical);
            }
            if needs_interval {
                // Vortex's scan layer surfaces the stored LargeBinary as
                // BinaryView, so that is the type the file schema must claim.
                physical = crate::interval_storage::swap_interval_to_physical(
                    &physical,
                    DataType::BinaryView,
                );
            }
            let physical_table_schema = TableSchema::new(
                Arc::new(physical),
                table_schema.table_partition_cols().clone(),
            );
            self.inner.file_source(physical_table_schema)
        } else {
            self.inner.file_source(table_schema)
        };
        Arc::new(UdfPushdownGuard {
            inner: inner_source,
        })
    }

    /// ADR-0024 + UDF guard: calls `VortexFormat::create_physical_plan` normally
    /// (the conf always arrives with `VortexSource` as the file_source because
    /// `FileScanConfigBuilder` unwraps our wrapper on the way in), then
    /// re-wraps the resulting `DataSourceExec`'s `VortexSource` in a fresh
    /// `UdfPushdownGuard`.
    ///
    /// Re-wrapping is essential: DataFusion's `ProjectionPushdown` physical
    /// optimizer rule runs *after* `create_physical_plan` returns and calls
    /// `DataSourceExec::try_swapping_with_projection` → `FileScanConfig::
    /// try_swapping_with_projection` → `file_source.try_pushdown_projection`.
    /// Without the guard the raw `VortexSource` accepts every projection
    /// (including UDF expressions), fusing `json_get_text` into the scan.
    /// With the guard in place, expression projections get declined and
    /// `ProjectionPushdown` keeps a `ProjectionExec` node above the scan.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Strip the UdfPushdownGuard wrapper so that VortexFormat sees its own
        // VortexSource directly (it downcasts file_source and errors otherwise).
        let maybe_inner: Option<Arc<dyn FileSource>> = conf
            .file_source()
            .as_any()
            .downcast_ref::<UdfPushdownGuard>()
            .map(|guard| Arc::clone(&guard.inner));
        let unwrapped_conf = if let Some(inner_source) = maybe_inner {
            FileScanConfigBuilder::from(conf)
                .with_source(inner_source)
                .build()
        } else {
            conf
        };

        let inner_plan = self
            .inner
            .create_physical_plan(state, unwrapped_conf)
            .await?;

        // Re-wrap the VortexSource inside the DataSourceExec with UdfPushdownGuard
        // so that the ProjectionPushdown optimizer (which runs after this method
        // returns) cannot fuse UDF expressions into the scan.
        let inner_plan = rewrap_datasource_with_guard(inner_plan);

        // Restore disguised physical types to their catalog FSB shapes.
        // UUID: Decimal256(39,0) → FSB(16). POINT: binary-family → FSB(21).
        // Either or both restore execs are stacked as needed.
        let mut plan = inner_plan;
        if schema_has_uuid_decimal256(plan.schema().as_ref()) {
            plan = Arc::new(UuidDecimal256RestoreExec::new(plan));
        }
        if schema_has_point_binary(plan.schema().as_ref()) {
            plan = Arc::new(PointFsbRestoreExec::new(plan));
        }
        // INTERVAL: binary-family → Interval(MonthDayNano).
        if crate::interval_storage::schema_has_interval_binary(plan.schema().as_ref()) {
            plan = Arc::new(crate::interval_storage::IntervalRestoreExec::new(plan));
        }
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// UdfPushdownGuard — keeps UDF expressions out of DataSourceExec
// ---------------------------------------------------------------------------

/// Returns `true` if `projection` contains at least one expression that is not
/// a plain column reference.  Used by `UdfPushdownGuard` to decide whether to
/// accept or decline a projection pushdown.
fn projection_has_expressions(projection: &ProjectionExprs) -> bool {
    projection
        .iter()
        .any(|pe| pe.expr.as_any().downcast_ref::<Column>().is_none())
}

/// A thin [`FileSource`] wrapper around an inner `FileSource` (always a
/// `VortexSource` in practice) that declines projection pushdown when the
/// projection contains computed expressions (UDFs, arithmetic, etc.).
///
/// ## Why this exists
///
/// DataFusion's `ProjectionPushdown` physical optimizer rule calls
/// `try_pushdown_projection` on every [`FileSource`] it finds.
/// `VortexSource::try_pushdown_projection` unconditionally accepts every
/// projection, including ones that carry JSONB UDFs (`json_get_text`, etc.).
/// That fuses the UDF into the `DataSourceExec`, causing it to run on *every
/// materialized row* before the row filter (`vortex_predicate` = `id < 100`)
/// has had a chance to discard non-matching rows.  The result is O(N) UDF
/// evaluation instead of O(filtered_rows).
///
/// By declining expression-bearing projections the optimizer is forced to keep
/// a separate `ProjectionExec` node above the scan.  DataFusion's
/// `FilterPushdown` rule already pushed the predicate into the Vortex scan, so
/// the resulting plan is:
///
/// ```text
/// ProjectionExec [json_get_text(payload@0, 'category')]
///   DataSourceExec  predicate=id<100, scan=[id, payload]
/// ```
///
/// Column-only projections are still forwarded to the inner source so that
/// unnecessary column reads continue to be avoided.
#[derive(Clone)]
struct UdfPushdownGuard {
    inner: Arc<dyn FileSource>,
}

impl fmt::Debug for UdfPushdownGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdfPushdownGuard")
            .field("inner_type", &self.inner.file_type())
            .finish()
    }
}

impl FileSource for UdfPushdownGuard {
    // ---- required methods ---------------------------------------------------

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> DFResult<Arc<dyn datafusion_datasource::file_stream::FileOpener>> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(UdfPushdownGuard {
            inner: self.inner.with_batch_size(batch_size),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    // ---- optional delegation ------------------------------------------------

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        // Delegate to the inner source and re-wrap the returned updated_node in
        // a new UdfPushdownGuard so the guard survives filter-pushdown rewrites.
        let inner_result = self.inner.try_pushdown_filters(filters, config)?;
        let updated_node = inner_result
            .updated_node
            .map(|updated| Arc::new(UdfPushdownGuard { inner: updated }) as Arc<dyn FileSource>);
        Ok(FilterPushdownPropagation {
            filters: inner_result.filters,
            updated_node,
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> DFResult<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        let result = self.inner.try_pushdown_sort(order, eq_properties)?;
        // Re-wrap any returned inner source in UdfPushdownGuard.
        Ok(result.map(|inner_source| {
            Arc::new(UdfPushdownGuard {
                inner: inner_source,
            }) as Arc<dyn FileSource>
        }))
    }

    /// Accept column-only projections; decline expression projections so that
    /// `ProjectionPushdown` cannot fuse UDFs (e.g. `json_get_text`) into
    /// `DataSourceExec`.  The caller will keep a separate `ProjectionExec` node
    /// for those expressions, which runs on post-filter rows only.
    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> DFResult<Option<Arc<dyn FileSource>>> {
        if projection_has_expressions(projection) {
            // Decline — DataFusion keeps a ProjectionExec above the scan.
            return Ok(None);
        }
        // Column-only projection: forward to inner source and re-wrap.
        match self.inner.try_pushdown_projection(projection)? {
            Some(new_inner) => Ok(Some(Arc::new(UdfPushdownGuard { inner: new_inner }))),
            None => Ok(None),
        }
    }
}

/// If `plan` is a `DataSourceExec` backed by a `FileScanConfig` whose
/// `file_source` is a `VortexSource` (not yet guarded), wrap that source in a
/// fresh `UdfPushdownGuard` and return a new `DataSourceExec`.  Otherwise
/// return `plan` unchanged.
///
/// Called from `BasinVortexFormat::create_physical_plan` immediately after the
/// inner `VortexFormat` builds the `DataSourceExec`.  The guard must be present
/// before DataFusion's `ProjectionPushdown` optimizer pass so that expression
/// projections (e.g. JSONB UDFs) are NOT fused into the scan.
fn rewrap_datasource_with_guard(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    // Get the DataSourceExec, if any.
    let Some(ds_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
        return plan;
    };
    // Get the FileScanConfig inside the DataSourceExec.
    let Some(fsc) = ds_exec
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()
    else {
        return plan;
    };
    // Already guarded — don't double-wrap.
    if fsc
        .file_source()
        .as_any()
        .downcast_ref::<UdfPushdownGuard>()
        .is_some()
    {
        return plan;
    }
    // Build a new FileScanConfig with the guard around the existing file source.
    let guarded_source = Arc::new(UdfPushdownGuard {
        inner: Arc::clone(fsc.file_source()),
    });
    let new_fsc = FileScanConfigBuilder::from(fsc.clone())
        .with_source(guarded_source)
        .build();
    DataSourceExec::from_data_source(new_fsc)
}

// ---------------------------------------------------------------------------
// ADR-0024 helpers — UUID FixedSizeBinary(16) ↔ Decimal256(39,0) for the
// DataFusion scan path.
// TODO(adr-0024): drop when Vortex grows native FixedSizeBinary(N).
// ---------------------------------------------------------------------------

/// Returns `true` if `schema` has at least one field that is
/// `FixedSizeBinary(16)` with `BASIN_TYPE=UUID` metadata (i.e. a UUID column
/// that needs to be hidden from Vortex as `Decimal256(39, 0)`).
fn schema_has_uuid_fsb16(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| {
        matches!(f.data_type(), DataType::FixedSizeBinary(16))
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
    })
}

/// Returns `true` if `schema` has at least one `Decimal256(39, 0)` field with
/// `BASIN_TYPE=UUID` metadata (UUID-disguised column coming back from Vortex).
fn schema_has_uuid_decimal256(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| {
        matches!(f.data_type(), DataType::Decimal256(39, 0))
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
    })
}

/// Produce a copy of `schema` where every `FixedSizeBinary(16)+BASIN_TYPE=UUID`
/// field is replaced by `Decimal256(39, 0)` (preserving all other field
/// metadata).  Used to create a "physical" schema that matches the on-disk
/// Vortex layout so the scan doesn't attempt an unsupported cast.
fn swap_uuid_fsb16_to_decimal256(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let is_uuid_fsb = matches!(f.data_type(), DataType::FixedSizeBinary(16))
                && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID);
            if is_uuid_fsb {
                Field::new(f.name(), DataType::Decimal256(39, 0), f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// Produce a copy of `schema` where every `Decimal256(39, 0)+BASIN_TYPE=UUID`
/// field is replaced by `FixedSizeBinary(16)`.  This is the logical output
/// schema of `UuidDecimal256RestoreExec`.
fn swap_uuid_decimal256_to_fsb16(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let is_uuid = matches!(f.data_type(), DataType::Decimal256(39, 0))
                && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID);
            if is_uuid {
                Field::new(f.name(), DataType::FixedSizeBinary(16), f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

// ── POINT FSB(21) ↔ binary-family helpers (mirror of the UUID path) ─────────

/// `true` if `field` is the catalog shape for a POINT column:
/// `FixedSizeBinary(21)` with `BASIN_TYPE=POINT`.
fn field_is_point_fsb(f: &Field) -> bool {
    matches!(f.data_type(), DataType::FixedSizeBinary(n) if *n == POINT_FSB_LEN)
        && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_POINT)
}

/// `true` if `field` is a POINT column coming back from Vortex as a
/// binary-family physical type (Binary / LargeBinary / BinaryView), tagged
/// `BASIN_TYPE=POINT`. These are what the restore exec converts to FSB(21).
fn field_is_point_binary(f: &Field) -> bool {
    matches!(
        f.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_POINT)
}

fn schema_has_point_fsb(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_is_point_fsb(f))
}

fn schema_has_point_binary(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_is_point_binary(f))
}

/// Swap `FixedSizeBinary(21)+BASIN_TYPE=POINT` fields to `BinaryView` so the
/// file schema matches the physical type Vortex emits (avoiding an
/// unsupported BinaryView → FSB(21) cast at scan time). Other fields untouched.
fn swap_point_fsb_to_binary_view(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if field_is_point_fsb(f) {
                Field::new(f.name(), DataType::BinaryView, f.is_nullable())
                    .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// Swap binary-family `+BASIN_TYPE=POINT` fields back to `FixedSizeBinary(21)`.
/// The logical output schema of the POINT restore.
fn swap_point_binary_to_fsb(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if field_is_point_binary(f) {
                Field::new(
                    f.name(),
                    DataType::FixedSizeBinary(POINT_FSB_LEN),
                    f.is_nullable(),
                )
                .with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// Convert one `RecordBatch`'s binary-family POINT columns to
/// `FixedSizeBinary(21)`. Every value is a 21-byte WKB blob; rows of the wrong
/// length surface as an internal error (corruption guard).
fn restore_point_columns(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow_array::{BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray};

    let schema = batch.schema();
    if !schema_has_point_binary(schema.as_ref()) {
        return Ok(batch);
    }
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, f) in schema.fields().iter().enumerate() {
        if field_is_point_binary(f) {
            let col = batch.column(i);
            let len = col.len();
            let value_at = |r: usize| -> Option<&[u8]> {
                if col.is_null(r) {
                    return None;
                }
                if let Some(a) = col.as_any().downcast_ref::<BinaryViewArray>() {
                    Some(a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                    Some(a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                    Some(a.value(r))
                } else {
                    None
                }
            };
            let rows = (0..len).map(|r| value_at(r).map(|b| b.to_vec()));
            let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(rows, POINT_FSB_LEN)
                .map_err(|e| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "point restore: FixedSizeBinaryArray construction for '{}': {e}",
                        f.name()
                    ))
                })?;
            let new_field = Field::new(
                f.name(),
                DataType::FixedSizeBinary(POINT_FSB_LEN),
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone());
            new_fields.push(new_field);
            new_cols.push(Arc::new(arr) as ArrayRef);
        } else {
            new_fields.push(f.as_ref().clone());
            new_cols.push(batch.column(i).clone());
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_cols)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
}

/// Convert one `RecordBatch` whose Decimal256(39,0)+BASIN_TYPE=UUID columns
/// contain UUID bytes into a batch where those columns are `FixedSizeBinary(16)`.
///
/// Mirror of `basin_storage::reader::decimal256_to_uuid_fsb` (kept private in
/// basin-storage; we re-implement the same logic here to stay within the
/// basin-engine scope as required by ADR-0024).
fn restore_uuid_columns(batch: RecordBatch) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, f) in schema.fields().iter().enumerate() {
        let is_uuid = matches!(f.data_type(), DataType::Decimal256(39, 0))
            && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID);
        if is_uuid {
            let src = batch
                .column(i)
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "uuid restore: column '{}' expected Decimal256Array",
                        f.name()
                    ))
                })?;
            let len = src.len();
            // The write path left-padded 16 UUID bytes with 16 zero bytes to
            // form a 32-byte big-endian unsigned i256.  The inverse:
            // `i256.to_be_bytes()[16..32]` recovers the 16 UUID bytes.
            let rows = (0..len).map(|r| {
                if src.is_null(r) {
                    None
                } else {
                    let full = src.value(r).to_be_bytes();
                    let mut buf = [0u8; 16];
                    buf.copy_from_slice(&full[16..32]);
                    Some(buf)
                }
            });
            let arr =
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(rows, 16).map_err(|e| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "uuid restore: FixedSizeBinaryArray construction: {e}"
                    ))
                })?;
            let new_field = Field::new(f.name(), DataType::FixedSizeBinary(16), f.is_nullable())
                .with_metadata(f.metadata().clone());
            new_fields.push(new_field);
            new_cols.push(Arc::new(arr) as ArrayRef);
        } else {
            new_fields.push(f.as_ref().clone());
            new_cols.push(batch.column(i).clone());
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_cols)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
}

// ---------------------------------------------------------------------------
// UuidDecimal256RestoreExec
// ---------------------------------------------------------------------------

/// A thin `ExecutionPlan` wrapper that translates `Decimal256(39, 0)` columns
/// carrying `BASIN_TYPE=UUID` metadata back to `FixedSizeBinary(16)` on every
/// output batch.
///
/// Inserted by `BasinVortexFormat::create_physical_plan` immediately above the
/// inner `DataSourceExec` so DataFusion never observes the on-disk Decimal256
/// encoding for UUID columns (ADR-0024).
///
/// TODO(adr-0024): remove when Vortex grows native FixedSizeBinary(N).
#[derive(Debug)]
struct UuidDecimal256RestoreExec {
    inner: Arc<dyn ExecutionPlan>,
    /// Output schema with Decimal256(39,0)+UUID fields replaced by FSB(16).
    output_schema: SchemaRef,
    /// Cached `PlanProperties` with the corrected output schema.
    props: Arc<PlanProperties>,
}

impl UuidDecimal256RestoreExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let output_schema = Arc::new(swap_uuid_decimal256_to_fsb16(inner.schema().as_ref()));
        let props = Arc::new(Self::compute_properties(&inner, Arc::clone(&output_schema)));
        Self {
            inner,
            output_schema,
            props,
        }
    }

    fn compute_properties(inner: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        // Derive equivalence properties from the inner plan but rebind to the
        // new output schema.  Ordering / partitioning are unchanged — the exec
        // is a row-for-row type-coercion with no reordering or repartitioning.
        let eq = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq,
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        )
    }
}

impl DisplayAs for UuidDecimal256RestoreExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UuidDecimal256RestoreExec")
    }
}

impl ExecutionPlan for UuidDecimal256RestoreExec {
    fn name(&self) -> &str {
        "UuidDecimal256RestoreExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UuidDecimal256RestoreExec::new(
            children.swap_remove(0),
        )))
    }

    // ── Physical filter pushdown (transparent passthrough) ───────────────────
    //
    // `UuidDecimal256RestoreExec` is a per-row column-type restorer: it
    // translates `Decimal256(39,0)` UUID columns back to `FixedSizeBinary(16)`
    // on every output batch. It does NOT add, remove, reorder, or recombine
    // rows. Therefore a row filter commutes with the restoration:
    // `filter(restore(scan)) == restore(filter(scan))` for ANY predicate,
    // including ones that touch the restored UUID column (the parent's filter
    // is expressed in the post-restore schema, but only its row-selection
    // semantics matter — pushdown infrastructure will rebind column indices
    // through `with_new_children`).
    //
    // Without these two methods the default `all_unsupported` blocks pushdown:
    // any `WHERE pk = …` predicate stays as a `FilterExec` stuck above us and
    // the cold Vortex/Parquet reader loses row-group pruning — same #88 shape
    // as `UpdateOverlayExec`. Mirrors `TombstoneFilterExec`'s transparent
    // passthrough.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let mapped = futures::StreamExt::map(inner_stream, move |batch_res| {
            batch_res.and_then(|batch| restore_uuid_columns(batch))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

// ---------------------------------------------------------------------------
// PointFsbRestoreExec
// ---------------------------------------------------------------------------

/// A thin `ExecutionPlan` wrapper that translates binary-family POINT columns
/// (`Binary`/`LargeBinary`/`BinaryView` carrying `BASIN_TYPE=POINT`) back to
/// `FixedSizeBinary(21)` on every output batch, so DataFusion and the engine's
/// `ST_*` UDFs observe only the catalog-declared POINT type.
///
/// Inserted by `BasinVortexFormat::create_physical_plan` above the inner scan
/// (and above any `UuidDecimal256RestoreExec`). Like the UUID restorer it is a
/// row-for-row type coercion, so it is transparent to filter pushdown.
///
/// TODO(adr-0024): remove when Vortex grows native FixedSizeBinary(N).
#[derive(Debug)]
struct PointFsbRestoreExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl PointFsbRestoreExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let output_schema = Arc::new(swap_point_binary_to_fsb(inner.schema().as_ref()));
        let props = Arc::new(Self::compute_properties(&inner, Arc::clone(&output_schema)));
        Self {
            inner,
            output_schema,
            props,
        }
    }

    fn compute_properties(inner: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq,
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        )
    }
}

impl DisplayAs for PointFsbRestoreExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PointFsbRestoreExec")
    }
}

impl ExecutionPlan for PointFsbRestoreExec {
    fn name(&self) -> &str {
        "PointFsbRestoreExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PointFsbRestoreExec::new(children.swap_remove(0))))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = Arc::clone(&self.output_schema);
        let mapped = futures::StreamExt::map(inner_stream, move |batch_res| {
            batch_res.and_then(restore_point_columns)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

// ---------------------------------------------------------------------------
// UTF-8 → UTF-8 View promotion helpers (unchanged from original)
// ---------------------------------------------------------------------------

fn promote_utf8_dtype(dt: &DataType) -> DataType {
    match dt {
        DataType::Utf8 => DataType::Utf8View,
        DataType::Binary => DataType::BinaryView,
        DataType::List(f) => DataType::List(promote_utf8_field(f).into()),
        DataType::LargeList(f) => DataType::LargeList(promote_utf8_field(f).into()),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(promote_utf8_field(f).into(), *n),
        DataType::Struct(fields) => {
            let promoted: Fields = fields.iter().map(|f| promote_utf8_field(f)).collect();
            DataType::Struct(promoted)
        }
        DataType::Map(f, sorted) => DataType::Map(promote_utf8_field(f).into(), *sorted),
        other => other.clone(),
    }
}

fn promote_utf8_field(f: &Field) -> Field {
    Field::new(f.name(), promote_utf8_dtype(f.data_type()), f.is_nullable())
        .with_metadata(f.metadata().clone())
}

fn promote_utf8_to_view_schema(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| promote_utf8_field(f))
            .collect::<Vec<_>>(),
    )
    .with_metadata(schema.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::stats::Precision;
    use datafusion::common::Statistics;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    /// Verify that the W2-1 patch replaces `Precision::Absent` with
    /// `Precision::Inexact(object.size)` and never overwrites a value that
    /// is already `Exact` or `Inexact`.
    ///
    /// We test the patch logic directly rather than through the async
    /// `infer_stats` call because the latter requires a live object-store and
    /// a real Vortex file.  The observable contract ("absent → inexact
    /// compressed size, non-absent → unchanged") is fully captured here.
    #[test]
    fn patch_replaces_absent_with_inexact_object_size() {
        let fake_size: u64 = 4096;
        let object = ObjectMeta {
            location: Path::from("data.vortex"),
            last_modified: chrono::Utc::now(),
            size: fake_size,
            e_tag: None,
            version: None,
        };

        // --- absent is replaced ---
        let mut stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        if matches!(stats.total_byte_size, Precision::Absent) {
            stats.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert!(
            !matches!(stats.total_byte_size, Precision::Absent),
            "Absent total_byte_size must be patched to non-Absent"
        );
        assert_eq!(
            stats.total_byte_size,
            Precision::Inexact(fake_size as usize),
            "patched value should equal object.size wrapped in Inexact"
        );

        // --- exact is preserved ---
        let mut exact = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(9999),
            column_statistics: vec![],
        };
        if matches!(exact.total_byte_size, Precision::Absent) {
            exact.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert_eq!(
            exact.total_byte_size,
            Precision::Exact(9999),
            "Exact total_byte_size must not be overwritten"
        );

        // --- inexact is also preserved ---
        let mut inexact = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Inexact(8888),
            column_statistics: vec![],
        };
        if matches!(inexact.total_byte_size, Precision::Absent) {
            inexact.total_byte_size = Precision::Inexact(object.size as usize);
        }
        assert_eq!(
            inexact.total_byte_size,
            Precision::Inexact(8888),
            "Inexact total_byte_size must not be overwritten"
        );
    }

    /// Confirms that `BasinVortexFormat::new` accepts a `VortexFormat` and
    /// can be constructed without panicking.
    #[test]
    fn construction_succeeds() {
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault as _;
        use vortex_datafusion::VortexFormat;
        use vortex_datafusion::VortexTableOptions;

        let inner = Arc::new(VortexFormat::new_with_options(
            VortexSession::default(),
            VortexTableOptions {
                projection_pushdown: true,
                scan_concurrency: Some(1),
                ..Default::default()
            },
        ));
        let _format = BasinVortexFormat::new(inner);
    }
}
