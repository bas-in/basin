//! INTERVAL-as-bytes storage disguise: the DataFusion-side half.
//!
//! basin-storage cannot hand either codec an Arrow `Interval(MonthDayNano)`
//! column:
//!
//!   vortex 0.71:    "Array encoding not implemented for Arrow data type
//!                    Interval(MonthDayNano)"
//!   parquet 58.4.0: "Attempting to write an Arrow interval type MonthDayNano
//!                    to parquet" (and NYI on read)
//!
//! So its writer stores interval columns as `LargeBinary`, 16 bytes per row —
//! `months(i32) | days(i32) | nanos(i64)`, little-endian — marked
//! `BASIN_TYPE=INTERVAL`. That is a storage-layer disguise, but the DataFusion
//! fallback path reads those files through `ListingTable`, NOT through
//! basin-storage's reader, so it needs its own half of the inverse:
//!
//! 1. the file schema handed to the scan must claim the PHYSICAL type
//!    (otherwise DataFusion's schema adapter attempts an unsupported
//!    `LargeBinary` → `Interval(MonthDayNano)` cast and the query dies), and
//! 2. an [`IntervalRestoreExec`] on top of the scan decodes the blobs back
//!    into `Interval(MonthDayNano)` so everything above the scan sees the
//!    logical type the catalog declares.
//!
//! Both format wrappers use this module: `vortex_listing_format`
//! (`BinaryView` — what Vortex's scan layer emits) and
//! `parquet_listing_format` (`LargeBinary` — what the Parquet reader emits).
//!
//! Unlike the ADR-0024 UUID disguise and the POINT disguise, this one is NOT
//! Vortex-specific — hence a shared module rather than a copy in each format
//! wrapper.
//!
//! months, days and nanos are kept in their OWN slots throughout. PG's
//! interval is not normalisable: `INTERVAL '1 mon'` and `INTERVAL '30 days'`
//! are distinct values (verified live on PG 18.2: `SELECT INTERVAL '1 mon' -
//! INTERVAL '30 days'` prints `1 mon -30 days`, not `00:00:00`), so any
//! encoding that folded them together would be silent corruption.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// Field-metadata key. Mirrors `crate::types::BASIN_TYPE_KEY`.
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";

/// `BASIN_TYPE` marker for INTERVAL columns. Mirrors
/// `crate::types::BASIN_TYPE_INTERVAL` and basin-storage's private copy.
const BASIN_TYPE_INTERVAL: &str = "INTERVAL";

/// Stored width of one interval value: months(i32) + days(i32) + nanos(i64).
const INTERVAL_STORAGE_LEN: usize = 16;


/// `true` if `field` is the catalog shape for an interval column:
/// `Interval(MonthDayNano)`. The marker is not required here — the catalog
/// type alone is unambiguous, and an interval column that somehow reached
/// the catalog without a marker still must not be handed to a codec as an
/// interval (neither can decode one).
pub(crate) fn field_is_interval_native(f: &Field) -> bool {
    matches!(f.data_type(), DataType::Interval(IntervalUnit::MonthDayNano))
}

/// `true` if `field` is an interval column coming back from a codec as a
/// binary-family physical type (`BinaryView` from Vortex, `LargeBinary` from
/// Parquet), tagged `BASIN_TYPE=INTERVAL`. The marker IS required in this
/// direction so a genuine BYTEA column is never decoded as an interval.
pub(crate) fn field_is_interval_binary(f: &Field) -> bool {
    matches!(
        f.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) && f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_INTERVAL)
}

pub(crate) fn schema_has_interval_native(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_is_interval_native(f))
}

pub(crate) fn schema_has_interval_binary(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| field_is_interval_binary(f))
}

/// Swap `Interval(MonthDayNano)` fields to `physical` — the binary-family
/// type the codec's scan layer actually emits (`BinaryView` for Vortex,
/// `LargeBinary` for Parquet) — so the scan never attempts the unsupported
/// binary → interval cast. The `BASIN_TYPE=INTERVAL` marker is added when
/// absent so the restore below can identify the column.
pub(crate) fn swap_interval_to_physical(schema: &Schema, physical: DataType) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if field_is_interval_native(f) {
                let mut md = f.metadata().clone();
                md.entry(BASIN_TYPE_KEY.to_string())
                    .or_insert_with(|| BASIN_TYPE_INTERVAL.to_string());
                Field::new(f.name(), physical.clone(), f.is_nullable()).with_metadata(md)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

/// Swap binary-family `+BASIN_TYPE=INTERVAL` fields back to
/// `Interval(MonthDayNano)`. The logical output schema of the interval
/// restore.
pub(crate) fn swap_interval_binary_to_native(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if field_is_interval_binary(f) {
                Field::new(
                    f.name(),
                    DataType::Interval(IntervalUnit::MonthDayNano),
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

/// Convert one `RecordBatch`'s binary-family INTERVAL columns back to
/// `Interval(MonthDayNano)`, decoding the 16-byte little-endian
/// `months|days|nanos` payload basin-storage's writer produced.
///
/// months, days and nanos are decoded into their OWN slots — PG's interval
/// is not normalisable, so `INTERVAL '1 mon'` (1, 0, 0) must stay distinct
/// from `INTERVAL '30 days'` (0, 30, 0).
pub(crate) fn restore_interval_columns(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow_array::{BinaryViewArray, IntervalMonthDayNanoArray, LargeBinaryArray};

    let schema = batch.schema();
    if !schema_has_interval_binary(schema.as_ref()) {
        return Ok(batch);
    }
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (i, f) in schema.fields().iter().enumerate() {
        if field_is_interval_binary(f) {
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
            let mut values: Vec<Option<arrow_array::types::IntervalMonthDayNano>> =
                Vec::with_capacity(len);
            for r in 0..len {
                match value_at(r) {
                    None => values.push(None),
                    Some(b) => {
                        if b.len() != INTERVAL_STORAGE_LEN {
                            return Err(datafusion::common::DataFusionError::Internal(format!(
                                "interval restore: column '{}' row {r} has {} bytes, expected {}",
                                f.name(),
                                b.len(),
                                INTERVAL_STORAGE_LEN
                            )));
                        }
                        let months = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                        let days = i32::from_le_bytes([b[4], b[5], b[6], b[7]]);
                        let mut n = [0u8; 8];
                        n.copy_from_slice(&b[8..16]);
                        values.push(Some(arrow_array::types::IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds: i64::from_le_bytes(n),
                        }));
                    }
                }
            }
            let arr: IntervalMonthDayNanoArray = values.into_iter().collect();
            let new_field = Field::new(
                f.name(),
                DataType::Interval(IntervalUnit::MonthDayNano),
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

// ---------------------------------------------------------------------------
// IntervalRestoreExec
// ---------------------------------------------------------------------------

/// A thin `ExecutionPlan` wrapper that translates binary-family INTERVAL
/// columns (`Binary`/`LargeBinary`/`BinaryView` carrying
/// `BASIN_TYPE=INTERVAL`) back to `Interval(MonthDayNano)` on every output
/// batch, so DataFusion's interval arithmetic and the engine's encoders see
/// the logical type the catalog declares.
///
/// Structurally identical to `PointFsbRestoreExec`; only the per-batch
/// translation differs. Unlike the UUID and POINT disguises this one is not
/// a Vortex workaround alone — Parquet cannot store the Arrow interval type
/// either — but only the Vortex scan path routes through this file.
#[derive(Debug)]
pub(crate) struct IntervalRestoreExec {
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl IntervalRestoreExec {
    pub(crate) fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let output_schema = Arc::new(swap_interval_binary_to_native(inner.schema().as_ref()));
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

impl DisplayAs for IntervalRestoreExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalRestoreExec")
    }
}

impl ExecutionPlan for IntervalRestoreExec {
    fn name(&self) -> &str {
        "IntervalRestoreExec"
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
        Ok(Arc::new(IntervalRestoreExec::new(children.swap_remove(0))))
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
            batch_res.and_then(restore_interval_columns)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

