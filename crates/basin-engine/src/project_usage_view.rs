//! `basin_project_usage` SQL view — per-project live read of the
//! [`ProjectCounterRegistry`].
//!
//! # Design
//!
//! This view is the SQL surface for the metering counters maintained in
//! [`basin_common::ProjectCounterRegistry`]. Operators — and any cloud-side
//! quota cron — can `SELECT * FROM basin_project_usage` to read
//! the calling project's current counter values without any meter scrape.
//!
//! ## Scoping
//!
//! Per-project scoped. Each session sees exactly one row — its own project's
//! row. The provider captures the session's [`ProjectId`] at registration
//! time and looks up only that project's counters on every `scan()`, so a
//! session in project A NEVER sees project B's row. This mirrors the
//! `pg_stat_activity` / `pg_locks` per-project gate.
//!
//! ## Liveness
//!
//! Implemented as a [`TableProvider`], not a materialised snapshot. Every
//! `scan()` call reads the registry fresh, so a `SELECT` always reflects
//! the current counter values.
//!
//! ## v0.1 scope: observability only
//!
//! No engine-enforced quotas. Operators read this view from cron and
//! externally drop leases / throttle for over-budget projects. Hard
//! enforcement is deferred — keeping this slice purely observational keeps
//! it shippable without touching the storage / planning hot path.
//!
//! ## Unit conversion: cpu_micros → cpu_seconds
//!
//! The registry stores cumulative CPU time as `u64` microseconds
//! ([`basin_common::ProjectCounters::cpu_micros_total`]). The SQL surface
//! exposes `cpu_seconds_total` as a `DOUBLE` so dashboards can sum across
//! projects without integer overflow and read the value directly in
//! human-friendly units. The conversion (divide by 1e6) is done at this
//! view layer; the registry's unit choice is preserved.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use basin_common::{ProjectCounterRegistry, ProjectId};

const SNAPSHOT_TZ: &str = "+00:00";

// TODO: add to the column list when ProjectCounterRegistry gains additional
// counters (e.g. egress bytes, function-invocation counts). If
// `cpu_micros_total` is ever removed from the registry, drop the
// `cpu_seconds_total` column here.
fn basin_project_usage_schema() -> Schema {
    Schema::new(vec![
        Field::new("project_id", DataType::Utf8, false),
        Field::new("bytes_read_total", DataType::Int64, false),
        Field::new("bytes_written_total", DataType::Int64, false),
        Field::new("class_a_ops_total", DataType::Int64, false),
        Field::new("class_b_ops_total", DataType::Int64, false),
        Field::new("cpu_seconds_total", DataType::Float64, false),
        Field::new(
            "snapshot_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(SNAPSHOT_TZ.into())),
            false,
        ),
    ])
}

/// `TableProvider` for `basin_project_usage` backed by the process-wide
/// [`ProjectCounterRegistry`].
pub struct BasinProjectUsageProvider {
    registry: Arc<ProjectCounterRegistry>,
    project: ProjectId,
    schema: SchemaRef,
}

impl std::fmt::Debug for BasinProjectUsageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasinProjectUsageProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl BasinProjectUsageProvider {
    pub fn new(registry: Arc<ProjectCounterRegistry>, project: ProjectId) -> Self {
        Self {
            registry,
            project,
            schema: Arc::new(basin_project_usage_schema()),
        }
    }
}

fn u64_to_i64_sat(n: u64) -> i64 {
    if n > i64::MAX as u64 {
        i64::MAX
    } else {
        n as i64
    }
}

#[async_trait]
impl TableProvider for BasinProjectUsageProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
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
        let snap = self.registry.snapshot(&self.project).unwrap_or_default();
        let project_id_str = self.project.to_string();

        let project_id_arr: ArrayRef =
            Arc::new(StringArray::from(vec![Some(project_id_str.as_str())]));
        let bytes_read_arr: ArrayRef =
            Arc::new(Int64Array::from(vec![u64_to_i64_sat(snap.bytes_read_total)]));
        let bytes_written_arr: ArrayRef = Arc::new(Int64Array::from(vec![u64_to_i64_sat(
            snap.bytes_written_total,
        )]));
        let class_a_ops_arr: ArrayRef = Arc::new(Int64Array::from(vec![u64_to_i64_sat(
            snap.class_a_ops_total,
        )]));
        let class_b_ops_arr: ArrayRef = Arc::new(Int64Array::from(vec![u64_to_i64_sat(
            snap.class_b_ops_total,
        )]));
        let cpu_seconds_arr: ArrayRef = Arc::new(Float64Array::from(vec![
            snap.cpu_micros_total as f64 / 1_000_000.0,
        ]));

        let now_micros: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
            .unwrap_or_default();
        let snapshot_at_arr: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![now_micros]).with_timezone(SNAPSHOT_TZ),
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                project_id_arr,
                bytes_read_arr,
                bytes_written_arr,
                class_a_ops_arr,
                class_b_ops_arr,
                cpu_seconds_arr,
                snapshot_at_arr,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let partitions = vec![vec![batch]];
        let exec = MemorySourceConfig::try_new_exec(
            &partitions,
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

/// Register `basin_project_usage` under the `public` schema of `ctx`.
pub(crate) fn register_basin_project_usage(
    ctx: &datafusion::prelude::SessionContext,
    registry: Arc<ProjectCounterRegistry>,
    project: ProjectId,
) -> DfResult<()> {
    let provider = Arc::new(BasinProjectUsageProvider::new(registry, project));
    ctx.register_table("basin_project_usage", provider)
        .map_err(|e| {
            DataFusionError::Internal(format!("register basin_project_usage: {e}"))
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectCounterRegistry;

    #[tokio::test]
    async fn scan_returns_single_row_with_calling_project_id() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let project = ProjectId::new();
        let counters = registry.for_project(&project);
        counters.record_bytes_written(4096);
        counters.record_class_a_op();
        counters.record_cpu_micros(2_500_000);

        let provider = BasinProjectUsageProvider::new(registry, project);
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("basin_project_usage", Arc::new(provider)).unwrap();
        let df = ctx
            .sql(
                "SELECT project_id, bytes_written_total, class_a_ops_total, cpu_seconds_total \
                 FROM basin_project_usage",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        let b = &batches[0];
        let pid = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(pid.value(0), project.to_string());
        let bw = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(bw.value(0), 4096);
        let ca = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ca.value(0), 1);
        let cpu = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((cpu.value(0) - 2.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn cross_project_isolation_a_cannot_see_b() {
        let registry = Arc::new(ProjectCounterRegistry::new());
        let a = ProjectId::new();
        let b = ProjectId::new();
        registry.for_project(&b).record_bytes_written(1_000_000);

        let provider_a = BasinProjectUsageProvider::new(registry.clone(), a);
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("basin_project_usage", Arc::new(provider_a))
            .unwrap();
        let batches = ctx
            .sql("SELECT project_id, bytes_written_total FROM basin_project_usage")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let row = &batches[0];
        let pid = row.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let bw = row.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(pid.value(0), a.to_string());
        assert_eq!(bw.value(0), 0, "project A must NOT see project B's bytes");
    }
}
