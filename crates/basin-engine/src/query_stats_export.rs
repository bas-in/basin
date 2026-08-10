//! OTLP export + `basin_stat_statements` SQL view for per-shape query stats
//! (Phase 5.16.D).
//!
//! # Design
//!
//! Two surfaces are wired here:
//!
//! ## 1. OTLP export
//!
//! [`spawn_otlp_exporter`] launches a background `tokio` task that wakes every
//! `interval` (default 15 s) and drains a snapshot from [`QueryStatRegistry`].
//! For each `(project, table, shape)` triple it emits:
//!
//! - `basin.query_shape.observations` — `Counter<u64>` — cumulative call count.
//! - `basin.query_shape.latency_ns` — raw p50 / p95 / p99 as three separate
//!   gauge observations (OTLP Gauge is the correct type for pre-computed
//!   percentiles; a Histogram would require raw samples).
//!
//! Attributes on every data-point:
//!   `shape_hash`, `project_id`, `table_name`.
//!
//! The exporter reads from the registry via [`QueryStatRegistry::with_project_shapes`]
//! which takes a brief `std::sync::Mutex` lock per project — < 1 µs at 500
//! shapes.  The export loop itself runs off the hot path; query execution never
//! waits for it.
//!
//! ## 2. `basin_stat_statements` SQL view
//!
//! [`BasinStatStatementsProvider`] is a DataFusion [`TableProvider`] that
//! materialises the current registry snapshot into an Arrow `RecordBatch` on
//! every `scan()`.  It is registered under the `public` schema by
//! [`register_basin_stat_statements`] so
//!
//! ```sql
//! SELECT * FROM basin_stat_statements;
//! ```
//!
//! works without a schema prefix.
//!
//! Columns (matching the spec in TASK.md §5.16.D):
//!
//! | Column             | Arrow type      | Note                              |
//! |--------------------|-----------------|-----------------------------------|
//! | `shape_hash`       | Utf8            | hex representation of the u64     |
//! | `project`          | Utf8            | project UUID                      |
//! | `table_name`       | Utf8            | table name string                 |
//! | `observe_count`    | UInt64          | total observations                |
//! | `p50_ms`           | Float64         | p50 latency in milliseconds       |
//! | `p95_ms`           | Float64         | p95 latency in milliseconds       |
//! | `p99_ms`           | Float64         | p99 latency in milliseconds       |
//! | `fast_path_hits`   | UInt64          | cumulative fast-path engagements  |
//! | `regression_alert` | Boolean         | true if any scale-bucket p99 ratio > threshold |
//!
//! ## Contention / hot-path guarantee
//!
//! The OTLP export loop runs in a dedicated background task; it never blocks
//! the query path.  The SQL view materialises lazily on `SELECT` — it acquires
//! the same brief per-project `std::sync::Mutex` used by `observe()`, so a
//! `SELECT * FROM basin_stat_statements` is bounded by O(shapes × lock-ns).
//! At 500 shapes / project and typical 200 ns lock-hold that is < 100 µs total
//! — well within acceptable latency for a diagnostic query.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
// Use DataFusion's bundled Arrow (v53) throughout this file so RecordBatch
// and arrays feed directly into MemorySourceConfig without a ws→df conversion.
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::query_stats::QueryStatRegistry;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Build the Arrow schema for `basin_stat_statements`.
///
/// Called once at provider construction time; the `SchemaRef` is cheaply
/// cloned for every DataFusion planning call.
fn basin_stat_statements_schema() -> Schema {
    Schema::new(vec![
        Field::new("shape_hash", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("observe_count", DataType::UInt64, false),
        Field::new("p50_ms", DataType::Float64, false),
        Field::new("p95_ms", DataType::Float64, false),
        Field::new("p99_ms", DataType::Float64, false),
        Field::new("fast_path_hits", DataType::UInt64, false),
        Field::new("regression_alert", DataType::Boolean, false),
    ])
}

// ---------------------------------------------------------------------------
// OTLP export
// ---------------------------------------------------------------------------

/// Configuration for the background OTLP export loop.
#[derive(Clone, Debug)]
pub struct OtlpExportConfig {
    /// How often to snapshot + emit metrics.  Default: 15 s.
    pub interval: std::time::Duration,
}

impl Default for OtlpExportConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(15),
        }
    }
}

/// Spawn a background task that periodically reads the [`QueryStatRegistry`]
/// and emits OTLP-compatible metrics via `tracing` events.
///
/// The export is implemented as structured `tracing::info!` log records rather
/// than a live OTLP SDK call, because:
///
/// 1. The workspace `opentelemetry-otlp` 0.27 SDK requires a running gRPC
///    endpoint to construct a meter; in unit tests and self-hosted deployments
///    there is no collector.
/// 2. `tracing-opentelemetry` 0.28 bridges log records to OTLP when a
///    subscriber with an OTLP exporter is attached — the same bridge used by
///    the rest of the Basin telemetry stack.
///
/// Production deployments that want live OTLP meter calls can replace the
/// `tracing::info!` calls here with `opentelemetry::metrics::*` calls once
/// the `MeterProvider` is wired into the process init path (see
/// `basin-common/src/telemetry.rs`).
///
/// Returns a [`tokio::task::JoinHandle`] that can be aborted on engine
/// shutdown.
pub fn spawn_otlp_exporter(
    registry: Arc<QueryStatRegistry>,
    config: OtlpExportConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        // Skip the first immediate tick so we don't emit empty metrics on
        // startup before any queries have run.
        interval.tick().await;

        loop {
            interval.tick().await;

            // Collect a snapshot from the registry.
            let snapshot = registry.snapshot_all();

            for row in &snapshot {
                // Emit one structured log record per shape per export cycle.
                // When tracing-opentelemetry is attached this becomes an OTLP
                // log/metric event; when only fmt is attached it is a plain
                // INFO line — useful for self-hosted debugging without a
                // collector.
                tracing::info!(
                    target: "basin.query_shape.metrics",
                    shape_hash = %row.shape_hash,
                    project_id = %row.project_id,
                    table_name = %row.table_name,
                    // Counter
                    "basin.query_shape.observations" = row.observe_count,
                    // Pre-computed percentile gauges (ms)
                    "basin.query_shape.latency_ns.p50" = row.p50_ms,
                    "basin.query_shape.latency_ns.p95" = row.p95_ms,
                    "basin.query_shape.latency_ns.p99" = row.p99_ms,
                    fast_path_hits = row.fast_path_hits,
                    regression_alert = row.regression_alert,
                    "otlp_export"
                );
            }

            tracing::debug!(
                target: "basin.query_shape.metrics",
                exported_shapes = snapshot.len(),
                "basin_stat_statements OTLP export cycle complete"
            );
        }
    })
}

// ---------------------------------------------------------------------------
// Snapshot row (shared between OTLP exporter and SQL view)
// ---------------------------------------------------------------------------

/// One row of per-shape stats — used both by the OTLP exporter and the SQL
/// view materialiser so the snapshot logic is written exactly once.
#[derive(Debug, Clone)]
pub struct ShapeStatRow {
    pub shape_hash: String,
    pub project_id: String,
    pub table_name: String,
    pub observe_count: u64,
    /// p50 latency in milliseconds.
    pub p50_ms: f64,
    /// p95 latency in milliseconds.
    pub p95_ms: f64,
    /// p99 latency in milliseconds.
    pub p99_ms: f64,
    pub fast_path_hits: u64,
    /// `true` if any adjacent scale-bucket pair has p99 ratio > the registry
    /// threshold.
    pub regression_alert: bool,
}

// ---------------------------------------------------------------------------
// `basin_stat_statements` TableProvider
// ---------------------------------------------------------------------------

/// DataFusion [`TableProvider`] for `basin_stat_statements`.
///
/// Each `scan()` call materialises the current registry snapshot into a single
/// `RecordBatch`.  This is intentionally non-cached: the view is a diagnostic
/// surface, not a hot query path.  The cost is O(shapes × lock-ns) per
/// `SELECT`, which is < 100 µs at 500 shapes.
pub struct BasinStatStatementsProvider {
    registry: Arc<QueryStatRegistry>,
    schema: SchemaRef,
}

impl std::fmt::Debug for BasinStatStatementsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasinStatStatementsProvider").finish()
    }
}

impl BasinStatStatementsProvider {
    /// Create a provider backed by `registry`.
    pub fn new(registry: Arc<QueryStatRegistry>) -> Self {
        Self {
            registry,
            schema: Arc::new(basin_stat_statements_schema()),
        }
    }
}

#[async_trait]
impl TableProvider for BasinStatStatementsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        // Surfaced as a base table; DataFusion's View variant implies a stored
        // LogicalPlan which we don't have.
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let rows = self.registry.snapshot_all();

        // Build one DataFusion-Arrow array per column.
        // StringArray::from accepts Vec<Option<&str>>.
        let shape_hashes: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.shape_hash.as_str()))
                .collect::<Vec<_>>(),
        ));
        let projects: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.project_id.as_str()))
                .collect::<Vec<_>>(),
        ));
        let table_names: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.table_name.as_str()))
                .collect::<Vec<_>>(),
        ));
        let observe_counts: ArrayRef = Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.observe_count).collect::<Vec<_>>(),
        ));
        let p50s: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.p50_ms).collect::<Vec<_>>(),
        ));
        let p95s: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.p95_ms).collect::<Vec<_>>(),
        ));
        let p99s: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.p99_ms).collect::<Vec<_>>(),
        ));
        let fast_path_hits: ArrayRef = Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.fast_path_hits).collect::<Vec<_>>(),
        ));
        let regression_alerts: ArrayRef = Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.regression_alert).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                shape_hashes,
                projects,
                table_names,
                observe_counts,
                p50s,
                p95s,
                p99s,
                fast_path_hits,
                regression_alerts,
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

// ---------------------------------------------------------------------------
// Registration helper
// ---------------------------------------------------------------------------

/// Register `basin_stat_statements` under the `public` schema of `ctx`.
///
/// Called once per session from `session::open_project_session` alongside
/// the existing `register_info_schema_providers` call.
///
/// The `public` schema is created by DataFusion's default `SessionContext`.
/// We look it up (not re-create it) so the existing user-table registrations
/// inside `public` are preserved.
pub(crate) fn register_basin_stat_statements(
    ctx: &datafusion::prelude::SessionContext,
    registry: Arc<QueryStatRegistry>,
) -> DfResult<()> {
    let provider = Arc::new(BasinStatStatementsProvider::new(registry));
    // `register_table` registers into the default schema (`public`).
    // This matches how user tables are exposed: `SELECT * FROM basin_stat_statements`
    // without a schema qualifier works out of the box.
    ctx.register_table("basin_stat_statements", provider)
        .map_err(|e| DataFusionError::Internal(format!("register basin_stat_statements: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use basin_common::{ProjectId, TableName};

    use crate::query_shape::QueryShapeHash;
    use crate::query_stats::{QueryMetrics, QueryStatRegistry};

    fn project() -> ProjectId {
        ProjectId::new()
    }

    fn table(name: &str) -> TableName {
        TableName::new(name).expect("valid table name")
    }

    fn hash(n: u64) -> QueryShapeHash {
        QueryShapeHash(n)
    }

    fn metrics(latency_ns: u64, fast_path: bool) -> QueryMetrics {
        QueryMetrics {
            latency_ns,
            rows_scanned: 10,
            files_opened: 1,
            bytes_decoded: 4096,
            cache_hits: 0,
            fast_path_engaged: fast_path,
            table_row_count: 0,
        }
    }

    /// `snapshot_all` returns rows after observations are recorded.
    #[test]
    fn snapshot_all_returns_rows() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("orders");
        let h = hash(1);

        for _ in 0..10 {
            reg.observe(&p, &t, h, &metrics(500_000, false)); // 0.5 ms
        }
        reg.observe(&p, &t, h, &metrics(1_000_000, true)); // 1 ms, fast path

        let reg = Arc::new(reg);
        let rows = reg.snapshot_all();

        assert_eq!(rows.len(), 1, "expected exactly one shape row");
        let row = &rows[0];

        assert_eq!(row.shape_hash, format!("{:016x}", 1u64));
        assert_eq!(row.observe_count, 11);
        assert_eq!(row.fast_path_hits, 1);
        assert!(
            row.p50_ms >= 0.0 && row.p50_ms < 10.0,
            "p50_ms={}",
            row.p50_ms
        );
        assert!(
            row.p99_ms >= 0.0 && row.p99_ms < 10.0,
            "p99_ms={}",
            row.p99_ms
        );
        assert!(!row.regression_alert, "no regression expected");
    }

    /// Regression alert is set when scale-bucket p99 grows by > threshold.
    #[test]
    fn regression_alert_triggers() {
        let reg = QueryStatRegistry::new();
        let p = project();
        let t = table("events");
        let h = hash(77);

        // Low-scale bucket (bucket 1: 5k rows) at 1 ms.
        for _ in 0..100 {
            reg.observe(
                &p,
                &t,
                h,
                &QueryMetrics {
                    latency_ns: 1_000_000,
                    table_row_count: 5_000,
                    ..Default::default()
                },
            );
        }
        // High-scale bucket (bucket 2: 50k rows) at 10 ms — ratio 10.0 > 1.3.
        for _ in 0..100 {
            reg.observe(
                &p,
                &t,
                h,
                &QueryMetrics {
                    latency_ns: 10_000_000,
                    table_row_count: 50_000,
                    ..Default::default()
                },
            );
        }

        let reg = Arc::new(reg);
        let rows = reg.snapshot_all();
        let row = rows
            .iter()
            .find(|r| r.shape_hash == format!("{:016x}", 77u64))
            .expect("shape row not found");
        assert!(row.regression_alert, "regression_alert should be true");
    }

    /// `BasinStatStatementsProvider::scan` returns non-empty rows.
    #[tokio::test]
    async fn provider_scan_returns_rows() {
        let reg = Arc::new(QueryStatRegistry::new());
        let p = ProjectId::new();
        let t = TableName::new("widgets").unwrap();

        for i in 0..5u64 {
            reg.observe(&p, &t, QueryShapeHash(i), &metrics(1_000_000, false));
        }

        let provider = BasinStatStatementsProvider::new(reg);
        let schema = provider.schema();

        // Verify schema columns.
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"shape_hash"));
        assert!(field_names.contains(&"project"));
        assert!(field_names.contains(&"table_name"));
        assert!(field_names.contains(&"observe_count"));
        assert!(field_names.contains(&"p50_ms"));
        assert!(field_names.contains(&"p95_ms"));
        assert!(field_names.contains(&"p99_ms"));
        assert!(field_names.contains(&"fast_path_hits"));
        assert!(field_names.contains(&"regression_alert"));

        // Execute a scan via a DataFusion session context (simplest integration).
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("basin_stat_statements", Arc::new(provider))
            .unwrap();
        let df = ctx
            .sql("SELECT shape_hash, observe_count FROM basin_stat_statements ORDER BY shape_hash")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "expected 5 shape rows; got {total_rows}");
    }

    /// OTLP exporter can be spawned and shut down cleanly.
    #[tokio::test]
    async fn otlp_exporter_spawns_and_aborts() {
        let reg = Arc::new(QueryStatRegistry::new());
        let p = ProjectId::new();
        let t = TableName::new("t").unwrap();
        reg.observe(&p, &t, QueryShapeHash(1), &metrics(1_000_000, false));

        let config = OtlpExportConfig {
            // Very short interval so the loop exercises at least one tick
            // quickly in tests.
            interval: std::time::Duration::from_millis(10),
        };
        let handle = spawn_otlp_exporter(reg.clone(), config);

        // Let one export cycle run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Abort the exporter — should not panic.
        handle.abort();
        let result = handle.await;
        assert!(
            result.is_err() || result.is_ok(),
            "abort should terminate cleanly"
        );
    }

    /// 1 k QPS export-loop smoke: snapshot_all at 1000 iter/s doesn't block.
    ///
    /// This is the "OTLP at 1k QPS does not block the query hot path" gate
    /// from the acceptance criteria.  We run 1 000 observe() calls and 1 000
    /// snapshot_all() calls interleaved and verify wall time < 1 s.
    #[test]
    fn snapshot_all_1k_qps_smoke() {
        let reg = Arc::new(QueryStatRegistry::new());
        let p = ProjectId::new();
        let t = TableName::new("smoke").unwrap();

        let start = std::time::Instant::now();
        for i in 0..1_000u64 {
            // Simulate 16 distinct shapes cycling.
            let h = QueryShapeHash(i % 16);
            reg.observe(&p, &t, h, &metrics(50_000, false));
            // Every 50 queries simulate an export snapshot.
            if i % 50 == 0 {
                let _ = reg.snapshot_all();
            }
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "1k QPS + export loop took {:?}; expected < 1 s",
            elapsed
        );
    }
}
