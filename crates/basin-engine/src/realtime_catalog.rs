//! `basin_realtime.channels` and `basin_realtime.stats` virtual tables.
//!
//! # Design
//!
//! Two DataFusion [`TableProvider`]s are registered under the `basin_realtime`
//! schema on every per-project session so that
//!
//! ```sql
//! SELECT * FROM basin_realtime.channels;
//! SELECT * FROM basin_realtime.stats;
//! ```
//!
//! resolve to live runtime state without any disk I/O.
//!
//! ## Data sources
//!
//! | Table | Source | Access |
//! |-------|--------|--------|
//! | `channels` | [`NotifyRegistry`] (LISTEN/NOTIFY) + optional [`RealtimeChannelSource`] (broadcast) | Direct (engine holds NotifyRegistry) + trait object |
//! | `stats` | [`RealtimeChannelSource`] (budget) | Optional trait object |
//!
//! ## `RealtimeChannelSource` trait
//!
//! `basin-engine` cannot depend on `basin-realtime` (that would create a
//! dependency cycle through `basin-webhooks`). The trait is the seam: callers
//! that wire up the realtime sink (e.g. `basin-rest`, `basin-server`) also call
//! [`Engine::attach_realtime_source`] with an implementation that wraps
//! `ChannelRegistry` + `BudgetTracker`. When not attached both providers still
//! return valid (empty) rows — `channels` shows only LISTEN/NOTIFY channels and
//! `stats` shows zeroes.
//!
//! ## Columns
//!
//! ### `basin_realtime.channels`
//!
//! | Column | Type | Nullable | Notes |
//! |--------|------|----------|-------|
//! | `channel_name` | TEXT | no | Table name (broadcast) or SQL channel name (LISTEN/NOTIFY) |
//! | `channel_type` | TEXT | no | `'postgres_changes'` or `'postgres_notify'` |
//! | `subscriber_count` | BIGINT | no | Live receiver count |
//! | `member_count` | BIGINT | yes | NULL (presence not tracked) |
//! | `created_at` | TIMESTAMPTZ | yes | NULL (creation time not tracked) |
//!
//! ### `basin_realtime.stats`
//!
//! | Column | Type | Nullable | Notes |
//! |--------|------|----------|-------|
//! | `events_per_sec` | DOUBLE | no | **Flagged missing** — returns 0.0; no live counter |
//! | `errors_per_sec` | DOUBLE | no | **Flagged missing** — returns 0.0; no live counter |
//! | `bytes_per_sec` | DOUBLE | no | **Flagged missing** — returns 0.0; no live counter |
//! | `memory_budget_bytes` | BIGINT | no | Real: from `BudgetTracker::hard_cap()` |
//! | `memory_used_bytes` | BIGINT | no | Real: from `BudgetTracker::bytes_in_flight(project)` |
//! | `captured_at` | TIMESTAMPTZ | no | Current wall-clock timestamp at scan time |
//!
//! **Rate columns are NOT live counters.** `events_per_sec`, `errors_per_sec`,
//! and `bytes_per_sec` are always 0.0. Adding lightweight atomic counters to
//! `RealtimeSink::publish` is tracked as a follow-up (basin Phase 5.11.R7).
//! The cloud handler falls back gracefully when these are zero, so 0.0 is
//! correct-but-conservative rather than fabricated.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, Session};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use basin_common::ProjectId;

// ---------------------------------------------------------------------------
// Seam trait (avoids basin-realtime dependency from basin-engine)
// ---------------------------------------------------------------------------

/// Row emitted by [`RealtimeChannelSource::broadcast_channels_for_project`].
#[derive(Debug, Clone)]
pub struct BroadcastChannelRow {
    /// Table name used as the broadcast channel key.
    pub channel_name: String,
    /// Number of live broadcast receivers.
    pub subscriber_count: i64,
}

/// Budget snapshot emitted by [`RealtimeChannelSource::budget_for_project`].
#[derive(Debug, Clone)]
pub struct BudgetSnapshot {
    /// Per-project in-flight byte hard cap (from `BudgetTracker::hard_cap()`).
    pub memory_budget_bytes: i64,
    /// Current in-flight bytes for the project (from `BudgetTracker::bytes_in_flight(project)`).
    pub memory_used_bytes: i64,
}

/// Thin trait allowing `basin-realtime`'s `ChannelRegistry` + `BudgetTracker`
/// to feed live data into the engine's `basin_realtime.*` virtual tables
/// without creating a crate dependency cycle.
///
/// Implementors:
/// - `basin-realtime` crate: wraps `ChannelRegistry` + `BudgetTracker`.
/// - Tests in this crate: trivial mock.
///
/// When not attached (no implementor registered), the virtual tables return
/// empty/zero rows rather than failing.
pub trait RealtimeChannelSource: Send + Sync {
    /// Live broadcast (table-change) channels for `project` with at least one
    /// active receiver. Called on every `SELECT` from `basin_realtime.channels`.
    fn broadcast_channels_for_project(&self, project: ProjectId) -> Vec<BroadcastChannelRow>;

    /// Memory budget snapshot for `project`. Called on every `SELECT` from
    /// `basin_realtime.stats`.
    fn budget_for_project(&self, project: ProjectId) -> BudgetSnapshot;
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const SNAP_TZ: &str = "+00:00";

fn channels_schema() -> Schema {
    Schema::new(vec![
        Field::new("channel_name", DataType::Utf8, false),
        Field::new("channel_type", DataType::Utf8, false),
        Field::new("subscriber_count", DataType::Int64, false),
        // member_count: NULL for all rows (presence not tracked)
        Field::new("member_count", DataType::Int64, true),
        // created_at: NULL for all rows (creation time not tracked)
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(SNAP_TZ.into())),
            true,
        ),
    ])
}

fn stats_schema() -> Schema {
    Schema::new(vec![
        Field::new("events_per_sec", DataType::Float64, false),
        Field::new("errors_per_sec", DataType::Float64, false),
        Field::new("bytes_per_sec", DataType::Float64, false),
        Field::new("memory_budget_bytes", DataType::Int64, false),
        Field::new("memory_used_bytes", DataType::Int64, false),
        Field::new(
            "captured_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(SNAP_TZ.into())),
            false,
        ),
    ])
}

// ---------------------------------------------------------------------------
// basin_realtime.channels TableProvider
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` for `basin_realtime.channels`.
///
/// On every `scan()` it unions:
/// 1. Broadcast (table-change) channels from the optional [`RealtimeChannelSource`].
/// 2. SQL LISTEN/NOTIFY channels from the engine's [`NotifyRegistry`].
///
/// Both sets are scoped to the session's `project`.
pub struct RealtimeChannelsProvider {
    project: ProjectId,
    /// Optional broadcast channel source (from basin-realtime). When `None`
    /// only LISTEN/NOTIFY channels are visible.
    source: Option<Arc<dyn RealtimeChannelSource>>,
    /// Engine-side LISTEN/NOTIFY registry. Always present.
    notify: Arc<crate::notify_registry::NotifyRegistry>,
    schema: SchemaRef,
}

impl std::fmt::Debug for RealtimeChannelsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealtimeChannelsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl RealtimeChannelsProvider {
    pub fn new(
        project: ProjectId,
        source: Option<Arc<dyn RealtimeChannelSource>>,
        notify: Arc<crate::notify_registry::NotifyRegistry>,
    ) -> Self {
        Self {
            project,
            source,
            notify,
            schema: Arc::new(channels_schema()),
        }
    }
}

#[async_trait]
impl TableProvider for RealtimeChannelsProvider {
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
        let mut channel_names: Vec<String> = Vec::new();
        let mut channel_types: Vec<String> = Vec::new();
        let mut subscriber_counts: Vec<i64> = Vec::new();

        // Broadcast (postgres_changes) channels from the realtime source.
        if let Some(src) = &self.source {
            for row in src.broadcast_channels_for_project(self.project) {
                channel_names.push(row.channel_name);
                channel_types.push("postgres_changes".to_string());
                subscriber_counts.push(row.subscriber_count);
            }
        }

        // LISTEN/NOTIFY (postgres_notify) channels from the notify registry.
        for row in self.notify.channels_for_project(self.project) {
            channel_names.push(row.channel_name);
            channel_types.push("postgres_notify".to_string());
            subscriber_counts.push(row.subscriber_count);
        }

        let n = channel_names.len();

        let channel_name_arr: ArrayRef =
            Arc::new(StringArray::from(channel_names));
        let channel_type_arr: ArrayRef =
            Arc::new(StringArray::from(channel_types));
        let subscriber_count_arr: ArrayRef =
            Arc::new(Int64Array::from(subscriber_counts));
        // member_count: all NULL (presence not tracked yet)
        let member_count_arr: ArrayRef =
            Arc::new(Int64Array::from(vec![None::<i64>; n]));
        // created_at: all NULL (creation timestamp not tracked)
        let created_at_arr: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![None::<i64>; n])
                .with_timezone(SNAP_TZ),
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                channel_name_arr,
                channel_type_arr,
                subscriber_count_arr,
                member_count_arr,
                created_at_arr,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let exec = MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

// ---------------------------------------------------------------------------
// basin_realtime.stats TableProvider
// ---------------------------------------------------------------------------

/// DataFusion `TableProvider` for `basin_realtime.stats`.
///
/// Returns a single row per `SELECT` reflecting the current moment's budget
/// snapshot. Rate columns (`events_per_sec`, `errors_per_sec`,
/// `bytes_per_sec`) are 0.0 — no live rate counters are maintained yet.
pub struct RealtimeStatsProvider {
    project: ProjectId,
    /// Optional source for budget data. When `None` all values are zero/0.0.
    source: Option<Arc<dyn RealtimeChannelSource>>,
    schema: SchemaRef,
}

impl std::fmt::Debug for RealtimeStatsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealtimeStatsProvider")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

impl RealtimeStatsProvider {
    pub fn new(project: ProjectId, source: Option<Arc<dyn RealtimeChannelSource>>) -> Self {
        Self {
            project,
            source,
            schema: Arc::new(stats_schema()),
        }
    }
}

#[async_trait]
impl TableProvider for RealtimeStatsProvider {
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
        let budget = self
            .source
            .as_ref()
            .map(|s| s.budget_for_project(self.project))
            .unwrap_or(BudgetSnapshot {
                memory_budget_bytes: 0,
                memory_used_bytes: 0,
            });

        let now_us: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
            .unwrap_or_default();

        // Rate columns: not yet tracked — always 0.0.
        // (See module doc: basin Phase 5.11.R7 follow-up for live counters.)
        let events_per_sec_arr: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64]));
        let errors_per_sec_arr: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64]));
        let bytes_per_sec_arr: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64]));
        let memory_budget_arr: ArrayRef =
            Arc::new(Int64Array::from(vec![budget.memory_budget_bytes]));
        let memory_used_arr: ArrayRef =
            Arc::new(Int64Array::from(vec![budget.memory_used_bytes]));
        let captured_at_arr: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![now_us]).with_timezone(SNAP_TZ),
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                events_per_sec_arr,
                errors_per_sec_arr,
                bytes_per_sec_arr,
                memory_budget_arr,
                memory_used_arr,
                captured_at_arr,
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let exec = MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}

// ---------------------------------------------------------------------------
// Registration helper
// ---------------------------------------------------------------------------

/// Register `basin_realtime.channels` and `basin_realtime.stats` into `ctx`.
///
/// Creates (or reuses) the `basin_realtime` DataFusion schema and registers
/// both providers. Called once per session from
/// `session::open_project_session` alongside the other schema registrations.
///
/// The `source` argument is the optional handle to `basin-realtime`'s runtime
/// registries. When `None` (no realtime sink wired up), the providers still
/// return valid empty rows.
pub(crate) fn register_realtime_providers(
    ctx: &datafusion::prelude::SessionContext,
    project: ProjectId,
    source: Option<Arc<dyn RealtimeChannelSource>>,
    notify: Arc<crate::notify_registry::NotifyRegistry>,
) -> DfResult<()> {
    let catalog_name = ctx
        .state()
        .config_options()
        .catalog
        .default_catalog
        .clone();
    let df_catalog = match ctx.catalog(&catalog_name) {
        Some(c) => c,
        None => {
            return Err(DataFusionError::Internal(format!(
                "basin_realtime registration: default catalog '{catalog_name}' not found"
            )));
        }
    };

    let rt_schema = Arc::new(MemorySchemaProvider::new());

    let channels_provider: Arc<dyn TableProvider> = Arc::new(
        RealtimeChannelsProvider::new(project, source.clone(), notify),
    );
    let stats_provider: Arc<dyn TableProvider> =
        Arc::new(RealtimeStatsProvider::new(project, source));

    rt_schema
        .register_table("channels".to_string(), channels_provider)
        .map_err(|e| {
            DataFusionError::Internal(format!(
                "basin_realtime.channels register: {e}"
            ))
        })?;
    rt_schema
        .register_table("stats".to_string(), stats_provider)
        .map_err(|e| {
            DataFusionError::Internal(format!(
                "basin_realtime.stats register: {e}"
            ))
        })?;

    // Use `basin_realtime` as the schema name to match the SQL queries issued
    // by the cloud handler (`FROM basin_realtime.channels`). DataFusion
    // resolves two-part names as `<catalog>.<schema>.<table>`, and a
    // `schema.table` reference resolves against the default catalog.
    df_catalog
        .register_schema("basin_realtime", rt_schema)
        .map_err(|e| {
            DataFusionError::Internal(format!(
                "basin_realtime schema register: {e}"
            ))
        })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use basin_common::ProjectId;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::notify_registry::NotifyRegistry;

    // ── Minimal in-test RealtimeChannelSource implementation ────────────────

    struct MockSource {
        channels: Vec<BroadcastChannelRow>,
        budget: BudgetSnapshot,
    }

    impl RealtimeChannelSource for MockSource {
        fn broadcast_channels_for_project(&self, _project: ProjectId) -> Vec<BroadcastChannelRow> {
            self.channels.clone()
        }
        fn budget_for_project(&self, _project: ProjectId) -> BudgetSnapshot {
            self.budget.clone()
        }
    }

    fn register_and_ctx(
        project: ProjectId,
        source: Option<Arc<dyn RealtimeChannelSource>>,
        notify: Arc<NotifyRegistry>,
    ) -> SessionContext {
        let ctx = SessionContext::new();
        register_realtime_providers(&ctx, project, source, notify).expect("registration must succeed");
        ctx
    }

    // ── channels table ───────────────────────────────────────────────────────

    /// No source attached, no notify channels → channels table is empty.
    #[tokio::test]
    async fn channels_empty_when_no_source_and_no_notify() {
        let project = ProjectId::new();
        let notify = Arc::new(NotifyRegistry::new());
        let ctx = register_and_ctx(project, None, notify);

        let batches = ctx
            .sql("SELECT channel_name FROM basin_realtime.channels")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    /// Broadcast channel from mock source appears with correct type and count.
    #[tokio::test]
    async fn channels_shows_broadcast_channel() {
        let project = ProjectId::new();
        let source = Arc::new(MockSource {
            channels: vec![BroadcastChannelRow {
                channel_name: "orders".to_string(),
                subscriber_count: 3,
            }],
            budget: BudgetSnapshot {
                memory_budget_bytes: 0,
                memory_used_bytes: 0,
            },
        });
        let notify = Arc::new(NotifyRegistry::new());
        let ctx = register_and_ctx(project, Some(source), notify);

        let batches = ctx
            .sql("SELECT channel_name, channel_type, subscriber_count FROM basin_realtime.channels")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);

        let b = &batches[0];
        let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let types = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(names.value(0), "orders");
        assert_eq!(types.value(0), "postgres_changes");
        assert_eq!(counts.value(0), 3);
    }

    /// LISTEN/NOTIFY channel from notify registry appears with correct type.
    #[tokio::test]
    async fn channels_shows_notify_channel() {
        let project = ProjectId::new();
        let notify = Arc::new(NotifyRegistry::new());
        let _sub = notify.subscribe(project, "alerts");

        let ctx = register_and_ctx(project, None, notify);
        let batches = ctx
            .sql("SELECT channel_name, channel_type, subscriber_count FROM basin_realtime.channels")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);

        let b = &batches[0];
        let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let types = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(names.value(0), "alerts");
        assert_eq!(types.value(0), "postgres_notify");
        assert_eq!(counts.value(0), 1);
    }

    /// Both broadcast and notify channels appear together.
    #[tokio::test]
    async fn channels_unions_broadcast_and_notify() {
        let project = ProjectId::new();
        let source = Arc::new(MockSource {
            channels: vec![BroadcastChannelRow {
                channel_name: "events".to_string(),
                subscriber_count: 2,
            }],
            budget: BudgetSnapshot {
                memory_budget_bytes: 0,
                memory_used_bytes: 0,
            },
        });
        let notify = Arc::new(NotifyRegistry::new());
        let _sub = notify.subscribe(project, "jobs");

        let ctx = register_and_ctx(project, Some(source), notify);
        let batches = ctx
            .sql("SELECT channel_name FROM basin_realtime.channels ORDER BY channel_name")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    // ── stats table ──────────────────────────────────────────────────────────

    /// Stats table has exactly one row with real budget values.
    #[tokio::test]
    async fn stats_returns_one_row_with_budget() {
        let project = ProjectId::new();
        let source = Arc::new(MockSource {
            channels: vec![],
            budget: BudgetSnapshot {
                memory_budget_bytes: 16 * 1024 * 1024,
                memory_used_bytes: 1024,
            },
        });
        let notify = Arc::new(NotifyRegistry::new());
        let ctx = register_and_ctx(project, Some(source), notify);

        let batches = ctx
            .sql("SELECT memory_budget_bytes, memory_used_bytes, events_per_sec FROM basin_realtime.stats")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);

        let b = &batches[0];
        let budget = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let used = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let eps = b.column(2).as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
        assert_eq!(budget.value(0), 16 * 1024 * 1024);
        assert_eq!(used.value(0), 1024);
        // Rate columns are flagged missing — must be 0.0
        assert_eq!(eps.value(0), 0.0, "events_per_sec not tracked; must be 0.0");
    }

    /// Stats `captured_at` self-join used by the cloud query works correctly.
    #[tokio::test]
    async fn stats_captured_at_self_join_works() {
        let project = ProjectId::new();
        let source = Arc::new(MockSource {
            channels: vec![],
            budget: BudgetSnapshot {
                memory_budget_bytes: 1000,
                memory_used_bytes: 42,
            },
        });
        let notify = Arc::new(NotifyRegistry::new());
        let ctx = register_and_ctx(project, Some(source), notify);

        // Mirror the exact cloud query (minus the project filter that the
        // cloud handler adds via pgwire — here we just run it in a fresh ctx).
        let batches = ctx
            .sql(
                "SELECT events_per_sec, errors_per_sec, bytes_per_sec, \
                        memory_budget_bytes, memory_used_bytes \
                 FROM basin_realtime.stats \
                 WHERE captured_at = (SELECT MAX(captured_at) FROM basin_realtime.stats) \
                 LIMIT 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "cloud stats query must return exactly one row");

        let b = &batches[0];
        let budget = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        let used = b.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(budget.value(0), 1000);
        assert_eq!(used.value(0), 42);
    }

    /// Stats table with no source attached returns zero budget and zero rates.
    #[tokio::test]
    async fn stats_no_source_returns_zeroes() {
        let project = ProjectId::new();
        let notify = Arc::new(NotifyRegistry::new());
        let ctx = register_and_ctx(project, None, notify);

        let batches = ctx
            .sql("SELECT memory_budget_bytes, memory_used_bytes FROM basin_realtime.stats")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &batches[0];
        let budget = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let used = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(budget.value(0), 0);
        assert_eq!(used.value(0), 0);
    }
}
