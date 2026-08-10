//! Adapter that implements `basin-engine`'s [`RealtimeChannelSource`] trait
//! using `basin-realtime`'s live registries.
//!
//! This module bridges the seam between `basin-engine` (which defines the
//! trait) and `basin-realtime` (which holds the live state). It lives here
//! because `basin-rest` is already downstream of both crates and can safely
//! depend on them simultaneously without creating a dependency cycle.
//!
//! # Wiring
//!
//! Call [`make_realtime_source`] after constructing a [`RealtimeSink`] and
//! pass the result to [`Engine::attach_realtime_source`]:
//!
//! ```ignore
//! let sink = basin_realtime::RealtimeSink::new();
//! engine.attach_realtime_source(realtime_source::make_realtime_source(&sink));
//! engine.attach_post_commit_sink(Arc::new(sink));
//! ```

use std::sync::Arc;

use basin_common::ProjectId;
use basin_engine::realtime_catalog::{BroadcastChannelRow, BudgetSnapshot, RealtimeChannelSource};
use basin_realtime::{BudgetTracker, ChannelRegistry};

/// Combined view of `ChannelRegistry` (broadcast channels) and `BudgetTracker`
/// (memory budget) that backs the `basin_realtime.channels` and
/// `basin_realtime.stats` virtual tables.
pub struct RealtimeSinkSource {
    registry: ChannelRegistry,
    budget: BudgetTracker,
}

impl RealtimeSinkSource {
    pub fn new(registry: ChannelRegistry, budget: BudgetTracker) -> Self {
        Self { registry, budget }
    }
}

impl RealtimeChannelSource for RealtimeSinkSource {
    fn broadcast_channels_for_project(&self, project: ProjectId) -> Vec<BroadcastChannelRow> {
        self.registry
            .channels_for_project(project)
            .into_iter()
            .map(|row| BroadcastChannelRow {
                channel_name: row.channel_name,
                subscriber_count: row.subscriber_count,
            })
            .collect()
    }

    fn budget_for_project(&self, project: ProjectId) -> BudgetSnapshot {
        BudgetSnapshot {
            memory_budget_bytes: self.budget.hard_cap() as i64,
            memory_used_bytes: self.budget.bytes_in_flight(project) as i64,
        }
    }
}

/// Build an [`Arc<dyn RealtimeChannelSource>`] from a [`RealtimeSink`].
///
/// Extracts the registry and budget tracker from the sink so that the engine's
/// virtual tables (`basin_realtime.channels`, `basin_realtime.stats`) see the
/// same live state as the post-commit fanout path.
pub fn make_realtime_source(sink: &basin_realtime::RealtimeSink) -> Arc<dyn RealtimeChannelSource> {
    Arc::new(RealtimeSinkSource::new(
        sink.registry().clone(),
        sink.budget().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;
    use basin_common::{ChangeEvent, ChangeOp, TableName};
    use basin_engine::realtime_catalog::RealtimeChannelSource;
    use basin_realtime::{ChannelKey, RealtimeSink};
    use chrono::Utc;

    fn make_sink() -> RealtimeSink {
        RealtimeSink::new()
    }

    fn make_event(project: ProjectId, table: &str) -> ChangeEvent {
        ChangeEvent {
            project,
            table: TableName::new(table).unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        }
    }

    /// Subscribe to a broadcast channel, then verify the source reports it
    /// with the correct subscriber count.
    #[test]
    fn broadcast_channel_appears_with_subscriber_count() {
        let sink = make_sink();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        // Subscribe — this creates the channel entry.
        let _rx1 = sink.registry().subscribe(key.clone());
        let _rx2 = sink.registry().subscribe(key.clone());

        let source = make_realtime_source(&sink);
        let rows = source.broadcast_channels_for_project(project);
        assert_eq!(rows.len(), 1, "one channel for the project");
        assert_eq!(rows[0].channel_name, "orders");
        assert_eq!(rows[0].subscriber_count, 2);
    }

    /// After the receivers drop the channel is no longer reported.
    #[test]
    fn channel_absent_after_receivers_drop() {
        let sink = make_sink();
        let project = ProjectId::new();
        let key = ChannelKey::new(project, TableName::new("users").unwrap());

        {
            let _rx = sink.registry().subscribe(key.clone());
            // rx alive: channel appears
            let source = make_realtime_source(&sink);
            assert_eq!(source.broadcast_channels_for_project(project).len(), 1);
        }
        // rx dropped: subscriber_count == 0, channel excluded
        let source = make_realtime_source(&sink);
        assert_eq!(source.broadcast_channels_for_project(project).len(), 0);
    }

    /// Budget snapshot reflects the tracker's hard cap.
    #[test]
    fn budget_snapshot_reflects_hard_cap() {
        let sink = RealtimeSink::with_capacity(64);
        let project = ProjectId::new();
        let source = make_realtime_source(&sink);
        let snap = source.budget_for_project(project);
        // Hard cap comes from the environment (DEFAULT_PER_PROJECT_BUDGET_BYTES
        // = 16 MiB) or the tracker default.
        assert!(snap.memory_budget_bytes > 0);
        // No events published yet → used is 0.
        assert_eq!(snap.memory_used_bytes, 0);
    }

    /// Channels from a different project are not returned.
    #[test]
    fn cross_project_isolation() {
        let sink = make_sink();
        let project_a = ProjectId::new();
        let project_b = ProjectId::new();
        let _rx = sink
            .registry()
            .subscribe(ChannelKey::new(project_a, TableName::new("tbl").unwrap()));

        let source = make_realtime_source(&sink);
        // project_a has one channel, project_b has none.
        assert_eq!(source.broadcast_channels_for_project(project_a).len(), 1);
        assert_eq!(source.broadcast_channels_for_project(project_b).len(), 0);
    }
}
