//! In-process per-project live broadcast for the CDC stream's live-tail.
//!
//! This is the "fast-path overlay" of ADR 0028 §1: the durable ring is the
//! at-least-once contract, while this in-memory broadcast lets a connected,
//! caught-up subscriber receive new events with sub-millisecond latency
//! without round-tripping the object store.
//!
//! It is intentionally **project-scoped** (one channel per project), not
//! per-`(project, table)` like `basin-realtime` — CDC streams are
//! project-scoped (ADR §4) and the SSE handler applies table/op filters
//! itself. A lagged or absent subscriber simply falls back to a durable
//! ring replay on (re)connect, so this channel can be lossy without violating
//! the delivery contract.

use std::sync::Arc;

use basin_common::ProjectId;
use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::record::CdcRecord;

/// Per-project live-tail broadcast capacity. A subscriber that lags past this
/// is told to reconnect; reconnect replays from the durable ring, so no
/// committed event is lost.
pub const LIVE_CHANNEL_CAPACITY: usize = 1_024;

/// Registry of per-project broadcast channels for the live tail. Cheap to
/// clone (all state behind an `Arc`).
#[derive(Clone, Default)]
pub struct LiveRegistry {
    channels: Arc<DashMap<ProjectId, broadcast::Sender<Arc<CdcRecord>>>>,
}

impl LiveRegistry {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Subscribe to live records for `project`. Creates the channel lazily.
    pub fn subscribe(&self, project: ProjectId) -> broadcast::Receiver<Arc<CdcRecord>> {
        let entry = self
            .channels
            .entry(project)
            .or_insert_with(|| broadcast::channel(LIVE_CHANNEL_CAPACITY).0);
        entry.subscribe()
    }

    /// Publish a record to live subscribers of its project. No-op (0 reach)
    /// when nobody is connected — the common idle case.
    pub fn publish(&self, project: ProjectId, rec: Arc<CdcRecord>) {
        if let Some(tx) = self.channels.get(&project) {
            // `send` errors only when there are no receivers; that is fine.
            let _ = tx.send(rec);
        }
    }
}
