//! `basin-realtime` — WebSocket / SSE change-event fan-out (Phase 5.11.R series).
//!
//! # Architecture
//!
//! [`RealtimeSink`] implements [`ChangeEventSink`] and is attached to the
//! engine as a post-commit sink via `Engine::attach_post_commit_sink`. On each
//! committed mutation the sink broadcasts the event to every subscriber that
//! has an open channel on `(project, table)`.
//!
//! The per-`(project, table)` channel is a `tokio::sync::broadcast` channel.
//! The channel registry is a [`DashMap`] keyed by [`ChannelKey`] so
//! subscriber lookup is O(1) in the common case. A lagged receiver gets a
//! [`tokio::sync::broadcast::error::RecvError::Lagged`] error — it is the
//! subscriber's responsibility to reconnect and replay via the webhook retry
//! log (5.11.I) to catch up.
//!
//! # Replay cursor
//!
//! On reconnect, a client supplies the last `seq` it successfully processed.
//! [`ReplayCursor`] wraps the webhook retry-log interface (5.11.I) and
//! re-feeds events with `seq > last_seen_seq` back into the client before
//! handing off to the live broadcast stream. This ensures at-least-once
//! delivery across transient disconnects without requiring the broadcast
//! channel to buffer unboundedly.
//!
//! # Module layout
//!
//! | Module | Status | Phase |
//! |--------|--------|-------|
//! | [`sse`] | stub | R2 |
//! | [`ws`] | stub | R3 |
//! | [`presence`] | stub | R4 |
//! | [`filter`] | stub | R5 |
//! | [`budget`] | stub | R6 |
//!
//! See each module for the TODO comments that define the R2-R6 contracts.

pub mod budget;
pub mod filter;
pub mod presence;
pub mod sse;
pub mod ws;

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ChangeEvent, ChangeEventSink, ProjectId, Result, TableName};
use dashmap::DashMap;
use tokio::sync::broadcast;

/// Capacity of each per-`(project, table)` broadcast channel.
///
/// 1 024 events per channel. At ~1 KB per event that is ~1 MiB of in-flight
/// headroom per active table. A lagged receiver falls off the ring and must
/// replay from the webhook retry log (5.11.I). Raise via
/// `RealtimeSink::with_capacity` if profiling shows frequent lag events.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1_024;

/// Key for the per-`(project, table)` broadcast channel registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChannelKey {
    pub project: ProjectId,
    pub table: TableName,
}

impl ChannelKey {
    pub fn new(project: ProjectId, table: TableName) -> Self {
        Self { project, table }
    }
}

/// Per-channel state: a broadcast sender and the configured capacity.
/// The capacity is stored so we can re-create the channel with the same
/// settings if the last receiver drops and a new one arrives later.
///
/// TODO(R4): use `capacity` in `ChannelRegistry::subscribe` to re-create
/// channels with the same buffer size when the sender is replaced after
/// all receivers have dropped (the current impl keeps the sender alive
/// as long as `ChannelRegistry` holds it, so re-creation is not needed yet).
#[allow(dead_code)]
struct ChannelEntry {
    sender: broadcast::Sender<Arc<ChangeEvent>>,
    capacity: usize,
}

/// Registry of per-`(project, table)` broadcast channels.
///
/// Cheap to clone — all state is behind an `Arc`-backed [`DashMap`].
#[derive(Clone, Default)]
pub struct ChannelRegistry {
    channels: Arc<DashMap<ChannelKey, ChannelEntry>>,
    default_capacity: usize,
}

impl ChannelRegistry {
    pub fn new(default_capacity: usize) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            default_capacity,
        }
    }

    /// Subscribe to events for `(project, table)`. Creates the channel if
    /// none exists yet. Multiple receivers share the same broadcast sender.
    pub fn subscribe(&self, key: ChannelKey) -> broadcast::Receiver<Arc<ChangeEvent>> {
        let entry = self.channels.entry(key).or_insert_with(|| {
            let (sender, _) = broadcast::channel(self.default_capacity);
            ChannelEntry {
                sender,
                capacity: self.default_capacity,
            }
        });
        entry.sender.subscribe()
    }

    /// Publish an event into the channel for `(project, table)`.
    ///
    /// Returns `Ok(n)` where `n` is the number of active receivers the send
    /// reached (0 when no subscribers are connected — a no-op). Returns
    /// `Err` only if the channel no longer exists, which cannot happen as long
    /// as at least one sender is alive (i.e. `self` is alive).
    pub fn publish(&self, key: &ChannelKey, event: Arc<ChangeEvent>) -> usize {
        match self.channels.get(key) {
            Some(entry) => entry.sender.send(event).unwrap_or(0),
            None => 0,
        }
    }

    /// Number of currently registered `(project, table)` channels (channels
    /// persist even when all receivers have dropped — they are GC'd lazily
    /// when a new subscriber arrives or when `prune` is called).
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Remove channels whose sender has no live receivers. Intended to be
    /// called from a periodic maintenance task to bound memory use when
    /// tables are dropped or projects become idle.
    ///
    /// TODO(R4): wire into basin-presence so presence signals trigger prune.
    pub fn prune(&self) {
        self.channels
            .retain(|_, entry| entry.sender.receiver_count() > 0);
    }
}

/// Post-commit [`ChangeEventSink`] that fans committed mutations out to
/// per-`(project, table)` broadcast channels.
///
/// # Wiring
///
/// ```ignore
/// // In services/basin-server/src/main.rs, behind #[cfg(feature = "realtime")]:
/// let realtime_sink = RealtimeSink::new();
/// engine.attach_post_commit_sink(Arc::new(realtime_sink.clone()));
/// ```
///
/// # Zero-overhead when idle
///
/// `publish` performs a [`DashMap`] lookup per event. When no subscribers are
/// connected the lookup finds no entry (or finds an entry with 0 receivers)
/// and returns immediately. The allocation cost is O(1) regardless of project
/// or table count.
#[derive(Clone)]
pub struct RealtimeSink {
    registry: ChannelRegistry,
}

impl RealtimeSink {
    /// Build with [`DEFAULT_CHANNEL_CAPACITY`].
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// Build with a custom per-channel ring buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            registry: ChannelRegistry::new(capacity),
        }
    }

    /// Borrow the underlying [`ChannelRegistry`]. R2-R6 use this to hand
    /// receivers to SSE / WebSocket handlers.
    pub fn registry(&self) -> &ChannelRegistry {
        &self.registry
    }
}

impl Default for RealtimeSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ChangeEventSink for RealtimeSink {
    /// Fan the event out to all open subscribers for `(project, table)`.
    ///
    /// This is O(1) — one [`DashMap`] shard lock, one `broadcast::Sender::send`.
    /// Lagged receivers get a [`tokio::sync::broadcast::error::RecvError::Lagged`]
    /// error on their next `recv()` and are responsible for reconnecting via
    /// the replay path ([`ReplayCursor`]).
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        let key = ChannelKey::new(event.project, event.table.clone());
        self.registry.publish(&key, Arc::new(event.clone()));
        Ok(())
    }
}

/// Replay cursor for catch-up after a client reconnect.
///
/// On reconnect a client supplies the last `seq` it successfully received.
/// The cursor re-feeds events with `seq > last_seen_seq` from the webhook
/// retry log (5.11.I) back to the client, then hands off to the live
/// broadcast receiver.
///
/// # Status: stub for R2
///
/// TODO(R2): inject a `RetryQueue` / WAL handle here and implement
///   `replay_from(project, table, last_seen_seq)` to drain catch-up events.
///   The live receiver should be created *before* draining catch-up to
///   avoid a race: new events published between drain-end and subscribe
///   would be lost otherwise.
pub struct ReplayCursor {
    pub project: ProjectId,
    pub table: TableName,
    /// The last sequence number the client saw. Events with `seq > last_seen`
    /// will be replayed on the next `drain` call.
    pub last_seen: u64,
}

impl ReplayCursor {
    pub fn new(project: ProjectId, table: TableName, last_seen: u64) -> Self {
        Self {
            project,
            table,
            last_seen,
        }
    }

    /// Drain catch-up events from the webhook retry log.
    ///
    /// TODO(R2): implement using the 5.11.I `RetryQueue` interface once the
    ///   API for querying by `(project, table, seq > last_seen)` is exposed.
    ///   For now returns an empty iterator.
    pub fn drain(&self) -> Vec<Arc<ChangeEvent>> {
        // TODO(R2): replace with actual retry-log query:
        //   queue.events_after(self.project, &self.table, self.last_seen)
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ChangeEvent, ChangeOp};
    use chrono::Utc;

    fn make_event(project: ProjectId, table: &str, seq: u64) -> ChangeEvent {
        ChangeEvent {
            project,
            table: TableName::new(table).unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": seq})),
            committed_at: Utc::now(),
            seq,
            causation_user: None,
        }
    }

    /// Smoke test: a ChangeEvent published by the engine (via RealtimeSink)
    /// appears on the in-memory broadcast channel.
    #[tokio::test]
    async fn smoke_publish_received_on_channel() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        // Subscribe before publishing.
        let mut rx = sink.registry().subscribe(key.clone());

        // Publish via the ChangeEventSink trait (simulates engine post-commit).
        let event = make_event(project, "orders", 1);
        sink.publish(&event).await.expect("publish must not fail");

        // The broadcast channel must deliver the event.
        let received = rx.try_recv().expect("event must be available immediately");
        assert_eq!(received.seq, 1);
        assert_eq!(received.project, project);
        assert_eq!(received.table, table);
    }

    /// No subscribers — publish is a silent no-op (0 receivers reached).
    #[tokio::test]
    async fn publish_with_no_subscribers_is_noop() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let event = make_event(project, "orders", 42);
        // Must not panic or error even when nobody is subscribed.
        sink.publish(&event).await.expect("publish must not fail");
    }

    /// Multiple subscribers on the same (project, table) both receive events.
    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let table = TableName::new("events").unwrap();
        let key = ChannelKey::new(project, table.clone());

        let mut rx1 = sink.registry().subscribe(key.clone());
        let mut rx2 = sink.registry().subscribe(key.clone());

        let event = make_event(project, "events", 7);
        sink.publish(&event).await.unwrap();

        let got1 = rx1.try_recv().unwrap();
        let got2 = rx2.try_recv().unwrap();
        assert_eq!(got1.seq, 7);
        assert_eq!(got2.seq, 7);
    }

    /// Different (project, table) pairs get isolated channels.
    #[tokio::test]
    async fn different_tables_are_isolated() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();

        let key_a = ChannelKey::new(project, TableName::new("alpha").unwrap());
        let key_b = ChannelKey::new(project, TableName::new("beta").unwrap());

        let mut rx_a = sink.registry().subscribe(key_a);
        let mut rx_b = sink.registry().subscribe(key_b);

        // Publish only to "alpha".
        let event = make_event(project, "alpha", 1);
        sink.publish(&event).await.unwrap();

        // "alpha" receiver has the event.
        assert!(rx_a.try_recv().is_ok());
        // "beta" receiver has nothing.
        assert!(rx_b.try_recv().is_err());
    }

    /// ReplayCursor stub returns an empty vec and does not panic.
    #[test]
    fn replay_cursor_stub_returns_empty() {
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let cursor = ReplayCursor::new(project, table, 0);
        assert!(cursor.drain().is_empty());
    }
}
