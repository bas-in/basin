//! `NotifyRegistry` — SQL-level `LISTEN` / `NOTIFY` pub-sub primitive.
//!
//! # Why a separate registry from `basin-realtime`?
//!
//! `basin-realtime::ChannelRegistry` is keyed by `(ProjectId, TableName)` and
//! delivers `ChangeEvent` payloads (post-commit table mutations) to SSE / WS
//! subscribers. SQL `LISTEN <channel>` / `NOTIFY <channel>, '<payload>'`
//! addresses an entirely different namespace:
//!
//! * Channel name is an arbitrary identifier chosen by the application,
//!   unrelated to any table.
//! * Payload is an opaque text string, not a structured `ChangeEvent`.
//! * Subscribers are per-pgwire-session, not per-HTTP-connection.
//!
//! Trying to overload `ChannelRegistry` with both would either bloat
//! `ChangeEvent` or force a synthetic table name; both are worse than a
//! purpose-built primitive. (The two registries can co-exist on the same
//! `Engine` — they serve different transports and different payloads.)
//!
//! # Placement
//!
//! Lives in `basin-engine` because `basin-engine -> basin-realtime` would
//! create a dependency cycle (`basin-realtime -> basin-webhooks ->
//! basin-engine`). The primitive is a thin DashMap fanout; moving it
//! crate-side does not couple it to engine internals.
//!
//! # Async-delivery follow-up
//!
//! Pgwire `NotificationResponse` (message type `'A'`) is delivered
//! asynchronously between query executions. This module only owns the
//! in-process fanout; wiring `NotificationResponse` emit into the pgwire
//! protocol driver (`basin-router::protocol`) is tracked as a follow-up
//! task. Until that lands, `NOTIFY` writes to the registry but subscribers
//! can only observe by polling [`ListenSubscription::try_recv`] (used by
//! the `pg_listening_channels()` virtual function and integration tests).

use std::any::Any;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_common::ProjectId;
use dashmap::DashMap;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::Result as DFResult;
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tokio::sync::broadcast;

/// Per-channel ring-buffer capacity. Small by design: SQL `LISTEN` is a
/// fire-and-forget signalling channel, not a durable queue. A subscriber
/// that falls behind by more than this many notifications gets a
/// `RecvError::Lagged` and must reconnect. Postgres itself enforces an
/// 8 GB *cluster-wide* queue cap; we make the per-channel buffer small to
/// keep idle memory bounded.
pub const DEFAULT_NOTIFY_CAPACITY: usize = 256;

/// One `NOTIFY` event: channel name + opaque text payload + the originating
/// session's backend pid (for the eventual pgwire `NotificationResponse`).
#[derive(Clone, Debug)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub from_pid: i32,
}

/// Key for the per-project channel map: `(ProjectId, channel-name)`.
///
/// Channel names are case-insensitive in PG (unquoted identifiers fold to
/// lowercase). We normalise to lowercase at insert / lookup time so
/// `LISTEN Foo` and `NOTIFY foo` address the same subscribers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NotifyKey {
    project: ProjectId,
    channel: String,
}

impl NotifyKey {
    fn new(project: ProjectId, channel: &str) -> Self {
        Self {
            project,
            channel: channel.to_ascii_lowercase(),
        }
    }
}

struct ChannelEntry {
    sender: broadcast::Sender<Notification>,
}

/// One row returned by [`NotifyRegistry::channels_for_project`]. Represents
/// a single active SQL LISTEN/NOTIFY channel with at least one live subscriber.
#[derive(Debug, Clone)]
pub struct NotifyChannelRow {
    /// Normalised (lowercase) channel name.
    pub channel_name: String,
    /// Number of live receivers (sessions that issued LISTEN on this channel).
    pub subscriber_count: i64,
}

/// Process-wide SQL `LISTEN` / `NOTIFY` registry. One per [`crate::Engine`].
/// Cheap to clone (all state behind an `Arc`).
#[derive(Clone)]
pub struct NotifyRegistry {
    channels: Arc<DashMap<NotifyKey, ChannelEntry>>,
    capacity: usize,
}

impl NotifyRegistry {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            capacity: DEFAULT_NOTIFY_CAPACITY,
        }
    }

    /// Subscribe to `channel` in `project`. Returns a handle the caller
    /// stores; when the handle is dropped the broadcast receiver is closed
    /// automatically (no explicit UNLISTEN required for session-end
    /// cleanup).
    pub fn subscribe(&self, project: ProjectId, channel: &str) -> ListenSubscription {
        let key = NotifyKey::new(project, channel);
        let entry = self.channels.entry(key.clone()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(self.capacity);
            ChannelEntry { sender }
        });
        let rx = entry.sender.subscribe();
        ListenSubscription {
            channel: key.channel.clone(),
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    /// Publish `payload` on `channel` in `project`. Returns the number of
    /// active subscribers the notification reached (0 when no listeners —
    /// that is not an error, matches PG).
    pub fn publish(
        &self,
        project: ProjectId,
        channel: &str,
        payload: &str,
        from_pid: i32,
    ) -> usize {
        let key = NotifyKey::new(project, channel);
        match self.channels.get(&key) {
            Some(entry) => {
                let n = Notification {
                    channel: key.channel.clone(),
                    payload: payload.to_string(),
                    from_pid,
                };
                entry.sender.send(n).unwrap_or(0)
            }
            None => 0,
        }
    }

    /// Active receiver count for `(project, channel)`. Used by tests to
    /// confirm a subscriber dropped after session close / pool return.
    pub fn subscriber_count(&self, project: ProjectId, channel: &str) -> usize {
        let key = NotifyKey::new(project, channel);
        match self.channels.get(&key) {
            Some(entry) => entry.sender.receiver_count(),
            None => 0,
        }
    }

    /// Number of distinct `(project, channel)` pairs that have ever had a
    /// subscriber (entries persist until `prune` is called).
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Snapshot all active LISTEN/NOTIFY channels for `project`.
    ///
    /// Returns one [`NotifyChannelRow`] per `(project, channel)` entry whose
    /// sender still has at least one live receiver. Entries that have been
    /// `prune`d or never subscribed are omitted.
    pub fn channels_for_project(&self, project: ProjectId) -> Vec<NotifyChannelRow> {
        self.channels
            .iter()
            .filter_map(|entry| {
                if entry.key().project != project {
                    return None;
                }
                let subscriber_count = entry.value().sender.receiver_count();
                if subscriber_count == 0 {
                    return None;
                }
                Some(NotifyChannelRow {
                    channel_name: entry.key().channel.clone(),
                    subscriber_count: subscriber_count as i64,
                })
            })
            .collect()
    }

    /// GC channels whose last subscriber has dropped. Intended for a
    /// periodic sweeper; not required for correctness (an entry with 0
    /// receivers just costs the `DashMap` slot until the next prune).
    pub fn prune(&self) {
        self.channels
            .retain(|_, entry| entry.sender.receiver_count() > 0);
    }
}

impl Default for NotifyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A per-session subscription handle. Dropping it removes the underlying
/// broadcast receiver from the channel; once all receivers are gone the
/// entry is eligible for `NotifyRegistry::prune`.
pub struct ListenSubscription {
    /// Normalised (lowercase) channel name this subscription is bound to.
    /// Exposed so `pg_listening_channels()` can report what the session is
    /// listening on without re-walking the registry.
    pub channel: String,
    /// Wrapped in a `Mutex` because the session stores subscriptions
    /// behind an `Arc` so the per-session UDTF can read them without
    /// re-acquiring the outer `ListenState` mutex on each scan, while
    /// `broadcast::Receiver::try_recv` still needs `&mut self`.
    rx: tokio::sync::Mutex<broadcast::Receiver<Notification>>,
}

impl ListenSubscription {
    /// Non-blocking poll. Returns `Some` if a notification is queued,
    /// `None` otherwise. Used by the eventual pgwire async-delivery emitter
    /// between query executions, and by tests.
    pub fn try_recv(&self) -> Option<Notification> {
        let mut guard = match self.rx.try_lock() {
            Ok(g) => g,
            Err(_) => return None,
        };
        match guard.try_recv() {
            Ok(n) => Some(n),
            Err(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// pg_listening_channels() — per-session UDTF
// ---------------------------------------------------------------------------

/// Per-session SQL UDTF wrapping a closure that produces the current set
/// of LISTEN channels for the calling session. Each `ProjectSession::open`
/// registers a fresh instance bound to its own `Arc<SessionState>`.
struct PgListeningChannelsTf {
    /// Snapshotter — returns the channel names this session is currently
    /// listening on, in sorted order. Heap-allocated because we cannot
    /// name the closure type in the struct.
    snapshot: Box<dyn Fn() -> Vec<String> + Send + Sync + 'static>,
}

impl std::fmt::Debug for PgListeningChannelsTf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgListeningChannelsTf")
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for PgListeningChannelsTf {
    fn call(&self, _args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let channels = (self.snapshot)();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "pg_listening_channels",
            DataType::Utf8,
            false,
        )]));
        let arr: ArrayRef = Arc::new(StringArray::from(channels));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!(
                "pg_listening_channels: RecordBatch build failed: {e}"
            ))
        })?;
        Ok(Arc::new(PgListeningChannelsTable {
            schema,
            batches: vec![batch],
        }))
    }
}

#[derive(Debug)]
struct PgListeningChannelsTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for PgListeningChannelsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// Register a per-session `pg_listening_channels()` table function on
/// `ctx`. The snapshot closure captures `state` so each session sees its
/// own subscription set (PG semantics — `pg_listening_channels` is a
/// per-backend view).
pub(crate) fn register_pg_listening_channels(
    ctx: &SessionContext,
    state: Arc<crate::session::SessionState>,
) {
    let tf = PgListeningChannelsTf {
        snapshot: Box::new(move || crate::session::listen_channels(&state)),
    };
    ctx.register_udtf("pg_listening_channels", Arc::new(tf));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_and_receive_roundtrip() {
        let reg = NotifyRegistry::new();
        let project = ProjectId::new();
        let sub = reg.subscribe(project, "ch1");
        let n = reg.publish(project, "ch1", "hello", 1);
        assert_eq!(n, 1);
        let got = sub.try_recv().expect("notification must be buffered");
        assert_eq!(got.channel, "ch1");
        assert_eq!(got.payload, "hello");
        assert_eq!(got.from_pid, 1);
    }

    #[tokio::test]
    async fn channel_names_are_case_insensitive() {
        let reg = NotifyRegistry::new();
        let project = ProjectId::new();
        let sub = reg.subscribe(project, "Foo");
        let n = reg.publish(project, "foo", "p", 7);
        assert_eq!(n, 1);
        assert_eq!(sub.try_recv().unwrap().channel, "foo");
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_returns_zero() {
        let reg = NotifyRegistry::new();
        let project = ProjectId::new();
        assert_eq!(reg.publish(project, "ghost", "x", 1), 0);
    }

    #[tokio::test]
    async fn projects_are_isolated() {
        let reg = NotifyRegistry::new();
        let project_a = ProjectId::new();
        let project_b = ProjectId::new();
        let sub_a = reg.subscribe(project_a, "shared");
        // No subscribers in project_b for the same channel name.
        let n = reg.publish(project_b, "shared", "p", 1);
        assert_eq!(n, 0);
        assert!(sub_a.try_recv().is_none());
        let n2 = reg.publish(project_a, "shared", "p", 1);
        assert_eq!(n2, 1);
        assert_eq!(sub_a.try_recv().unwrap().payload, "p");
    }

    #[tokio::test]
    async fn dropping_subscription_releases_slot() {
        let reg = NotifyRegistry::new();
        let project = ProjectId::new();
        let sub = reg.subscribe(project, "ephemeral");
        assert_eq!(reg.subscriber_count(project, "ephemeral"), 1);
        drop(sub);
        assert_eq!(reg.subscriber_count(project, "ephemeral"), 0);
    }
}
