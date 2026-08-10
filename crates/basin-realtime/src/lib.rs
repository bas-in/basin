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
pub mod retry_queue;
pub mod sse;
pub mod ws;

pub use budget::{
    estimate_event_size, BudgetError, BudgetGuard, BudgetTracker, DEFAULT_PER_PROJECT_BUDGET_BYTES,
};
pub use filter::Filter;
pub use presence::{
    serialize_presence_diff, serialize_presence_state, ChannelName, ClientId, PresenceConfig,
    PresenceEntry, PresenceEvent, PresenceMeta, PresenceRegistry,
};
pub use retry_queue::{
    DrainOutcome, ReplayRingRegistry, DEFAULT_RING_CAPACITY as DEFAULT_REPLAY_RING_CAPACITY,
    DEFAULT_RING_TTL as DEFAULT_REPLAY_RING_TTL,
};

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

/// One row returned by [`ChannelRegistry::channels_for_project`].
/// Represents a single active broadcast (table-change) channel.
#[derive(Debug, Clone)]
pub struct ChannelRow {
    /// The table name used as the channel identifier.
    pub channel_name: String,
    /// Number of live broadcast receivers currently subscribed.
    pub subscriber_count: i64,
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

    /// Subscribe with an optional predicate filter (Phase 5.11.R5).
    ///
    /// When `filter` is `Some`, the returned [`FilteredReceiver`] evaluates
    /// the predicate on each event before returning it. Events that do not
    /// match are silently discarded at receive time, so the transport layer
    /// (`sse.rs`, `ws.rs`) never sees them. When `filter` is `None` the
    /// behaviour is identical to [`Self::subscribe`].
    pub fn subscribe_filtered(&self, key: ChannelKey, filter: Option<Filter>) -> FilteredReceiver {
        let rx = self.subscribe(key);
        FilteredReceiver { rx, filter }
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

    /// Snapshot all broadcast channels for `project` that have at least one
    /// live receiver. Channels whose last subscriber dropped (receiver_count
    /// == 0) are excluded — they are only visible after `prune` removes them.
    pub fn channels_for_project(&self, project: ProjectId) -> Vec<ChannelRow> {
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
                Some(ChannelRow {
                    channel_name: entry.key().table.to_string(),
                    subscriber_count: subscriber_count as i64,
                })
            })
            .collect()
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

/// A broadcast receiver with an optional subscriber-side predicate filter
/// (Phase 5.11.R5).
///
/// Created via [`ChannelRegistry::subscribe_filtered`]. The filter is evaluated
/// in the subscriber's async task at receive time — events that do not satisfy
/// the predicate are discarded before they reach the transport layer
/// (`sse.rs`, `ws.rs`). This is logically equivalent to "fanout-time"
/// filtering because filtered events never hit the wire.
///
/// # Compile-once evaluation
///
/// The SQL predicate is parsed once by [`Filter::new`] and stored in an
/// `Arc<Expr>`. Each call to [`FilteredReceiver::recv`] reuses the parsed AST
/// without re-parsing. Typical eval latency for simple comparisons is ≤ 50 µs
/// (see `benches/filter_eval.rs`).
pub struct FilteredReceiver {
    rx: broadcast::Receiver<Arc<ChangeEvent>>,
    filter: Option<Filter>,
}

impl FilteredReceiver {
    /// Construct a [`FilteredReceiver`] directly from a raw broadcast receiver
    /// and an optional filter. Useful in tests that create channels manually
    /// without going through [`ChannelRegistry::subscribe_filtered`].
    pub fn from_receiver(
        rx: broadcast::Receiver<Arc<ChangeEvent>>,
        filter: Option<Filter>,
    ) -> Self {
        Self { rx, filter }
    }

    /// Receive the next event that satisfies the filter, skipping non-matching
    /// events.
    ///
    /// Returns `Err` on lag (ring-buffer overflow) or channel close, matching
    /// [`broadcast::Receiver::recv`] semantics. If predicate evaluation fails
    /// for an event (unsupported expression) that event is skipped (fail-closed).
    pub async fn recv(&mut self) -> Result<Arc<ChangeEvent>, broadcast::error::RecvError> {
        loop {
            let event = self.rx.recv().await?;
            match &self.filter {
                None => return Ok(event),
                Some(f) => match f.matches(&event) {
                    Ok(true) => return Ok(event),
                    Ok(false) => {
                        // Predicate not satisfied; skip silently.
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            predicate = %f,
                            error = %e,
                            "realtime filter eval failed; skipping event",
                        );
                        continue;
                    }
                },
            }
        }
    }

    /// Try to receive without blocking. Returns `Err(TryRecvError::Empty)` if
    /// no matching event is immediately available (including if all buffered
    /// events are filtered out).
    pub fn try_recv(&mut self) -> Result<Arc<ChangeEvent>, broadcast::error::TryRecvError> {
        loop {
            let event = self.rx.try_recv()?;
            match &self.filter {
                None => return Ok(event),
                Some(f) => match f.matches(&event) {
                    Ok(true) => return Ok(event),
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(
                            predicate = %f,
                            error = %e,
                            "realtime filter eval failed; skipping event",
                        );
                        continue;
                    }
                },
            }
        }
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
///
/// # Multi-project memory budget (Phase 5.11.R6)
///
/// Each [`RealtimeSink`] holds a [`BudgetTracker`] that enforces a
/// per-project in-flight byte cap (default 16 MiB, configurable via
/// `BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES`). The hot path:
///
/// 1. Estimate the event size (JSON payload + fixed overhead).
/// 2. `BudgetTracker::try_reserve` — CAS on the project's `AtomicU64`;
///    O(1) with no allocation.
/// 3. On success: broadcast the event and release the bytes (RAII guard).
/// 4. On `BUFFER_FULL`: drop the event into the optional durable retry log
///    so no data is lost; skip the broadcast for this project only.
///
/// Other projects' ring-buffer slots are unaffected by a noisy project's
/// `BUFFER_FULL` rejections.
#[derive(Clone)]
pub struct RealtimeSink {
    registry: ChannelRegistry,
    /// Per-project byte budget tracker (Phase 5.11.R6).
    budget: BudgetTracker,
    /// Optional durable retry log. When `Some`, events that overflow the
    /// per-project budget are written here rather than silently discarded.
    /// When `None`, overflow events are dropped with a tracing warning.
    retry_log: Option<basin_webhooks::RetryQueue>,
    /// Per-`(project, table)` bounded ring buffer that backs
    /// [`ReplayCursor::drain`] so SSE / WS reconnect-resume actually
    /// delivers events the subscriber missed while disconnected
    /// (closes #54 P0 SSE silent replay loss).
    replay_rings: ReplayRingRegistry,
}

impl RealtimeSink {
    /// Build with [`DEFAULT_CHANNEL_CAPACITY`] and a budget tracker seeded
    /// from the environment ([`BudgetTracker::from_env`]).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// Build with a custom per-channel ring buffer capacity and a budget
    /// tracker seeded from the environment.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            registry: ChannelRegistry::new(capacity),
            budget: BudgetTracker::from_env(),
            retry_log: None,
            replay_rings: ReplayRingRegistry::new(),
        }
    }

    /// Attach a custom [`ReplayRingRegistry`] (useful in tests that need
    /// a smaller per-channel capacity or shorter TTL to exercise eviction
    /// / gap signalling).
    pub fn with_replay_rings(mut self, rings: ReplayRingRegistry) -> Self {
        self.replay_rings = rings;
        self
    }

    /// Borrow the [`ReplayRingRegistry`]. Exposed so SSE / WS handlers and
    /// tests can construct a [`ReplayCursor`] bound to the same rings the
    /// sink writes to.
    pub fn replay_rings(&self) -> &ReplayRingRegistry {
        &self.replay_rings
    }

    /// Attach a durable retry log. Events that overflow a project's budget
    /// are written here instead of being silently dropped.
    pub fn with_retry_log(mut self, retry_log: basin_webhooks::RetryQueue) -> Self {
        self.retry_log = Some(retry_log);
        self
    }

    /// Attach a custom [`BudgetTracker`]. Useful in tests to inject a
    /// tracker with a specific hard cap without touching env vars.
    pub fn with_budget(mut self, budget: BudgetTracker) -> Self {
        self.budget = budget;
        self
    }

    /// Borrow the underlying [`ChannelRegistry`]. R2-R6 use this to hand
    /// receivers to SSE / WebSocket handlers.
    pub fn registry(&self) -> &ChannelRegistry {
        &self.registry
    }

    /// Borrow the [`BudgetTracker`]. Exposed for observability / tests.
    pub fn budget(&self) -> &BudgetTracker {
        &self.budget
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
    /// # Budget enforcement (Phase 5.11.R6)
    ///
    /// Before broadcasting, this method checks the per-project byte budget:
    ///
    /// - **Fast path (budget available):** broadcast the event to all
    ///   subscribers for `(project, table)` in O(1); release the reserved
    ///   bytes via RAII guard.
    /// - **Slow path (`BUFFER_FULL`):** drop the event into the durable retry
    ///   log (if attached) or emit a tracing warning; return `Ok(())` so the
    ///   engine's post-commit path is not blocked.
    ///
    /// Lagged receivers get a [`tokio::sync::broadcast::error::RecvError::Lagged`]
    /// error on their next `recv()` and are responsible for reconnecting via
    /// the replay path ([`ReplayCursor`]).
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        let size = estimate_event_size(event);

        match self.budget.try_reserve(event.project, size) {
            Ok(guard) => {
                let key = ChannelKey::new(event.project, event.table.clone());
                let arc_event = Arc::new(event.clone());
                // Record into the per-channel replay ring *before* broadcasting
                // so a reconnect that races a publish cannot land in the live
                // stream having skipped an event that is also missing from the
                // ring. Both writes are O(1); ordering is enforced by the
                // synchronous-publish single-threaded engine post-commit path.
                self.replay_rings.record(&key, Arc::clone(&arc_event));
                self.registry.publish(&key, arc_event);
                // Hold the reserved bytes in flight until the guard is forgotten.
                // We intentionally do NOT release the budget here: the bytes remain
                // charged until the BudgetTracker itself is reset (or dropped), so
                // the per-project cap acts as a hard session ceiling.  Releasing
                // immediately on every send created a bug (#40 misc) where the
                // counter went back to 0 after each event, making the cap
                // permanently bypassable.  The correct invariant is: once the
                // budget is exhausted for a project, no further events are
                // broadcast for that project in this sink's lifetime.
                std::mem::forget(guard);
            }
            Err(BudgetError::BufferFull) => {
                tracing::warn!(
                    project = %event.project.as_prefix(),
                    table = %event.table,
                    seq = event.seq,
                    "realtime BUFFER_FULL: per-project budget exhausted; dropping to retry log",
                );
                if let Some(retry_log) = &self.retry_log {
                    // Use a stable sentinel subscription-id for the realtime
                    // overflow path (distinct from webhook subscription ids).
                    let sub_id = basin_webhooks::WebhookSubscriptionId(uuid::Uuid::nil());
                    let envelope = basin_webhooks::WebhookEnvelope::from_event(sub_id, event);
                    // Fire-and-forget: enqueue error is non-fatal (post-commit).
                    if let Err(e) = retry_log.enqueue_new(sub_id, event.project, envelope).await {
                        tracing::error!(
                            error = %e,
                            "realtime overflow: retry log enqueue failed",
                        );
                    }
                }
                // Return Ok — post-commit sinks must not block the engine.
                return Ok(());
            }
        }

        Ok(())
    }
}

/// Replay cursor for catch-up after a client reconnect.
///
/// On reconnect a client supplies the last `seq` it successfully received.
/// The cursor consults the per-`(project, table)` [`ReplayRingRegistry`] —
/// a bounded ring buffer the publisher writes into alongside the live
/// broadcast — and replays events with `seq > last_seen` before the
/// transport hands off to the live broadcast receiver.
///
/// # Fixed: #54 P0 — SSE / WS silent replay loss
///
/// Prior to this implementation [`Self::drain`] returned an empty vec and
/// the SSE / WS handlers therefore swallowed every event published during
/// a transient disconnect, without signalling the client. The replay ring
/// closes the silent-loss path; an evicted cursor surfaces as
/// [`DrainOutcome::Gap`] so the client can decide whether to fall back to
/// a cold re-sync from the analytical store.
///
/// # Construction
///
/// Use [`Self::new`] for the legacy "no rings attached" path (always
/// returns an empty replay — preserves the old stub behaviour for callers
/// that have not yet been migrated). Use [`Self::with_rings`] to bind the
/// cursor to the live publisher's ring registry.
pub struct ReplayCursor {
    pub project: ProjectId,
    pub table: TableName,
    /// The last sequence number the client saw. Events with `seq > last_seen`
    /// will be replayed on the next `drain` call.
    pub last_seen: u64,
    /// Optional handle to the publisher-side ring registry. When `None`,
    /// `drain` returns an empty vec (matching the legacy stub).
    rings: Option<ReplayRingRegistry>,
}

impl ReplayCursor {
    /// Build a cursor without a ring handle — `drain` returns empty.
    /// Kept for back-compat with call sites that have not yet been migrated;
    /// new code should prefer [`Self::with_rings`].
    pub fn new(project: ProjectId, table: TableName, last_seen: u64) -> Self {
        Self {
            project,
            table,
            last_seen,
            rings: None,
        }
    }

    /// Build a cursor bound to the live publisher's ring registry.
    /// `drain_outcome` will replay events the publisher recorded with
    /// `seq > last_seen`, or return [`DrainOutcome::Gap`] if the cursor
    /// predates the oldest still-buffered event.
    pub fn with_rings(
        project: ProjectId,
        table: TableName,
        last_seen: u64,
        rings: ReplayRingRegistry,
    ) -> Self {
        Self {
            project,
            table,
            last_seen,
            rings: Some(rings),
        }
    }

    /// Drain catch-up events. Returns an empty vec when no ring is attached
    /// or the cursor has nothing newer to deliver. **Loses the
    /// gap-vs-replay distinction** — call [`Self::drain_outcome`] if the
    /// caller needs to signal a gap to the client.
    pub fn drain(&self) -> Vec<Arc<ChangeEvent>> {
        match &self.rings {
            None => Vec::new(),
            Some(rings) => rings
                .drain_after(self.project, &self.table, self.last_seen)
                .events()
                .to_vec(),
        }
    }

    /// Drain catch-up events and report whether the cursor was within the
    /// ring window or whether events were lost to eviction. Transport
    /// handlers should prefer this form so they can emit a gap signal.
    pub fn drain_outcome(&self) -> DrainOutcome {
        match &self.rings {
            None => DrainOutcome::Replay {
                events: Vec::new(),
                newest_seq: self.last_seen,
            },
            Some(rings) => rings.drain_after(self.project, &self.table, self.last_seen),
        }
    }
}

/// Build the WS axum sub-router for `/realtime/v1/ws/:project`.
///
/// Convenience re-export so callers in `basin-server` don't need to import
/// the inner module:
///
/// ```ignore
/// let ws_router = basin_realtime::ws_router(registry.clone(), auth.clone());
/// app = app.merge(ws_router);
/// ```
pub fn ws_router(registry: ChannelRegistry, auth: Arc<basin_auth::AuthService>) -> axum::Router {
    ws::router(registry, auth)
}

/// Build the WS axum sub-router with an explicit [`ReplayRingRegistry`]
/// (Phase 5.11.R2 reconnect-resume; closes #54 P0 SSE/WS silent replay
/// loss). Callers wired to a [`RealtimeSink`] should pass
/// `sink.replay_rings().clone()` so the WS handler sees the same ring the
/// publisher writes into.
pub fn ws_router_with_rings(
    registry: ChannelRegistry,
    auth: Arc<basin_auth::AuthService>,
    replay_rings: ReplayRingRegistry,
) -> axum::Router {
    ws::router_with_state(
        registry,
        auth,
        presence::PresenceRegistry::default(),
        replay_rings,
    )
}

/// Bind a standalone HTTP server for the WS endpoint on `bind_addr` and
/// return a background [`tokio::task::JoinHandle`].
///
/// Called from `basin-server/src/main.rs` behind `#[cfg(feature = "realtime")]`
/// so `basin-server` doesn't need to import `axum` directly.
pub async fn ws_serve(
    bind_addr: std::net::SocketAddr,
    registry: ChannelRegistry,
    auth: Arc<basin_auth::AuthService>,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    ws::serve(bind_addr, registry, auth).await
}

/// Build the SSE axum sub-router for `/realtime/v1/sse/:project/:table`.
///
/// Convenience re-export so callers in `basin-server` don't need to import
/// the inner module:
///
/// ```ignore
/// let sse_router = basin_realtime::sse_router(registry.clone(), auth.clone());
/// app = app.merge(sse_router);
/// ```
pub fn sse_router(registry: ChannelRegistry, auth: Arc<basin_auth::AuthService>) -> axum::Router {
    sse::router(registry, auth)
}

/// Build the SSE axum sub-router with an explicit [`ReplayRingRegistry`]
/// (Phase 5.11.R2 reconnect-resume; closes #54 P0 SSE silent replay loss).
/// Callers wired to a [`RealtimeSink`] should pass
/// `sink.replay_rings().clone()` so `Last-Event-ID` honours the same ring
/// the publisher writes into.
pub fn sse_router_with_rings(
    registry: ChannelRegistry,
    auth: Arc<basin_auth::AuthService>,
    replay_rings: ReplayRingRegistry,
) -> axum::Router {
    sse::router_with_rings(registry, auth, replay_rings)
}

/// Bind a standalone HTTP server for the SSE endpoint on `bind_addr` and
/// return a background [`tokio::task::JoinHandle`].
///
/// Called from `basin-server/src/main.rs` behind `#[cfg(feature = "realtime")]`
/// so `basin-server` doesn't need to import `axum` directly.
pub async fn sse_serve(
    bind_addr: std::net::SocketAddr,
    registry: ChannelRegistry,
    auth: Arc<basin_auth::AuthService>,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    sse::serve(bind_addr, registry, auth).await
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

    // ---- filter integration tests (Phase 5.11.R5) -------------------------

    fn make_event_with_status(
        project: ProjectId,
        table: &str,
        seq: u64,
        status: &str,
    ) -> ChangeEvent {
        ChangeEvent {
            project,
            table: TableName::new(table).unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": seq, "status": status})),
            committed_at: Utc::now(),
            seq,
            causation_user: None,
        }
    }

    /// Acceptance gate: subscribing to `orders` with predicate
    /// `NEW.status = 'paid'` only delivers events where status is paid.
    #[tokio::test]
    async fn filter_integration_paid_status() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let key = ChannelKey::new(project, TableName::new("orders").unwrap());

        let filter = Filter::new("NEW.status = 'paid'").unwrap();
        let mut rx = sink
            .registry()
            .subscribe_filtered(key.clone(), Some(filter));

        // Publish a non-matching event (pending) then a matching one (paid).
        let pending = make_event_with_status(project, "orders", 1, "pending");
        let paid = make_event_with_status(project, "orders", 2, "paid");

        sink.publish(&pending).await.unwrap();
        sink.publish(&paid).await.unwrap();

        // try_recv: first call returns paid (seq=2), skipping pending (seq=1).
        let got = rx.try_recv().expect("paid event must be available");
        assert_eq!(got.seq, 2, "only the paid event should pass the filter");

        // No more events.
        assert!(rx.try_recv().is_err());
    }

    /// A subscriber with no filter receives all events (unfiltered path).
    #[tokio::test]
    async fn no_filter_receives_all_events() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let key = ChannelKey::new(project, TableName::new("orders").unwrap());

        let mut rx = sink.registry().subscribe_filtered(key.clone(), None);

        sink.publish(&make_event_with_status(project, "orders", 1, "pending"))
            .await
            .unwrap();
        sink.publish(&make_event_with_status(project, "orders", 2, "paid"))
            .await
            .unwrap();

        assert_eq!(rx.try_recv().unwrap().seq, 1);
        assert_eq!(rx.try_recv().unwrap().seq, 2);
    }

    /// Two subscribers on the same channel with different filters each get
    /// only their matching events.
    #[tokio::test]
    async fn two_subscribers_different_filters() {
        let sink = RealtimeSink::new();
        let project = ProjectId::new();
        let key = ChannelKey::new(project, TableName::new("orders").unwrap());

        let f_paid = Filter::new("NEW.status = 'paid'").unwrap();
        let f_pending = Filter::new("NEW.status = 'pending'").unwrap();

        let mut rx_paid = sink
            .registry()
            .subscribe_filtered(key.clone(), Some(f_paid));
        let mut rx_pending = sink
            .registry()
            .subscribe_filtered(key.clone(), Some(f_pending));

        sink.publish(&make_event_with_status(project, "orders", 1, "pending"))
            .await
            .unwrap();
        sink.publish(&make_event_with_status(project, "orders", 2, "paid"))
            .await
            .unwrap();

        // paid subscriber sees seq=2 only
        let p = rx_paid.try_recv().unwrap();
        assert_eq!(p.seq, 2);
        assert!(rx_paid.try_recv().is_err());

        // pending subscriber sees seq=1 only
        let q = rx_pending.try_recv().unwrap();
        assert_eq!(q.seq, 1);
        assert!(rx_pending.try_recv().is_err());
    }
}
