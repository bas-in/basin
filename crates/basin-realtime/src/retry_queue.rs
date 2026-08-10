//! Per-channel bounded replay ring buffer — Phase 5.11.R2 reconnect-resume
//! (closes #54 P0 SSE/WS silent replay loss).
//!
//! # Why this exists
//!
//! Before this module landed, [`crate::ReplayCursor::drain`] returned an
//! empty vec. Clients that honoured `Last-Event-ID` on SSE reconnect
//! believed they had caught up, but had in fact silently missed every
//! event published during the disconnect window — at-least-once was
//! reduced to at-most-once with no signal.
//!
//! The webhook [`basin_webhooks::RetryQueue`] is a per-subscription
//! retry-with-attempts queue keyed by an `IdempotencyKey`; its shape is
//! unsuitable for "give me every event newer than `seq` for `(project, table)`".
//! Reusing it would have meant a re-key, a new index, and an async lock on
//! the SSE hot path. We therefore keep a tiny, in-process, bounded ring
//! buffer per channel that the broadcast publisher writes into alongside
//! the live broadcast — O(1) bytes per event, O(channels × cap) total.
//!
//! # Sizing
//!
//! - Default cap: [`DEFAULT_RING_CAPACITY`] = 1 024 events per channel.
//! - Default TTL: [`DEFAULT_RING_TTL`] = 5 minutes — older events are
//!   evicted on the next write or read regardless of buffer occupancy.
//!
//! With ~1 KB per event that bounds memory at ~1 MiB per active channel,
//! same headroom as the broadcast ring itself. The cap can be tuned per
//! sink via [`ReplayRingRegistry::with_capacity`].
//!
//! # Gap semantics
//!
//! When a reconnect arrives with `Last-Event-ID = N` and the oldest event
//! still in the ring has `seq > N + 1`, the missed events have already
//! been evicted. [`ReplayRingRegistry::drain_after`] surfaces this via the
//! [`DrainOutcome::Gap`] variant so the transport layer can signal the
//! client (SSE emits a `gap` comment frame so the client can fall back to
//! a cold re-sync). This is the difference between "honest at-most-once
//! with a warning" and the silent loss bug we are fixing.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{ChangeEvent, ProjectId, TableName};
use dashmap::DashMap;

use crate::ChannelKey;

/// Default per-channel ring capacity (events).
///
/// 1 024 events at ~1 KB each ≈ 1 MiB per active channel. Mirrors
/// [`crate::DEFAULT_CHANNEL_CAPACITY`] so a subscriber that survives a
/// reconnect within the ring window can always catch up.
pub const DEFAULT_RING_CAPACITY: usize = 1_024;

/// Default per-event TTL inside the ring. Events older than this are
/// evicted on the next write or drain regardless of buffer occupancy.
///
/// 5 minutes is a deliberate "long enough for a transient TCP reconnect,
/// short enough that idle channels do not pin memory". Long disconnects
/// must do a cold re-sync via the analytical read path.
pub const DEFAULT_RING_TTL: Duration = Duration::from_secs(5 * 60);

/// One event slot in the ring.
struct Slot {
    event: Arc<ChangeEvent>,
    /// Wall-clock-monotonic stamp recorded at publish time. Used to evict
    /// events older than the TTL.
    inserted_at: Instant,
}

/// Bounded ring buffer for one `(project, table)` channel.
struct ChannelRing {
    /// `seq`-ordered queue. Invariant: events are appended in strictly
    /// increasing `seq` order (the engine guarantees this via the WAL
    /// counter). Out-of-order writes are dropped with a `tracing::warn`.
    deque: VecDeque<Slot>,
    capacity: usize,
    ttl: Duration,
}

impl ChannelRing {
    fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            deque: VecDeque::with_capacity(capacity.min(1_024)),
            capacity,
            ttl,
        }
    }

    /// Append a freshly-published event. Out-of-order writes (a `seq` that
    /// is not strictly greater than the last stored `seq`) are dropped to
    /// preserve the ordering invariant.
    fn push(&mut self, event: Arc<ChangeEvent>) {
        if let Some(back) = self.deque.back() {
            if event.seq <= back.event.seq {
                tracing::warn!(
                    last_seq = back.event.seq,
                    new_seq = event.seq,
                    "replay ring: out-of-order publish; dropping (engine should emit strictly-increasing seq)",
                );
                return;
            }
        }
        let now = Instant::now();
        self.evict_expired(now);
        while self.deque.len() >= self.capacity {
            self.deque.pop_front();
        }
        self.deque.push_back(Slot {
            event,
            inserted_at: now,
        });
    }

    /// Drop slots older than the TTL. Called on every write and drain.
    fn evict_expired(&mut self, now: Instant) {
        while let Some(front) = self.deque.front() {
            if now.duration_since(front.inserted_at) > self.ttl {
                self.deque.pop_front();
            } else {
                break;
            }
        }
    }

    /// Drain catch-up events with `seq > after_seq`. Returns an outcome that
    /// tells the caller whether the requested cursor is still within the
    /// ring window (`Replay`) or has already been evicted (`Gap`).
    fn drain_after(&mut self, after_seq: u64) -> DrainOutcome {
        let now = Instant::now();
        self.evict_expired(now);

        // Empty ring: nothing to replay. We treat this as "no events seen yet
        // since the cursor was set" — neither a gap nor a replay; the client
        // simply continues with the live stream.
        if self.deque.is_empty() {
            return DrainOutcome::Replay {
                events: Vec::new(),
                newest_seq: after_seq,
            };
        }

        let oldest_seq = self.deque.front().expect("non-empty").event.seq;
        let newest_seq = self.deque.back().expect("non-empty").event.seq;

        // Cursor is caught up or ahead — nothing to replay.
        if after_seq >= newest_seq {
            return DrainOutcome::Replay {
                events: Vec::new(),
                newest_seq,
            };
        }

        // Cursor predates the oldest still-buffered event — we have lost
        // events to ring eviction. Signal a gap so the client can decide
        // whether to cold re-sync. `after_seq == 0` is the "fresh connect
        // with no Last-Event-ID" case and is never a gap.
        if after_seq > 0 && after_seq + 1 < oldest_seq {
            // Drain everything we still have (best-effort) so the client
            // resumes the live stream from `newest_seq` afterwards.
            let events = self.deque.iter().map(|s| Arc::clone(&s.event)).collect();
            return DrainOutcome::Gap {
                events,
                oldest_seq,
                newest_seq,
            };
        }

        let events = self
            .deque
            .iter()
            .filter(|s| s.event.seq > after_seq)
            .map(|s| Arc::clone(&s.event))
            .collect();
        DrainOutcome::Replay { events, newest_seq }
    }
}

/// Outcome of [`ReplayRingRegistry::drain_after`].
#[derive(Debug)]
pub enum DrainOutcome {
    /// The cursor is still within the ring window. `events` are the
    /// catch-up events (possibly empty). `newest_seq` is the most recent
    /// `seq` the ring has seen — clients can advance their cursor to this
    /// value before handing off to the live stream.
    Replay {
        events: Vec<Arc<ChangeEvent>>,
        newest_seq: u64,
    },
    /// The cursor predates the oldest event still in the ring; events
    /// between `after_seq` and `oldest_seq - 1` were evicted and are
    /// permanently lost from this ring (they may still be recoverable from
    /// the cold analytical store).
    ///
    /// The transport layer should signal this to the client (SSE: comment
    /// frame, WS: structured error message) and resume the live stream
    /// from `newest_seq`. `events` contains everything still in the ring
    /// so the client gets best-effort delivery for the trailing window.
    Gap {
        events: Vec<Arc<ChangeEvent>>,
        oldest_seq: u64,
        newest_seq: u64,
    },
}

impl DrainOutcome {
    /// Convenience: the catch-up events regardless of variant.
    pub fn events(&self) -> &[Arc<ChangeEvent>] {
        match self {
            DrainOutcome::Replay { events, .. } => events,
            DrainOutcome::Gap { events, .. } => events,
        }
    }

    /// `true` when the cursor was inside the ring window and no events
    /// were lost.
    pub fn is_replay(&self) -> bool {
        matches!(self, DrainOutcome::Replay { .. })
    }

    /// `true` when events between the cursor and the oldest ring entry
    /// were evicted.
    pub fn is_gap(&self) -> bool {
        matches!(self, DrainOutcome::Gap { .. })
    }

    /// Newest `seq` the ring has seen, suitable for advancing the client's
    /// cursor before handing off to the live stream.
    pub fn newest_seq(&self) -> u64 {
        match self {
            DrainOutcome::Replay { newest_seq, .. } => *newest_seq,
            DrainOutcome::Gap { newest_seq, .. } => *newest_seq,
        }
    }
}

/// Registry of per-`(project, table)` replay rings. Cheap to clone — all
/// state is behind an `Arc`-backed [`DashMap`].
#[derive(Clone)]
pub struct ReplayRingRegistry {
    rings: Arc<DashMap<ChannelKey, parking_lot_mutex::Mutex<ChannelRing>>>,
    capacity: usize,
    ttl: Duration,
}

// Use std Mutex via a tiny shim — the realtime crate already depends on
// dashmap which uses parking_lot internally, but we don't want to add a
// direct parking_lot dep just for one Mutex. std::sync::Mutex is fine:
// the critical section is O(1) for push and O(min(cap, drained)) for
// drain; it never crosses an await boundary.
mod parking_lot_mutex {
    pub use std::sync::Mutex;
}

impl ReplayRingRegistry {
    /// Build with the default per-channel capacity and TTL.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_RING_CAPACITY, DEFAULT_RING_TTL)
    }

    /// Build with a custom per-channel ring capacity and event TTL.
    pub fn with_capacity(capacity: usize, ttl: Duration) -> Self {
        Self {
            rings: Arc::new(DashMap::new()),
            capacity: capacity.max(1),
            ttl,
        }
    }

    /// Record a published event in the ring for `(project, table)`. Safe to
    /// call from the broadcast publish hot path: the critical section is
    /// O(1) and never crosses an `.await`.
    pub fn record(&self, key: &ChannelKey, event: Arc<ChangeEvent>) {
        let entry = self.rings.entry(key.clone()).or_insert_with(|| {
            parking_lot_mutex::Mutex::new(ChannelRing::new(self.capacity, self.ttl))
        });
        // unwrap: the mutex is never poisoned because the critical section
        // does not panic (it only pushes / pops a VecDeque).
        let mut ring = entry.value().lock().expect("replay ring mutex poisoned");
        ring.push(event);
    }

    /// Drain catch-up events newer than `after_seq` for `(project, table)`.
    ///
    /// If no ring exists yet (channel never published an event), returns
    /// [`DrainOutcome::Replay`] with an empty event list.
    pub fn drain_after(
        &self,
        project: ProjectId,
        table: &TableName,
        after_seq: u64,
    ) -> DrainOutcome {
        let key = ChannelKey::new(project, table.clone());
        let Some(entry) = self.rings.get(&key) else {
            return DrainOutcome::Replay {
                events: Vec::new(),
                newest_seq: after_seq,
            };
        };
        let mut ring = entry.value().lock().expect("replay ring mutex poisoned");
        ring.drain_after(after_seq)
    }

    /// Current ring count (visible for diagnostics / tests).
    pub fn channel_count(&self) -> usize {
        self.rings.len()
    }

    /// Drop rings that are both empty and TTL-expired. Intended for a
    /// periodic maintenance task; idempotent and cheap when there is
    /// nothing to evict.
    pub fn prune(&self) {
        self.rings.retain(|_, m| {
            let mut ring = m.lock().expect("replay ring mutex poisoned");
            ring.evict_expired(Instant::now());
            !ring.deque.is_empty()
        });
    }
}

impl Default for ReplayRingRegistry {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn empty_ring_returns_empty_replay() {
        let reg = ReplayRingRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let outcome = reg.drain_after(project, &table, 0);
        assert!(outcome.is_replay());
        assert!(outcome.events().is_empty());
    }

    #[test]
    fn record_then_drain_returns_newer_events_only() {
        let reg = ReplayRingRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        for seq in 1u64..=5 {
            reg.record(&key, Arc::new(make_event(project, "orders", seq)));
        }

        // Cursor at seq=2 → must replay seq=3,4,5.
        let outcome = reg.drain_after(project, &table, 2);
        assert!(outcome.is_replay(), "cursor inside ring: not a gap");
        let seqs: Vec<u64> = outcome.events().iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
        assert_eq!(outcome.newest_seq(), 5);
    }

    #[test]
    fn cursor_caught_up_returns_empty_replay() {
        let reg = ReplayRingRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        for seq in 1u64..=3 {
            reg.record(&key, Arc::new(make_event(project, "orders", seq)));
        }

        let outcome = reg.drain_after(project, &table, 3);
        assert!(outcome.is_replay());
        assert!(outcome.events().is_empty());
        assert_eq!(outcome.newest_seq(), 3);
    }

    #[test]
    fn out_of_order_publish_is_dropped() {
        let reg = ReplayRingRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        reg.record(&key, Arc::new(make_event(project, "orders", 5)));
        // Out-of-order — must be dropped.
        reg.record(&key, Arc::new(make_event(project, "orders", 3)));
        reg.record(&key, Arc::new(make_event(project, "orders", 7)));

        let outcome = reg.drain_after(project, &table, 0);
        let seqs: Vec<u64> = outcome.events().iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![5, 7], "out-of-order seq=3 must be dropped");
    }

    #[test]
    fn capacity_evicts_oldest() {
        let reg = ReplayRingRegistry::with_capacity(3, DEFAULT_RING_TTL);
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        for seq in 1u64..=5 {
            reg.record(&key, Arc::new(make_event(project, "orders", seq)));
        }

        // Capacity 3 → only seq 3, 4, 5 survive.
        let outcome = reg.drain_after(project, &table, 0);
        let seqs: Vec<u64> = outcome.events().iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
    }

    #[test]
    fn gap_returned_when_cursor_precedes_oldest() {
        let reg = ReplayRingRegistry::with_capacity(3, DEFAULT_RING_TTL);
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        // Publish seq=1..=5; capacity 3 → ring now holds seq 3,4,5; seq 2 was evicted.
        for seq in 1u64..=5 {
            reg.record(&key, Arc::new(make_event(project, "orders", seq)));
        }

        // Client says "I last saw seq=1". The next event it expects is 2,
        // but the oldest in the ring is 3 → gap.
        let outcome = reg.drain_after(project, &table, 1);
        match outcome {
            DrainOutcome::Gap {
                ref events,
                oldest_seq,
                newest_seq,
            } => {
                assert_eq!(oldest_seq, 3);
                assert_eq!(newest_seq, 5);
                let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
                assert_eq!(seqs, vec![3, 4, 5], "best-effort: still emit what we have");
            }
            other => panic!("expected Gap, got {:?}", other),
        }
    }

    #[test]
    fn fresh_connect_zero_cursor_is_never_a_gap() {
        let reg = ReplayRingRegistry::with_capacity(3, DEFAULT_RING_TTL);
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        // Pre-fill so the ring's oldest is far above 0.
        for seq in 100u64..=110 {
            reg.record(&key, Arc::new(make_event(project, "orders", seq)));
        }

        // Cursor 0 = "fresh client; just hand me the live stream and any
        // recent context". This must not be a Gap (the client never claimed
        // to have seen seq 1..99).
        let outcome = reg.drain_after(project, &table, 0);
        assert!(outcome.is_replay(), "cursor=0 must be Replay, not Gap");
    }

    #[test]
    fn ttl_evicts_old_events() {
        let reg = ReplayRingRegistry::with_capacity(1_024, Duration::from_millis(20));
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        reg.record(&key, Arc::new(make_event(project, "orders", 1)));
        std::thread::sleep(Duration::from_millis(40));
        reg.record(&key, Arc::new(make_event(project, "orders", 2)));

        let outcome = reg.drain_after(project, &table, 0);
        let seqs: Vec<u64> = outcome.events().iter().map(|e| e.seq).collect();
        // seq=1 should have been evicted by TTL before seq=2 was pushed.
        assert_eq!(seqs, vec![2], "TTL must evict events older than the window");
    }

    #[test]
    fn different_channels_isolated() {
        let reg = ReplayRingRegistry::new();
        let project = ProjectId::new();
        let orders = TableName::new("orders").unwrap();
        let users = TableName::new("users").unwrap();
        let k_orders = ChannelKey::new(project, orders.clone());
        let k_users = ChannelKey::new(project, users.clone());

        reg.record(&k_orders, Arc::new(make_event(project, "orders", 1)));
        reg.record(&k_users, Arc::new(make_event(project, "users", 2)));

        let o = reg.drain_after(project, &orders, 0);
        let u = reg.drain_after(project, &users, 0);
        assert_eq!(o.events().len(), 1);
        assert_eq!(o.events()[0].seq, 1);
        assert_eq!(u.events().len(), 1);
        assert_eq!(u.events()[0].seq, 2);
    }

    #[test]
    fn prune_removes_empty_rings() {
        let reg = ReplayRingRegistry::with_capacity(8, Duration::from_millis(10));
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let key = ChannelKey::new(project, table.clone());

        reg.record(&key, Arc::new(make_event(project, "orders", 1)));
        assert_eq!(reg.channel_count(), 1);

        std::thread::sleep(Duration::from_millis(30));
        reg.prune();
        assert_eq!(reg.channel_count(), 0, "expired empty ring must be pruned");
    }
}
