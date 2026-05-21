//! Background idle-in-transaction session reaper (Phase 5.28.C).
//!
//! ## Problem
//!
//! A client that issues `BEGIN` and then stops sending queries (network drop,
//! crash, slow application) leaves an open transaction indefinitely, holding
//! any in-memory resources (pending files, HTAP buffers) and potentially
//! blocking dependent operations.  PostgreSQL's
//! `idle_in_transaction_session_timeout` GUC solves this by terminating
//! connections that have been idle *inside* a transaction for longer than the
//! configured limit.
//!
//! ## Design
//!
//! The reaper is a lightweight background task:
//!
//! 1. Every `check_interval` (default 1 s) it iterates over a shared registry
//!    of in-flight sessions.
//! 2. For each session it checks:
//!    - Is a transaction currently open? (`tx_is_active`)
//!    - Has the session been idle for longer than the configured timeout?
//!      (compares `last_active` against `Instant::now()`)
//! 3. If both conditions hold, the reaper flags the session as **reaped** via
//!    an `AtomicBool`.  On the next `execute()` call the executor reads the
//!    flag and returns `BasinError::IdleInTransactionTimeout` before doing any
//!    work.
//!
//! ## Registry
//!
//! [`SessionReaperRegistry`] is a cheaply-cloneable `Arc`-wrapped map from
//! session token → weak reference to `SessionState`.  Sessions register
//! themselves on creation (via [`SessionReaperRegistry::register`]) and the
//! weak ref is automatically cleaned up when the `SessionState` is dropped
//! (the `Weak::upgrade` fails and the entry is pruned on the next sweep).
//!
//! ## Per-session vs process-wide timeout
//!
//! The reaper respects the **per-session** GUC value stored in
//! `SessionState::idle_in_transaction_session_timeout`.  If the session has not
//! configured a timeout (`None`), it is skipped.  This matches PostgreSQL's
//! behaviour: the parameter is settable per-session and defaults to 0 (off).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::session::{
    last_active, session_idle_in_transaction_timeout, tx_is_active, SessionState,
};

/// Per-session flag set by the reaper when the idle-in-txn timeout fires.
/// The executor reads this flag at the top of every `execute()` and returns
/// `BasinError::IdleInTransactionTimeout` when it is set.
#[derive(Debug, Default)]
pub(crate) struct ReapedFlag(pub(crate) AtomicBool);

impl ReapedFlag {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self(AtomicBool::new(false)))
    }

    pub(crate) fn is_reaped(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn set_reaped(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Entry in the registry: weak reference to the session state + the reap flag.
struct RegistryEntry {
    state: Weak<SessionState>,
    reaped: Arc<ReapedFlag>,
}

/// Process-wide registry of live sessions for idle-in-txn reaping.
///
/// Cheap to clone (Arc inside). The reaper background task holds one clone;
/// each [`crate::ProjectSession`] holds another via its `reaped_flag` field
/// and the registry entry.
#[derive(Clone, Default)]
pub struct SessionReaperRegistry {
    inner: Arc<Mutex<HashMap<u64, RegistryEntry>>>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl SessionReaperRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a session. Returns `(session_id, Arc<ReapedFlag>)`.  Drop the
    /// returned `ReapedFlag` to signal "session closed" — the weak ref in the
    /// registry will fail to upgrade on the next sweep and the entry will be
    /// pruned automatically.
    pub fn register(&self, state: Arc<SessionState>) -> (u64, Arc<ReapedFlag>) {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let reaped = ReapedFlag::new();
        let entry = RegistryEntry {
            state: Arc::downgrade(&state),
            reaped: reaped.clone(),
        };
        self.inner.lock().expect("registry lock poisoned").insert(id, entry);
        (id, reaped)
    }

    /// Remove a session from the registry (called when the session is dropped).
    pub fn deregister(&self, id: u64) {
        self.inner.lock().expect("registry lock poisoned").remove(&id);
    }

    /// Run one reaper sweep. For every registered session whose state is still
    /// alive, check whether it is idle-in-transaction past its configured
    /// timeout. Expired sessions are flagged; dead entries are pruned.
    pub fn sweep(&self) {
        let now = std::time::Instant::now();
        let mut dead = Vec::new();

        {
            let map = self.inner.lock().expect("registry lock poisoned");
            for (&id, entry) in map.iter() {
                let Some(state) = entry.state.upgrade() else {
                    dead.push(id);
                    continue;
                };
                // Only act on sessions with an open transaction.
                if !tx_is_active(&state) {
                    continue;
                }
                // Check per-session timeout.
                let timeout = match session_idle_in_transaction_timeout(&state) {
                    Some(t) => t,
                    None => continue, // disabled for this session
                };
                let idle = now.saturating_duration_since(last_active(&state));
                if idle >= timeout {
                    entry.reaped.set_reaped();
                }
            }
        }

        // Prune dead entries.
        if !dead.is_empty() {
            let mut map = self.inner.lock().expect("registry lock poisoned");
            for id in dead {
                map.remove(&id);
            }
        }
    }

    /// Spawn the background reaper loop. Returns a handle — drop it to stop
    /// the loop. Typically called once at engine startup.
    ///
    /// `check_interval`: how often to sweep (e.g. 1 s in production,
    /// 10 ms in tests).
    pub(crate) fn spawn(self, check_interval: Duration) -> ReaperHandle {
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(check_interval) => {
                        self.sweep();
                    }
                    _ = &mut rx => {
                        break;
                    }
                }
            }
        });
        ReaperHandle {
            shutdown: tx,
            join: handle,
        }
    }
}

/// Returned from [`SessionReaperRegistry::spawn`]; drop to stop the loop.
pub(crate) struct ReaperHandle {
    shutdown: tokio::sync::oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

impl ReaperHandle {
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.join.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{
        set_session_idle_in_transaction_timeout, touch_last_active, tx_begin,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_state() -> Arc<SessionState> {
        Arc::new(SessionState::new())
    }

    /// A session with no open transaction is never reaped.
    #[test]
    fn no_txn_not_reaped() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_millis(10)));
        let (_id, flag) = registry.register(state.clone());
        std::thread::sleep(Duration::from_millis(30));
        registry.sweep();
        assert!(!flag.is_reaped(), "no-txn session must not be reaped");
    }

    /// A session in a txn that is still active (recent touch) is not reaped.
    #[test]
    fn active_txn_not_reaped() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_millis(200)));
        tx_begin(&state, HashMap::new());
        touch_last_active(&state);
        let (_id, flag) = registry.register(state.clone());
        registry.sweep(); // immediate sweep — not yet expired
        assert!(!flag.is_reaped(), "recently-touched txn must not be reaped");
    }

    /// A session in a txn that has been idle past the timeout is reaped.
    #[test]
    fn idle_txn_past_timeout_is_reaped() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_millis(20)));
        tx_begin(&state, HashMap::new());
        // Simulate idle: last_active is already old (set at SessionState::new)
        let (_id, flag) = registry.register(state.clone());
        std::thread::sleep(Duration::from_millis(40));
        registry.sweep();
        assert!(flag.is_reaped(), "idle-in-txn past timeout must be reaped");
    }

    /// A session with timeout disabled (None) is never reaped even if idle.
    #[test]
    fn no_timeout_never_reaped() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        // Don't set timeout — it's None by default.
        tx_begin(&state, HashMap::new());
        let (_id, flag) = registry.register(state.clone());
        std::thread::sleep(Duration::from_millis(50));
        registry.sweep();
        assert!(
            !flag.is_reaped(),
            "session with disabled timeout must not be reaped"
        );
    }

    /// After a session is dropped the entry is pruned on the next sweep.
    #[test]
    fn dead_session_pruned() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_millis(10)));
        tx_begin(&state, HashMap::new());
        let (id, _flag) = registry.register(state.clone());
        drop(state); // session "closes"
        std::thread::sleep(Duration::from_millis(20));
        registry.sweep(); // prune the dead entry
        let map = registry.inner.lock().unwrap();
        assert!(!map.contains_key(&id), "dead entry must be pruned");
    }

    /// `spawn` + `shutdown` round-trip.
    #[tokio::test]
    async fn spawn_and_shutdown() {
        let registry = SessionReaperRegistry::new();
        let handle = registry.spawn(Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.shutdown().await;
        // No assertion needed — just verify it doesn't hang or panic.
    }

    /// The reaper actually fires and marks a session via the spawned loop.
    #[tokio::test]
    async fn spawned_reaper_fires() {
        let registry = SessionReaperRegistry::new();
        let state = make_state();
        set_session_idle_in_transaction_timeout(&state, Some(Duration::from_millis(30)));
        tx_begin(&state, HashMap::new());
        let (_id, flag) = registry.register(state.clone());

        // Spawn with a short check interval so the test finishes quickly.
        let handle = registry.spawn(Duration::from_millis(10));

        // Wait long enough for the reaper to fire.
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.shutdown().await;

        assert!(flag.is_reaped(), "spawned reaper must have fired");
    }
}
