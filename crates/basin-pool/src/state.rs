//! Pool internal state.
//!
//! All shared state lives behind a single `tokio::sync::Mutex<PoolState>`.
//! The mutex is held only across map insertions / removals and the
//! `oneshot::Sender::send(())` calls used to wake waiters; it is **never**
//! held across `engine.open_session(...).await`. That separation is what
//! lets one project open a fresh session without blocking another project's
//! cache hit.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use basin_common::ProjectId;
use basin_engine::ProjectSession;
use tokio::sync::oneshot;

/// Map key. `client_key = None` means "any session for this project"; this
/// is the default when the router doesn't yet know which user is behind a
/// connection (pre-JWT-resolver world per ADR 0007).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    pub(crate) project: ProjectId,
    pub(crate) client_key: Option<String>,
}

/// One idle session sitting in the pool. `last_used` is stamped at the moment
/// the previous holder dropped its `PooledSession`.
pub(crate) struct PooledEntry {
    pub(crate) session: Arc<ProjectSession>,
    pub(crate) last_used: Instant,
}

/// The state behind the outer mutex.
pub(crate) struct PoolState {
    /// Idle sessions, partitioned by `(project, client_key)`. New entries
    /// are pushed at the back so `pop_back()` returns the most-recently
    /// returned session (warm cache); the eviction loop scans from the
    /// front (oldest) and stops at the first non-stale entry.
    pub(crate) available: HashMap<PoolKey, VecDeque<PooledEntry>>,

    /// Total sessions resident per project — incremented when we open a
    /// fresh session and decremented when one is evicted. In-use sessions
    /// count too, so this drives the per-project cap.
    pub(crate) per_project: HashMap<ProjectId, usize>,

    /// `sum(per_project.values())` maintained eagerly so we never have to
    /// walk the map under contention.
    pub(crate) total: usize,

    /// Per-project FIFO of waiters that hit the per-project cap. The first
    /// `Drop`-triggered return for that project pops the front waiter and
    /// sends `()` on its oneshot; the waiter then loops back into
    /// `acquire`.
    pub(crate) waiters: HashMap<ProjectId, VecDeque<oneshot::Sender<()>>>,
}

impl PoolState {
    pub(crate) fn new() -> Self {
        Self {
            available: HashMap::new(),
            per_project: HashMap::new(),
            total: 0,
            waiters: HashMap::new(),
        }
    }

    /// Returns the maximum count across all projects, or 0 if the map is
    /// empty. Used by the public `stats()` accessor.
    pub(crate) fn max_per_project(&self) -> usize {
        self.per_project.values().copied().max().unwrap_or(0)
    }

    /// Push `entry` onto the idle queue for `key` and wake at most one
    /// waiter for that project. Used by `PooledSession::Drop`.
    pub(crate) fn return_entry(&mut self, key: PoolKey, entry: PooledEntry) {
        self.available
            .entry(key.clone())
            .or_default()
            .push_back(entry);

        // Wake one waiter for this project, if any. We don't need to match
        // the `client_key`: a waiter will retry `acquire` and pick up
        // whichever session matches its key (or open a new one — either
        // way it's progress, because we just made a per-project slot
        // available).
        if let Some(queue) = self.waiters.get_mut(&key.project) {
            while let Some(tx) = queue.pop_front() {
                if tx.send(()).is_ok() {
                    break;
                }
                // The waiter dropped its receiver (cancelled / timed out).
                // Try the next one.
            }
            if queue.is_empty() {
                self.waiters.remove(&key.project);
            }
        }
    }

    /// Reserve a per-project slot. Caller must already have observed
    /// `per_project.get(&project) < cap` under the same lock acquisition.
    pub(crate) fn reserve_slot(&mut self, project: ProjectId) {
        *self.per_project.entry(project).or_insert(0) += 1;
        self.total += 1;
    }

    /// Release a per-project slot — used when `engine.open_session` fails
    /// after we already incremented the counters.
    pub(crate) fn release_slot(&mut self, project: ProjectId) {
        if let Some(count) = self.per_project.get_mut(&project) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_project.remove(&project);
            }
        }
        self.total = self.total.saturating_sub(1);
    }

    /// Wake one waiter for `project`, if any.
    ///
    /// Called by transaction-mode `PooledSession::Drop` after releasing the
    /// slot so that a parked `acquire` for the same project can proceed.
    /// In session mode the equivalent wake happens inside `return_entry`.
    pub(crate) fn wake_one_waiter(&mut self, project: ProjectId) {
        if let Some(queue) = self.waiters.get_mut(&project) {
            while let Some(tx) = queue.pop_front() {
                if tx.send(()).is_ok() {
                    break;
                }
            }
            if queue.is_empty() {
                self.waiters.remove(&project);
            }
        }
    }
}
