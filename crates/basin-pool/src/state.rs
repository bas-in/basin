//! Pool internal state.
//!
//! All shared state lives behind a single `tokio::sync::Mutex<PoolState>`.
//! The mutex is held only across map insertions / removals and the
//! `oneshot::Sender::send(())` calls used to wake waiters; it is **never**
//! held across `engine.open_session(...).await`. That separation is what
//! lets one tenant open a fresh session without blocking another tenant's
//! cache hit.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use basin_common::TenantId;
use basin_engine::TenantSession;
use tokio::sync::oneshot;

/// Map key. `client_key = None` means "any session for this tenant"; this
/// is the default when the router doesn't yet know which user is behind a
/// connection (pre-JWT-resolver world per ADR 0007).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    pub(crate) tenant: TenantId,
    pub(crate) client_key: Option<String>,
}

/// One idle session sitting in the pool. `last_used` is stamped at the moment
/// the previous holder dropped its `PooledSession`.
pub(crate) struct PooledEntry {
    pub(crate) session: Arc<TenantSession>,
    pub(crate) last_used: Instant,
}

/// The state behind the outer mutex.
pub(crate) struct PoolState {
    /// Idle sessions, partitioned by `(tenant, client_key)`. New entries
    /// are pushed at the back so `pop_back()` returns the most-recently
    /// returned session (warm cache); the eviction loop scans from the
    /// front (oldest) and stops at the first non-stale entry.
    pub(crate) available: HashMap<PoolKey, VecDeque<PooledEntry>>,

    /// Total sessions resident per tenant — incremented when we open a
    /// fresh session and decremented when one is evicted. In-use sessions
    /// count too, so this drives the per-tenant cap.
    pub(crate) per_tenant: HashMap<TenantId, usize>,

    /// `sum(per_tenant.values())` maintained eagerly so we never have to
    /// walk the map under contention.
    pub(crate) total: usize,

    /// Per-tenant FIFO of waiters that hit the per-tenant cap. The first
    /// `Drop`-triggered return for that tenant pops the front waiter and
    /// sends `()` on its oneshot; the waiter then loops back into
    /// `acquire`.
    pub(crate) waiters: HashMap<TenantId, VecDeque<oneshot::Sender<()>>>,
}

impl PoolState {
    pub(crate) fn new() -> Self {
        Self {
            available: HashMap::new(),
            per_tenant: HashMap::new(),
            total: 0,
            waiters: HashMap::new(),
        }
    }

    /// Returns the maximum count across all tenants, or 0 if the map is
    /// empty. Used by the public `stats()` accessor.
    pub(crate) fn max_per_tenant(&self) -> usize {
        self.per_tenant.values().copied().max().unwrap_or(0)
    }

    /// Push `entry` onto the idle queue for `key` and wake at most one
    /// waiter for that tenant. Used by `PooledSession::Drop`.
    pub(crate) fn return_entry(&mut self, key: PoolKey, entry: PooledEntry) {
        self.available.entry(key.clone()).or_default().push_back(entry);

        // Wake one waiter for this tenant, if any. We don't need to match
        // the `client_key`: a waiter will retry `acquire` and pick up
        // whichever session matches its key (or open a new one — either
        // way it's progress, because we just made a per-tenant slot
        // available).
        if let Some(queue) = self.waiters.get_mut(&key.tenant) {
            while let Some(tx) = queue.pop_front() {
                if tx.send(()).is_ok() {
                    break;
                }
                // The waiter dropped its receiver (cancelled / timed out).
                // Try the next one.
            }
            if queue.is_empty() {
                self.waiters.remove(&key.tenant);
            }
        }
    }

    /// Reserve a per-tenant slot. Caller must already have observed
    /// `per_tenant.get(&tenant) < cap` under the same lock acquisition.
    pub(crate) fn reserve_slot(&mut self, tenant: TenantId) {
        *self.per_tenant.entry(tenant).or_insert(0) += 1;
        self.total += 1;
    }

    /// Release a per-tenant slot — used when `engine.open_session` fails
    /// after we already incremented the counters.
    pub(crate) fn release_slot(&mut self, tenant: TenantId) {
        if let Some(count) = self.per_tenant.get_mut(&tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_tenant.remove(&tenant);
            }
        }
        self.total = self.total.saturating_sub(1);
    }
}
