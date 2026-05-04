//! Idle-session eviction loop.
//!
//! Same shape as `basin-shard`'s eviction tick: a `select!` between a
//! shutdown oneshot and a periodic sleep, with the actual scan factored
//! into [`run_once`] so tests can drive it deterministically.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, instrument};

use crate::Inner;

/// Handle returned by [`SessionPool::spawn_eviction`](crate::SessionPool::spawn_eviction).
/// Drop it (or call [`shutdown`](Self::shutdown)) to stop the loop.
///
/// On `Drop` of the handle, the oneshot sender is dropped — the background
/// task observes the closed channel and exits, but we don't await its
/// `JoinHandle` from `Drop`. Callers who want a clean wait should call
/// [`shutdown`](Self::shutdown) explicitly.
pub struct EvictionHandle {
    pub(crate) shutdown: Option<oneshot::Sender<()>>,
    pub(crate) join: Option<JoinHandle<()>>,
}

impl EvictionHandle {
    /// Stop the eviction loop and wait for it to exit. Idempotent only in
    /// the sense that calling twice is a no-op the second time around;
    /// it consumes `self`.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            // Signal the loop to exit. Ignore send errors — the receiver
            // having been dropped means the loop is already gone.
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for EvictionHandle {
    fn drop(&mut self) {
        // Best-effort signal so the task exits even if the user drops the
        // handle without awaiting `shutdown()`. We don't await the join
        // because `Drop` is sync.
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn the periodic eviction task. Returns immediately.
pub(crate) fn spawn(pool: Arc<Inner>) -> EvictionHandle {
    let (tx, rx) = oneshot::channel::<()>();
    let interval = pool.cfg.eviction_interval;
    let join = tokio::spawn(async move {
        let mut shutdown = rx;
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(interval) => {}
            }
            run_once(&pool).await;
        }
    });
    EvictionHandle {
        shutdown: Some(tx),
        join: Some(join),
    }
}

/// Walk the idle queues and drop sessions older than `idle_ttl`. Visible
/// to tests via `pub(crate)` so they can drive evictions synchronously
/// without waiting on a sleep.
#[instrument(skip(pool))]
pub(crate) async fn run_once(pool: &Arc<Inner>) {
    let now = Instant::now();
    let idle_ttl = pool.cfg.idle_ttl;

    let mut state = pool.state.lock().await;
    let mut to_drop_per_tenant: Vec<basin_common::TenantId> = Vec::new();
    let mut evicted_total: u64 = 0;

    for (key, queue) in state.available.iter_mut() {
        while let Some(front) = queue.front() {
            if now.duration_since(front.last_used) >= idle_ttl {
                queue.pop_front();
                evicted_total += 1;
                to_drop_per_tenant.push(key.tenant);
            } else {
                // Queues are MRU-at-back / LRU-at-front; once we hit a
                // non-stale entry the rest are even fresher.
                break;
            }
        }
    }
    state.available.retain(|_, q| !q.is_empty());

    for tenant in to_drop_per_tenant {
        if let Some(count) = state.per_tenant.get_mut(&tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_tenant.remove(&tenant);
            }
        }
        state.total = state.total.saturating_sub(1);
    }

    if evicted_total > 0 {
        for _ in 0..evicted_total {
            pool.stats.record_eviction();
        }
        debug!(evicted = evicted_total, "eviction tick dropped idle sessions");
    }
}
