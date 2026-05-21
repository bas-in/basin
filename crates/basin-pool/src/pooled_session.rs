//! [`PooledSession`] — RAII handle that returns its session to the pool
//! when dropped.

use std::sync::Arc;
use std::time::Instant;

use basin_common::ProjectId;
use basin_engine::ProjectSession;

use crate::state::{PoolKey, PooledEntry};
use crate::Inner;

/// A leased session. Dropping the handle returns the underlying
/// [`ProjectSession`] to the pool's idle queue.
///
/// The session reference is exposed via [`PooledSession::session`] rather
/// than by `Deref`. We deliberately do not hand out ownership: the pool
/// must see every release.
pub struct PooledSession {
    pub(crate) entry: Option<PooledEntry>,
    pub(crate) key: PoolKey,
    pub(crate) pool: Arc<Inner>,
}

impl PooledSession {
    /// The underlying engine session. Use this exactly the way the router
    /// would use the value returned by `Engine::open_session`.
    pub fn session(&self) -> &ProjectSession {
        &self
            .entry
            .as_ref()
            .expect("PooledSession used after drop")
            .session
    }

    /// The project this session is bound to. Convenience accessor that
    /// matches `ProjectSession::project` so callers don't need to deref.
    pub fn project(&self) -> ProjectId {
        self.key.project
    }
}

impl Drop for PooledSession {
    fn drop(&mut self) {
        // `entry` is `None` only if `drop` ran twice, which Rust prevents.
        let entry = match self.entry.take() {
            Some(e) => e,
            None => return,
        };
        let pool = self.pool.clone();
        let key = self.key.clone();

        // Transaction / statement mode: destroy the session rather than
        // returning it to the idle cache.  Dropping `entry` here releases
        // the `Arc<ProjectSession>` (and thus all cursor / prepared-statement
        // state held by that session), then we release the capacity slot so
        // the next waiter can open a fresh session.  This is the load-bearing
        // isolation invariant for transaction-mode: no per-session state
        // (cursors, SET variables, prepared statements) can cross a checkout
        // boundary because the physical session is destroyed at checkout end.
        if pool.cfg.pool_mode.destroys_on_return() {
            // Drop the session Arc here, outside the lock, so its
            // destructors (cursor registry, advisory locks, etc.) run
            // before we re-acquire the pool mutex.
            drop(entry);
            // Release the slot under the lock and wake one waiter.
            // Bind the try_lock result to an owned variable so we don't
            // borrow `pool` across the spawn boundary below.
            let released = pool.state.try_lock().ok().map(|mut state| {
                state.release_slot(key.project);
                state.wake_one_waiter(key.project);
            });
            if released.is_none() {
                tokio::spawn(async move {
                    let mut state = pool.state.lock().await;
                    state.release_slot(key.project);
                    state.wake_one_waiter(key.project);
                });
            }
            return;
        }

        // Session mode: return the session to the idle cache as before.
        let mut entry = entry;
        entry.last_used = Instant::now();

        // Fast path: no contention on the outer mutex.
        if let Ok(mut state) = pool.state.try_lock() {
            state.return_entry(key, entry);
            return;
        }

        // Slow path: someone else holds the lock right now (almost always
        // another `acquire` in the same runtime). Hand the return off to
        // a fresh task so `Drop` itself stays sync.
        tokio::spawn(async move {
            let mut state = pool.state.lock().await;
            state.return_entry(key, entry);
        });
    }
}

/// Wrap a freshly-acquired entry plus its key in a `PooledSession`. Kept in
/// this module so the field-visibility surface stays small.
pub(crate) fn build(pool: Arc<Inner>, key: PoolKey, session: Arc<ProjectSession>) -> PooledSession {
    PooledSession {
        entry: Some(PooledEntry {
            session,
            last_used: Instant::now(),
        }),
        key,
        pool,
    }
}
