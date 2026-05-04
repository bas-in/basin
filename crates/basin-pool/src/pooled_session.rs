//! [`PooledSession`] — RAII handle that returns its session to the pool
//! when dropped.

use std::sync::Arc;
use std::time::Instant;

use basin_common::TenantId;
use basin_engine::TenantSession;

use crate::state::{PoolKey, PooledEntry};
use crate::Inner;

/// A leased session. Dropping the handle returns the underlying
/// [`TenantSession`] to the pool's idle queue.
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
    pub fn session(&self) -> &TenantSession {
        &self
            .entry
            .as_ref()
            .expect("PooledSession used after drop")
            .session
    }

    /// The tenant this session is bound to. Convenience accessor that
    /// matches `TenantSession::tenant` so callers don't need to deref.
    pub fn tenant(&self) -> TenantId {
        self.key.tenant
    }
}

impl Drop for PooledSession {
    fn drop(&mut self) {
        // `entry` is `None` only if `drop` ran twice, which Rust prevents.
        let mut entry = match self.entry.take() {
            Some(e) => e,
            None => return,
        };
        entry.last_used = Instant::now();
        let pool = self.pool.clone();
        let key = self.key.clone();

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
pub(crate) fn build(pool: Arc<Inner>, key: PoolKey, session: Arc<TenantSession>) -> PooledSession {
    PooledSession {
        entry: Some(PooledEntry {
            session,
            last_used: Instant::now(),
        }),
        key,
        pool,
    }
}
