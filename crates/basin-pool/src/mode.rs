//! Pool-mode enum: controls how a [`SessionPool`] assigns physical connections
//! to logical clients.
//!
//! ## Session mode (classic)
//!
//! One physical connection is bound to one logical client for the entire
//! lifetime of that client's connection.  The pool acts as a fast session
//! cache: cold opens are amortised over many requests.  Per-session state
//! (prepared statements, cursors, `SET` variables) persists across
//! successive calls from the same client.
//!
//! ## Transaction mode (5.27.B)
//!
//! A physical connection is assigned to a logical client only for the
//! duration of one `BEGIN … COMMIT/ROLLBACK` cycle.  On return the session
//! is **destroyed** (slot released, `Arc<ProjectSession>` dropped) rather
//! than cached.  The next checkout for the same project always opens a fresh
//! session, guaranteeing that cursors, prepared statements, and per-session
//! state from the previous logical client cannot bleed into the next one.
//!
//! The pool still enforces `max_sessions` / `per_project_cap` — these
//! count *concurrent in-use* sessions, not cached ones.  Waiters are woken
//! as usual when a slot is released.
//!
//! ## Statement mode (future)
//!
//! Placeholder for pgbouncer-compatible statement-level multiplexing.
//! Behaviour is identical to `Transaction` in this implementation.

/// Determines how the pool assigns physical sessions to logical clients.
///
/// See the module-level doc for the full contract of each variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PoolMode {
    /// Classic session-per-client caching.  Sessions are returned to the idle
    /// queue on checkout completion and reused by the next client with the
    /// same `(project, client_key)`.
    Session,

    /// Transaction-scoped assignment.  Each checkout opens a **fresh**
    /// session (or waits for a slot to become available) and the session is
    /// dropped — not cached — when the checkout is returned.  This prevents
    /// any per-session state (cursors, prepared statements, `SET LOCAL`)
    /// from leaking across logical-client boundaries.
    ///
    /// This is the **default** pool mode.
    #[default]
    Transaction,

    /// Statement-scoped assignment (placeholder; behaves like `Transaction`
    /// in the current implementation).
    Statement,
}

impl PoolMode {
    /// Returns `true` if this mode requires the session to be destroyed
    /// (not cached) on checkout return.
    pub fn destroys_on_return(self) -> bool {
        matches!(self, PoolMode::Transaction | PoolMode::Statement)
    }
}
