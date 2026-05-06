//! Glue point for `basin-cron`.
//!
//! `basin-cron` is the pg_cron-compatible scheduler. It depends on this
//! crate (via `Engine::open_session`); to keep the dependency graph
//! one-directional we cannot pull `basin-cron` into `basin-engine`. What
//! we can do here is reserve the symbol the engine calls during
//! [`Engine::new`] so a future wiring (e.g. registering the
//! `cron.schedule` / `cron.unschedule` scalar UDFs at per-session open
//! time) has an obvious, named landing spot in the source tree.
//!
//! Today's implementation is a deliberate no-op: the SQL surface
//! (`SELECT cron.schedule(...)` / `SELECT cron.unschedule(...)`) is
//! exercised in v0.1 by calling `basin_cron::CronStore` directly. When
//! the executor's parser-level hook learns to recognise the `cron.`
//! schema-qualified function call form, the registration will go here
//! and `Engine::new` will pick it up automatically.
//!
//! See `basin-cron`'s crate docs for the full surface.

/// Hook called once from [`crate::Engine::new`]. No-op today; reserved
/// for the future SQL-surface wiring described in the module doc.
#[inline]
pub(crate) fn install() {
    // Intentionally empty. See the module-level doc for why.
}
