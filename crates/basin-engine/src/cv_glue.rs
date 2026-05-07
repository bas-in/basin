//! Glue point for `basin-cv`.
//!
//! `basin-cv` is the TimescaleDB-style continuous aggregate layer. It depends
//! on this crate (via `Engine::open_session`); to keep the dependency graph
//! one-directional we cannot pull `basin-cv` into `basin-engine`. What we can
//! do here is reserve the symbol the engine calls during [`crate::Engine::new`]
//! so a future wiring (e.g. registering the
//! `CREATE MATERIALIZED VIEW … WITH (basin.continuous, ...)` and
//! `CALL basin.refresh_continuous_aggregate(...)` interceptors at per-session
//! open time) has an obvious, named landing spot in the source tree.
//!
//! Today's implementation is a deliberate no-op: the SQL surface
//! (`CREATE MATERIALIZED VIEW ... WITH (basin.continuous, ...)`,
//! `CALL basin.refresh_continuous_aggregate(...)`) is exercised in v0.1 by
//! calling `basin_cv::CvStore` / `basin_cv::CvRefresher` directly. When the
//! executor's pre-parse hook learns to recognise the
//! `WITH (basin.continuous, ...)` option-list shape and the
//! `basin.refresh_continuous_aggregate(name, start, end)` schema-qualified
//! procedure-call form, the registration will land here and `Engine::new`
//! will pick it up automatically. The same staging pattern is used by
//! `basin-cron` for `cron.schedule(...)` and `basin-net` for
//! `http_get(...)` / `net.http_post(...)`.
//!
//! See `basin-cv`'s crate docs for the full surface.

/// Hook called once from [`crate::Engine::new`]. No-op today; reserved
/// for the future SQL-surface wiring described in the module doc.
#[inline]
pub(crate) fn install() {
    // Intentionally empty. See the module-level doc for why.
}
