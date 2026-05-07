//! Glue point for `basin-net`.
//!
//! `basin-net` is the `http`-extension + `pg_net`-compatible HTTP layer. It
//! depends on this crate (via `Engine::open_session`); to keep the
//! dependency graph one-directional we cannot pull `basin-net` into
//! `basin-engine`. What we can do here is reserve the symbol the engine
//! calls during [`crate::Engine::new`] so a future wiring (e.g. registering
//! the `http_get(url)` table function and the `net.http_post(...)` scalar
//! UDF at per-session open time) has an obvious, named landing spot in the
//! source tree.
//!
//! Today's implementation is a deliberate no-op: the SQL surface
//! (`SELECT * FROM http_get(...)`, `SELECT net.http_post(...)`) is exercised
//! in v0.1 by calling `basin_net::HttpClient` / `basin_net::RequestQueue` /
//! `basin_net::ResponseStore` directly. When the executor's pre-parse hook
//! learns to recognise the `http_get` / `http_post` table-function shape
//! and the `net.http_post` schema-qualified call form, the registration
//! will go here and `Engine::new` will pick it up automatically.
//!
//! See `basin-net`'s crate docs for the full surface.

/// Hook called once from [`crate::Engine::new`]. No-op today; reserved
/// for the future SQL-surface wiring described in the module doc.
#[inline]
pub(crate) fn install() {
    // Intentionally empty. See the module-level doc for why.
}
