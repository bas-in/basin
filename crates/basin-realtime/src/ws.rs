//! WebSocket transport layer — stub for Phase 5.11.R3.
//!
//! # TODO(R3)
//!
//! - Expose an `axum` WebSocket upgrade handler at `GET /realtime/ws`.
//! - After upgrade, read the initial JSON subscription message:
//!   `{"table": "<name>", "last_seq": <u64>}`.
//! - Subscribe to the matching [`ChannelKey`](crate::ChannelKey) via
//!   [`ChannelRegistry::subscribe`](crate::ChannelRegistry::subscribe).
//! - Drain catch-up events via [`ReplayCursor`](crate::ReplayCursor) for
//!   `seq > last_seq`.
//! - Stream events as JSON text frames. Handle `Ping`/`Pong` keepalives.
//! - On lag error: send `{"type":"lag","last_seq":<n>}` to prompt client
//!   reconnect with `last_seq` set correctly.
//! - On client `{"type":"unsubscribe"}` message: remove the receiver cleanly.
//!
//! # Wire-up (R3)
//!
//! ```ignore
//! // In services/basin-server/src/main.rs behind #[cfg(feature = "realtime")]:
//! let ws_router = basin_realtime::ws::router(realtime_sink.registry().clone(), auth.clone());
//! app = app.merge(ws_router);
//! ```
