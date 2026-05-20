//! Server-Sent Events (SSE) transport layer — stub for Phase 5.11.R2.
//!
//! # TODO(R2)
//!
//! - Expose an `axum` route handler that accepts a `GET /realtime/sse?table=<name>`
//!   request (authenticated via basin-auth JWT).
//! - Subscribe to the matching [`ChannelKey`](crate::ChannelKey) via
//!   [`ChannelRegistry::subscribe`](crate::ChannelRegistry::subscribe).
//! - Before entering the live stream, call [`ReplayCursor::drain`](crate::ReplayCursor::drain)
//!   to replay missed events for `seq > X-Last-Seq` (client header).
//! - Stream each [`ChangeEvent`](basin_common::ChangeEvent) as an SSE `data:` line
//!   (newline-delimited JSON) with `id: <seq>` for client-side cursor tracking.
//! - On disconnect or lag error, close the response body gracefully; the
//!   client reconnects with `Last-Event-ID` to trigger catch-up from the
//!   webhook retry log.
//!
//! # Wire-up (R2)
//!
//! ```ignore
//! // In services/basin-server/src/main.rs behind #[cfg(feature = "realtime")]:
//! let sse_router = basin_realtime::sse::router(realtime_sink.registry().clone(), auth.clone());
//! app = app.merge(sse_router);
//! ```
