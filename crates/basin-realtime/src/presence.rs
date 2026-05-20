//! Presence tracking — stub for Phase 5.11.R4.
//!
//! Presence tracks which clients are currently subscribed to which
//! `(project, table)` channel. It powers:
//!
//! - `SELECT * FROM basin_realtime.presence(project_id, table_name)` — returns
//!   connected client IDs and subscription metadata.
//! - Automatic [`ChannelRegistry::prune`](crate::ChannelRegistry::prune) on
//!   client disconnect so idle channels do not accumulate.
//!
//! # TODO(R4)
//!
//! - Define a `PresenceRecord` (client_id, subscribed_since, last_heartbeat).
//! - Maintain a per-`(project, table)` `DashMap<ClientId, PresenceRecord>`.
//! - Insert on subscribe, remove on disconnect or heartbeat timeout.
//! - Call `ChannelRegistry::prune()` when the presence count for a key
//!   drops to zero.
//! - Expose via the SSE / WebSocket handlers (R2/R3) so they register on
//!   connect and deregister on disconnect.
//! - Gate presence write-path behind the `budget` module (R6) so a single
//!   client cannot create O(tables) presence entries cheaply.
