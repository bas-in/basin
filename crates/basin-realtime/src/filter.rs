//! Filter pushdown for realtime subscriptions — stub for Phase 5.11.R5.
//!
//! R5 lets clients subscribe to a subset of rows by attaching a WHERE
//! predicate to the subscription. The filter is evaluated *in the sink*
//! (before broadcasting) so filtered-out events never hit the wire.
//!
//! # TODO(R5)
//!
//! - Define a `SubscriptionFilter` type that holds a parsed SQL predicate AST
//!   (reuse `sqlparser` already in the lock file).
//! - Evaluate the filter against `ChangeEvent::after` / `ChangeEvent::before`
//!   JSON using the same `predicate_eval` approach as `basin-webhooks`.
//! - Accept the filter at subscribe time: extend the WebSocket subscription
//!   message to `{"table": "...", "where": "amount > 100"}`.
//! - Store per-receiver filters in an `Arc<SubscriptionFilter>` parallel to
//!   the broadcast `Receiver` handle; evaluate before forwarding each event
//!   from the live channel to the transport layer.
//! - Metrics: track filter-hit rate per `(project, table)` for capacity
//!   planning (low hit-rate means the client should push-down a stricter
//!   filter or the table is under-segmented).
