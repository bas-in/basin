//! `basin-webhooks` — disk-backed, idempotency-keyed webhook fanout for
//! Basin's change-event substrate.
//!
//! Plugs into the Tier 0 [`ChangeEventSink`](basin_common::ChangeEventSink)
//! trait as a **post-commit** sink: every committed mutation that matches a
//! registered [`WebhookSubscription`] is enqueued for delivery to the
//! subscriber's URL. Delivery is fire-and-forget from the mutation path —
//! errors here cannot roll back the source mutation (that's the contract
//! ADR 0012 nails down).
//!
//! ## What ships in v0.1
//!
//! - In-memory subscription registry ([`WebhookRegistry`]). The catalog-
//!   persistence path lands with the `ALTER TABLE … SUBSCRIBE WEBHOOK …`
//!   SQL surface in a follow-up; until then registrations are driven by
//!   Rust callers (typically the test harness or a router-side wiring).
//! - Disk-backed retry queue ([`RetryQueue`]) keyed by a stable
//!   `(subscription_id, project, table, seq)` SHA-256 idempotency key.
//! - Worker ([`WebhookWorker`]) that drains the queue with exponential
//!   backoff (1s → 2s → … → 5min cap), stops at `max_retries`, and lands
//!   exhausted entries in a per-subscription dead-letter file.
//! - Auto-pause: a subscription whose oldest still-failing attempt
//!   crosses [`WebhookConfig::auto_pause_after`] (default 24h) is flipped
//!   to `paused = true` and an audit-log entry is emitted. Resume is
//!   explicit ([`WebhookRegistry::resume`]).
//!
//! ## Per-project cost discipline
//!
//! This crate is built to the
//! [feedback_multiproject_isolation](../../docs/feedback_multiproject_isolation.md)
//! rule: **per-project cost is O(bytes), not O(pool)**. Concretely:
//!
//! - The retry queue is a *single* shared append-only file, not a per-
//!   project file. Subscriptions carry their `ProjectId`, but no pooled
//!   resource (worker task, file handle, semaphore, connection) is
//!   allocated per-project.
//! - Per-project rate limiting comes from `basin-net::HttpClient`'s
//!   built-in keyed token bucket (governor crate). One bucket per project
//!   that has actually issued an outbound request, evicted with the
//!   `governor` keyed-state TTL — bounded.
//! - Idempotency keys are 32-byte SHA-256 digests, hex-encoded once on
//!   the way into the queue. A project churning millions of events does
//!   not allocate millions of `String`s on the engine.
//!
//! If a future change wants `HashMap<ProjectId, Worker>` or
//! `HashMap<ProjectId, File>`, that's the failure mode this rule exists
//! to prevent — stop and rethink.
//!
//! ## What's deliberately NOT here
//!
//! - The `ALTER TABLE … SUBSCRIBE WEBHOOK …` parser path. The DDL
//!   parser is being edited concurrently in another agent thread; the
//!   SQL surface lands once those edits settle.
//! - Catalog persistence for subscriptions. In-memory only in v0.1; the
//!   row-shape for the catalog table is defined when SQL surface ships.
//! - `WHERE` predicate evaluation. v0.1 only supports match-all
//!   subscriptions — [`WebhookRegistry::add`] rejects subscriptions
//!   that carry a `when_predicate`. Predicate evaluation requires the
//!   engine's expression machinery, which is part of the SQL surface
//!   work.
//! - A `webhook_dead_letters` SQL view. Dead letters are enumerable via
//!   [`RetryQueue::list_dead_letters`] today; the view is added when
//!   the SQL surface lands.

#![forbid(unsafe_code)]

mod clock;
mod config;
mod envelope;
pub mod predicate_eval;
mod queue;
#[cfg(feature = "delivery")]
mod sink;
mod telemetry;
#[cfg(feature = "delivery")]
mod worker;

pub use clock::{Clock, SystemClock, TestClock};
pub use config::WebhookConfig;
pub use envelope::{WebhookEnvelope, WebhookOpStr};
pub use queue::{DeadLetterEntry, IdempotencyKey, QueueEntry, RetryQueue};
// Subscription registry + DDL matchers live in `basin-engine` so the
// engine's `ALTER TABLE … SUBSCRIBE WEBHOOK …` pre-screen can route
// to them without re-introducing a `basin-engine -> basin-webhooks
// -> basin-net -> basin-engine` dependency cycle. We re-export the
// types here so existing callers keep their `basin_webhooks::…`
// import paths.
pub use basin_engine::{
    exec_subscribe_webhook, exec_unsubscribe_webhook, match_alter_table_subscribe_webhook,
    match_alter_table_unsubscribe_webhook, SubscribeIntent, SubscriptionError, UnsubscribeIntent,
    WebhookOps, WebhookRegistry, WebhookSubscription, WebhookSubscriptionId,
};
#[cfg(feature = "delivery")]
pub use sink::WebhookSink;
pub use telemetry::{WebhookCounters, WebhookCountersRegistry, WebhookCountersSnapshot};
#[cfg(feature = "delivery")]
pub use worker::{AttemptOutcome, WebhookWorker};
