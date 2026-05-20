//! Per-tenant realtime budget enforcement — stub for Phase 5.11.R6.
//!
//! Per ADR 0018 and the multi-tenant isolation rule: per-tenant cost must be
//! O(bytes), not O(pool). The budget module enforces:
//!
//! - **Max concurrent subscriptions per project** — a cheap `AtomicUsize`
//!   counter, not a semaphore pool (O(bytes) per tenant).
//! - **Max events/s per project** — a token-bucket rate limiter (reuse
//!   `governor` already in the lock file) with a shared background bucket,
//!   not a per-connection allocator.
//! - **Max broadcast lag before force-disconnect** — if a receiver falls more
//!   than `N` events behind (configurable), it is forcibly dropped so the
//!   sender ring does not stall other receivers.
//!
//! # TODO(R6)
//!
//! - Define `BudgetConfig { max_subscriptions: usize, max_events_per_sec: u32,
//!   max_lag_events: usize }`.
//! - Implement `BudgetLedger` (one per project, stored in a `DashMap<ProjectId, _>`).
//! - Increment the subscription counter on subscribe; decrement on unsubscribe
//!   (hook into presence.rs R4).
//! - Check the token bucket in the SSE/WS handler before forwarding each event;
//!   drop or back-pressure if the budget is exhausted.
//! - Surface per-project budget metrics as Prometheus counters (reuse the
//!   `opentelemetry` workspace dep).
