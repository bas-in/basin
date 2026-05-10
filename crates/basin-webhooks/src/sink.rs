//! [`WebhookSink`] — the
//! [`ChangeEventSink`](basin_common::ChangeEventSink) implementation.
//!
//! Wired into the engine via `Engine::attach_post_commit_sink`. On every
//! committed mutation, looks up matching subscriptions for `(tenant,
//! table, op)`, builds an envelope per match, and enqueues each delivery.
//! The actual HTTP POST happens out-of-band on [`WebhookWorker`] —
//! `publish` returns `Ok(())` as soon as the entries are durable in the
//! queue.

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, Result};

use basin_engine::{WebhookOps, WebhookRegistry};

use crate::clock::{Clock, SystemClock};
use crate::envelope::WebhookEnvelope;
use crate::queue::RetryQueue;
use crate::telemetry::WebhookCountersRegistry;

/// Post-commit sink. Cheap to clone (`Arc` pointers inside the registry
/// and queue).
#[derive(Clone)]
pub struct WebhookSink {
    registry: WebhookRegistry,
    queue: Arc<RetryQueue>,
    clock: Arc<dyn Clock>,
    counters: Arc<WebhookCountersRegistry>,
}

impl WebhookSink {
    pub fn new(registry: WebhookRegistry, queue: Arc<RetryQueue>) -> Self {
        Self::with_clock(registry, queue, Arc::new(SystemClock))
    }

    /// Build with an explicit clock. Production wiring uses
    /// [`Self::new`] (which is `SystemClock`); tests pass a
    /// [`TestClock`](crate::TestClock) so the sink's "now" matches the
    /// worker's "now" when fast-forwarding.
    pub fn with_clock(
        registry: WebhookRegistry,
        queue: Arc<RetryQueue>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::with_clock_and_counters(
            registry,
            queue,
            clock,
            Arc::new(WebhookCountersRegistry::new()),
        )
    }

    /// Build with both an explicit clock and an explicit counters
    /// registry. Production wiring constructs one [`WebhookCountersRegistry`]
    /// at process start and shares it between the sink and the worker so
    /// enqueue/dequeue both bump the same per-tenant counters.
    pub fn with_clock_and_counters(
        registry: WebhookRegistry,
        queue: Arc<RetryQueue>,
        clock: Arc<dyn Clock>,
        counters: Arc<WebhookCountersRegistry>,
    ) -> Self {
        Self {
            registry,
            queue,
            clock,
            counters,
        }
    }

    pub fn registry(&self) -> &WebhookRegistry {
        &self.registry
    }

    pub fn queue(&self) -> &Arc<RetryQueue> {
        &self.queue
    }

    /// Per-tenant counters registry. Cloneable handle suitable for
    /// admin handlers that want to expose webhook delivery metrics
    /// per tenant.
    pub fn counters(&self) -> Arc<WebhookCountersRegistry> {
        self.counters.clone()
    }

    /// Translate a [`ChangeOp`] into the matching [`WebhookOps`] bit.
    fn op_bit(op: ChangeOp) -> WebhookOps {
        match op {
            ChangeOp::Insert => WebhookOps::INSERT,
            ChangeOp::Update => WebhookOps::UPDATE,
            ChangeOp::Delete => WebhookOps::DELETE,
        }
    }
}

#[async_trait]
impl ChangeEventSink for WebhookSink {
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        let bit = Self::op_bit(event.op);
        let subs = self.registry.list(&event.tenant).await;
        for sub in subs {
            // Match: same table, op-bit set, not paused, no predicate.
            if sub.table != event.table {
                continue;
            }
            if !sub.ops.contains(bit) {
                continue;
            }
            if sub.paused {
                continue;
            }
            if let Some(predicate) = &sub.when_predicate {
                match crate::predicate_eval::eval(predicate, event) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        // Defensive: a predicate that fails to parse /
                        // evaluate skips delivery rather than fanning
                        // out unfiltered. Surfaces via `tracing` so
                        // operators can find malformed subscriptions.
                        tracing::warn!(
                            subscription_id = %sub.id,
                            predicate = %predicate,
                            error = %e,
                            "webhook predicate eval failed; skipping",
                        );
                        continue;
                    }
                }
            }

            let envelope = WebhookEnvelope::from_event(sub.id, event);
            // Enqueue. A queue-write error surfaces as `Err(_)` here, but
            // because this sink is wired in as **post-commit**, the
            // engine's dispatcher logs and drops it — the source mutation
            // has already committed. See `dispatch_post_commit` in
            // `basin-engine/src/events.rs`.
            //
            // We pass the injected clock's `now` (rather than letting
            // the queue read `Utc::now()` itself) so a `TestClock` in a
            // fast-forward test produces an entry whose
            // `next_attempt_at` is on the test timeline, not the wall
            // clock.
            self.queue
                .enqueue_at(sub.id, event.tenant, envelope, self.clock.now())
                .await?;
            // Bump the per-tenant pending depth; the worker decrements it
            // on dequeue. Enqueue-then-fail-to-decrement produces a stale
            // positive count, which is the right signal for "events are
            // accumulating".
            self.counters
                .for_tenant(&event.tenant)
                .record_enqueue();
        }
        Ok(())
    }
}
