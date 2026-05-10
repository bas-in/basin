//! Background delivery worker.
//!
//! One [`WebhookWorker`] per process. Walks the shared [`RetryQueue`] on
//! each tick, attempts delivery via the shared `basin-net::HttpClient`,
//! and applies the post-attempt state transition:
//!
//! - 2xx → drop from queue (delivered).
//! - 4xx other than 408 / 425 / 429 → dead-letter immediately.
//! - 5xx / network error / 408 / 425 / 429 → requeue with exponential
//!   backoff; dead-letter once `attempt_count >= max_retries`.
//!
//! The worker also runs an auto-pause sweep: any subscription whose
//! oldest still-failing entry has been on the queue longer than
//! [`WebhookConfig::auto_pause_after`] is flipped to `paused = true`
//! and emits an audit log entry.
//!
//! ## Per-tenant cost
//!
//! There is **one** worker process-wide. A `HashMap<TenantId, Worker>`
//! would be a per-tenant heavy resource — that's the failure mode the
//! crate-level discipline rule exists to prevent. Per-tenant fairness
//! is delegated to `basin-net`'s keyed token bucket.
//!
//! ## Clock injection
//!
//! [`WebhookWorker`] takes a [`Clock`] (from `chrono::Utc::now` in
//! production; an [`AtomicI64`]-backed [`TestClock`] in tests). All
//! "is this entry due?" / "is this subscription stale?" decisions
//! consult the injected clock so test fixtures can fast-forward 25
//! hours without spinning a real wall clock.

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_net::HttpClient;
use chrono::DateTime;
use chrono::Utc;

use basin_engine::{WebhookRegistry, WebhookSubscriptionId};

use crate::clock::{Clock, SystemClock};
use crate::config::WebhookConfig;
use crate::queue::{QueueEntry, RetryQueue};
use crate::telemetry::WebhookCountersRegistry;

/// Outcome of one attempt. Surfaced from [`WebhookWorker::tick`] for
/// test assertions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// 2xx — delivered, dropped from the queue.
    Delivered {
        subscription_id: WebhookSubscriptionId,
        attempt_count: u32,
    },
    /// 5xx / 408 / 425 / 429 / network error — requeued.
    Requeued {
        subscription_id: WebhookSubscriptionId,
        attempt_count: u32,
        next_attempt_at: DateTime<Utc>,
    },
    /// Dead-lettered (terminal 4xx, or attempt cap hit).
    DeadLettered {
        subscription_id: WebhookSubscriptionId,
        attempt_count: u32,
        reason: String,
    },
    /// Subscription was removed between enqueue and delivery; drop the
    /// entry without contacting the customer endpoint.
    Skipped {
        subscription_id: WebhookSubscriptionId,
        reason: String,
    },
}

/// Driver loop. Hold one in the process; [`Self::run`] spawns the timer,
/// or call [`Self::tick`] directly from a test for determinism.
#[derive(Clone)]
pub struct WebhookWorker {
    inner: Arc<WorkerInner>,
}

struct WorkerInner {
    cfg: WebhookConfig,
    queue: Arc<RetryQueue>,
    registry: WebhookRegistry,
    net: HttpClient,
    clock: Arc<dyn Clock>,
    counters: Arc<WebhookCountersRegistry>,
}

impl WebhookWorker {
    pub fn new(
        cfg: WebhookConfig,
        queue: Arc<RetryQueue>,
        registry: WebhookRegistry,
        net: HttpClient,
    ) -> Self {
        Self::with_clock(cfg, queue, registry, net, Arc::new(SystemClock))
    }

    pub fn with_clock(
        cfg: WebhookConfig,
        queue: Arc<RetryQueue>,
        registry: WebhookRegistry,
        net: HttpClient,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::with_clock_and_counters(
            cfg,
            queue,
            registry,
            net,
            clock,
            Arc::new(WebhookCountersRegistry::new()),
        )
    }

    /// Build with an explicit per-tenant counters registry. Production
    /// wiring shares the registry between [`crate::WebhookSink`] (which
    /// bumps `pending_queue_depth` on enqueue) and the worker (which
    /// decrements on dequeue and records success / failure / retry /
    /// dead-letter).
    pub fn with_clock_and_counters(
        cfg: WebhookConfig,
        queue: Arc<RetryQueue>,
        registry: WebhookRegistry,
        net: HttpClient,
        clock: Arc<dyn Clock>,
        counters: Arc<WebhookCountersRegistry>,
    ) -> Self {
        Self {
            inner: Arc::new(WorkerInner {
                cfg,
                queue,
                registry,
                net,
                clock,
                counters,
            }),
        }
    }

    /// Per-tenant counters registry handle. Same registry the worker
    /// updates on every delivery attempt.
    pub fn counters(&self) -> Arc<WebhookCountersRegistry> {
        self.inner.counters.clone()
    }

    /// Drain due entries until the queue has nothing more to deliver
    /// at this instant. Also runs the auto-pause sweep. Returns the
    /// outcomes for direct test assertion (the production loop ignores
    /// them and relies on the queue + tracing).
    #[tracing::instrument(skip_all, name = "webhook_worker_tick")]
    pub async fn tick(&self) -> Vec<AttemptOutcome> {
        let mut outcomes = Vec::new();
        // Auto-pause sweep first: a subscription that has just crossed
        // the staleness threshold should not have its next tick attempt
        // delivered.
        self.auto_pause_sweep().await;
        loop {
            let now = self.inner.clock.now();
            let entry = match self.inner.queue.take_due(now).await {
                Some(e) => e,
                None => break,
            };
            // Dequeue-side bookkeeping: the sink bumped `pending_queue_depth`
            // on enqueue; balance it here so a stable working set shows
            // depth ≈ 0 between ticks.
            self.inner
                .counters
                .for_tenant(&entry.tenant)
                .record_dequeue();
            let outcome = self.attempt(entry).await;
            outcomes.push(outcome);
        }
        outcomes
    }

    /// One delivery attempt. The result drives the requeue / dead-letter
    /// / drop decision.
    #[tracing::instrument(
        skip_all,
        name = "webhook_delivery",
        fields(
            tenant = %entry.tenant,
            subscription_id = %entry.subscription_id,
            table = %entry.envelope.table,
            op = ?entry.envelope.op,
            seq = entry.envelope.seq,
            attempt_count = entry.attempt_count,
            idempotency_key = %entry.idempotency_key.as_str(),
        ),
    )]
    async fn attempt(&self, entry: QueueEntry) -> AttemptOutcome {
        let counters = self.inner.counters.for_tenant(&entry.tenant);
        let sub = match self.inner.registry.get(entry.subscription_id).await {
            Some(s) => s,
            None => {
                // Subscription was removed; drop the entry on the floor.
                tracing::info!(
                    subscription_id = %entry.subscription_id,
                    "webhook delivery skipped: subscription removed",
                );
                return AttemptOutcome::Skipped {
                    subscription_id: entry.subscription_id,
                    reason: "subscription removed".into(),
                };
            }
        };
        if sub.paused {
            // Paused subscriptions don't deliver, but we *do* keep the
            // entry around for a future resume — push it back with a
            // delay so the worker doesn't busy-loop on it.
            let next = self.inner.clock.now()
                + chrono::Duration::from_std(self.inner.cfg.backoff_max)
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
            let attempt_count = entry.attempt_count;
            // Re-queue without incrementing attempt_count so a paused
            // subscription doesn't burn through `max_retries` while it's
            // sitting paused.
            let mut e = entry;
            e.next_attempt_at = next;
            if let Err(err) = self.inner.queue.requeue(e, next).await {
                tracing::warn!(error = %err, "requeue paused entry failed");
            } else {
                // Re-add to pending depth: tick() decremented at dequeue,
                // but the entry just went back on the queue.
                counters.record_enqueue();
            }
            return AttemptOutcome::Skipped {
                subscription_id: sub.id,
                reason: format!("subscription paused (attempt_count={})", attempt_count),
            };
        }

        // Build the request. Headers carry the idempotency key + sub id
        // so the receiver can dedupe on the wire.
        let body = match serde_json::to_vec(&entry.envelope) {
            Ok(b) => b,
            Err(e) => {
                // Serialization failure is structural; dead-letter
                // immediately because retrying the same payload won't
                // help.
                let reason = format!("envelope serialize: {e}");
                let _ = self
                    .inner
                    .queue
                    .dead_letter(entry.clone(), reason.clone())
                    .await;
                counters.record_failure();
                counters.record_dead_letter();
                tracing::warn!(reason = %reason, "webhook delivery dead-lettered: envelope serialize");
                return AttemptOutcome::DeadLettered {
                    subscription_id: sub.id,
                    attempt_count: entry.attempt_count,
                    reason,
                };
            }
        };
        let req = basin_net::HttpRequest::post(&sub.url, body, "application/json")
            .with_header(
                "X-Basin-Idempotency-Key",
                entry.idempotency_key.as_str().to_string(),
            )
            .with_header(
                "X-Basin-Webhook-Subscription",
                sub.id.to_string(),
            );

        let attempt_count_after = entry.attempt_count + 1;
        let started = Instant::now();
        let resp = self.inner.net.send(&entry.tenant, &req).await;
        let elapsed_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        counters.record_latency_ms(elapsed_ms);

        let (delivered, retryable, terminal_reason) = match resp {
            Ok(r) => classify_status(r.status),
            // basin-net surfaces network / allowlist / rate-limit / body-
            // cap failures here. All of them are "the request didn't
            // produce a response" — treat as retryable, with an upper
            // bound from `max_retries`.
            Err(e) => (false, true, format!("transport: {e}")),
        };

        if delivered {
            counters.record_success();
            tracing::info!(
                latency_ms = elapsed_ms,
                attempt_count = attempt_count_after,
                "webhook delivered",
            );
            return AttemptOutcome::Delivered {
                subscription_id: sub.id,
                attempt_count: attempt_count_after,
            };
        }

        // Past this point the attempt did not deliver. Always count it
        // as a failure; retry / dead-letter is a *separate* dimension.
        counters.record_failure();

        if !retryable {
            let reason = terminal_reason;
            let mut e = entry;
            e.attempt_count = attempt_count_after;
            let _ = self.inner.queue.dead_letter(e, reason.clone()).await;
            counters.record_dead_letter();
            tracing::warn!(reason = %reason, "webhook dead-lettered: terminal");
            return AttemptOutcome::DeadLettered {
                subscription_id: sub.id,
                attempt_count: attempt_count_after,
                reason,
            };
        }

        // Hit the cap → dead-letter.
        if attempt_count_after >= sub.max_retries {
            let reason = format!(
                "max_retries {} (last error: {})",
                sub.max_retries, terminal_reason
            );
            let mut e = entry;
            e.attempt_count = attempt_count_after;
            let _ = self.inner.queue.dead_letter(e, reason.clone()).await;
            counters.record_dead_letter();
            tracing::warn!(reason = %reason, "webhook dead-lettered: max_retries");
            return AttemptOutcome::DeadLettered {
                subscription_id: sub.id,
                attempt_count: attempt_count_after,
                reason,
            };
        }

        // Otherwise: retry with exponential backoff.
        let backoff = exp_backoff(
            attempt_count_after,
            self.inner.cfg.backoff_initial,
            self.inner.cfg.backoff_max,
        );
        let next_attempt_at = self.inner.clock.now()
            + chrono::Duration::from_std(backoff)
                .unwrap_or_else(|_| chrono::Duration::seconds(1));
        let requeued = self.inner.queue.requeue(entry, next_attempt_at).await;
        if let Err(e) = &requeued {
            tracing::warn!(error = %e, "requeue failed");
        }
        if requeued.is_ok() {
            // Going back on the queue: re-add to pending depth.
            counters.record_enqueue();
            counters.record_retry();
        }
        tracing::info!(
            attempt_count = attempt_count_after,
            ?next_attempt_at,
            reason = %terminal_reason,
            "webhook requeued",
        );
        AttemptOutcome::Requeued {
            subscription_id: sub.id,
            attempt_count: attempt_count_after,
            next_attempt_at,
        }
    }

    /// Walk the queue looking for entries whose `first_attempt_at` is
    /// older than `auto_pause_after` ago. The corresponding subscriptions
    /// are flipped to `paused = true` and an audit `tracing::warn!` line
    /// is emitted (until the SQL surface lands a `webhook_auto_pauses`
    /// table view).
    async fn auto_pause_sweep(&self) {
        let now = self.inner.clock.now();
        let cutoff_secs = self.inner.cfg.auto_pause_after.as_secs() as i64;
        let cutoff = now - chrono::Duration::seconds(cutoff_secs);
        // Snapshot to avoid holding the queue lock while we mutate the
        // registry (different mutex; deadlock-free, but cleaner this
        // way).
        let entries = self.inner.queue.snapshot().await;
        let mut already_paused: std::collections::HashSet<WebhookSubscriptionId> =
            std::collections::HashSet::new();
        for entry in entries {
            // An attempt-count of 0 means "scheduled, never tried" — that
            // shouldn't auto-pause a healthy newcomer. Auto-pause only
            // fires when we have at least one failed attempt *and* the
            // entry has been around longer than the threshold.
            if entry.attempt_count == 0 {
                continue;
            }
            if entry.first_attempt_at > cutoff {
                continue;
            }
            if already_paused.contains(&entry.subscription_id) {
                continue;
            }
            if let Some(sub) = self.inner.registry.get(entry.subscription_id).await {
                if sub.paused {
                    already_paused.insert(entry.subscription_id);
                    continue;
                }
            } else {
                continue;
            }
            self.inner.registry.pause(entry.subscription_id).await;
            already_paused.insert(entry.subscription_id);
            // Audit log. The SQL view comes with the surface; this is
            // the search-grep handle until then.
            tracing::warn!(
                target: "basin_webhooks::auto_pause",
                subscription_id = %entry.subscription_id,
                tenant = %entry.tenant,
                attempt_count = entry.attempt_count,
                first_attempt_at = %entry.first_attempt_at,
                "webhook_auto_pauses: auto-paused subscription after sustained failures",
            );
        }
    }

    /// Production tick loop. Spawns on the current tokio runtime; the
    /// returned [`tokio::task::JoinHandle`] aborts on drop.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let tick = self.inner.cfg.worker_tick;
        tokio::spawn(async move {
            loop {
                let _ = self.tick().await;
                tokio::time::sleep(tick).await;
            }
        })
    }
}

/// Classify an HTTP status into (delivered, retryable, reason).
///
/// - 2xx is `(true, _, _)` and the rest is ignored.
/// - 408 / 425 / 429 are retryable 4xx (Postgres / cloud convention).
/// - Other 4xx are terminal — bad request, dead-letter immediately.
/// - 5xx is retryable.
/// - Anything else (1xx, 3xx that survived reqwest's redirect follower)
///   is treated as retryable; we don't know what it means.
fn classify_status(status: u16) -> (bool, bool, String) {
    if (200..300).contains(&status) {
        return (true, false, String::new());
    }
    if status == 408 || status == 425 || status == 429 {
        return (false, true, format!("http {status}"));
    }
    if (400..500).contains(&status) {
        return (false, false, format!("http {status}"));
    }
    if (500..600).contains(&status) {
        return (false, true, format!("http {status}"));
    }
    (false, true, format!("http {status} (unclassified)"))
}

/// `1s, 2s, 4s, 8s, ...` capped at `cap`. `attempt` is 1-indexed.
fn exp_backoff(attempt: u32, initial: Duration, cap: Duration) -> Duration {
    // Saturate at 30 to keep the shift inside `u64`. A real cap fires
    // long before that anyway.
    let shift = attempt.saturating_sub(1).min(30);
    let factor: u64 = 1u64 << shift;
    let secs = initial.as_secs().saturating_mul(factor);
    let candidate = Duration::from_secs(secs);
    if candidate > cap {
        cap
    } else {
        candidate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_2xx_delivered() {
        let (delivered, retryable, _) = classify_status(200);
        assert!(delivered);
        assert!(!retryable);
        let (delivered, _, _) = classify_status(204);
        assert!(delivered);
    }

    #[test]
    fn classify_4xx_terminal_except_few() {
        let (_, retryable, _) = classify_status(404);
        assert!(!retryable);
        let (_, retryable, _) = classify_status(422);
        assert!(!retryable);
        for s in [408, 425, 429] {
            let (delivered, retryable, _) = classify_status(s);
            assert!(!delivered);
            assert!(retryable, "{s} should be retryable");
        }
    }

    #[test]
    fn classify_5xx_retryable() {
        for s in [500, 502, 503, 504] {
            let (_, retryable, _) = classify_status(s);
            assert!(retryable, "{s} should be retryable");
        }
    }

    #[test]
    fn backoff_doubles_then_caps() {
        let init = Duration::from_secs(1);
        let cap = Duration::from_secs(8);
        assert_eq!(exp_backoff(1, init, cap), Duration::from_secs(1));
        assert_eq!(exp_backoff(2, init, cap), Duration::from_secs(2));
        assert_eq!(exp_backoff(3, init, cap), Duration::from_secs(4));
        assert_eq!(exp_backoff(4, init, cap), Duration::from_secs(8));
        assert_eq!(exp_backoff(5, init, cap), Duration::from_secs(8));
        assert_eq!(exp_backoff(20, init, cap), Duration::from_secs(8));
    }
}
