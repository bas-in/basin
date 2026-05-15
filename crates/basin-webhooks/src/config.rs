//! Static knobs for the webhook fanout subsystem.
//!
//! Constructed once at process start (typically by `basin-server`'s
//! wiring layer) and threaded through [`WebhookSink`](crate::WebhookSink),
//! [`RetryQueue`](crate::RetryQueue), and [`WebhookWorker`](crate::WebhookWorker).
//!
//! Per-project cost discipline is preserved here: every value is process-
//! global, never per-project.

use std::path::PathBuf;
use std::time::Duration;

/// Webhook fanout configuration. Cheap to clone.
#[derive(Clone, Debug)]
pub struct WebhookConfig {
    /// Filesystem root for the queue + dead-letter sidecars. Each comes
    /// from `${data_dir}/webhooks/{queue,dead_letter}/...`. Production
    /// wiring sets this from `BASIN_DATA_DIR`; tests use a `TempDir`.
    pub data_dir: PathBuf,
    /// First-attempt backoff. Each subsequent attempt doubles up to
    /// [`Self::backoff_max`].
    pub backoff_initial: Duration,
    /// Cap on the per-attempt backoff. Default 5 minutes.
    pub backoff_max: Duration,
    /// A subscription whose oldest still-unflushed failing delivery has
    /// been retrying for longer than this is auto-paused. Default 24h.
    pub auto_pause_after: Duration,
    /// Tick interval for [`WebhookWorker::run`]. The worker wakes on
    /// this period and processes any due entries; smaller values reduce
    /// delivery latency at the cost of CPU.
    pub worker_tick: Duration,
}

impl WebhookConfig {
    pub const DEFAULT_BACKOFF_INITIAL_SECS: u64 = 1;
    pub const DEFAULT_BACKOFF_MAX_SECS: u64 = 5 * 60;
    pub const DEFAULT_AUTO_PAUSE_AFTER_SECS: u64 = 24 * 60 * 60;
    pub const DEFAULT_WORKER_TICK_MILLIS: u64 = 250;

    /// Build a config rooted at `data_dir` with all backoff / pause
    /// defaults. Equivalent to what production wiring constructs.
    pub fn rooted_at(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            backoff_initial: Duration::from_secs(Self::DEFAULT_BACKOFF_INITIAL_SECS),
            backoff_max: Duration::from_secs(Self::DEFAULT_BACKOFF_MAX_SECS),
            auto_pause_after: Duration::from_secs(Self::DEFAULT_AUTO_PAUSE_AFTER_SECS),
            worker_tick: Duration::from_millis(Self::DEFAULT_WORKER_TICK_MILLIS),
        }
    }

    /// Filesystem path of the (single, shared) retry queue file. Per-
    /// project cost rule: this is one file process-wide, not per-project.
    pub fn queue_path(&self) -> PathBuf {
        self.data_dir
            .join("webhooks")
            .join("queue")
            .join("queue.jsonl")
    }

    /// Filesystem path of the dead-letter file for `subscription_id`.
    /// Dead-letter storage is per-subscription (a customer-facing entity)
    /// not per-project.
    pub fn dead_letter_path(&self, subscription_id: &crate::WebhookSubscriptionId) -> PathBuf {
        self.data_dir
            .join("webhooks")
            .join("dead_letter")
            .join(format!("{subscription_id}.jsonl"))
    }
}
