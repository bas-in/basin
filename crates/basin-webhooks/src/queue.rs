//! Disk-backed retry queue with idempotency-keyed deduplication.
//!
//! ## Layout
//!
//! - **One** append-only `queue.jsonl` file per process. Not per-project.
//!   Per-project cost rule: a single project burning through millions of
//!   events does not allocate per-project file handles. Records are
//!   tagged with `project` so the worker can route to per-project rate
//!   limits inside `basin-net`, but the storage layer itself is shared.
//! - **One** `dead_letter/<subscription_id>.jsonl` per subscription.
//!   Dead-letters are per-subscription (a customer-facing entity), not
//!   per-project.
//!
//! ## Idempotency keys
//!
//! Stable SHA-256 of `subscription_id || project || table || seq`. The
//! `seq` field is a monotonic per-(project, table) counter the engine
//! mints in `next_event_seq`; the same change-event re-played through
//! the queue produces the same key. Subscribers can dedupe on
//! `X-Basin-Idempotency-Key`.
//!
//! ## In-memory mirror
//!
//! For v0.1 the queue is an in-memory `VecDeque` *that also* appends to
//! the disk file on every mutation. The disk file is the durable
//! source-of-truth for crash-recovery; reload time the file is replayed
//! into the in-memory deque. The in-memory state is the canonical
//! ordering signal for the worker (avoids re-reading the file each
//! tick).
//!
//! `Mutex` is the correct primitive here — every queue operation
//! mutates both the deque and the file.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use basin_common::{BasinError, ProjectId, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::envelope::WebhookEnvelope;
use crate::WebhookSubscriptionId;

/// Hex-encoded SHA-256. 64 ASCII chars; one allocation per delivery
/// (the hex form is what we POST in the header, so we keep it as a
/// string from the start).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    /// Stable hash. Rebuilds to the same digest for the same logical
    /// event-to-subscription delivery.
    pub fn compute(sub: WebhookSubscriptionId, project: &ProjectId, table: &str, seq: u64) -> Self {
        let mut h = Sha256::new();
        h.update(sub.0.as_bytes());
        h.update(project.as_prefix().as_bytes());
        h.update(b"|");
        h.update(table.as_bytes());
        h.update(b"|");
        h.update(seq.to_be_bytes());
        Self(hex::encode(h.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One pending delivery. Persisted as one JSONL line per state
/// transition; the latest line wins on reload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueEntry {
    pub subscription_id: WebhookSubscriptionId,
    pub idempotency_key: IdempotencyKey,
    pub project: ProjectId,
    pub envelope: WebhookEnvelope,
    /// Number of attempts already made. New entries start at 0.
    pub attempt_count: u32,
    /// Wall-clock the first attempt was scheduled. Used for the
    /// auto-pause "stale > 24h" check.
    pub first_attempt_at: DateTime<Utc>,
    /// Wall-clock at which the next attempt becomes due.
    pub next_attempt_at: DateTime<Utc>,
}

/// One row in a per-subscription dead-letter file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub idempotency_key: IdempotencyKey,
    pub project: ProjectId,
    pub envelope: WebhookEnvelope,
    pub attempt_count: u32,
    /// Why this one died: `"4xx 422"`, `"max_retries 16"`, etc.
    pub reason: String,
    pub recorded_at: DateTime<Utc>,
}

/// Disk-backed FIFO of pending deliveries. Cheap to clone (`Arc` inside);
/// every clone shares the same in-memory deque + file mutex.
#[derive(Clone)]
pub struct RetryQueue {
    inner: Arc<RetryQueueInner>,
}

struct RetryQueueInner {
    queue_path: PathBuf,
    dead_letter_root: PathBuf,
    state: Mutex<RetryQueueState>,
}

struct RetryQueueState {
    /// In-memory ordered list. The disk file is the durable record.
    entries: VecDeque<QueueEntry>,
    /// Append handle into `queue_path`. Held open so the steady-state
    /// case is one write syscall per enqueue.
    file: Option<fs::File>,
}

impl RetryQueue {
    /// Open (or create) the queue at `queue_path`, with dead-letter files
    /// living under `dead_letter_root/<subscription_id>.jsonl`.
    ///
    /// On open: replays `queue_path` into the in-memory deque so a
    /// crash-restart resumes pending deliveries. The replay is a
    /// linear scan over all historical entries; we apply a "latest line
    /// per idempotency-key wins" rule so failed → re-enqueued entries
    /// don't double-count.
    pub async fn open(queue_path: PathBuf, dead_letter_root: PathBuf) -> Result<Self> {
        if let Some(parent) = queue_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| BasinError::internal(format!("create queue dir: {e}")))?;
        }
        fs::create_dir_all(&dead_letter_root)
            .await
            .map_err(|e| BasinError::internal(format!("create dead-letter dir: {e}")))?;

        let entries = Self::replay(&queue_path).await?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&queue_path)
            .await
            .map_err(|e| BasinError::internal(format!("open queue file: {e}")))?;

        Ok(Self {
            inner: Arc::new(RetryQueueInner {
                queue_path,
                dead_letter_root,
                state: Mutex::new(RetryQueueState {
                    entries,
                    file: Some(file),
                }),
            }),
        })
    }

    /// Enqueue a fresh delivery. The idempotency key is derived from the
    /// envelope so repeated `enqueue_new` calls for the same logical
    /// delivery are caller-detectable (we deliberately do not silently
    /// dedup here — the caller controls the retry semantics).
    pub async fn enqueue_new(
        &self,
        subscription_id: WebhookSubscriptionId,
        project: ProjectId,
        envelope: WebhookEnvelope,
    ) -> Result<IdempotencyKey> {
        self.enqueue_at(subscription_id, project, envelope, Utc::now())
            .await
    }

    /// Same as [`Self::enqueue_new`] but with an explicit `now`. The
    /// sink uses this so the test clock matches between enqueue and
    /// take-due (otherwise the wall-clock from `Utc::now()` could be
    /// past the test clock and the entry would never come due in
    /// fast-forward tests).
    pub async fn enqueue_at(
        &self,
        subscription_id: WebhookSubscriptionId,
        project: ProjectId,
        envelope: WebhookEnvelope,
        now: DateTime<Utc>,
    ) -> Result<IdempotencyKey> {
        let key = IdempotencyKey::compute(subscription_id, &project, &envelope.table, envelope.seq);
        let entry = QueueEntry {
            subscription_id,
            idempotency_key: key.clone(),
            project,
            envelope,
            attempt_count: 0,
            first_attempt_at: now,
            next_attempt_at: now,
        };
        self.persist_and_push_back(entry).await?;
        Ok(key)
    }

    /// Pop the first entry whose `next_attempt_at <= now`. Returns
    /// `None` if no entry is due. The popped entry is removed from the
    /// in-memory deque; the worker is expected to either re-enqueue
    /// (on retry) or drop (on terminal success / dead-letter).
    pub async fn take_due(&self, now: DateTime<Utc>) -> Option<QueueEntry> {
        let mut g = self.inner.state.lock().await;
        let pos = g.entries.iter().position(|e| e.next_attempt_at <= now)?;
        g.entries.remove(pos)
    }

    /// Number of in-memory entries. Useful for tests and for emitting
    /// `webhook_queue_depth` metrics.
    pub async fn depth(&self) -> usize {
        self.inner.state.lock().await.entries.len()
    }

    /// Snapshot the whole queue (in-order). Used by the auto-pause
    /// check, which has to look across all entries to find the oldest
    /// still-failing attempt for a given subscription. Cheap-ish (one
    /// allocation; bounded by total pending deliveries).
    pub async fn snapshot(&self) -> Vec<QueueEntry> {
        self.inner
            .state
            .lock()
            .await
            .entries
            .iter()
            .cloned()
            .collect()
    }

    /// Re-enqueue an entry whose attempt failed transiently. Increments
    /// `attempt_count` and pushes the entry back at the tail of the
    /// deque. The caller computes `next_attempt_at` (so the worker's
    /// backoff policy is centralised there, not split across two files).
    pub async fn requeue(
        &self,
        mut entry: QueueEntry,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<()> {
        entry.attempt_count += 1;
        entry.next_attempt_at = next_attempt_at;
        self.persist_and_push_back(entry).await
    }

    /// Persist `entry` to the dead-letter file for its subscription and
    /// drop it from the queue. `reason` is the operator-facing string.
    pub async fn dead_letter(&self, entry: QueueEntry, reason: String) -> Result<()> {
        let path = self
            .inner
            .dead_letter_root
            .join(format!("{}.jsonl", entry.subscription_id));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| BasinError::internal(format!("dead-letter dir: {e}")))?;
        }
        let dle = DeadLetterEntry {
            idempotency_key: entry.idempotency_key,
            project: entry.project,
            envelope: entry.envelope,
            attempt_count: entry.attempt_count,
            reason,
            recorded_at: Utc::now(),
        };
        let line = serde_json::to_string(&dle)
            .map_err(|e| BasinError::internal(format!("serialize dead-letter: {e}")))?;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| BasinError::internal(format!("open dead-letter: {e}")))?;
        f.write_all(line.as_bytes())
            .await
            .map_err(|e| BasinError::internal(format!("write dead-letter: {e}")))?;
        f.write_all(b"\n")
            .await
            .map_err(|e| BasinError::internal(format!("write dead-letter newline: {e}")))?;
        f.flush()
            .await
            .map_err(|e| BasinError::internal(format!("flush dead-letter: {e}")))?;
        Ok(())
    }

    /// Read all dead-letter entries for `subscription_id`. Returns an
    /// empty vec if the file doesn't exist yet.
    pub async fn list_dead_letters(
        &self,
        subscription_id: WebhookSubscriptionId,
    ) -> Result<Vec<DeadLetterEntry>> {
        let path = self
            .inner
            .dead_letter_root
            .join(format!("{}.jsonl", subscription_id));
        let bytes = match fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(BasinError::internal(format!("read dead-letter: {e}"))),
        };
        let s = String::from_utf8(bytes)
            .map_err(|e| BasinError::internal(format!("dead-letter not utf-8: {e}")))?;
        let mut out = Vec::new();
        for line in s.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: DeadLetterEntry = serde_json::from_str(line)
                .map_err(|e| BasinError::internal(format!("dead-letter parse: {e}")))?;
            out.push(entry);
        }
        Ok(out)
    }

    /// Append `entry` to the disk log and the in-memory tail.
    async fn persist_and_push_back(&self, entry: QueueEntry) -> Result<()> {
        let line = serde_json::to_string(&entry)
            .map_err(|e| BasinError::internal(format!("serialize queue entry: {e}")))?;
        let mut g = self.inner.state.lock().await;
        if let Some(f) = g.file.as_mut() {
            f.write_all(line.as_bytes())
                .await
                .map_err(|e| BasinError::internal(format!("write queue: {e}")))?;
            f.write_all(b"\n")
                .await
                .map_err(|e| BasinError::internal(format!("write queue newline: {e}")))?;
            f.flush()
                .await
                .map_err(|e| BasinError::internal(format!("flush queue: {e}")))?;
        }
        g.entries.push_back(entry);
        Ok(())
    }

    /// Replay an existing queue file into a deque. The "latest line per
    /// idempotency-key wins" rule lets us reload a queue that has had
    /// many requeue-after-failure transitions without double-counting
    /// in-flight deliveries.
    async fn replay(path: &PathBuf) -> Result<VecDeque<QueueEntry>> {
        let bytes = match fs::read(path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(VecDeque::new()),
            Err(e) => return Err(BasinError::internal(format!("read queue: {e}"))),
        };
        let s = String::from_utf8(bytes)
            .map_err(|e| BasinError::internal(format!("queue not utf-8: {e}")))?;
        let mut latest: std::collections::HashMap<IdempotencyKey, QueueEntry> =
            std::collections::HashMap::new();
        let mut order: Vec<IdempotencyKey> = Vec::new();
        for line in s.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: QueueEntry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if !latest.contains_key(&entry.idempotency_key) {
                order.push(entry.idempotency_key.clone());
            }
            latest.insert(entry.idempotency_key.clone(), entry);
        }
        let mut out = VecDeque::with_capacity(order.len());
        for k in order {
            if let Some(e) = latest.remove(&k) {
                out.push_back(e);
            }
        }
        Ok(out)
    }
}

impl std::fmt::Debug for RetryQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryQueue")
            .field("queue_path", &self.inner.queue_path)
            .finish_non_exhaustive()
    }
}
