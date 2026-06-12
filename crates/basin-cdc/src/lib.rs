//! `basin-cdc` — durable change-data-capture ring + SSE stream (ADR 0028
//! Phase 1).
//!
//! # What this crate is
//!
//! A second consumer of the engine's post-commit [`ChangeEventSink`] hook
//! (the same seam `basin-realtime` uses for its lossy live broadcast). Where
//! realtime is an in-memory, best-effort overlay, **CDC is the durable,
//! resumable, at-least-once contract**: every committed change is appended to
//! a per-project append-only ring on object storage, and subscribers consume
//! it over Server-Sent Events with a resumable `seq` cursor.
//!
//! # Capture wiring — no executor changes
//!
//! [`CdcRingWriter`] implements [`ChangeEventSink`] and is attached as a
//! **post-commit** sink (`Engine::attach_post_commit_sink`) alongside the
//! realtime sink. The post-commit hook (`dispatch_post_commit` in
//! `basin-engine/src/executor.rs`) already fires for **every** committed
//! mutation — including the HTAP hot-tier UPDATE/DELETE fast paths that bypass
//! the WAL (`hot_tier_update_by_pk` / `hot_tier_delete_by_pk`), which build
//! their events via `build_update_events` / `build_delete_events` and dispatch
//! them through the same hook. So CDC captures fast-path mutations with **zero
//! edits to `executor.rs`**. The Phase-1 regression test (`tests/`) is the
//! standing gate that proves this stays true.
//!
//! # Committed-only, commit-ordered
//!
//! Post-commit means rolled-back transactions never reach the sink (the
//! executor drops `TxState::tx_overlay` on ROLLBACK before the hook fires).
//! Within a project, the engine assigns `seq` monotonically and the hook is
//! serialised per project, so append order equals `seq` order. Auto-commit
//! statements are their own commit (one event per affected row); explicit
//! transactions emit their rows with contiguous `seq` and the same
//! `committed_at` — captured as one logical group by the time-window batcher.
//!
//! # Batching & the crash-loss window
//!
//! Incoming events are buffered per project in memory and flushed to a durable
//! object-store segment on a size threshold ([`ring::SEGMENT_MAX_RECORDS`] /
//! [`ring::SEGMENT_MAX_BYTES`]) or a short time window
//! ([`CdcConfig::flush_interval`]). Because the sink is post-commit, the row is
//! already durably committed before its CDC record is flushed: a crash in the
//! `commit → flush` window loses that record from the *stream* (not from the
//! table). The flush PUT is retried with bounded backoff to shrink the window.
//! This is the same honest at-least-once posture the ADR documents (§5,
//! "Trigger to revisit" §3); Phase 1 does not block the commit path on the PUT.
//!
//! # Retention
//!
//! The ring is pruned by age, independently of WAL truncation (ADR §5).
//! Retention is `BASIN_CDC_RETENTION_HOURS` (default 24, clamped to the ADR's
//! 168h / 7-day max). A subscriber whose cursor predates the retained window
//! receives a `cursor_expired` frame and must re-snapshot.

mod gc;
mod live;
mod record;
mod ring;
pub mod sse;
/// ADR 0028 Phase 2 — webhook push sink (delivery worker).
pub mod webhook;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{ChangeEvent, ChangeEventSink, ProjectId, Result};
use basin_storage::Storage;
use tokio::sync::Mutex;

pub use gc::{run_once as gc_run_once, spawn_retention_gc, RetentionGc};
pub use live::{LiveRegistry, LIVE_CHANNEL_CAPACITY};
pub use record::{CdcRecord, CdcSseFrame};
pub use ring::{CdcIndex, ProjectRing, SEGMENT_MAX_BYTES, SEGMENT_MAX_RECORDS};
pub use sse::{cdc_router, CdcSseState};
pub use webhook::{
    mint_secret_hex, mint_webhook_id, sign_body, spawn_webhook_dispatcher, ReqwestHttp,
    WebhookConfig, WebhookDispatcher, WebhookHttp, SEQ_RANGE_HEADER, SIGNATURE_HEADER,
};

/// Env var: ring retention window in hours. Default 24, max 168 (7 days).
pub const RETENTION_HOURS_ENV: &str = "BASIN_CDC_RETENTION_HOURS";
/// Env var: flush time-window in milliseconds. Default 250ms.
pub const FLUSH_INTERVAL_MS_ENV: &str = "BASIN_CDC_FLUSH_INTERVAL_MS";
/// Env var: retention GC tick interval in seconds. Default 3600 (1h).
pub const GC_INTERVAL_SECS_ENV: &str = "BASIN_CDC_GC_INTERVAL_SECS";

const DEFAULT_RETENTION_HOURS: u64 = 24;
const MAX_RETENTION_HOURS: u64 = 168;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Tunable CDC configuration, seeded from the environment.
#[derive(Clone, Debug)]
pub struct CdcConfig {
    /// Ring retention window. Segments older than this are GC'd.
    pub retention: Duration,
    /// Max time an event waits in the in-memory buffer before a flush.
    pub flush_interval: Duration,
    /// Retention GC tick interval.
    pub gc_interval: Duration,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(DEFAULT_RETENTION_HOURS * 3600),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            gc_interval: Duration::from_secs(3600),
        }
    }
}

impl CdcConfig {
    /// Build from the `BASIN_CDC_*` env vars, clamping retention to the ADR
    /// maximum of 7 days.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(h) = std::env::var(RETENTION_HOURS_ENV).ok().and_then(|s| s.parse::<u64>().ok())
        {
            let h = h.clamp(1, MAX_RETENTION_HOURS);
            cfg.retention = Duration::from_secs(h * 3600);
        }
        if let Some(ms) = std::env::var(FLUSH_INTERVAL_MS_ENV)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            cfg.flush_interval = Duration::from_millis(ms.max(1));
        }
        if let Some(secs) = std::env::var(GC_INTERVAL_SECS_ENV)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            cfg.gc_interval = Duration::from_secs(secs.max(1));
        }
        cfg
    }
}

/// Per-project in-memory flush buffer.
#[derive(Default)]
struct ProjectBuffer {
    pending: Vec<CdcRecord>,
    pending_bytes: usize,
}

/// The durable CDC sink. Attach via `Engine::attach_post_commit_sink`.
///
/// Cheap to clone — all state is behind `Arc`s. The SSE router and the
/// retention GC task share the same [`Storage`] handle and live registry.
#[derive(Clone)]
pub struct CdcRingWriter {
    storage: Storage,
    cfg: CdcConfig,
    /// Per-project flush buffers, guarded by an async mutex per writer.
    buffers: Arc<Mutex<HashMap<ProjectId, ProjectBuffer>>>,
    /// In-process live-tail broadcast (SSE fast path).
    live: LiveRegistry,
}

impl CdcRingWriter {
    /// Build a writer over the engine's [`Storage`] with env-seeded config.
    pub fn new(storage: Storage) -> Self {
        Self::with_config(storage, CdcConfig::from_env())
    }

    /// Build with explicit config (tests).
    pub fn with_config(storage: Storage, cfg: CdcConfig) -> Self {
        Self {
            storage,
            cfg,
            buffers: Arc::new(Mutex::new(HashMap::new())),
            live: LiveRegistry::new(),
        }
    }

    /// The in-process live-tail registry, for the SSE handler's live phase.
    pub fn live(&self) -> &LiveRegistry {
        &self.live
    }

    /// The storage handle, for the SSE handler's replay phase and the GC task.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// The active config (retention, GC interval).
    pub fn config(&self) -> &CdcConfig {
        &self.cfg
    }

    /// A durable ring handle for `project`, bound to the project-scoped store
    /// and honouring the configured `root_prefix`.
    pub fn ring_for(&self, project: &ProjectId) -> ProjectRing {
        ProjectRing::with_root(
            *project,
            self.storage.project_object_store(project),
            self.storage.root_prefix_handle().map(|p| p.to_string()),
        )
    }

    /// Flush a project's pending buffer to a durable segment. Drains the
    /// buffer first (so a slow PUT does not hold the lock), then appends.
    ///
    /// The post-commit hook (`dispatch_post_commit`) fans events out one
    /// `tokio::spawn` per event, so `publish` calls for a single commit can
    /// arrive at this sink concurrently and out of `seq` order. We restore the
    /// total order here by sorting the drained batch by `seq` before the
    /// segment PUT; combined with `ProjectRing::replay_after`'s global sort,
    /// durable delivery is in strict per-project commit order (ADR §4). The
    /// best-effort live tail may momentarily reorder, but the SSE handler's
    /// `seq <= high` de-dup guard drops late stragglers, and a reconnect
    /// replays from the ordered ring.
    async fn flush_project(&self, project: ProjectId) {
        let mut drained = {
            let mut bufs = self.buffers.lock().await;
            match bufs.get_mut(&project) {
                Some(buf) if !buf.pending.is_empty() => {
                    buf.pending_bytes = 0;
                    std::mem::take(&mut buf.pending)
                }
                _ => return,
            }
        };
        if drained.is_empty() {
            return;
        }
        drained.sort_by_key(|r| r.seq);
        let ring = self.ring_for(&project);
        if let Err(e) = ring.append(&drained).await {
            tracing::error!(
                project = %project,
                count = drained.len(),
                error = %e,
                "cdc: flush failed; records lost from ring",
            );
        }
    }

    /// Flush every project's buffer. Called by the time-window flusher and on
    /// shutdown.
    async fn flush_all(&self) {
        let projects: Vec<ProjectId> = {
            let bufs = self.buffers.lock().await;
            bufs.iter()
                .filter(|(_, b)| !b.pending.is_empty())
                .map(|(p, _)| *p)
                .collect()
        };
        for p in projects {
            self.flush_project(p).await;
        }
    }

    /// Force a flush of every project's buffer. Exposed for the regression
    /// harness, which asserts durable contents without waiting for the
    /// time-window flusher to tick. Production uses [`Self::spawn_flusher`].
    pub async fn flush_all_for_test(&self) {
        self.flush_all().await;
    }

    /// Spawn the background time-window flusher. Call once at startup. The
    /// returned handle keeps flushing every [`CdcConfig::flush_interval`]
    /// until dropped.
    pub fn spawn_flusher(&self) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        let interval = self.cfg.flush_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                me.flush_all().await;
            }
        })
    }
}

#[async_trait]
impl ChangeEventSink for CdcRingWriter {
    /// Buffer one committed change. Flushed to the durable ring on a size
    /// threshold or the next time-window tick. Published to the in-process
    /// live tail immediately so connected subscribers see it without waiting
    /// for the durable flush.
    ///
    /// Returns `Ok(())` even on internal trouble — post-commit sinks must not
    /// roll back the engine (ADR 0012). Durability is the flush's job, retried
    /// with backoff.
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        // `wal_lsn` is not carried on `ChangeEvent` at this seam; store 0 (see
        // CdcRecord::wal_lsn doc — Phase 5 concern, not exposed in Phase 1).
        let rec = Arc::new(CdcRecord::from_event(event, 0));

        // Live tail first: a reconnect that races a flush replays from the
        // ring, so a live drop is never a lost-event (mirrors realtime).
        self.live.publish(event.project, Arc::clone(&rec));

        let should_flush = {
            let mut bufs = self.buffers.lock().await;
            let buf = bufs.entry(event.project).or_default();
            // Estimate encoded size cheaply (record + newline framing).
            let est = serde_json::to_vec(rec.as_ref()).map(|b| b.len() + 1).unwrap_or(256);
            buf.pending.push((*rec).clone());
            buf.pending_bytes += est;
            buf.pending.len() >= SEGMENT_MAX_RECORDS || buf.pending_bytes >= SEGMENT_MAX_BYTES
        };
        if should_flush {
            self.flush_project(event.project).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    fn storage_in(dir: &TempDir) -> Storage {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        Storage::new(basin_storage::StorageConfig {
            object_store: StdArc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        })
    }

    fn ev(project: ProjectId, table: &str, seq: u64) -> ChangeEvent {
        ChangeEvent {
            project,
            table: basin_common::TableName::new(table).unwrap(),
            op: basin_common::ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({ "id": seq })),
            committed_at: chrono::Utc::now(),
            seq,
            causation_user: None,
        }
    }

    #[tokio::test]
    async fn append_and_readback_across_reopen() {
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();

        // Writer #1: publish 3 events, flush.
        {
            let w = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
            for seq in 1..=3 {
                w.publish(&ev(project, "t", seq)).await.unwrap();
            }
            w.flush_all().await;
        }

        // Writer #2 (fresh instance — simulates process reopen): replay all.
        let w2 = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
        let recs = w2.ring_for(&project).replay_after(0).await.unwrap();
        let seqs: Vec<u64> = recs.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3], "all events survive writer reopen");
    }

    #[tokio::test]
    async fn replay_from_cursor_returns_only_missed() {
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let w = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
        for seq in 1..=5 {
            w.publish(&ev(project, "t", seq)).await.unwrap();
        }
        w.flush_all().await;

        let recs = w.ring_for(&project).replay_after(2).await.unwrap();
        let seqs: Vec<u64> = recs.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5], "resume_after=2 returns only > 2");
    }

    #[tokio::test]
    async fn cross_project_isolation() {
        let dir = TempDir::new().unwrap();
        let a = ProjectId::new();
        let b = ProjectId::new();
        let w = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
        for seq in 1..=3 {
            w.publish(&ev(a, "t", seq)).await.unwrap();
        }
        for seq in 1..=2 {
            w.publish(&ev(b, "t", seq)).await.unwrap();
        }
        w.flush_all().await;

        let a_recs = w.ring_for(&a).replay_after(0).await.unwrap();
        let b_recs = w.ring_for(&b).replay_after(0).await.unwrap();
        assert_eq!(a_recs.len(), 3);
        assert_eq!(b_recs.len(), 2);
        assert!(a_recs.iter().all(|r| r.project == a), "A ring is A-only");
        assert!(b_recs.iter().all(|r| r.project == b), "B ring never sees A");
    }

    #[tokio::test]
    async fn retention_prune_and_cursor_gap() {
        use chrono::Utc;
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let w = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());

        // Two events committed "long ago".
        for seq in 1..=2 {
            let mut e = ev(project, "t", seq);
            e.committed_at = Utc::now() - chrono::Duration::hours(48);
            w.publish(&e).await.unwrap();
        }
        w.flush_all().await;
        // One fresh event.
        w.publish(&ev(project, "t", 3)).await.unwrap();
        w.flush_all().await;

        let ring = w.ring_for(&project);
        let deleted = ring
            .prune_older_than(Utc::now() - chrono::Duration::hours(24))
            .await
            .unwrap();
        assert_eq!(deleted, 1, "the old segment is pruned");

        // The surviving min seq is 3; a cursor at seq=1 predates the window.
        let min = ring.min_available_seq().await.unwrap();
        assert_eq!(min, Some(3));
        let resumed = ring.replay_after(0).await.unwrap();
        assert_eq!(
            resumed.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![3],
            "pruned events are gone; only seq=3 replays",
        );
    }

    #[tokio::test]
    async fn gc_sweep_discovers_projects_and_prunes() {
        use chrono::Utc;
        let dir = TempDir::new().unwrap();
        let a = ProjectId::new();
        let b = ProjectId::new();
        // Short retention so the sweep prunes the "old" segments.
        let cfg = CdcConfig {
            retention: Duration::from_secs(3600), // 1h
            ..CdcConfig::default()
        };
        let w = CdcRingWriter::with_config(storage_in(&dir), cfg);

        for project in [a, b] {
            // Old event in its own segment...
            let mut e = ev(project, "t", 1);
            e.committed_at = Utc::now() - chrono::Duration::hours(48);
            w.publish(&e).await.unwrap();
            w.flush_all().await;
            // ...fresh event in a separate segment, so the GC can drop the old
            // segment without losing the fresh one (a segment is retained if
            // ANY of its records is at/after the cutoff).
            w.publish(&ev(project, "t", 2)).await.unwrap();
            w.flush_all().await;
        }

        // The cross-project sweep discovers both rings via the cdc/ prefix and
        // prunes each project's stale segment.
        let deleted = crate::gc_run_once(&w, Duration::from_secs(3600)).await.unwrap();
        assert_eq!(deleted, 2, "one stale segment pruned per project");

        for project in [a, b] {
            let remaining = w.ring_for(&project).replay_after(0).await.unwrap();
            assert_eq!(
                remaining.iter().map(|r| r.seq).collect::<Vec<_>>(),
                vec![2],
                "only the fresh event survives in each project's ring",
            );
        }
    }

    #[tokio::test]
    async fn size_threshold_forces_flush_without_window() {
        // A buffer that hits SEGMENT_MAX_RECORDS flushes synchronously inside
        // publish(), so the durable ring has the records even with a very long
        // flush window configured.
        let dir = TempDir::new().unwrap();
        let project = ProjectId::new();
        let cfg = CdcConfig {
            flush_interval: Duration::from_secs(3600),
            ..CdcConfig::default()
        };
        let w = CdcRingWriter::with_config(storage_in(&dir), cfg);
        for seq in 1..=(SEGMENT_MAX_RECORDS as u64) {
            w.publish(&ev(project, "t", seq)).await.unwrap();
        }
        let recs = w.ring_for(&project).replay_after(0).await.unwrap();
        assert_eq!(recs.len(), SEGMENT_MAX_RECORDS, "size threshold flushed");
    }
}
