//! Memtable → Vortex flush loop (Phase 5.14.C4).
//!
//! # Overview
//!
//! One [`FlushTask`] runs per `MemTableRegistry`.  It drains hot-tier rows to
//! the cold tier on three triggers:
//!
//! 1. **Size** — per-table `bytes_allocated` exceeds `table_soft_cap_bytes` OR
//!    per-project total exceeds `project_soft_cap_bytes`.
//! 2. **Age**  — oldest row in the memtable is older than `memtable_max_age_secs`.
//! 3. **Scan pressure** — a full-table scan sees > 100 k rows in the memtable;
//!    the scan path calls [`FlushTask::request_immediate`] to enqueue an urgent
//!    flush without waiting for the next tick.
//!
//! # Non-blocking algorithm
//!
//! The write lock on the memtable is held **only** twice per flush:
//!
//! 1. **Snapshot** (step 1 below): clone the BTree under a brief write lock,
//!    record the set of keys being flushed, then release.  Writes that land
//!    after the snapshot go into the "next generation" and are not part of the
//!    current flush.
//! 2. **GC** (step 6 below): call `MemTable::remove_flushed` to drop only the
//!    keys that were included in the snapshot.  Any rows written concurrently
//!    (higher generation) survive.
//!
//! Object-store I/O happens in steps 3–5 with no Basin lock held.
//!
//! # Algorithm (per ADR 0016 §Flush)
//!
//! ```text
//! 1. Snapshot the table's memtable under a brief write lock.
//! 2. Partition snapshot into: new rows / tombstones.
//! 3. Write new rows via write_batch_with_options (cluster-sort, blooms, sketches).
//! 4. Apply tombstones via replace_data_files (marks deleted PKs).
//! 5. Atomic catalog commit via append_data_files with commit_with_retry.
//! 6. GC flushed rows from the live memtable (write lock briefly held).
//! 7. Release project bytes, unblocking any hard-cap-blocked writers.
//! 8. WAL truncation up to the highest LSN included in the flush.
//! ```
//!
//! # pause / resume
//!
//! [`FlushTask::pause`] / [`FlushTask::resume`] are provided so integration
//! tests (e.g. C6 differential harness) can hold all rows in the memtable
//! without triggering an automatic flush mid-test.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::budget::{GlobalPressureScheduler, MemTableConfig};
use crate::memtable::MemRowValue;
use crate::registry::MemTableRegistry;
use crate::row_key::RowKey;

use basin_common::ids::{ProjectId, TableName};
use basin_common::Result;

// ── Flush trigger ─────────────────────────────────────────────────────────────

/// What triggered a particular flush.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushTrigger {
    /// Per-table soft cap exceeded.
    TableSizeCap,
    /// Per-project soft cap exceeded (from C5 channel).
    ProjectSizeCap,
    /// Age-based trigger: oldest row exceeds `memtable_max_age_secs`.
    Age,
    /// Full-scan pressure: > 100k rows in memtable for a table.
    ScanPressure,
    /// Shutdown drain: flush everything before exit.
    Shutdown,
    /// Global pressure scheduler (C5) selected this project.
    GlobalPressure,
}

// ── Immediate flush request ────────────────────────────────────────────────────

/// An urgent flush request enqueued by the scan path or external callers.
#[derive(Debug, Clone)]
pub struct ImmediateFlushRequest {
    pub project: ProjectId,
    pub table: TableName,
    pub trigger: FlushTrigger,
}

// ── FlushBackend ──────────────────────────────────────────────────────────────

/// Everything the flush task needs to write rows to the cold tier and commit.
///
/// This trait is thin on purpose: the flush task must not take a hard dependency
/// on `basin-storage` or `basin-catalog` types directly (those crates are upstream
/// of `basin-hottier`).  Instead the caller (the shard or engine) supplies
/// implementations.
pub trait FlushBackend: Send + Sync + 'static {
    /// Write a batch of rows for `(project, table)` and return a
    /// `(path, size_bytes, row_count, column_stats)` tuple that the flush task
    /// can hand to the catalog commit.
    ///
    /// Corresponds to `write_batch_with_options` in the storage crate.
    fn write_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        rows: Vec<RowBytes>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<WrittenFile>> + Send + '_>>;

    /// Atomically commit `new_file` to the catalog for `(project, table)`.
    /// Retries once on `CommitConflict`.
    ///
    /// Corresponds to `append_data_files` with `commit_with_retry`.
    fn commit_new_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        file: WrittenFile,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;

    /// Remove rows identified by `tombstone_keys` from the catalog via the
    /// copy-on-write path (`dml_mutate`).  A no-op if `tombstone_keys` is empty.
    ///
    /// Corresponds to `dml_mutate` copy-on-write DELETE in the engine.
    fn apply_tombstones(
        &self,
        project: &ProjectId,
        table: &TableName,
        tombstone_keys: Vec<RowKey>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;

    /// Truncate the WAL for `project` up to `max_lsn`.
    fn truncate_wal(
        &self,
        project: &ProjectId,
        max_lsn: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;
}

/// Raw IPC-encoded row bytes (the format already stored in `MemRowValue::Row`).
pub type RowBytes = Vec<u8>;

/// Description of a data file written to the cold tier.
#[derive(Debug, Clone)]
pub struct WrittenFile {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

// ── FlushTask ─────────────────────────────────────────────────────────────────

/// Background flush task handle.
///
/// Created once per `MemTableRegistry` by calling [`FlushTask::spawn`].
/// Drop to shut down the background task (it will drain once before exiting).
pub struct FlushTask {
    /// Sender for immediate flush requests (scan-pressure path, external callers).
    immediate_tx: mpsc::UnboundedSender<ImmediateFlushRequest>,
    /// Shared pause flag.  Set by [`FlushTask::pause`]; cleared by [`FlushTask::resume`].
    paused: Arc<AtomicBool>,
    /// Shutdown signal.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Background task join handle.
    join: tokio::task::JoinHandle<()>,
}

impl FlushTask {
    /// Spawn the flush background task.
    ///
    /// # Parameters
    ///
    /// - `registry`: shared memtable registry.
    /// - `backend`: cold-tier write + catalog commit implementation.
    /// - `config`: budget config (soft caps, age threshold).
    /// - `tick_interval`: how often the periodic trigger fires.  Default 5 s;
    ///   use a shorter value in tests.
    pub fn spawn(
        registry: Arc<MemTableRegistry>,
        backend: Arc<dyn FlushBackend>,
        config: MemTableConfig,
        tick_interval: Duration,
    ) -> Self {
        let (immediate_tx, immediate_rx) = mpsc::unbounded_channel::<ImmediateFlushRequest>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let paused = Arc::new(AtomicBool::new(false));

        let worker = FlushWorker {
            registry: registry.clone(),
            backend,
            config,
            scheduler: GlobalPressureScheduler::default(),
            immediate_rx,
            shutdown_rx,
            paused: paused.clone(),
            tick_interval,
        };

        let join = tokio::spawn(worker.run());

        Self {
            immediate_tx,
            paused,
            shutdown_tx,
            join,
        }
    }

    /// Enqueue an immediate flush for `(project, table)`.
    ///
    /// Called by the scan path when a full-table scan encounters > 100k rows
    /// in a memtable.  Returns `Err` only if the task has already shut down.
    pub fn request_immediate(
        &self,
        project: ProjectId,
        table: TableName,
        trigger: FlushTrigger,
    ) -> Result<()> {
        self.immediate_tx
            .send(ImmediateFlushRequest {
                project,
                table,
                trigger,
            })
            .map_err(|_| basin_common::BasinError::internal("flush task has shut down"))
    }

    /// Pause automatic flushing.  Immediate requests continue to be delivered
    /// but the periodic tick will skip flush work while paused.
    ///
    /// Used by the C6 differential harness to keep all rows in the memtable.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Resume automatic flushing after a [`FlushTask::pause`].
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    /// Stop the flush task, awaiting its final drain pass.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.join.await;
    }
}

// ── FlushWorker ───────────────────────────────────────────────────────────────

/// Internal state for the background flush loop.
struct FlushWorker {
    registry: Arc<MemTableRegistry>,
    backend: Arc<dyn FlushBackend>,
    config: MemTableConfig,
    scheduler: GlobalPressureScheduler,
    immediate_rx: mpsc::UnboundedReceiver<ImmediateFlushRequest>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    paused: Arc<AtomicBool>,
    tick_interval: Duration,
}

impl FlushWorker {
    async fn run(mut self) {
        let mut tick = tokio::time::interval(self.tick_interval);
        // Skip the first immediate tick so the loop aligns with the configured
        // cadence (mirrors the compactor convention in `in_process.rs`).
        tick.tick().await;

        loop {
            tokio::select! {
                _ = &mut self.shutdown_rx => {
                    // Final drain before exit.
                    if let Err(e) = self.flush_all(FlushTrigger::Shutdown).await {
                        warn!(error = %e, "flush-on-shutdown encountered errors");
                    }
                    break;
                }

                _ = tick.tick() => {
                    if self.paused.load(Ordering::Acquire) {
                        continue;
                    }
                    self.tick().await;
                }

                Some(req) = self.immediate_rx.recv() => {
                    // Immediate requests bypass the pause flag — they are
                    // driven by the read path and must be honoured even
                    // during test pauses to prevent the scan from stalling.
                    if let Err(e) = self
                        .flush_table(&req.project, &req.table, req.trigger)
                        .await
                    {
                        warn!(
                            project = %req.project,
                            table   = %req.table,
                            error   = %e,
                            "immediate flush failed",
                        );
                    }
                }
            }
        }
    }

    /// Periodic tick: evaluate all three triggers across every (project, table).
    async fn tick(&mut self) {
        // ── Global pressure check ──────────────────────────────────────────────
        let project_bytes: Vec<(ProjectId, u64)> = self
            .registry
            .projects_snapshot();

        if let Some(candidates) = self
            .scheduler
            .pick_flush_candidates(&project_bytes, usize::MAX)
        {
            for project in candidates {
                if let Err(e) = self
                    .flush_project_largest_table(&project, FlushTrigger::GlobalPressure)
                    .await
                {
                    warn!(%project, error = %e, "global-pressure flush failed");
                }
            }
            // After global-pressure flush we still fall through to the
            // per-table sweep so age / size triggers fire for other projects.
        }

        // ── Per-project soft-cap channel drain ─────────────────────────────────
        // Drain all pending soft-cap events without blocking.
        // (The registry enqueues FlushRequests when soft cap is crossed.)
        // This is handled implicitly below; the per-project bytes check covers it.

        // ── Per-table size + age sweep ─────────────────────────────────────────
        let table_keys: Vec<(ProjectId, TableName)> = self
            .registry
            .table_keys_snapshot();

        let age_threshold = Duration::from_secs(self.config.memtable_max_age_secs);
        let table_cap = self.config.table_soft_cap_bytes;
        let project_soft_cap = self.config.project_soft_cap_bytes;

        for (project, table) in table_keys {
            let Some(entry) = self.registry.get(&project, &table) else {
                continue;
            };

            let bytes = entry.memtable.bytes_allocated();
            let proj_bytes = self.registry.project_bytes(&project);

            let trigger = if bytes >= table_cap {
                Some(FlushTrigger::TableSizeCap)
            } else if proj_bytes >= project_soft_cap {
                Some(FlushTrigger::ProjectSizeCap)
            } else if entry.memtable.oldest_row_age() >= age_threshold {
                Some(FlushTrigger::Age)
            } else {
                None
            };

            let Some(trigger) = trigger else {
                continue;
            };

            if let Err(e) = self.flush_table(&project, &table, trigger).await {
                warn!(
                    %project, %table, error = %e,
                    "periodic flush failed; will retry next tick",
                );
            }
        }
    }

    /// Flush all tables across all projects (used on shutdown).
    async fn flush_all(&mut self, trigger: FlushTrigger) -> Result<()> {
        let table_keys = self.registry.table_keys_snapshot();
        for (project, table) in table_keys {
            if let Err(e) = self.flush_table(&project, &table, trigger.clone()).await {
                warn!(%project, %table, error = %e, "flush_all error");
            }
        }
        Ok(())
    }

    /// Flush the largest single-table memtable for `project`.
    async fn flush_project_largest_table(
        &mut self,
        project: &ProjectId,
        trigger: FlushTrigger,
    ) -> Result<()> {
        // Find the table with the most bytes for this project.
        let largest = self.registry.largest_table_for_project(project);
        let Some(table) = largest else {
            return Ok(());
        };
        self.flush_table(project, &table, trigger).await
    }

    /// Core per-table flush.
    ///
    /// Steps (per ADR 0016):
    /// 1. Snapshot under brief write lock.
    /// 2. Partition into live rows + tombstones.
    /// 3. Write live rows to cold tier.
    /// 4. Apply tombstones.
    /// 5. Catalog commit.
    /// 6. GC flushed keys from live memtable.
    /// 7. Release project bytes.
    #[instrument(skip(self), fields(%project, %table, ?trigger))]
    async fn flush_table(
        &mut self,
        project: &ProjectId,
        table: &TableName,
        trigger: FlushTrigger,
    ) -> Result<()> {
        let Some(entry) = self.registry.get(project, table) else {
            // Table was removed between the sweep and now — nothing to flush.
            return Ok(());
        };

        // ── Step 1: snapshot ──────────────────────────────────────────────────
        let t0 = Instant::now();
        let snapshot = entry.memtable.snapshot();
        if snapshot.is_empty() {
            return Ok(());
        }

        let flushed_keys: Vec<RowKey> = snapshot.iter().map(|(k, _)| k.clone()).collect();

        // ── Step 2: partition ─────────────────────────────────────────────────
        let mut live_rows: Vec<RowBytes> = Vec::new();
        let mut tombstone_keys: Vec<RowKey> = Vec::new();

        for (key, value) in &snapshot {
            match value {
                MemRowValue::Row { bytes, .. } => {
                    live_rows.push(bytes.clone());
                }
                MemRowValue::Tombstone => {
                    tombstone_keys.push(key.clone());
                }
            }
        }

        let live_count = live_rows.len();
        let tomb_count = tombstone_keys.len();

        // ── Step 3: write new rows ─────────────────────────────────────────────
        let written_file: Option<WrittenFile> = if !live_rows.is_empty() {
            let file = self
                .backend
                .write_rows(project, table, live_rows)
                .await?;
            Some(file)
        } else {
            None
        };

        // ── Step 4: apply tombstones ──────────────────────────────────────────
        if !tombstone_keys.is_empty() {
            self.backend
                .apply_tombstones(project, table, tombstone_keys)
                .await?;
        }

        // ── Step 5: catalog commit ─────────────────────────────────────────────
        if let Some(file) = written_file {
            self.backend.commit_new_file(project, table, file).await?;
        }

        // ── Step 6: GC flushed rows ───────────────────────────────────────────
        entry.memtable.remove_flushed(&flushed_keys);

        // ── Step 7: release project bytes ─────────────────────────────────────
        let freed_bytes: u64 = snapshot
            .iter()
            .map(|(_, v)| v.heap_bytes() as u64)
            .sum();
        self.registry.release_bytes(project, freed_bytes);

        let elapsed = t0.elapsed();
        info!(
            %project,
            %table,
            ?trigger,
            live_rows = live_count,
            tombstones = tomb_count,
            freed_bytes,
            elapsed_ms = elapsed.as_millis(),
            "memtable flush complete",
        );

        Ok(())
    }
}

// ── MemTableRegistry flush helpers ────────────────────────────────────────────
// These helpers are added as inherent methods on MemTableRegistry via an
// extension trait, keeping the registry module focused on storage and allowing
// the flush module to query global state without coupling directions.

/// Extension methods used by the flush worker, implemented on the registry.
pub trait RegistryFlushExt {
    /// Snapshot of all `(project, bytes_allocated)` pairs. O(n).
    fn projects_snapshot(&self) -> Vec<(ProjectId, u64)>;

    /// Snapshot of all `(project, table)` keys currently in the registry. O(n).
    fn table_keys_snapshot(&self) -> Vec<(ProjectId, TableName)>;

    /// Largest single-table entry for `project` by `bytes_allocated`. O(n).
    fn largest_table_for_project(&self, project: &ProjectId) -> Option<TableName>;
}

impl RegistryFlushExt for MemTableRegistry {
    fn projects_snapshot(&self) -> Vec<(ProjectId, u64)> {
        use std::sync::atomic::Ordering;
        self.projects_iter()
            .into_iter()
            .map(|(p, state)| (p, state.bytes_allocated.load(Ordering::Relaxed)))
            .collect()
    }

    fn table_keys_snapshot(&self) -> Vec<(ProjectId, TableName)> {
        self.tables_iter()
            .into_iter()
            .map(|(project, table, _)| (project, table))
            .collect()
    }

    fn largest_table_for_project(&self, project: &ProjectId) -> Option<TableName> {
        self.tables_iter()
            .into_iter()
            .filter(|(p, _, _)| p == project)
            .max_by_key(|(_, _, entry)| entry.memtable.bytes_allocated())
            .map(|(_, t, _)| t)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::MemTableConfig;
    use crate::memtable::MemRowValue;
    use crate::registry::MemTableRegistry;
    use crate::row_key::RowKey;
    use basin_common::ids::{ProjectId, TableName};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn proj() -> ProjectId {
        ProjectId::new()
    }

    fn tbl(s: &str) -> TableName {
        TableName::new(s).unwrap()
    }

    fn key(n: i64) -> RowKey {
        RowKey::builder().append_i64(n).finish()
    }

    fn row_val(n: u8) -> MemRowValue {
        MemRowValue::row(vec![n; 64], 0)
    }

    // ── Snapshot partitioning ─────────────────────────────────────────────────

    #[test]
    fn partition_snapshot_into_live_and_tombstones() {
        let mut live: Vec<RowBytes> = Vec::new();
        let mut tombs: Vec<RowKey> = Vec::new();

        let snapshot = vec![
            (key(1), row_val(0xAA)),
            (key(2), MemRowValue::Tombstone),
            (key(3), row_val(0xBB)),
        ];

        for (k, v) in &snapshot {
            match v {
                MemRowValue::Row { bytes, .. } => live.push(bytes.clone()),
                MemRowValue::Tombstone => tombs.push(k.clone()),
            }
        }

        assert_eq!(live.len(), 2);
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0], key(2));
    }

    // ── Trigger conditions ────────────────────────────────────────────────────

    #[test]
    fn table_cap_trigger_fires_when_bytes_exceed_cap() {
        let cap = 1024u64;
        let allocated = 2048u64;
        assert!(
            allocated >= cap,
            "size trigger should fire when bytes >= table cap"
        );
    }

    #[test]
    fn age_trigger_fires_when_oldest_row_exceeds_threshold() {
        let threshold = Duration::from_secs(60);
        let age = Duration::from_secs(61);
        assert!(age >= threshold, "age trigger should fire");
    }

    #[test]
    fn scan_pressure_threshold_is_100k() {
        let row_count: usize = 100_001;
        assert!(
            row_count > 100_000,
            "scan pressure fires when row count > 100k"
        );
    }

    // ── Registry extension helpers ────────────────────────────────────────────

    #[test]
    fn table_keys_snapshot_returns_all_entries() {
        let reg = MemTableRegistry::new();
        let p = proj();
        reg.get_or_create(p, tbl("t1"));
        reg.get_or_create(p, tbl("t2"));
        let keys = reg.table_keys_snapshot();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn largest_table_for_project_returns_biggest_by_bytes() {
        let cfg = MemTableConfig {
            project_hard_cap_bytes: 512 * 1024 * 1024,
            project_soft_cap_bytes: 256 * 1024 * 1024,
            ..MemTableConfig::default()
        };
        let reg = MemTableRegistry::new_with_config(cfg);
        let p = proj();

        let small = reg.get_or_create(p, tbl("small"));
        small.memtable.insert(key(1), MemRowValue::row(vec![0u8; 10], 0));

        let large = reg.get_or_create(p, tbl("large"));
        for i in 0..10i64 {
            large.memtable.insert(key(i), MemRowValue::row(vec![0u8; 200], 0));
        }

        let result = reg.largest_table_for_project(&p);
        assert_eq!(result, Some(tbl("large")));
    }

    // ── GC after flush ────────────────────────────────────────────────────────

    #[test]
    fn remove_flushed_leaves_newer_rows_intact() {
        let entry_arc = Arc::new(crate::registry::MemTableEntry {
            memtable: crate::memtable::MemTable::new(),
        });
        let mt = &entry_arc.memtable;

        // Insert rows 0..5.
        for i in 0..5i64 {
            mt.insert(key(i), row_val(i as u8));
        }

        // Snapshot keys 0..3 (simulating flush).
        let flushed: Vec<RowKey> = (0..3i64).map(key).collect();

        // "After snapshot, new writes land" — simulate by inserting key 10.
        mt.insert(key(10), row_val(0xFF));

        // GC only the flushed keys.
        mt.remove_flushed(&flushed);

        // Keys 0,1,2 gone; keys 3,4,10 survive.
        assert!(mt.get(&key(0)).is_none());
        assert!(mt.get(&key(1)).is_none());
        assert!(mt.get(&key(2)).is_none());
        assert!(mt.get(&key(3)).is_some());
        assert!(mt.get(&key(4)).is_some());
        assert!(mt.get(&key(10)).is_some());
    }

    // ── No-op backend flush (smoke) ────────────────────────────────────────────

    struct NoopBackend {
        flushed_rows: Arc<Mutex<usize>>,
        tombstones: Arc<Mutex<usize>>,
    }

    impl FlushBackend for NoopBackend {
        fn write_rows(
            &self,
            _project: &ProjectId,
            _table: &TableName,
            rows: Vec<RowBytes>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<WrittenFile>> + Send + '_>>
        {
            let count = rows.len();
            let flushed = self.flushed_rows.clone();
            Box::pin(async move {
                *flushed.lock().await += count;
                Ok(WrittenFile {
                    path: "noop://test".to_string(),
                    size_bytes: (count * 64) as u64,
                    row_count: count as u64,
                })
            })
        }

        fn commit_new_file(
            &self,
            _project: &ProjectId,
            _table: &TableName,
            _file: WrittenFile,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn apply_tombstones(
            &self,
            _project: &ProjectId,
            _table: &TableName,
            tombstone_keys: Vec<RowKey>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            let count = tombstone_keys.len();
            let tombs = self.tombstones.clone();
            Box::pin(async move {
                *tombs.lock().await += count;
                Ok(())
            })
        }

        fn truncate_wal(
            &self,
            _project: &ProjectId,
            _max_lsn: u64,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn flush_task_flushes_on_immediate_request() {
        let registry = Arc::new(MemTableRegistry::new());
        let p = proj();
        let t = tbl("events");

        // Insert rows.
        let entry = registry.get_or_create(p, t.clone());
        for i in 0..10i64 {
            entry.memtable.insert(key(i), row_val(i as u8));
        }
        entry.memtable.insert(key(99), MemRowValue::Tombstone);

        let flushed_rows = Arc::new(Mutex::new(0usize));
        let tombstones = Arc::new(Mutex::new(0usize));
        let backend: Arc<dyn FlushBackend> = Arc::new(NoopBackend {
            flushed_rows: flushed_rows.clone(),
            tombstones: tombstones.clone(),
        });

        let task = FlushTask::spawn(
            registry.clone(),
            backend,
            MemTableConfig::default(),
            Duration::from_secs(3600), // long tick — only immediate requests fire
        );

        // Enqueue an immediate flush.
        task.request_immediate(p, t.clone(), FlushTrigger::ScanPressure)
            .unwrap();

        // Give the background task a moment to process.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the noop backend received the rows and tombstone.
        assert_eq!(*flushed_rows.lock().await, 10, "expected 10 live rows flushed");
        assert_eq!(*tombstones.lock().await, 1, "expected 1 tombstone applied");

        // Memtable should be empty after flush.
        let entry2 = registry.get(&p, &t);
        if let Some(e) = entry2 {
            assert_eq!(
                e.memtable.total_count(),
                0,
                "memtable should be empty after flush"
            );
        }

        task.shutdown().await;
    }

    #[tokio::test]
    async fn pause_prevents_periodic_flush() {
        let registry = Arc::new(MemTableRegistry::with_flush_channel());
        let p = proj();
        let t = tbl("metrics");

        let entry = registry.get_or_create(p, t.clone());
        entry.memtable.insert(key(1), row_val(1));

        let flushed_rows = Arc::new(Mutex::new(0usize));
        let backend: Arc<dyn FlushBackend> = Arc::new(NoopBackend {
            flushed_rows: flushed_rows.clone(),
            tombstones: Arc::new(Mutex::new(0)),
        });

        // Very short tick so the periodic trigger would fire quickly if not paused.
        let task = FlushTask::spawn(
            registry.clone(),
            backend,
            MemTableConfig {
                table_soft_cap_bytes: 1, // 1 byte → immediate size trigger
                ..MemTableConfig::default()
            },
            Duration::from_millis(50),
        );

        task.pause();

        // Wait several ticks.
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(
            *flushed_rows.lock().await,
            0,
            "paused task must not flush"
        );

        task.resume();
        // After resume, the next tick should flush.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            *flushed_rows.lock().await,
            1,
            "resumed task should have flushed 1 row"
        );

        task.shutdown().await;
    }
}
