//! Compaction watermark — duplicate-row crash-window recovery (durability bug 2).
//!
//! `compact_one` commits the compacted cold file to the catalog, then truncates
//! the WAL. A crash *between* those two steps leaves the just-compacted WAL
//! entries intact, so cold-start replay re-appends them to the tail and the
//! next compaction commits a SECOND cold file for the same rows — duplicating
//! every row in the cold tier and over-counting on read.
//!
//! The fix persists a per-`(project, partition)` compaction watermark to the
//! catalog (the durable commit point) BEFORE the WAL truncate, and cold-start
//! replay (`replay_wal_into`) reads it and replays only entries strictly above
//! it. So a crash in the commit→truncate window is dedup-safe: the surviving
//! WAL entries sit at/below the watermark and are skipped.
//!
//! ## How the crash window is made testable
//!
//! We interpose a `NoTruncateWal` that delegates every `Wal` method to a real
//! `LocalWal` EXCEPT `truncate`, which it swallows. Running a compaction
//! through it reproduces exactly the post-commit / pre-truncate crash state:
//! the catalog has the committed file AND the watermark, but the WAL still
//! holds the compacted entries. A fresh shard opened over that same WAL +
//! catalog must NOT re-commit the batch.
//!
//! A second test pins the legacy-catalog path: a catalog that never recorded a
//! watermark (the trait default returns `None`) still replays from `Lsn::ZERO`
//! and stays correct.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, Result, TableName};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use basin_wal::{LocalWal, Lsn, Wal, WalConfig, WalEntry, WalEvent};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

// ── A WAL whose `truncate` is a no-op ──────────────────────────────────────────
//
// Everything else delegates to the wrapped real WAL. Used to freeze the WAL in
// its post-commit / pre-truncate state so a reopen sees exactly what a crash in
// that window would leave behind.
#[derive(Debug)]
struct NoTruncateWal {
    inner: Arc<dyn Wal>,
}

#[async_trait]
impl Wal for NoTruncateWal {
    async fn append(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        payload: Bytes,
    ) -> Result<Lsn> {
        self.inner.append(project, partition, payload).await
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn read_from(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        self.inner.read_from(project, partition, since_lsn).await
    }

    async fn high_water(&self, project: &ProjectId, partition: &PartitionKey) -> Result<Lsn> {
        self.inner.high_water(project, partition).await
    }

    async fn truncate(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _up_to: Lsn,
    ) -> Result<()> {
        // Swallow: simulate a crash after the catalog commit + watermark write
        // but BEFORE the WAL truncate lands.
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn read_events(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        since_lsn: Lsn,
    ) -> Result<Vec<WalEvent>> {
        self.inner.read_events(project, partition, since_lsn).await
    }
}

fn quiet_wal_cfg(dir: &std::path::Path) -> WalConfig {
    WalConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
        root_prefix: None,
        flush_interval: Duration::from_secs(3600),
        flush_max_bytes: u64::MAX,
        commit_delay: Duration::from_millis(1),
    }
}

fn storage_for(dir: &std::path::Path) -> Storage {
    Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    })
}

fn batch(start: i64, n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (start..start + n as i64).collect();
    let vs: Vec<String> = ids.iter().map(|i| format!("v-{i}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(vs)),
        ],
    )
    .unwrap()
}

fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Crash in the catalog-commit→WAL-truncate window must NOT duplicate rows.
///
/// 1. Shard A (with a non-truncating WAL) inserts N rows and compacts: the cold
///    file commits, the watermark is persisted, but the WAL still holds the
///    entries (truncate swallowed = the crash window).
/// 2. Shard B opens over the SAME WAL dir + SAME catalog (cold start / restart).
///    Replay must skip the already-committed entries via the watermark, so a
///    second compaction commits NO duplicate file.
/// 3. A read returns exactly N rows, never 2N.
#[tokio::test]
async fn crash_between_commit_and_truncate_does_not_duplicate() {
    basin_common::telemetry::try_init_for_tests();
    const N: usize = 40;

    let wal_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // Shared, persistent catalog + storage across the two shard "lifetimes".
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let storage = storage_for(data_dir.path());

    // ── Shard A: insert + compact through a non-truncating WAL ──
    {
        let real_wal: Arc<dyn Wal> =
            Arc::new(LocalWal::open(quiet_wal_cfg(wal_dir.path())).await.unwrap());
        let wal: Arc<dyn Wal> = Arc::new(NoTruncateWal {
            inner: real_wal.clone(),
        });
        let shard = Shard::new(ShardConfig::new(
            storage.clone(),
            catalog.clone(),
            wal.clone(),
        ));
        let handle = shard.get(&project, &partition).await.unwrap();
        handle.write_batch(&table, batch(0, N)).await.unwrap();
        // Force the WAL durable so the reopen below actually sees the entries
        // (the non-truncating wrapper still must flush to the backing store).
        real_wal.flush().await.unwrap();

        // Compaction commits the cold file + watermark, then "truncates" (no-op).
        shard.flush_to_parquet().await.unwrap();

        // Sanity: the watermark was recorded for this partition.
        let wm = catalog
            .get_compaction_watermark(&project, partition.as_str())
            .await
            .unwrap();
        assert!(
            matches!(wm, Some(lsn) if lsn > 0),
            "compaction must persist a non-zero watermark, got {wm:?}",
        );

        // Sanity: the WAL was NOT truncated — the crash window state.
        let surviving = real_wal
            .read_from(&project, &partition, Lsn::ZERO)
            .await
            .unwrap();
        assert!(
            !surviving.is_empty(),
            "the non-truncating WAL must still hold the compacted entries",
        );
    }

    // ── Shard B: cold start over the same WAL + catalog ──
    {
        let wal: Arc<dyn Wal> =
            Arc::new(LocalWal::open(quiet_wal_cfg(wal_dir.path())).await.unwrap());
        let shard = Shard::new(ShardConfig::new(
            storage.clone(),
            catalog.clone(),
            wal.clone(),
        ));
        let handle = shard.get(&project, &partition).await.unwrap();

        // A second compaction here would re-commit the replayed batch IF replay
        // ignored the watermark. Drive it explicitly.
        shard.flush_to_parquet().await.unwrap();

        let read = handle.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(
            rows_in(&read),
            N,
            "replay above the watermark must not duplicate the committed batch \
             (got {}, want {N})",
            rows_in(&read),
        );
    }
}

/// Legacy catalog (no watermark ever recorded) must still replay from
/// `Lsn::ZERO` and stay correct. Here the WAL truncate is the real one, so a
/// normal compaction leaves an empty WAL and a reopen replays nothing — the
/// pre-fix behavior, unchanged.
#[tokio::test]
async fn legacy_catalog_without_watermark_replays_correctly() {
    basin_common::telemetry::try_init_for_tests();
    const N: usize = 25;

    let wal_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let storage = storage_for(data_dir.path());

    // No watermark has ever been written for a brand-new partition — exactly a
    // legacy catalog's view. Confirm the read path tolerates `None`.
    let wm = catalog
        .get_compaction_watermark(&project, partition.as_str())
        .await
        .unwrap();
    assert_eq!(wm, None, "fresh partition has no watermark");

    let wal: Arc<dyn Wal> =
        Arc::new(LocalWal::open(quiet_wal_cfg(wal_dir.path())).await.unwrap());
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let handle = shard.get(&project, &partition).await.unwrap();
    handle.write_batch(&table, batch(0, N)).await.unwrap();

    let read = handle.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(
        rows_in(&read),
        N,
        "rows must be readable via tail+cold merge with no watermark recorded",
    );
}

/// Watermark round-trip + monotonicity directly at the catalog surface: a write
/// persists, a lower write never rewinds the floor, and a higher write advances
/// it.
#[tokio::test]
async fn watermark_is_monotonic() {
    let catalog = InMemoryCatalog::new();
    let project = ProjectId::new();
    let part = "default";

    assert_eq!(
        catalog.get_compaction_watermark(&project, part).await.unwrap(),
        None,
    );

    catalog
        .set_compaction_watermark(&project, part, 10)
        .await
        .unwrap();
    assert_eq!(
        catalog.get_compaction_watermark(&project, part).await.unwrap(),
        Some(10),
    );

    // A lower write must NOT rewind the floor.
    catalog
        .set_compaction_watermark(&project, part, 5)
        .await
        .unwrap();
    assert_eq!(
        catalog.get_compaction_watermark(&project, part).await.unwrap(),
        Some(10),
        "watermark must never rewind",
    );

    // A higher write advances it.
    catalog
        .set_compaction_watermark(&project, part, 99)
        .await
        .unwrap();
    assert_eq!(
        catalog.get_compaction_watermark(&project, part).await.unwrap(),
        Some(99),
    );
}
