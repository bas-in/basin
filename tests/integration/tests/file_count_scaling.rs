//! File-count scale-invariant gate: prove that per-query WORK for OLTP point
//! ops stays ~flat as the number of cold FILES grows (10 -> 100 -> 500), not
//! just as the number of rows grows. A table accumulates files over its
//! lifetime (every flush/compaction tick adds some); if a point lookup's cost
//! grew with file count the product would degrade as a table ages even if it
//! never gets bigger in rows. This gate pins `files_opened` to a CONSTANT
//! across all three file counts.
//!
//! The mechanism: each flush cycle seeds a disjoint id range and writes ONE
//! PK-sorted cold file with a tight [min,max] id zone map. `reader::read` prunes
//! whole files by `column_stats` min/max before opening any, so a fresh-key
//! point lookup opens only the single file whose zone map contains the key —
//! regardless of how many other files exist. That is the design source of the
//! constant `files_opened` assertion.
//!
//! # The bloom-probe-loop cost (recorded, NOT asserted)
//!
//! Even when `files_opened` is constant, the PRUNE step itself iterates the
//! file list (one zone-map check, and for the Parquet/bloom path one bloom
//! probe, per candidate file). That loop is O(files) work that happens BEFORE
//! any file is opened — so it does not show up in `files_opened`, but it is the
//! one cost that legitimately grows with file count. We therefore PRINT the
//! per-query probe wall time at each size as an observability signal (so an
//! O(files) prune-loop is visible) but assert NOTHING on time. The hard gate is
//! `files_opened` staying constant.
//!
//! # Counter discipline
//!
//! Same as `scale_invariants.rs`: `ReadCounters` are process-global atomics;
//! the per-query measurement is a snapshot before/after `delta()`, sound only
//! because the queries run serially. Run the file with `--test-threads=1`. Each
//! size builds its own fresh engine + storage so the file sets never overlap.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// One Vortex chunk's worth of rows (default row-block cap).
const ONE_CHUNK: u64 = 65_536;

/// Rows seeded per flush cycle. Each cycle = one cold file (one disjoint id
/// range). Kept small so 500 files = 50k rows total and the test stays in the
/// seconds-range while still exercising real multi-file pruning.
const ROWS_PER_FILE: i64 = 100;

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Storage,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage)
}

fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

/// Seed `n_files` cold files (one per flush cycle, each `ROWS_PER_FILE`
/// disjoint ids) into a fresh table named `t`. Returns total row count.
async fn seed(sess: &basin_engine::ProjectSession, shard: &Shard, n_files: i64) -> i64 {
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut id = 0i64;
    for _ in 0..n_files {
        let lo = id;
        let hi = id + ROWS_PER_FILE;
        let mut stmt = String::with_capacity(ROWS_PER_FILE as usize * 16);
        stmt.push_str("INSERT INTO t VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        // One flush per batch -> one cold file per disjoint range. This is how
        // we grow file count without growing per-file size.
        shard.flush_to_parquet().await.unwrap();
        id = hi;
    }
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_count_scaling() {
    // (file_count, files_opened observed, probe_micros) for the summary.
    let mut summary: Vec<(i64, u64, u128)> = Vec::new();

    for &n_files in &[10i64, 100, 500] {
        // Fresh engine per size so file sets are isolated and counters start
        // clean for this size's run.
        let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
        let sess = engine.open_session(ProjectId::new()).await.unwrap();
        let total_rows = seed(&sess, &shard, n_files).await;

        // Fresh interior key in the MIDDLE of the key space (never read, not a
        // flush boundary) so the lookup is a genuine cold point read that must
        // prune across ALL `n_files` files down to the one containing it.
        let key = (n_files / 2) * ROWS_PER_FILE + ROWS_PER_FILE / 2 + 7;

        let before = storage.read_counters().snapshot();
        let t0 = Instant::now();
        let res = sess
            .execute(&format!("SELECT v FROM t WHERE id = {key}"))
            .await
            .unwrap();
        let probe_micros = t0.elapsed().as_micros();
        let after = storage.read_counters().snapshot();
        let d = after.delta(&before);

        assert_eq!(
            row_count(&res),
            1,
            "fresh-key point SELECT must return exactly 1 row at {n_files} files"
        );

        // ── HARD GATE: files_opened is a CONSTANT in file count ──────────────
        // INVARIANT: whole-file zone-map (column_stats min/max) pruning drops
        // every file whose id range excludes `key` BEFORE opening it, so the
        // point lookup opens the single containing file (<=2 for a boundary
        // straddle) at 10, 100 AND 500 files alike. If this grew with n_files
        // the prune regressed to opening-then-checking every file — the O(N)
        // file-count bug that makes an aging table slow even at constant row
        // count. A constant here is what proves point reads scale indefinitely
        // in file count.
        //
        // Note on the prompt's bloom-FPP concern: at 500 files the bound held
        // in design because the decisive prune is the EXACT zone-map min/max
        // check (disjoint ranges -> exactly one match), not a probabilistic
        // bloom. Blooms (row-group-level, Parquet-only) are not the mechanism
        // here, so there is no false-positive tail to widen for. We keep the
        // honest exact bound <=2 at all sizes; if a future regression ever
        // introduced bloom-FPP file opens, the failure message below tells the
        // reader to switch to a "<= 1% of files" bound with a comment.
        assert!(
            d.files_opened <= 2,
            "fresh-key point SELECT opened {} files at n_files={n_files}; \
             design bound is <=2 (exact zone-map prune). If this fails only at \
             500 due to bloom false positives, set the honest bound to \
             <= 1% of files and document it.",
            d.files_opened
        );
        // INVARIANT: decode volume is bounded by one chunk of the single
        // surviving file, independent of file count.
        assert!(
            d.rows_decoded <= 2 * ONE_CHUNK,
            "fresh-key point SELECT decoded {} rows at n_files={n_files}; bound <= 2 chunks",
            d.rows_decoded
        );

        println!(
            "[file-count-scaling] n_files={n_files} total_rows={total_rows} \
             files_opened={} rows_decoded={} bytes_fetched={} probe={}us",
            d.files_opened, d.rows_decoded, d.bytes_fetched, probe_micros
        );

        summary.push((n_files, d.files_opened, probe_micros));

        bg.shutdown().await;
        wal.close().await.unwrap();
    }

    // ── Summary: files_opened constant across sizes; probe time grows with the
    // O(files) prune loop (observability only, not asserted) ─────────────────
    println!("[file-count-scaling] SUMMARY (files_opened must be constant):");
    for (n, files_opened, micros) in &summary {
        println!(
            "[file-count-scaling]   n_files={n:>4} files_opened={files_opened} probe={micros}us"
        );
    }
    // Cross-size invariant: the MAX files_opened across all three sizes equals
    // the MIN — i.e. it is genuinely constant, not merely "small". This is the
    // strongest statement of file-count scale invariance for the point read.
    let max_opened = summary.iter().map(|(_, f, _)| *f).max().unwrap();
    let min_opened = summary.iter().map(|(_, f, _)| *f).min().unwrap();
    assert_eq!(
        max_opened, min_opened,
        "files_opened must be CONSTANT across 10/100/500 files (min={min_opened} max={max_opened}); \
         any growth means the point-read prune is O(file_count)"
    );
}
