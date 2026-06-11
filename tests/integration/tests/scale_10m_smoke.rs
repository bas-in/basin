//! 10M-row smoke: seed ten million rows through the real bulk-INSERT path,
//! settle the layout (flush + stripe merge, the same quiesce recipe the
//! `compare_postgres_*` cards use), then pin the point-shaped I/O invariants
//! from `scale_invariants.rs` at 100x that suite's seed size:
//!
//!   * PK point SELECT      — `files_opened <= 2` (zone-map prune to the one
//!                            [min,max]-containing file; +1 boundary slack);
//!   * keyset page          — `files_opened <= 3` (cursor band + boundary
//!                            neighbour + slack, O(1) in file count);
//!   * range scan           — exactly the 1 000 ids in the window (correctness);
//!   * COUNT(*)             — exactly 10 000 000 (correctness).
//!
//! Wall times for the seed, the settle step, and every probe are printed so a
//! run doubles as a coarse perf snapshot — but the ASSERTS are all on
//! deterministic work counters and row counts, never on wall-clock, so the
//! smoke is meaningful even on a loaded box.
//!
//! # Running it
//!
//! The test is `#[ignore]`d (sanctioned: a genuinely expensive probe slice,
//! run explicitly — the `copy_ingest_probe.rs` / `delete_latency_probe.rs`
//! convention). Seeding 10M rows takes on the order of ~10 minutes:
//!
//! ```text
//! cargo test -p basin-integration-tests --test scale_10m_smoke -- --ignored --nocapture
//! ```
//!
//! Per the repo's cargo discipline: run it ALONE (one cargo at a time on this
//! box), and treat the printed wall times as indicative unless the box is
//! idle.
//!
//! # Layout note
//!
//! The seed inserts 10k-row statements WITHOUT per-statement flushes (the
//! production write shape), so statement-affine striping spreads consecutive
//! id bands across the write stripes. The post-seed `flush_to_parquet` +
//! `run_stripe_merge_once` settle the cold tier into the merged disjoint-PK
//! file layout — the state every deployed instance reaches between merge
//! ticks, and the layout the `files_opened` bounds below are derived from.
//! The live-file count is printed before the probes so a bound failure can be
//! diagnosed against the actual layout.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// One Vortex chunk's worth of rows — see the twin const in
/// `scale_invariants.rs` for the derivation.
const ONE_CHUNK: u64 = 65_536;

/// Build a real in-process engine (Shard + WAL + LocalFileSystem storage).
/// Mirror of `scale_invariants::build` — sibling test binaries each carry
/// their own copy of this helper by suite convention.
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
            commit_delay: Duration::from_millis(2),
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

/// Count rows in an `ExecResult::Rows`, asserting it is a result set.
fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

/// Collect the first Int64 column of a result in result order.
fn ids_of(res: &ExecResult) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 must be Int64");
                for r in 0..ids.len() {
                    out.push(ids.value(r));
                }
            }
            out
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "10M smoke; ~10min; run explicitly with --ignored --nocapture, alone on an idle box"]
async fn scale_10m_smoke() {
    let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    const SCALE: i64 = 10_000_000;
    const BATCH: i64 = 10_000; // matches `insert_batch_for(10M)` in the compare suite

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();

    // ── Seed: 1 000 statements x 10k literal rows ────────────────────────────
    // The same bulk-INSERT shape (and batch size) the published 10M compare
    // card seeds with. No per-statement flush — the background loop and the
    // post-seed settle step below produce the deployed cold-tier layout.
    let seed_started = Instant::now();
    let mut id = 0i64;
    while id < SCALE {
        let lo = id;
        let hi = (id + BATCH).min(SCALE);
        let mut stmt = String::with_capacity((hi - lo) as usize * 20);
        stmt.push_str("INSERT INTO t VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
        if id % 1_000_000 == 0 {
            println!(
                "[10m-smoke] seeded {id} rows ({:.1}s elapsed)",
                seed_started.elapsed().as_secs_f64()
            );
        }
    }
    let seed_s = seed_started.elapsed().as_secs_f64();
    println!("[10m-smoke] seed {SCALE} rows: {seed_s:.1}s");

    // ── Settle: flush + stripe merge + stop background loops ────────────────
    // Identical quiesce recipe to `run_full_compare_inner`: drain the WAL
    // tail to cold files, run one stripe-merge pass to reach the merged
    // disjoint-PK layout, then stop the background loop so nothing rewrites
    // files under the probes.
    let settle_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    bg.shutdown().await;
    let settle_s = settle_started.elapsed().as_secs_f64();
    println!("[10m-smoke] settle (flush + stripe merge): {settle_s:.1}s");

    // Print the live cold-file count so a bound failure below is diagnosable
    // against the actual layout the merge produced.
    let table = TableName::new("t").unwrap();
    let live_files = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .live_data_files()
        .len();
    println!("[10m-smoke] settled layout: {live_files} live data files");

    // Per-probe counter delta helper (counter discipline: probes run one at a
    // time on this single session; the background loop is already stopped).
    let probe = |sql: String| {
        let sess = &sess;
        let storage = &storage;
        async move {
            let before = storage.read_counters().snapshot();
            let started = Instant::now();
            let res = sess.execute(&sql).await.unwrap();
            let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
            let after = storage.read_counters().snapshot();
            (res, after.delta(&before), wall_ms)
        }
    };

    // ── 1. PK point SELECT: files_opened <= 2 at 10M ─────────────────────────
    // Interior, non-boundary key. The zone-map prune must leave the single
    // [min,max]-containing file (2 with boundary slack) — the same O(1)-in-N
    // invariant `scale_invariants.rs` pins at 100k, two orders of magnitude up.
    let key = SCALE / 2 + 137;
    let (res, d, wall) = probe(format!("SELECT v FROM t WHERE id = {key}")).await;
    assert_eq!(row_count(&res), 1, "point SELECT must return exactly the row");
    assert!(
        d.files_opened <= 2,
        "10M point SELECT opened {} files; design bound is <=2 (zone-map prune) — \
         growth here means the prune regressed to a scan at scale",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "10M point SELECT decoded {} rows; bound <= 2 chunks",
        d.rows_decoded
    );
    println!(
        "[10m-smoke] point SELECT: {wall:.2}ms, files_opened={}, rows_decoded={}",
        d.files_opened, d.rows_decoded
    );

    // ── 2. Keyset page: files_opened <= 3 at 10M ─────────────────────────────
    // Mid-table cursor; the ordered short-circuit traversal must stop after
    // the cursor band (+ a boundary neighbour). <=3 carries one file of slack
    // over the disjoint-layout design bound of 2 — still O(1) in file count.
    let cursor = SCALE / 2 + 10_345;
    let (res, d, wall) = probe(format!(
        "SELECT id, v FROM t WHERE id > {cursor} ORDER BY id LIMIT 50"
    ))
    .await;
    assert_eq!(
        ids_of(&res),
        ((cursor + 1)..=(cursor + 50)).collect::<Vec<i64>>(),
        "keyset page must be the 50 smallest ids > cursor"
    );
    assert!(
        d.files_opened <= 3,
        "10M keyset page opened {} files; bound <=3 (cursor band + boundary + slack) — \
         more means the ordered traversal regressed to the all-candidate fan-out",
        d.files_opened
    );
    println!(
        "[10m-smoke] keyset page: {wall:.2}ms, files_opened={}, rows_decoded={}",
        d.files_opened, d.rows_decoded
    );

    // ── 3. Range scan correctness ────────────────────────────────────────────
    // 1 000-id window in the upper half of the keyspace: exactly the window's
    // ids, in order. Correctness gate (no counter bound — a range legitimately
    // touches the overlapping band(s)); wall + counters printed for the record.
    let range_lo = SCALE / 4 * 3; // 7.5M
    let range_hi = range_lo + 999;
    let (res, d, wall) = probe(format!(
        "SELECT id, v FROM t WHERE id BETWEEN {range_lo} AND {range_hi} ORDER BY id"
    ))
    .await;
    assert_eq!(
        ids_of(&res),
        (range_lo..=range_hi).collect::<Vec<i64>>(),
        "range scan must return exactly the 1000 ids in the window, in order"
    );
    println!(
        "[10m-smoke] range scan (1k rows): {wall:.2}ms, files_opened={}, rows_decoded={}",
        d.files_opened, d.rows_decoded
    );

    // ── 4. COUNT(*) correctness ──────────────────────────────────────────────
    // Exact full-table count — proves no row was lost or duplicated across
    // 1 000 statements, the tail flush, and the stripe merge.
    let (res, d, wall) = probe("SELECT COUNT(*) FROM t".to_string()).await;
    assert_eq!(
        ids_of(&res),
        vec![SCALE],
        "COUNT(*) must be exactly {SCALE} after seed + flush + merge"
    );
    println!(
        "[10m-smoke] COUNT(*): {wall:.2}ms, files_opened={}, rows_decoded={}",
        d.files_opened, d.rows_decoded
    );

    println!(
        "[10m-smoke] scale={SCALE} files={live_files}: seed {seed_s:.1}s, settle {settle_s:.1}s; \
         point <=2 opens, keyset <=3 opens, range + COUNT(*) exact — all PASS"
    );

    wal.close().await.unwrap();
}
