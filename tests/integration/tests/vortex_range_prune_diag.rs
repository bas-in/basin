//! Diagnostic (task #83): does Vortex prune ROWS within a surviving chunk for
//! range (Gt/Lt) predicates, or only prune at CHUNK granularity and then
//! decode the whole chunk?
//!
//! Method: build a quiesced (flushed, bg-stopped) Vortex `events` table, then
//! time queries that all hit the SAME first chunk but select wildly different
//! row counts:
//!   * `id = 350`        → 1 row     (Eq — known fast, ~0.1ms on the 1M bench)
//!   * `id < 100`        → 100 rows
//!   * `id < 50000`      → ~50k rows (still chunk 0)
//!   * `id IN (clustered 100)` → 100 rows from chunk 0 (the bench shape)
//!
//! INTERPRETATION:
//!   - If `id<100` ≈ `id<50000` in latency → range pruning is CHUNK-level only;
//!     the whole ~65k-row chunk is decoded regardless of selectivity. This is
//!     the hypothesised root cause of large_in_list_100 (23ms) and
//!     jsonb_*_steady (47-118ms) on the 1M Vortex card.
//!   - If `id<100` ≪ `id<50000` → Vortex DOES row-prune ranges, and the gap is
//!     elsewhere (refuted hypothesis).
//!
//! Run: cargo test --release -p basin-integration-tests \
//!        --test vortex_range_prune_diag -- --ignored --nocapture

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

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
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
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// Mirrors compare_postgres_common::payload_for — ~150-byte multi-key JSONB.
fn bench_payload(i: i64) -> String {
    let category = match i % 3 {
        0 => "purchase",
        1 => "signup",
        _ => "click",
    };
    let device_os = if i % 2 == 0 { "ios" } else { "android" };
    let major = 1 + (i % 3);
    let minor = (i / 3) % 5;
    let patch = i % 10;
    let tags = match i % 3 {
        0 => r#"["red","green"]"#,
        1 => r#"["blue"]"#,
        _ => r#"["red","green","blue"]"#,
    };
    let campaign = match i % 4 {
        0 => "promo_2024",
        1 => "spring_sale",
        2 => "newsletter",
        _ => "referral",
    };
    let score = 0.5 + ((i % 199) as f64) * 0.5;
    format!(
        r#"{{"category":"{category}","tags":{tags},"device":{{"os":"{device_os}","version":"{major}.{minor}.{patch}"}},"metadata":{{"campaign":"{campaign}","score":{score}}}}}"#
    )
}

async fn time_query(sess: &basin_engine::ProjectSession, q: &str, iters: usize) -> (f64, usize) {
    // warm
    let _ = sess.execute(q).await.unwrap();
    let mut best = f64::MAX;
    let mut rows = 0;
    for _ in 0..iters {
        let t0 = Instant::now();
        let res = sess.execute(q).await.unwrap();
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        best = best.min(ms);
        rows = match res {
            ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            _ => 0,
        };
    }
    (best, rows)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run with --ignored --nocapture --release"]
async fn vortex_range_prune_signature() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Default format is Vortex (no WITH clause) — matches the compare_postgres card.
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, \
         amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let n: i64 = std::env::var("DIAG_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300_000);
    let batch = 10_000i64;
    let mut id = 0;
    while id < n {
        let hi = (id + batch).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 200);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            // Realistic payload identical in shape to the bench's payload_for:
            // category, tags[], nested device.{os,version}, metadata.{campaign,score}.
            stmt.push_str(&format!(
                "({k}, {}, {}, 'pending', {k}, '{}')",
                k % 1000,
                k as f64 * 0.5,
                bench_payload(k)
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }

    // QUIESCE exactly like the bench: flush tail to cold files, stop the
    // background compactor so all reads hit cold Vortex files.
    shard.flush_to_parquet().await.unwrap();
    bg.shutdown().await;

    let table = basin_common::TableName::new("events").unwrap();
    let files = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();
    println!(
        "[vortex-prune-diag] live data files after quiesce: {}",
        files.len()
    );

    let in_list: String = (0..100i64)
        .map(|k| (k * 7 + 1).to_string())
        .collect::<Vec<_>>()
        .join(",");

    // All four hit the first chunk (ids 0..~65k). Differing only in row count.
    let q_eq = "SELECT id, user_id, amount FROM events WHERE id = 350";
    let q_lt_100 = "SELECT id, user_id, amount FROM events WHERE id < 100";
    let q_lt_50k = "SELECT id, user_id, amount FROM events WHERE id < 50000";
    let q_in = format!("SELECT id, user_id, amount FROM events WHERE id IN ({in_list})");

    // Cold JSONB selective range — the exact bench shape (UDF path, no
    // promotion). Reproduces jsonb_get_text_*: does a 100-row range pay a
    // full-chunk worth of per-row JSONB extraction?
    let q_json = "SELECT payload->>'category' FROM events WHERE id < 100";

    // EXPLAIN both range queries to see WHY the JSONB-projection query prunes
    // (~1ms) while the plain-projection query scans the chunk (~17ms).
    for q in [q_lt_100, q_json] {
        if let Ok(ExecResult::Rows { batches, .. }) =
            sess.execute(&format!("EXPLAIN {q}")).await
        {
            use arrow_array::{Array, StringArray};
            let mut plan = String::new();
            for b in &batches {
                if let Ok(idx) = b.schema().index_of("QUERY PLAN") {
                    if let Some(a) = b.column(idx).as_any().downcast_ref::<StringArray>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                plan.push_str(a.value(i));
                                plan.push_str(" | ");
                            }
                        }
                    }
                }
            }
            println!(
                "[vortex-prune-diag] EXPLAIN `{}`:\n    {}\n",
                &q.chars().take(55).collect::<String>(),
                &plan.chars().take(400).collect::<String>()
            );
        }
    }

    let (eq_ms, eq_rows) = time_query(&sess, q_eq, 20).await;
    let (lt100_ms, lt100_rows) = time_query(&sess, q_lt_100, 20).await;
    let (lt50k_ms, lt50k_rows) = time_query(&sess, q_lt_50k, 10).await;
    let (in_ms, in_rows) = time_query(&sess, &q_in, 20).await;
    let (json_ms, json_rows) = time_query(&sess, q_json, 10).await;

    println!("\n[vortex-prune-diag] n={n} (quiesced, cold Vortex, best-of-N ms)");
    println!("  id = 350                       rows={eq_rows:>6}  time={eq_ms:8.3}ms");
    println!("  id < 100                       rows={lt100_rows:>6}  time={lt100_ms:8.3}ms");
    println!("  id < 50000                     rows={lt50k_rows:>6}  time={lt50k_ms:8.3}ms");
    println!("  id IN (100 vals)               rows={in_rows:>6}  time={in_ms:8.3}ms");
    println!("  payload->>'category' id<100    rows={json_rows:>6}  time={json_ms:8.3}ms");

    let ratio = lt50k_ms / lt100_ms.max(1e-9);
    println!(
        "\n  lt50k/lt100 time ratio = {ratio:.2}x  (≈1 => CHUNK-level prune only / full-chunk decode; \
         ≫1 => Vortex row-prunes ranges)"
    );
    println!(
        "  in_list vs eq          = {:.1}x slower  (IN-list pays full-chunk decode vs Eq's 1-row prune)\n",
        in_ms / eq_ms.max(1e-9)
    );

    // =====================================================================
    // PHASE 2: dirty the hot tier like the bench does. The bench shuts down
    // the compactor (bg) AFTER quiesce, then run_basin_core_suite mutates
    // ~1/3 of rows (bulk UPDATE) + DELETEs before the IN-list/JSONB reads.
    // Those overlays/tombstones are NEVER compacted (bg off), so every
    // subsequent read merges hot+cold. Re-measure the same shapes against a
    // dirtied table — the hypothesis is that THIS is what makes the bench's
    // large_in_list_100 (23ms) and jsonb_*_steady (47-88ms) blow up, not the
    // cold-read mechanism above (which is sub-ms on a clean table).
    // =====================================================================
    let third = n / 3;
    let upd = format!("UPDATE events SET amount = amount + 1.0 WHERE id < {third}");
    let t_upd = Instant::now();
    sess.execute(&upd).await.unwrap();
    let upd_ms = t_upd.elapsed().as_secs_f64() * 1000.0;
    // NOTE: deliberately do NOT flush/compact — mirror the bench (bg is off).

    let (eq2, _) = time_query(&sess, q_eq, 20).await;
    let (in2, in2_rows) = time_query(&sess, &q_in, 20).await;
    let (json2, json2_rows) = time_query(&sess, q_json, 10).await;
    let (lt100_2, _) = time_query(&sess, q_lt_100, 20).await;

    println!(
        "[vortex-prune-diag] PHASE 2 — after bulk UPDATE of {third} rows ({upd_ms:.0}ms), \
         hot tier dirty, NOT compacted (bench-like):"
    );
    println!("  id = 350                       time={eq2:8.3}ms");
    println!("  id < 100                       time={lt100_2:8.3}ms");
    println!("  id IN (100 vals)               rows={in2_rows:>6}  time={in2:8.3}ms");
    println!("  payload->>'category' id<100    rows={json2_rows:>6}  time={json2:8.3}ms");
    println!(
        "\n  IN-list  clean->dirty: {in_ms:.3}ms -> {in2:.3}ms  ({:.1}x)",
        in2 / in_ms.max(1e-9)
    );
    println!(
        "  JSONB    clean->dirty: {json_ms:.3}ms -> {json2:.3}ms  ({:.1}x)\n",
        json2 / json_ms.max(1e-9)
    );

    // =====================================================================
    // PHASE 3: replicate the bench's JSONB steady-state setup exactly —
    // run the JSONB read enough times to trip auto-promotion
    // (AUTO_PROMOTE_MIN_HITS=3), then flush_to_parquet + the backfill sweep
    // (which REWRITES every cold file to add the shadow column). Then
    // re-measure. Hypothesis: the sweep/compaction rewrite degrades the
    // file zone-maps / id-ordering, so id<100 and the IN-list can no longer
    // prune to the first chunk -> full scan -> the bench's 23ms / 47ms.
    // =====================================================================
    for _ in 0..12 {
        let _ = sess.execute(q_json).await;
        let _ = sess.execute(&q_in).await;
    }
    shard.flush_to_parquet().await.unwrap();
    let rewritten = shard
        .run_promoted_column_backfill_sweep(&project, &table)
        .await
        .unwrap_or(0);
    let files_after = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();

    let (eq3, _) = time_query(&sess, q_eq, 20).await;
    let (in3, in3_rows) = time_query(&sess, &q_in, 20).await;
    let (json3, json3_rows) = time_query(&sess, q_json, 10).await;
    let (lt100_3, _) = time_query(&sess, q_lt_100, 20).await;

    println!(
        "[vortex-prune-diag] PHASE 3 — after promote + flush + backfill sweep \
         (rewrote {rewritten} files; now {} live files):",
        files_after.len()
    );
    println!("  id = 350                       time={eq3:8.3}ms");
    println!("  id < 100                       time={lt100_3:8.3}ms");
    println!("  id IN (100 vals)               rows={in3_rows:>6}  time={in3:8.3}ms");
    println!("  payload->>'category' id<100    rows={json3_rows:>6}  time={json3:8.3}ms");
    println!(
        "\n  IN-list  clean->swept: {in_ms:.3}ms -> {in3:.3}ms  ({:.1}x)",
        in3 / in_ms.max(1e-9)
    );
    println!(
        "  JSONB    clean->swept: {json_ms:.3}ms -> {json3:.3}ms  ({:.1}x)\n",
        json3 / json_ms.max(1e-9)
    );

    // =====================================================================
    // PHASE 3b: force the HtapUnionTable merge path (apply_overlay=true) by
    // DELETEing rows OUTSIDE the id<100 range — this leaves a hot-tier
    // tombstone so the next read unions cold + hot + overlay. Then run the
    // selective id<100 query and watch DIAG_VORTEX_ROWS: with the
    // supports_filters_pushdown fix the cold scan should show
    // filter_pushed=true (decoded_rows≈100); without it, the whole chunk.
    // =====================================================================
    sess.execute("DELETE FROM events WHERE id >= 500000 AND id < 500050")
        .await
        .unwrap();
    let (lt100_ov, lt100_ov_rows) = time_query(&sess, q_lt_100, 10).await;
    let (json_ov, json_ov_rows) = time_query(&sess, q_json, 10).await;
    println!(
        "[vortex-prune-diag] PHASE 3b — HtapUnion overlay active (deleted 50 rows, NOT flushed):"
    );
    println!("  id < 100                       rows={lt100_ov_rows:>6}  time={lt100_ov:8.3}ms");
    println!("  payload->>'category' id<100    rows={json_ov_rows:>6}  time={json_ov:8.3}ms\n");

    // =====================================================================
    // PHASE 4: replicate the EXACT bench measurement on freshly-flushed
    // (cold) files: n=2 samples, NO warm-up, "median" (which for n=2 is the
    // LARGER sample — percentile(50) of 2 elems = s[1]). This is what
    // basin_p50_try does. Hypothesis: this reports the COLD first read,
    // reproducing the bench's 23ms (IN-list) / 47ms (JSONB), while the warm
    // best-of-N above shows the true steady-state ~1ms.
    // =====================================================================
    let bench_p50 = |a: f64, b: f64| -> f64 { a.max(b) }; // percentile(50) of [a,b]

    // Force cold: rewrite files once more so the footer/page cache is stale
    // for the next read (mirrors the JSONB sweep rewriting files pre-timing).
    shard.flush_to_parquet().await.unwrap();
    let _ = shard.run_promoted_column_backfill_sweep(&project, &table).await;

    // IN-list: 2 raw samples, no warm-up.
    let s1 = {
        let t = Instant::now();
        let _ = sess.execute(&q_in).await.unwrap();
        t.elapsed().as_secs_f64() * 1000.0
    };
    let s2 = {
        let t = Instant::now();
        let _ = sess.execute(&q_in).await.unwrap();
        t.elapsed().as_secs_f64() * 1000.0
    };
    // JSONB: 2 raw samples, no warm-up.
    let j1 = {
        let t = Instant::now();
        let _ = sess.execute(q_json).await.unwrap();
        t.elapsed().as_secs_f64() * 1000.0
    };
    let j2 = {
        let t = Instant::now();
        let _ = sess.execute(q_json).await.unwrap();
        t.elapsed().as_secs_f64() * 1000.0
    };

    println!("[vortex-prune-diag] PHASE 4 — EXACT bench measurement (n=2, no warm-up, median=max):");
    println!("  IN-list  samples=[{s1:.3}, {s2:.3}]ms  bench_p50={:.3}ms", bench_p50(s1, s2));
    println!("  JSONB    samples=[{j1:.3}, {j2:.3}]ms  bench_p50={:.3}ms", bench_p50(j1, j2));
    println!(
        "\n  => cold first read dominates the n=2 median. Warm best-of-N (above): \
         IN-list {in3:.3}ms, JSONB {json3:.3}ms.\n"
    );

    wal.close().await.unwrap();
}
