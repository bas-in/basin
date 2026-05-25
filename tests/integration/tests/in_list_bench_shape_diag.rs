//! Diagnostic: does the IN-list PK-probe fire on the EXACT bench shape
//! (`SELECT id, user_id, amount FROM events WHERE id IN (1,8,...,694)`)?
//! The bench's 100 ids are clustered in [1,694] → all in the first file at
//! scale, so the probe should prune to ~1 file. If the bloom-skip counter
//! advances, the probe fires (581ms is elsewhere); if not, the multi-col
//! projection or a gate bails it.

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

async fn build() -> (TempDir, TempDir, Engine, basin_shard::ShardBackgroundHandle, Arc<dyn Wal>) {
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
    let engine = Engine::new(EngineConfig { storage, catalog, shard: Some(shard) });
    (sd, wd, engine, bg, wal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "diagnostic — run with --ignored --nocapture"]
async fn in_list_bench_shape_probe_fires() {
    let (_sd, _wd, engine, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    // Bench events schema (incl JSONB payload).
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)")
        .await.unwrap();

    let n = 200_000i64;
    let batch = 10_000i64;
    let mut id = 0;
    while id < n {
        let hi = (id + batch).min(n);
        let mut stmt = String::with_capacity((hi - id) as usize * 60);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id { stmt.push(','); }
            stmt.push_str(&format!("({k}, {}, {}, 'pending', {k}, '{{\"kind\":\"k{}\"}}')", k % 1000, k as f64 * 0.5, k % 20));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }

    // Exact bench IN-list: ids {1,8,15,...,694} — all in the first 10k file.
    let in_list: String = (0..100i64).map(|k| (k * 7 + 1).to_string()).collect::<Vec<_>>().join(",");
    let q = format!("SELECT id, user_id, amount FROM events WHERE id IN ({in_list})");

    // warm
    let _ = sess.execute(&q).await.unwrap();

    let before = engine.blooms_skipped_count();
    let t0 = Instant::now();
    let res = sess.execute(&q).await.unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    let after = engine.blooms_skipped_count();
    let rows = match res { ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(), _ => 0 };

    let approx_files = n / batch;
    println!("[in-list-diag] n={n} files~{approx_files} rows_returned={rows} time={ms:.2}ms bloom_skips={} (probe fires iff skips>0)", after - before);

    // Also test the single-col projection (what the agent's passing test used)
    let q1 = format!("SELECT id FROM events WHERE id IN ({in_list})");
    let _ = sess.execute(&q1).await.unwrap();
    let b1 = engine.blooms_skipped_count();
    let _ = sess.execute(&q1).await.unwrap();
    let a1 = engine.blooms_skipped_count();
    println!("[in-list-diag] single-col 'SELECT id' IN-list bloom_skips={}", a1 - b1);

    // CONTROL: single-Eq point query on the SAME shard data. If THIS prunes
    // (skips>0) but IN-list doesn't, the bug is IN-list-specific. If neither
    // prunes, the shard files lack id zone-maps and single-Eq is fast for
    // another reason (memtable probe / small data).
    let qeq = "SELECT id, user_id, amount FROM events WHERE id = 350";
    let _ = sess.execute(qeq).await.unwrap();
    let be = engine.blooms_skipped_count();
    let teq = Instant::now();
    let _ = sess.execute(qeq).await.unwrap();
    let eq_ms = teq.elapsed().as_secs_f64() * 1000.0;
    let ae = engine.blooms_skipped_count();
    println!("[in-list-diag] CONTROL single-Eq 'id = 350': bloom_skips={} time={eq_ms:.2}ms", ae - be);

    // Does the multi-col IN-list even reach the fast path? Check EXPLAIN.
    if let Ok(ExecResult::Rows { batches, .. }) =
        sess.execute(&format!("EXPLAIN {q}")).await
    {
        use arrow_array::{Array, StringArray};
        for b in &batches {
            if let Ok(idx) = b.schema().index_of("QUERY PLAN") {
                if let Some(a) = b.column(idx).as_any().downcast_ref::<StringArray>() {
                    let plan: String = (0..a.len()).filter(|i| !a.is_null(*i)).map(|i| a.value(i)).collect::<Vec<_>>().join(" | ");
                    println!("[in-list-diag] IN-list EXPLAIN (first 300): {}", &plan.chars().take(300).collect::<String>());
                }
            }
        }
    }

    bg.shutdown().await;
    wal.close().await.unwrap();
}
