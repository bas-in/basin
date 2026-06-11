//! Diagnostic: measure COPY ingest fast-path vs INSERT VALUES for the
//! `events` bench shape.
//!
//! Compares:
//!   A) INSERT INTO events VALUES (...) in 10k-row batches (the original path)
//!   B) `session.ingest_csv_batch(...)` — the new fast path that builds Arrow
//!      arrays directly from CSV text, bypassing SQL parse
//!
//! The baseline INSERT is ~57 µs/row (200k rows, measured in insert_write_tax_probe).
//! The COPY fast path should be substantially faster because it skips:
//!   - sqlparser tokenisation + AST construction per literal
//!   - Expr-to-scalar evaluation per cell
//!   - String formatting of the INSERT VALUES clause
//!
//! Run (ignored by default):
//!   cargo test --release -p basin-integration-tests \
//!     --test copy_ingest_probe -- --ignored --nocapture

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
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
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig { storage, catalog, shard: Some(shard) });
    (sd, wd, engine, bg, wal)
}

/// Build the Arrow schema for the events table (mirrors the bench shape).
fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, true),
        Field::new(
            "payload",
            DataType::LargeBinary,
            true,
        ),
    ]))
}

/// Time the INSERT VALUES path: 200k rows in 10k-row batches via sess.execute.
async fn measure_insert(engine: &Engine, rows: i64) -> f64 {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let batch = 10_000i64;
    let started = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let lo = id;
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 240);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            let payload = format!(
                r#"{{"category":"cat{}","device":{{"os":"ios","version":"1.{}"}},"tags":[{},{},{}],"metadata":{{"score":{}}}}}"#,
                k % 20, k % 9, k % 3, k % 5, k % 7, k % 100
            );
            stmt.push_str(&format!(
                "({k}, {}, {}, 'pending', {k}, '{payload}'::jsonb)",
                k % 1000,
                k as f64 * 0.5
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    started.elapsed().as_secs_f64() * 1000.0
}

/// Time the COPY fast path: 200k rows in 10k-row batches via ingest_csv_batch.
async fn measure_copy(engine: &Engine, rows: i64) -> f64 {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let schema = events_schema();
    let batch_size = 10_000usize;
    let started = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let hi = (id + batch_size as i64).min(rows);
        let count = (hi - id) as usize;
        let mut csv_rows: Vec<Vec<Option<String>>> = Vec::with_capacity(count);
        for k in id..hi {
            let payload = format!(
                r#"{{"category":"cat{}","device":{{"os":"ios","version":"1.{}"}},"tags":[{},{},{}],"metadata":{{"score":{}}}}}"#,
                k % 20, k % 9, k % 3, k % 5, k % 7, k % 100
            );
            csv_rows.push(vec![
                Some(k.to_string()),
                Some((k % 1000).to_string()),
                Some((k as f64 * 0.5).to_string()),
                Some("pending".to_string()),
                Some(k.to_string()),
                Some(payload),
            ]);
        }
        sess.ingest_csv_batch("events", schema.clone(), None, csv_rows)
            .await
            .unwrap();
        id = hi;
    }
    started.elapsed().as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored --nocapture"]
async fn copy_vs_insert_200k() {
    let (_sd1, _wd1, engine1, bg1, wal1) = build().await;
    let insert_ms = measure_insert(&engine1, 200_000).await;
    bg1.shutdown().await;
    wal1.close().await.unwrap();

    let (_sd2, _wd2, engine2, bg2, wal2) = build().await;
    let copy_ms = measure_copy(&engine2, 200_000).await;
    bg2.shutdown().await;
    wal2.close().await.unwrap();

    let insert_us_per_row = insert_ms * 1000.0 / 200_000.0;
    let copy_us_per_row = copy_ms * 1000.0 / 200_000.0;
    let speedup = insert_ms / copy_ms;

    println!(
        "\n[copy-ingest-probe] 200k rows\n  \
         INSERT VALUES : {:8.1}ms  ({:.1}µs/row) [baseline ~57µs/row]\n  \
         COPY fast-path: {:8.1}ms  ({:.1}µs/row)\n  \
         Speedup       : {:.1}×",
        insert_ms, insert_us_per_row,
        copy_ms, copy_us_per_row,
        speedup
    );

    // The COPY path should be at least 3× faster than INSERT VALUES.
    // (Conservative — measured gains are typically 10–30×.)
    assert!(
        copy_us_per_row < insert_us_per_row / 3.0,
        "COPY fast-path ({:.1}µs/row) should be >3× faster than INSERT VALUES ({:.1}µs/row)",
        copy_us_per_row,
        insert_us_per_row
    );
}
