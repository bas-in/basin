//! Correctness guard for the `TombstoneFilterExec` transparent filter-pushdown
//! (the JSONB-at-1M fix). The exec lets DataFusion push a selective predicate
//! THROUGH it down into the cold Vortex/Parquet scan and REMOVE the redundant
//! `FilterExec` above — valid only because tombstone suppression commutes with
//! a row filter. This test pins the invariant: a row that is BOTH tombstoned
//! AND matches the pushed-down predicate must NOT leak into the result.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn build() -> (TempDir, TempDir, Engine, Shard, basin_shard::ShardBackgroundHandle, Arc<dyn Wal>) {
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
    let engine = Engine::new(EngineConfig { storage, catalog, shard: Some(shard.clone()) });
    (sd, wd, engine, shard, bg, wal)
}

async fn ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        out.push(col.value(i));
                    }
                }
            }
            out.sort_unstable();
            out
        }
        _ => vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selective_filter_does_not_leak_tombstoned_row() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    // Default format is Vortex (matches the bench card that exposed the bug).
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // Seed 0..1000 and flush so all rows live in a cold Vortex file.
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..1000i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{})", k * 10));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Fast-path DELETE of a row INSIDE the predicate range (id < 100) — this
    // registers a hot-tier tombstone, so the read goes through
    // TombstoneFilteringTable / TombstoneFilterExec.
    sess.execute("DELETE FROM t WHERE id = 42").await.unwrap();
    // And one OUTSIDE the range, to be thorough.
    sess.execute("DELETE FROM t WHERE id = 500").await.unwrap();

    // The selective predicate is pushed THROUGH TombstoneFilterExec into the
    // cold scan and the FilterExec is removed. id=42 matches id<100 in the
    // cold scan but must be suppressed by the tombstone filter.
    let got = ids(&sess, "SELECT id FROM t WHERE id < 100 ORDER BY id").await;
    let expected: Vec<i64> = (0..100).filter(|&k| k != 42).collect();
    assert_eq!(got, expected, "id=42 (tombstoned, id<100) must not leak");

    // Sanity: full-table read also excludes both tombstones.
    let all = ids(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(all.len(), 998, "two rows deleted -> 998 remain");
    assert!(!all.contains(&42) && !all.contains(&500), "both tombstones suppressed");

    // Range that excludes the tombstone: id BETWEEN 200 AND 300 -> untouched.
    let mid = ids(&sess, "SELECT id FROM t WHERE id >= 200 AND id < 300 ORDER BY id").await;
    assert_eq!(mid, (200..300).collect::<Vec<_>>(), "unrelated range intact");

    bg.shutdown().await;
    wal.close().await.unwrap();
}
