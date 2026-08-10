//! Security regression — per-project resource partitions (noisy neighbour P0).
//!
//! These pin the three cross-project isolation fixes that came out of the
//! isolation audit. Each fix bounds a shared, process-global resource so that
//! one project saturating it cannot drain a sibling below its guaranteed floor:
//!
//!   1. **GIN posting budget** is partitioned per project with a floor: project
//!      A flooding JSONB postings must not evict project B's postings while B is
//!      under its floor (`gin_flood_does_not_evict_other_projects_postings`).
//!   2. **Overlay reconciler** is round-robin across projects with a per-project
//!      work quantum: project A parking a huge overlay backlog must not starve
//!      project B's reconciliation within a tick
//!      (`overlay_reconciler_round_robin_drains_small_project_under_flood`).
//!
//! The PkRowCache per-project waterline + share cap is covered by deterministic
//! unit tests in `basin-engine/src/pk_row_cache.rs` (the cache is not reachable
//! from an integration binary; its eviction logic is exercised there with
//! explicit, tiny budgets — see `flooding_project_does_not_evict_protected_
//! project` and `single_project_capped_at_its_share`).
//!
//! Determinism: the GIN budget/floor are read once per process via `OnceLock`,
//! so this file sets them BEFORE any GIN posting work and keeps every other
//! sec_* GIN test out of this binary. Budgets are tiny so the test is fast and
//! the eviction boundary is crossed deterministically.

#![allow(clippy::print_stdout)]

use arrow_array::Array;
use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Serialises env mutation across tests in this binary (the GIN budget knobs
/// are process-global). Mirrors the convention in `delta_update_reconcile.rs`.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[allow(clippy::type_complexity)]
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
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

// ---------------------------------------------------------------------------
// 1. GIN posting budget — per-project partition with a floor.
//
// Project A floods one column with high-cardinality (near-unique) JSONB values
// so its posting list crosses the budget and evicts. Project B indexes a small,
// stable set that stays under its floor. After A's flood, B's postings must
// still be present and B must NOT have suffered any eviction — A's pressure is
// confined to A's partition.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_flood_does_not_evict_other_projects_postings() {
    let _g = ENV_LOCK.lock().await;
    // Tiny, deterministic budget. Floor guarantees B's small index survives no
    // matter how hard A churns. These are read once per process via OnceLock;
    // this binary contains no other GIN posting test, so the values stick.
    std::env::set_var("BASIN_GIN_POSTING_BUDGET", "2000");
    std::env::set_var("BASIN_GIN_POSTING_FLOOR", "500");

    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let registry = eng.gin_index_registry_for_test();

    let pa = ProjectId::new();
    let pb = ProjectId::new();
    let table = TableName::new("docs").unwrap();
    let col = "payload";
    let opclass = "jsonb_ops";

    // Project B: a small, stable set of unique terms (well under the 500 floor).
    for i in 0..20i64 {
        let doc = serde_json::to_vec(&json!({ "owner": "b", "ref": format!("b-{i}") })).unwrap();
        registry.index_row(
            &pb,
            &table,
            col,
            opclass,
            &doc,
            "b_file.parquet",
            0,
            i as u64,
        );
        registry.mark_file_indexed(&pb, &table, col, "b_file.parquet");
    }
    let b_pairs_before = registry.project_pair_count(&pb);
    assert!(b_pairs_before > 0, "project B must hold postings");
    assert!(
        !registry.has_evicted(&pb, &table, col),
        "B must not have evicted before A floods"
    );

    // Project A: flood with thousands of near-unique key=value pairs, far past
    // its per-project partition, forcing repeated eviction on A's list.
    for i in 0..6000i64 {
        let doc = serde_json::to_vec(&json!({ "owner": "a", "uniq": format!("a-{i}") })).unwrap();
        registry.index_row(
            &pa,
            &table,
            col,
            opclass,
            &doc,
            "a_file.parquet",
            0,
            i as u64,
        );
    }
    assert!(
        registry.has_evicted(&pa, &table, col),
        "A's flood must have crossed its partition and evicted"
    );

    // THE property: B's postings are intact — A's eviction never touched B.
    assert!(
        !registry.has_evicted(&pb, &table, col),
        "SECURITY: project A's GIN flood evicted project B's postings — partition breach"
    );
    assert_eq!(
        registry.project_pair_count(&pb),
        b_pairs_before,
        "SECURITY: project B's posting-pair count changed under A's flood"
    );
    // B's file is still marked fully indexed (completeness guard intact), so B's
    // probes keep their file-level pruning — eviction never un-marked B's file.
    let b_indexed = registry.indexed_files_for(&pb, &table, col);
    assert!(
        b_indexed.contains("b_file.parquet"),
        "SECURITY: project B lost its file-completeness coverage under A's flood"
    );

    std::env::remove_var("BASIN_GIN_POSTING_BUDGET");
    std::env::remove_var("BASIN_GIN_POSTING_FLOOR");
}

// ---------------------------------------------------------------------------
// 2. Overlay reconciler — per-project round-robin under a flood.
//
// Project A parks a large overlay backlog across MANY tables; project B parks a
// small overlay on ONE table. With a tiny tick and a small per-project quantum,
// the round-robin scheduler must drain B's overlay within a few cycles even
// while A's much larger backlog is still draining — B is not starved.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlay_reconciler_round_robin_drains_small_project_under_flood() {
    let _g = ENV_LOCK.lock().await;
    // 1s tick, bytes threshold 1 (every non-empty overlay is "due"), small
    // per-project quantum so A's many tables span multiple rounds. Read at
    // Engine::new below.
    std::env::set_var("BASIN_OVERLAY_RECONCILE_SECS", "1");
    std::env::set_var("BASIN_OVERLAY_RECONCILE_BYTES", "1");
    std::env::set_var("BASIN_OVERLAY_RECONCILE_QUANTUM", "1");

    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let pa = ProjectId::new();
    let pb = ProjectId::new();
    let sa = eng.open_session(pa).await.unwrap();
    let sb = eng.open_session(pb).await.unwrap();

    // Project A: many tables, each carrying a delta-UPDATE overlay (a large,
    // multi-table backlog the reconciler must work through).
    const A_TABLES: usize = 12;
    for t in 0..A_TABLES {
        let name = format!("a_t{t}");
        sa.execute(&format!(
            "CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap();
        sa.execute(&format!("INSERT INTO {name} VALUES (1, 1), (2, 2), (3, 3)"))
            .await
            .unwrap();
        // A conditional UPDATE parks an overlay (delta-update fast path).
        sa.execute(&format!("UPDATE {name} SET v = v + 100 WHERE id >= 1"))
            .await
            .unwrap();
    }

    // Project B: one small table with one overlay.
    sb.execute("CREATE TABLE b_small (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    sb.execute("INSERT INTO b_small VALUES (1, 1), (2, 2)")
        .await
        .unwrap();
    sb.execute("UPDATE b_small SET v = v + 100 WHERE id >= 1")
        .await
        .unwrap();

    // Poll: B's overlay must drain within a bounded number of 1s ticks. Without
    // round-robin, A's 12-table backlog (quantum 1 → up to 12 tables ahead of B
    // per round under a naive sequential pass) could push B's first materialize
    // out; with round-robin B gets a turn every round.
    let registry = eng.memtable_registry();
    let b_table = TableName::new("b_small").unwrap();
    let mut drained = false;
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(750)).await;
        // Read B's values — must always be the post-UPDATE values (read-your-
        // writes holds whether served from overlay or materialized cold).
        // b_small seeded (1,1),(2,2); UPDATE +100 -> (1,101),(2,102); sum=203.
        let v_sum = sum_v(&sb, "SELECT v FROM b_small").await;
        assert_eq!(
            v_sum, 203,
            "SECURITY: B's read-your-writes broke under the reconciler flood"
        );
        if let Some(entry) = registry.get(&pb, &b_table) {
            let pending = entry.memtable.update_count() + entry.memtable.tombstone_count();
            if pending == 0 {
                drained = true;
                break;
            }
        } else {
            drained = true;
            break;
        }
    }
    assert!(
        drained,
        "SECURITY: project B's overlay never reconciled under project A's flood — \
         round-robin starvation"
    );

    std::env::remove_var("BASIN_OVERLAY_RECONCILE_SECS");
    std::env::remove_var("BASIN_OVERLAY_RECONCILE_BYTES");
    std::env::remove_var("BASIN_OVERLAY_RECONCILE_QUANTUM");
}

/// Sum the (single) Int64 `v` column returned by `sql`.
async fn sum_v(sess: &ProjectSession, sql: &str) -> i64 {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let mut total = 0i64;
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("v is Int64");
        for i in 0..col.len() {
            if !col.is_null(i) {
                total += col.value(i);
            }
        }
    }
    total
}
