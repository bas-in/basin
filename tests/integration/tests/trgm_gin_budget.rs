//! Budget-pressure eviction for the trigram GIN index (`gin_trgm_ops`).
//!
//! `BASIN_GIN_POSTING_BUDGET` is read ONCE per process via `OnceLock`, so a
//! tiny budget set here would leak into any other GIN-posting test sharing the
//! binary. This file therefore contains EXACTLY ONE test so the env value
//! sticks (same discipline as `sec_resource_bounds_per_project.rs`).
//!
//! The invariant: under a deliberately tiny budget, backfill eviction fires and
//! the trigram index degrades to PARTIAL per-file coverage. Evicted files become
//! forced candidates (full scan) and a file whose row tier loses any block falls
//! back to coarse decode — so the `%` result MUST stay identical to a no-index
//! full-scan oracle. Eviction is a pure accelerator loss, never a dropped match.

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn ids_for(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let mut ids: Vec<i64> = Vec::new();
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 ids from {sql:?}"));
        for r in 0..col.len() {
            ids.push(col.value(r));
        }
    }
    ids.sort_unstable();
    ids
}

fn name_for(i: i64) -> String {
    const FIRST: &[&str] = &[
        "alice", "alyce", "bob", "carol", "dave", "erin", "frank", "grace",
    ];
    const LAST: &[&str] = &["smith", "smyth", "jones", "brown", "taylor"];
    let first = FIRST[(i as usize) % FIRST.len()];
    let last = LAST[((i as usize) / FIRST.len()) % LAST.len()];
    if i % 13 == 0 {
        format!("zephyrine {first} {last}")
    } else {
        format!("{first} {last}")
    }
}

const ROWS: i64 = 260;

async fn seed(dir: &TempDir, with_index: bool) -> (Engine, ProjectSession, ProjectId) {
    let eng = engine_in(dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    exec(&sess, "CREATE TABLE people (id bigint, name text)").await;
    let batch = 40i64;
    let mut off = 0i64;
    while off < ROWS {
        let hi = (off + batch).min(ROWS);
        let mut vals: Vec<String> = Vec::new();
        for i in off..hi {
            let nm = name_for(i).replace('\'', "''");
            vals.push(format!("({i}, '{nm}')"));
        }
        exec(
            &sess,
            &format!("INSERT INTO people (id, name) VALUES {}", vals.join(", ")),
        )
        .await;
        off = hi;
    }
    if with_index {
        exec(
            &sess,
            "CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)",
        )
        .await;
    }
    (eng, sess, project)
}

async fn oracle_ids(query_tail: &str) -> Vec<i64> {
    let dir = TempDir::new().unwrap();
    let (_eng, sess, _p) = seed(&dir, false).await;
    ids_for(&sess, &format!("SELECT id FROM people WHERE {query_tail}")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_pressure_eviction_matches_oracle() {
    // Tiny budget read once per process via OnceLock; this binary contains no
    // other GIN posting test, so the value sticks.
    std::env::set_var("BASIN_GIN_POSTING_BUDGET", "8");

    let dir = TempDir::new().unwrap();
    let (eng, sess, project) = seed(&dir, true).await;
    let table = TableName::new("people").unwrap();
    let reg = eng.gin_index_registry_for_test();
    // ~260 rows of trigrams under an 8-pair budget — eviction must fire.
    assert!(
        reg.has_evicted(&project, &table, "name"),
        "tiny budget must trigger posting-list eviction"
    );

    for tail in [
        "name % 'smith'",
        "name % 'zephyrine'",
        "name % 'alyce smyth'",
    ] {
        let got = ids_for(&sess, &format!("SELECT id FROM people WHERE {tail}")).await;
        let oracle = oracle_ids(tail).await;
        assert_eq!(
            got, oracle,
            "budget-evicted `{tail}` must equal full-scan oracle"
        );
    }
}
