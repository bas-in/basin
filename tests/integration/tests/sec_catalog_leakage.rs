//! Security regression — catalog / data-at-rest leakage.
//!
//! Scenarios:
//!
//! 1. `dropped_table_is_unreachable_on_new_session` — after `DROP TABLE`,
//!    a *fresh* session of the same project must not be able to query
//!    the table via cached metadata.
//! 2. `dropped_table_information_schema_is_gone` — the dropped table
//!    disappears from `information_schema.tables`.
//! 3. `delete_project_removes_physical_files` — `Storage::delete_project`
//!    purges the project prefix from object_store; subsequent LIST
//!    returns zero objects for that project. (Bytes-after-delete is a
//!    P0 disclosure risk if missed.)
//! 4. `parallel_namespace_isolation_after_delete` — after deleting
//!    project A, project B's data is untouched.
//! 5. `project_namespace_reuse_does_not_resurrect_data` — creating a
//!    *new* project with the same ULID-string after delete (synthetic
//!    test; ProjectId::new() collisions are astronomically unlikely)
//!    sees no data — the storage layer keys on `(project, ...)` so a
//!    reused id would NOT inherit deleted bytes.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

#[allow(clippy::type_complexity)]
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Arc<dyn Catalog>,
    Storage,
    Arc<dyn ObjectStore>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
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
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, catalog, storage, object_store)
}

fn row_count(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// 1. After DROP, a FRESH session can't query the table.
//
// SECURITY P1 (sec_catalog_leakage): when rows live ONLY in the hot tier
// (no `flush_to_parquet` between INSERT and DROP), `DROP TABLE` must purge
// the shard's in-memory tail AND the hot-tier MemTableRegistry entry for
// `(project, table)`. Without it, the catalog row is removed but the
// in-RAM overlay survives the session boundary — a fresh session of the
// same project can still `SELECT * FROM <dropped_table>` and retrieve
// every memtable row. Mitigation: `exec_drop_table` calls
// `shard.drop_table(project, table)` and `MemTableRegistry::remove(...)`
// after the catalog drop succeeds.
//
// Test asserts STRICTLY: a fresh session must NOT see any rows from the
// dropped table — either an error (table not found) or an empty result
// is acceptable; a non-zero row count is the leak this test pins.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_is_unreachable_on_new_session() {
    let (_sd, _wd, eng, _sh, _bg, _wal, _cat, _st, _os) = build().await;
    let p = ProjectId::new();

    // First session: create, drop.
    let s1 = eng.open_session(p).await.unwrap();
    s1.execute("CREATE TABLE ephemeral (id BIGINT)")
        .await
        .unwrap();
    s1.execute("INSERT INTO ephemeral VALUES (1), (2), (3)")
        .await
        .unwrap();
    s1.execute("DROP TABLE ephemeral").await.unwrap();
    drop(s1);

    // Fresh session: the dropped rows must NOT be reachable. Either an
    // error (table not found) or an empty result is acceptable; a
    // non-zero row count is the P1 leak.
    let s2 = eng.open_session(p).await.unwrap();
    let r = s2.execute("SELECT * FROM ephemeral").await;
    match r {
        Ok(res) => {
            let n = row_count(res);
            assert_eq!(
                n, 0,
                "SECURITY P1: SELECT from dropped table returned {n} rows on a fresh session — \
                 hot-tier overlay survived DROP. exec_drop_table must purge the shard tail \
                 and MemTableRegistry entry for (project, table)."
            );
        }
        Err(_e) => {
            // Desired outcome: a typed error from the read path.
        }
    }
}

// ---------------------------------------------------------------------------
// 2. information_schema reflects the drop.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_information_schema_is_gone() {
    let (_sd, _wd, eng, _sh, _bg, _wal, _cat, _st, _os) = build().await;
    let p = ProjectId::new();
    let s = eng.open_session(p).await.unwrap();

    s.execute("CREATE TABLE doomed (id BIGINT)").await.unwrap();
    let before = s
        .execute(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_name = 'doomed'",
        )
        .await
        .unwrap();
    assert_eq!(row_count(before), 1, "doomed must appear before drop");

    s.execute("DROP TABLE doomed").await.unwrap();
    let after = s
        .execute(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_name = 'doomed'",
        )
        .await
        .unwrap();
    assert_eq!(
        row_count(after),
        0,
        "SECURITY: dropped table still surfaces in information_schema.tables"
    );
}

// ---------------------------------------------------------------------------
// 3. delete_project removes the project prefix from object_store.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_project_removes_physical_files() {
    use futures::StreamExt;
    use object_store::path::Path;

    let (_sd, _wd, eng, shard, _bg, _wal, catalog, storage, object_store) = build().await;
    let p = ProjectId::new();

    // Seed the project with data and flush to cold so physical files exist.
    let s = eng.open_session(p).await.unwrap();
    s.execute("CREATE TABLE keep (id BIGINT)").await.unwrap();
    s.execute("INSERT INTO keep VALUES (1), (2), (3)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();
    drop(s);

    // Confirm there ARE objects under projects/<p>/ before delete.
    let prefix_str = format!("projects/{p}/");
    let prefix = Path::from(prefix_str.as_str());
    let mut before_count = 0usize;
    let mut s = object_store.list(Some(&prefix));
    while let Some(_obj) = s.next().await {
        before_count += 1;
    }
    println!(
        "[sec_catalog_leakage] objects before delete: {before_count} under {prefix_str}"
    );
    // The exact count varies with the storage layout; just demand >0.
    assert!(
        before_count > 0,
        "test setup: project's data was never written to object_store"
    );

    // Delete the project. This must purge the prefix.
    let n_deleted = storage
        .delete_project(catalog.as_ref(), &p)
        .await
        .expect("delete_project ok");
    println!("[sec_catalog_leakage] delete_project deleted {n_deleted} objects");

    // After delete: zero objects under the prefix.
    let mut after_count = 0usize;
    let mut s = object_store.list(Some(&prefix));
    while let Some(_obj) = s.next().await {
        after_count += 1;
    }
    assert_eq!(
        after_count, 0,
        "SECURITY P0: delete_project left {after_count} objects under {prefix_str} — \
         data-at-rest leakage after tenant deletion"
    );
}

// ---------------------------------------------------------------------------
// 4. Deleting project A does NOT touch project B's data.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_namespace_isolation_after_delete() {
    let (_sd, _wd, eng, shard, _bg, _wal, catalog, storage, _os) = build().await;
    let pa = ProjectId::new();
    let pb = ProjectId::new();

    let sa = eng.open_session(pa).await.unwrap();
    let sb = eng.open_session(pb).await.unwrap();
    sa.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    sa.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();
    sb.execute("CREATE TABLE u (id BIGINT)").await.unwrap();
    sb.execute("INSERT INTO u VALUES (10), (20), (30)")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();
    drop(sa);
    drop(sb);

    // Delete project A.
    storage
        .delete_project(catalog.as_ref(), &pa)
        .await
        .expect("delete A");

    // Project B's data must be intact.
    let sb = eng.open_session(pb).await.unwrap();
    let n = sb.execute("SELECT count(*) FROM u").await.unwrap();
    match n {
        ExecResult::Rows { batches, .. } => {
            let v = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(
                v, 3,
                "SECURITY P0: B's count(*) returned {v} after deleting A; \
                 cross-project blast radius"
            );
        }
        _ => panic!("expected rows"),
    }
}

// ---------------------------------------------------------------------------
// 5. Reused namespace id sees no data — the post-delete contract.
//
// We can't replay a ProjectId::new() collision (the ULIDs are random); we
// instead drive the post-delete-then-new-session shape: after delete A,
// opening a new session for A and creating a same-named table must not
// surface the old rows.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_namespace_reuse_does_not_resurrect_data() {
    let (_sd, _wd, eng, shard, _bg, _wal, catalog, storage, _os) = build().await;
    let p = ProjectId::new();

    let s1 = eng.open_session(p).await.unwrap();
    s1.execute("CREATE TABLE relic (id BIGINT, payload TEXT)")
        .await
        .unwrap();
    s1.execute("INSERT INTO relic VALUES (1, 'pre-delete-data')")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();
    drop(s1);

    // Delete the whole project's data and the catalog namespace.
    storage
        .delete_project(catalog.as_ref(), &p)
        .await
        .expect("delete project");

    // Re-create the namespace (some catalogs require explicit re-create
    // after `drop_namespace`; InMemoryCatalog auto-creates on first table).
    let _ = catalog.create_namespace(&p).await;

    // Open a fresh session and re-create a same-named table. The old row
    // must NOT resurface.
    let s2 = eng.open_session(p).await.unwrap();
    s2.execute("CREATE TABLE relic (id BIGINT, payload TEXT)")
        .await
        .unwrap();
    let r = s2.execute("SELECT payload FROM relic").await.unwrap();
    assert_eq!(
        row_count(r),
        0,
        "SECURITY P0: post-delete same-name table resurfaced pre-delete rows"
    );
}
