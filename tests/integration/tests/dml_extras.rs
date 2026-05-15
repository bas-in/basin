//! DML-category coverage for features added in the "+5 DML" milestone:
//!
//! 1. INSERT … ON CONFLICT (col) DO UPDATE SET …
//! 2. UPDATE t SET … WHERE col IN (SELECT id FROM u)
//! 3. DELETE FROM t WHERE col IN (SELECT id FROM u)
//! 4. INSERT INTO t DEFAULT VALUES
//! 5. INSERT INTO t VALUES (…) RETURNING *

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use arrow_array::RecordBatch;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness ────────────────────────────────────────────────────────────────

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

async fn session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

#[allow(dead_code)]
fn ok(r: ExecResult) -> ExecResult { r }

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

// ─── 1. INSERT … ON CONFLICT (col) DO UPDATE SET … ─────────────────────────

#[tokio::test]
async fn on_conflict_do_update_inserts_on_no_conflict() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await.unwrap();

    // First upsert — no conflict, should insert.
    let res = sess.execute(
        "INSERT INTO kv (k, v) VALUES (1, 'hello') ON CONFLICT (k) DO UPDATE SET v = 'updated'"
    ).await.unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("INSERT")),
        "expected INSERT tag, got {res:?}");

    // Verify the row is there.
    let ExecResult::Rows { batches, .. } = sess.execute("SELECT v FROM kv WHERE k = 1").await.unwrap()
        else { panic!("expected rows") };
    assert_eq!(col_string(&batches, "v"), vec!["hello"]);
}

#[tokio::test]
async fn on_conflict_do_update_updates_on_conflict() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await.unwrap();
    sess.execute("INSERT INTO kv (k, v) VALUES (1, 'hello')").await.unwrap();

    // Second upsert — conflict on k=1, should UPDATE.
    let res = sess.execute(
        "INSERT INTO kv (k, v) VALUES (1, 'world') ON CONFLICT (k) DO UPDATE SET v = 'world'"
    ).await.unwrap();
    // The conflict path runs UPDATE which returns an "UPDATE N" tag.
    match res {
        ExecResult::Empty { ref tag } => {
            assert!(
                tag.starts_with("UPDATE") || tag.starts_with("INSERT"),
                "expected UPDATE or INSERT tag, got {tag}"
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }

    // Verify the row was updated.
    let ExecResult::Rows { batches, .. } = sess.execute("SELECT v FROM kv WHERE k = 1").await.unwrap()
        else { panic!("expected rows") };
    let vals = col_string(&batches, "v");
    assert_eq!(vals, vec!["world"], "value should have been updated to 'world'");
}

// ─── 2. UPDATE … WHERE col IN (SELECT …) ────────────────────────────────────

#[tokio::test]
async fn update_where_in_subquery() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await.unwrap();
    sess.execute("CREATE TABLE skip_ids (sid BIGINT NOT NULL)")
        .await.unwrap();

    // Insert some items.
    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await.unwrap();
    // IDs to update.
    sess.execute("INSERT INTO skip_ids (sid) VALUES (1), (3)")
        .await.unwrap();

    // UPDATE using IN (SELECT ...).
    let res = sess.execute(
        "UPDATE items SET name = 'updated' WHERE id IN (SELECT sid FROM skip_ids)"
    ).await.unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("UPDATE")),
        "expected UPDATE tag, got {res:?}");

    // Check that rows 1 and 3 were updated but row 2 was not.
    let ExecResult::Rows { batches, .. } = sess.execute("SELECT id, name FROM items ORDER BY id")
        .await.unwrap() else { panic!("expected rows") };
    let ids = col_i64(&batches, "id");
    let names = col_string(&batches, "name");
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(names, vec!["updated", "b", "updated"]);
}

// ─── 3. DELETE … WHERE col IN (SELECT …) ────────────────────────────────────

#[tokio::test]
async fn delete_where_in_subquery() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await.unwrap();
    sess.execute("CREATE TABLE del_ids (did BIGINT NOT NULL)")
        .await.unwrap();

    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await.unwrap();
    sess.execute("INSERT INTO del_ids (did) VALUES (1), (3)")
        .await.unwrap();

    // DELETE using IN (SELECT ...).
    let res = sess.execute(
        "DELETE FROM items WHERE id IN (SELECT did FROM del_ids)"
    ).await.unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("DELETE")),
        "expected DELETE tag, got {res:?}");

    // Only row 2 should remain.
    let ExecResult::Rows { batches, .. } = sess.execute("SELECT id FROM items").await.unwrap()
        else { panic!("expected rows") };
    assert_eq!(col_i64(&batches, "id"), vec![2]);
}

// ─── 4. INSERT INTO t DEFAULT VALUES ────────────────────────────────────────

#[tokio::test]
async fn insert_default_values() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    // Table with nullable columns (all default to NULL).
    sess.execute("CREATE TABLE defaults_test (a BIGINT, b TEXT)")
        .await.unwrap();

    let res = sess.execute("INSERT INTO defaults_test DEFAULT VALUES")
        .await.unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("INSERT")),
        "expected INSERT tag, got {res:?}");

    let ExecResult::Rows { batches, .. } = sess.execute("SELECT a, b FROM defaults_test")
        .await.unwrap() else { panic!("expected rows") };
    assert_eq!(rows(&batches), 1, "should have exactly one row");
    // Both columns should be NULL.
    let b = batches.first().unwrap();
    assert!(b.column(0).is_null(0), "column a should be NULL");
    assert!(b.column(1).is_null(0), "column b should be NULL");
}

// ─── 5. INSERT … RETURNING * ─────────────────────────────────────────────────

#[tokio::test]
async fn insert_returning_star() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE ret_test (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await.unwrap();

    let res = sess.execute(
        "INSERT INTO ret_test (id, name) VALUES (42, 'answer') RETURNING *"
    ).await.unwrap();

    let ExecResult::Rows { batches, .. } = res
        else { panic!("expected Rows from INSERT RETURNING *") };

    assert_eq!(rows(&batches), 1, "RETURNING should return the inserted row");
    let ids = col_i64(&batches, "id");
    let names = col_string(&batches, "name");
    assert_eq!(ids, vec![42]);
    assert_eq!(names, vec!["answer"]);
}

#[tokio::test]
async fn insert_returning_star_multiple_rows() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi_ret (id BIGINT NOT NULL, val TEXT NOT NULL)")
        .await.unwrap();

    let res = sess.execute(
        "INSERT INTO multi_ret (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three') RETURNING *"
    ).await.unwrap();

    let ExecResult::Rows { batches, .. } = res
        else { panic!("expected Rows from INSERT RETURNING *") };

    assert_eq!(rows(&batches), 3, "RETURNING should return all 3 inserted rows");
    let mut ids = col_i64(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}
