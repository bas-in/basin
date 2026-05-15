//! Cursor lifecycle integration test.
//!
//! Exercises DECLARE / FETCH (all directions) / MOVE / CLOSE through the
//! engine's public `ProjectSession::execute` API so the full executor dispatch
//! path is covered.
//!
//! All cursors in Basin v0.1 are fully materialised at DECLARE time
//! (in-memory, no streaming).  The tests here verify:
//!
//! 1. `DECLARE c CURSOR FOR SELECT …` materialises the result and registers
//!    the cursor under name `c`.
//! 2. `FETCH 5 FROM c` returns the next 5 rows and advances the position.
//! 3. `FETCH NEXT FROM c`, `FETCH ALL FROM c` — direction variants.
//! 4. `FETCH PRIOR FROM c`, `FETCH FIRST FROM c`, `FETCH LAST FROM c` —
//!    backward / positional directions.
//! 5. `MOVE 3 FROM c` — advances the cursor without returning rows.
//! 6. `CLOSE c` — removes the cursor from the registry; a subsequent FETCH
//!    must fail.
//! 7. `DECLARE c SCROLL CURSOR FOR …` — SCROLL keyword accepted.

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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

/// Count the total number of rows across all Arrow batches in an ExecResult.
fn row_count(result: &ExecResult) -> usize {
    match result {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Assert that `result` is `ExecResult::Empty` with the given command tag.
fn assert_empty(result: &ExecResult, expected_tag: &str) {
    match result {
        ExecResult::Empty { tag } => {
            assert_eq!(
                tag.as_str(),
                expected_tag,
                "expected tag {expected_tag:?}, got {tag:?}"
            );
        }
        ExecResult::Rows { batches, .. } => {
            panic!(
                "expected Empty {{ tag: {expected_tag:?} }} but got {} rows",
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
        }
    }
}

#[tokio::test]
async fn cursor_declare_fetch_close() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Create a table and insert 20 rows.
    sess.execute("CREATE TABLE nums (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let mut sql = String::from("INSERT INTO nums VALUES ");
    for i in 0..20usize {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    sess.execute(&sql).await.unwrap();

    // ---- DECLARE ----
    let res = sess
        .execute("DECLARE c CURSOR FOR SELECT id FROM nums ORDER BY id")
        .await
        .unwrap();
    assert_empty(&res, "DECLARE");

    // ---- FETCH 5 ----
    let res = sess.execute("FETCH 5 FROM c").await.unwrap();
    assert_eq!(row_count(&res), 5, "FETCH 5 should return 5 rows");

    // ---- FETCH NEXT ----
    let res = sess.execute("FETCH NEXT FROM c").await.unwrap();
    assert_eq!(row_count(&res), 1, "FETCH NEXT should return 1 row");

    // ---- FETCH ALL ----
    let res = sess.execute("FETCH ALL FROM c").await.unwrap();
    // 20 total rows, 6 consumed above → 14 remaining.
    assert_eq!(row_count(&res), 14, "FETCH ALL should return remaining 14 rows");

    // ---- CLOSE ----
    let res = sess.execute("CLOSE c").await.unwrap();
    assert_empty(&res, "CLOSE");

    // After CLOSE, any FETCH must fail.
    let err = sess.execute("FETCH NEXT FROM c").await;
    assert!(err.is_err(), "FETCH on closed cursor should error");
}

#[tokio::test]
async fn cursor_fetch_directions() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut sql = String::from("INSERT INTO items VALUES ");
    for i in 0..10usize {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    sess.execute(&sql).await.unwrap();

    sess.execute("DECLARE cur CURSOR FOR SELECT id FROM items ORDER BY id")
        .await
        .unwrap();

    // FETCH FIRST — should get 1 row (row 0), cursor now at position 1.
    let res = sess.execute("FETCH FIRST FROM cur").await.unwrap();
    assert_eq!(row_count(&res), 1, "FETCH FIRST");

    // FETCH LAST — jumps to last row (row 9), returns 1.
    let res = sess.execute("FETCH LAST FROM cur").await.unwrap();
    assert_eq!(row_count(&res), 1, "FETCH LAST");

    // FETCH PRIOR — one row before current position.
    let res = sess.execute("FETCH PRIOR FROM cur").await.unwrap();
    assert_eq!(row_count(&res), 1, "FETCH PRIOR");

    sess.execute("CLOSE cur").await.unwrap();
}

#[tokio::test]
async fn cursor_move_advances_position() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE seq (n BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut sql = String::from("INSERT INTO seq VALUES ");
    for i in 0..10usize {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    sess.execute(&sql).await.unwrap();

    sess.execute("DECLARE m CURSOR FOR SELECT n FROM seq ORDER BY n")
        .await
        .unwrap();

    // MOVE 3 — advance 3 rows without returning them.
    let res = sess.execute("MOVE 3 FROM m").await.unwrap();
    assert_empty(&res, "MOVE");

    // Now FETCH NEXT should return row 3 (0-indexed), the 4th row.
    let res = sess.execute("FETCH NEXT FROM m").await.unwrap();
    assert_eq!(row_count(&res), 1, "FETCH NEXT after MOVE 3");

    // Verify the returned value is 3 (0-indexed row 3).
    if let ExecResult::Rows { batches, .. } = &res {
        use arrow_array::Int64Array;
        let batch = &batches[0];
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        assert_eq!(col.value(0), 3, "row 3 should have value 3");
    }

    sess.execute("CLOSE m").await.unwrap();
}

#[tokio::test]
async fn cursor_scroll_keyword_accepted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t2 (x BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t2 VALUES (1),(2),(3)").await.unwrap();

    // SCROLL keyword must be accepted without error.
    let res = sess
        .execute("DECLARE sc SCROLL CURSOR FOR SELECT x FROM t2 ORDER BY x")
        .await
        .unwrap();
    assert_empty(&res, "DECLARE");

    let res = sess.execute("FETCH ALL FROM sc").await.unwrap();
    assert_eq!(row_count(&res), 3, "SCROLL cursor should return all rows");

    sess.execute("CLOSE sc").await.unwrap();
}
