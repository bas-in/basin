//! Integration tests for plain views (CREATE/DROP VIEW) and join-form DML
//! (DELETE … USING, UPDATE … FROM).
//!
//! CAPABILITIES.md rows exercised:
//! - CREATE VIEW / CREATE OR REPLACE VIEW
//! - DROP VIEW [IF EXISTS]
//! - CREATE TEMP VIEW (accepted, same impl as plain view in v0.1)
//! - SELECT from a view (query-rewrite inline subquery)
//! - DELETE FROM t USING u WHERE …
//! - UPDATE t SET col = v FROM u WHERE …
//! - INSERT INTO t SELECT … FROM other JOIN third ON … (already works via DataFusion)

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Extract all i64 values from a named column across all batches.
fn col_i64(result: &ExecResult, col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    if let ExecResult::Rows { batches, .. } = result {
        for b in batches {
            if let Some(arr) = b.column_by_name(col) {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("column {col} is not Int64"));
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
        }
    }
    out
}

/// Extract all string values from a named column across all batches.
fn col_str(result: &ExecResult, col: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let ExecResult::Rows { batches, .. } = result {
        for b in batches {
            if let Some(arr) = b.column_by_name(col) {
                let arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("column {col} is not StringArray"));
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            }
        }
    }
    out
}

/// Helper: count rows returned.
fn row_count(result: &ExecResult) -> usize {
    if let ExecResult::Rows { batches, .. } = result {
        batches.iter().map(|b| b.num_rows()).sum()
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// VIEW tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_create_and_select() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE employees (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, dept TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'eng'), (3, 'Carol', 'hr')")
        .await
        .unwrap();

    // Create a plain view.
    let res = sess
        .execute("CREATE VIEW eng_staff AS SELECT id, name FROM employees WHERE dept = 'eng'")
        .await
        .unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag == "CREATE VIEW"), "got: {res:?}");

    // SELECT from the view.
    let res = sess
        .execute("SELECT id, name FROM eng_staff ORDER BY id")
        .await
        .unwrap();
    let ids = col_i64(&res, "id");
    assert_eq!(ids, vec![1, 2], "view should expose only eng rows");
    let names = col_str(&res, "name");
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_create_or_replace() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE nums (n BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nums VALUES (1), (2), (3), (4)")
        .await
        .unwrap();

    sess.execute("CREATE VIEW big AS SELECT n FROM nums WHERE n > 2")
        .await
        .unwrap();

    // Initial: only 3, 4.
    let res = sess.execute("SELECT n FROM big ORDER BY n").await.unwrap();
    assert_eq!(col_i64(&res, "n"), vec![3, 4]);

    // Replace with a different predicate.
    sess.execute("CREATE OR REPLACE VIEW big AS SELECT n FROM nums WHERE n > 1")
        .await
        .unwrap();

    let res = sess.execute("SELECT n FROM big ORDER BY n").await.unwrap();
    assert_eq!(col_i64(&res, "n"), vec![2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_create_duplicate_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("CREATE VIEW v AS SELECT id FROM t")
        .await
        .unwrap();

    // Second CREATE without OR REPLACE should fail.
    let res = sess.execute("CREATE VIEW v AS SELECT id FROM t").await;
    assert!(res.is_err(), "duplicate view should be rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_drop_if_exists() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("CREATE VIEW v AS SELECT id FROM t")
        .await
        .unwrap();

    // Drop existing view.
    let res = sess.execute("DROP VIEW v").await.unwrap();
    assert!(matches!(res, ExecResult::Empty { ref tag } if tag == "DROP VIEW"), "got: {res:?}");

    // IF EXISTS on non-existent view → no error.
    let res = sess.execute("DROP VIEW IF EXISTS v").await.unwrap();
    assert!(matches!(res, ExecResult::Empty { .. }));

    // Without IF EXISTS → error.
    let res = sess.execute("DROP VIEW v").await;
    assert!(res.is_err(), "dropping non-existent view without IF EXISTS should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_temp_accepted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();

    // TEMPORARY / TEMP should be accepted.
    sess.execute("CREATE TEMP VIEW tv AS SELECT id, v FROM t")
        .await
        .unwrap();

    let res = sess.execute("SELECT v FROM tv").await.unwrap();
    let vals = col_str(&res, "v");
    assert_eq!(vals, vec!["hello"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_join_in_query() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE orders (oid BIGINT NOT NULL PRIMARY KEY, uid BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE users (uid BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 2, 200), (12, 1, 50)")
        .await
        .unwrap();

    // View joining two tables.
    sess.execute(
        "CREATE VIEW user_orders AS \
         SELECT o.oid, u.name, o.amount \
         FROM orders o JOIN users u ON o.uid = u.uid",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT oid, name, amount FROM user_orders ORDER BY oid")
        .await
        .unwrap();
    assert_eq!(row_count(&res), 3);
    let oids = col_i64(&res, "oid");
    assert_eq!(oids, vec![10, 11, 12]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_cascade_drop_ignored_v0() {
    // CASCADE and RESTRICT are accepted syntactically in v0.1 (no dependents tracked).
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("CREATE VIEW v AS SELECT id FROM t")
        .await
        .unwrap();

    // Both CASCADE and RESTRICT should succeed without error.
    sess.execute("DROP VIEW IF EXISTS v CASCADE").await.unwrap();
    sess.execute("CREATE VIEW v AS SELECT id FROM t")
        .await
        .unwrap();
    sess.execute("DROP VIEW IF EXISTS v RESTRICT").await.unwrap();
}

// ---------------------------------------------------------------------------
// Join-form DML tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_using_basic() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE cancelled (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (1, 'active'), (2, 'active'), (3, 'active')")
        .await
        .unwrap();
    sess.execute("INSERT INTO cancelled VALUES (2), (3)")
        .await
        .unwrap();

    // Delete orders that appear in cancelled.
    let res = sess
        .execute("DELETE FROM orders USING cancelled WHERE orders.id = cancelled.id")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { ref tag } => assert!(
            tag.starts_with("DELETE "),
            "expected DELETE tag, got {tag}"
        ),
        other => panic!("unexpected: {other:?}"),
    }

    // Only order 1 should remain.
    let remaining = sess
        .execute("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(col_i64(&remaining, "id"), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_using_no_match() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    sess.execute("INSERT INTO u VALUES (99)")
        .await
        .unwrap();

    let res = sess
        .execute("DELETE FROM t USING u WHERE t.id = u.id")
        .await
        .unwrap();
    // No rows matched → DELETE 0.
    match res {
        ExecResult::Empty { ref tag } => {
            assert!(tag.contains('0'), "expected DELETE 0, got {tag}")
        }
        other => panic!("unexpected: {other:?}"),
    }

    let remaining = sess
        .execute("SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(col_i64(&remaining, "id"), vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_from_basic() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE products (id BIGINT NOT NULL PRIMARY KEY, price BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute("CREATE TABLE price_updates (id BIGINT NOT NULL PRIMARY KEY, new_price BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO products VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();
    sess.execute("INSERT INTO price_updates VALUES (1, 150), (3, 350)")
        .await
        .unwrap();

    // Update products whose ids appear in price_updates.
    let res = sess
        .execute(
            "UPDATE products SET price = price_updates.new_price \
             FROM price_updates WHERE products.id = price_updates.id",
        )
        .await
        .unwrap();
    match res {
        ExecResult::Empty { ref tag } => assert!(
            tag.starts_with("UPDATE "),
            "expected UPDATE tag, got {tag}"
        ),
        other => panic!("unexpected: {other:?}"),
    }

    let after = sess
        .execute("SELECT id, price FROM products ORDER BY id")
        .await
        .unwrap();
    let ids = col_i64(&after, "id");
    let prices = col_i64(&after, "price");
    assert_eq!(ids, vec![1, 2, 3]);
    // Products 1 and 3 updated; product 2 untouched.
    assert_eq!(prices[0], 150, "product 1 price should be 150");
    assert_eq!(prices[1], 200, "product 2 price should be 200 (untouched)");
    assert_eq!(prices[2], 350, "product 3 price should be 350");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_select_join_works() {
    // INSERT INTO t SELECT … FROM a JOIN b ON … should work via the
    // existing exec_insert_select path (DataFusion handles the join).
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, extra BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, total BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    sess.execute("INSERT INTO b VALUES (1, 5), (2, 8)")
        .await
        .unwrap();

    // Insert into c using a join.
    let res = sess
        .execute("INSERT INTO c SELECT a.id, a.val + b.extra FROM a JOIN b ON a.id = b.id")
        .await
        .unwrap();
    assert!(matches!(res, ExecResult::Empty { .. }), "got: {res:?}");

    let after = sess
        .execute("SELECT id, total FROM c ORDER BY id")
        .await
        .unwrap();
    let ids = col_i64(&after, "id");
    let totals = col_i64(&after, "total");
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(totals, vec![15, 28]);
}
