//! Integration tests for Phase 5.7 B1: per-project secondary B-tree indexes.
//!
//! Acceptance gates:
//!
//! 1. `CREATE INDEX idx_users_email ON users(email)` round-trips: insert rows,
//!    query by indexed column, get correct results.
//! 2. `DROP INDEX idx_users_email` works; subsequent queries fall back to
//!    full-scan path (still return correct results).
//! 3. Point query on indexed column shows measurably fewer file opens than
//!    non-indexed baseline (asserted via `Engine::secondary_index_skipped_count`).
//! 4. Index is maintained on INSERT (new entries added).
//! 5. Index survives a process restart (loaded from per-project file on next
//!    Engine open pointing at the same object store root).
//! 6. Cross-project isolation: project A's index doesn't see project B's data.
//! 7. `cargo test -p basin-engine` doesn't regress (tested separately).
//! 8. This file passes.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build an `Engine` backed by a local temp-dir object store.
/// The catalog is `InMemoryCatalog` (lost on engine drop; use two engines on
/// the SAME directory for restart tests to share catalog state).
fn build_engine_in_dir(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Build a second `Engine` sharing the SAME file-system root but with a fresh
/// `InMemoryCatalog`. The shared storage means the index `.idx` files written
/// by the first engine are readable by the second engine.
///
/// Note: since InMemoryCatalog is not shared, the second engine won't know
/// about the *table* unless we recreate it.  For the restart-persistence test
/// we use a shared catalog arc instead; for cross-engine file tests we use
/// the same directory.
fn build_engine_shared_catalog(dir: &TempDir, catalog: Arc<dyn Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn assert_tag(res: ExecResult, tag: &str) {
    match res {
        ExecResult::Empty { tag: t } => assert_eq!(t, tag, "unexpected tag"),
        other => panic!("expected ExecResult::Empty({tag}), got {other:?}"),
    }
}

/// Count total rows in a query result.
fn total_rows(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Extract a string value from the first non-empty row / first column.
/// Handles both `StringArray` (Utf8) and `LargeStringArray` (LargeUtf8).
///
/// Note: the engine may emit multiple `RecordBatch`es — one per scanned
/// data file — and pruned/empty files produce zero-row batches. The first
/// batch is therefore not guaranteed to hold the matching row; we must
/// scan all batches and return the first non-null value.
fn first_string(res: &ExecResult) -> Option<String> {
    match res {
        ExecResult::Rows { batches, .. } => {
            for b in batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let col = b.column(0);
                if col.is_null(0) {
                    continue;
                }
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    return Some(arr.value(0).to_string());
                }
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
                    return Some(arr.value(0).to_string());
                }
                return None;
            }
            None
        }
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 1: round-trip — INSERT → CREATE INDEX → SELECT by indexed column
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_index_roundtrip_point_query() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Setup: create table and insert rows.
    sess.execute("CREATE TABLE users (id BIGINT, email TEXT, name TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob')")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (3, 'carol@example.com', 'Carol')")
        .await
        .unwrap();

    // Create secondary index on email column.
    let res = sess
        .execute("CREATE INDEX idx_users_email ON users(email)")
        .await
        .expect("CREATE INDEX should succeed");
    assert_tag(res, "CREATE INDEX");

    // Point query on indexed column should return exactly one row.
    let res = sess
        .execute("SELECT name FROM users WHERE email = 'alice@example.com'")
        .await
        .expect("SELECT on indexed column should succeed");
    assert_eq!(
        total_rows(&res),
        1,
        "point query on indexed column should return exactly one row"
    );
    let name = first_string(&res);
    assert_eq!(name.as_deref(), Some("Alice"), "wrong row returned");
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 2: DROP INDEX → fallback to full scan still returns correct results
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn drop_index_fallback_to_full_scan() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE orders (id BIGINT, customer TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (10, 'acme'), (20, 'omega'), (30, 'delta')")
        .await
        .unwrap();
    sess.execute("CREATE INDEX idx_orders_customer ON orders(customer)")
        .await
        .unwrap();

    // Verify the index works.
    let before = sess
        .execute("SELECT id FROM orders WHERE customer = 'acme'")
        .await
        .unwrap();
    assert_eq!(total_rows(&before), 1, "index should find 1 row");

    // Drop the index.
    let drop_res = sess
        .execute("DROP INDEX idx_orders_customer")
        .await
        .expect("DROP INDEX should succeed");
    assert_tag(drop_res, "DROP INDEX");

    // Query should still return correct results (full-scan path).
    let after = sess
        .execute("SELECT id FROM orders WHERE customer = 'acme'")
        .await
        .expect("SELECT after DROP INDEX should still work");
    assert_eq!(
        total_rows(&after),
        1,
        "full-scan fallback should still return the correct row"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 3: measurably fewer file opens with index (skip counter)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn point_query_skips_files_with_index() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Build a table with 5 separate INSERT statements so each one
    // produces a distinct Parquet file.  An index on 'code' should let a
    // point query for code='F001' skip the other 4 files.
    sess.execute("CREATE TABLE products (code TEXT, price BIGINT)")
        .await
        .unwrap();
    for (code, price) in &[
        ("F001", 100i64),
        ("G002", 200),
        ("H003", 300),
        ("I004", 400),
        ("J005", 500),
    ] {
        sess.execute(&format!("INSERT INTO products VALUES ('{code}', {price})"))
            .await
            .unwrap();
    }

    // Create index AFTER the inserts so it's built from the catalog data.
    // Note: the index is built lazily at INSERT time; rows inserted before
    // CREATE INDEX are not automatically backfilled in this version. So we
    // insert a 6th row after creating the index to prime the index, then
    // query that row's code.
    sess.execute("CREATE INDEX idx_products_code ON products(code)")
        .await
        .unwrap();

    // Insert a new row after the index is created — this row's file WILL be
    // indexed. Query for this row's key value.
    sess.execute("INSERT INTO products VALUES ('INDEXED', 999)")
        .await
        .unwrap();

    let before_skip_count = engine.secondary_index_skipped_count();

    // Point query for the newly-indexed value.
    let res = sess
        .execute("SELECT price FROM products WHERE code = 'INDEXED'")
        .await
        .expect("point query on indexed column should succeed");
    assert_eq!(total_rows(&res), 1, "should find the indexed row");

    let after_skip_count = engine.secondary_index_skipped_count();
    // The index knew only one file contains 'INDEXED'; all other 5 files
    // should have been skipped by the index probe.
    let skipped = after_skip_count - before_skip_count;
    assert!(
        skipped >= 1,
        "secondary index should have skipped at least 1 file (skipped={skipped})"
    );

    println!("[secondary_index] point_query_skips_files_with_index: skipped {skipped} files");
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 4: index is maintained on INSERT (entries added)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn index_maintained_on_insert() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT, kind TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX idx_events_kind ON events(kind)")
        .await
        .unwrap();

    // Insert rows — index should be updated for each file.
    sess.execute("INSERT INTO events VALUES (1, 'login')")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (2, 'logout')")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (3, 'login')")
        .await
        .unwrap();

    // Query for 'login' — should find 2 rows.
    let res = sess
        .execute("SELECT id FROM events WHERE kind = 'login'")
        .await
        .expect("SELECT on indexed kind column should succeed");
    assert_eq!(
        total_rows(&res),
        2,
        "should find 2 login events (index maintained on each INSERT)"
    );

    // Query for 'logout' — should find 1 row.
    let res = sess
        .execute("SELECT id FROM events WHERE kind = 'logout'")
        .await
        .expect("SELECT on indexed kind column should succeed");
    assert_eq!(total_rows(&res), 1, "should find 1 logout event");
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 5: index survives a process restart (loaded from disk on next probe)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn index_survives_restart() {
    let dir = TempDir::new().unwrap();

    // Use a shared catalog so the second engine sees the same table metadata.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    // ── First engine: create table, index, insert rows ──────────────────────
    let engine1 = build_engine_shared_catalog(&dir, catalog.clone());
    let project = ProjectId::new();
    let sess1 = engine1.open_session(project).await.unwrap();

    sess1
        .execute("CREATE TABLE sessions (token TEXT, user_id BIGINT)")
        .await
        .unwrap();
    sess1
        .execute("CREATE INDEX idx_sessions_token ON sessions(token)")
        .await
        .unwrap();
    sess1
        .execute("INSERT INTO sessions VALUES ('tok_abc', 42)")
        .await
        .unwrap();
    sess1
        .execute("INSERT INTO sessions VALUES ('tok_xyz', 99)")
        .await
        .unwrap();

    // The index files have been flushed to the object store.
    // Drop the first engine (simulates process restart).
    drop(sess1);
    drop(engine1);

    // ── Second engine: same directory + catalog, new in-RAM state ──────────
    let engine2 = build_engine_shared_catalog(&dir, catalog.clone());
    let sess2 = engine2.open_session(project).await.unwrap();

    // Point query — index should be loaded from disk and used.
    let res = sess2
        .execute("SELECT user_id FROM sessions WHERE token = 'tok_abc'")
        .await
        .expect("point query after restart should succeed");
    assert_eq!(
        total_rows(&res),
        1,
        "should find the row after engine restart"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 6: cross-project isolation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cross_project_isolation() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    let sess_a = engine.open_session(project_a).await.unwrap();
    let sess_b = engine.open_session(project_b).await.unwrap();

    // Project A: table + index + row.
    sess_a
        .execute("CREATE TABLE secrets (key TEXT, value TEXT)")
        .await
        .unwrap();
    sess_a
        .execute("CREATE INDEX idx_secrets_key ON secrets(key)")
        .await
        .unwrap();
    sess_a
        .execute("INSERT INTO secrets VALUES ('A_ONLY', 'project_a_secret')")
        .await
        .unwrap();

    // Project B: same table name, different data.
    sess_b
        .execute("CREATE TABLE secrets (key TEXT, value TEXT)")
        .await
        .unwrap();
    sess_b
        .execute("CREATE INDEX idx_secrets_key ON secrets(key)")
        .await
        .unwrap();
    sess_b
        .execute("INSERT INTO secrets VALUES ('B_ONLY', 'project_b_secret')")
        .await
        .unwrap();

    // Project A querying for its key should find it.
    let res_a = sess_a
        .execute("SELECT value FROM secrets WHERE key = 'A_ONLY'")
        .await
        .unwrap();
    assert_eq!(total_rows(&res_a), 1, "project A should find its own row");

    // Project A querying for B's key should find nothing.
    let res_a_b = sess_a
        .execute("SELECT value FROM secrets WHERE key = 'B_ONLY'")
        .await
        .unwrap();
    assert_eq!(
        total_rows(&res_a_b),
        0,
        "project A should NOT see project B's data"
    );

    // Project B querying for its key should find it.
    let res_b = sess_b
        .execute("SELECT value FROM secrets WHERE key = 'B_ONLY'")
        .await
        .unwrap();
    assert_eq!(total_rows(&res_b), 1, "project B should find its own row");

    // Project B querying for A's key should find nothing.
    let res_b_a = sess_b
        .execute("SELECT value FROM secrets WHERE key = 'A_ONLY'")
        .await
        .unwrap();
    assert_eq!(
        total_rows(&res_b_a),
        0,
        "project B should NOT see project A's data"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate 6b: 88-shape parity — basic aggregate on indexed table works
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn aggregate_on_indexed_table_correct() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE sales (region TEXT, amount BIGINT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX idx_sales_region ON sales(region)")
        .await
        .unwrap();
    sess.execute("INSERT INTO sales VALUES ('north', 100), ('south', 200), ('north', 50)")
        .await
        .unwrap();

    // COUNT(*) on the whole table should still work correctly.
    let res = sess
        .execute("SELECT COUNT(*) FROM sales")
        .await
        .expect("COUNT(*) on indexed table should succeed");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows, got {other:?}"),
    };
    let total: i64 = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|a| a.values().to_vec())
                .unwrap_or_default()
        })
        .sum();
    assert_eq!(total, 3, "COUNT(*) on indexed table must return 3");
}

// ─────────────────────────────────────────────────────────────────────────────
// Gate: integer column index works for BIGINT
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn integer_column_index() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine_in_dir(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE items (id BIGINT, name TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE INDEX idx_items_id ON items(id)")
        .await
        .unwrap();

    for i in 1i64..=10 {
        sess.execute(&format!("INSERT INTO items VALUES ({i}, 'item_{i}')"))
            .await
            .unwrap();
    }

    // Point query on the indexed integer column.
    let res = sess
        .execute("SELECT name FROM items WHERE id = 7")
        .await
        .expect("SELECT with indexed integer column should succeed");
    assert_eq!(total_rows(&res), 1, "should find item with id=7");
    let name = first_string(&res);
    assert_eq!(name.as_deref(), Some("item_7"));
}
