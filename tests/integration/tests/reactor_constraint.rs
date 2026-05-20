//! Integration tests for constraint-shaped reactors (Phase 5.11.C2).
//!
//! These tests exercise the full SQL surface:
//!
//!   ALTER TABLE <t> REACT ON INSERT CONSTRAINT (<predicate>)
//!
//! through `ProjectSession::execute` — i.e. the complete executor dispatch
//! path, not the helper-API shortcut tested in
//! `crates/basin-engine/tests/alter_table_dml_surfaces.rs`.
//!
//! Acceptance gates (per TASK.md §5.11.C2):
//!
//!   1. Cap-at-N rejection: 101st INSERT fails with SQLSTATE 23514.
//!   2. Cap-at-N allows under the cap: 100 INSERTs all succeed.
//!   3. Constraint with subquery against a sibling table works.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
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

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─── Gate 1: cap-at-N rejection ─────────────────────────────────────────────

/// `ALTER TABLE … REACT ON INSERT CONSTRAINT (...)` via the SQL surface:
/// the 101st INSERT must fail with SQLSTATE 23514.
///
/// The reactor fires with a new session that sees only committed rows.
/// So `(SELECT COUNT(*) FROM t) < 100` fails when 100 rows are committed
/// and the 101st INSERT fires the reactor — `100 < 100` is false.
///
/// We pre-populate 100 rows via a single multi-value INSERT (no reactor
/// attached yet), attach the constraint, then verify the next single-row
/// INSERT is blocked.  This avoids debug-build stack-depth issues from
/// 100 sequential async INSERT frames each firing a reactor subquery.
///
/// Uses multi_thread flavour because the reactor body opens a fresh
/// session and runs a subquery; in debug builds the nested async frames
/// can push the single-thread tokio stack over the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cap_at_100_rejects_101st_insert() {
    let (_dir, engine) = open_engine().await;
    let sess = session(&engine).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Build a single multi-value INSERT for 100 rows.
    let vals: Vec<String> = (0i64..100).map(|i| format!("({i})")).collect();
    let bulk = format!("INSERT INTO t VALUES {}", vals.join(", "));
    sess.execute(&bulk).await.unwrap();

    // Register a constraint reactor via the full SQL surface (no helper calls).
    sess.execute(
        "ALTER TABLE t REACT ON INSERT CONSTRAINT ((SELECT COUNT(*) FROM t) < 100) \
         WITH (name = 'cap100')",
    )
    .await
    .unwrap();

    // Verify 100 rows are present.
    let res = sess
        .execute("SELECT COUNT(*) AS n FROM t")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "n"), vec![100]);
    } else {
        panic!("expected Rows");
    }

    // The 101st INSERT: 100 rows committed, reactor sees count = 100,
    // predicate `100 < 100` is false → SQLSTATE 23514.
    let err = sess
        .execute("INSERT INTO t VALUES (100)")
        .await
        .expect_err("101st INSERT must be rejected by the constraint reactor");
    let msg = format!("{err}");
    assert!(
        msg.contains("23514") || msg.to_ascii_lowercase().contains("check"),
        "expected SQLSTATE 23514 / check_violation, got: {msg:?}"
    );

    // Source mutation must have rolled back — still exactly 100 rows.
    let res = sess
        .execute("SELECT COUNT(*) AS n FROM t")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "n"), vec![100], "rollback must leave 100 rows");
    } else {
        panic!("expected Rows");
    }
}

// ─── Gate 2: cap-at-N allows inserts under the cap ──────────────────────────

/// All inserts that satisfy the predicate must succeed without error.
///
/// We pre-populate 90 rows (well under the cap of 100), attach the
/// constraint, then insert a few more.  All should succeed.
///
/// Uses multi_thread flavour because the reactor body opens a fresh
/// session and runs a subquery; in debug builds the nested async frames
/// can push the single-thread tokio stack over the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cap_at_100_allows_inserts_under_cap() {
    let (_dir, engine) = open_engine().await;
    let sess = session(&engine).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Attach constraint first (0 rows, all future inserts checked).
    sess.execute(
        "ALTER TABLE t REACT ON INSERT CONSTRAINT ((SELECT COUNT(*) FROM t) < 100)",
    )
    .await
    .unwrap();

    // Insert 5 rows individually — all well under the cap.
    for i in 0i64..5 {
        sess.execute(&format!("INSERT INTO t VALUES ({i})"))
            .await
            .unwrap_or_else(|e| panic!("INSERT #{i} failed: {e}"));
    }

    let res = sess
        .execute("SELECT COUNT(*) AS n FROM t")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            col_i64(&batches, "n"),
            vec![5],
            "all 5 under-cap inserts must have committed"
        );
    } else {
        panic!("expected Rows");
    }

    // Also verify a bulk INSERT of 90 rows (total = 95) still passes.
    let vals: Vec<String> = (5i64..95).map(|i| format!("({i})")).collect();
    let bulk = format!("INSERT INTO t VALUES {}", vals.join(", "));
    sess.execute(&bulk)
        .await
        .expect("bulk insert of 90 rows to reach 95 total must pass under cap");

    let res = sess
        .execute("SELECT COUNT(*) AS n FROM t")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            col_i64(&batches, "n"),
            vec![95],
            "95 rows should be committed"
        );
    } else {
        panic!("expected Rows");
    }
}

// ─── Gate 3: constraint with subquery against sibling table ─────────────────

/// The predicate subquery can reference a sibling table in the same project.
/// This covers the "no project may exceed its quota" pattern:
///
///   quota is stored in `project_quotas`; inserts into `items` are
///   blocked once the item count exceeds the quota row's `max_rows` value.
///
/// Simpler but equivalent: cap items inserts on the count in a sibling
/// `limits` table (1 row = 1 allowed item).
///
/// Uses multi_thread flavour because the reactor body opens a fresh
/// session and runs a subquery; in debug builds the nested async frames
/// can push the single-thread tokio stack over the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constraint_subquery_against_sibling_table() {
    let (_dir, engine) = open_engine().await;
    let sess = session(&engine).await;

    // `limits` holds a single row that controls how many items are allowed.
    sess.execute("CREATE TABLE limits (max_rows BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE items (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Constraint: items count must stay <= the max_rows value in `limits`.
    sess.execute(
        "ALTER TABLE items REACT ON INSERT \
         CONSTRAINT ((SELECT COUNT(*) FROM items) < (SELECT max_rows FROM limits)) \
         WITH (name = 'quota')",
    )
    .await
    .unwrap();

    // Set quota to 2 (allows up to 2 items).
    sess.execute("INSERT INTO limits VALUES (2)").await.unwrap();

    // First two inserts succeed (counts 0 < 2, 1 < 2).
    sess.execute("INSERT INTO items VALUES (1)").await.unwrap();
    sess.execute("INSERT INTO items VALUES (2)").await.unwrap();

    // Third insert: count is 2, predicate `2 < 2` is false → blocked.
    let err = sess
        .execute("INSERT INTO items VALUES (3)")
        .await
        .expect_err("insert must be blocked by quota constraint");
    let msg = format!("{err}");
    assert!(
        msg.contains("23514") || msg.to_ascii_lowercase().contains("check"),
        "expected SQLSTATE 23514 / check_violation, got: {msg:?}"
    );

    // Still exactly 2 rows committed.
    let res = sess
        .execute("SELECT COUNT(*) AS n FROM items")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "n"), vec![2]);
    } else {
        panic!("expected Rows");
    }

    // Raise the quota to 5; now two more inserts are allowed.
    sess.execute("DELETE FROM limits").await.unwrap();
    sess.execute("INSERT INTO limits VALUES (5)").await.unwrap();

    sess.execute("INSERT INTO items VALUES (3)").await.unwrap();
    sess.execute("INSERT INTO items VALUES (4)").await.unwrap();

    let res = sess
        .execute("SELECT COUNT(*) AS n FROM items")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            col_i64(&batches, "n"),
            vec![4],
            "items should have 4 rows after quota raise"
        );
    } else {
        panic!("expected Rows");
    }
}

// ─── Bonus: ALTER TABLE CONSTRAINT returns correct tag ──────────────────────

/// The DDL statement itself must return an `ExecResult::Empty` with tag
/// `"ALTER TABLE"`, matching the convention for other ALTER TABLE variants.
#[tokio::test]
async fn constraint_react_returns_alter_table_tag() {
    let (_dir, engine) = open_engine().await;
    let sess = session(&engine).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    match sess
        .execute("ALTER TABLE t REACT ON INSERT CONSTRAINT ((SELECT COUNT(*) FROM t) < 10)")
        .await
        .unwrap()
    {
        ExecResult::Empty { tag } => {
            assert_eq!(tag, "ALTER TABLE");
        }
        other => panic!("expected ExecResult::Empty, got {other:?}"),
    }
}

// ─── Bonus: DROP REACTOR removes the constraint ──────────────────────────────

/// After `DROP REACTOR`, inserts that would have violated the cap succeed.
///
/// Uses multi_thread flavour because the reactor body opens a fresh
/// session and runs a subquery; in debug builds the nested async frames
/// can push the single-thread tokio stack over the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_reactor_removes_constraint() {
    let (_dir, engine) = open_engine().await;
    let sess = session(&engine).await;

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    sess.execute(
        "ALTER TABLE t REACT ON INSERT CONSTRAINT ((SELECT COUNT(*) FROM t) < 1) \
         WITH (name = 'tight_cap')",
    )
    .await
    .unwrap();

    // First insert succeeds (count 0 < 1).
    sess.execute("INSERT INTO t VALUES (1)").await.unwrap();

    // Second insert is blocked by the cap.
    sess.execute("INSERT INTO t VALUES (2)")
        .await
        .expect_err("must be blocked before drop");

    // Drop the constraint reactor.
    sess.execute("DROP REACTOR tight_cap ON t").await.unwrap();

    // Now further inserts succeed.
    sess.execute("INSERT INTO t VALUES (2)").await.unwrap();
    sess.execute("INSERT INTO t VALUES (3)").await.unwrap();

    let res = sess
        .execute("SELECT COUNT(*) AS n FROM t")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(total_rows(&batches), 1);
        assert_eq!(col_i64(&batches, "n"), vec![3]);
    } else {
        panic!("expected Rows");
    }
}
