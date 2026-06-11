//! Integration tests for:
//!  - UPDATE expression RHS (`SET col = col + 1`, `SET col = NOW()`)
//!  - DELETE RETURNING

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn make_engine(dir: &TempDir) -> Engine {
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

/// Helper: extract Int64 column from result batches.
fn int64_col(res: ExecResult, col: &str) -> Vec<i64> {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name(col)
                    .unwrap_or_else(|| panic!("column {col:?} not found"))
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("column {col:?} is not Int64"));
                for i in 0..b.num_rows() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Helper: extract String column from result batches.
fn str_col(res: ExecResult, col: &str) -> Vec<String> {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name(col)
                    .unwrap_or_else(|| panic!("column {col:?} not found"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("column {col:?} is not String"));
                for i in 0..b.num_rows() {
                    out.push(arr.value(i).to_string());
                }
            }
            out
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn tag(res: ExecResult) -> String {
    match res {
        ExecResult::Empty { tag } => tag,
        other => panic!("expected Empty, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// UPDATE SET col = col + 1  (column-reference arithmetic on RHS)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_set_col_plus_literal() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE counters (id BIGINT NOT NULL, cnt BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO counters VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    // Increment cnt for id = 1.
    let res = sess
        .execute("UPDATE counters SET cnt = cnt + 1 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 1");

    // Verify.
    let vals = int64_col(
        sess.execute("SELECT cnt FROM counters WHERE id = 1")
            .await
            .unwrap(),
        "cnt",
    );
    assert_eq!(vals, vec![11], "cnt should have been incremented to 11");

    // Other rows must be unchanged.
    let vals2 = int64_col(
        sess.execute("SELECT cnt FROM counters WHERE id = 2")
            .await
            .unwrap(),
        "cnt",
    );
    assert_eq!(vals2, vec![20]);
}

// ---------------------------------------------------------------------------
// UPDATE SET col = col * 2  (arithmetic expression)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_set_col_multiply() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE vals (id BIGINT NOT NULL, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO vals VALUES (1, 5)")
        .await
        .unwrap();

    let res = sess
        .execute("UPDATE vals SET v = v * 2 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 1");

    let vals = int64_col(
        sess.execute("SELECT v FROM vals WHERE id = 1")
            .await
            .unwrap(),
        "v",
    );
    assert_eq!(vals, vec![10]);
}

// ---------------------------------------------------------------------------
// UPDATE SET ts = NOW()  (timestamp function on non-timestamp path tested
//                         via text col — literal NOW() test is below)
// UPDATE SET name = upper(name)  (string function on col)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_set_string_function() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE words (id BIGINT NOT NULL, w TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO words VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();

    // upper() is a DataFusion built-in.
    let res = sess
        .execute("UPDATE words SET w = upper(w) WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 1");

    let vals = str_col(
        sess.execute("SELECT w FROM words WHERE id = 1")
            .await
            .unwrap(),
        "w",
    );
    assert_eq!(vals, vec!["HELLO"]);

    // Row 2 must be untouched.
    let vals2 = str_col(
        sess.execute("SELECT w FROM words WHERE id = 2")
            .await
            .unwrap(),
        "w",
    );
    assert_eq!(vals2, vec!["world"]);
}

// ---------------------------------------------------------------------------
// UPDATE SET ts = now()  (timestamp function — existing fast path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_set_now_timestamp() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, ts TIMESTAMPTZ)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, NULL)")
        .await
        .unwrap();

    let before_micros = chrono::Utc::now().timestamp_micros();
    let res = sess
        .execute("UPDATE events SET ts = now() WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 1");
    let after_micros = chrono::Utc::now().timestamp_micros();

    // Verify the stored timestamp is within the before/after window.
    // TIMESTAMPTZ columns come back as TimestampMicrosecondArray.
    let res = sess
        .execute("SELECT ts FROM events WHERE id = 1")
        .await
        .unwrap();
    let stored = match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            arr.value(0)
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert!(
        stored >= before_micros && stored <= after_micros,
        "stored ts {stored} should be within [{before_micros}, {after_micros}]"
    );
}

// ---------------------------------------------------------------------------
// DELETE FROM t RETURNING *
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_returning_star() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await
        .unwrap();

    // Delete id=2 and get it back.
    let res = sess
        .execute("DELETE FROM items WHERE id = 2 RETURNING *")
        .await
        .unwrap();

    match res {
        ExecResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 1, "expected 1 deleted row in RETURNING");
            let id_arr = batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(id_arr.value(0), 2);
            let name_arr = batches[0]
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(name_arr.value(0), "beta");
        }
        other => panic!("expected Rows from DELETE RETURNING, got {other:?}"),
    }

    // Confirm the row is actually gone.
    let remaining = int64_col(sess.execute("SELECT id FROM items").await.unwrap(), "id");
    let mut remaining = remaining.clone();
    remaining.sort();
    assert_eq!(remaining, vec![1, 3]);
}

// ---------------------------------------------------------------------------
// DELETE FROM t RETURNING id  (column subset)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_returning_column_subset() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE things (id BIGINT NOT NULL, label TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO things VALUES (10, 'x'), (20, 'y'), (30, 'z')")
        .await
        .unwrap();

    // Delete all with id > 15 and return just the id.
    let res = sess
        .execute("DELETE FROM things WHERE id > 15 RETURNING id")
        .await
        .unwrap();

    let ids = int64_col(res, "id");
    let mut ids = ids.clone();
    ids.sort();
    assert_eq!(ids, vec![20, 30]);

    // Confirm remaining.
    let remaining = int64_col(sess.execute("SELECT id FROM things").await.unwrap(), "id");
    assert_eq!(remaining, vec![10]);
}

// ---------------------------------------------------------------------------
// DELETE with predicate matching all rows returns all rows via RETURNING
// (DELETE without any WHERE does not parse with sqlparser's PG dialect
//  when RETURNING is present — use id >= 0 as a tautology instead)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_all_returning() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE scratch (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO scratch VALUES (1), (2), (3)")
        .await
        .unwrap();

    // Use a tautological predicate so the DELETE touches all rows.
    let res = sess
        .execute("DELETE FROM scratch WHERE id >= 0 RETURNING *")
        .await
        .unwrap();

    let ids = int64_col(res, "id");
    assert_eq!(ids.len(), 3, "should have returned all 3 deleted rows");

    // Table should now be empty.
    let remaining = int64_col(sess.execute("SELECT id FROM scratch").await.unwrap(), "id");
    assert!(remaining.is_empty());
}

// ---------------------------------------------------------------------------
// Regression: `WHERE int_col = 'string_literal'` must coerce the text literal
// to the column type (PG semantics) instead of panicking the worker on an
// unchecked Arrow `as_string()` downcast over a non-string array.
// Covers SELECT / UPDATE / DELETE — every predicate path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn where_int_col_eq_string_literal_coerces_not_panics() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap();

    // SELECT: int column = string literal — must return the matching row.
    let names = str_col(
        sess.execute("SELECT name FROM t WHERE id = '1'")
            .await
            .unwrap(),
        "name",
    );
    assert_eq!(names, vec!["alice".to_string()]);

    // UPDATE: the exact repro from the bug report.
    let res = sess
        .execute("UPDATE t SET name = 'x' WHERE id = '1'")
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 1");
    let names = str_col(
        sess.execute("SELECT name FROM t WHERE id = 1")
            .await
            .unwrap(),
        "name",
    );
    assert_eq!(names, vec!["x".to_string()]);

    // DELETE: int column = string literal.
    let res = sess
        .execute("DELETE FROM t WHERE id = '2'")
        .await
        .unwrap();
    assert_eq!(tag(res), "DELETE 1");
    let remaining = int64_col(
        sess.execute("SELECT id FROM t ORDER BY id").await.unwrap(),
        "id",
    );
    assert_eq!(remaining, vec![1, 3]);
}

// ---------------------------------------------------------------------------
// No-WHERE UPDATE with a CASE expression RHS (the benchmark
// `conditional_update` shape). With no sinks / audit / generated columns /
// RETURNING this takes the parallel no-capture cold path; every row must
// still get the correct CASE arm and untouched columns must be preserved.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_no_where_case_expression_all_rows() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE txns (id BIGINT NOT NULL, amount BIGINT NOT NULL, status TEXT NOT NULL)",
    )
    .await
    .unwrap();
    // Three separate INSERTs → multiple data files, so the per-file fan-out
    // (not just the single-file case) is exercised. Amounts cover all three
    // CASE arms, including the boundary values 49/50 and 149/150.
    sess.execute("INSERT INTO txns VALUES (1, 10, 'pending'), (2, 49, 'pending')")
        .await
        .unwrap();
    sess.execute("INSERT INTO txns VALUES (3, 50, 'pending'), (4, 149, 'pending')")
        .await
        .unwrap();
    sess.execute("INSERT INTO txns VALUES (5, 150, 'pending'), (6, 500, 'pending')")
        .await
        .unwrap();

    let res = sess
        .execute(
            "UPDATE txns SET status = CASE WHEN amount < 50 THEN 'low' \
             WHEN amount < 150 THEN 'mid' ELSE 'high' END",
        )
        .await
        .unwrap();
    assert_eq!(tag(res), "UPDATE 6");

    let statuses = str_col(
        sess.execute("SELECT status FROM txns ORDER BY id")
            .await
            .unwrap(),
        "status",
    );
    assert_eq!(statuses, vec!["low", "low", "mid", "mid", "high", "high"]);

    // Columns not in the SET list must be untouched.
    let amounts = int64_col(
        sess.execute("SELECT amount FROM txns ORDER BY id")
            .await
            .unwrap(),
        "amount",
    );
    assert_eq!(amounts, vec![10, 49, 50, 149, 150, 500]);
}

// ---------------------------------------------------------------------------
// Same CASE-expression UPDATE but with RETURNING: RETURNING forces the
// serial capture path, which must still evaluate the expression against the
// OLD row values and project the post-update rows.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_case_expression_with_returning() {
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE txns (id BIGINT NOT NULL, amount BIGINT NOT NULL, status TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO txns VALUES (1, 10, 'pending'), (2, 100, 'pending'), (3, 900, 'pending')",
    )
    .await
    .unwrap();

    let res = sess
        .execute(
            "UPDATE txns SET status = CASE WHEN amount < 50 THEN 'low' \
             WHEN amount < 150 THEN 'mid' ELSE 'high' END RETURNING id, status",
        )
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("UPDATE … RETURNING must return rows, got {other:?}"),
    };
    let mut returned: Vec<(i64, String)> = Vec::new();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .expect("RETURNING id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let sts = b
            .column_by_name("status")
            .expect("RETURNING status column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status is String");
        for i in 0..b.num_rows() {
            returned.push((ids.value(i), sts.value(i).to_string()));
        }
    }
    returned.sort();
    assert_eq!(
        returned,
        vec![
            (1, "low".to_string()),
            (2, "mid".to_string()),
            (3, "high".to_string()),
        ],
        "RETURNING must surface the post-update CASE values"
    );

    // The table itself must hold the same post-update values.
    let statuses = str_col(
        sess.execute("SELECT status FROM txns ORDER BY id")
            .await
            .unwrap(),
        "status",
    );
    assert_eq!(statuses, vec!["low", "mid", "high"]);
}
