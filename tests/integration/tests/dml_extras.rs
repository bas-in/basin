//! DML-category coverage for features added in the "+5 DML" milestone:
//!
//! 1. INSERT … ON CONFLICT (col) DO UPDATE SET …
//! 2. UPDATE t SET … WHERE col IN (SELECT id FROM u)
//! 3. DELETE FROM t WHERE col IN (SELECT id FROM u)
//! 4. INSERT INTO t DEFAULT VALUES
//! 5. INSERT INTO t VALUES (…) RETURNING *

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
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
fn ok(r: ExecResult) -> ExecResult {
    r
}

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
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
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
        .await
        .unwrap();

    // First upsert — no conflict, should insert.
    let res = sess
        .execute(
            "INSERT INTO kv (k, v) VALUES (1, 'hello') ON CONFLICT (k) DO UPDATE SET v = 'updated'",
        )
        .await
        .unwrap();
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("INSERT")),
        "expected INSERT tag, got {res:?}"
    );

    // Verify the row is there.
    let ExecResult::Rows { batches, .. } =
        sess.execute("SELECT v FROM kv WHERE k = 1").await.unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(col_string(&batches, "v"), vec!["hello"]);
}

#[tokio::test]
async fn on_conflict_do_update_updates_on_conflict() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO kv (k, v) VALUES (1, 'hello')")
        .await
        .unwrap();

    // Second upsert — conflict on k=1, should UPDATE.
    let res = sess
        .execute(
            "INSERT INTO kv (k, v) VALUES (1, 'world') ON CONFLICT (k) DO UPDATE SET v = 'world'",
        )
        .await
        .unwrap();
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
    let ExecResult::Rows { batches, .. } =
        sess.execute("SELECT v FROM kv WHERE k = 1").await.unwrap()
    else {
        panic!("expected rows")
    };
    let vals = col_string(&batches, "v");
    assert_eq!(
        vals,
        vec!["world"],
        "value should have been updated to 'world'"
    );
}

// ─── 2. UPDATE … WHERE col IN (SELECT …) ────────────────────────────────────

#[tokio::test]
async fn update_where_in_subquery() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE skip_ids (sid BIGINT NOT NULL)")
        .await
        .unwrap();

    // Insert some items.
    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    // IDs to update.
    sess.execute("INSERT INTO skip_ids (sid) VALUES (1), (3)")
        .await
        .unwrap();

    // UPDATE using IN (SELECT ...).
    let res = sess
        .execute("UPDATE items SET name = 'updated' WHERE id IN (SELECT sid FROM skip_ids)")
        .await
        .unwrap();
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("UPDATE")),
        "expected UPDATE tag, got {res:?}"
    );

    // Check that rows 1 and 3 were updated but row 2 was not.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM items ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
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
        .await
        .unwrap();
    sess.execute("CREATE TABLE del_ids (did BIGINT NOT NULL)")
        .await
        .unwrap();

    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    sess.execute("INSERT INTO del_ids (did) VALUES (1), (3)")
        .await
        .unwrap();

    // DELETE using IN (SELECT ...).
    let res = sess
        .execute("DELETE FROM items WHERE id IN (SELECT did FROM del_ids)")
        .await
        .unwrap();
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("DELETE")),
        "expected DELETE tag, got {res:?}"
    );

    // Only row 2 should remain.
    let ExecResult::Rows { batches, .. } = sess.execute("SELECT id FROM items").await.unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(col_i64(&batches, "id"), vec![2]);
}

// ─── 4. INSERT INTO t DEFAULT VALUES ────────────────────────────────────────

#[tokio::test]
async fn insert_default_values() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    // Table with nullable columns (all default to NULL).
    sess.execute("CREATE TABLE defaults_test (a BIGINT, b TEXT)")
        .await
        .unwrap();

    let res = sess
        .execute("INSERT INTO defaults_test DEFAULT VALUES")
        .await
        .unwrap();
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag.starts_with("INSERT")),
        "expected INSERT tag, got {res:?}"
    );

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT a, b FROM defaults_test")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
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
        .await
        .unwrap();

    let res = sess
        .execute("INSERT INTO ret_test (id, name) VALUES (42, 'answer') RETURNING *")
        .await
        .unwrap();

    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows from INSERT RETURNING *")
    };

    assert_eq!(
        rows(&batches),
        1,
        "RETURNING should return the inserted row"
    );
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
        .await
        .unwrap();

    let res = sess.execute(
        "INSERT INTO multi_ret (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three') RETURNING *"
    ).await.unwrap();

    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows from INSERT RETURNING *")
    };

    assert_eq!(
        rows(&batches),
        3,
        "RETURNING should return all 3 inserted rows"
    );
    let mut ids = col_i64(&batches, "id");
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ─── 6. Hot-tier DELETE fast path ───────────────────────────────────────────
//
// Pin the new bulk-DELETE-WHERE-IN write-amp fix (decisions.md 2026-05-23):
// `DELETE FROM pk_table WHERE pk IN (…)` writes tombstones to the
// MemTableRegistry instead of doing a copy-on-write Parquet rewrite.

/// `DELETE FROM t WHERE id IN (1, 2, 3)` on a table with single-column PK
/// reports `DELETE 3` and the underlying memtable picks up the tombstones.
///
/// The fast path is opt-in (env var `BASIN_HOTTIER_DELETE_FASTPATH=1`)
/// because the merge-on-read suppression isn't yet wired — see the gate
/// comment in `dml_mutate::try_resolve_fast_path_pks`. The test sets the
/// env var inline to exercise the wired-up DELETE write path.
///
/// NOTE: these tests are serial because `set_var` is a process-wide
/// mutation. The `serial_test` dependency isn't available, so we
/// intentionally do NOT assert read-after-delete visibility (which is
/// the gap the env var documents).
#[tokio::test]
async fn fast_path_bulk_delete_where_in_writes_tombstones() {
    // SAFETY: only this test mutates the env var; we don't assert
    // cross-test ordering. The variable is also process-wide so
    // parallel suites would race — keep this gated explicitly.
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE pk_items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO pk_items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    )
    .await
    .unwrap();

    let res = sess
        .execute("DELETE FROM pk_items WHERE id IN (1, 3, 5)")
        .await
        .expect("fast-path DELETE must succeed");
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 3"),
        "fast path reports the requested PK count, got {res:?}"
    );

    // The memtable for pk_items must now hold 3 tombstones for keys 1/3/5.
    let registry = eng.memtable_registry();
    let table = basin_common::TableName::new("pk_items").unwrap();
    let entry = registry
        .get(&sess.project(), &table)
        .expect("registry entry exists after fast-path DELETE");
    let snap = entry.memtable.snapshot();
    let tombs = snap
        .iter()
        .filter(|(_, v)| matches!(v, basin_hottier::MemRowValue::Tombstone))
        .count();
    assert_eq!(tombs, 3, "three tombstones expected, snapshot was {snap:?}");
}

/// `DELETE FROM t WHERE id = 7` on a single-PK table also fast-paths.
#[tokio::test]
async fn fast_path_eq_lit_writes_single_tombstone() {
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE pk_one (id BIGINT PRIMARY KEY, val TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO pk_one (id, val) VALUES (7, 'lucky')")
        .await
        .unwrap();

    let res = sess
        .execute("DELETE FROM pk_one WHERE id = 7")
        .await
        .expect("fast path DELETE = lit must succeed");
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 1"),
        "got {res:?}"
    );

    let registry = eng.memtable_registry();
    let table = basin_common::TableName::new("pk_one").unwrap();
    let entry = registry.get(&sess.project(), &table).expect("entry");
    let snap = entry.memtable.snapshot();
    assert_eq!(snap.len(), 1);
    assert!(snap[0].1.is_tombstone());
}

/// Inside an explicit transaction the fast path MUST fall through to the
/// copy-on-write slow path. Process-wide registry tombstones cannot be
/// rolled back, so the gate at the top of `exec_delete` checks
/// `tx_is_active` and skips the fast path when set. This test asserts the
/// gate fires: after a `BEGIN; DELETE …; COMMIT/ROLLBACK` no tombstones
/// land in the registry (the slow path, even if it had committed via the
/// auto-commit catalog write, would have written replacement Parquet
/// files instead). Pre-existing rollback-of-DELETE semantics are a
/// separate gap tracked outside this PR.
#[tokio::test]
async fn fast_path_skipped_inside_explicit_transaction() {
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE pk_tx (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO pk_tx (id, name) VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    sess.execute("BEGIN").await.unwrap();
    sess.execute("DELETE FROM pk_tx WHERE id IN (1, 2)")
        .await
        .unwrap();
    sess.execute("COMMIT").await.unwrap();

    // No tombstones must have been written to the registry — the gate
    // intercepted the fast path BEFORE any registry write. The slow
    // CoW path may or may not have committed (separate concern), but
    // the registry's tombstone count for pk_tx must be zero.
    let registry = eng.memtable_registry();
    let table = basin_common::TableName::new("pk_tx").unwrap();
    let tomb_count = registry
        .get(&sess.project(), &table)
        .map(|e| {
            e.memtable
                .snapshot()
                .iter()
                .filter(|(_, v)| matches!(v, basin_hottier::MemRowValue::Tombstone))
                .count()
        })
        .unwrap_or(0);
    assert_eq!(
        tomb_count, 0,
        "fast path must NOT fire inside an explicit transaction; \
         found {tomb_count} tombstone(s) in the registry"
    );
}

/// Compare DELETE-WHERE-IN latency: fast path (single PK, env var ON)
/// vs slow path (composite PK, fast-path gate forces fall-through). The
/// fast path skips the copy-on-write Parquet rewrite
/// (`pre_mutation_flush`, `list_data_files_with_stats`,
/// `write_replacement`, `commit_replace`, `delete_objects`,
/// `refresh_table`) and reports back in microseconds; the slow path on
/// this small dataset is still tens of milliseconds.
///
/// We deliberately use a *table-shape* trigger (single vs composite PK)
/// for the slow/fast split instead of toggling the env var mid-test.
/// `std::env::set_var` is process-wide; sibling cargo tests running in
/// parallel could flip the var between our two timed runs and produce a
/// false ratio. The PK-shape split is stable.
///
/// The threshold is generous — the spread on a real workload is 2-3
/// orders of magnitude — so this test is robust on a hot/cold/loaded
/// host but still proves "fast path is firing and is materially cheaper
/// than the slow path".
#[tokio::test]
async fn fast_path_bulk_delete_is_materially_faster_than_slow_path() {
    use std::time::Instant;

    // Slow path: composite PK forces the fast-path gate to bail
    // regardless of the env var.
    let slow_ms: f64 = {
        let (_dir, eng) = open_engine().await;
        let sess = session(&eng).await;
        sess.execute(
            "CREATE TABLE pk_perf_composite (\
                a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT NOT NULL, \
                PRIMARY KEY (a, b))",
        )
        .await
        .unwrap();
        let mut stmt = String::from("INSERT INTO pk_perf_composite (a, b, v) VALUES ");
        for i in 0..100i64 {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({i}, 1, 'v{i}')"));
        }
        sess.execute(&stmt).await.unwrap();
        // Hit one row at a time on the slow path so the work shape
        // mirrors the fast path's per-PK cost.
        let start = Instant::now();
        sess.execute(
            "DELETE FROM pk_perf_composite WHERE a IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AND b = 1",
        )
        .await
        .unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    };

    // Fast path: single-column PK + env var on.
    let fast_ms: f64 = {
        std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
        let (_dir, eng) = open_engine().await;
        let sess = session(&eng).await;
        sess.execute("CREATE TABLE pk_perf_single (id BIGINT PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .unwrap();
        let mut stmt = String::from("INSERT INTO pk_perf_single (id, v) VALUES ");
        for i in 0..100i64 {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({i}, 'v{i}')"));
        }
        sess.execute(&stmt).await.unwrap();
        let start = Instant::now();
        sess.execute(
            "DELETE FROM pk_perf_single WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        )
        .await
        .unwrap();
        start.elapsed().as_secs_f64() * 1000.0
    };

    eprintln!(
        "[fast_path_bench] slow(composite-pk)={slow_ms:.2}ms fast(single-pk)={fast_ms:.2}ms \
         ratio={:.1}x",
        slow_ms / fast_ms.max(1e-6)
    );
    // The slow path on 100 rows + a 10-id IN list rewrites the file: at
    // minimum 3x slower than the fast path. Real-workload spread is much
    // larger; this threshold tolerates a noisy CI box.
    assert!(
        slow_ms > fast_ms * 3.0,
        "expected fast path to be >3x faster; slow={slow_ms:.2}ms fast={fast_ms:.2}ms"
    );
}

/// Composite-PK tables must NOT fast-path — fall through to slow CoW.
/// The slow path still produces the correct DELETE result; the assertion
/// here is correctness (the row really disappears, observed via SELECT),
/// not that the slow path was specifically used.
#[tokio::test]
async fn composite_pk_falls_through_slow_path_correctness() {
    // Enable the fast path env var: even with it ON, composite PK must
    // bail to slow CoW so deleted rows actually disappear from reads.
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE composite_pk (\
            a BIGINT NOT NULL, b BIGINT NOT NULL, val TEXT NOT NULL, \
            PRIMARY KEY (a, b))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO composite_pk VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')")
        .await
        .unwrap();

    // Compound WHERE on composite PK — slow path.
    let res = sess
        .execute("DELETE FROM composite_pk WHERE a = 1 AND b = 2")
        .await
        .unwrap();
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 1"),
        "composite PK slow path reports affected count, got {res:?}"
    );

    // Survivors: (1,1) and (2,1) — confirms the slow path actually
    // dropped the cold-tier row (proves we didn't accidentally
    // shortcut into the fast path which would only tombstone).
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT a, b FROM composite_pk ORDER BY a, b")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows(&batches), 2);
}

// ─── 6. UPDATE SET … table-stakes ORM shapes (audit 2026-05-23) ────────────
//
// These pin the end-to-end behaviour of UPDATE forms that every ORM emits:
// CASE WHEN on the RHS, COALESCE fallback, IN (SELECT …) with an empty
// subquery, and the new 100k IN-subquery materialisation cap. The "views
// increment" and "updated_at" round-trips live below; they prove the
// AssignmentRhs::Expr path actually mutates storage (not just parses).

/// `UPDATE t SET status = CASE WHEN amount > 100 THEN 'high' ELSE 'low' END
///  WHERE id IN (1, 2)` — the SET RHS routes to the Expr path and DataFusion
/// evaluates CASE per row against the pre-update values.
#[tokio::test]
async fn update_set_case_when_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, amount BIGINT NOT NULL, status TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO orders (id, amount, status) VALUES \
         (1, 50, 'pending'), (2, 200, 'pending'), (3, 999, 'pending')",
    )
    .await
    .unwrap();

    sess.execute(
        "UPDATE orders SET status = CASE WHEN amount > 100 THEN 'high' ELSE 'low' END \
         WHERE id IN (1, 2)",
    )
    .await
    .unwrap();

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, status FROM orders ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    let ids = col_i64(&batches, "id");
    let statuses = col_string(&batches, "status");
    assert_eq!(ids, vec![1, 2, 3]);
    // Row 1 (amount=50) → 'low'; row 2 (amount=200) → 'high'; row 3 not touched.
    assert_eq!(statuses, vec!["low", "high", "pending"]);
}

/// `UPDATE t SET name = COALESCE(nickname, name)` — when `nickname` is NULL
/// the name keeps its current value, otherwise the nickname replaces it.
/// Exercises the function-call-with-column-refs branch of the Expr path.
#[tokio::test]
async fn update_set_coalesce_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, nickname TEXT, name TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO users (id, nickname, name) VALUES \
         (1, 'bee', 'Beatrice'), (2, NULL, 'Charlie')",
    )
    .await
    .unwrap();

    sess.execute("UPDATE users SET name = COALESCE(nickname, name)")
        .await
        .unwrap();

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(col_string(&batches, "name"), vec!["bee", "Charlie"]);
}

/// `UPDATE … WHERE id IN (SELECT id FROM staging)` where staging is empty —
/// must return 0 rows updated, no error. ORMs do "cascade delete by IDs from
/// a temp table" routinely; if the staging set happens to be empty (e.g. the
/// outer transaction already cleared it) we must not crash.
#[tokio::test]
async fn update_where_in_subquery_empty() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE staging (sid BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    // staging is empty — no INSERT.

    let res = sess
        .execute("UPDATE items SET name = 'updated' WHERE id IN (SELECT sid FROM staging)")
        .await
        .unwrap();
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 0"),
        "empty subquery must report UPDATE 0, got {res:?}"
    );

    // Confirm no row was touched.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT name FROM items ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(col_string(&batches, "name"), vec!["a", "b", "c"]);
}

/// `UPDATE … WHERE id IN (SELECT id FROM huge)` where `huge` exceeds the
/// 100k materialisation cap → typed error suggesting the JOIN form. We seed
/// 100_001 rows so we trip exactly past the limit; assert the error mentions
/// "JOIN" and the row-count threshold so users get actionable guidance.
#[tokio::test]
async fn update_where_in_subquery_too_large_refused() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items (id, name) VALUES (1, 'a')")
        .await
        .unwrap();

    sess.execute("CREATE TABLE huge (hid BIGINT NOT NULL)")
        .await
        .unwrap();
    // 100_001 distinct ids — one past MAX_IN_SUBQUERY_ROWS. Insert in chunks so
    // the SQL parser doesn't choke on a single multi-megabyte literal.
    let chunk_size: i64 = 1_000;
    let chunks: i64 = 101; // 101_000 rows ≥ 100_001
    for c in 0..chunks {
        let mut values = String::with_capacity(chunk_size as usize * 12);
        for i in 0..chunk_size {
            if i > 0 {
                values.push_str(", ");
            }
            let id = c * chunk_size + i;
            values.push('(');
            values.push_str(&id.to_string());
            values.push(')');
        }
        sess.execute(&format!("INSERT INTO huge (hid) VALUES {values}"))
            .await
            .unwrap();
    }

    let err = sess
        .execute("UPDATE items SET name = 'x' WHERE id IN (SELECT hid FROM huge)")
        .await
        .expect_err("oversized subquery must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("100000") || msg.contains("100_000"),
        "error must mention the row limit; got {msg:?}"
    );
    assert!(
        msg.to_uppercase().contains("JOIN"),
        "error must suggest the JOIN form; got {msg:?}"
    );
}

/// Round-trip: increment a counter twice and SELECT it back. Two separate
/// UPDATE statements so each one reads the row's current value, increments
/// it, and writes back — the classic "views += 1" pattern every app does.
#[tokio::test]
async fn views_increment_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE counters (id BIGINT NOT NULL PRIMARY KEY, views BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO counters (id, views) VALUES (1, 0)")
        .await
        .unwrap();

    for _ in 0..2 {
        sess.execute("UPDATE counters SET views = views + 1 WHERE id = 1")
            .await
            .unwrap();
    }

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT views FROM counters WHERE id = 1")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        col_i64(&batches, "views"),
        vec![2],
        "two increments must land cumulatively"
    );
}

/// Round-trip: `UPDATE … SET updated_at = NOW()` actually advances the stored
/// timestamp. We stamp once, sleep a millisecond, stamp again, and assert
/// the second value is strictly greater than the first.
#[tokio::test]
async fn updated_at_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE rows (id BIGINT NOT NULL PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL)",
    )
    .await
    .unwrap();
    // Seed with a far-past timestamp so the first NOW() update is clearly
    // observable.
    sess.execute("INSERT INTO rows (id, updated_at) VALUES (1, '2000-01-01T00:00:00Z')")
        .await
        .unwrap();

    sess.execute("UPDATE rows SET updated_at = NOW() WHERE id = 1")
        .await
        .unwrap();
    let first = {
        let ExecResult::Rows { batches, .. } = sess
            .execute("SELECT updated_at FROM rows WHERE id = 1")
            .await
            .unwrap()
        else {
            panic!("expected rows")
        };
        batches[0]
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0)
    };

    // Sleep so the second NOW() is observably later (timestamps are
    // microsecond-resolution but the wall clock has nanosecond jitter on
    // some platforms, hence the explicit small sleep).
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    sess.execute("UPDATE rows SET updated_at = NOW() WHERE id = 1")
        .await
        .unwrap();
    let second = {
        let ExecResult::Rows { batches, .. } = sess
            .execute("SELECT updated_at FROM rows WHERE id = 1")
            .await
            .unwrap()
        else {
            panic!("expected rows")
        };
        batches[0]
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0)
    };

    assert!(
        second > first,
        "second NOW() ({second}) must be strictly greater than first ({first})"
    );
}

// ─── Prisma `"public"."Table"` qualified DML round-trips ────────────────────
//
// Prisma's query compiler emits schema-qualified table names on every
// `findUnique` / `update` / `delete`:
//
//   UPDATE "public"."User" SET "name" = $1
//     WHERE "public"."User"."id" = $2
//     RETURNING "public"."User"."id", "public"."User"."name", ...
//
// PG resolves `"public"` via `search_path`; Basin's flat-namespace catalog
// stores everything under bare names. The DML resolver strips the
// `"public"."` prefix (identity transform under default search_path) so the
// ORM shape round-trips. Non-public schemas are rejected with a clearer
// message — see the unit tests in `crates/basin-engine/src/dml_mutate.rs`.
//
// These tests pin the verbatim shape; if a refactor regresses the
// resolver, the ORM-compat corpus loses 2-3 Prisma shapes immediately.

/// Prisma `findUnique` + `update` round-trip: the canonical Prisma
/// UPDATE statement emitted by `prisma.user.update({where: {id}, data})`.
/// All three identifier sites are schema-qualified (table, WHERE col,
/// RETURNING cols).
#[tokio::test]
async fn prisma_findunique_update_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE \"User\" (\
             \"id\" BIGINT NOT NULL PRIMARY KEY, \
             \"email\" TEXT NOT NULL, \
             \"name\" TEXT NOT NULL\
         )",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO \"User\" (\"id\", \"email\", \"name\") \
         VALUES (1, 'alice@example.com', 'Alice')",
    )
    .await
    .unwrap();

    // Verbatim Prisma shape — schema-qualified target + WHERE + RETURNING.
    let res = sess
        .execute(
            "UPDATE \"public\".\"User\" SET \"name\" = 'AliceUpdated' \
             WHERE \"public\".\"User\".\"id\" = 1 \
             RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"name\"",
        )
        .await
        .expect("Prisma-shaped UPDATE must succeed");

    let ExecResult::Rows { batches, .. } = res else {
        panic!("Prisma UPDATE … RETURNING must produce Rows");
    };
    assert_eq!(rows(&batches), 1, "exactly one row matched id=1");

    // The change is visible on a subsequent read.
    let ExecResult::Rows { batches: rb, .. } = sess
        .execute("SELECT \"id\", \"name\" FROM \"User\" WHERE \"id\" = 1")
        .await
        .unwrap()
    else {
        panic!("SELECT must produce Rows");
    };
    assert_eq!(col_string(&rb, "name"), vec!["AliceUpdated"]);
}

/// Prisma `delete` round-trip: the canonical Prisma DELETE statement
/// emitted by `prisma.user.delete({where: {id}})`. Same shape as the
/// UPDATE above but with DELETE FROM.
#[tokio::test]
async fn prisma_delete_round_trip() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE \"User\" (\
             \"id\" BIGINT NOT NULL PRIMARY KEY, \
             \"email\" TEXT NOT NULL\
         )",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO \"User\" (\"id\", \"email\") VALUES \
         (998, 'doomed@example.com'), (999, 'survivor@example.com')",
    )
    .await
    .unwrap();

    // Verbatim Prisma DELETE shape.
    let res = sess
        .execute(
            "DELETE FROM \"public\".\"User\" \
             WHERE \"public\".\"User\".\"id\" = 998 \
             RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\"",
        )
        .await
        .expect("Prisma-shaped DELETE must succeed");

    let ExecResult::Rows { batches, .. } = res else {
        panic!("Prisma DELETE … RETURNING must produce Rows");
    };
    assert_eq!(rows(&batches), 1, "exactly one row matched id=998");

    // The survivor row is still there; the doomed row is gone.
    let ExecResult::Rows { batches: rb, .. } = sess
        .execute("SELECT \"id\" FROM \"User\" ORDER BY \"id\"")
        .await
        .unwrap()
    else {
        panic!("SELECT must produce Rows");
    };
    assert_eq!(col_i64(&rb, "id"), vec![999]);
}

// ─── 8. Hot-tier DELETE fast path — read-path tombstone suppression ────────
//
// The DELETE fast path writes `MemRowValue::Tombstone` entries into the
// process-wide `MemTableRegistry` without rewriting cold-tier Parquet. The
// matching read-path filter (`crates/basin-engine/src/hot_tombstone.rs`,
// wired from `session.rs`) must drop those tombstoned rows from any
// subsequent SELECT, or queries would return ghost rows.
//
// These tests prove the read-path wiring actually fires:
//
//   1. SELECT * after DELETE returns the right surviving rows.
//   2. SELECT COUNT(*) reflects the suppression (aggregate path).
//   3. WHERE-clause pushdown still composes correctly with tombstone filtering.
//   4. When the memtable has no tombstones at all, SELECT returns the cold
//      data unchanged — proves the `maybe_*` short-circuit avoids a regression
//      on the common no-DELETE read path.

/// 1. INSERT 5 rows, fast-path DELETE 2 of them, SELECT * returns the right 3.
#[tokio::test]
async fn fast_path_delete_then_select_omits_tombstoned_rows() {
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE rd_items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO rd_items (id, name) VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    )
    .await
    .unwrap();

    // Sanity: SELECT before DELETE returns all 5 rows.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_items ORDER BY id")
        .await
        .unwrap()
    else {
        panic!("expected Rows pre-DELETE")
    };
    assert_eq!(rows(&batches), 5, "5 rows pre-DELETE");

    // Fast-path DELETE 2 rows (ids 2 and 4).
    let res = sess
        .execute("DELETE FROM rd_items WHERE id IN (2, 4)")
        .await
        .expect("fast-path DELETE must succeed");
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 2"),
        "fast path tag, got {res:?}"
    );

    // Read back: tombstoned rows must be suppressed, leaving 1, 3, 5.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_items ORDER BY id")
        .await
        .expect("SELECT after fast-path DELETE must succeed")
    else {
        panic!("expected Rows post-DELETE")
    };
    assert_eq!(
        rows(&batches),
        3,
        "exactly 3 rows must survive (1, 3, 5); ghost rows would mean the \
         read-path tombstone filter did not fire"
    );
    let ids = col_i64(&batches, "id");
    let names = col_string(&batches, "name");
    assert_eq!(ids, vec![1, 3, 5], "survivor ids must be 1, 3, 5");
    assert_eq!(
        names,
        vec!["a", "c", "e"],
        "survivor names must align with survivor ids"
    );
}

/// 2. Same setup, SELECT COUNT(*) reports the post-suppression count.
#[tokio::test]
async fn fast_path_delete_then_count_excludes_tombstoned() {
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE rd_count (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO rd_count (id, name) VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    )
    .await
    .unwrap();

    // Fast-path DELETE ids 1 and 5.
    let res = sess
        .execute("DELETE FROM rd_count WHERE id IN (1, 5)")
        .await
        .expect("fast-path DELETE must succeed");
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 2"),
        "fast path tag, got {res:?}"
    );

    // Aggregate count: must reflect the 2 tombstones.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM rd_count")
        .await
        .expect("COUNT(*) after fast-path DELETE must succeed")
    else {
        panic!("expected Rows from COUNT(*)")
    };
    let counts = col_i64(&batches, "n");
    assert_eq!(
        counts,
        vec![3],
        "COUNT(*) must be 3 after fast-path DELETE of 2 rows; \
         a 5 here means the aggregate scan path missed the tombstone filter"
    );
}

/// 3. WHERE-clause pushdown must still compose correctly with tombstone
///    suppression. The same WHERE-by-PK query that returns 1 row for a live
///    PK must return 0 rows for a tombstoned PK.
#[tokio::test]
async fn fast_path_delete_then_select_with_where_clause() {
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE rd_where (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO rd_where (id, name) VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    )
    .await
    .unwrap();

    // Tombstone ids 2 and 4.
    let res = sess
        .execute("DELETE FROM rd_where WHERE id IN (2, 4)")
        .await
        .expect("fast-path DELETE must succeed");
    assert!(
        matches!(res, ExecResult::Empty { ref tag } if tag == "DELETE 2"),
        "fast path tag, got {res:?}"
    );

    // WHERE pk = <tombstoned pk> → 0 rows.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_where WHERE id = 2")
        .await
        .expect("SELECT WHERE id = <tombstoned> must succeed")
    else {
        panic!("expected Rows")
    };
    assert_eq!(
        rows(&batches),
        0,
        "WHERE id = 2 (tombstoned) must return 0 rows; \
         a 1 means the read-path filter is bypassed on the WHERE path"
    );

    // WHERE pk = <live pk> → 1 row.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_where WHERE id = 3")
        .await
        .expect("SELECT WHERE id = <live> must succeed")
    else {
        panic!("expected Rows")
    };
    assert_eq!(rows(&batches), 1, "WHERE id = 3 (live) must return 1 row");
    assert_eq!(col_i64(&batches, "id"), vec![3]);
    assert_eq!(col_string(&batches, "name"), vec!["c"]);

    // WHERE id IN (<tombstoned>, <live>) → 1 row (the live one).
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_where WHERE id IN (2, 3, 4) ORDER BY id")
        .await
        .expect("SELECT WHERE id IN (...) must succeed")
    else {
        panic!("expected Rows")
    };
    assert_eq!(
        rows(&batches),
        1,
        "WHERE id IN (2, 3, 4) must return only the live row (id=3); \
         tombstones 2 and 4 must be filtered out"
    );
    assert_eq!(col_i64(&batches, "id"), vec![3]);
}

/// 4. When the memtable carries no tombstones at all, SELECT must return the
///    full cold-tier data unchanged. This pins the `maybe_*` short-circuit:
///    a hypothetical filter that always ran would not regress correctness,
///    but it would silently add per-row overhead on the no-DELETE hot path.
///    The check here is correctness-equivalence to a baseline SELECT, which
///    a faulty filter (e.g. mis-keyed snapshot dropping live rows) would
///    break.
#[tokio::test]
async fn fast_path_delete_with_no_tombstones_is_zero_overhead() {
    // Set the env var to mirror the other tests; the table has no DELETEs
    // so the registry should never receive any tombstones, and the read
    // path must not invent any.
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE rd_nodel (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO rd_nodel (id, name) VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    )
    .await
    .unwrap();

    // SELECT * with no prior DELETE must return all 5 rows untouched.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT id, name FROM rd_nodel ORDER BY id")
        .await
        .expect("SELECT must succeed on no-tombstone table")
    else {
        panic!("expected Rows")
    };
    assert_eq!(
        rows(&batches),
        5,
        "no DELETEs were issued; all 5 cold rows must surface unchanged. \
         A row count != 5 means the tombstone filter is dropping live rows"
    );
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4, 5]);
    assert_eq!(
        col_string(&batches, "name"),
        vec!["a", "b", "c", "d", "e"]
    );

    // Defence in depth: prove the registry actually contains zero
    // tombstones for this table, so the read path is short-circuiting
    // because the snapshot is empty (not because the filter is broken).
    let registry = eng.memtable_registry();
    let table = basin_common::TableName::new("rd_nodel").unwrap();
    let tomb_count = registry
        .get(&sess.project(), &table)
        .map(|e| {
            e.memtable
                .snapshot()
                .iter()
                .filter(|(_, v)| matches!(v, basin_hottier::MemRowValue::Tombstone))
                .count()
        })
        .unwrap_or(0);
    assert_eq!(
        tomb_count, 0,
        "no DELETEs were issued — registry tombstone count must be 0"
    );

    // COUNT(*) also returns 5.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM rd_nodel")
        .await
        .expect("COUNT(*) must succeed")
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![5],
        "COUNT(*) on no-tombstone table must be 5"
    );
}

// ─── 9. Multi-row ON CONFLICT DO UPDATE (bulk upsert) ───────────────────────
//
// These tests pin the multi-row upsert path added to `try_on_conflict_do_update`
// in executor.rs.  Before the fix the function bailed out with `Ok(None)` for
// any batch with more than one row, causing the caller to attempt a plain INSERT
// of the entire batch — which hit the PK constraint on every id that already
// existed.
//
// PG semantics for ON CONFLICT DO UPDATE:
//   * Each row is checked independently: conflict → UPDATE, no conflict → INSERT.
//   * If the VALUES list contains two rows with the same conflict-key tuple PG
//     errors with "ON CONFLICT DO UPDATE command cannot affect row a second time".
//     We match that behaviour.

/// 50 rows all exist → all must be updated, row count stays 50.
#[tokio::test]
async fn bulk_upsert_all_conflict() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE upsert_all (id BIGINT NOT NULL PRIMARY KEY, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    // Seed ids 100..149 with amount=0.
    let mut seed = String::from("INSERT INTO upsert_all (id, amount) VALUES ");
    for i in 100i64..150 {
        if i > 100 {
            seed.push(',');
        }
        seed.push_str(&format!("({i}, 0)"));
    }
    sess.execute(&seed).await.unwrap();

    // Upsert the same 50 ids with amount=99.
    let mut upsert = String::from(
        "INSERT INTO upsert_all (id, amount) VALUES ",
    );
    for i in 100i64..150 {
        if i > 100 {
            upsert.push(',');
        }
        upsert.push_str(&format!("({i}, 99)"));
    }
    upsert.push_str(
        " ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount",
    );

    let res = sess
        .execute(&upsert)
        .await
        .expect("bulk upsert all-conflict must not error");
    assert!(
        matches!(res, ExecResult::Empty { .. }),
        "expected Empty result, got {res:?}"
    );

    // Row count must still be 50.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_all")
        .await
        .unwrap()
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(col_i64(&batches, "n"), vec![50], "COUNT must stay 50");

    // All amounts must now be 99.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_all WHERE amount = 99")
        .await
        .unwrap()
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![50],
        "all 50 rows must have amount=99 after upsert"
    );
}

/// Half-existing, half-new → updates for existing + inserts for new.
/// Total count must equal 50 (25 original + 25 new).
#[tokio::test]
async fn bulk_upsert_mixed() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE upsert_mixed (id BIGINT NOT NULL PRIMARY KEY, status TEXT NOT NULL)",
    )
    .await
    .unwrap();

    // Seed ids 0..24 with status='old'.
    let mut seed = String::from("INSERT INTO upsert_mixed (id, status) VALUES ");
    for i in 0i64..25 {
        if i > 0 {
            seed.push(',');
        }
        seed.push_str(&format!("({i}, 'old')"));
    }
    sess.execute(&seed).await.unwrap();

    // Upsert ids 0..49: ids 0..24 exist (→ UPDATE), ids 25..49 are new (→ INSERT).
    let mut upsert = String::from("INSERT INTO upsert_mixed (id, status) VALUES ");
    for i in 0i64..50 {
        if i > 0 {
            upsert.push(',');
        }
        upsert.push_str(&format!("({i}, 'new')"));
    }
    upsert.push_str(" ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status");

    let res = sess
        .execute(&upsert)
        .await
        .expect("bulk upsert mixed must not error");
    assert!(
        matches!(res, ExecResult::Empty { .. }),
        "expected Empty result, got {res:?}"
    );

    // Total row count must be 50.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_mixed")
        .await
        .unwrap()
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(col_i64(&batches, "n"), vec![50], "total must be 50");

    // All rows must have status='new'.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_mixed WHERE status = 'new'")
        .await
        .unwrap()
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![50],
        "all 50 rows must have status='new'"
    );

    // The 25 previously-seeded rows must have been updated (not duplicated).
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_mixed WHERE status = 'old'")
        .await
        .unwrap()
    else {
        panic!("expected Rows from COUNT(*)")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![0],
        "no 'old' rows must remain after upsert"
    );
}

/// ON CONFLICT DO NOTHING with multiple rows — both the all-conflict and mixed
/// cases must work without surfacing a constraint error.
///
/// This exercises the same root-cause path: the `filter_rows_do_nothing`
/// function already handles multi-row batches correctly; this test confirms
/// the wiring in `exec_insert` routes there for multi-row batches too.
#[tokio::test]
async fn bulk_upsert_do_nothing() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE upsert_nothing (id BIGINT NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
    )
    .await
    .unwrap();

    // Seed ids 1..5.
    sess.execute(
        "INSERT INTO upsert_nothing (id, val) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
    )
    .await
    .unwrap();

    // Re-insert ids 1..5 with different values — all must be suppressed.
    let res = sess
        .execute(
            "INSERT INTO upsert_nothing (id, val) VALUES \
             (1,'x'),(2,'x'),(3,'x'),(4,'x'),(5,'x') \
             ON CONFLICT (id) DO NOTHING",
        )
        .await
        .expect("multi-row DO NOTHING on existing rows must not error");
    assert!(
        matches!(res, ExecResult::Empty { .. }),
        "expected Empty result, got {res:?}"
    );

    // Count must still be 5 and values unchanged.
    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_nothing")
        .await
        .unwrap()
    else {
        panic!("expected Rows")
    };
    assert_eq!(col_i64(&batches, "n"), vec![5], "count must stay 5");

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_nothing WHERE val = 'x'")
        .await
        .unwrap()
    else {
        panic!("expected Rows")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![0],
        "DO NOTHING must not overwrite existing values"
    );

    // Mixed: ids 1..5 exist, ids 6..8 are new → 3 new rows inserted.
    let res = sess
        .execute(
            "INSERT INTO upsert_nothing (id, val) VALUES \
             (1,'y'),(2,'y'),(3,'y'),(6,'new'),(7,'new'),(8,'new') \
             ON CONFLICT (id) DO NOTHING",
        )
        .await
        .expect("mixed DO NOTHING must not error");
    assert!(
        matches!(res, ExecResult::Empty { .. }),
        "expected Empty result, got {res:?}"
    );

    let ExecResult::Rows { batches, .. } = sess
        .execute("SELECT COUNT(*) AS n FROM upsert_nothing")
        .await
        .unwrap()
    else {
        panic!("expected Rows")
    };
    assert_eq!(
        col_i64(&batches, "n"),
        vec![8],
        "3 new rows must have been inserted; existing 5 unchanged"
    );
}
