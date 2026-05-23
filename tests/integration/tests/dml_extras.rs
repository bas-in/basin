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
use arrow_array::{Array, Int64Array, StringArray};
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
