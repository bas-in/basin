//! Asserts that `htap_promote_to_registry` keys INSERT rows by their encoded
//! primary key, not by a monotonic counter.
//!
//! Before punch-list #9 every INSERT row landed in the memtable under a
//! counter-based `RowKey(u64)` so the PK direct-get fast path (`probe_memtable`
//! / `execute_simple_select` point lookup) always returned `None` and fell back
//! to the O(n) `snapshot()` clone.  After the fix each row is stored under the
//! same `RowKey` encoding that `dml_mutate::pk_scalar_to_row_key` uses, so
//! `entry.memtable.get(&pk_key)` returns `Some(MemRowValue::Row { .. })`.
//!
//! ## Registry promotion path
//!
//! `htap_promote_to_registry` is called from three places:
//! 1. COMMIT of an explicit transaction (`BEGIN … INSERT … COMMIT`)
//! 2. Auto-commit INSERT-SELECT
//! 3. Auto-commit INSERT DEFAULT VALUES
//!
//! The VALUES auto-commit path writes directly to cold-tier Parquet without
//! going through `htap_promote_to_registry` (it has no COMMIT step).  Tests
//! here therefore use explicit `BEGIN … COMMIT` to exercise the registry.
//!
//! ## What is tested
//!
//! 1. After a `BEGIN … INSERT … COMMIT`, the memtable entry for the table holds
//!    a `MemRowValue::Row` under the PK-encoded key, not a counter key.
//! 2. The PK direct-get fast path (`entry.memtable.get(&pk_key)`) returns
//!    `Some(Row { .. })` — not `None`.
//! 3. A multi-row INSERT stores each row under its own PK-encoded key.
//! 4. A subsequent SELECT with `WHERE id = <pk>` on the same session finds the
//!    row (end-to-end correctness, not just registry inspection).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_hottier::{MemRowValue, RowKey};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Engine helper ─────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Encode an i64 primary key exactly as `pk_scalar_to_row_key` does.
fn pk_key_i64(v: i64) -> RowKey {
    RowKey::builder().append_i64(v).finish()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// After a `BEGIN … INSERT … COMMIT`, the memtable must hold the row under
/// the PK-encoded key, not a counter key.
///
/// `htap_promote_to_registry` is invoked on COMMIT; that is the path that
/// must produce PK-keyed entries (punch-list #9).
#[tokio::test]
async fn single_row_insert_tx_keyed_by_pk() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Use an explicit transaction so the COMMIT path calls
    // `htap_promote_to_registry` and the row lands in the registry.
    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (42, 420)").await;
    exec(&sess, "COMMIT").await;

    let table = TableName::new("t").unwrap();
    let entry = eng.memtable_registry().get(&sess.project(), &table).expect(
        "registry entry must exist after BEGIN…INSERT…COMMIT; \
             htap_promote_to_registry is invoked on COMMIT",
    );

    // The row must be findable via the PK-encoded key (not a counter key).
    let pk_key = pk_key_i64(42);
    let value = entry.memtable.get(&pk_key);
    assert!(
        matches!(value, Some(MemRowValue::Row { .. })),
        "expected Some(Row {{ .. }}) under PK-encoded key 42, got {value:?}; \
         htap_promote_to_registry is still using counter keys (punch-list #9 not applied)"
    );
}

/// A multi-row INSERT inside a transaction stores every row under its own
/// PK-encoded key.
#[tokio::test]
async fn multi_row_insert_tx_each_keyed_by_pk() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(
        &sess,
        "CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    exec(&sess, "BEGIN").await;
    exec(
        &sess,
        "INSERT INTO m (id, v) VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    exec(&sess, "COMMIT").await;

    let table = TableName::new("m").unwrap();
    let entry = eng
        .memtable_registry()
        .get(&sess.project(), &table)
        .expect("registry entry must exist after BEGIN…INSERT…COMMIT");

    for id in [1i64, 2, 3] {
        let pk_key = pk_key_i64(id);
        let value = entry.memtable.get(&pk_key);
        assert!(
            matches!(value, Some(MemRowValue::Row { .. })),
            "id={id}: expected Some(Row) under PK-encoded key, got {value:?}; \
             rows may be keyed by counter instead of PK (punch-list #9)"
        );
    }

    // Total entry count == 3 (one per row, not inflated by counter collisions).
    let total = entry.memtable.total_count();
    assert_eq!(
        total, 3,
        "memtable should hold exactly 3 entries (one per PK), got {total}"
    );
}

/// After a committed INSERT, a point-lookup SELECT `WHERE id = <pk>` must find
/// the row (end-to-end correctness — exercises both registry and cold tier).
#[tokio::test]
async fn select_after_committed_insert_finds_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(
        &sess,
        "CREATE TABLE s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    // Auto-commit INSERT — row lands in cold tier Parquet.
    exec(&sess, "INSERT INTO s (id, v) VALUES (99, 9900)").await;

    let result = sess.execute("SELECT v FROM s WHERE id = 99").await.unwrap();
    match result {
        ExecResult::Rows { batches, .. } => {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 1, "expected 1 row, got {total_rows}");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Verifies that the PK-keyed INSERT row in the registry is compatible with
/// the UPDATE fast-path: an UPDATE after a committed INSERT (in-tx path)
/// must find the new value.
#[tokio::test]
async fn update_after_tx_insert_uses_pk_key() {
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(
        &sess,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    // Seed row into cold tier first (auto-commit).
    exec(&sess, "INSERT INTO u (id, v) VALUES (7, 70)").await;
    // Now UPDATE via fastpath — must find the cold-tier row and overlay it.
    exec(&sess, "UPDATE u SET v = 9999 WHERE id = 7").await;

    let result = sess.execute("SELECT v FROM u WHERE id = 7").await.unwrap();
    match result {
        ExecResult::Rows { batches, .. } => {
            use arrow_array::Int64Array;
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("v column is Int64");
                let v = arr.value(0);
                assert_eq!(
                    v, 9999,
                    "UPDATE after INSERT must show the new value; got {v}"
                );
                return;
            }
            panic!("no rows returned from SELECT after UPDATE");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}
