//! `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` end-to-end through the SQL
//! surface. PG-shaped semantics: the column is an integer that
//! auto-increments on INSERT via an implicitly-created sequence named
//! `<table>_<col>_seq`, with `NOT NULL` and `DEFAULT nextval(...)`.
//!
//! The pseudo-type expansion lives in `basin_engine::ddl`; the
//! implicit sequence is registered by `executor::create_table` after
//! the catalog `create_table` call returns. This test pins the full
//! round-trip so a future refactor can't silently regress the
//! invariant.
//!
//! Phase 5.14 made the engine PG-faithful on serial widths:
//!   SMALLSERIAL / SERIAL2  → Int16 (INT2)
//!   SERIAL / SERIAL4       → Int32 (INT4)
//!   BIGSERIAL / SERIAL8    → Int64 (INT8)

use std::sync::Arc;

use arrow_array::{Array, Int16Array, Int32Array, Int64Array};
use basin_catalog::InMemoryCatalog;
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
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Pull the first row's `id` column as i64 from a BIGSERIAL/BIGINT column.
fn first_id_i64(res: ExecResult) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is BIGINT (Int64)");
            arr.value(0)
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Pull the first row's `id` column as i32 from a SERIAL/INT4 column.
fn first_id_i32(res: ExecResult) -> i32 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is INT4 (Int32)");
            arr.value(0)
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Pull the first row's `id` column as i16 from a SMALLSERIAL/INT2 column.
fn first_id_i16(res: ExecResult) -> i16 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let b = &batches[0];
            let arr = b
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("id is INT2 (Int16)");
            arr.value(0)
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn serial_column_creates_sequence_and_autonumbers() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // SERIAL pseudo-type expands to INT4 + sequence + DEFAULT nextval
    // (PG-faithful: SERIAL → int4, OID 23).
    sess.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    // The implicit `orders_id_seq` sequence must be reachable to a
    // bare `nextval` call right after CREATE TABLE — proves the
    // sequence was registered, not just promised on the column.
    // nextval returns INT8 regardless of the column backing width.
    let v = first_id_i64(
        sess.execute("SELECT nextval('orders_id_seq') AS id")
            .await
            .unwrap(),
    );
    assert_eq!(v, 1, "first nextval on an implicit SERIAL sequence is 1");

    // Two INSERTs without an explicit id; the DEFAULT must fire and
    // hand out two distinct, increasing ids. The first `nextval` above
    // already consumed `1`, so these land at 2 and 3.
    sess.execute("INSERT INTO orders (name) VALUES ('a')")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders (name) VALUES ('b')")
        .await
        .unwrap();

    let rows = match sess
        .execute("SELECT id, name FROM orders ORDER BY id")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows, got {other:?}"),
    };
    // SERIAL is INT4-backed (Int32) since Phase 5.14 PG-faithfulness update.
    let ids = rows[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 3);
}

#[tokio::test]
async fn bigserial_is_int64_backed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE evt (id BIGSERIAL, payload TEXT)")
        .await
        .unwrap();

    sess.execute("INSERT INTO evt (payload) VALUES ('x')")
        .await
        .unwrap();
    let v = first_id_i64(sess.execute("SELECT id FROM evt LIMIT 1").await.unwrap());
    assert_eq!(v, 1);
}

#[tokio::test]
async fn smallserial_works_end_to_end() {
    // SMALLSERIAL is INT2-backed (Int16) since Phase 5.14 PG-faithfulness
    // update. The PG-shaped pseudo-type behaviour (sequence + DEFAULT +
    // NOT NULL) is still in force.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE tiny (id SMALLSERIAL, label TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO tiny (label) VALUES ('first')")
        .await
        .unwrap();

    // SMALLSERIAL → Int16; use first_id_i16 to match the backing width.
    let v = first_id_i16(sess.execute("SELECT id FROM tiny LIMIT 1").await.unwrap());
    assert_eq!(v, 1);
}

#[tokio::test]
async fn serial_implies_not_null() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id SERIAL, n TEXT)")
        .await
        .unwrap();
    // Explicit NULL for `id` must be rejected — PG forbids NULL on a
    // SERIAL column. (Omitting the column entirely is fine; that path
    // hits the DEFAULT.)
    let err = sess
        .execute("INSERT INTO t (id, n) VALUES (NULL, 'oops')")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_ascii_lowercase().contains("null") || msg.to_ascii_lowercase().contains("not null"),
        "unexpected error on NULL into SERIAL: {msg}"
    );
}

#[tokio::test]
async fn serial_aliases_serial4_serial8_serial2() {
    // SERIAL4 / SERIAL8 / SERIAL2 are PG aliases for SERIAL / BIGSERIAL
    // / SMALLSERIAL respectively. Each must produce a working table.
    for (alias, table) in [("SERIAL4", "t4"), ("SERIAL8", "t8"), ("SERIAL2", "t2")] {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let ddl = format!("CREATE TABLE {table} (id {alias}, n TEXT)");
        sess.execute(&ddl)
            .await
            .unwrap_or_else(|e| panic!("alias {alias}: {e}"));
        sess.execute(&format!("INSERT INTO {table} (n) VALUES ('a')"))
            .await
            .unwrap_or_else(|e| panic!("alias {alias} insert: {e}"));
    }
}
