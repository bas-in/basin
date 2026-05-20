//! Integration test for the `CREATE/REFRESH/DROP MATERIALIZED VIEW` SQL
//! surface.
//!
//! Round-trips a continuous aggregate end-to-end through the engine's SQL
//! entry point. The Rust API in `basin-cv` is exercised by its own
//! `tests/unit_test_*` files; this test only proves the SQL surface
//! routes correctly to the same catalog calls.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
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

#[tokio::test]
async fn create_refresh_select_drop_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // 1. Source table.
    sess.execute("CREATE TABLE events (bucket BIGINT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 10), (1, 20), (2, 30), (2, 40), (3, 5)")
        .await
        .unwrap();

    // 2. CREATE MATERIALIZED VIEW with the Basin continuous-aggregate
    //    options. The WITH clause uses a dotted-key flag (`basin.continuous`)
    //    that sqlparser cannot ingest natively — the engine pre-screens it.
    let res = sess
        .execute(
            "CREATE MATERIALIZED VIEW mv \
             WITH (basin.continuous, refresh_interval = '5m') \
             AS SELECT bucket, sum(n) AS total FROM events GROUP BY bucket",
        )
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE MATERIALIZED VIEW"),
        other => panic!("unexpected result: {other:?}"),
    }

    // 3. The CV is readable as a regular table immediately after creation
    //    (the bootstrap rows are materialised at registration time).
    let res = sess
        .execute("SELECT bucket, total FROM mv ORDER BY bucket")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "bucket"), vec![1, 2, 3]);
    assert_eq!(col_i64(&batches, "total"), vec![30, 70, 5]);

    // 4. Insert a row that changes one bucket's aggregate; SELECT against
    //    the CV still returns the pre-refresh materialisation.
    sess.execute("INSERT INTO events VALUES (1, 100)")
        .await
        .unwrap();
    let pre_refresh = sess
        .execute("SELECT total FROM mv WHERE bucket = 1")
        .await
        .unwrap();
    let pre = match pre_refresh {
        ExecResult::Rows { batches, .. } => col_i64(&batches, "total"),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        pre,
        vec![30],
        "before REFRESH the CV must still show the bootstrap aggregate"
    );

    // 5. REFRESH MATERIALIZED VIEW re-materialises the source query.
    let res = sess.execute("REFRESH MATERIALIZED VIEW mv").await.unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "REFRESH MATERIALIZED VIEW"),
        other => panic!("unexpected: {other:?}"),
    }
    let post_refresh = sess
        .execute("SELECT total FROM mv WHERE bucket = 1")
        .await
        .unwrap();
    let post = match post_refresh {
        ExecResult::Rows { batches, .. } => col_i64(&batches, "total"),
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        post,
        vec![130],
        "after REFRESH the CV must reflect the new INSERT"
    );

    // 6. DROP MATERIALIZED VIEW removes the view; subsequent SELECTs error.
    sess.execute("DROP MATERIALIZED VIEW mv").await.unwrap();
    let err = sess.execute("SELECT * FROM mv").await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("mv") || msg.to_ascii_lowercase().contains("not"),
        "expected a 'relation does not exist'-shaped error after DROP, got: {msg}"
    );
}

#[tokio::test]
async fn create_snapshot_materialized_view_succeeds() {
    // A plain `CREATE MATERIALIZED VIEW` (no `WITH (basin.continuous)`) is a
    // one-shot snapshot: it materialises the source query immediately and
    // stores the result as a regular table.  The view is queryable right away.
    // `REFRESH MATERIALIZED VIEW` is NOT supported for snapshot views (no
    // CvDef is stored); the engine correctly rejects it.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1), (2), (3)")
        .await
        .unwrap();

    // CREATE MATERIALIZED VIEW without basin.continuous succeeds (snapshot path).
    let res = sess
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM events")
        .await
        .unwrap();
    match res {
        ExecResult::Empty { tag } => assert_eq!(tag, "CREATE MATERIALIZED VIEW"),
        other => panic!("expected Empty{{tag}}, got: {other:?}"),
    }

    // The snapshot rows are immediately queryable.
    let res = sess
        .execute("SELECT id FROM mv ORDER BY id")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got: {other:?}"),
    };
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);

    // REFRESH MATERIALIZED VIEW is rejected for snapshot views because no
    // CvDef (continuous-aggregate definition) was stored — the engine has no
    // source query to re-execute automatically.
    let err = sess
        .execute("REFRESH MATERIALIZED VIEW mv")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("not a continuous aggregate") || msg.contains("continuous"),
        "expected REFRESH to reject snapshot view, got: {err}"
    );
}

#[tokio::test]
async fn refresh_on_non_existent_view_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess
        .execute("REFRESH MATERIALIZED VIEW does_not_exist")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("does_not_exist") || msg.contains("not"),
        "expected 'does not exist'-shaped error, got: {err}"
    );
}

#[tokio::test]
async fn drop_if_exists_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // DROP IF EXISTS on a non-existent view returns Ok.
    sess.execute("DROP MATERIALIZED VIEW IF EXISTS not_there")
        .await
        .unwrap();
}
