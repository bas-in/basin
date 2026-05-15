//! Integration test for the full PG `CREATE SEQUENCE` grammar through the
//! engine's textual pre-screen.
//!
//! sqlparser 0.52 only parses one option per `CREATE SEQUENCE` statement;
//! the engine pre-screens the statement before sqlparser sees it so the
//! full PG grammar (`START 100 INCREMENT 5 MINVALUE 1 MAXVALUE 1000 …`)
//! routes to `Catalog::create_sequence`. This test proves the round-trip
//! end-to-end through the SQL surface.

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

fn first_i64(res: ExecResult) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            assert!(!batches.is_empty());
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column 0 is BIGINT");
            assert!(!arr.is_empty());
            arr.value(0)
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn create_sequence_full_grammar_round_trip() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // Multi-option CREATE SEQUENCE: this is the form sqlparser 0.52
    // cannot parse natively (it bails at the second option). The
    // textual pre-screen routes the full statement to the catalog.
    sess.execute("CREATE SEQUENCE order_id_seq START 1000 INCREMENT 5 MINVALUE 1000 MAXVALUE 9999")
        .await
        .unwrap();

    let v1 = first_i64(
        sess.execute("SELECT nextval('order_id_seq')")
            .await
            .unwrap(),
    );
    assert_eq!(v1, 1000);
    let v2 = first_i64(
        sess.execute("SELECT nextval('order_id_seq')")
            .await
            .unwrap(),
    );
    assert_eq!(v2, 1005);
    let v3 = first_i64(
        sess.execute("SELECT nextval('order_id_seq')")
            .await
            .unwrap(),
    );
    assert_eq!(v3, 1010);
}

#[tokio::test]
async fn create_sequence_if_not_exists_idempotent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE SEQUENCE foo START 10 INCREMENT 2")
        .await
        .unwrap();
    // Second create with IF NOT EXISTS must not error.
    sess.execute("CREATE SEQUENCE IF NOT EXISTS foo START 10 INCREMENT 2")
        .await
        .unwrap();
    // Sequence state survives the no-op create; first nextval returns 10.
    let v = first_i64(sess.execute("SELECT nextval('foo')").await.unwrap());
    assert_eq!(v, 10);
}

#[tokio::test]
async fn create_temporary_sequence_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess
        .execute("CREATE TEMPORARY SEQUENCE foo START 1")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("TEMPORARY") || msg.contains("project-scoped"),
        "unexpected error: {msg}"
    );
}
