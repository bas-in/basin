//! BUG #132 — `CREATE TRIGGER` / `DROP TRIGGER` honest-reject.
//!
//! Basin has no PL/pgSQL trigger runtime (ADR 0012). Before this fix
//! `CREATE TRIGGER` and `DROP TRIGGER` were silently accepted as no-ops,
//! so apps relying on triggers (audit rows, derived columns, validation)
//! silently got wrong data with no error. The honest behaviour, matching
//! how MERGE / DEFERRABLE / EXCLUDE were handled, is:
//!
//!   * `CREATE TRIGGER`             → Err (feature not supported, 0A000)
//!   * `CREATE CONSTRAINT TRIGGER`  → Err (feature not supported, 0A000)
//!   * `DROP TRIGGER ... IF EXISTS` → Ok  (faithful PG no-op: nothing to drop)
//!   * `DROP TRIGGER` (no IF EXISTS)→ Err (PG: "trigger ... does not exist")

use std::sync::Arc;

use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig};
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
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

#[tokio::test]
async fn create_trigger_is_honestly_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id int, audited bool)")
        .await
        .unwrap();

    let err = sess
        .execute(
            "CREATE TRIGGER trg AFTER INSERT ON t \
             FOR EACH ROW EXECUTE FUNCTION audit_fn()",
        )
        .await
        .expect_err("CREATE TRIGGER must NOT be silently accepted (BUG #132)");

    match err {
        BasinError::FeatureNotSupported(msg) => {
            assert!(
                msg.to_ascii_uppercase().contains("CREATE TRIGGER")
                    && msg.contains("not supported"),
                "expected a clear 'CREATE TRIGGER not supported' message, got: {msg}"
            );
            assert!(msg.contains("0A000"), "expected SQLSTATE 0A000, got: {msg}");
        }
        other => panic!("expected FeatureNotSupported, got {other:?}"),
    }
}

#[tokio::test]
async fn create_constraint_trigger_is_honestly_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id int)").await.unwrap();

    let err = sess
        .execute(
            "CREATE CONSTRAINT TRIGGER ct AFTER INSERT ON t \
             FOR EACH ROW EXECUTE FUNCTION check_fn()",
        )
        .await
        .expect_err("CREATE CONSTRAINT TRIGGER must NOT be silently accepted");

    match err {
        BasinError::FeatureNotSupported(msg) => {
            assert!(
                msg.contains("CREATE CONSTRAINT TRIGGER") && msg.contains("not supported"),
                "expected 'CREATE CONSTRAINT TRIGGER not supported', got: {msg}"
            );
            assert!(msg.contains("0A000"), "expected SQLSTATE 0A000, got: {msg}");
        }
        other => panic!("expected FeatureNotSupported, got {other:?}"),
    }
}

#[tokio::test]
async fn drop_trigger_if_exists_is_a_silent_noop() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id int)").await.unwrap();

    // PG-faithful: nothing to drop, no error.
    sess.execute("DROP TRIGGER IF EXISTS trg ON t")
        .await
        .expect("DROP TRIGGER IF EXISTS must succeed as a no-op");
}

#[tokio::test]
async fn drop_trigger_without_if_exists_errors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id int)").await.unwrap();

    // PG raises: trigger "trg" does not exist. Basin mirrors that because no
    // trigger can ever exist (no trigger runtime).
    let err = sess
        .execute("DROP TRIGGER trg ON t")
        .await
        .expect_err("bare DROP TRIGGER must error (PG: 'does not exist')");

    match err {
        BasinError::FeatureNotSupported(msg) => {
            assert!(
                msg.contains("does not exist"),
                "expected 'does not exist' semantics, got: {msg}"
            );
        }
        other => panic!("expected FeatureNotSupported, got {other:?}"),
    }
}
