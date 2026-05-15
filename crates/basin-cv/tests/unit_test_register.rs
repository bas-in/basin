//! Unit tests for [`basin_cv::CvStore::register_cv`].
//!
//! Round-trips a CV through register → list → get and asserts the read
//! shape matches what we wrote. Exercises:
//!
//! - The catalog table for the CV is created (it appears in `list_tables`).
//! - The bootstrap query result is written as the first Parquet file.
//! - `last_refreshed_at` is stamped at registration.

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_cv::{CvSpec, CvStore};
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

#[tokio::test]
async fn register_then_list_returns_same_spec() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CvStore::new(eng.clone());

    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let spec = CvSpec {
        name: "event_count".into(),
        source_table: "events".into(),
        // No GROUP BY here so the single-row count is sufficient evidence
        // the query was actually planned and run.
        query_sql: "SELECT count(*) AS c FROM events".into(),
        refresh_interval_secs: 60,
        last_refreshed_at: None,
        last_bucket_max: None,
    };

    store.register_cv(&project, spec.clone()).await.unwrap();

    // Round-trip via list.
    let listed = store.list_cvs(&project).await.unwrap();
    assert_eq!(listed.len(), 1, "exactly one CV should be registered");
    assert_eq!(listed[0].name, spec.name);
    assert_eq!(listed[0].source_table, spec.source_table);
    assert_eq!(listed[0].query_sql, spec.query_sql);
    assert_eq!(listed[0].refresh_interval_secs, spec.refresh_interval_secs);
    assert!(
        listed[0].last_refreshed_at.is_some(),
        "registration must stamp last_refreshed_at"
    );

    // Round-trip via get.
    let got = store.get_cv(&project, &spec.name).await.unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().query_sql, spec.query_sql);
}

#[tokio::test]
async fn registered_cv_appears_as_a_readable_table() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CvStore::new(eng.clone());

    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let spec = CvSpec {
        name: "event_count".into(),
        source_table: "events".into(),
        query_sql: "SELECT count(*) AS c FROM events".into(),
        refresh_interval_secs: 60,
        last_refreshed_at: None,
        last_bucket_max: None,
    };
    store.register_cv(&project, spec).await.unwrap();

    // Open a fresh session so the CV's catalog table is picked up.
    let observer = eng.open_session(project).await.unwrap();
    let res = observer.execute("SELECT c FROM event_count").await.unwrap();
    let total: i64 = match res {
        ExecResult::Rows { batches, .. } => {
            let mut sum = 0i64;
            for b in &batches {
                let arr = b
                    .column_by_name("c")
                    .expect("c column")
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("Int64Array");
                for i in 0..arr.len() {
                    sum += arr.value(i);
                }
            }
            sum
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(total, 3, "bootstrap materialisation should reflect 3 rows");
}

#[tokio::test]
async fn duplicate_name_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CvStore::new(eng.clone());

    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1), (2)")
        .await
        .unwrap();

    let spec = CvSpec {
        name: "event_count".into(),
        source_table: "events".into(),
        query_sql: "SELECT count(*) AS c FROM events".into(),
        refresh_interval_secs: 60,
        last_refreshed_at: None,
        last_bucket_max: None,
    };
    store.register_cv(&project, spec.clone()).await.unwrap();
    let err = store.register_cv(&project, spec).await.unwrap_err();
    assert!(
        matches!(err, basin_cv::RegisterError::Duplicate(_)),
        "got {err:?}"
    );
}
