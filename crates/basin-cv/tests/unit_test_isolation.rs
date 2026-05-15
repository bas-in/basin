//! Unit test confirming project isolation across the CV refresh path.
//!
//! Project A registers a CV over its own `events` table. Project B has its own
//! independent `events` table with a different number of rows. After the
//! refresher ticks, A's CV reflects A's row count and B's table is
//! untouched.

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_cv::{CvRefreshOutcome, CvRefresher, CvSpec, TestClock};
use basin_engine::{Engine, EngineConfig, ExecResult};
use chrono::{Duration, TimeZone, Utc};
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

fn count(eng: &Engine, project: ProjectId, sql: &str) -> tokio::task::JoinHandle<i64> {
    let eng = eng.clone();
    let sql = sql.to_string();
    tokio::spawn(async move {
        let s = eng.open_session(project).await.unwrap();
        let res = s.execute(&sql).await.unwrap();
        let mut sum = 0i64;
        match res {
            ExecResult::Rows { batches, .. } => {
                for b in batches {
                    let arr = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap();
                    for i in 0..arr.len() {
                        sum += arr.value(i);
                    }
                }
            }
            other => panic!("unexpected: {other:?}"),
        }
        sum
    })
}

#[tokio::test]
async fn cv_refresh_respects_project_isolation() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let a = ProjectId::new();
    let b = ProjectId::new();

    // Both projects get their own `events` table. A has 3 rows, B has 7.
    for (project, n) in [(a, 3), (b, 7)] {
        let s = eng.open_session(project).await.unwrap();
        s.execute("CREATE TABLE events (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let mut sql = String::from("INSERT INTO events VALUES ");
        for i in 0..n {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({})", i + 1));
        }
        s.execute(&sql).await.unwrap();
    }

    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock = TestClock::new(t0);
    let refresher = CvRefresher::new(eng.clone(), Arc::new(clock.clone()));
    refresher.register_project(a).await;
    // Note: deliberately do NOT register B with the refresher. The point is
    // that A's CV refresh tick must not even open a session against B.

    refresher
        .store()
        .register_cv_at(
            &a,
            CvSpec {
                name: "event_count".into(),
                source_table: "events".into(),
                query_sql: "SELECT count(*) AS c FROM events".into(),
                refresh_interval_secs: 60,
                last_refreshed_at: None,
                last_bucket_max: None,
            },
            t0,
        )
        .await
        .unwrap();

    clock.advance(Duration::seconds(120));
    let outcomes = refresher.tick().await.unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(
        matches!(outcomes[0].outcome, CvRefreshOutcome::Refreshed { .. }),
        "got {outcomes:?}"
    );

    // A's CV reflects A's 3 rows.
    let a_count = count(&eng, a, "SELECT c FROM event_count").await.unwrap();
    assert_eq!(
        a_count, 3,
        "A's CV should reflect A's 3 rows, got {a_count}"
    );

    // B does NOT have an `event_count` table — A's CV must not have leaked
    // into B's namespace.
    let b_obs = eng.open_session(b).await.unwrap();
    let res = b_obs.execute("SHOW TABLES").await.unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut names: Vec<String> = Vec::new();
            for batch in &batches {
                let arr = batch
                    .column_by_name("table_name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                for i in 0..arr.len() {
                    names.push(arr.value(i).to_string());
                }
            }
            assert!(
                !names.iter().any(|n| n == "event_count"),
                "A's CV table leaked into B: {names:?}"
            );
            assert!(
                names.iter().any(|n| n == "events"),
                "B's events table should still exist: {names:?}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    // And B's data is still intact.
    let b_total = count(&eng, b, "SELECT count(*) FROM events").await.unwrap();
    assert_eq!(b_total, 7, "B's events table should still have 7 rows");
}
