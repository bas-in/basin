//! Unit tests for [`basin_cv::CvRefresher::tick`] with a mock clock.
//!
//! Asserts the interval gate: a tick within `refresh_interval_secs` of the
//! last refresh is a no-op (`NotDue`); a tick at or after the next interval
//! re-materialises the CV.

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_cv::{CvRefreshOutcome, CvRefresher, CvSpec, TestClock};
use basin_engine::{Engine, EngineConfig};
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

#[tokio::test]
async fn tick_within_interval_is_not_due() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1), (2), (3)")
        .await
        .unwrap();

    // Anchor the clock at a known instant. Registration stamps this as
    // `last_refreshed_at`.
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock = TestClock::new(t0);
    let refresher = CvRefresher::new(eng.clone(), Arc::new(clock.clone()));
    refresher.register_tenant(tenant).await;

    let spec = CvSpec {
        name: "event_count".into(),
        source_table: "events".into(),
        query_sql: "SELECT count(*) AS c FROM events".into(),
        refresh_interval_secs: 3600, // 1h
        last_refreshed_at: None,
        last_bucket_max: None,
    };
    refresher
        .store()
        .register_cv_at(&tenant, spec, t0)
        .await
        .unwrap();

    // Advance 30 minutes — half the interval. Tick must be a no-op.
    clock.advance(Duration::seconds(30 * 60));
    let outcomes = refresher.tick().await.unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(
        matches!(outcomes[0].outcome, CvRefreshOutcome::NotDue),
        "expected NotDue at +30min, got {outcomes:?}"
    );

    // Advance another 31 minutes — total 61 min, past the 1-hour interval.
    clock.advance(Duration::seconds(31 * 60));
    let outcomes = refresher.tick().await.unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(
        matches!(outcomes[0].outcome, CvRefreshOutcome::Refreshed { .. }),
        "expected Refreshed at +61min, got {outcomes:?}"
    );
}

#[tokio::test]
async fn refresh_picks_up_new_rows() {
    // After the first refresh, insert more rows; the next refresh must
    // observe them.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1), (2), (3)")
        .await
        .unwrap();

    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock = TestClock::new(t0);
    let refresher = CvRefresher::new(eng.clone(), Arc::new(clock.clone()));
    refresher.register_tenant(tenant).await;

    refresher
        .store()
        .register_cv_at(
            &tenant,
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

    // Insert more rows.
    let writer = eng.open_session(tenant).await.unwrap();
    writer
        .execute("INSERT INTO events VALUES (4), (5), (6), (7)")
        .await
        .unwrap();

    // Advance past the interval and tick.
    clock.advance(Duration::seconds(120));
    let outcomes = refresher.tick().await.unwrap();
    assert_eq!(outcomes.len(), 1);
    let rows_written = match &outcomes[0].outcome {
        CvRefreshOutcome::Refreshed { rows_written } => *rows_written,
        other => panic!("expected Refreshed, got {other:?}"),
    };
    assert_eq!(rows_written, 1, "count(*) returns one row");

    // Read the CV and check the count is 7 now.
    let observer = eng.open_session(tenant).await.unwrap();
    let res = observer.execute("SELECT c FROM event_count").await.unwrap();
    let total: i64 = match res {
        basin_engine::ExecResult::Rows { batches, .. } => {
            let mut sum = 0i64;
            for b in &batches {
                let arr = b
                    .column_by_name("c")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    sum += arr.value(i);
                }
            }
            sum
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(
        total, 7,
        "refresh should reflect the 7 events now in the table"
    );
}
