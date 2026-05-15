//! Viability test: pg_cron-compatible scheduler.
//!
//! Card: `viability_basin_cron`
//! Bar: `runs_executed == ticks` (3 == 3).
//!
//! Schedules a `* * * * *` cron job in project A. The job inserts one row
//! into a `markers` table on each fire. We drive three simulated ticks
//! 60 seconds apart via [`basin_cron::TestClock`] and assert:
//!
//! 1. `markers` ends up with exactly 3 rows.
//! 2. `cron_job_run_details` ends up with exactly 3 successful entries.
//!
//! What this proves: the scheduler ticks are coalesced correctly across
//! 60-second intervals, the job's SQL runs through a real `Engine` session
//! against the project's namespace, and the audit log mirrors the real run
//! count one-for-one.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Int64Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_cron::{CronRunner, JobStatus, TestClock};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use chrono::{Duration, TimeZone, Utc};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn viability_basin_cron() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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

    let project = ProjectId::new();

    // Anchor the test clock one second before a minute boundary so the
    // very first +60s advance crosses exactly one boundary, fires once,
    // and lands one second past the next minute. Subsequent +60s advances
    // each cross exactly one further boundary.
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 30, 59).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(engine.clone(), Arc::new(clock.clone()));
    runner.register_project(project).await;

    // Seed a `markers` table the scheduled job will append into.
    let admin = engine.open_session(project).await.unwrap();
    admin
        .execute("CREATE TABLE markers (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Schedule a `* * * * *` job that inserts one marker row per fire.
    let jobid = runner
        .store()
        .schedule(
            &project,
            "alice",
            "tick-marker",
            "* * * * *",
            "INSERT INTO markers VALUES (1)",
        )
        .await
        .unwrap();
    assert!(jobid >= 1);

    // Drive 3 ticks 60 seconds apart.
    const TICKS: usize = 3;
    let mut runs_executed = 0usize;
    for _ in 0..TICKS {
        clock.advance(Duration::seconds(60));
        let outcomes = runner.tick().await.unwrap();
        runs_executed += outcomes
            .iter()
            .filter(|o| o.status == JobStatus::Succeeded)
            .count();
    }

    // Marker rows. Reopen the session to pick up the new commit.
    let observer = engine.open_session(project).await.unwrap();
    let res = observer.execute("SELECT id FROM markers").await.unwrap();
    let marker_rows = match res {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            // Sanity: every marker is `1`.
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    assert_eq!(arr.value(i), 1);
                }
            }
            total
        }
        other => panic!("unexpected: {other:?}"),
    };

    // Audit rows.
    let details = runner.store().list_run_details(&project).await.unwrap();
    let succeeded = details
        .iter()
        .filter(|d| d.status == JobStatus::Succeeded)
        .count();

    let pass = runs_executed == TICKS && marker_rows == TICKS && succeeded == TICKS;
    println!(
        "[VIABILITY basin_cron] ticks={TICKS} runs_executed={runs_executed} markers={marker_rows} \
         audit_succeeded={succeeded} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "basin_cron",
        "Cron-style scheduler runs jobs at minute boundaries",
        "Three +60s ticks fire a `* * * * *` job exactly three times; markers and audit log match.",
        pass,
        PrimaryMetric {
            label: "runs_executed".into(),
            value: runs_executed as f64,
            unit: "runs".into(),
            bar: BarOp::eq(TICKS as f64),
        },
        json!({
            "ticks": TICKS,
            "runs_executed": runs_executed,
            "marker_rows": marker_rows,
            "audit_succeeded": succeeded,
        }),
    );

    assert!(pass);
}
