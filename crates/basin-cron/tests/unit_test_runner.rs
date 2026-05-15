//! Unit tests for the [`basin_cron::CronRunner`] tick loop with a
//! mock clock.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_cron::{CronRunner, JobStatus, TestClock};
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
async fn tick_at_t0_runs_due_minutely_job_once() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let project = ProjectId::new();
    // Anchor the test clock at a known instant. We pick a moment one second
    // before the start of a minute so the next minute boundary lies between
    // T0 and T0+60s.
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 30, 59).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(eng.clone(), Arc::new(clock.clone()));
    runner.register_project(project).await;

    // Seed a side-effect target table for the job to write into.
    let admin = eng.open_session(project).await.unwrap();
    admin
        .execute("CREATE TABLE markers (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Schedule a `* * * * *` job that inserts a marker row.
    runner
        .store()
        .schedule(
            &project,
            "alice",
            "every-minute",
            "* * * * *",
            "INSERT INTO markers VALUES (1)",
        )
        .await
        .unwrap();

    // Advance to T0 + 2s — past the minute boundary at 12:31:00.
    clock.advance(Duration::seconds(2));
    let outcomes = runner.tick().await.unwrap();
    let runs: Vec<_> = outcomes
        .iter()
        .filter(|o| o.status == JobStatus::Succeeded)
        .collect();
    assert_eq!(
        runs.len(),
        1,
        "expected one successful run at T0+2s, got {outcomes:?}"
    );
}

#[tokio::test]
async fn tick_at_t30s_does_not_run_again_within_the_same_minute() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let project = ProjectId::new();
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 31, 5).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(eng.clone(), Arc::new(clock.clone()));
    runner.register_project(project).await;

    let admin = eng.open_session(project).await.unwrap();
    admin
        .execute("CREATE TABLE markers (id BIGINT NOT NULL)")
        .await
        .unwrap();
    runner
        .store()
        .schedule(
            &project,
            "alice",
            "every-minute",
            "* * * * *",
            "INSERT INTO markers VALUES (1)",
        )
        .await
        .unwrap();

    // First tick: cross 12:32:00. Run fires.
    clock.advance(Duration::seconds(60));
    let outcomes = runner.tick().await.unwrap();
    let first_run_count = outcomes
        .iter()
        .filter(|o| o.status == JobStatus::Succeeded)
        .count();
    assert_eq!(first_run_count, 1, "expected first run to fire");

    // Second tick: only 30s later. No new minute boundary crossed.
    clock.advance(Duration::seconds(30));
    let outcomes = runner.tick().await.unwrap();
    let second_run_count = outcomes
        .iter()
        .filter(|o| o.status == JobStatus::Succeeded)
        .count();
    assert_eq!(
        second_run_count, 0,
        "expected zero new runs at T+30s, got outcomes {outcomes:?}"
    );
}

#[tokio::test]
async fn tick_at_t60s_runs_again_after_one_minute() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let project = ProjectId::new();
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 32, 5).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(eng.clone(), Arc::new(clock.clone()));
    runner.register_project(project).await;

    let admin = eng.open_session(project).await.unwrap();
    admin
        .execute("CREATE TABLE markers (id BIGINT NOT NULL)")
        .await
        .unwrap();
    runner
        .store()
        .schedule(
            &project,
            "alice",
            "every-minute",
            "* * * * *",
            "INSERT INTO markers VALUES (1)",
        )
        .await
        .unwrap();

    let mut total_runs = 0usize;
    for _ in 0..3 {
        clock.advance(Duration::seconds(60));
        let outcomes = runner.tick().await.unwrap();
        total_runs += outcomes
            .iter()
            .filter(|o| o.status == JobStatus::Succeeded)
            .count();
    }
    assert_eq!(
        total_runs, 3,
        "expected 3 runs across three +60s ticks, got {total_runs}"
    );
}
