//! Unit tests for [`basin_cron::CronStore`] schedule / unschedule.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_cron::{CronStore, ScheduleError};
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
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

#[tokio::test]
async fn schedule_inserts_a_row_in_cron_job() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CronStore::new(eng);
    let tenant = TenantId::new();

    let jobid = store
        .schedule(
            &tenant,
            "alice",
            "audit-cleanup",
            "0 3 * * *",
            "DELETE FROM events WHERE id < 100",
        )
        .await
        .unwrap();
    assert!(jobid >= 1, "first jobid should be >= 1, got {jobid}");

    let jobs = store.list_jobs(&tenant).await.unwrap();
    assert_eq!(jobs.len(), 1, "expected exactly one job");
    let job = &jobs[0];
    assert_eq!(job.jobname, "audit-cleanup");
    assert_eq!(job.schedule, "0 3 * * *");
    assert_eq!(job.command, "DELETE FROM events WHERE id < 100");
    assert!(job.active);
    assert!(job.last_run.is_none());
    assert!(job.last_status.is_none());
}

#[tokio::test]
async fn unschedule_removes_the_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CronStore::new(eng);
    let tenant = TenantId::new();

    store
        .schedule(&tenant, "alice", "tmp", "* * * * *", "SELECT 1")
        .await
        .unwrap();
    assert_eq!(store.list_jobs(&tenant).await.unwrap().len(), 1);

    let jobid = store.unschedule(&tenant, "tmp").await.unwrap();
    assert!(jobid >= 1);
    let jobs = store.list_jobs(&tenant).await.unwrap();
    assert!(
        jobs.is_empty(),
        "expected zero jobs after unschedule, got {jobs:?}"
    );
}

#[tokio::test]
async fn invalid_cron_expression_is_rejected_at_schedule_time() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CronStore::new(eng);
    let tenant = TenantId::new();

    let err = store
        .schedule(
            &tenant,
            "alice",
            "bad",
            "this is not a cron expression",
            "SELECT 1",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ScheduleError::InvalidExpression { .. }));

    // Make sure no row leaked into cron_job after the rejection.
    let jobs = store.list_jobs(&tenant).await.unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn duplicate_jobname_is_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CronStore::new(eng);
    let tenant = TenantId::new();

    store
        .schedule(&tenant, "alice", "dupe", "* * * * *", "SELECT 1")
        .await
        .unwrap();
    let err = store
        .schedule(&tenant, "alice", "dupe", "0 0 * * *", "SELECT 2")
        .await
        .unwrap_err();
    assert!(matches!(err, ScheduleError::Duplicate(_)));

    // Only the original row should exist.
    let jobs = store.list_jobs(&tenant).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].command, "SELECT 1");
}

#[tokio::test]
async fn unschedule_unknown_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let store = CronStore::new(eng);
    let tenant = TenantId::new();

    let err = store.unschedule(&tenant, "ghost").await.unwrap_err();
    assert!(matches!(err, ScheduleError::NotFound(_)));
}
