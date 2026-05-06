//! Unit tests confirming tenant isolation across the cron surface.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_cron::{CronRunner, TestClock};
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
async fn each_tenant_only_sees_its_own_jobs() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let runner = CronRunner::with_system_clock(eng);
    let store = runner.store();

    let a = TenantId::new();
    let b = TenantId::new();

    store
        .schedule(&a, "alice", "a-job", "* * * * *", "SELECT 'a'")
        .await
        .unwrap();
    store
        .schedule(&b, "bob", "b-job", "0 0 * * *", "SELECT 'b'")
        .await
        .unwrap();

    let a_jobs = store.list_jobs(&a).await.unwrap();
    let b_jobs = store.list_jobs(&b).await.unwrap();

    assert_eq!(a_jobs.len(), 1);
    assert_eq!(a_jobs[0].jobname, "a-job");
    assert_eq!(a_jobs[0].command, "SELECT 'a'");

    assert_eq!(b_jobs.len(), 1);
    assert_eq!(b_jobs[0].jobname, "b-job");
    assert_eq!(b_jobs[0].command, "SELECT 'b'");

    for j in &a_jobs {
        assert!(!j.command.contains('b'), "A leaked into B: {j:?}");
    }
    for j in &b_jobs {
        assert!(!j.command.contains('a'), "B leaked into A: {j:?}");
    }
}

#[tokio::test]
async fn jobs_only_run_against_their_owning_tenant() {
    // Tenant A schedules an INSERT into a `markers` table. Tenant B has its
    // own `markers` table. After ticking, A's table has one row and B's
    // has zero — the runner must never have touched B's table.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 1).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(eng.clone(), Arc::new(clock.clone()));

    let a = TenantId::new();
    let b = TenantId::new();
    runner.register_tenant(a).await;
    runner.register_tenant(b).await;

    // Both tenants get a `markers` table, both empty.
    for tenant in [a, b] {
        let s = eng.open_session(tenant).await.unwrap();
        s.execute("CREATE TABLE markers (id BIGINT NOT NULL)")
            .await
            .unwrap();
    }

    // Only A schedules a job.
    runner
        .store()
        .schedule(
            &a,
            "alice",
            "every-minute",
            "* * * * *",
            "INSERT INTO markers VALUES (1)",
        )
        .await
        .unwrap();

    // Cross a minute boundary.
    clock.advance(Duration::seconds(60));
    let _outcomes = runner.tick().await.unwrap();

    // A's `markers` should have exactly one row; B's should be empty.
    let count_markers = |tenant: TenantId| {
        let eng = eng.clone();
        async move {
            let s = eng.open_session(tenant).await.unwrap();
            let res = s.execute("SELECT id FROM markers").await.unwrap();
            match res {
                basin_engine::ExecResult::Rows { batches, .. } => {
                    batches.iter().map(|b| b.num_rows()).sum::<usize>()
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    };

    let a_count = count_markers(a).await;
    let b_count = count_markers(b).await;
    assert_eq!(a_count, 1, "A should have exactly 1 marker, got {a_count}");
    assert_eq!(b_count, 0, "B should have 0 markers, got {b_count}");
}
