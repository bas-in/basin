//! S3 port of `viability_basin_cron.rs`.
//!
//! Card: `viability_basin_cron` (real-cloud dashboard).
//! Bar: `runs_executed == ticks` (3 == 3).
//!
//! Same setup as LocalFS but the `markers` and `cron_job_run_details` rows
//! land in Parquet on a real S3-compatible bucket. We drive three simulated
//! ticks via `TestClock` and verify that:
//!
//! 1. The job inserts exactly 3 marker rows (one per tick).
//! 2. The audit log records 3 successful runs.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Int64Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_cron::{CronRunner, JobStatus, TestClock};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use chrono::{Duration, TimeZone, Utc};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_basin_cron";

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_viability_basin_cron() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
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
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 30, 59).unwrap();
    let clock = TestClock::new(t0);
    let runner = CronRunner::new(engine.clone(), Arc::new(clock.clone()));
    runner.register_project(project).await;

    let admin = engine.open_session(project).await.unwrap();
    admin
        .execute("CREATE TABLE markers (id BIGINT NOT NULL)")
        .await
        .unwrap();

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

    let observer = engine.open_session(project).await.unwrap();
    let res = observer.execute("SELECT id FROM markers").await.unwrap();
    let marker_rows = match res {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
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

    let details = runner.store().list_run_details(&project).await.unwrap();
    let succeeded = details
        .iter()
        .filter(|d| d.status == JobStatus::Succeeded)
        .count();

    let pass = runs_executed == TICKS && marker_rows == TICKS && succeeded == TICKS;
    println!(
        "[S3 viability_basin_cron] ticks={TICKS} runs_executed={runs_executed} markers={marker_rows} \
         audit_succeeded={succeeded} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "basin_cron",
        "Cron-style scheduler runs jobs at minute boundaries (real S3)",
        "Three +60s ticks fire a `* * * * *` job exactly three times against a real S3-backed Storage; markers and audit log match.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(pass);
}
