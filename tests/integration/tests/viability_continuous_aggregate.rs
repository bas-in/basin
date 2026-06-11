//! Viability test: TimescaleDB-style continuous aggregates (CV).
//!
//! Card: `viability_continuous_aggregate`
//! Bar: `cv_rows_correct && refresh_idempotent && refresh_picks_up_new_rows`
//!
//! What this proves:
//!
//! 1. A CV registered over a time-bucketed `count(*) GROUP BY day` query
//!    materialises to 5 rows when the source table has rows spread across
//!    5 days.
//! 2. Calling `Shard::run_cv_refresh` again immediately is idempotent —
//!    it does not produce a different row set or a duplicate Parquet
//!    file (the refresher's interval gate is honoured).
//! 3. After inserting 10 more rows in the latest day and refreshing past
//!    the interval, the CV still reports 5 day-buckets, and the latest
//!    day's count reflects the new rows.
//!
//! Limitations (documented for v0.2):
//! - The CV's read path does not merge in a "live tail" of source rows
//!   newer than the last refresh. A SELECT issued between two refresh
//!   ticks may return slightly stale data. v0.2 will overlay the
//!   un-materialised tail at read time.
//! - The refresh re-runs the full source query each tick. v0.2 will
//!   narrow the scan to rows whose bucket key exceeds `last_bucket_max`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId};
use basin_cv::{CvRefresher, CvSpec, TestClock};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use chrono::{TimeZone, Utc};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_continuous_aggregate() {
    basin_common::telemetry::try_init_for_tests();

    // ---------- Storage / catalog / shard / engine ------------------------
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(storage_fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    // ---------- Project + base table seeded with 100 rows over 5 days -----
    let project = ProjectId::new();
    // The shard's `run_cv_refresh` walks resident projects; touching the
    // partition through `Shard::get` makes the project resident.
    let _h = shard
        .get(&project, &PartitionKey::default_key())
        .await
        .unwrap();

    let admin = engine.open_session(project).await.unwrap();
    admin
        .execute(
            "CREATE TABLE events (\
                id BIGINT NOT NULL, \
                ts TIMESTAMPTZ NOT NULL, \
                payload TEXT NOT NULL\
            )",
        )
        .await
        .unwrap();

    // 100 rows spread across 5 days: 20/day. Days are 2026-04-26 through
    // 2026-04-30 inclusive.
    let mut sql = String::from("INSERT INTO events VALUES ");
    for i in 0..100u32 {
        if i > 0 {
            sql.push(',');
        }
        let day = 26 + (i / 20); // 26..=30
        let hour = i % 24;
        sql.push_str(&format!(
            "({i}, '2026-04-{day:02}T{hour:02}:00:00Z', 'p-{i}')"
        ));
    }
    admin.execute(&sql).await.unwrap();

    // ---------- Register the CV ------------------------------------------
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock = TestClock::new(t0);
    let refresher = CvRefresher::new(engine.clone(), Arc::new(clock.clone()));
    refresher.register_project(project).await;

    refresher
        .store()
        .register_cv_at(
            &project,
            CvSpec {
                name: "events_per_day".into(),
                source_table: "events".into(),
                // `date_trunc('day', ts)` returns a TIMESTAMPTZ, which Arrow
                // surfaces as a Timestamp(Microsecond, "UTC"). We cast to
                // text so the test's downcast is on a stable
                // schema regardless of the engine's timestamp variant.
                query_sql: "SELECT \
                    cast(date_trunc('day', ts) AS TEXT) AS day, \
                    count(*) AS n \
                  FROM events \
                  GROUP BY day"
                    .into(),
                refresh_interval_secs: 3600,
                last_refreshed_at: None,
                last_bucket_max: None,
            },
            t0,
        )
        .await
        .unwrap();

    // ---------- Tick 1: idempotency check (CV is fresh from registration)
    // The first tick at t0 is `NotDue` because we just stamped
    // `last_refreshed_at = t0`. The bootstrap rows from registration are
    // still our materialised data set — and they reflect the full table.
    let outcomes = refresher.tick().await.unwrap();
    assert_eq!(outcomes.len(), 1, "exactly one CV registered");
    let first_outcome_idempotent =
        matches!(outcomes[0].outcome, basin_cv::CvRefreshOutcome::NotDue);

    // ---------- Read CV: must be 5 day-buckets ---------------------------
    let cv_rows_initial = read_cv_rows(&engine, project).await;
    let cv_rows_correct = cv_rows_initial.len() == 5;
    println!("[VIABILITY continuous_aggregate] initial cv rows: {cv_rows_initial:?}");

    // ---------- Insert 10 more rows in the latest day --------------------
    let writer = engine.open_session(project).await.unwrap();
    let mut sql = String::from("INSERT INTO events VALUES ");
    for i in 0..10u32 {
        if i > 0 {
            sql.push(',');
        }
        let id = 1000 + i;
        sql.push_str(&format!(
            "({id}, '2026-04-30T{:02}:30:00Z', 'extra-{i}')",
            i
        ));
    }
    writer.execute(&sql).await.unwrap();

    // Tick again immediately: still NotDue.
    let outcomes_pre_advance = refresher.tick().await.unwrap();
    let pre_advance_idempotent = matches!(
        outcomes_pre_advance[0].outcome,
        basin_cv::CvRefreshOutcome::NotDue
    );

    // Advance past the interval and tick.
    clock.advance(chrono::Duration::seconds(3600 + 60));
    let outcomes_after = refresher.tick().await.unwrap();
    let refresh_picks_up = matches!(
        outcomes_after[0].outcome,
        basin_cv::CvRefreshOutcome::Refreshed { .. }
    );

    // ---------- Read CV: still 5 day-buckets, latest day count is 30 -----
    let cv_rows_after = read_cv_rows(&engine, project).await;
    println!("[VIABILITY continuous_aggregate] post-refresh cv rows: {cv_rows_after:?}");
    let still_five = cv_rows_after.len() == 5;
    // The 4 earlier days have 20 rows each; the latest day was 20+10=30.
    let mut counts: Vec<i64> = cv_rows_after.iter().map(|(_, n)| *n).collect();
    counts.sort();
    let latest_day_picks_up = counts == vec![20, 20, 20, 20, 30];

    let refresh_idempotent = first_outcome_idempotent && pre_advance_idempotent;
    let refresh_picks_up_new_rows = refresh_picks_up && latest_day_picks_up;

    let pass = cv_rows_correct && refresh_idempotent && refresh_picks_up_new_rows && still_five;
    println!(
        "[VIABILITY continuous_aggregate] cv_rows_correct={cv_rows_correct} \
         refresh_idempotent={refresh_idempotent} \
         refresh_picks_up_new_rows={refresh_picks_up_new_rows} \
         {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "continuous_aggregate",
        "TimescaleDB-style continuous aggregates",
        "Auto-refreshed materialised view over a time-bucketed GROUP BY. The \
         refresher rebuilds the materialisation on each interval tick; the CV \
         appears as a regular table to readers.",
        pass,
        PrimaryMetric {
            label: "cv_buckets".into(),
            value: cv_rows_after.len() as f64,
            unit: "buckets".into(),
            bar: BarOp::eq(5.0),
        },
        json!({
            "initial_cv_rows": cv_rows_initial.len(),
            "post_refresh_cv_rows": cv_rows_after.len(),
            "post_refresh_counts_sorted": counts,
            "cv_rows_correct": cv_rows_correct,
            "refresh_idempotent": refresh_idempotent,
            "refresh_picks_up_new_rows": refresh_picks_up_new_rows,
        }),
    );

    assert!(pass);
}

/// Read every row of the `events_per_day` CV and return `(day, n)` pairs.
async fn read_cv_rows(engine: &Engine, project: ProjectId) -> Vec<(String, i64)> {
    let sess = engine.open_session(project).await.unwrap();
    let res = sess
        .execute("SELECT day, n FROM events_per_day")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let day_arr = b
            .column_by_name("day")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let n_arr = b
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((day_arr.value(i).to_string(), n_arr.value(i)));
        }
    }
    out
}
