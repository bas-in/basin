//! Incremental-refresh tests for [`basin_cv::CvRefresher`].
//!
//! Asserts the watermark / bucket-spanning correctness invariant
//! documented in `basin_engine::cv_time_bucket`: incremental refresh
//! produces a matview state byte-equivalent to a single full rebuild for
//! the supported time-bucket GROUP BY shape.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_cv::{
    CvRefreshOutcome, CvRefresher, CvSpec, RefreshMode, RefreshSpy, TestClock,
};
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

#[derive(Clone, Default)]
struct CapturingSpy {
    records: Arc<Mutex<Vec<(String, RefreshMode, String)>>>,
}

impl CapturingSpy {
    fn drain(&self) -> Vec<(String, RefreshMode, String)> {
        std::mem::take(&mut *self.records.lock().unwrap())
    }
}

impl RefreshSpy for CapturingSpy {
    fn record(&self, cv_name: &str, mode: RefreshMode, sql: &str) {
        self.records
            .lock()
            .unwrap()
            .push((cv_name.to_string(), mode, sql.to_string()));
    }
}

/// Read every `(bucket_text, n)` row out of `event_count_per_hour`.
async fn read_hourly_rows(eng: &Engine, tenant: TenantId) -> Vec<(String, i64)> {
    let s = eng.open_session(tenant).await.unwrap();
    let res = s
        .execute("SELECT bucket, n FROM event_count_per_hour ORDER BY bucket")
        .await
        .unwrap();
    let mut out = Vec::new();
    if let ExecResult::Rows { batches, .. } = res {
        for b in &batches {
            let bucket = b
                .column_by_name("bucket")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let n = b
                .column_by_name("n")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((bucket.value(i).to_string(), n.value(i)));
            }
        }
    }
    out
}

/// Boilerplate: build engine, create `events`, register a CV that
/// counts events per `cast(date_trunc('hour', ts) AS TEXT)` bucket.
async fn setup_hourly_cv(
    dir: &TempDir,
    refresh_interval_secs: u64,
) -> (Engine, TenantId, CvRefresher, TestClock) {
    let eng = engine_in(dir);
    let tenant = TenantId::new();
    let admin = eng.open_session(tenant).await.unwrap();
    admin
        .execute("CREATE TABLE events (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL)")
        .await
        .unwrap();

    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock = TestClock::new(t0);
    let refresher = CvRefresher::new(eng.clone(), Arc::new(clock.clone()));
    refresher.register_tenant(tenant).await;

    // Bootstrap with a single row so registration succeeds (registration
    // requires at least one row to infer the schema).
    admin
        .execute("INSERT INTO events VALUES (0, '2026-05-01T10:00:00Z')")
        .await
        .unwrap();

    refresher
        .store()
        .register_cv_at(
            &tenant,
            CvSpec {
                name: "event_count_per_hour".into(),
                source_table: "events".into(),
                query_sql: "SELECT \
                    cast(date_trunc('hour', ts) AS TEXT) AS bucket, \
                    count(*) AS n \
                  FROM events \
                  GROUP BY 1"
                    .into(),
                refresh_interval_secs,
                last_refreshed_at: None,
                last_bucket_max: None,
            },
            t0,
        )
        .await
        .unwrap();
    (eng, tenant, refresher, clock)
}

#[tokio::test]
async fn first_refresh_full_scan_then_watermark_set() {
    let dir = TempDir::new().unwrap();
    let (eng, tenant, refresher, clock) = setup_hourly_cv(&dir, 60).await;

    // No watermark after registration.
    let cvs = refresher.store().list_cvs(&tenant).await.unwrap();
    assert!(cvs[0].last_bucket_max.is_none(), "registration should not set watermark");

    // Insert a few rows, advance, tick. First refresh = full.
    let s = eng.open_session(tenant).await.unwrap();
    s.execute(
        "INSERT INTO events VALUES \
            (1, '2026-05-01T11:00:00Z'), \
            (2, '2026-05-01T11:30:00Z'), \
            (3, '2026-05-01T12:15:00Z')",
    )
    .await
    .unwrap();

    let spy = Arc::new(CapturingSpy::default());
    refresher.set_spy(spy.clone()).await;
    clock.advance(Duration::seconds(120));
    let outcomes = refresher.tick().await.unwrap();
    assert!(matches!(
        outcomes[0].outcome,
        CvRefreshOutcome::Refreshed { .. }
    ));

    let logged = spy.drain();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].1, RefreshMode::Full, "first refresh must be full");

    // Watermark must be set now to the start of the latest bucket (12:00).
    let cvs = refresher.store().list_cvs(&tenant).await.unwrap();
    let wm = cvs[0].last_bucket_max.expect("watermark stamped");
    assert_eq!(wm, Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap());
}

#[tokio::test]
async fn incremental_refresh_only_aggregates_new_rows() {
    let dir = TempDir::new().unwrap();
    let (eng, tenant, refresher, clock) = setup_hourly_cv(&dir, 60).await;

    // First tick — full rebuild — sets the watermark.
    let s = eng.open_session(tenant).await.unwrap();
    s.execute(
        "INSERT INTO events VALUES \
            (1, '2026-05-01T11:00:00Z'), \
            (2, '2026-05-01T11:30:00Z')",
    )
    .await
    .unwrap();
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();

    // Insert rows in a NEW bucket (13:00).
    s.execute(
        "INSERT INTO events VALUES \
            (10, '2026-05-01T13:00:00Z'), \
            (11, '2026-05-01T13:30:00Z')",
    )
    .await
    .unwrap();

    // Second tick — must be incremental.
    let spy = Arc::new(CapturingSpy::default());
    refresher.set_spy(spy.clone()).await;
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();

    let logged = spy.drain();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].1, RefreshMode::Incremental, "second tick must be incremental");
    let sql = &logged[0].2;
    let lower = sql.to_ascii_lowercase();
    assert!(
        lower.contains("ts >= timestamptz")
            || lower.contains("ts >= 'timestamptz")
            || lower.contains("ts >="),
        "incremental rewrite must inject a watermark predicate; got {sql}"
    );
}

#[tokio::test]
async fn bucket_spanning_correctness() {
    // Watermark covers the LAST bucket as of the previous refresh. New
    // rows arriving in that bucket between refreshes must be picked up.
    let dir = TempDir::new().unwrap();
    let (eng, tenant, refresher, clock) = setup_hourly_cv(&dir, 60).await;

    // Insert rows in bucket 11:00 and 12:00.
    let s = eng.open_session(tenant).await.unwrap();
    s.execute(
        "INSERT INTO events VALUES \
            (1, '2026-05-01T11:00:00Z'), \
            (2, '2026-05-01T11:30:00Z'), \
            (3, '2026-05-01T12:15:00Z'), \
            (4, '2026-05-01T12:30:00Z')",
    )
    .await
    .unwrap();
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();
    let after_first = read_hourly_rows(&eng, tenant).await;
    // The bootstrap row at 10:00 is also present.
    assert_eq!(after_first.len(), 3);

    // Now add MORE rows in bucket 12:00 (the prior watermark) and one in
    // bucket 13:00.
    s.execute(
        "INSERT INTO events VALUES \
            (5, '2026-05-01T12:45:00Z'), \
            (6, '2026-05-01T12:55:00Z'), \
            (7, '2026-05-01T13:00:00Z')",
    )
    .await
    .unwrap();
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();
    let after_second = read_hourly_rows(&eng, tenant).await;

    // Expected per-bucket counts (sorted by bucket start):
    //   10:00 → 1, 11:00 → 2, 12:00 → 4, 13:00 → 1
    let mut counts: Vec<i64> = after_second.iter().map(|(_, n)| *n).collect();
    counts.sort();
    assert_eq!(counts, vec![1, 1, 2, 4]);
}

#[tokio::test]
async fn manual_refresh_with_full_opt_out() {
    // The SQL surface routes through basin_engine::cv_ddl, which is the
    // production path for `REFRESH MATERIALIZED VIEW … WITH (full=true)`.
    // This test exercises that path end-to-end.
    let dir = TempDir::new().unwrap();
    let (eng, tenant, _refresher, _clock) = setup_hourly_cv(&dir, 60).await;

    let s = eng.open_session(tenant).await.unwrap();
    s.execute(
        "INSERT INTO events VALUES \
            (1, '2026-05-01T11:00:00Z')",
    )
    .await
    .unwrap();

    // First refresh via SQL (incremental path defaults; without a prior
    // watermark this is a full rebuild that also sets the watermark).
    s.execute("REFRESH MATERIALIZED VIEW event_count_per_hour")
        .await
        .unwrap();

    // Subsequent insert in the same bucket as the watermark.
    s.execute(
        "INSERT INTO events VALUES \
            (2, '2026-05-01T11:15:00Z')",
    )
    .await
    .unwrap();

    // WITH (full = true) must not error.
    s.execute("REFRESH MATERIALIZED VIEW event_count_per_hour WITH (full = true)")
        .await
        .unwrap();

    // The matview must reflect both rows in 11:00.
    let rows = read_hourly_rows(&eng, tenant).await;
    let row_for_11 = rows.iter().find(|(b, _)| b.contains("11:00"));
    assert!(row_for_11.is_some(), "11:00 bucket missing: {rows:?}");
    assert_eq!(row_for_11.unwrap().1, 2);
}

#[tokio::test]
async fn incremental_refresh_on_unsupported_body_falls_back() {
    // No GROUP BY at all → no time-bucket detected → must fall back to
    // full re-execution on every refresh.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let tenant = TenantId::new();
    let admin = eng.open_session(tenant).await.unwrap();
    admin
        .execute("CREATE TABLE events (id BIGINT NOT NULL)")
        .await
        .unwrap();
    admin
        .execute("INSERT INTO events VALUES (1), (2)")
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
                name: "events_total".into(),
                source_table: "events".into(),
                query_sql: "SELECT count(*) AS n FROM events".into(),
                refresh_interval_secs: 60,
                last_refreshed_at: None,
                last_bucket_max: None,
            },
            t0,
        )
        .await
        .unwrap();

    let spy = Arc::new(CapturingSpy::default());
    refresher.set_spy(spy.clone()).await;

    // First tick — full.
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();
    // Insert + second tick — still full (no time-bucket detected).
    let s = eng.open_session(tenant).await.unwrap();
    s.execute("INSERT INTO events VALUES (3)").await.unwrap();
    clock.advance(Duration::seconds(120));
    refresher.tick().await.unwrap();

    let logged = spy.drain();
    assert_eq!(logged.len(), 2);
    assert!(
        logged.iter().all(|(_, mode, _)| *mode == RefreshMode::Full),
        "every refresh on an unsupported body must be full: {logged:?}"
    );
}

#[tokio::test]
async fn incremental_refresh_correctness_vs_full() {
    // Insert rows in batches, refresh incrementally after each batch, and
    // compare against the result of a single full rebuild on the same
    // source state. We respect the v0.1 watermark invariant: each batch
    // only adds rows to (a) the current watermark bucket — i.e., the
    // last bucket touched by the previous batch — or (b) NEW buckets
    // strictly after it. Inserts into pre-watermark buckets are not
    // covered by v0.1 incremental refresh (documented limitation in
    // `cv_time_bucket`).
    let dir = TempDir::new().unwrap();
    let (eng, tenant, refresher, clock) = setup_hourly_cv(&dir, 60).await;

    // 10 batches × 100 rows. Each batch fills (a) the watermark bucket
    // (which is `batch + 1` because the bootstrap row at 10:00 makes that
    // the initial last bucket) with 30 extra rows, then opens 2 new
    // buckets ahead with 35 rows each. Watermark advances every tick.
    for batch in 0..10u32 {
        let s = eng.open_session(tenant).await.unwrap();
        let mut sql = String::from("INSERT INTO events VALUES ");
        let base_hour = 10 + batch * 2; // 10, 12, 14, ..., 28 — wraps days below.
        let mut wrote_any = false;
        for i in 0..100u32 {
            // Three buckets per batch, all >= the prior watermark.
            let bucket_idx = i % 3;
            let hour_raw = base_hour + bucket_idx;
            let day = 1 + (hour_raw / 24);
            let hour = hour_raw % 24;
            let minute = (i * 3) % 60;
            let id = batch * 100 + i;
            if wrote_any {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({id}, '2026-05-{:02}T{:02}:{:02}:00Z')",
                day, hour, minute
            ));
            wrote_any = true;
        }
        s.execute(&sql).await.unwrap();
        clock.advance(Duration::seconds(120));
        refresher.tick().await.unwrap();
    }
    let incremental_state = read_hourly_rows(&eng, tenant).await;

    // Build a parallel CV under a fresh tenant with the same source state
    // and refresh once with `WITH (full=true)`.
    let dir2 = TempDir::new().unwrap();
    let eng2 = engine_in(&dir2);
    let t2 = TenantId::new();
    let admin = eng2.open_session(t2).await.unwrap();
    admin
        .execute("CREATE TABLE events (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL)")
        .await
        .unwrap();
    admin
        .execute("INSERT INTO events VALUES (0, '2026-05-01T10:00:00Z')")
        .await
        .unwrap();
    let t0 = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
    let clock2 = TestClock::new(t0);
    let refresher2 = CvRefresher::new(eng2.clone(), Arc::new(clock2.clone()));
    refresher2.register_tenant(t2).await;
    refresher2
        .store()
        .register_cv_at(
            &t2,
            CvSpec {
                name: "event_count_per_hour".into(),
                source_table: "events".into(),
                query_sql: "SELECT \
                    cast(date_trunc('hour', ts) AS TEXT) AS bucket, \
                    count(*) AS n \
                  FROM events \
                  GROUP BY 1"
                    .into(),
                refresh_interval_secs: 60,
                last_refreshed_at: None,
                last_bucket_max: None,
            },
            t0,
        )
        .await
        .unwrap();
    for batch in 0..10u32 {
        let s = eng2.open_session(t2).await.unwrap();
        let mut sql = String::from("INSERT INTO events VALUES ");
        let base_hour = 10 + batch * 2;
        for i in 0..100u32 {
            let bucket_idx = i % 3;
            let hour_raw = base_hour + bucket_idx;
            let day = 1 + (hour_raw / 24);
            let hour = hour_raw % 24;
            let minute = (i * 3) % 60;
            let id = batch * 100 + i;
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({id}, '2026-05-{:02}T{:02}:{:02}:00Z')",
                day, hour, minute
            ));
        }
        s.execute(&sql).await.unwrap();
    }
    let s = eng2.open_session(t2).await.unwrap();
    s.execute("REFRESH MATERIALIZED VIEW event_count_per_hour WITH (full = true)")
        .await
        .unwrap();
    let full_state = read_hourly_rows(&eng2, t2).await;

    let mut a = incremental_state.clone();
    let mut b = full_state.clone();
    a.sort();
    b.sort();
    assert_eq!(
        a, b,
        "incremental refresh diverged from full rebuild:\n  inc={incremental_state:?}\n  full={full_state:?}"
    );
}

#[tokio::test]
async fn incremental_speedup_observability() {
    // Observability test only — print elapsed times for full vs.
    // incremental refresh on a 10K-row source plus 100 new rows.
    let dir = TempDir::new().unwrap();
    let (eng, tenant, refresher, clock) = setup_hourly_cv(&dir, 60).await;

    // Bulk-load 10K rows across many hourly buckets.
    let s = eng.open_session(tenant).await.unwrap();
    let mut sql = String::from("INSERT INTO events VALUES ");
    for i in 0..10_000u32 {
        if i > 0 {
            sql.push(',');
        }
        let hour = i % 24;
        let day = 1 + (i / 24) % 28;
        let minute = (i * 7) % 60;
        sql.push_str(&format!(
            "({i}, '2026-05-{:02}T{:02}:{:02}:00Z')",
            day, hour, minute
        ));
    }
    s.execute(&sql).await.unwrap();
    clock.advance(Duration::seconds(120));

    // Tick #1: full rebuild (no prior watermark).
    let t0 = Instant::now();
    refresher.tick().await.unwrap();
    let full_elapsed = t0.elapsed();

    // Insert 100 new rows.
    let s = eng.open_session(tenant).await.unwrap();
    let mut sql = String::from("INSERT INTO events VALUES ");
    for i in 0..100u32 {
        if i > 0 {
            sql.push(',');
        }
        let id = 100_000 + i;
        // All in one bucket so the rewrite plan is small.
        let minute = i % 60;
        sql.push_str(&format!(
            "({id}, '2026-06-15T12:{:02}:00Z')",
            minute
        ));
    }
    s.execute(&sql).await.unwrap();
    clock.advance(Duration::seconds(120));

    // Tick #2: incremental.
    let t1 = Instant::now();
    refresher.tick().await.unwrap();
    let incr_elapsed = t1.elapsed();

    println!(
        "[incremental_speedup] full={:?} incremental={:?} ratio={:.2}x",
        full_elapsed,
        incr_elapsed,
        full_elapsed.as_secs_f64() / incr_elapsed.as_secs_f64().max(1e-9),
    );
}
