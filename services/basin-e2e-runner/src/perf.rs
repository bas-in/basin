//! perf workload — point_query / range_scan / INSERT / UPDATE latency
//! measurements exercised over real network paths.
//!
//! Schema: a simple events table with a monotonic id, a category label, and a
//! numeric value. The workload:
//!   1. Creates the table.
//!   2. INSERT baseline (1 000 rows, timed as bulk).
//!   3. Individual INSERT p50/p99 (100 single-row inserts).
//!   4. Point query p50/p99 (100 SELECT WHERE id = ?).
//!   5. Range scan p50/p99 (20 SELECT WHERE id BETWEEN ? AND ?).
//!   6. UPDATE p50/p99 (50 single-row updates).
//!   7. Drops the table.
//!
//! All measurements are wall-clock (network RTT included). Thresholds are
//! deliberately generous for a dev/Docker environment; tighten for CI.

use anyhow::Result;
use rand::Rng;
use tracing::info;

use crate::{
    connect, percentile, time_exec, time_query_f64, Args, ConnConfig, Metric, WorkloadReport,
};

const TABLE: &str = "e2e_perf_events";

// Latency budgets for the dev-stack (generous; Docker adds ~1–5 ms overhead).
const POINT_P99_BUDGET_MS: f64 = 150.0;
const RANGE_P99_BUDGET_MS: f64 = 500.0;
const INSERT_P99_BUDGET_MS: f64 = 200.0;
const UPDATE_P99_BUDGET_MS: f64 = 200.0;

pub async fn run(args: &Args) -> WorkloadReport {
    match run_inner(args).await {
        Ok(report) => report,
        Err(e) => WorkloadReport {
            name: "perf".into(),
            passed: false,
            error: Some(format!("{e:#}")),
            metrics: vec![],
        },
    }
}

async fn run_inner(args: &Args) -> Result<WorkloadReport> {
    let cfg = ConnConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user_alice.clone(),
        dbname: "postgres".into(),
    };
    let client = connect(&cfg).await?;

    // ── Setup ────────────────────────────────────────────────────────────────
    client
        .simple_query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .await?;
    client
        .simple_query(&format!(
            "CREATE TABLE {TABLE} (id BIGINT, category TEXT, value DOUBLE PRECISION)"
        ))
        .await?;

    // ── Bulk INSERT (1 000 rows) ──────────────────────────────────────────────
    info!("perf: bulk INSERT 1 000 rows...");
    let bulk_values: String = (1i64..=1000)
        .map(|i| format!("({i}, 'cat-{}', {})", i % 10, i as f64 * 0.01))
        .collect::<Vec<_>>()
        .join(", ");
    let bulk_sql = format!("INSERT INTO {TABLE} VALUES {bulk_values}");
    let bulk_dur = time_exec(&client, &bulk_sql).await?;
    let bulk_ms = bulk_dur.as_secs_f64() * 1000.0;
    info!("perf: bulk INSERT done in {:.1} ms", bulk_ms);

    // ── Single-row INSERT p50/p99 ─────────────────────────────────────────────
    info!("perf: single-row INSERT p50/p99 (100 samples)...");
    let mut insert_samples: Vec<f64> = Vec::with_capacity(100);
    for i in 1001i64..=1100 {
        let sql = format!(
            "INSERT INTO {TABLE} VALUES ({i}, 'bench', {})",
            i as f64 * 0.001
        );
        let d = time_exec(&client, &sql).await?;
        insert_samples.push(d.as_secs_f64() * 1000.0);
    }
    let insert_p50 = percentile(insert_samples.clone(), 50.0);
    let insert_p99 = percentile(insert_samples, 99.0);

    // ── Point SELECT p50/p99 ──────────────────────────────────────────────────
    info!("perf: point SELECT p50/p99 (100 samples)...");
    let mut rng = rand::thread_rng();
    let mut point_samples: Vec<f64> = Vec::with_capacity(100);
    for _ in 0..100 {
        let id: i64 = rng.gen_range(1..=1000);
        let sql = format!("SELECT COUNT(*) FROM {TABLE} WHERE id = {id}");
        let (_, d) = time_query_f64(&client, &sql).await?;
        point_samples.push(d.as_secs_f64() * 1000.0);
    }
    let point_p50 = percentile(point_samples.clone(), 50.0);
    let point_p99 = percentile(point_samples, 99.0);

    // ── Range scan p50/p99 ────────────────────────────────────────────────────
    info!("perf: range scan p50/p99 (20 samples, ~100 rows each)...");
    let mut range_samples: Vec<f64> = Vec::with_capacity(20);
    for _ in 0..20 {
        let lo: i64 = rng.gen_range(1..=900);
        let hi = lo + 99;
        let sql = format!("SELECT COUNT(*) FROM {TABLE} WHERE id BETWEEN {lo} AND {hi}");
        let (_, d) = time_query_f64(&client, &sql).await?;
        range_samples.push(d.as_secs_f64() * 1000.0);
    }
    let range_p50 = percentile(range_samples.clone(), 50.0);
    let range_p99 = percentile(range_samples, 99.0);

    // ── UPDATE p50/p99 ────────────────────────────────────────────────────────
    info!("perf: UPDATE p50/p99 (50 samples)...");
    let mut update_samples: Vec<f64> = Vec::with_capacity(50);
    for _ in 0..50 {
        let id: i64 = rng.gen_range(1..=1000);
        let sql = format!("UPDATE {TABLE} SET value = 9.99 WHERE id = {id}");
        let d = time_exec(&client, &sql).await?;
        update_samples.push(d.as_secs_f64() * 1000.0);
    }
    let update_p50 = percentile(update_samples.clone(), 50.0);
    let update_p99 = percentile(update_samples, 99.0);

    // ── Cleanup ───────────────────────────────────────────────────────────────
    client
        .simple_query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .await?;

    // ── Evaluate pass/fail ────────────────────────────────────────────────────
    let metrics = vec![
        Metric::latency_ms(format!("Bulk INSERT 1 000 rows"), bulk_ms),
        Metric::latency_ms("Single INSERT p50", insert_p50),
        Metric::latency_ms_with_budget("Single INSERT p99", insert_p99, INSERT_P99_BUDGET_MS),
        Metric::latency_ms("Point SELECT p50", point_p50),
        Metric::latency_ms_with_budget("Point SELECT p99", point_p99, POINT_P99_BUDGET_MS),
        Metric::latency_ms("Range scan p50", range_p50),
        Metric::latency_ms_with_budget("Range scan p99", range_p99, RANGE_P99_BUDGET_MS),
        Metric::latency_ms("UPDATE p50", update_p50),
        Metric::latency_ms_with_budget("UPDATE p99", update_p99, UPDATE_P99_BUDGET_MS),
    ];

    let passed = metrics.iter().all(|m| m.passed.unwrap_or(true));

    Ok(WorkloadReport {
        name: "perf".into(),
        passed,
        error: None,
        metrics,
    })
}
