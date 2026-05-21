//! noisy-neighbor workload — 2-tenant adversarial fairness.
//!
//! Two tenants (alice = whale, bob = quiet) connect to the same basin-server
//! over real TCP sockets. Alice bursts 200 consecutive INSERTs + SELECT COUNT(*)
//! loops concurrently; Bob interleaves quiet point queries throughout. We assert
//! that Bob's p99 latency does not degrade beyond the `BOB_P99_BUDGET_MS`
//! threshold while Alice is hammering.
//!
//! The test is a *network-path* test: both clients use separate tokio-postgres
//! connections to the same host:port, so the Basin pgwire router and engine
//! fairness mechanisms are exercised end-to-end.
//!
//! Interpretation of results:
//!   PASS — Bob's p99 stayed within budget during Alice's burst.
//!   FAIL — Bob's p99 exceeded budget; indicates a fairness regression.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{connect, percentile, time_query_f64, Args, ConnConfig, Metric, WorkloadReport};

const ALICE_TABLE: &str = "e2e_nn_alice";
const BOB_TABLE: &str = "e2e_nn_bob";

/// Bob's worst-case p99 latency (ms) while Alice is bursting.
/// Generous for Docker dev-stack; tighten for CI with SLA requirements.
const BOB_P99_BUDGET_MS: f64 = 500.0;

/// Number of Alice INSERT + COUNT burst iterations.
const ALICE_BURST_ITERS: usize = 200;

/// Number of Bob quiet point queries interleaved.
const BOB_SAMPLE_COUNT: usize = 50;

pub async fn run(args: &Args) -> WorkloadReport {
    match run_inner(args).await {
        Ok(report) => report,
        Err(e) => WorkloadReport {
            name: "noisy-neighbor".into(),
            passed: false,
            error: Some(format!("{e:#}")),
            metrics: vec![],
        },
    }
}

async fn run_inner(args: &Args) -> Result<WorkloadReport> {
    let alice_cfg = ConnConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user_alice.clone(),
        dbname: "postgres".into(),
    };
    let bob_cfg = ConnConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user_bob.clone(),
        dbname: "postgres".into(),
    };

    let alice = connect(&alice_cfg).await?;
    let bob = connect(&bob_cfg).await?;

    // ── Setup ────────────────────────────────────────────────────────────────
    alice
        .simple_query(&format!("DROP TABLE IF EXISTS {ALICE_TABLE}"))
        .await?;
    alice
        .simple_query(&format!(
            "CREATE TABLE {ALICE_TABLE} (id BIGINT, payload TEXT)"
        ))
        .await?;

    bob.simple_query(&format!("DROP TABLE IF EXISTS {BOB_TABLE}"))
        .await?;
    bob.simple_query(&format!(
        "CREATE TABLE {BOB_TABLE} (id BIGINT, val DOUBLE PRECISION)"
    ))
    .await?;
    // Pre-populate Bob's table so his queries hit data.
    let bob_seed: String = (1i64..=500)
        .map(|i| format!("({i}, {})", i as f64 * 0.1))
        .collect::<Vec<_>>()
        .join(", ");
    bob.simple_query(&format!("INSERT INTO {BOB_TABLE} VALUES {bob_seed}"))
        .await?;

    // ── Concurrent burst ──────────────────────────────────────────────────────
    // Signal channel: Alice notifies when her burst is complete.
    let alice_done = Arc::new(AtomicBool::new(false));
    let alice_done_clone = alice_done.clone();

    info!("noisy-neighbor: starting Alice burst ({ALICE_BURST_ITERS} iterations)...");

    // Spawn Alice's burst task.
    let alice_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        for i in 0..ALICE_BURST_ITERS {
            let sql = format!("INSERT INTO {ALICE_TABLE} VALUES ({i}, 'noise-payload-{i}')");
            alice.simple_query(&sql).await?;
            // Also run a COUNT to mix read + write load.
            alice
                .simple_query(&format!("SELECT COUNT(*) FROM {ALICE_TABLE}"))
                .await?;
        }
        alice_done_clone.store(true, Ordering::Release);
        Ok(())
    });

    // Bob's quiet queries — run concurrently with Alice's burst.
    let mut bob_samples: Vec<f64> = Vec::with_capacity(BOB_SAMPLE_COUNT);
    let mut bob_iter = 0usize;
    let mut bob_errors = 0usize;

    // Interleave Bob queries with a short yield between each to avoid
    // overwhelming the event loop (simulates realistic quiet traffic).
    while !alice_done.load(Ordering::Acquire) && bob_iter < BOB_SAMPLE_COUNT {
        let id = (bob_iter % 500 + 1) as i64;
        let sql = format!("SELECT COUNT(*) FROM {BOB_TABLE} WHERE id = {id}");
        match time_query_f64(&bob, &sql).await {
            Ok((_, d)) => {
                bob_samples.push(d.as_secs_f64() * 1000.0);
                bob_iter += 1;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Bob query failed during burst");
                bob_errors += 1;
                if bob_errors > 5 {
                    break;
                }
            }
        }
        // Brief yield so Alice can also make progress.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Drain remaining Bob samples after Alice finishes (if Alice finished fast).
    while bob_iter < BOB_SAMPLE_COUNT {
        let id = (bob_iter % 500 + 1) as i64;
        let sql = format!("SELECT COUNT(*) FROM {BOB_TABLE} WHERE id = {id}");
        match time_query_f64(&bob, &sql).await {
            Ok((_, d)) => {
                bob_samples.push(d.as_secs_f64() * 1000.0);
                bob_iter += 1;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Bob post-burst query failed");
                break;
            }
        }
    }

    // Wait for Alice to complete.
    alice_handle.await??;
    info!("noisy-neighbor: Alice burst complete; Bob collected {} samples", bob_samples.len());

    // ── Cleanup ───────────────────────────────────────────────────────────────
    bob.simple_query(&format!("DROP TABLE IF EXISTS {BOB_TABLE}"))
        .await?;

    // ── Evaluate ──────────────────────────────────────────────────────────────
    let bob_p50 = percentile(bob_samples.clone(), 50.0);
    let bob_p99 = percentile(bob_samples, 99.0);

    let metrics = vec![
        Metric::count("Alice burst iterations", ALICE_BURST_ITERS as f64),
        Metric::count("Bob samples collected", bob_iter as f64),
        Metric::count("Bob query errors during burst", bob_errors as f64),
        Metric::latency_ms("Bob point-query p50 during burst", bob_p50),
        Metric::latency_ms_with_budget(
            "Bob point-query p99 during burst",
            bob_p99,
            BOB_P99_BUDGET_MS,
        ),
    ];

    let passed = metrics.iter().all(|m| m.passed.unwrap_or(true)) && bob_errors == 0;

    Ok(WorkloadReport {
        name: "noisy-neighbor".into(),
        passed,
        error: None,
        metrics,
    })
}
