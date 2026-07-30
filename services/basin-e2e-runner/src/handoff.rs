//! handoff workload — kill replica-0, assert replica-1 picks up within budget.
//!
//! ADR 0023 specifies a default lease TTL of 15 s with 5 s heartbeat renewal.
//! After the leaseholder dies, the next heartbeat cycle (≤15 s) allows a new
//! replica to steal the expired lease. The handoff budget here is:
//!   `HANDOFF_BUDGET_SECS` (default 20 s) — generous for a dev-stack with
//!   Docker stop/start overhead. Tighten for production integration tests.
//!
//! Mechanics:
//!   1. Verify replica-0 and replica-1 are both accepting connections.
//!   2. Write a known row via replica-0 (establishes a lease).
//!   3. Kill replica-0 via `docker compose stop basin-server-0`.
//!   4. Poll replica-1 with a SELECT until the row is visible (lease handoff).
//!   5. Record wall time from stop to first successful SELECT.
//!   6. Restart replica-0 (`docker compose start basin-server-0`) for cleanup.
//!
//! Requires:
//!   - `--replicas 2` (the handoff target must exist)
//!   - The runner must be able to invoke `docker compose` (in the repo's dev/ dir).

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::info;

use crate::{connect, Args, ConnConfig, Metric, WorkloadReport};

const TABLE: &str = "e2e_handoff_marker";

/// ADR 0023: lease TTL = 15 s, heartbeat = 5 s.
/// Handoff budget = TTL + one heartbeat + Docker stop overhead.
const HANDOFF_BUDGET_SECS: f64 = 20.0;

/// How long to poll replica-1 after killing replica-0.
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const POLL_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run(args: &Args) -> WorkloadReport {
    match run_inner(args).await {
        Ok(report) => report,
        Err(e) => WorkloadReport {
            name: "handoff".into(),
            passed: false,
            error: Some(format!("{e:#}")),
            metrics: vec![],
        },
    }
}

async fn run_inner(args: &Args) -> Result<WorkloadReport> {
    let r0_cfg = ConnConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user_alice.clone(),
        dbname: "postgres".into(),
    };
    let r1_cfg = ConnConfig {
        host: args.host.clone(),
        port: args.port_replica1,
        user: args.user_alice.clone(),
        dbname: "postgres".into(),
    };

    // ── Verify both replicas are reachable ────────────────────────────────────
    let r0 = connect(&r0_cfg)
        .await
        .context("handoff: replica-0 not reachable; is --replicas 2 active?")?;
    let r1 = connect(&r1_cfg)
        .await
        .context("handoff: replica-1 not reachable; start with --replicas 2")?;

    info!("handoff: both replicas reachable");

    // ── Setup ────────────────────────────────────────────────────────────────
    r0.simple_query(&format!("DROP TABLE IF EXISTS {TABLE}"))
        .await?;
    r0.simple_query(&format!(
        "CREATE TABLE {TABLE} (id BIGINT, marker TEXT)"
    ))
    .await?;
    r0.simple_query(&format!(
        "INSERT INTO {TABLE} VALUES (1, 'handoff-marker')"
    ))
    .await?;
    info!("handoff: marker row written via replica-0");

    // Ensure replica-1 can see the table before we kill replica-0.
    // This also warms the lease cache on replica-1.
    let pre_check = r1
        .query_one(&format!("SELECT COUNT(*) FROM {TABLE}"), &[])
        .await?;
    let pre_count: i64 = pre_check.get(0);
    info!("handoff: replica-1 pre-check COUNT = {pre_count}");

    // Drop both clients before stopping replica-0 to avoid dangling connections.
    drop(r0);
    drop(r1);

    // ── Kill replica-0 ────────────────────────────────────────────────────────
    info!("handoff: stopping basin-server-0 via docker compose...");
    let kill_t0 = Instant::now();

    let stop_status = std::process::Command::new("docker")
        .args(["compose", "stop", "basin-server-0"])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(std::path::Path::new(".")),
        )
        .status()
        .context("docker compose stop basin-server-0 failed")?;

    if !stop_status.success() {
        anyhow::bail!(
            "docker compose stop basin-server-0 exited with status {}",
            stop_status
        );
    }
    info!("handoff: replica-0 stopped; polling replica-1 for recovery...");

    // ── Poll replica-1 until it accepts connections + sees the marker ─────────
    let mut handoff_ms: f64 = 0.0;
    let mut poll_iters = 0u32;
    let mut recovered = false;

    let poll_deadline = Instant::now() + POLL_TIMEOUT;
    while Instant::now() < poll_deadline {
        poll_iters += 1;
        tokio::time::sleep(POLL_INTERVAL).await;

        // Fresh connection each poll — simulates a real client reconnecting.
        let r1_poll = match connect(&r1_cfg).await {
            Ok(c) => c,
            Err(_) => continue, // Not yet accepting — keep polling.
        };

        match r1_poll
            .query_one(&format!("SELECT COUNT(*) FROM {TABLE} WHERE id = 1"), &[])
            .await
        {
            Ok(row) => {
                let count: i64 = row.get(0);
                if count >= 1 {
                    handoff_ms = kill_t0.elapsed().as_secs_f64() * 1000.0;
                    info!(
                        "handoff: replica-1 sees marker row after {:.1} ms ({} polls)",
                        handoff_ms, poll_iters
                    );
                    recovered = true;
                    break;
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "replica-1 poll: query failed, retrying");
            }
        }
    }

    // ── Restart replica-0 (cleanup — best effort) ─────────────────────────────
    info!("handoff: restarting basin-server-0...");
    let _ = std::process::Command::new("docker")
        .args(["compose", "start", "basin-server-0"])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(std::path::Path::new(".")),
        )
        .status();

    // Reconnect to whichever replica is up and drop the test table.
    if let Ok(cleanup_client) = connect(&r1_cfg).await {
        let _ = cleanup_client
            .simple_query(&format!("DROP TABLE IF EXISTS {TABLE}"))
            .await;
    }

    // ── Build report ──────────────────────────────────────────────────────────
    let budget_ms = HANDOFF_BUDGET_SECS * 1000.0;

    if !recovered {
        return Ok(WorkloadReport {
            name: "handoff".into(),
            passed: false,
            error: Some(format!(
                "replica-1 did not see marker row within {}s timeout",
                POLL_TIMEOUT.as_secs()
            )),
            metrics: vec![
                Metric::count("Poll iterations", poll_iters as f64),
                Metric::latency_ms_with_budget(
                    "Handoff latency (TIMEOUT — no recovery)",
                    POLL_TIMEOUT.as_secs_f64() * 1000.0,
                    budget_ms,
                ),
            ],
        });
    }

    let metrics = vec![
        Metric::count("Poll iterations until recovery", poll_iters as f64),
        Metric::latency_ms_with_budget(
            &format!("Handoff latency (ADR 0023 budget {HANDOFF_BUDGET_SECS}s)"),
            handoff_ms,
            budget_ms,
        ),
    ];

    let passed = metrics.iter().all(|m| m.passed.unwrap_or(true));

    Ok(WorkloadReport {
        name: "handoff".into(),
        passed,
        error: None,
        metrics,
    })
}
