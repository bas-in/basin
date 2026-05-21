//! basin-e2e-runner — exercises Basin over real network paths.
//!
//! Three workload modes:
//!   perf            point/range/INSERT/UPDATE latency measurements
//!   noisy-neighbor  2-tenant adversarial fairness check
//!   handoff         kill replica-0, assert replica-1 picks up within budget
//!
//! Usage:
//!   basin-e2e-runner \
//!     --workload perf \
//!     --host 127.0.0.1 --port 5533 \
//!     --user-alice alice --user-bob bob \
//!     --replicas 1 \
//!     --out dev/results/run.json

mod handoff;
mod noisy_neighbor;
mod perf;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::info;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "basin-e2e-runner", about = "Basin E2E workload runner")]
pub struct Args {
    /// Workload to run: perf | noisy-neighbor | handoff
    #[arg(long, default_value = "perf")]
    pub workload: String,

    /// Basin server host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Basin server port (replica 0)
    #[arg(long, default_value_t = 5533)]
    pub port: u16,

    /// Basin server port (replica 1) — used by handoff workload
    #[arg(long, default_value_t = 5534)]
    pub port_replica1: u16,

    /// Number of available replicas
    #[arg(long, default_value_t = 1)]
    pub replicas: u8,

    /// Username for tenant Alice (project)
    #[arg(long, default_value = "alice")]
    pub user_alice: String,

    /// Username for tenant Bob (project)
    #[arg(long, default_value = "bob")]
    pub user_bob: String,

    /// Output JSON file path (appended if exists, overwritten fresh each run)
    #[arg(long, default_value = "dev/results/latest.json")]
    pub out: PathBuf,
}

// ── Output schema (mirroring benchmark/data JSON structure) ───────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct RunReport {
    pub kind: String,
    pub id: String,
    pub name: String,
    pub generated_at: String,
    pub workloads: Vec<WorkloadReport>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadReport {
    pub name: String,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub metrics: Vec<Metric>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metric {
    pub label: String,
    pub value: f64,
    pub unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub passed: Option<bool>,
}

impl Metric {
    pub fn latency_ms(label: impl Into<String>, ms: f64) -> Self {
        Metric {
            label: label.into(),
            value: ms,
            unit: "ms".into(),
            threshold: None,
            passed: None,
        }
    }

    pub fn latency_ms_with_budget(label: impl Into<String>, ms: f64, budget_ms: f64) -> Self {
        Metric {
            label: label.into(),
            value: ms,
            unit: "ms".into(),
            threshold: Some(budget_ms),
            passed: Some(ms <= budget_ms),
        }
    }

    pub fn count(label: impl Into<String>, n: f64) -> Self {
        Metric {
            label: label.into(),
            value: n,
            unit: "count".into(),
            threshold: None,
            passed: None,
        }
    }
}

// ── Connection helpers ────────────────────────────────────────────────────────

pub struct ConnConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
}

impl ConnConfig {
    pub fn dsn(&self) -> String {
        format!(
            "host={} port={} user={} dbname={} connect_timeout=10",
            self.host, self.port, self.user, self.dbname
        )
    }
}

/// Open a tokio-postgres connection to basin-server. Basin speaks pgwire; no
/// password is required for the dev stack.
pub async fn connect(cfg: &ConnConfig) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(&cfg.dsn(), tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connect to basin-server {}:{}", cfg.host, cfg.port))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::warn!(error = %e, "basin-server connection dropped");
        }
    });
    Ok(client)
}

/// Measure the wall time of a single SQL statement (no result rows needed).
pub async fn time_exec(client: &tokio_postgres::Client, sql: &str) -> Result<Duration> {
    let t0 = Instant::now();
    client
        .simple_query(sql)
        .await
        .with_context(|| format!("exec: {}", &sql[..sql.len().min(80)]))?;
    Ok(t0.elapsed())
}

/// Measure wall time of a query and return its first column of first row as f64.
pub async fn time_query_f64(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<(f64, Duration)> {
    let t0 = Instant::now();
    let row = client
        .query_one(sql, &[])
        .await
        .with_context(|| format!("query_one: {}", &sql[..sql.len().min(80)]))?;
    let elapsed = t0.elapsed();
    let val: f64 = row.try_get::<_, i64>(0).map(|v| v as f64).unwrap_or_else(|_| {
        row.try_get::<_, f64>(0).unwrap_or(0.0)
    });
    Ok((val, elapsed))
}

// ── Percentile helper ─────────────────────────────────────────────────────────

pub fn percentile(mut samples: Vec<f64>, p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (samples.len() as f64 - 1.0)).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("basin_e2e_runner=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!(workload = %args.workload, host = %args.host, port = args.port, "E2E runner starting");

    let workload_reports: Vec<WorkloadReport> = match args.workload.as_str() {
        "perf" => {
            vec![perf::run(&args).await]
        }
        "noisy-neighbor" => {
            vec![noisy_neighbor::run(&args).await]
        }
        "handoff" => {
            if args.replicas < 2 {
                vec![WorkloadReport {
                    name: "handoff".into(),
                    passed: false,
                    error: Some(
                        "handoff workload requires --replicas 2; only 1 replica configured".into(),
                    ),
                    metrics: vec![],
                }]
            } else {
                vec![handoff::run(&args).await]
            }
        }
        other => {
            anyhow::bail!("Unknown workload: {other:?}. Use: perf | noisy-neighbor | handoff");
        }
    };

    // Build the report.
    let report = RunReport {
        kind: "e2e".into(),
        id: format!("basin-e2e-{}", args.workload),
        name: format!("Basin E2E — {}", args.workload),
        generated_at: chrono::Utc::now().to_rfc3339(),
        workloads: workload_reports,
    };

    // Merge into existing file or create fresh.
    let out_path = &args.out;
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }

    // If the file already exists and has the same run-id structure, merge.
    // For simplicity: always overwrite with the latest run's data appended
    // to the existing workloads list.
    let final_report = if out_path.exists() {
        let existing: RunReport = serde_json::from_str(
            &std::fs::read_to_string(out_path)
                .with_context(|| format!("read existing output {}", out_path.display()))?,
        )
        .unwrap_or_else(|_| RunReport {
            kind: "e2e".into(),
            id: report.id.clone(),
            name: report.name.clone(),
            generated_at: report.generated_at.clone(),
            workloads: vec![],
        });
        let mut merged = existing;
        merged.generated_at = report.generated_at;
        merged.workloads.extend(report.workloads);
        merged
    } else {
        report
    };

    let json = serde_json::to_string_pretty(&final_report)?;
    std::fs::write(out_path, &json)
        .with_context(|| format!("write output {}", out_path.display()))?;

    info!(path = %out_path.display(), "E2E results written");

    // Print human-readable summary.
    println!("\n── E2E Summary ──────────────────────────────────────────────────");
    let mut any_fail = false;
    for w in &final_report.workloads {
        let status = if w.passed { "PASS" } else { "FAIL" };
        let err = w
            .error
            .as_deref()
            .map(|e| format!(" — {e}"))
            .unwrap_or_default();
        println!("  [{status}] {}{}", w.name, err);
        if !w.passed {
            any_fail = true;
        }
        for m in &w.metrics {
            let pass_str = match m.passed {
                Some(true) => " [ok]",
                Some(false) => " [OVER BUDGET]",
                None => "",
            };
            println!("        {}: {:.3} {}{}", m.label, m.value, m.unit, pass_str);
        }
    }
    println!("─────────────────────────────────────────────────────────────────\n");

    if any_fail {
        std::process::exit(1);
    }
    Ok(())
}
