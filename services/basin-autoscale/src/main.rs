//! `basin-autoscale` daemon — engine shard autoscale controller.
//!
//! # Overview
//!
//! This binary reads per-project usage snapshots (from the
//! `project_usage_periods` catalog table or from an OTLP collector) and
//! drives shard split / rebalance operations via `basin-cli admin split` /
//! `basin-cli admin rebalance`.
//!
//! # What this is NOT
//!
//! The Fly Machines autoscaler (`backend-rs/src/bin/autoscaler.rs`) manages
//! the *cloud infrastructure* layer — spawning or destroying Fly VMs based on
//! how many projects are active. This binary is the *engine shard* layer:
//! deciding when individual projects need their hash-bucket shards split or
//! merged based on CPU / ops load.
//!
//! # Configuration
//!
//! CLI flags are the primary interface; all flags are also readable from
//! `BASIN_AUTOSCALE_*` env vars (listed in each flag's help text). The only
//! mandatory configuration is a working Postgres DSN for the catalog
//! (`BASIN_CATALOG_PG` env var, no default).
//!
//! # Sources
//!
//! ## Catalog poll mode (default)
//!
//! Reads the `project_usage_periods` table directly from the basin catalog
//! Postgres instance. Suitable for single-node deployments without a
//! dedicated OTLP collector.
//!
//! ## OTLP mode
//!
//! When `BASIN_OTLP_ENDPOINT` is set, the daemon subscribes to the OTLP
//! counter stream instead. (Implementation: the `basin.project.usage` target
//! events emitted by `basin-catalog::usage::spawn_usage_snapshot_task` are
//! scraped from the collector's query API.)
//!
//! At present the OTLP path falls back to catalog poll mode and emits a
//! `tracing::warn!` — a full OTLP subscriber is planned for v0.3.
//!
//! # Actions
//!
//! When `Scaler::evaluate` returns `SplitShard` or `MergeShards`, the daemon
//! shells out:
//!
//! ```text
//! basin-cli admin split   <project_id>
//! basin-cli admin rebalance <project_id>
//! ```
//!
//! If `basin-cli` is not on PATH, or if `--dry-run` is set, the action is
//! logged but not executed.

use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use basin_autoscale::{ProjectSnapshot, ScaleDecision, Scaler, ScalerConfig};
use basin_common::ProjectId;
use clap::Parser;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

/// Basin engine shard autoscale controller.
///
/// Reads per-project usage counters and drives `basin-cli admin split` /
/// `basin-cli admin rebalance` when thresholds are exceeded.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Tick interval between evaluation cycles, in seconds.
    /// Env: BASIN_AUTOSCALE_INTERVAL_SECS (default 60).
    #[arg(long, default_value = "60", env = "BASIN_AUTOSCALE_INTERVAL_SECS")]
    interval: u64,

    /// Log decisions but do not execute any `basin-cli` commands.
    /// Env: BASIN_AUTOSCALE_DRY_RUN.
    #[arg(long, env = "BASIN_AUTOSCALE_DRY_RUN")]
    dry_run: bool,

    /// Run exactly one evaluation cycle then exit (useful for cron /
    /// one-shot invocations from an orchestrator).
    /// Env: BASIN_AUTOSCALE_ONCE.
    #[arg(long, env = "BASIN_AUTOSCALE_ONCE")]
    once: bool,

    /// CPU percentage threshold above which a project is considered "hot".
    /// Range: 1–100. Env: BASIN_AUTOSCALE_THRESHOLD_CPU_PCT (default 80).
    #[arg(
        long,
        default_value = "80",
        env = "BASIN_AUTOSCALE_THRESHOLD_CPU_PCT",
        value_parser = clap::value_parser!(u8).range(1..=100),
    )]
    threshold_cpu_pct: u8,

    /// CPU percentage threshold below which a project is considered "cool"
    /// and eligible for shard merge. Must be ≤ threshold_cpu_pct.
    /// Env: BASIN_AUTOSCALE_MERGE_THRESHOLD_CPU_PCT (default 30).
    #[arg(
        long,
        default_value = "30",
        env = "BASIN_AUTOSCALE_MERGE_THRESHOLD_CPU_PCT",
        value_parser = clap::value_parser!(u8).range(1..=100),
    )]
    merge_threshold_cpu_pct: u8,

    /// Consecutive hot (or cool) ticks required before a split (or merge).
    /// Env: BASIN_AUTOSCALE_HYSTERESIS_WINDOW (default 3).
    #[arg(long, default_value = "3", env = "BASIN_AUTOSCALE_HYSTERESIS_WINDOW")]
    hysteresis_window: u32,

    /// Minimum number of shards per project.
    /// Env: BASIN_AUTOSCALE_MIN_SHARDS (default 1).
    #[arg(long, default_value = "1", env = "BASIN_AUTOSCALE_MIN_SHARDS")]
    min_shards: u32,

    /// Maximum number of shards per project.
    /// Env: BASIN_AUTOSCALE_MAX_SHARDS (default 16).
    #[arg(long, default_value = "16", env = "BASIN_AUTOSCALE_MAX_SHARDS")]
    max_shards: u32,

    /// Cooldown in seconds between successive split/merge actions for the
    /// same project. Env: BASIN_AUTOSCALE_COOLDOWN_SECS (default 300).
    #[arg(long, default_value = "300", env = "BASIN_AUTOSCALE_COOLDOWN_SECS")]
    cooldown_secs: u64,

    /// basin-catalog Postgres DSN. Only needed in catalog-poll mode.
    /// Env: BASIN_CATALOG_PG (required unless BASIN_OTLP_ENDPOINT is set).
    #[arg(long, env = "BASIN_CATALOG_PG")]
    catalog_pg: Option<String>,

    /// OTLP collector endpoint. When set, the daemon prefers OTLP over
    /// catalog-poll (OTLP path is stubbed in v0.2; falls back to poll).
    /// Env: BASIN_OTLP_ENDPOINT.
    #[arg(long, env = "BASIN_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let args = Args::parse();
    run(args).await
}

async fn run(args: Args) -> Result<()> {
    let config = ScalerConfig {
        // CPU budget: interval_secs × 1 000 ms (1 vCPU equivalent).
        // Operators with multi-vCPU machines should increase this or set
        // BASIN_AUTOSCALE_INTERVAL_SECS accordingly.
        cpu_budget_ms_per_tick: args.interval.saturating_mul(1_000),
        threshold_cpu_pct: args.threshold_cpu_pct,
        merge_threshold_cpu_pct: args.merge_threshold_cpu_pct,
        hysteresis_window: args.hysteresis_window,
        min_shards: args.min_shards,
        max_shards: args.max_shards,
        cooldown: Duration::from_secs(args.cooldown_secs),
    };

    info!(
        interval_secs = args.interval,
        dry_run = args.dry_run,
        once = args.once,
        threshold_cpu_pct = config.threshold_cpu_pct,
        merge_threshold_cpu_pct = config.merge_threshold_cpu_pct,
        hysteresis_window = config.hysteresis_window,
        min_shards = config.min_shards,
        max_shards = config.max_shards,
        cooldown_secs = args.cooldown_secs,
        "basin-autoscale starting",
    );

    // Warn about the OTLP stub.
    if let Some(ref ep) = args.otlp_endpoint {
        warn!(
            otlp_endpoint = %ep,
            "BASIN_OTLP_ENDPOINT is set but the OTLP subscriber is not yet \
             implemented (v0.3); falling back to catalog-poll mode",
        );
    }

    let scaler = Mutex::new(Scaler::new(config));
    let interval = Duration::from_secs(args.interval);

    loop {
        let snapshots = fetch_snapshots(&args).await;

        match snapshots {
            Err(e) => {
                warn!(error = %e, "failed to fetch usage snapshots; skipping tick");
            }
            Ok(snaps) => {
                info!(projects = snaps.len(), "evaluating autoscale tick");

                let mut guard = scaler.lock().expect("scaler mutex poisoned");
                for snap in &snaps {
                    let decision = guard.evaluate(snap);
                    handle_decision(decision, args.dry_run);
                }
            }
        }

        if args.once {
            info!("--once flag set; exiting after first tick");
            break;
        }

        tokio::time::sleep(interval).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Source: catalog poll
// ---------------------------------------------------------------------------

/// Fetch the latest `ProjectSnapshot` for every active project from the
/// `project_usage_periods` catalog table.
///
/// Returns the most-recent row per project (ordered by `period_start DESC`).
/// When `BASIN_OTLP_ENDPOINT` is set this function still runs because the
/// OTLP subscriber is a v0.3 stub; the OTLP warning is emitted in `run`.
async fn fetch_snapshots(args: &Args) -> Result<Vec<ProjectSnapshot>> {
    let pg_dsn = args
        .catalog_pg
        .as_deref()
        .context("BASIN_CATALOG_PG is required for catalog-poll mode")?;

    let (client, conn) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .context("failed to connect to catalog postgres")?;

    // Drive the connection on a separate task.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!(error = %e, "catalog postgres connection error");
        }
    });

    // Query: most-recent row per project from project_usage_periods.
    let rows = client
        .query(
            r#"
            SELECT DISTINCT ON (project_id)
                project_id,
                period_start,
                storage_bytes,
                ops_count,
                active_hours,
                cpu_ms
            FROM project_usage_periods
            ORDER BY project_id, period_start DESC
            "#,
            &[],
        )
        .await
        .context("failed to query project_usage_periods")?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let project_id_str: String = row.try_get("project_id")?;
        let project_id: ProjectId = project_id_str
            .parse()
            .context("invalid project_id in project_usage_periods")?;

        let period_start: chrono::DateTime<chrono::Utc> = row.try_get("period_start")?;
        let storage_bytes: i64 = row.try_get("storage_bytes")?;
        let ops_count: i64 = row.try_get("ops_count")?;
        let active_hours: f64 = row.try_get("active_hours")?;
        let cpu_ms: i64 = row.try_get("cpu_ms")?;

        result.push(ProjectSnapshot {
            project_id,
            period_start,
            storage_bytes: storage_bytes.max(0) as u64,
            ops_count: ops_count.max(0) as u64,
            active_hours,
            cpu_ms: cpu_ms.max(0) as u64,
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Action executor
// ---------------------------------------------------------------------------

/// Execute or log a scale decision.
fn handle_decision(decision: ScaleDecision, dry_run: bool) {
    match decision {
        ScaleDecision::Hold => {
            // No action; debug-level to avoid log spam on steady state.
        }
        ScaleDecision::SplitShard { project_id, reason } => {
            info!(
                project_id = %project_id,
                reason = %reason,
                dry_run,
                "ScaleDecision::SplitShard",
            );
            if !dry_run {
                exec_cli_action("split", &project_id.to_string());
            }
        }
        ScaleDecision::MergeShards { project_id, reason } => {
            info!(
                project_id = %project_id,
                reason = %reason,
                dry_run,
                "ScaleDecision::MergeShards",
            );
            if !dry_run {
                exec_cli_action("rebalance", &project_id.to_string());
            }
        }
    }
}

/// Shell out to `basin-cli admin <subcommand> <project_id>`.
///
/// If `basin-cli` is not on PATH, or if the invocation fails, a `warn!` is
/// emitted but the daemon continues — autoscale actions are best-effort and
/// the next tick will re-evaluate.
fn exec_cli_action(subcommand: &str, project_id: &str) {
    if which_basin_cli().is_none() {
        warn!(
            subcommand,
            project_id,
            "basin-cli not found on PATH; install it or set PATH correctly \
             to enable autoscale actions",
        );
        return;
    }

    let output = std::process::Command::new("basin-cli")
        .args(["admin", subcommand, project_id])
        .output();

    match output {
        Ok(out) if out.status.success() => {
            info!(
                subcommand,
                project_id, "basin-cli admin {} succeeded", subcommand,
            );
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            warn!(
                subcommand,
                project_id,
                exit_code = out.status.code(),
                stderr = %stderr,
                "basin-cli admin {} failed", subcommand,
            );
        }
        Err(e) => {
            warn!(
                subcommand,
                project_id,
                error = %e,
                "failed to spawn basin-cli",
            );
        }
    }
}

/// Returns `Some(path)` if `basin-cli` can be found on PATH; `None` otherwise.
fn which_basin_cli() -> Option<String> {
    std::env::var("PATH").ok().and_then(|path| {
        std::env::split_paths(&path).find_map(|dir| {
            let candidate = dir.join("basin-cli");
            if candidate.exists() {
                candidate.to_str().map(String::from)
            } else {
                None
            }
        })
    })
}
