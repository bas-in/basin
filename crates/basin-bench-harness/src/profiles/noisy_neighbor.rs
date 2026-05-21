//! `noisy-neighbor` profile — exercises the scenarios from
//! `docs/audits/2026-05-21-noisy-neighbor-fairness.md`.
//!
//! Today's `tests/integration/tests/scaling_noisy_neighbor.rs` ships ONE
//! scenario (quiet point-select + 4 noisy full-scans, baseline-vs-under-load
//! degradation ratio). The audit calls out three additional P0/P1 axes:
//!
//! 1. **`BASIN_QUERY_COST_LIMIT_ROWS` off**: a noisy tenant runs an unbounded
//!    cartesian self-join; quiet tenants observe their p99 climb past the
//!    5x bar. Currently records the gap; the fix is in W6's scope.
//! 2. **Catalog Postgres = single Mutex<Client>**: every noisy tenant DDL
//!    contends with every quiet tenant `load_table`. We surface the
//!    contention by issuing N parallel `CREATE INDEX` against the noisy
//!    tenant while the quiet tenants point-query.
//! 3. **Per-project counters but no per-project executor**: the noisy
//!    tenant saturates the runtime; we measure how many quiet tenants
//!    degrade as a function of N (= `quiet_tenants`).
//!
//! This first cut wires scenario (3) — the other two require knobs the
//! harness doesn't own yet (cost-limit env var; catalog backend). Each
//! scenario emits its own card so adding them is additive, not rewriting.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::config::{BenchConfig, NoisyKnobs, WorkloadKnobs};
use crate::emit::{report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec};
use crate::harness::BenchHarness;
use crate::sample::LatencySamples;

use super::{BenchProfile, BenchShape, ShapeKind};

fn baseline() -> BenchConfig {
    BenchConfig {
        workload: WorkloadKnobs {
            project_count: 9, // 1 noisy + 8 quiet (default)
            table_rows: 50_000,
            working_set: 1_000,
            iterations: 200,
            concurrency: 1,
            deadline_secs: 60,
            noisy: Some(NoisyKnobs::default()),
            ..WorkloadKnobs::default()
        },
        profile_slug: "noisy_neighbor".into(),
        ..BenchConfig::default()
    }
}

/// Scenario: how many quiet tenants does ONE noisy tenant degrade?
///
/// We run the noisy tenant doing `SELECT COUNT(*) FROM events` in a tight
/// loop, while each quiet tenant runs N point-selects. We report the per-
/// tenant p99 vs the baseline p99 (taken from a no-noisy warmup).
fn run_quiet_degradation<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let noisy = cfg
            .workload
            .noisy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("noisy knobs required for noisy-neighbor profile"))?;

        // Project[0] is noisy; remainder are quiet.
        let noisy_project = h.projects[0];
        let quiet_projects: Vec<_> = h
            .projects
            .iter()
            .skip(1)
            .take(noisy.quiet_tenants)
            .copied()
            .collect();
        let table = h.table.clone();

        // --- Phase A: baseline (no noisy) ---
        let baseline_samples = LatencySamples::new();
        for &p in &quiet_projects {
            let session = h.engine.open_session(p).await?;
            for i in 0..cfg.workload.iterations.min(100) {
                let t0 = std::time::Instant::now();
                session
                    .execute(&format!("SELECT id FROM {} WHERE id = {}", table, i as i64))
                    .await?;
                baseline_samples.record(t0.elapsed());
            }
        }
        let baseline = baseline_samples.report();

        // --- Phase B: under load ---
        let under_load_samples = LatencySamples::new();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Spawn noisy: N concurrent full-scans on noisy_project.
        let mut noisy_set = JoinSet::new();
        for _ in 0..noisy.noisy_concurrency {
            let engine = h.engine.clone();
            let table = table.clone();
            let stop = stop.clone();
            noisy_set.spawn(async move {
                let session = engine.open_session(noisy_project).await.expect("noisy session");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = session
                        .execute(&format!("SELECT COUNT(*) FROM {}", table))
                        .await;
                }
            });
        }

        // Quiet point-queries with noisy running.
        let mut quiet_set = JoinSet::new();
        for &p in &quiet_projects {
            let engine = h.engine.clone();
            let table = table.clone();
            let s = under_load_samples.clone();
            let iters = cfg.workload.iterations.min(100);
            quiet_set.spawn(async move {
                let session = engine.open_session(p).await.expect("quiet session");
                for i in 0..iters {
                    let t0 = std::time::Instant::now();
                    let _ = session
                        .execute(&format!("SELECT id FROM {} WHERE id = {}", table, i as i64))
                        .await;
                    s.record(t0.elapsed());
                }
            });
        }
        while let Some(r) = quiet_set.join_next().await {
            r.expect("quiet panic");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        while let Some(r) = noisy_set.join_next().await {
            r.expect("noisy panic");
        }
        let under_load = under_load_samples.report();

        // Bar: under_load.p99 / baseline.p99 < 5x.
        let ratio = if baseline.p99_us > 0 {
            under_load.p99_us as f64 / baseline.p99_us as f64
        } else {
            f64::INFINITY
        };
        let passed = ratio < 5.0;

        let rows = vec![json!({
            "quiet_tenants": noisy.quiet_tenants,
            "baseline_p99_us": baseline.p99_us,
            "under_load_p99_us": under_load.p99_us,
            "ratio": ratio,
            "baseline_n": baseline.n,
            "under_load_n": under_load.n,
        })];
        let path = report_scaling(
            &h.output_dir(cfg),
            "noisy_neighbor_quiet_degradation",
            "Noisy-neighbor: quiet-tenant p99 under load",
            "Quiet tenants' p99 latency stays within 5x of baseline when one \
             noisy tenant runs full-scans.",
            passed,
            AxisSpec {
                key: "quiet_tenants".into(),
                label: "Quiet tenants".into(),
            },
            vec![
                SeriesSpec {
                    key: "baseline_p99_us".into(),
                    label: "Baseline p99".into(),
                    unit: Some("us".into()),
                },
                SeriesSpec {
                    key: "under_load_p99_us".into(),
                    label: "Under-load p99".into(),
                    unit: Some("us".into()),
                },
            ],
            rows,
            Some(PrimaryMetric {
                label: "under-load p99 / baseline p99".into(),
                value: ratio,
                unit: "x".into(),
                bar: BarOp::lt(5.0),
            }),
        );
        Ok((path, passed, json!({"ratio": ratio, "passed": passed})))
    })
}

pub static PROFILE: BenchProfile = BenchProfile {
    name: "noisy-neighbor",
    baseline,
    shapes: &[BenchShape {
        kind: ShapeKind::Scaling,
        id: "noisy_neighbor_quiet_degradation",
        name: "Quiet-tenant p99 under noisy load",
        claim: "Quiet tenants' p99 stays within 5x of baseline.",
        run: run_quiet_degradation,
    }],
};
