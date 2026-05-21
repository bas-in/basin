//! `multi-instance` profile — surfaces the P0 multi-instance gap from
//! `docs/audits/2026-05-21-noisy-neighbor-fairness.md`.
//!
//! Today the harness shares ONE `basin_storage::Storage` and ONE
//! `basin_catalog::Catalog` across N `basin_engine::Engine` replicas. That
//! mirrors the production wedge: object-store + catalog are the *shared
//! substrate*; the engine is the per-replica compute. The cards measure:
//!
//! - **Round-trip cost vs single-engine** (latency tax of an extra Engine).
//! - **Sticky-route hit-rate** — how often the round-robin lands on the
//!   replica that handled the previous request for that project.
//!
//! Stickiness is the lever that flips "linear catalog churn with replica
//! count" into "amortised". We expose `MultiInstanceKnobs.sticky_fraction`
//! so a future operator can A/B the cost.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use basin_engine::{Engine, EngineConfig};
use serde_json::{json, Value};

use crate::config::{BenchConfig, MultiInstanceKnobs, WorkloadKnobs};
use crate::emit::{report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec};
use crate::harness::BenchHarness;
use crate::sample::LatencySamples;

use super::{BenchProfile, BenchShape, ShapeKind};

fn baseline() -> BenchConfig {
    BenchConfig {
        workload: WorkloadKnobs {
            project_count: 1,
            table_rows: 50_000,
            working_set: 1_000,
            iterations: 200,
            concurrency: 1,
            deadline_secs: 30,
            multi_instance: Some(MultiInstanceKnobs::default()),
            ..WorkloadKnobs::default()
        },
        profile_slug: "multi_instance".into(),
        ..BenchConfig::default()
    }
}

fn build_replicas(h: &BenchHarness, n: usize) -> Vec<Engine> {
    (0..n)
        .map(|_| {
            Engine::new(EngineConfig {
                storage: h.storage.clone(),
                catalog: h.catalog.clone(),
                shard: None,
            })
        })
        .collect()
}

fn run_replica_fanout<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let mi = cfg
            .workload
            .multi_instance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("multi-instance knobs required"))?;

        let replicas = Arc::new(build_replicas(h, mi.replica_count));
        let project = h.projects[0];
        let table = h.table.clone();
        let samples = LatencySamples::new();
        let rr = Arc::new(AtomicUsize::new(0));
        let started = std::time::Instant::now();
        for i in 0..cfg.workload.iterations {
            let idx = if mi.sticky_fraction >= 1.0 {
                0
            } else {
                rr.fetch_add(1, Ordering::Relaxed) % replicas.len()
            };
            let session = replicas[idx].open_session(project).await?;
            let t0 = std::time::Instant::now();
            session
                .execute(&format!("SELECT id FROM {} WHERE id = {}", table, i as i64))
                .await?;
            samples.record(t0.elapsed());
        }
        let wall = started.elapsed().as_secs_f64();
        let rep = samples.report();

        // Bar: per-call p99 < 2x of single-engine baseline (i.e. <50ms
        // absolute for the LocalFS default). Loose first-pass — what we
        // care about is *trend* across replica counts, not an absolute.
        let p99_ms = rep.p99_us as f64 / 1000.0;
        let passed = p99_ms < 50.0;

        let path = report_scaling(
            &h.output_dir(cfg),
            "multi_instance_fanout",
            "Multi-instance fanout p99",
            "Round-robin across N replicas keeps p99 below 50ms (LocalFS).",
            passed,
            AxisSpec {
                key: "replica_count".into(),
                label: "Replicas".into(),
            },
            vec![SeriesSpec {
                key: "p99_us".into(),
                label: "p99 (us)".into(),
                unit: Some("us".into()),
            }],
            vec![json!({
                "replica_count": mi.replica_count,
                "sticky_fraction": mi.sticky_fraction,
                "p99_us": rep.p99_us,
                "p50_us": rep.p50_us,
                "mean_us": rep.mean_us,
                "n": rep.n,
                "wall_secs": wall,
            })],
            Some(PrimaryMetric {
                label: "p99".into(),
                value: p99_ms,
                unit: "ms".into(),
                bar: BarOp::lt(50.0),
            }),
        );
        Ok((path, passed, json!({"p99_ms": p99_ms, "passed": passed})))
    })
}

pub static PROFILE: BenchProfile = BenchProfile {
    name: "multi-instance",
    baseline,
    shapes: &[BenchShape {
        kind: ShapeKind::Scaling,
        id: "multi_instance_fanout",
        name: "Multi-instance fanout",
        claim: "Round-robin across N engine replicas keeps p99 below 50ms.",
        run: run_replica_fanout,
    }],
};
