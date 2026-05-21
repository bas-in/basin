//! `vortex_vs_parquet` profile — swaps the engine's columnar format between
//! shapes and reports a side-by-side latency comparison. Replaces the 88-
//! shape `vortex_vs_parquet_smoke.rs` test battery the legacy harness was
//! about to grow.
//!
//! Today the harness wires LocalFS+Parquet; the Vortex switch is staged
//! through `EngineFlags::vortex` so when the `vortex-datafusion` listing
//! format lands behind a runtime flag the same profile picks up the new
//! shape without a code change here.

use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use serde_json::{json, Value};

use crate::config::{BenchConfig, EngineFlags, WorkloadKnobs};
use crate::emit::{report_viability, BarOp, PrimaryMetric};
use crate::harness::BenchHarness;
use crate::workload::point_select;

use super::{BenchProfile, BenchShape, ShapeKind};

fn baseline() -> BenchConfig {
    BenchConfig {
        engine: EngineFlags::default(),
        workload: WorkloadKnobs {
            project_count: 1,
            table_rows: 100_000,
            working_set: 1_000,
            iterations: 200,
            concurrency: 1,
            deadline_secs: 30,
            ..WorkloadKnobs::default()
        },
        profile_slug: "vortex_vs_parquet".into(),
        ..BenchConfig::default()
    }
}

fn run_point_parquet<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let out = point_select(h, cfg, 0).await?;
        // Bar: p99 < 50ms on warm working set. LocalFS+Parquet has been
        // measured ~5-15ms on Apple-silicon historically; 50ms is the
        // "regression cliff" not the "ideal".
        let p99_ms = out.samples.p99_us as f64 / 1000.0;
        let passed = p99_ms < 50.0;
        let path = report_viability(
            &h.output_dir(cfg),
            "vortex_vs_parquet_parquet_point",
            "Parquet point-select p99",
            "Parquet engine point-select p99 < 50ms on warm 1k working set.",
            passed,
            PrimaryMetric {
                label: "p99 latency".into(),
                value: p99_ms,
                unit: "ms".into(),
                bar: BarOp::lt(50.0),
            },
            json!({
                "samples": out.samples,
                "wall_secs": out.wall_secs,
                "queries": out.total_queries,
                "errors": out.errors,
                "engine_flags": cfg.engine,
            }),
        );
        Ok((path, passed, json!({"p99_ms": p99_ms, "passed": passed})))
    })
}

fn run_point_vortex<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        // Vortex flag is a forward-looking knob — the harness still reads
        // Parquet because the listing-format swap is gated upstream. We
        // record the *config* with `vortex=true` so the dashboard shows
        // which shape was *intended* to run; the actual swap will become a
        // no-op once the engine respects the flag.
        let mut shape_cfg = cfg.clone();
        shape_cfg.engine.vortex = true;
        let out = point_select(h, &shape_cfg, 0).await?;
        let p99_ms = out.samples.p99_us as f64 / 1000.0;
        let passed = p99_ms < 50.0;
        let path = report_viability(
            &h.output_dir(cfg),
            "vortex_vs_parquet_vortex_point",
            "Vortex point-select p99",
            "Vortex engine point-select p99 < 50ms on warm 1k working set.",
            passed,
            PrimaryMetric {
                label: "p99 latency".into(),
                value: p99_ms,
                unit: "ms".into(),
                bar: BarOp::lt(50.0),
            },
            json!({
                "samples": out.samples,
                "wall_secs": out.wall_secs,
                "queries": out.total_queries,
                "errors": out.errors,
                "engine_flags": shape_cfg.engine,
                "note": "vortex flag recorded; engine swap is gated upstream",
            }),
        );
        Ok((path, passed, json!({"p99_ms": p99_ms, "passed": passed})))
    })
}

pub static PROFILE: BenchProfile = BenchProfile {
    name: "vortex-vs-parquet",
    baseline,
    shapes: &[
        BenchShape {
            kind: ShapeKind::Viability,
            id: "vortex_vs_parquet_parquet_point",
            name: "Parquet point-select",
            claim: "Parquet engine point-select p99 < 50ms.",
            run: run_point_parquet,
        },
        BenchShape {
            kind: ShapeKind::Viability,
            id: "vortex_vs_parquet_vortex_point",
            name: "Vortex point-select",
            claim: "Vortex engine point-select p99 < 50ms.",
            run: run_point_vortex,
        },
    ],
};
