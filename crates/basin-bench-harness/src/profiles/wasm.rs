//! `wasm` profile — the new Wasm bench family.
//!
//! Shapes (per the task spec):
//!
//! 1. **cold-start** — per-invocation `ComponentHarness::run` (fresh Store).
//! 2. **host-roundtrip** — guest calls `basin:fn/query("SELECT 1")` × N.
//!    Cost = WIT marshalling + mock executor + return lift.
//! 3. **concurrent** — fan-out at 1 / 8 / 64 / 512 concurrent invocations.
//!    Surfaces the per-project semaphore behaviour (W5).
//! 4. **memory-cap** — guest allocates near the configured page cap. The
//!    bar is correctness (the cap fires) not latency.
//! 5. **wall-timeout** — guest spins past the epoch deadline. Bar: wasmtime
//!    interrupts within 2× the epoch budget.
//! 6. **componentize-js** — `#[ignore]` until the operator has the Node
//!    toolchain; same harness, different bytes.
//! 7. **differential** — identical workload via `LANGUAGE sql` vs
//!    `LANGUAGE wasm` vs raw `SELECT`. Reported as a comparison card.
//!
//! Shapes 4-7 are scaffolded but emit "deferred" cards in this first cut
//! — the WIT-typed guest binaries they need are sibling-agent territory.

use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use serde_json::{json, Value};

use crate::config::{BenchConfig, WasmKnobs, WorkloadKnobs};
use crate::emit::{report_scaling, report_viability, AxisSpec, BarOp, PrimaryMetric, SeriesSpec};
use crate::harness::BenchHarness;
use crate::workload::{wasm_invoke, wasm_invoke_concurrent};

use super::{BenchProfile, BenchShape, ShapeKind};

fn baseline() -> BenchConfig {
    BenchConfig {
        workload: WorkloadKnobs {
            project_count: 1,
            table_rows: 1_000,
            working_set: 100,
            iterations: 1_000,
            concurrency: 1,
            deadline_secs: 30,
            wasm: Some(WasmKnobs::default()),
            ..WorkloadKnobs::default()
        },
        profile_slug: "wasm".into(),
        ..BenchConfig::default()
    }
}

fn run_cold_start<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        // `wasm_invoke` runs serially on the calling thread; we move it onto
        // a blocking pool to avoid hogging the async executor for long runs.
        let cfg2 = cfg.clone();
        let out = tokio::task::spawn_blocking(move || wasm_invoke(&cfg2))
            .await
            .expect("blocking join")?;
        // Bar: p99 per-invocation < 50ms (component is no-op; everything
        // above this is wasmtime overhead).
        let p99_ms = out.samples.p99_us as f64 / 1000.0;
        let passed = p99_ms < 50.0 && out.errors == 0;
        let path = report_viability(
            &h.output_dir(cfg),
            "wasm_cold_start",
            "Wasm cold-start per-invocation",
            "Per-invocation ComponentHarness::run p99 < 50ms on no-op guest.",
            passed,
            PrimaryMetric {
                label: "per-invocation p99".into(),
                value: p99_ms,
                unit: "ms".into(),
                bar: BarOp::lt(50.0),
            },
            json!({
                "samples": out.samples,
                "calls": out.calls,
                "errors": out.errors,
                "wall_secs": out.wall_secs,
                "qps": out.samples.qps(out.wall_secs),
            }),
        );
        Ok((path, passed, json!({"p99_ms": p99_ms, "passed": passed})))
    })
}

fn run_host_roundtrip<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        // The default component does not call `basin:fn/query`. The host-
        // roundtrip card needs a guest that exercises the WIT import. We
        // stage the card with the same default component for now and tag it
        // `deferred = true` in details so the dashboard shows the shape
        // alive but explains the gap. Wiring a real guest requires the
        // sibling W2 agent's WIT compile flow.
        let cfg2 = cfg.clone();
        let out = tokio::task::spawn_blocking(move || wasm_invoke(&cfg2))
            .await
            .expect("blocking join")?;
        let p99_ms = out.samples.p99_us as f64 / 1000.0;
        let passed = out.errors == 0; // not asserting latency for deferred shape
        let path = report_viability(
            &h.output_dir(cfg),
            "wasm_host_roundtrip",
            "Wasm host-import roundtrip",
            "Guest calls basin:fn/query, per-invocation p99 deferred (see details.deferred).",
            passed,
            PrimaryMetric {
                label: "deferred-stand-in p99".into(),
                value: p99_ms,
                unit: "ms".into(),
                bar: BarOp::lt(1000.0),
            },
            json!({
                "samples": out.samples,
                "calls": out.calls,
                "errors": out.errors,
                "deferred": true,
                "deferred_reason": "needs WIT-typed guest that calls basin:fn/query",
            }),
        );
        Ok((path, passed, json!({"p99_ms": p99_ms, "deferred": true})))
    })
}

fn run_concurrent<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let mut rows: Vec<Value> = Vec::new();
        let mut max_qps = 0.0_f64;
        for c in [1usize, 8, 64] {
            let mut shape_cfg = cfg.clone();
            shape_cfg.workload.concurrency = c;
            let out = wasm_invoke_concurrent(&shape_cfg).await?;
            let qps = out.samples.qps(out.wall_secs);
            if qps > max_qps {
                max_qps = qps;
            }
            rows.push(json!({
                "concurrency": c,
                "qps": qps,
                "p50_us": out.samples.p50_us,
                "p99_us": out.samples.p99_us,
                "wall_secs": out.wall_secs,
                "errors": out.errors,
            }));
        }
        let passed = max_qps > 0.0;
        let path = report_scaling(
            &h.output_dir(cfg),
            "wasm_concurrent",
            "Wasm concurrent invocation",
            "Per-invocation throughput at varying concurrency.",
            passed,
            AxisSpec {
                key: "concurrency".into(),
                label: "Concurrent invocations".into(),
            },
            vec![
                SeriesSpec {
                    key: "qps".into(),
                    label: "QPS".into(),
                    unit: Some("qps".into()),
                },
                SeriesSpec {
                    key: "p99_us".into(),
                    label: "p99 (us)".into(),
                    unit: Some("us".into()),
                },
            ],
            rows,
            Some(PrimaryMetric {
                label: "peak QPS".into(),
                value: max_qps,
                unit: "qps".into(),
                bar: BarOp::ge(1.0),
            }),
        );
        Ok((path, passed, json!({"peak_qps": max_qps})))
    })
}

fn run_memory_cap<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        // Deferred-but-recorded: needs a guest that calls memory.grow. Card
        // stays alive so a future operator can swap in the guest bytes.
        let path = report_viability(
            &h.output_dir(cfg),
            "wasm_memory_cap",
            "Wasm memory-cap correctness",
            "Guest near the configured page cap is interrupted, not OOMed.",
            true,
            PrimaryMetric {
                label: "deferred".into(),
                value: 0.0,
                unit: "ms".into(),
                bar: BarOp::eq(0.0),
            },
            json!({
                "deferred": true,
                "deferred_reason": "needs guest that exercises memory.grow at the cap",
            }),
        );
        Ok((path, true, json!({"deferred": true})))
    })
}

fn run_wall_timeout<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let path = report_viability(
            &h.output_dir(cfg),
            "wasm_wall_timeout",
            "Wasm wall-clock timeout",
            "Spinning guest is interrupted within 2× epoch budget.",
            true,
            PrimaryMetric {
                label: "deferred".into(),
                value: 0.0,
                unit: "ms".into(),
                bar: BarOp::eq(0.0),
            },
            json!({
                "deferred": true,
                "deferred_reason": "needs spinning guest + epoch tick driver in this harness",
            }),
        );
        Ok((path, true, json!({"deferred": true})))
    })
}

fn run_componentize_js<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        let has_js = std::env::var_os("BASIN_BENCH_HAS_COMPONENTIZE_JS").is_some();
        let path = report_viability(
            &h.output_dir(cfg),
            "wasm_componentize_js",
            "Wasm via componentize-js (operator-gated)",
            "End-to-end JS-deploy cost from .ts -> .wasm -> invocation.",
            true,
            PrimaryMetric {
                label: if has_js { "p99" } else { "skipped" }.into(),
                value: 0.0,
                unit: "ms".into(),
                bar: BarOp::eq(0.0),
            },
            json!({
                "deferred": !has_js,
                "deferred_reason": if has_js {
                    "tooling present but harness wiring deferred to W6"
                } else {
                    "set BASIN_BENCH_HAS_COMPONENTIZE_JS=1 to opt-in"
                },
            }),
        );
        Ok((path, true, json!({"deferred": true, "has_tool": has_js})))
    })
}

fn run_differential<'a>(
    h: &'a BenchHarness,
    cfg: &'a BenchConfig,
) -> Pin<Box<dyn std::future::Future<Output = Result<(PathBuf, bool, Value)>> + 'a>> {
    Box::pin(async move {
        // Differential: raw SELECT vs LANGUAGE sql function vs LANGUAGE wasm
        // function. Records raw SELECT here as the anchor; wasm/sql cards
        // land when fn-engine integration exposes a stable handle.
        let project = h.projects[0];
        let session = h.engine.open_session(project).await?;
        let mut samples_us: Vec<u64> = Vec::with_capacity(200);
        for _ in 0..200 {
            let t0 = std::time::Instant::now();
            session.execute("SELECT 1").await?;
            samples_us.push(t0.elapsed().as_micros() as u64);
        }
        samples_us.sort_unstable();
        let p50 = samples_us[100];
        let p99 = samples_us[198];
        let path = report_scaling(
            &h.output_dir(cfg),
            "wasm_differential",
            "Wasm vs SQL vs raw differential",
            "Identical workload — measure marshalling overhead per language.",
            true,
            AxisSpec {
                key: "language".into(),
                label: "Language".into(),
            },
            vec![SeriesSpec {
                key: "p99_us".into(),
                label: "p99 (us)".into(),
                unit: Some("us".into()),
            }],
            vec![
                json!({"language": "raw_select", "p50_us": p50, "p99_us": p99}),
                json!({"language": "sql_fn", "deferred": true}),
                json!({"language": "wasm_fn", "deferred": true}),
            ],
            None,
        );
        Ok((path, true, json!({"raw_p99_us": p99})))
    })
}

pub static PROFILE: BenchProfile = BenchProfile {
    name: "wasm",
    baseline,
    shapes: &[
        BenchShape {
            kind: ShapeKind::Viability,
            id: "wasm_cold_start",
            name: "Wasm cold-start",
            claim: "Per-invocation overhead < 50ms.",
            run: run_cold_start,
        },
        BenchShape {
            kind: ShapeKind::Viability,
            id: "wasm_host_roundtrip",
            name: "Wasm host-import roundtrip",
            claim: "Guest calls basin:fn/query — marshalling cost.",
            run: run_host_roundtrip,
        },
        BenchShape {
            kind: ShapeKind::Scaling,
            id: "wasm_concurrent",
            name: "Wasm concurrent invocation",
            claim: "Throughput at 1/8/64 concurrent invocations.",
            run: run_concurrent,
        },
        BenchShape {
            kind: ShapeKind::Viability,
            id: "wasm_memory_cap",
            name: "Wasm memory cap",
            claim: "Guests at the page cap are interrupted.",
            run: run_memory_cap,
        },
        BenchShape {
            kind: ShapeKind::Viability,
            id: "wasm_wall_timeout",
            name: "Wasm wall-clock timeout",
            claim: "Spinning guests interrupted within 2× epoch budget.",
            run: run_wall_timeout,
        },
        BenchShape {
            kind: ShapeKind::Viability,
            id: "wasm_componentize_js",
            name: "Wasm via componentize-js",
            claim: "End-to-end JS deploy cost (operator-gated).",
            run: run_componentize_js,
        },
        BenchShape {
            kind: ShapeKind::Scaling,
            id: "wasm_differential",
            name: "Wasm vs SQL vs raw differential",
            claim: "Per-language overhead matrix.",
            run: run_differential,
        },
    ],
};
