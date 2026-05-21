//! Smoke tests for the unified harness — the CI fast subset.
//!
//! These run a tiny version of each profile (≤200 iterations, ≤20k rows) so
//! the whole file finishes well under the CI <5min budget. They assert the
//! harness builds, the workload primitives run, and a JSON sidecar lands —
//! NOT that any latency bar passes (numbers vary by machine). The heavier
//! "full baseline" runs go through `scripts/run-bench.sh <profile>` and are
//! NOT part of `cargo test`.

use basin_bench_harness::config::{BenchConfig, WasmKnobs, WorkloadKnobs};
use basin_bench_harness::runner::BenchSuite;
use basin_bench_harness::workload::{wasm_invoke, wasm_invoke_concurrent};

/// The default no-op Wasm component runs and records samples.
#[test]
fn wasm_invoke_serial_smoke() {
    let cfg = BenchConfig {
        workload: WorkloadKnobs {
            iterations: 16,
            wasm: Some(WasmKnobs::default()),
            ..WorkloadKnobs::default()
        },
        ..BenchConfig::default()
    };
    let out = wasm_invoke(&cfg).expect("wasm invoke");
    assert_eq!(out.calls, 16);
    assert_eq!(out.errors, 0, "no-op component must not error");
    assert!(out.samples.n >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wasm_invoke_concurrent_smoke() {
    let cfg = BenchConfig {
        workload: WorkloadKnobs {
            iterations: 32,
            concurrency: 8,
            wasm: Some(WasmKnobs::default()),
            ..WorkloadKnobs::default()
        },
        ..BenchConfig::default()
    };
    let out = wasm_invoke_concurrent(&cfg).await.expect("wasm concurrent");
    assert_eq!(out.errors, 0);
    assert!(out.calls >= 1);
}

/// The wasm profile runs all seven shapes and writes JSON to a temp dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wasm_profile_runs_all_shapes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let out_dir = tmp.path().to_path_buf();
    let results = BenchSuite::run("wasm", move |cfg| {
        cfg.workload.iterations = 32;
        cfg.workload.table_rows = 1_000;
        cfg.output_dir = Some(out_dir.clone());
    })
    .await
    .expect("wasm profile");
    // Seven shapes registered.
    assert_eq!(results.len(), 7, "expected 7 wasm shapes");
    // At least the cold-start + concurrent + differential JSONs landed.
    let written: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        written.iter().any(|f| f.contains("wasm_cold_start")),
        "cold-start sidecar should exist; got {written:?}"
    );
}

/// The multi-instance profile builds N replicas and fans out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_instance_profile_smoke() {
    let tmp = tempfile::TempDir::new().unwrap();
    let out_dir = tmp.path().to_path_buf();
    let results = BenchSuite::run("multi-instance", move |cfg| {
        cfg.workload.iterations = 32;
        cfg.workload.table_rows = 1_000;
        cfg.output_dir = Some(out_dir.clone());
    })
    .await
    .expect("multi-instance profile");
    assert_eq!(results.len(), 1);
}

/// The vortex-vs-parquet profile runs both format shapes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vortex_vs_parquet_profile_smoke() {
    let tmp = tempfile::TempDir::new().unwrap();
    let out_dir = tmp.path().to_path_buf();
    let results = BenchSuite::run("vortex-vs-parquet", move |cfg| {
        cfg.workload.iterations = 32;
        cfg.workload.table_rows = 2_000;
        cfg.output_dir = Some(out_dir.clone());
    })
    .await
    .expect("vortex-vs-parquet profile");
    assert_eq!(results.len(), 2);
}

/// The noisy-neighbor profile runs the quiet-degradation scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn noisy_neighbor_profile_smoke() {
    let tmp = tempfile::TempDir::new().unwrap();
    let out_dir = tmp.path().to_path_buf();
    let results = BenchSuite::run("noisy-neighbor", move |cfg| {
        cfg.workload.iterations = 32;
        cfg.workload.table_rows = 2_000;
        if let Some(n) = cfg.workload.noisy.as_mut() {
            n.quiet_tenants = 3;
        }
        cfg.workload.project_count = 4; // 1 noisy + 3 quiet
        cfg.output_dir = Some(out_dir.clone());
    })
    .await
    .expect("noisy-neighbor profile");
    assert_eq!(results.len(), 1);
}
