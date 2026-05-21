//! Criterion front-end for the Wasm bench family.
//!
//! This bench delegates to the SAME workload primitive
//! ([`basin_bench_harness::workload::wasm_invoke`]) the `wasm` profile uses,
//! so `cargo bench -p basin-bench-harness` and
//! `scripts/run-bench.sh wasm` exercise identical code paths. Criterion adds
//! statistical rigor (warm-up, outlier rejection, HTML report); the profile
//! adds the dashboard JSON. Both are first-class — neither is the "real" one.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use basin_bench_harness::config::{BenchConfig, WasmKnobs, WorkloadKnobs};
use basin_bench_harness::workload::wasm_invoke;

fn bench_cold_start(c: &mut Criterion) {
    let cfg = BenchConfig {
        workload: WorkloadKnobs {
            iterations: 1, // criterion drives the repeat loop
            wasm: Some(WasmKnobs::default()),
            ..WorkloadKnobs::default()
        },
        ..BenchConfig::default()
    };
    c.bench_function("wasm_cold_start_per_invocation", |b| {
        b.iter(|| {
            let out = wasm_invoke(black_box(&cfg)).expect("invoke");
            black_box(out.calls);
        });
    });
}

criterion_group!(benches, bench_cold_start);
criterion_main!(benches);
