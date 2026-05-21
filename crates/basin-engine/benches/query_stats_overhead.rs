//! Synthetic overhead benchmark for `QueryStatRegistry` (Phase 5.16.B).
//!
//! Acceptance gate: a tight 10k-query loop with the registry attached must
//! have p99 overhead ≤ 1 % vs the same loop with the registry detached.
//!
//! Run with:
//! ```
//! cargo bench -p basin-engine --bench query_stats_overhead
//! ```
//!
//! This bench is not part of CI (Criterion benches require `--bench` flag
//! and are excluded from `cargo test`).

use basin_common::{ProjectId, TableName};
use basin_engine::query_shape::QueryShapeHash;
use basin_engine::query_stats::{QueryMetrics, QueryStatRegistry};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_registry_overhead(c: &mut Criterion) {
    let registry = QueryStatRegistry::new();
    let project = ProjectId::new();
    let table = TableName::new("events").expect("valid table name");
    // Use a small set of shapes (16) so the LRU is warm and we measure the
    // fast path (cache hit), not eviction churn.
    let hashes: Vec<QueryShapeHash> = (0..16u64).map(QueryShapeHash).collect();

    let metrics = QueryMetrics {
        latency_ns: 50_000,
        rows_scanned: 100,
        files_opened: 2,
        bytes_decoded: 131_072,
        cache_hits: 5,
        fast_path_engaged: false,
        table_row_count: 5_000,
    };

    // Pre-warm the registry so the first bench iteration doesn't pay
    // allocation cost.
    for h in &hashes {
        registry.observe(&project, &table, *h, &metrics);
    }

    let mut group = c.benchmark_group("query_stats");
    group.sample_size(200);

    // Baseline: tight loop doing nothing (just the overhead measurement harness).
    group.bench_function("baseline_noop", |b| {
        let mut i = 0usize;
        b.iter(|| {
            black_box(i);
            i = i.wrapping_add(1);
        });
    });

    // With registry: same tight loop but calling observe() each iteration.
    group.bench_function("observe_10k_shapes", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let h = hashes[i % hashes.len()];
            registry.observe(
                black_box(&project),
                black_box(&table),
                black_box(h),
                black_box(&metrics),
            );
            i = i.wrapping_add(1);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_registry_overhead);
criterion_main!(benches);
