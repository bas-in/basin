use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use basin_hottier::memtable::{MemRowValue, MemTable};
use basin_hottier::row_key::RowKey;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_key(n: i64) -> RowKey {
    RowKey::builder().append_i64(n).finish()
}

fn make_row(schema_version: u32) -> MemRowValue {
    // Simulate a realistic Arrow IPC single-row payload (~256 bytes).
    MemRowValue::row(vec![0xABu8; 256], schema_version)
}

fn populate(mt: &MemTable, n: usize) {
    for i in 0..n as i64 {
        mt.insert(make_key(i), make_row(0));
    }
}

// ── memtable_insert ──────────────────────────────────────────────────────────

fn bench_insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("memtable_insert");
    for &n in &[10_000usize, 100_000, 500_000] {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mt = MemTable::new();
                for i in 0..n as i64 {
                    mt.insert(black_box(make_key(i)), black_box(make_row(0)));
                }
                black_box(mt.total_count());
            });
        });
    }
    g.finish();
}

// ── memtable_point_lookup ────────────────────────────────────────────────────

fn bench_point_lookup(c: &mut Criterion) {
    let mut g = c.benchmark_group("memtable_point_lookup");
    // Pre-populate at 1M rows and measure point-lookup latency.
    for &n in &[100_000usize, 500_000, 1_000_000] {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mt = MemTable::new();
            populate(&mt, n);

            // Deterministic random lookup keys within the populated range.
            let mut rng = StdRng::seed_from_u64(0xBA51_4145);
            let lookups: Vec<i64> = (0..1000).map(|_| rng.gen_range(0..n as i64)).collect();
            let mut idx = 0usize;

            b.iter(|| {
                let k = make_key(lookups[idx % lookups.len()]);
                idx = idx.wrapping_add(1);
                black_box(mt.get(black_box(&k)));
            });
        });
    }
    g.finish();
}

// ── memtable_range_scan ──────────────────────────────────────────────────────

fn bench_range_scan(c: &mut Criterion) {
    let mut g = c.benchmark_group("memtable_range_scan");
    // 1M rows; scan 1k-row windows.
    let n = 1_000_000usize;
    let window = 1_000i64;

    g.bench_function("1k_range_at_1M_rows", |b| {
        let mt = MemTable::new();
        populate(&mt, n);

        let mut rng = StdRng::seed_from_u64(0xBA51_4145);
        let starts: Vec<i64> = (0..100)
            .map(|_| rng.gen_range(0..(n as i64 - window)))
            .collect();
        let mut idx = 0usize;

        b.iter(|| {
            let lo_val = starts[idx % starts.len()];
            idx = idx.wrapping_add(1);
            let lo = make_key(lo_val);
            let hi = make_key(lo_val + window - 1);
            black_box(mt.range_scan(black_box(&lo), black_box(&hi)));
        });
    });
    g.finish();
}

criterion_group!(benches, bench_insert, bench_point_lookup, bench_range_scan);
criterion_main!(benches);
