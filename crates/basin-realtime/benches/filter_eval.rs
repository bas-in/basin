//! Microbenchmark for subscriber-side predicate evaluation (Phase 5.11.R5).
//!
//! Acceptance gate: ≤ 50 µs per predicate eval.
//! Run manually (not CI-gated):
//!
//!   cargo bench -p basin-realtime --bench filter_eval
//!
//! The bench measures [`Filter::matches`] in isolation — compile-once, reuse
//! per event — so it reflects the per-event cost after subscription is set up.

use basin_common::{ChangeEvent, ChangeOp, ProjectId, TableName};
use basin_realtime::Filter;
use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;

fn make_event(status: &str, amount: u64) -> ChangeEvent {
    ChangeEvent {
        project: ProjectId::new(),
        table: TableName::new("orders").unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(json!({"status": status, "amount": amount})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    }
}

fn bench_simple_eq(c: &mut Criterion) {
    // Single equality comparison — the most common predicate shape.
    let filter = Filter::new("NEW.status = 'paid'").unwrap();
    let event_match = make_event("paid", 100);
    let event_miss = make_event("pending", 100);

    c.bench_function("filter_eval/simple_eq_match", |b| {
        b.iter(|| {
            black_box(filter.matches(black_box(&event_match)).unwrap());
        });
    });

    c.bench_function("filter_eval/simple_eq_miss", |b| {
        b.iter(|| {
            black_box(filter.matches(black_box(&event_miss)).unwrap());
        });
    });
}

fn bench_and_composition(c: &mut Criterion) {
    // AND of two comparisons — typical real-world filter.
    let filter = Filter::new("NEW.status = 'paid' AND NEW.amount > 50").unwrap();
    let event = make_event("paid", 100);

    c.bench_function("filter_eval/and_two_terms", |b| {
        b.iter(|| {
            black_box(filter.matches(black_box(&event)).unwrap());
        });
    });
}

fn bench_is_null(c: &mut Criterion) {
    let filter = Filter::new("NEW.deleted_at IS NULL").unwrap();
    // Field absent → treated as NULL → filter passes.
    let event = make_event("paid", 100);

    c.bench_function("filter_eval/is_null_missing_field", |b| {
        b.iter(|| {
            black_box(filter.matches(black_box(&event)).unwrap());
        });
    });
}

criterion_group!(benches, bench_simple_eq, bench_and_composition, bench_is_null);
criterion_main!(benches);
