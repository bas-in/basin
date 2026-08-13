//! Batch-boundary correctness tests for every `basin-exec` operator.
//!
//! # Why this file exists
//!
//! `basin-exec`'s operators emit `RecordBatch`es of up to 8192 rows. Nearly
//! every test elsewhere in this crate uses a handful of rows, which never
//! exercises what happens when data crosses a batch boundary — a benchmark
//! written against this crate once pulled ONE batch from an aggregate and
//! compared its row count against the full group count (`8192 == 100000`),
//! silently assuming one pull drains the operator. That class of bug —
//! correct at 3 rows, wrong at 8193 — is what this file is built to catch.
//!
//! # Method
//!
//! Every test controls exactly where batch boundaries fall by feeding an
//! operator pre-chunked `RecordBatch`es (via [`Feed`]/[`VecBatchSource`])
//! rather than relying on any real batching source. Sizes are chosen to
//! straddle 8192 (one batch), 16384 (two batches) and a comfortably larger
//! size — see [`SIZES`]. Wherever practical, the expected answer is computed
//! independently in plain Rust (a HashMap, a sort, a BFS) rather than by
//! comparing one operator's output against another's, so a bug shared by both
//! implementations cannot make a test pass anyway.
//!
//! Every batch this file ever pulls is checked, via [`drain_checked`], to
//! carry exactly the operator's declared `schema()`, and every drained
//! operator is confirmed to keep returning `None` (not resurrect a batch)
//! once exhausted — the two schema/EOF-stability requirements of
//! [`basin_exec::operator::Operator`]'s contract. The third contract
//! requirement — a batch consumed entirely by `OFFSET` must not read as
//! end-of-stream — is exercised directly in the `Limit`/`Offset` section
//! below, at batch-boundary scale.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use basin_exec::aggregate::{AggFunc, AggregateSpec, HashAggregate};
use basin_exec::join::HashJoin;
use basin_exec::limit::Limit;
use basin_exec::operator::{ExecError, Operator};
use basin_exec::project::Filter;
use basin_exec::recursive::{RecursiveCte, RecursiveTermFactory};
use basin_exec::scan::{Scan, VecBatchSource};
use basin_exec::setop::SetOp;
use basin_exec::sort::{Sort, SortKey, TopK};
use basin_exec::window::{
    FrameBound, FrameOffset, FrameUnits, OrderKey, WindowAgg, WindowFrame, WindowFunc, WindowSpec,
};

use basin_pgtype::{Oid, PgType};
use basin_plan::{ColumnRef, Datum, Expr, JoinKind, OpId, SetOpKind};

/// Sizes chosen to straddle basin-exec's 8192-row batch size: zero, one, the
/// row just below/at/above one batch, the same around two batches, and one
/// size comfortably larger still.
const SIZES: [usize; 8] = [0, 1, 8191, 8192, 8193, 16384, 16385, 100_000];

/// The batch size basin-exec's operators emit (`aggregate.rs`'s
/// `OUTPUT_BATCH_SIZE`, `window.rs`'s `OUTPUT_BATCH_SIZE`, and the size real
/// storage batches arrive in) — used throughout this file both to chunk test
/// input and to size boundary-straddling offsets/limits/partitions.
const BATCH: usize = 8192;

// ============================================================================
// Generic operator test doubles
// ============================================================================

/// Replays a fixed list of batches, one per `next_batch` call — the same test
/// double every operator file in this crate uses internally (`sort.rs`'s
/// `Feed`, `join.rs`'s `Feed`, …). Reimplemented here because this file is
/// external to the crate and cannot reach into a private `#[cfg(test)]`
/// module.
struct Feed {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl Feed {
    fn boxed(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
        Box::new(Feed {
            schema,
            batches: batches.into(),
        })
    }
}

impl Operator for Feed {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        Ok(self.batches.pop_front())
    }
}

/// Same as [`Feed`], but counts how many times `next_batch` was called — lets
/// a test observe early-exit (e.g. `LIMIT` must stop pulling once satisfied)
/// directly instead of assuming it.
struct CountingFeed {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    pulls: Arc<AtomicUsize>,
}

impl Operator for CountingFeed {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        self.pulls.fetch_add(1, Ordering::Relaxed);
        Ok(self.batches.pop_front())
    }
}

// ============================================================================
// Schema / batch construction helpers
// ============================================================================

fn schema_i32(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int32, true))
            .collect::<Vec<_>>(),
    ))
}

fn schema_i64(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ))
}

/// Chunk `cols` (one `Vec` per output column, all the same length) into
/// `RecordBatch`es of at most `chunk` rows each, in row order — controls
/// exactly where batch boundaries fall, the way a real multi-batch source
/// would.
fn chunk_i32(schema: &SchemaRef, cols: &[Vec<Option<i32>>], chunk: usize) -> Vec<RecordBatch> {
    let n = cols.first().map(|c| c.len()).unwrap_or(0);
    if n == 0 {
        return Vec::new();
    }
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + chunk).min(n);
        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|c| Arc::new(Int32Array::from(c[start..end].to_vec())) as ArrayRef)
            .collect();
        out.push(RecordBatch::try_new(schema.clone(), arrays).unwrap());
        start = end;
    }
    out
}

fn chunk_i64(schema: &SchemaRef, col: &[Option<i64>], chunk: usize) -> Vec<RecordBatch> {
    let n = col.len();
    if n == 0 {
        return Vec::new();
    }
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + chunk).min(n);
        out.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(col[start..end].to_vec())) as ArrayRef],
            )
            .unwrap(),
        );
        start = end;
    }
    out
}

fn batch_i64_single(schema: &SchemaRef, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )
    .unwrap()
}

fn col_i32(batch: &RecordBatch, i: usize) -> Vec<Option<i32>> {
    batch
        .column(i)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}

fn col_i64(batch: &RecordBatch, i: usize) -> Vec<Option<i64>> {
    batch
        .column(i)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

fn col_f64(batch: &RecordBatch, i: usize) -> Vec<Option<f64>> {
    batch
        .column(i)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}

fn concat_col_i32(batches: &[RecordBatch], i: usize) -> Vec<Option<i32>> {
    batches.iter().flat_map(|b| col_i32(b, i)).collect()
}

// ============================================================================
// Expression helpers (Scan/Filter predicates) — Oids confirmed against
// `basin-pgtype/src/operator.rs`'s live-Postgres-checked table.
// ============================================================================

fn col_expr(index: u16, name: &str) -> Expr {
    Expr::Column(ColumnRef {
        relation: 0,
        index,
        name: name.to_string(),
    })
}

fn lit_i32(v: i32) -> Expr {
    Expr::Literal(Datum::Int32(v), PgType::INT4)
}

fn bin(oid: u32, lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary {
        op: OpId(Oid(oid)),
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

const INT4_EQ: u32 = 96;
const INT4_GT: u32 = 521;
const INT4_GE: u32 = 525;
const INT4_MOD: u32 = 530;

// ============================================================================
// The operator contract, enforced on every batch this file ever pulls:
//  - every yielded batch's schema equals the operator's declared schema()
//  - next_batch keeps returning None (never resurrects a batch) once
//    exhausted, which also exercises "keeps returning batches until None".
// ============================================================================

fn drain_checked(op: &mut dyn Operator) -> Vec<RecordBatch> {
    let schema = op.schema();
    let mut out = Vec::new();
    loop {
        match op.next_batch().unwrap() {
            Some(b) => {
                assert_eq!(
                    b.schema(),
                    schema,
                    "every batch's schema must match the operator's declared schema()"
                );
                out.push(b);
            }
            None => break,
        }
    }
    assert!(
        op.next_batch().unwrap().is_none(),
        "next_batch must keep returning None once exhausted, not resurrect a batch"
    );
    out
}

// ============================================================================
// Scan + Filter
// ============================================================================

#[test]
fn scan_filter_matches_none_all_and_one_per_batch_across_batch_boundaries() {
    for &n in &SIZES {
        let schema = schema_i32(&["a"]);
        let values: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
        let batches = chunk_i32(&schema, std::slice::from_ref(&values), BATCH);
        let expected_chunks = if n == 0 { 0 } else { n.div_ceil(BATCH) };
        assert_eq!(batches.len(), expected_chunks, "n={n}: sanity on chunk count");

        // Matches everything: `a >= 0`.
        {
            let source = VecBatchSource::new(schema.clone(), batches.clone());
            let predicate = bin(INT4_GE, col_expr(0, "a"), lit_i32(0));
            let mut scan = Scan::new(Box::new(source), vec![0], vec![predicate]).unwrap();
            let out = drain_checked(&mut scan);
            assert_eq!(
                concat_col_i32(&out, 0),
                values,
                "n={n}: a predicate matching everything must keep every row"
            );
        }

        // Matches nothing: `a > n` is never true, including for n == 0. Every
        // rejected batch must still come back as Some(empty), never be
        // silently skipped.
        {
            let source = VecBatchSource::new(schema.clone(), batches.clone());
            let predicate = bin(INT4_GT, col_expr(0, "a"), lit_i32(n as i32));
            let mut scan = Scan::new(Box::new(source), vec![0], vec![predicate]).unwrap();
            let out = drain_checked(&mut scan);
            assert_eq!(
                out.len(),
                batches.len(),
                "n={n}: a rejected batch must still be Some(empty), not skipped"
            );
            assert_eq!(
                out.iter().map(|b| b.num_rows()).sum::<usize>(),
                0,
                "n={n}: predicate matching nothing must keep zero rows"
            );
        }

        // Matches exactly one row per input batch: `a % BATCH == 0` is true
        // only for the first row of every BATCH-sized chunk, since values are
        // 0..n laid out in order.
        {
            let source = VecBatchSource::new(schema.clone(), batches.clone());
            let modded = bin(INT4_MOD, col_expr(0, "a"), lit_i32(BATCH as i32));
            let predicate = bin(INT4_EQ, modded, lit_i32(0));
            let mut scan = Scan::new(Box::new(source), vec![0], vec![predicate]).unwrap();
            let out = drain_checked(&mut scan);
            let expected: Vec<Option<i32>> = (0..expected_chunks)
                .map(|i| Some((i * BATCH) as i32))
                .collect();
            assert_eq!(
                concat_col_i32(&out, 0),
                expected,
                "n={n}: exactly one row per batch must survive"
            );
        }
    }
}

#[test]
fn filter_operator_matches_none_all_and_one_per_batch_across_batch_boundaries() {
    for &n in &SIZES {
        let schema = schema_i32(&["a"]);
        let values: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
        let batches = chunk_i32(&schema, std::slice::from_ref(&values), BATCH);

        {
            let child = Feed::boxed(schema.clone(), batches.clone());
            let predicate = bin(INT4_GE, col_expr(0, "a"), lit_i32(0));
            let mut filter = Filter::new(child, predicate);
            let out = drain_checked(&mut filter);
            assert_eq!(concat_col_i32(&out, 0), values, "n={n}");
        }
        {
            let child = Feed::boxed(schema.clone(), batches.clone());
            let predicate = bin(INT4_GT, col_expr(0, "a"), lit_i32(n as i32));
            let mut filter = Filter::new(child, predicate);
            let out = drain_checked(&mut filter);
            assert_eq!(
                out.len(),
                batches.len(),
                "n={n}: a rejected batch must still be Some(empty)"
            );
            assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        }
        {
            let child = Feed::boxed(schema.clone(), batches.clone());
            let modded = bin(INT4_MOD, col_expr(0, "a"), lit_i32(BATCH as i32));
            let predicate = bin(INT4_EQ, modded, lit_i32(0));
            let mut filter = Filter::new(child, predicate);
            let out = drain_checked(&mut filter);
            let expected_chunks = if n == 0 { 0 } else { n.div_ceil(BATCH) };
            let expected: Vec<Option<i32>> = (0..expected_chunks)
                .map(|i| Some((i * BATCH) as i32))
                .collect();
            assert_eq!(concat_col_i32(&out, 0), expected, "n={n}");
        }
    }
}

// ============================================================================
// Sort + TopK
// ============================================================================

#[test]
fn sort_matches_independent_rust_sort_across_batch_boundaries() {
    for &n in &SIZES {
        let schema = schema_i32(&["v", "payload"]);
        // Heavy duplication on the sort key so the arrival-order tie-break
        // has to do real work, the same way sort.rs's own
        // `sort_and_topk_agree_on_the_same_input` does, but here spanning
        // batch-boundary-scale input.
        let v: Vec<Option<i32>> = (0..n).map(|i| Some((i % 1000) as i32)).collect();
        let payload: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
        let batches = chunk_i32(&schema, &[v, payload], BATCH);

        let child = Feed::boxed(schema.clone(), batches);
        let keys = vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }];
        let mut sort = Sort::new(child, keys, 1 << 32);
        let out = drain_checked(&mut sort);
        let actual: Vec<(i32, i32)> = concat_col_i32(&out, 0)
            .into_iter()
            .zip(concat_col_i32(&out, 1))
            .map(|(a, b)| (a.unwrap(), b.unwrap()))
            .collect();

        let mut expected: Vec<(i32, i32)> = (0..n).map(|i| ((i % 1000) as i32, i as i32)).collect();
        expected.sort();
        assert_eq!(
            actual, expected,
            "n={n}: Sort must match an independent Rust sort, regardless of batching"
        );
    }
}

#[test]
fn topk_matches_full_sort_truncation_across_k_values_spanning_batch_boundaries() {
    let n = 20_000usize;
    let schema = schema_i32(&["v", "payload"]);
    let v: Vec<Option<i32>> = (0..n).map(|i| Some((i % 1000) as i32)).collect();
    let payload: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
    let batches = chunk_i32(&schema, &[v, payload], BATCH);
    assert_eq!(
        batches.len(),
        3,
        "sanity: n=20000 at BATCH=8192 spans 3 batches (8192,8192,3616)"
    );

    let mut full_expected: Vec<(i32, i32)> = (0..n).map(|i| ((i % 1000) as i32, i as i32)).collect();
    full_expected.sort();

    for &k in &[0usize, 1, 8191, 8192, 8193, 16384, 16385, n, n + 5_000] {
        let child = Feed::boxed(schema.clone(), batches.clone());
        let keys = vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }];
        let mut topk = TopK::new(child, keys, k);
        let out = drain_checked(&mut topk);
        let actual: Vec<(i32, i32)> = concat_col_i32(&out, 0)
            .into_iter()
            .zip(concat_col_i32(&out, 1))
            .map(|(a, b)| (a.unwrap(), b.unwrap()))
            .collect();
        let expected: Vec<(i32, i32)> = full_expected.iter().cloned().take(k).collect();
        assert_eq!(actual, expected, "k={k}: TopK must match Sort-then-truncate(k)");
    }
}

// ============================================================================
// Limit + Offset
// ============================================================================

#[test]
fn limit_offset_matches_independent_slicing_across_batch_boundaries() {
    for &n in &SIZES {
        let schema = schema_i32(&["n"]);
        let raw_values: Vec<i32> = (0..n as i32).collect();
        let values: Vec<Option<i32>> = raw_values.iter().map(|&v| Some(v)).collect();
        let batches = chunk_i32(&schema, std::slice::from_ref(&values), BATCH);

        let combos: Vec<(usize, Option<usize>)> = vec![
            (0, Some(0)),                 // LIMIT 0
            (0, Some(1)),
            (0, None),                    // OFFSET-only
            (BATCH, Some(1)),             // offset lands exactly on a batch boundary
            (BATCH, Some(BATCH)),
            (BATCH - 1, Some(3)),         // offset lands one row before a boundary
            (2 * BATCH + 1, Some(5)),     // offset skips whole batches plus a partial one
            (n, Some(10)),                // offset exactly at the end
            (0, Some(n + 1_000)),         // fetch larger than the whole input
        ];

        for (skip, fetch) in combos {
            let child = Feed::boxed(schema.clone(), batches.clone());
            let mut limit = Limit::new(child, skip, fetch);
            let out = drain_checked(&mut limit);
            let actual: Vec<i32> = concat_col_i32(&out, 0)
                .into_iter()
                .map(|v| v.unwrap())
                .collect();

            let expected: Vec<i32> = if skip >= raw_values.len() {
                Vec::new()
            } else {
                let avail = &raw_values[skip..];
                match fetch {
                    Some(f) => avail.iter().take(f).cloned().collect(),
                    None => avail.to_vec(),
                }
            };
            assert_eq!(actual, expected, "n={n} skip={skip} fetch={fetch:?}");
        }
    }
}

/// A batch consumed ENTIRELY by `OFFSET` must not read as end-of-stream —
/// the operator contract's third requirement, exercised at 8192-row-batch
/// scale: the offset spans two whole batches before the fetch begins.
#[test]
fn offset_spanning_whole_batches_does_not_read_as_end_of_stream() {
    let n = 3 * BATCH;
    let schema = schema_i32(&["n"]);
    let values: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
    let batches = chunk_i32(&schema, std::slice::from_ref(&values), BATCH);
    assert_eq!(batches.len(), 3);

    let child = Feed::boxed(schema, batches);
    let mut limit = Limit::new(child, 2 * BATCH, Some(4));
    let out = drain_checked(&mut limit);
    let actual: Vec<i32> = concat_col_i32(&out, 0)
        .into_iter()
        .map(|v| v.unwrap())
        .collect();
    let expected: Vec<i32> = ((2 * BATCH) as i32..(2 * BATCH + 4) as i32).collect();
    assert_eq!(actual, expected);
}

#[test]
fn limit_stops_pulling_once_satisfied_at_scale() {
    let n = 100_000usize;
    let schema = schema_i32(&["n"]);
    let values: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
    let batches = chunk_i32(&schema, std::slice::from_ref(&values), BATCH);
    let pulls = Arc::new(AtomicUsize::new(0));
    let child: Box<dyn Operator> = Box::new(CountingFeed {
        schema: schema.clone(),
        batches: batches.into(),
        pulls: Arc::clone(&pulls),
    });
    let mut limit = Limit::new(child, 0, Some(5));
    let out = drain_checked(&mut limit);
    let actual: Vec<i32> = concat_col_i32(&out, 0)
        .into_iter()
        .map(|v| v.unwrap())
        .collect();
    assert_eq!(actual, vec![0, 1, 2, 3, 4]);
    assert_eq!(
        pulls.load(Ordering::Relaxed),
        1,
        "a LIMIT 5 satisfied by the first 8192-row batch must not pull a second, \
         even with 100,000 rows behind it"
    );
}

// ============================================================================
// HashAggregate
// ============================================================================

fn aggregate_spec(func: AggFunc, input_col: Option<usize>, alias: &str) -> AggregateSpec {
    AggregateSpec {
        func,
        input_col,
        distinct: false,
        filter_col: None,
        alias: alias.to_string(),
    }
}

#[test]
fn hash_aggregate_low_cardinality_spans_many_batches() {
    const GROUPS: i32 = 5;
    for &n in &SIZES {
        let schema = schema_i32(&["g", "val"]);
        let g: Vec<Option<i32>> = (0..n).map(|i| Some((i as i32) % GROUPS)).collect();
        // Every 11th value is NULL, to exercise sum/avg/count(val) ignoring
        // NULLs at scale.
        let val: Vec<Option<i32>> = (0..n)
            .map(|i| {
                if i % 11 == 10 {
                    None
                } else {
                    Some((i as i32) * 3 - 7)
                }
            })
            .collect();
        let batches = chunk_i32(&schema, &[g, val.clone()], BATCH);
        let child = Feed::boxed(schema.clone(), batches);
        let specs = vec![
            aggregate_spec(AggFunc::CountStar, None, "cnt_star"),
            aggregate_spec(AggFunc::Count, Some(1), "cnt_val"),
            aggregate_spec(AggFunc::Sum, Some(1), "sum_val"),
            aggregate_spec(AggFunc::Min, Some(1), "min_val"),
            aggregate_spec(AggFunc::Max, Some(1), "max_val"),
            aggregate_spec(AggFunc::Avg, Some(1), "avg_val"),
        ];
        let mut agg = HashAggregate::new(child, vec![0], specs, 1 << 32).unwrap();
        let out = drain_checked(&mut agg);

        if n == 0 {
            assert!(out.is_empty(), "GROUP BY over zero rows must yield zero groups");
            continue;
        }

        // Independent expected, computed in plain Rust over the same (g, val)
        // pairs — not by comparing against another operator.
        use std::collections::HashMap;
        #[derive(Default)]
        struct Acc {
            count_star: i64,
            count_val: i64,
            sum: i64,
            min: Option<i32>,
            max: Option<i32>,
            avg_sum: f64,
            avg_cnt: i64,
        }
        let mut accs: HashMap<i32, Acc> = HashMap::new();
        for i in 0..n {
            let gk = (i as i32) % GROUPS;
            let a = accs.entry(gk).or_default();
            a.count_star += 1;
            if let Some(v) = val[i] {
                a.count_val += 1;
                a.sum += v as i64;
                a.min = Some(a.min.map_or(v, |m| m.min(v)));
                a.max = Some(a.max.map_or(v, |m| m.max(v)));
                a.avg_sum += v as f64;
                a.avg_cnt += 1;
            }
        }

        type Row = (i64, i64, Option<i64>, Option<i32>, Option<i32>, Option<f64>);
        let mut actual: HashMap<i32, Row> = HashMap::new();
        for b in &out {
            let gcol = col_i32(b, 0);
            let cs = col_i64(b, 1);
            let cv = col_i64(b, 2);
            let sv = col_i64(b, 3);
            let mn = col_i32(b, 4);
            let mx = col_i32(b, 5);
            let av = col_f64(b, 6);
            for row in 0..b.num_rows() {
                actual.insert(
                    gcol[row].unwrap(),
                    (cs[row].unwrap(), cv[row].unwrap(), sv[row], mn[row], mx[row], av[row]),
                );
            }
        }

        // `accs.len()`, not `GROUPS`. `SIZES` includes n=1, and one row can
        // only ever produce one group — `min(n, GROUPS)`, not `GROUPS`. The
        // independent Rust model above already computes the correct expected
        // group set, so compare against that; together with the `actual[gk]`
        // lookups below this still asserts exact set equality.
        assert_eq!(
            actual.len(),
            accs.len(),
            "n={n}: must have exactly {} groups",
            accs.len()
        );
        for (gk, a) in &accs {
            let (cs, cv, sv, mn, mx, av) = actual[gk];
            assert_eq!(cs, a.count_star, "n={n} g={gk}: count(*)");
            assert_eq!(cv, a.count_val, "n={n} g={gk}: count(val)");
            assert_eq!(
                sv,
                if a.count_val > 0 { Some(a.sum) } else { None },
                "n={n} g={gk}: sum(val)"
            );
            assert_eq!(mn, a.min, "n={n} g={gk}: min(val)");
            assert_eq!(mx, a.max, "n={n} g={gk}: max(val)");
            let expected_avg = if a.avg_cnt > 0 {
                Some(a.avg_sum / a.avg_cnt as f64)
            } else {
                None
            };
            match (av, expected_avg) {
                (Some(x), Some(y)) => {
                    assert!((x - y).abs() < 1e-6, "n={n} g={gk}: avg(val) {x} vs {y}")
                }
                (None, None) => {}
                (x, y) => panic!("n={n} g={gk}: avg(val) mismatch {x:?} vs {y:?}"),
            }
        }
    }
}

/// More groups than one output batch (`OUTPUT_BATCH_SIZE == 8192` —
/// `aggregate.rs`), so the aggregate's OWN OUTPUT — not just its input — must
/// span multiple `next_batch` calls.
#[test]
fn hash_aggregate_high_cardinality_output_spans_batches() {
    let n = 20_000usize;
    let modulus: i32 = 9_000;
    let schema = schema_i32(&["g", "val"]);
    let g: Vec<Option<i32>> = (0..n).map(|i| Some((i as i32) % modulus)).collect();
    let val: Vec<Option<i32>> = (0..n).map(|i| Some(i as i32)).collect();
    let batches = chunk_i32(&schema, &[g, val], BATCH);
    let child = Feed::boxed(schema.clone(), batches);
    let specs = vec![
        aggregate_spec(AggFunc::CountStar, None, "cnt"),
        aggregate_spec(AggFunc::Sum, Some(1), "sum_val"),
    ];
    let mut agg = HashAggregate::new(child, vec![0], specs, 1 << 32).unwrap();
    let out = drain_checked(&mut agg);
    assert!(
        out.len() > 1,
        "with {modulus} groups (> 8192), the aggregate's OWN OUTPUT must span multiple \
         next_batch calls, got {} batch(es)",
        out.len()
    );

    use std::collections::HashMap;
    let mut expected: HashMap<i32, (i64, i64)> = HashMap::new();
    for i in 0..n {
        let gk = (i as i32) % modulus;
        let e = expected.entry(gk).or_insert((0, 0));
        e.0 += 1;
        e.1 += i as i64;
    }
    let mut actual_groups = 0usize;
    for b in &out {
        let gcol = col_i32(b, 0);
        let cs = col_i64(b, 1);
        let sv = col_i64(b, 2);
        for row in 0..b.num_rows() {
            actual_groups += 1;
            let gk = gcol[row].unwrap();
            let e = expected[&gk];
            assert_eq!(cs[row].unwrap(), e.0, "g={gk}: count(*)");
            assert_eq!(sv[row].unwrap(), e.1, "g={gk}: sum(val)");
        }
    }
    assert_eq!(
        actual_groups, modulus as usize,
        "must have exactly {modulus} groups total, spread across batches"
    );
}

// ============================================================================
// HashJoin — every kind, NULL keys, build side smaller/larger than a batch
// ============================================================================

fn keyed_batches(
    n: usize,
    key_of: impl Fn(usize) -> Option<i32>,
    val_offset: i32,
    chunk: usize,
    names: (&str, &str),
) -> (SchemaRef, Vec<RecordBatch>, Vec<Option<i32>>, Vec<i32>) {
    let schema = schema_i32(&[names.0, names.1]);
    let keys: Vec<Option<i32>> = (0..n).map(key_of).collect();
    let vals: Vec<i32> = (0..n).map(|i| val_offset + i as i32).collect();
    let vals_opt: Vec<Option<i32>> = vals.iter().map(|v| Some(*v)).collect();
    let batches = chunk_i32(&schema, &[keys.clone(), vals_opt], chunk);
    (schema, batches, keys, vals)
}

/// Independent (not operator-derived) equijoin semantics for
/// `Inner`/`Left`/`Right`/`Full`: a NULL key never matches anything,
/// including another NULL key (`join.rs`'s module docs).
fn expected_equi_join(
    kind: JoinKind,
    left_keys: &[Option<i32>],
    left_vals: &[i32],
    right_keys: &[Option<i32>],
    right_vals: &[i32],
) -> Vec<(Option<i32>, Option<i32>)> {
    use std::collections::HashMap;
    let mut right_by_key: HashMap<i32, Vec<usize>> = HashMap::new();
    for (i, k) in right_keys.iter().enumerate() {
        if let Some(k) = k {
            right_by_key.entry(*k).or_default().push(i);
        }
    }
    let mut right_matched = vec![false; right_keys.len()];
    let mut out = Vec::new();
    for (i, lk) in left_keys.iter().enumerate() {
        let lv = left_vals[i];
        let matches: Vec<usize> = lk
            .and_then(|k| right_by_key.get(&k))
            .cloned()
            .unwrap_or_default();
        if matches.is_empty() {
            if matches!(kind, JoinKind::Left | JoinKind::Full) {
                out.push((Some(lv), None));
            }
        } else {
            for &ri in &matches {
                right_matched[ri] = true;
                out.push((Some(lv), Some(right_vals[ri])));
            }
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (i, m) in right_matched.iter().enumerate() {
            if !m {
                out.push((None, Some(right_vals[i])));
            }
        }
    }
    out
}

fn expected_semi_anti(
    anti: bool,
    left_keys: &[Option<i32>],
    left_vals: &[i32],
    right_keys: &[Option<i32>],
) -> Vec<i32> {
    use std::collections::HashSet;
    let right_set: HashSet<i32> = right_keys.iter().filter_map(|k| *k).collect();
    left_keys
        .iter()
        .zip(left_vals)
        .filter_map(|(k, v)| {
            let has_match = k.map_or(false, |k| right_set.contains(&k));
            if has_match != anti {
                Some(*v)
            } else {
                None
            }
        })
        .collect()
}

#[test]
fn hash_join_every_kind_across_batch_boundaries_with_null_keys() {
    struct Case {
        left_n: usize,
        right_n: usize,
        left_chunk: usize,
        right_chunk: usize,
    }
    let cases = [
        // Build (right) side smaller than one batch; probe (left) side spans
        // many batches.
        Case {
            left_n: 20_000,
            right_n: 5,
            left_chunk: BATCH,
            right_chunk: BATCH,
        },
        // Build (right) side larger than one batch (spans multiple build
        // batches); probe side spans multiple batches too.
        Case {
            left_n: 8_193,
            right_n: 20_000,
            left_chunk: BATCH,
            right_chunk: 4096,
        },
        Case {
            left_n: 0,
            right_n: 10,
            left_chunk: BATCH,
            right_chunk: BATCH,
        },
        Case {
            left_n: 10,
            right_n: 0,
            left_chunk: BATCH,
            right_chunk: BATCH,
        },
        Case {
            left_n: 0,
            right_n: 0,
            left_chunk: BATCH,
            right_chunk: BATCH,
        },
    ];
    const MODULUS: i32 = 37;

    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Full,
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
    ] {
        for case in &cases {
            let left_key_of =
                |i: usize| if i % 13 == 12 { None } else { Some((i as i32) % MODULUS) };
            let right_key_of =
                |i: usize| if i % 17 == 16 { None } else { Some((i as i32) % MODULUS) };
            let (lschema, lbatches, lkeys, lvals) =
                keyed_batches(case.left_n, left_key_of, 0, case.left_chunk, ("lk", "lv"));
            let (rschema, rbatches, rkeys, rvals) = keyed_batches(
                case.right_n,
                right_key_of,
                10_000_000,
                case.right_chunk,
                ("rk", "rv"),
            );

            let left = Feed::boxed(lschema, lbatches);
            let right = Feed::boxed(rschema, rbatches);
            let mut join = HashJoin::new(left, right, kind, vec![0], vec![0], 1 << 32).unwrap();
            let out = drain_checked(&mut join);

            let tag = format!("kind={kind:?} left_n={} right_n={}", case.left_n, case.right_n);
            match kind {
                JoinKind::LeftSemi | JoinKind::LeftAnti => {
                    let mut actual: Vec<i32> =
                        out.iter().flat_map(|b| col_i32(b, 1)).flatten().collect();
                    let mut expected =
                        expected_semi_anti(kind == JoinKind::LeftAnti, &lkeys, &lvals, &rkeys);
                    actual.sort_unstable();
                    expected.sort_unstable();
                    assert_eq!(actual, expected, "{tag}");
                }
                _ => {
                    let mut actual: Vec<(Option<i32>, Option<i32>)> = out
                        .iter()
                        .flat_map(|b| col_i32(b, 1).into_iter().zip(col_i32(b, 3)))
                        .collect();
                    let mut expected = expected_equi_join(kind, &lkeys, &lvals, &rkeys, &rvals);
                    actual.sort();
                    expected.sort();
                    assert_eq!(actual, expected, "{tag}");
                }
            }
        }
    }
}

#[test]
fn cross_join_row_count_across_batch_boundaries_on_either_side() {
    let cases = [
        (20_000usize, 3usize, BATCH, BATCH),
        (5, 8_193, BATCH, 4096),
    ];
    for (left_n, right_n, left_chunk, right_chunk) in cases {
        let lschema = schema_i32(&["lv"]);
        let rschema = schema_i32(&["rv"]);
        let lvals: Vec<Option<i32>> = (0..left_n as i32).map(Some).collect();
        let rvals: Vec<Option<i32>> = (0..right_n as i32).map(Some).collect();
        let lbatches = chunk_i32(&lschema, std::slice::from_ref(&lvals), left_chunk);
        let rbatches = chunk_i32(&rschema, std::slice::from_ref(&rvals), right_chunk);
        let left = Feed::boxed(lschema, lbatches);
        let right = Feed::boxed(rschema, rbatches);
        let mut join = HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 32).unwrap();
        let out = drain_checked(&mut join);
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, left_n * right_n, "left_n={left_n} right_n={right_n}");

        let left_sum: i64 = out
            .iter()
            .flat_map(|b| col_i32(b, 0))
            .flatten()
            .map(i64::from)
            .sum();
        let expected_left_sum: i64 = (0..left_n as i64).sum::<i64>() * right_n as i64;
        assert_eq!(
            left_sum, expected_left_sum,
            "each left row must appear exactly right_n times"
        );
    }
}

// ============================================================================
// SetOp — UNION dedup across batch boundaries
// ============================================================================

#[test]
fn union_dedups_across_batch_boundaries_on_both_sides() {
    let n1 = 20_000usize;
    let n2 = 15_000usize;
    let schema = schema_i32(&["x"]);
    let left: Vec<Option<i32>> = (0..n1).map(|i| Some((i as i32) % 100)).collect();
    let right: Vec<Option<i32>> = (0..n2).map(|i| Some((i as i32) % 150)).collect();
    let lbatches = chunk_i32(&schema, std::slice::from_ref(&left), BATCH);
    let rbatches = chunk_i32(&schema, std::slice::from_ref(&right), BATCH);
    let l = Feed::boxed(schema.clone(), lbatches);
    let r = Feed::boxed(schema.clone(), rbatches);
    let mut op = SetOp::new(l, r, SetOpKind::Union, false, 1 << 32).unwrap();
    let out = drain_checked(&mut op);
    let mut actual: Vec<i32> = out.iter().flat_map(|b| col_i32(b, 0)).flatten().collect();
    actual.sort_unstable();

    use std::collections::HashSet;
    let expected_set: HashSet<i32> = left.iter().chain(right.iter()).filter_map(|v| *v).collect();
    let mut expected: Vec<i32> = expected_set.into_iter().collect();
    expected.sort_unstable();

    assert_eq!(
        actual, expected,
        "UNION must dedup to exactly the distinct value set spanning batches on both sides"
    );
}

#[test]
fn union_all_preserves_the_total_row_count_across_batch_boundaries() {
    let n1 = 20_000usize;
    let n2 = 15_000usize;
    let schema = schema_i32(&["x"]);
    let left: Vec<Option<i32>> = (0..n1).map(|i| Some((i as i32) % 100)).collect();
    let right: Vec<Option<i32>> = (0..n2).map(|i| Some((i as i32) % 150)).collect();
    let lbatches = chunk_i32(&schema, std::slice::from_ref(&left), BATCH);
    let rbatches = chunk_i32(&schema, std::slice::from_ref(&right), BATCH);
    let l = Feed::boxed(schema.clone(), lbatches);
    let r = Feed::boxed(schema.clone(), rbatches);
    let mut op = SetOp::new(l, r, SetOpKind::Union, true, 1 << 32).unwrap();
    let out = drain_checked(&mut op);
    let total: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, n1 + n2);
}

#[test]
fn except_all_and_intersect_all_match_multiset_arithmetic_across_batch_boundaries() {
    let n1 = 20_000usize;
    let n2 = 13_000usize;
    let modulus = 97i32;
    let schema = schema_i32(&["x"]);
    let left: Vec<Option<i32>> = (0..n1).map(|i| Some((i as i32) % modulus)).collect();
    let right: Vec<Option<i32>> = (0..n2)
        .map(|i| Some(((i as i32) * 2) % modulus))
        .collect();

    use std::collections::HashMap;
    let mut lcount: HashMap<i32, i64> = HashMap::new();
    for v in left.iter().flatten() {
        *lcount.entry(*v).or_default() += 1;
    }
    let mut rcount: HashMap<i32, i64> = HashMap::new();
    for v in right.iter().flatten() {
        *rcount.entry(*v).or_default() += 1;
    }

    let cases: Vec<(SetOpKind, fn(i64, i64) -> i64)> = vec![
        (SetOpKind::Except, |l, r| (l - r).max(0)),
        (SetOpKind::Intersect, |l, r| l.min(r)),
    ];
    for (op_kind, expect_fn) in cases {
        let lbatches = chunk_i32(&schema, std::slice::from_ref(&left), BATCH);
        let rbatches = chunk_i32(&schema, std::slice::from_ref(&right), BATCH);
        let l = Feed::boxed(schema.clone(), lbatches);
        let r = Feed::boxed(schema.clone(), rbatches);
        let mut op = SetOp::new(l, r, op_kind, true, 1 << 32).unwrap();
        let out = drain_checked(&mut op);
        let mut actual: HashMap<i32, i64> = HashMap::new();
        for v in out.iter().flat_map(|b| col_i32(b, 0)).flatten() {
            *actual.entry(v).or_default() += 1;
        }
        for k in 0..modulus {
            let l = lcount.get(&k).copied().unwrap_or(0);
            let r = rcount.get(&k).copied().unwrap_or(0);
            let expected = expect_fn(l, r);
            let got = actual.get(&k).copied().unwrap_or(0);
            assert_eq!(got, expected, "{op_kind:?} ALL key={k}: l={l} r={r}");
        }
    }
}

// ============================================================================
// Window functions — a PARTITION spanning a batch boundary
// ============================================================================

fn window_spec(func: WindowFunc, arg_col: Option<usize>, frame: Option<WindowFrame>, alias: &str) -> WindowSpec {
    WindowSpec {
        func,
        arg_col,
        offset_col: None,
        default_col: None,
        nth_col: None,
        frame,
        alias: alias.to_string(),
    }
}

/// Independent (not operator-derived) row_number/rank/dense_rank over one
/// partition's already-sorted `y` values.
fn expected_rank_family(y: &[i32]) -> (Vec<i64>, Vec<i64>, Vec<i64>) {
    let n = y.len();
    let mut row_number = vec![0i64; n];
    let mut rank = vec![0i64; n];
    let mut dense = vec![0i64; n];
    let mut cur_rank = 1i64;
    let mut cur_dense = 0i64;
    let mut i = 0usize;
    while i < n {
        let mut j = i;
        while j < n && y[j] == y[i] {
            j += 1;
        }
        cur_dense += 1;
        for k in i..j {
            row_number[k] = (k + 1) as i64;
            rank[k] = cur_rank;
            dense[k] = cur_dense;
        }
        cur_rank += (j - i) as i64;
        i = j;
    }
    (row_number, rank, dense)
}

#[test]
fn window_ranking_functions_agree_across_a_partition_spanning_a_batch_boundary() {
    // Partition 1 (global rows 8000..16000) straddles the BATCH=8192
    // input-chunk boundary; the whole input (20000 rows) also straddles
    // WindowAgg's own 8192-row OUTPUT chunking.
    let part_sizes = [8000usize, 8000, 4000];
    let mut g: Vec<Option<i32>> = Vec::new();
    let mut y: Vec<Option<i32>> = Vec::new();
    let mut expected_y_by_partition: Vec<Vec<i32>> = Vec::new();
    for (gi, &sz) in part_sizes.iter().enumerate() {
        let mut part_y = Vec::with_capacity(sz);
        for local in 0..sz {
            g.push(Some(gi as i32));
            let yv = (local / 2) as i32; // ties every two rows, so rank/dense_rank diverge
            y.push(Some(yv));
            part_y.push(yv);
        }
        expected_y_by_partition.push(part_y);
    }
    let n = g.len();
    assert_eq!(n, 20_000);

    let schema = schema_i32(&["g", "y"]);
    let batches = chunk_i32(&schema, &[g, y], BATCH);
    assert!(batches.len() > 1, "sanity: input must span multiple batches");

    let child = Feed::boxed(schema.clone(), batches);
    let windows = vec![
        window_spec(WindowFunc::RowNumber, None, None, "rn"),
        window_spec(WindowFunc::Rank, None, None, "rk"),
        window_spec(WindowFunc::DenseRank, None, None, "dr"),
    ];
    let order_by = vec![OrderKey {
        column: 1,
        descending: false,
        nulls_first: false,
    }];
    let mut win = WindowAgg::new(child, vec![0], order_by, windows, 1 << 32).unwrap();
    let out = drain_checked(&mut win);
    assert!(
        out.len() > 1,
        "the operator's OWN output (20000 rows) must span multiple 8192-row batches too"
    );

    let rn: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 2)).flatten().collect();
    let rk: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 3)).flatten().collect();
    let dr: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 4)).flatten().collect();
    assert_eq!(rn.len(), n);

    let mut expected_rn = Vec::with_capacity(n);
    let mut expected_rk = Vec::with_capacity(n);
    let mut expected_dr = Vec::with_capacity(n);
    for part_y in &expected_y_by_partition {
        let (prn, prk, pdr) = expected_rank_family(part_y);
        expected_rn.extend(prn);
        expected_rk.extend(prk);
        expected_dr.extend(pdr);
    }
    assert_eq!(rn, expected_rn, "row_number");
    assert_eq!(rk, expected_rk, "rank");
    assert_eq!(dr, expected_dr, "dense_rank");
}

#[test]
fn window_sliding_sum_frame_does_not_reset_across_a_batch_boundary_within_a_partition() {
    let part_sizes = [8000usize, 8000, 4000];
    let mut g: Vec<Option<i32>> = Vec::new();
    let mut y: Vec<Option<i32>> = Vec::new();
    let mut v: Vec<Option<i32>> = Vec::new();
    let mut expected_v_by_partition: Vec<Vec<i32>> = Vec::new();
    for (gi, &sz) in part_sizes.iter().enumerate() {
        let mut part_v = Vec::with_capacity(sz);
        for local in 0..sz {
            g.push(Some(gi as i32));
            y.push(Some(local as i32));
            let vv = (gi as i32) * 1_000_000 + local as i32;
            v.push(Some(vv));
            part_v.push(vv);
        }
        expected_v_by_partition.push(part_v);
    }
    let n = g.len();
    let schema = schema_i32(&["g", "y", "v"]);
    let batches = chunk_i32(&schema, &[g, y, v], BATCH);
    assert!(batches.len() > 1);

    let child = Feed::boxed(schema.clone(), batches);
    let frame = WindowFrame {
        units: FrameUnits::Rows,
        start: FrameBound::Preceding(FrameOffset::Count(2)),
        end: FrameBound::CurrentRow,
    };
    let windows = vec![window_spec(WindowFunc::Sum, Some(2), Some(frame), "s")];
    let order_by = vec![OrderKey {
        column: 1,
        descending: false,
        nulls_first: false,
    }];
    let mut win = WindowAgg::new(child, vec![0], order_by, windows, 1 << 32).unwrap();
    let out = drain_checked(&mut win);
    assert!(out.len() > 1, "output must span multiple batches");

    let actual: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 3)).flatten().collect();
    assert_eq!(actual.len(), n);

    let mut expected = Vec::with_capacity(n);
    for part_v in &expected_v_by_partition {
        for idx in 0..part_v.len() {
            let start = idx.saturating_sub(2);
            let sum: i64 = part_v[start..=idx].iter().map(|&x| x as i64).sum();
            expected.push(sum);
        }
    }
    assert_eq!(
        actual, expected,
        "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW must not reset at a batch boundary \
         within one partition"
    );
}

#[test]
fn window_default_frame_covers_the_whole_partition_when_it_spans_many_batches() {
    // A single un-ORDER-BYed partition of 10000 rows: still straddles both
    // the input's 8192-row chunking and the operator's own 8192-row output
    // chunking, kept smaller than the other window tests here since the
    // whole-partition frame re-sums the entire partition per row (a known,
    // documented performance gap in window.rs, not something this test needs
    // to stress).
    let n = 10_000usize;
    let schema = schema_i32(&["g", "v"]);
    let g: Vec<Option<i32>> = vec![Some(0); n];
    let v: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
    let batches = chunk_i32(&schema, &[g, v], BATCH);
    assert!(batches.len() > 1);

    let child = Feed::boxed(schema.clone(), batches);
    // No ORDER BY: the default frame is the WHOLE partition (module docs
    // item 1), not a running total.
    let windows = vec![window_spec(WindowFunc::Sum, Some(1), None, "s")];
    let mut win = WindowAgg::new(child, vec![0], vec![], windows, 1 << 32).unwrap();
    let out = drain_checked(&mut win);
    assert!(out.len() > 1);

    let actual: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 2)).flatten().collect();
    let expected_total: i64 = (0..n as i64).sum();
    assert!(
        actual.iter().all(|&s| s == expected_total),
        "every row of a single un-ORDER-BYed partition must see the WHOLE partition's sum, \
         regardless of which input or output batch it landed in"
    );
}

#[test]
fn window_lag_lead_never_cross_a_partition_boundary_spanning_batches() {
    let part_sizes = [8000usize, 8000, 4000];
    let mut g: Vec<Option<i32>> = Vec::new();
    let mut y: Vec<Option<i32>> = Vec::new();
    let mut v: Vec<Option<i32>> = Vec::new();
    let mut expected_v_by_partition: Vec<Vec<i32>> = Vec::new();
    for (gi, &sz) in part_sizes.iter().enumerate() {
        let mut part_v = Vec::with_capacity(sz);
        for local in 0..sz {
            g.push(Some(gi as i32));
            y.push(Some(local as i32));
            let vv = (gi as i32) * 1_000_000 + local as i32;
            v.push(Some(vv));
            part_v.push(vv);
        }
        expected_v_by_partition.push(part_v);
    }
    let n = g.len();
    let schema = schema_i32(&["g", "y", "v"]);
    let batches = chunk_i32(&schema, &[g, y, v], BATCH);
    assert!(batches.len() > 1);

    let child = Feed::boxed(schema.clone(), batches);
    let windows = vec![
        window_spec(WindowFunc::Lag, Some(2), None, "lag1"),
        window_spec(WindowFunc::Lead, Some(2), None, "lead1"),
    ];
    let order_by = vec![OrderKey {
        column: 1,
        descending: false,
        nulls_first: false,
    }];
    let mut win = WindowAgg::new(child, vec![0], order_by, windows, 1 << 32).unwrap();
    let out = drain_checked(&mut win);

    let actual_lag: Vec<Option<i32>> = out.iter().flat_map(|b| col_i32(b, 3)).collect();
    let actual_lead: Vec<Option<i32>> = out.iter().flat_map(|b| col_i32(b, 4)).collect();
    assert_eq!(actual_lag.len(), n);

    let mut expected_lag = Vec::with_capacity(n);
    let mut expected_lead = Vec::with_capacity(n);
    for part_v in &expected_v_by_partition {
        for idx in 0..part_v.len() {
            expected_lag.push(if idx == 0 { None } else { Some(part_v[idx - 1]) });
            expected_lead.push(if idx + 1 < part_v.len() {
                Some(part_v[idx + 1])
            } else {
                None
            });
        }
    }
    assert_eq!(
        actual_lag, expected_lag,
        "lag() must be NULL at each partition's first row, never a neighbouring \
         partition's value"
    );
    assert_eq!(
        actual_lead, expected_lead,
        "lead() must be NULL at each partition's last row, never a neighbouring \
         partition's value"
    );
}

// ============================================================================
// RecursiveCte — iterations that each produce multiple batches
// ============================================================================

#[test]
fn recursive_cte_union_all_iterations_span_multiple_batches() {
    const BRANCH: i64 = 100;
    const MAX_DEPTH: usize = 2; // iter 1: 100 rows; iter 2: 10,000 rows
    let schema = schema_i64(&["id"]);
    let anchor = Feed::boxed(schema.clone(), vec![batch_i64_single(&schema, vec![Some(0)])]);
    let depth = Rc::new(RefCell::new(0usize));
    let d = Rc::clone(&depth);
    let s = schema.clone();
    let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
        let cur_depth = {
            let mut dd = d.borrow_mut();
            *dd += 1;
            *dd
        };
        let mut next: Vec<Option<i64>> = Vec::new();
        if cur_depth <= MAX_DEPTH {
            for batch in &working_table {
                for id in col_i64(batch, 0).into_iter().flatten() {
                    for c in 1..=BRANCH {
                        next.push(Some(id * BRANCH + c));
                    }
                }
            }
        }
        // Each iteration's own working table is itself handed back across
        // MULTIPLE RecordBatches (chunked well below its size), so
        // RecursiveCte's per-phase buffering must correctly accumulate more
        // than one batch before treating the phase as finished.
        Ok(Feed::boxed(s.clone(), chunk_i64(&s, &next, 4096)))
    });
    let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 32, 10);
    let out = drain_checked(&mut op);
    assert!(
        out.len() > 3,
        "iteration 2 alone (10,000 rows chunked at 4096) must span multiple output \
         batches, got {}",
        out.len()
    );

    let mut actual: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 0)).flatten().collect();
    actual.sort_unstable();

    // Independent expected: plain Rust BFS simulation of the same fan-out
    // rule, not a re-run of the operator.
    let mut expected: Vec<i64> = vec![0];
    let mut frontier = vec![0i64];
    for _ in 0..MAX_DEPTH {
        let mut next = Vec::new();
        for &id in &frontier {
            for c in 1..=BRANCH {
                next.push(id * BRANCH + c);
            }
        }
        expected.extend(next.iter().copied());
        frontier = next;
    }
    expected.sort_unstable();

    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 1 + BRANCH as usize + (BRANCH * BRANCH) as usize);
}

#[test]
fn recursive_cte_union_dedups_across_iterations_that_each_span_multiple_batches() {
    const MODULUS: i64 = 5_000;
    let schema = schema_i64(&["id"]);
    let anchor = Feed::boxed(schema.clone(), vec![batch_i64_single(&schema, vec![Some(0)])]);
    let s = schema.clone();
    let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
        let mut next: Vec<Option<i64>> = Vec::new();
        for batch in &working_table {
            for id in col_i64(batch, 0).into_iter().flatten() {
                next.push(Some((2 * id + 1).rem_euclid(MODULUS)));
                next.push(Some((2 * id + 2).rem_euclid(MODULUS)));
            }
        }
        // Force at least some iterations to hand their working table back
        // across multiple RecordBatches.
        Ok(Feed::boxed(s.clone(), chunk_i64(&s, &next, 1_000)))
    });
    let mut op = RecursiveCte::new(anchor, recursive_term, false, 1 << 32, 200);
    let out = drain_checked(&mut op);
    let batch_count = out.len();
    let total: Vec<i64> = out.iter().flat_map(|b| col_i64(b, 0)).flatten().collect();

    // Independent expected: plain Rust reachability simulation implementing
    // the SAME documented rule (dedup against the whole history, module docs
    // item 1 in recursive.rs) — not a re-run of the operator.
    use std::collections::HashSet;
    let mut seen: HashSet<i64> = HashSet::new();
    seen.insert(0);
    let mut frontier = vec![0i64];
    loop {
        let mut next = Vec::new();
        for &id in &frontier {
            for cand in [(2 * id + 1).rem_euclid(MODULUS), (2 * id + 2).rem_euclid(MODULUS)] {
                if seen.insert(cand) {
                    next.push(cand);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }

    let mut actual_sorted = total.clone();
    actual_sorted.sort_unstable();
    let mut expected_sorted: Vec<i64> = seen.into_iter().collect();
    expected_sorted.sort_unstable();

    assert_eq!(
        actual_sorted, expected_sorted,
        "UNION must converge to exactly the reachable id set"
    );
    assert_eq!(
        total.len(),
        actual_sorted.len(),
        "no duplicate ids should ever be emitted under UNION, even though many \
         were generated"
    );
    assert!(
        batch_count > 1,
        "must have spanned multiple output batches given multi-batch-per-iteration \
         chunking"
    );
}
