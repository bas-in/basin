//! `Sort` and `TopK` — the ORDER BY family.
//!
//! Both operators consume a physical [`SortKey`] list: a column position in
//! the child's output plus direction and null placement. That is deliberately
//! *not* `basin_plan::expr::SortKey` (which carries a full [`Expr`] to
//! evaluate) — `eval.rs` is still a stub, so this layer cannot evaluate
//! expressions yet. Whatever lowers a logical `ORDER BY` into a physical plan
//! is responsible for projecting any computed sort expression into a real
//! column before it reaches here. The field names (`descending`,
//! `nulls_first`) intentionally echo `basin_plan::expr::SortKey` so that
//! wiring is a straight copy once expression evaluation exists.
//!
//! [`Expr`]: basin_plan::expr::Expr
//!
//! # Where these two operators disagree with Arrow, and why that matters
//!
//! Arrow's sort kernel (`arrow_ord::ord::make_comparator`) compares floats
//! with `f64`/`f32::total_cmp` — the IEEE 754 `totalOrder` predicate. That
//! predicate disagrees with Postgres's `float8`/`float4` comparison in two
//! ways that this module corrects explicitly rather than inheriting:
//!
//! - `totalOrder` puts `-0.0` strictly before `0.0`. Postgres's float
//!   comparison treats them as equal.
//! - `totalOrder` gives every bit pattern its own rank, so a *negatively
//!   signed* NaN sorts below `-Infinity` — off one end of the order — while a
//!   positively signed NaN sorts above `+Infinity` — off the other end.
//!   Postgres does not look at the sign bit: all NaNs compare equal to each
//!   other and greater than every non-NaN, including `+Infinity`.
//!
//! [`normalize_for_sort`] fixes both by rewriting the *comparison key* (never
//! the output value) so that every NaN collapses to one canonical NaN and
//! every zero collapses to `+0.0` before the array reaches Arrow's kernel.
//! Confirmed against `arrow-ord` 58.4.0's own tests
//! (`arrow_ord::ord::tests::test_f64_nan`, `test_f64_zeros`): ascending,
//! default options, arrow already ranks NaN above a plain float and above
//! `Infinity` (so the "NaN is the max" half of Postgres's rule falls out for
//! a positively-signed NaN for free) — but ranks `-0.0 < 0.0`, which is the
//! wrong answer requirement 3 below exists to prevent, and would rank a
//! negatively-signed NaN as the *minimum*, not the maximum, which is the
//! wrong answer requirement 2 exists to prevent.
//!
//! # Determinism and the Sort/TopK equivalence
//!
//! `lexsort_to_indices` is documented as an *unstable* sort. Left alone, rows
//! with equal keys could come out in a different relative order from `Sort`
//! (one call over the whole input) than from `TopK` (many small merges, one
//! per input batch) — which would break the equivalence requirement 5 below
//! demands. Both operators fix this by appending one more, invisible sort
//! key: each row's original arrival position, ascending. That turns the
//! comparator into a strict total order, so no two distinct rows ever
//! compare equal — the specific sort algorithm and the batch boundaries the
//! input happened to arrive in stop being able to affect the result.

use std::sync::Arc;

use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch, UInt64Array};
use arrow_schema::{ArrowError, DataType, SchemaRef};

use crate::operator::{ExecError, Operator};

/// One physical sort key. See the module docs for why this isn't
/// `basin_plan::expr::SortKey`.
#[derive(Debug, Clone)]
pub struct SortKey {
    /// Index of the column within the child operator's output schema.
    pub column: usize,
    pub descending: bool,
    /// Postgres's default depends on direction (`NULLS LAST` for `ASC`,
    /// `NULLS FIRST` for `DESC`) but that default is resolved by the time a
    /// physical `SortKey` exists — this is always explicit, never inferred
    /// here.
    pub nulls_first: bool,
}

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// Collapse `-0.0` to `0.0` and every NaN to one canonical NaN, for float
/// columns only. Only the returned *comparison key* is touched; callers
/// still `take` the original, unnormalized arrays into the output, so a row
/// that held `-0.0` still holds `-0.0` after sorting — it just no longer
/// sorts as though it were less than `0.0`. See module docs for why arrow's
/// kernel needs this correction.
fn normalize_for_sort(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("DataType::Float64 column backed by Float64Array");
            let normalized: Float64Array = arr
                .iter()
                .map(|v| v.map(|x| if x.is_nan() { f64::NAN } else if x == 0.0 { 0.0 } else { x }))
                .collect();
            Arc::new(normalized)
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("DataType::Float32 column backed by Float32Array");
            let normalized: Float32Array = arr
                .iter()
                .map(|v| v.map(|x| if x.is_nan() { f32::NAN } else if x == 0.0 { 0.0 } else { x }))
                .collect();
            Arc::new(normalized)
        }
        _ => Arc::clone(array),
    }
}

/// Build the `SortColumn` list arrow's kernel needs: one entry per key
/// (normalized if it's a float column), plus a trailing tie-break on
/// `seq` — each row's original arrival position — so the overall order is a
/// strict total order. See "Determinism" in the module docs.
fn sort_columns_with_tiebreak(
    batch: &RecordBatch,
    keys: &[SortKey],
    seq: &UInt64Array,
) -> Vec<SortColumn> {
    let mut columns: Vec<SortColumn> = keys
        .iter()
        .map(|k| SortColumn {
            values: normalize_for_sort(batch.column(k.column)),
            options: Some(SortOptions {
                descending: k.descending,
                nulls_first: k.nulls_first,
            }),
        })
        .collect();
    columns.push(SortColumn {
        values: Arc::new(seq.clone()) as ArrayRef,
        options: Some(SortOptions {
            descending: false,
            nulls_first: false, // seq is never null
        }),
    });
    columns
}

/// Reorder every column of `batch` by `indices`.
fn take_batch(batch: &RecordBatch, indices: &arrow_array::UInt32Array) -> Result<RecordBatch, ExecError> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(arrow_err)
}

/// Full sort: materializes the entire input, then sorts it in one shot.
///
/// # Memory
///
/// Every batch pulled from the child is buffered until the child is
/// exhausted — a full sort cannot emit its first row until it has seen every
/// row (the very last input row might sort first). `memory_used` reports the
/// buffered bytes honestly, and once the running total would exceed the
/// budget given to [`Sort::new`], `next_batch` returns
/// [`ExecError::OutOfMemory`] instead of buffering further.
///
/// Disk spill is deliberately **not** implemented — see
/// `docs/migration/df-removal/08-ir-design.md` §7. Basin wires a bounded
/// memory pool precisely so a heavy sort fails cleanly instead of paging or
/// OOM-killing a shared multi-tenant node; today "fails cleanly" is the whole
/// story, there is no fallback to disk. That is a real gap for a sort whose
/// input doesn't fit in its budget, not a subtle one.
///
/// # Cancellation
///
/// Because the operator must drain the child completely before producing
/// anything, the first call to `next_batch` loops over the child without
/// returning control in between — unlike a well-behaved streaming operator,
/// it cannot honour `statement_timeout` mid-materialization today. That is
/// a known gap, not an oversight: fixing it needs a cancellation signal
/// threaded through `Operator`, which does not exist yet.
pub struct Sort {
    child: Box<dyn Operator>,
    keys: Vec<SortKey>,
    budget: usize,
    schema: SchemaRef,
    buffered: Vec<RecordBatch>,
    buffered_bytes: usize,
    exhausted: bool,
}

impl Sort {
    pub fn new(child: Box<dyn Operator>, keys: Vec<SortKey>, memory_budget: usize) -> Self {
        let schema = child.schema();
        Self {
            child,
            keys,
            budget: memory_budget,
            schema,
            buffered: Vec::new(),
            buffered_bytes: 0,
            exhausted: false,
        }
    }
}

impl Operator for Sort {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.exhausted {
            return Ok(None);
        }

        while let Some(batch) = self.child.next_batch()? {
            let batch_bytes = batch.get_array_memory_size();
            let requested = self.buffered_bytes + batch_bytes;
            if requested > self.budget {
                return Err(ExecError::OutOfMemory {
                    requested,
                    budget: self.budget,
                });
            }
            self.buffered_bytes = requested;
            self.buffered.push(batch);
        }
        self.exhausted = true;

        if self.buffered.is_empty() {
            return Ok(None);
        }

        let combined = concat_batches(&self.schema, self.buffered.iter()).map_err(arrow_err)?;
        // Buffered per-batch copies are no longer needed once merged.
        self.buffered.clear();

        let seq = UInt64Array::from_iter_values(0..combined.num_rows() as u64);
        let sort_cols = sort_columns_with_tiebreak(&combined, &self.keys, &seq);
        let indices = lexsort_to_indices(&sort_cols, None).map_err(arrow_err)?;
        let sorted = take_batch(&combined, &indices)?;

        // Ownership of the sorted batch is about to move to the caller; the
        // operator itself retains nothing after this call returns.
        self.buffered_bytes = 0;
        Ok(Some(sorted))
    }

    fn memory_used(&self) -> usize {
        self.buffered_bytes
    }
}

/// `ORDER BY … LIMIT k`, without ever materializing more than `k` rows plus
/// whatever batch is currently being merged in.
///
/// # Why not "sort, then truncate"
///
/// A full sort followed by a limit has to hold the *entire* input before it
/// can drop anything — exactly the buffering [`Sort`] documents above.
/// Basin's published `ORDER BY … LIMIT` numbers depend on this operator
/// terminating early instead: as each input batch arrives, it's merged into
/// a running "best k so far" via one bounded partial sort
/// (`lexsort_to_indices` with a limit), and every row that doesn't make the
/// cut is dropped immediately rather than retained. Steady-state memory is
/// `O(k + one input batch)`, never `O(n)`. This is the same shape of
/// optimization `sort_streaming_limit.rs` (`basin-engine`) exists to protect
/// on the DataFusion side — that rule reorders the *scan* so a downstream
/// TopK heap can terminate early against an already-sorted stream; this
/// operator is the heap itself.
///
/// # Equivalence with Sort
///
/// The only correctness argument for doing this instead of a full sort is
/// that the result is identical to `Sort` followed by a truncation to `k`
/// rows. `sort_and_topk_agree_on_the_same_input` below tests exactly that,
/// including across multiple input batches — see the module docs'
/// "Determinism" section for how the tie-break on arrival order makes the
/// comparison exact rather than "usually the same".
pub struct TopK {
    child: Box<dyn Operator>,
    keys: Vec<SortKey>,
    k: usize,
    schema: SchemaRef,
    best: Option<RecordBatch>,
    best_seq: Vec<u64>,
    next_row_id: u64,
    exhausted: bool,
}

impl TopK {
    pub fn new(child: Box<dyn Operator>, keys: Vec<SortKey>, k: usize) -> Self {
        let schema = child.schema();
        Self {
            child,
            keys,
            k,
            schema,
            best: None,
            best_seq: Vec::new(),
            next_row_id: 0,
            exhausted: false,
        }
    }

    /// Merge one incoming batch into `self.best`, keeping only the best `k`
    /// rows seen so far (by input order across the whole stream, not just
    /// this batch).
    fn merge_batch(&mut self, batch: RecordBatch) -> Result<(), ExecError> {
        let n = batch.num_rows();
        let fresh_seq: Vec<u64> = (self.next_row_id..self.next_row_id + n as u64).collect();
        self.next_row_id += n as u64;

        let (combined, mut combined_seq) = match self.best.take() {
            Some(best) => {
                let combined =
                    concat_batches(&self.schema, [&best, &batch]).map_err(arrow_err)?;
                let mut seq = std::mem::take(&mut self.best_seq);
                seq.extend(fresh_seq);
                (combined, seq)
            }
            None => (batch, fresh_seq),
        };
        // combined_seq is parallel to combined's rows; reused after `take`
        // below to keep it parallel to `best`.
        let seq_array = UInt64Array::from(combined_seq.clone());

        let sort_cols = sort_columns_with_tiebreak(&combined, &self.keys, &seq_array);
        let limit = self.k.min(combined.num_rows());
        let indices = lexsort_to_indices(&sort_cols, Some(limit)).map_err(arrow_err)?;

        let taken_seq: Vec<u64> = indices
            .values()
            .iter()
            .map(|&i| combined_seq[i as usize])
            .collect();
        let taken = take_batch(&combined, &indices)?;

        combined_seq.clear(); // no longer needed; taken_seq replaces it
        self.best = Some(taken);
        self.best_seq = taken_seq;
        Ok(())
    }
}

impl Operator for TopK {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.exhausted {
            return Ok(None);
        }
        self.exhausted = true;

        // LIMIT 0: nothing can ever be retained, so there's no need to pull
        // the child at all.
        if self.k == 0 {
            return Ok(None);
        }

        while let Some(batch) = self.child.next_batch()? {
            if batch.num_rows() == 0 {
                continue;
            }
            self.merge_batch(batch)?;
        }

        Ok(self.best.take())
    }

    fn memory_used(&self) -> usize {
        // Between calls to `next_batch`, `best` (at most k rows) is the only
        // state this operator holds — the batch being merged in and the
        // concatenated scratch copy inside `merge_batch` are both dropped
        // before that function returns.
        self.best
            .as_ref()
            .map(RecordBatch::get_array_memory_size)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{Field, Schema};
    use std::collections::VecDeque;

    /// A child operator that replays a fixed list of batches, one per
    /// `next_batch` call. Lets tests control exactly where batch boundaries
    /// fall, which matters for exercising `TopK`'s incremental merge.
    struct Feed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Feed {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches: batches.into(),
            }
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

    fn int32_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]))
    }

    fn int32_batch(values: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(int32_schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn sort_int32_column(rows: Vec<Option<i32>>, descending: bool, nulls_first: bool) -> Vec<Option<i32>> {
        let schema = int32_schema();
        let child = Box::new(Feed::new(schema, vec![int32_batch(rows)]));
        let keys = vec![SortKey {
            column: 0,
            descending,
            nulls_first,
        }];
        let mut sort = Sort::new(child, keys, 1 << 30);
        let out = sort.next_batch().unwrap().unwrap();
        assert!(sort.next_batch().unwrap().is_none(), "single-shot output");
        out.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    // Requirement 1: null placement depends on direction, and this operator
    // must honour whatever `nulls_first` says rather than deriving its own
    // default from `descending`. Getting this backwards for any one of the
    // four combinations reproduces Postgres's actual default behaviour by
    // accident in that one case and gets the other three wrong.
    #[test]
    fn nulls_first_is_honoured_independent_of_direction() {
        let rows = vec![Some(1), None, Some(2)];

        // ASC, NULLS LAST (Postgres's default for ASC) — wrong answer this
        // prevents: nulls sorting first just because the operator assumed
        // "nulls_first=false" only ever applies to DESC.
        assert_eq!(
            sort_int32_column(rows.clone(), false, false),
            vec![Some(1), Some(2), None]
        );

        // ASC, NULLS FIRST (an explicit override of the default) — wrong
        // answer this prevents: the operator silently ignoring
        // `nulls_first=true` because it "knows" ASC means nulls last.
        assert_eq!(
            sort_int32_column(rows.clone(), false, true),
            vec![None, Some(1), Some(2)]
        );

        // DESC, NULLS FIRST (Postgres's default for DESC) — wrong answer
        // this prevents: nulls sorting last just because the operator
        // assumed nulls placement never changes with direction.
        assert_eq!(
            sort_int32_column(rows.clone(), true, true),
            vec![None, Some(2), Some(1)]
        );

        // DESC, NULLS LAST (an explicit override) — wrong answer this
        // prevents: the operator conflating "descending" with "nulls
        // first" and ignoring the explicit override.
        assert_eq!(sort_int32_column(rows, true, false), vec![Some(2), Some(1), None]);
    }

    // Requirement 2: Postgres defines NaN as the largest float value,
    // strictly greater than +Infinity. Confirmed above (module docs) that
    // arrow's kernel already ranks a positively-signed NaN this way, but a
    // negatively-signed one sorts as the *minimum* under `total_cmp` — the
    // wrong answer this test would catch if `normalize_for_sort` were
    // removed or skipped for either sign.
    #[test]
    fn nan_sorts_as_the_largest_float_regardless_of_sign_bit() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let rows: Vec<Option<f64>> = vec![Some(1.0), Some(f64::INFINITY), Some(-f64::NAN), Some(f64::NAN)];
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(rows))]).unwrap();
        let child = Box::new(Feed::new(schema, vec![batch]));
        let keys = vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }];
        let mut sort = Sort::new(child, keys, 1 << 30);
        let out = sort.next_batch().unwrap().unwrap();
        let values: Vec<f64> = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();

        assert_eq!(values[0], 1.0);
        assert_eq!(values[1], f64::INFINITY);
        // Both NaNs — the positively and the negatively signed input value —
        // must land after +Infinity, not before it and not before 1.0.
        assert!(values[2].is_nan());
        assert!(values[3].is_nan());
    }

    // Requirement 3: Postgres's float comparison treats -0.0 and 0.0 as
    // equal, so neither may be treated as strictly smaller than the other.
    // Arrow's `total_cmp`-based kernel disagrees (`-0.0 < 0.0`) — confirmed
    // directly against `arrow_ord::ord::tests::test_f64_zeros`. The wrong
    // answer this test catches: -0.0 silently sorting before 0.0 (or vice
    // versa) as though they were distinct values, which for equal-valued
    // rows also means the operator's tie-break — not a sign bit — must be
    // what decides their relative order.
    #[test]
    fn negative_and_positive_zero_sort_as_equal() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        // -0.0 arrives before 0.0 in the input; because they compare equal,
        // the tie-break (original arrival order) must keep them in that
        // relative order rather than a sign-bit comparison reordering them.
        let rows = vec![5.0_f64, -0.0, 0.0];
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(rows))]).unwrap();
        let child = Box::new(Feed::new(schema, vec![batch]));
        let keys = vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }];
        let mut sort = Sort::new(child, keys, 1 << 30);
        let out = sort.next_batch().unwrap().unwrap();
        let values = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Both zeros sort ahead of 5.0, and the original -0.0-then-0.0
        // arrival order survives because the keys compare equal.
        assert_eq!(values.value(0).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(values.value(1).to_bits(), (0.0_f64).to_bits());
        assert_eq!(values.value(2), 5.0);
    }

    // Requirement 4: each key in a multi-key ORDER BY has its own direction
    // and null placement — `ORDER BY a ASC, b DESC` must not have the second
    // key's direction leak from (or get overwritten by) the first. The wrong
    // answer this test catches: applying one direction to every key, which
    // would either leave b ascending within each a-group or reverse a too.
    #[test]
    fn multi_key_sort_applies_each_keys_own_direction() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Int32Array::from(vec![1, 1, 2]);
        let b = Int32Array::from(vec![5, 10, 1]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(a), Arc::new(b)]).unwrap();
        let child = Box::new(Feed::new(schema, vec![batch]));
        // ORDER BY a ASC, b DESC
        let keys = vec![
            SortKey {
                column: 0,
                descending: false,
                nulls_first: false,
            },
            SortKey {
                column: 1,
                descending: true,
                nulls_first: false,
            },
        ];
        let mut sort = Sort::new(child, keys, 1 << 30);
        let out = sort.next_batch().unwrap().unwrap();
        let a_out: Vec<i32> = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        let b_out: Vec<i32> = out
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();

        // a ascending: the a=2 row is last. Within a=1, b descending: 10
        // before 5.
        assert_eq!(a_out, vec![1, 1, 2]);
        assert_eq!(b_out, vec![10, 5, 1]);
    }

    // Requirement 5: the entire correctness argument for TopK's bounded
    // merge instead of a full sort is that it produces exactly the rows and
    // order a full Sort-then-truncate would. This test drives duplicate key
    // values (to force the tie-break to matter) through TWO input batches
    // for TopK, so the incremental merge actually exercises cross-batch
    // ordering, and checks the result against a single-shot Sort over the
    // same rows truncated to k. The wrong answer this catches: the bounded
    // heap dropping a row a full sort would have kept (or keeping one it
    // should have dropped), or reordering a tie differently than Sort would
    // because the merge saw the data in a different shape.
    #[test]
    fn sort_and_topk_agree_on_the_same_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]));
        // key has heavy duplication (mod 3) so many rows tie on the sort
        // key and the arrival-order tie-break has to do real work.
        let keys_col: Vec<i32> = (0..30).map(|i| i % 3).collect();
        let payload_col: Vec<i32> = (0..30).collect();

        let make_batch = |range: std::ops::Range<usize>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(keys_col[range.clone()].to_vec())),
                    Arc::new(Int32Array::from(payload_col[range].to_vec())),
                ],
            )
            .unwrap()
        };
        // Split into three batches so TopK's merge runs more than once.
        let batches = vec![make_batch(0..10), make_batch(10..22), make_batch(22..30)];

        let sort_keys = vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }];
        let k = 7;

        let sort_child = Box::new(Feed::new(Arc::clone(&schema), batches.clone()));
        let mut full_sort = Sort::new(sort_child, sort_keys.clone(), 1 << 30);
        let full = full_sort.next_batch().unwrap().unwrap();
        let full_key: Vec<i32> = full
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .take(k)
            .collect();
        let full_payload: Vec<i32> = full
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .take(k)
            .collect();

        let topk_child = Box::new(Feed::new(Arc::clone(&schema), batches));
        let mut topk = TopK::new(topk_child, sort_keys, k);
        let top = topk.next_batch().unwrap().unwrap();
        assert_eq!(top.num_rows(), k);
        let top_key: Vec<i32> = top
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        let top_payload: Vec<i32> = top
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();

        assert_eq!(top_key, full_key, "same key order as sort-then-truncate");
        assert_eq!(
            top_payload, full_payload,
            "same specific rows (not just same keys) as sort-then-truncate"
        );
    }

    #[test]
    fn topk_limit_zero_produces_no_output_and_does_not_pull_the_child() {
        let schema = int32_schema();
        let child = Box::new(Feed::new(schema, vec![int32_batch(vec![Some(1)])]));
        let mut topk = TopK::new(
            child,
            vec![SortKey {
                column: 0,
                descending: false,
                nulls_first: false,
            }],
            0,
        );
        assert!(topk.next_batch().unwrap().is_none());
    }

    #[test]
    fn sort_reports_out_of_memory_past_its_budget() {
        let schema = int32_schema();
        let big = int32_batch((0..1000).map(Some).collect());
        let size = big.get_array_memory_size();
        let child = Box::new(Feed::new(schema, vec![big]));
        let mut sort = Sort::new(
            child,
            vec![SortKey {
                column: 0,
                descending: false,
                nulls_first: false,
            }],
            size - 1,
        );
        let err = sort.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }
}
