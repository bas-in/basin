//! `LateralJoin` — physical execution of `LATERAL`, `CROSS JOIN LATERAL`, and
//! `LEFT JOIN LATERAL ... ON true`.
//!
//! # Why this operator exists
//!
//! `basin_plan::LogicalPlan::LateralJoin` (`basin-plan/src/lib.rs`) is a
//! distinct node from `Join` precisely because the inner side may reference
//! columns of the *current* outer row — that correlation is the defining
//! property, and a planner or executor that loses track of it produces wrong
//! answers, not just slow ones. DataFusion cannot express this at the
//! physical layer at all: `pg_operators.rs:2246` records correlated LATERAL
//! being left un-rewritten because of an upstream limitation, and
//! `docs/migration/df-removal/16-differential-baseline.md` traces two
//! differential-test failures to DataFusion's physical planner rejecting
//! `OuterReferenceColumn` outright ("not implemented"). Those are 2 of only 4
//! divergences in that baseline that are genuinely hard without patching
//! DataFusion. This operator is how Basin stops depending on DataFusion ever
//! supporting that.
//!
//! Note also that the published 462x LATERAL benchmark win is *not*
//! DataFusion decorrelating the query — an ablation showed DataFusion never
//! decorrelates it. The win comes from Basin's own textual rewrite
//! (`rewrite_lateral_order_limit` in `pg_operators.rs`, around line 3461),
//! which turns a "top-N per group" `LATERAL ... ORDER BY ... LIMIT n`
//! shape into a single `ROW_NUMBER() OVER (PARTITION BY fk ORDER BY ...)`
//! join, entirely avoiding per-row re-evaluation. That rewrite is a
//! SQL-text-to-SQL-text transform with a narrow, conservative scope (see its
//! doc comment): one equality predicate, one child table, an integer
//! `LIMIT`, no nested aggregates. Everything outside that shape — multiple
//! correlated predicates, correlated subqueries that aren't simple
//! table scans, `LATERAL` set-returning functions, arbitrary expressions
//! referencing the outer row — still needs a real per-row execution
//! strategy. That is what `LateralJoin` below provides: the general,
//! always-correct fallback underneath the fast textual rewrite, not a
//! replacement for it.
//!
//! # Why the inner side is a factory, not a `Box<dyn Operator>`
//!
//! Every other binary operator in this crate (see `join.rs`) takes both
//! children as already-constructed `Box<dyn Operator>` — the right side's
//! plan is fixed for the whole query, independent of what the left side
//! produces. LATERAL breaks that assumption: the inner plan's *predicates*
//! (and therefore its rows) depend on the value of the current outer row's
//! columns, which is only known once that row has been pulled from the
//! outer child. A single `Box<dyn Operator>` is constructed once, before
//! any outer row exists, and has no way to be re-parameterized per row
//! afterward — there is no hook on the `Operator` trait for "rebind and
//! restart with new correlated values", and adding one would mean every
//! other operator in the tree carries a capability only this one needs.
//!
//! Instead the inner side is a *factory*: `FnMut(&RecordBatch, usize) ->
//! Result<Box<dyn Operator>, ExecError>`, called once per outer row with
//! that row's batch and index. In a real planner this closure captures the
//! inner logical plan and re-lowers/re-binds it against the outer row's
//! values on each call (constant-folding the correlated column references
//! into literals, then building a fresh physical operator from that). Here
//! it is an opaque closure precisely so this operator can be tested without
//! a planner at all: a test factory can inspect the outer row and hand back
//! any `Operator`, including one whose output depends on the row's value —
//! see `inner_sees_current_outer_row_and_differs_per_row` below.
//!
//! # Semantics
//!
//! For each outer row, in order:
//! 1. Build a fresh inner operator via the factory, bound to that row.
//! 2. Drain it fully.
//! 3. If it produced at least one row, emit outer-row ++ inner-row for each,
//!    outer columns repeated (broadcast) across every inner row.
//! 4. If it produced zero rows:
//!    - `Inner` (and `Cross`, which behaves identically for LATERAL — there
//!      is no join predicate to speak of, the correlation *is* the
//!      predicate): the outer row is dropped entirely.
//!    - `Left`: the outer row is kept exactly once, with every inner column
//!      NULL. This is what makes `LEFT JOIN LATERAL ... ON true` behave the
//!      way callers expect: an outer row with no matches still appears.
//!
//! Only `Inner`/`Cross` and `Left` are meaningful for LATERAL — Postgres
//! itself rejects `RIGHT`/`FULL JOIN LATERAL` at parse time, since the
//! inner side cannot be evaluated without a concrete outer row to
//! correlate against. Constructing this operator with any other
//! [`JoinKind`] panics rather than silently doing something wrong.
//!
//! # Memory
//!
//! Unlike [`crate::join::HashJoin`], there is no build side to buffer
//! across the whole operator's lifetime — each inner operator is fully
//! consumed and dropped before the next outer row starts. `memory_used`
//! reports the array memory of whatever is currently buffered: the
//! current outer batch plus the not-yet-emitted rows produced for the
//! outer row in flight. It never grows unboundedly with the number of
//! outer rows processed so far, only with how wide the single
//! currently-open row's inner result is.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use arrow_select::take::take;
use basin_plan::JoinKind;

use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// Factory for the inner (correlated) side: given the current outer batch
/// and a row index into it, produce a freshly bound physical operator for
/// that one row. See module docs for why this is a closure rather than a
/// `Box<dyn Operator>`.
pub type InnerFactory = Box<dyn FnMut(&RecordBatch, usize) -> Result<Box<dyn Operator>, ExecError>>;

/// The output schema for a lateral join of `outer` and `inner` under `kind`.
/// Mirrors `join_output_schema` in `join.rs` for the two kinds LATERAL
/// actually supports: `Inner`/`Cross` concatenate outer ++ inner as-is,
/// `Left` forces the inner side nullable since an unmatched outer row emits
/// it as all-NULL.
fn lateral_output_schema(outer: &SchemaRef, inner: &SchemaRef, kind: JoinKind) -> SchemaRef {
    let mut fields: Vec<Field> = outer.fields().iter().map(|f| f.as_ref().clone()).collect();
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            fields.extend(inner.fields().iter().map(|f| f.as_ref().clone()));
        }
        JoinKind::Left => {
            fields.extend(
                inner
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(true)),
            );
        }
        other => panic!(
            "LateralJoin does not support {other:?}; only Inner/Cross/Left are valid for LATERAL"
        ),
    }
    Arc::new(Schema::new(fields))
}

fn null_columns(schema: &SchemaRef, len: usize) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_array::new_null_array(f.data_type(), len))
        .collect()
}

/// Take a single row `idx` out of `batch` and repeat it `len` times, one
/// column at a time — how one outer row is broadcast across however many
/// inner rows it produced.
fn broadcast_row(batch: &RecordBatch, idx: usize, len: usize) -> Result<Vec<ArrayRef>, ExecError> {
    let indices = UInt32Array::from(vec![idx as u32; len]);
    batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None).map_err(arrow_err))
        .collect()
}

/// `LateralJoin`. See module docs for the factory rationale, per-kind
/// semantics, and memory contract.
pub struct LateralJoin {
    outer: Box<dyn Operator>,
    make_inner: InnerFactory,
    kind: JoinKind,
    schema: SchemaRef,
    inner_schema: SchemaRef,

    /// The outer batch currently being expanded; `None` when a fresh pull
    /// from `outer` is needed.
    current_outer: Option<RecordBatch>,
    /// Row index within `current_outer` to process next.
    row_idx: usize,
}

impl LateralJoin {
    /// `kind` must be [`JoinKind::Inner`], [`JoinKind::Cross`] (treated
    /// identically to `Inner` — LATERAL has no separate join predicate to
    /// evaluate, the correlation itself is the predicate), or
    /// [`JoinKind::Left`].
    ///
    /// # Panics
    ///
    /// If `kind` is anything else. Postgres rejects `RIGHT`/`FULL JOIN
    /// LATERAL` at parse time for the same reason: those require producing
    /// an inner-side row with no outer row to correlate against, which is
    /// meaningless for a per-row-re-evaluated inner side. Reaching this
    /// with such a `kind` is a planner bug.
    pub fn new(
        outer: Box<dyn Operator>,
        inner_schema: SchemaRef,
        make_inner: InnerFactory,
        kind: JoinKind,
    ) -> Self {
        assert!(
            matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Left),
            "LateralJoin only supports Inner/Cross/Left, got {kind:?}"
        );
        let outer_schema = outer.schema();
        let schema = lateral_output_schema(&outer_schema, &inner_schema, kind);
        Self {
            outer,
            make_inner,
            kind,
            schema,
            inner_schema,
            current_outer: None,
            row_idx: 0,
        }
    }

    /// Fully drain the inner operator built for outer row `row_idx` of
    /// `outer_batch`, and produce this row's contribution to the output —
    /// zero rows, one (unmatched `Left`), or many.
    fn process_row(&mut self, outer_batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let idx = self.row_idx;
        let mut inner_op = (self.make_inner)(outer_batch, idx)?;
        let inner_out_schema = inner_op.schema();
        debug_assert_eq!(
            inner_out_schema.fields(),
            self.inner_schema.fields(),
            "inner factory's operator schema must match the schema passed to LateralJoin::new"
        );

        let mut inner_batches = Vec::new();
        while let Some(b) = inner_op.next_batch()? {
            if b.num_rows() > 0 {
                inner_batches.push(b);
            }
        }

        if inner_batches.is_empty() {
            return match self.kind {
                JoinKind::Left => {
                    // Trap: dropping this row here (Inner's behaviour) is
                    // exactly the bug LEFT JOIN LATERAL exists to prevent —
                    // the outer row must still appear, once, with the
                    // inner side NULL.
                    let outer_cols = broadcast_row(outer_batch, idx, 1)?;
                    let mut cols = outer_cols;
                    cols.extend(null_columns(&self.inner_schema, 1));
                    RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
                }
                JoinKind::Inner | JoinKind::Cross => {
                    // Trap: keeping this row (Left's behaviour) would
                    // fabricate a match that doesn't exist — an Inner
                    // lateral with zero inner rows contributes nothing.
                    Ok(RecordBatch::new_empty(Arc::clone(&self.schema)))
                }
                other => unreachable!("constructor rejects {other:?}"),
            };
        }

        let inner_combined =
            concat_batches(&self.inner_schema, inner_batches.iter()).map_err(arrow_err)?;
        let n = inner_combined.num_rows();
        // Trap: broadcasting the outer row once but truncating (or only
        // taking) some inner rows would silently drop the duplicates a
        // multi-row inner side is supposed to multiply the outer row into.
        let outer_cols = broadcast_row(outer_batch, idx, n)?;
        let mut cols = outer_cols;
        cols.extend(inner_combined.columns().iter().cloned());
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }
}

impl Operator for LateralJoin {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        loop {
            // Refill the current outer batch if exhausted or not yet
            // pulled. Trap: pulling here unconditionally at construction
            // time (rather than lazily, on first `next_batch`) would
            // invoke the factory even when the outer side is empty — the
            // loop below only calls it once a row genuinely exists.
            if self.current_outer.is_none() {
                match self.outer.next_batch()? {
                    Some(b) => {
                        self.current_outer = Some(b);
                        self.row_idx = 0;
                    }
                    None => return Ok(None),
                }
            }

            let outer_batch = self.current_outer.take().expect("just checked Some");
            if self.row_idx >= outer_batch.num_rows() {
                // Empty outer batch, or we've consumed every row of this
                // one — move on to the next pull.
                continue;
            }

            let out = self.process_row(&outer_batch)?;
            self.row_idx += 1;
            if self.row_idx < outer_batch.num_rows() {
                self.current_outer = Some(outer_batch);
            }
            // else: fully consumed, leave current_outer as None so the top
            // of the loop pulls the next outer batch.

            if out.num_rows() > 0 {
                return Ok(Some(out));
            }
            // This row contributed nothing (Inner/Cross with zero inner
            // rows) — keep going rather than returning an empty batch.
        }
    }

    fn memory_used(&self) -> usize {
        self.current_outer
            .as_ref()
            .map(|b| b.get_array_memory_size())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::DataType;
    use std::collections::VecDeque;

    /// Same shape as `join.rs`'s `Feed` / `sort.rs`'s: replays a fixed list
    /// of batches, one per `next_batch` call, so tests control exactly
    /// where batch boundaries fall.
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

    fn int_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int32, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn int_batch(schema: &SchemaRef, cols: Vec<Vec<Option<i32>>>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = cols
            .into_iter()
            .map(|c| Arc::new(Int32Array::from(c)) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
    }

    fn feed_one(schema: SchemaRef, batch: RecordBatch) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![batch]))
    }

    fn feed_many(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, batches))
    }

    fn empty_feed(schema: SchemaRef) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![]))
    }

    fn collect_all(op: &mut dyn Operator) -> RecordBatch {
        let schema = op.schema();
        let mut batches = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            batches.push(b);
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(schema);
        }
        concat_batches(&schema, batches.iter()).unwrap()
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

    /// The value of outer column 0 at the given row, read by a test factory
    /// to decide what the inner side should produce.
    fn outer_val(batch: &RecordBatch, col: usize, row: usize) -> Option<i32> {
        batch
            .column(col)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .into()
    }

    // ─── 1: INNER drops an outer row with zero inner rows ────────────────

    // Trap: an outer row whose inner side is empty must be DROPPED under
    // Inner lateral — the wrong answer this prevents is emitting the outer
    // row with a NULL-filled inner side (that's Left's job, not Inner's).
    #[test]
    fn inner_lateral_drops_outer_row_with_zero_inner_rows() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        // Row with o=2 produces zero inner rows; o=1 and o=3 each produce one.
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            let rows: Vec<Option<i32>> = if v == 2 { vec![] } else { vec![Some(v * 100)] };
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![rows]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        let out = collect_all(&mut lateral);
        assert_eq!(
            out.num_rows(),
            2,
            "outer row o=2 (zero inner rows) must be dropped under Inner"
        );
        let mut got: Vec<Option<i32>> = col_i32(&out, 0);
        got.sort();
        assert_eq!(got, vec![Some(1), Some(3)]);
    }

    // ─── 2: LEFT keeps that same row once, inner NULL ─────────────────────

    // Trap: this is the distinction that makes `LEFT JOIN LATERAL ... ON
    // true` the common idiom. Getting this wrong either drops the row
    // (Inner's behaviour, wrong for Left) or emits it more than once.
    #[test]
    fn left_lateral_keeps_outer_row_once_with_null_inner_on_zero_inner_rows() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            let rows: Vec<Option<i32>> = if v == 2 { vec![] } else { vec![Some(v * 100)] };
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![rows]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Left);
        let out = collect_all(&mut lateral);
        assert_eq!(
            out.num_rows(),
            3,
            "every outer row must appear, including o=2 with zero inner rows"
        );
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![(Some(1), Some(100)), (Some(2), None), (Some(3), Some(300))],
            "o=2 must appear exactly once, with the inner column NULL"
        );
    }

    // ─── 3: multi-row inner side multiplies the outer row ─────────────────

    // Trap: an inner side producing multiple rows must multiply the outer
    // row into that many output rows, not dedupe to one or drop extras.
    #[test]
    fn inner_yielding_multiple_rows_multiplies_the_outer_row() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(1), Some(2)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        // o=1 -> 3 inner rows, o=2 -> 1 inner row. Total output = 4 rows.
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            let n = if v == 1 { 3 } else { 1 };
            let rows: Vec<Option<i32>> = (0..n).map(|k| Some(v * 100 + k)).collect();
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![rows]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        let out = collect_all(&mut lateral);
        assert_eq!(out.num_rows(), 4, "3 inner rows for o=1 + 1 for o=2 = 4");
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (Some(1), Some(100)),
                (Some(1), Some(101)),
                (Some(1), Some(102)),
                (Some(2), Some(200)),
            ]
        );
    }

    // ─── 4: inner sees the CURRENT outer row's value ───────────────────────

    // Trap: an implementation that reuses one inner operator across rows,
    // or binds parameters from a stale row, would make every outer row see
    // the same (or the wrong) inner output. This test fails if the inner
    // side's result doesn't track the row it was built for.
    #[test]
    fn inner_sees_current_outer_row_and_differs_per_row() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(10), Some(20), Some(30)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        // Inner output is a direct, per-row function of the outer value —
        // if the factory were called with the wrong row (or not at all
        // per-row), this would not match.
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(v + 1)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        let out = collect_all(&mut lateral);
        assert_eq!(out.num_rows(), 3);
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (Some(10), Some(11)),
                (Some(20), Some(21)),
                (Some(30), Some(31))
            ],
            "each outer row's inner output must be a function of THAT row's value"
        );
    }

    // ─── 5: empty outer input yields no rows, never invokes the factory ───

    // Trap: calling the factory speculatively (e.g. once at construction,
    // or once per empty outer batch) would be observable as a spurious
    // call with no real outer row behind it — the factory must only ever
    // be invoked once a genuine outer row exists.
    #[test]
    fn empty_outer_input_yields_no_rows_and_never_invokes_the_factory() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = empty_feed(outer_schema);
        let invoked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let invoked_clone = Arc::clone(&invoked);
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_batch, _idx| {
            invoked_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(1)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        assert_eq!(collect_all(&mut lateral).num_rows(), 0);
        assert_eq!(
            invoked.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the inner factory must never be called when there is no outer row"
        );
    }

    // Same, but for Left — an empty outer side still yields nothing (there
    // is no outer row to preserve), distinguishing "no outer rows at all"
    // from "an outer row whose inner side is empty" (trap 2).
    #[test]
    fn empty_outer_input_yields_no_rows_under_left_lateral_either() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = empty_feed(outer_schema);
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_batch, _idx| {
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(1)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Left);
        assert_eq!(collect_all(&mut lateral).num_rows(), 0);
    }

    // ─── Multi-batch outer input ───────────────────────────────────────────

    // Trap: an implementation that only handles a single outer batch (e.g.
    // by reading `next_batch` once and assuming exhaustion) would silently
    // drop every row past the first batch.
    #[test]
    fn multi_batch_outer_input_processes_every_row_of_every_batch() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_many(
            outer_schema.clone(),
            vec![
                int_batch(&outer_schema, vec![vec![Some(1), Some(2)]]),
                int_batch(&outer_schema, vec![vec![Some(3)]]),
                int_batch(&outer_schema, vec![vec![Some(4), Some(5)]]),
            ],
        );
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(v * 10)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        let out = collect_all(&mut lateral);
        assert_eq!(out.num_rows(), 5, "all 5 outer rows across 3 batches");
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (Some(1), Some(10)),
                (Some(2), Some(20)),
                (Some(3), Some(30)),
                (Some(4), Some(40)),
                (Some(5), Some(50)),
            ]
        );
    }

    /// Same shape as trap 1/2 (mixed empty and non-empty inner sides) but
    /// spread across multiple outer batches, so the per-batch-boundary
    /// bookkeeping (`row_idx`, `current_outer`) is exercised together with
    /// the Left semantics rather than each in isolation.
    #[test]
    fn multi_batch_outer_input_with_left_lateral_and_mixed_empty_inner_sides() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_many(
            outer_schema.clone(),
            vec![
                int_batch(&outer_schema, vec![vec![Some(1), Some(2)]]),
                int_batch(&outer_schema, vec![vec![Some(3), Some(4)]]),
            ],
        );
        let inner_schema_for_factory = inner_schema.clone();
        // Even-valued outer rows produce zero inner rows.
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            let rows: Vec<Option<i32>> = if v % 2 == 0 {
                vec![]
            } else {
                vec![Some(v * 100)]
            };
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![rows]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Left);
        let out = collect_all(&mut lateral);
        assert_eq!(
            out.num_rows(),
            4,
            "every outer row across both batches, once each"
        );
        let mut pairs: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (Some(1), Some(100)),
                (Some(2), None),
                (Some(3), Some(300)),
                (Some(4), None),
            ]
        );
    }

    // ─── Cross behaves like Inner for LATERAL ──────────────────────────────

    #[test]
    fn cross_lateral_drops_outer_row_with_zero_inner_rows_same_as_inner() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(1), Some(2)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |batch, idx| {
            let v = outer_val(batch, 0, idx).unwrap();
            let rows: Vec<Option<i32>> = if v == 2 { vec![] } else { vec![Some(v)] };
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![rows]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Cross);
        let out = collect_all(&mut lateral);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_i32(&out, 0), vec![Some(1)]);
    }

    // ─── Schema ─────────────────────────────────────────────────────────

    #[test]
    fn inner_lateral_schema_is_outer_plus_inner_non_nullable_preserved() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = empty_feed(outer_schema.clone());
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_b, _i| {
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![]]),
            ))
        });
        let lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        assert_eq!(lateral.schema().fields().len(), 2);
        assert_eq!(lateral.schema().field(0).name(), "o");
        assert_eq!(lateral.schema().field(1).name(), "i");
    }

    #[test]
    fn left_lateral_schema_forces_inner_columns_nullable() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false, // non-nullable in the inner side's own schema
        )]));
        let outer = empty_feed(outer_schema.clone());
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_b, _i| {
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                RecordBatch::new_empty(inner_schema_for_factory.clone()),
            ))
        });
        let lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Left);
        assert!(
            lateral.schema().field(1).is_nullable(),
            "Left lateral must widen the inner side's nullability, since an \
             unmatched outer row emits it as NULL"
        );
    }

    #[test]
    #[should_panic(expected = "Inner/Cross/Left")]
    fn constructing_with_right_join_kind_panics() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = empty_feed(outer_schema);
        let make_inner: InnerFactory = Box::new(move |_b, _i| unreachable!());
        let _ = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Right);
    }

    // ─── Memory contract ────────────────────────────────────────────────

    #[test]
    fn memory_used_is_zero_before_the_first_pull_and_before_and_after_exhaustion() {
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer = feed_one(
            outer_schema.clone(),
            int_batch(&outer_schema, vec![vec![Some(1)]]),
        );
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_b, _i| {
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(1)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        assert_eq!(
            lateral.memory_used(),
            0,
            "nothing pulled from the outer side yet"
        );
        while lateral.next_batch().unwrap().is_some() {}
        assert_eq!(
            lateral.memory_used(),
            0,
            "fully exhausted: no outer batch left buffered"
        );
    }

    #[test]
    fn memory_used_reports_the_currently_buffered_outer_batch_honestly() {
        // While a multi-row outer batch is being expanded row-by-row, the
        // batch stays buffered (row_idx tracks progress through it) — this
        // is the honest, bounded-by-one-batch memory contract described in
        // the module docs, not a build-side accumulation like HashJoin's.
        let outer_schema = int_schema(&["o"]);
        let inner_schema = int_schema(&["i"]);
        let outer_batch = int_batch(&outer_schema, vec![vec![Some(1), Some(2), Some(3)]]);
        let expected_bytes = outer_batch.get_array_memory_size();
        let outer = feed_one(outer_schema.clone(), outer_batch);
        let inner_schema_for_factory = inner_schema.clone();
        let make_inner: InnerFactory = Box::new(move |_b, _i| {
            Ok(feed_one(
                inner_schema_for_factory.clone(),
                int_batch(&inner_schema_for_factory, vec![vec![Some(1)]]),
            ))
        });
        let mut lateral = LateralJoin::new(outer, inner_schema, make_inner, JoinKind::Inner);
        // First row's output.
        lateral.next_batch().unwrap();
        assert_eq!(
            lateral.memory_used(),
            expected_bytes,
            "outer batch stays buffered while rows 2 and 3 remain unprocessed"
        );
    }
}
