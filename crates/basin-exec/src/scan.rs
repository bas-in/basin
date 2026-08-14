//! `Scan` — the leaf every physical plan bottoms out in.
//!
//! # Shape
//!
//! Nothing in this crate can execute end to end without something that reads
//! data, but this crate must not depend on `basin-storage` (a scan operator
//! belongs to the physical layer; picking rows off disk/object storage is a
//! separate concern with its own crate, and a compile-time dependency from
//! `basin-exec` to `basin-storage` would make every operator test in this
//! crate need a storage engine spun up). [`BatchSource`] is the seam: `Scan`
//! takes a `Box<dyn BatchSource>` rather than anything storage-shaped, so it
//! is fully testable today with [`VecBatchSource`] (an in-memory `Vec` of
//! batches), and `basin-storage` plugs in later by implementing the trait —
//! nothing in this file changes when that happens.
//!
//! # What `Scan` does, in order
//!
//! Per source batch: evaluate `filters` (a conjunction — `LogicalPlan::Scan`
//! carries a `Vec<Expr>`, ANDed together, matching how pushdown accumulates
//! independent predicates) against the *unprojected* batch, since a pushed
//! filter can reference a column that is not in the final projection (e.g.
//! `SELECT a FROM t WHERE b > 5`); then apply `projection` to the filtered
//! result via `RecordBatch::project`. Filtering before projecting is required
//! for this reason, not an arbitrary ordering choice.
//!
//! Consequently `filters`' column indices here are positions in the SOURCE's
//! schema, the same space as `projection`'s entries. That is deliberately NOT
//! the space `basin_plan::LogicalPlan::Scan`'s filters are written in — those
//! are positions within the scan's own projection — so `build.rs` translates
//! between the two. Getting that translation wrong is a wrong answer rather
//! than an error, because both spaces are valid indices into *something*: see
//! `build::filters_to_source_positions`.
//!
//! # The empty-projection rule
//!
//! `projection = []` means zero columns, not all columns — `COUNT(*)` and
//! similar column-free reads rely on this (see `basin_plan::LogicalPlan::Scan`
//! doc comment). `RecordBatch::project` already does the right thing here: it
//! builds the output from `self.row_count` regardless of how many indices are
//! selected, so a zero-column projection still reports the correct row count
//! rather than collapsing to zero rows.
//!
//! # NULL in a filter predicate
//!
//! `WHERE` keeps only rows where the predicate is `TRUE` — `NULL` (unknown)
//! excludes the row, same as `FALSE`. `arrow::compute::filter_record_batch`
//! already implements exactly this (`prep_null_mask_filter` converts a NULL
//! mask bit to `false` before filtering), so no extra handling is needed here
//! beyond feeding it the evaluated predicate — but see
//! `null_predicate_excludes_the_row` below, which pins the behavior rather
//! than trusting the dependency silently.
//!
//! # Cancellation
//!
//! `next_batch` pulls and processes exactly one batch from `source` per call
//! — including a batch a filter rejects in full, which is returned as a
//! zero-row batch rather than silently skipped to fetch the next one. Every
//! operator downstream (e.g. [`crate::aggregate::HashAggregate`]) already
//! tolerates zero-row batches. Skipping internally would mean a filter that
//! rejects everything for a long run of source batches could make one
//! `next_batch` call do unbounded work with no chance for the caller to check
//! `statement_timeout` in between — see [`crate::operator::Operator`]'s docs.
//!
//! # Memory
//!
//! `Scan` never buffers more than the batch it is currently handing back:
//! [`Scan::memory_used`] reports that one batch's array memory while it is
//! the most recent thing produced, and `0` once the source is exhausted.
//! There is no hash table, sort run or join build side here to account for.

use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::and_kleene;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};

use basin_plan::Expr;

use crate::eval;
use crate::operator::{default_session, ExecError, Operator, SessionRef};

/// Where a [`Scan`] pulls its input batches from.
///
/// Deliberately storage-agnostic — see the module docs. Implementations must
/// obey the same "return control between batches" rule as [`Operator`]:
/// `next_batch` should do bounded work per call.
pub trait BatchSource {
    /// The Arrow schema of every batch this source yields. Constant for the
    /// lifetime of the source — `Scan`'s projection is resolved against this
    /// once, at construction.
    fn schema(&self) -> SchemaRef;

    /// Produce the next batch, or `None` when exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError>;
}

/// An in-memory [`BatchSource`] backed by a fixed `Vec<RecordBatch>`.
///
/// Exists for tests today, and stands in for `basin-storage` until that crate
/// exists — nothing about [`Scan`] changes when a real storage-backed source
/// replaces this.
pub struct VecBatchSource {
    schema: SchemaRef,
    batches: std::collections::VecDeque<RecordBatch>,
}

impl VecBatchSource {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
        }
    }
}

impl BatchSource for VecBatchSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        Ok(self.batches.pop_front())
    }
}

/// Sum of every column's Arrow array memory in `batch` — the honest cost of
/// holding one batch, used by [`Scan::memory_used`].
fn record_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum()
}

/// Evaluate every filter in `filters` against `batch` and fold the results
/// with `AND` (Kleene, matching the three-valued logic `WHERE` uses — see
/// `eval.rs`'s module docs on why `and_kleene` rather than the plain kernel).
/// `None` means "no filters", i.e. keep every row.
fn combined_predicate(
    filters: &[Expr],
    batch: &RecordBatch,
    session: &SessionRef,
) -> Result<Option<BooleanArray>, ExecError> {
    let mut acc: Option<BooleanArray> = None;
    for expr in filters {
        let arr = eval::eval_with(expr, batch, session)?;
        let this = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "scan filter must evaluate to boolean, got {:?} — a planner bug, not user error",
                    arr.data_type()
                ))
            })?
            .clone();
        acc = Some(match acc {
            Some(prev) => {
                and_kleene(&prev, &this).map_err(|e| ExecError::Internal(e.to_string()))?
            }
            None => this,
        });
    }
    Ok(acc)
}

/// The scan operator: pulls batches from a [`BatchSource`], applies pushed
/// filters, then applies a column projection. See the module docs for the
/// full design.
/// Manual `Debug` because [`BatchSource`] is a trait object and cannot be
/// derived through. The source is elided rather than given a placeholder
/// field, so the output stays useful: what a scan reads is the source's
/// business, what it does with the rows is this struct's.
impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scan")
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

pub struct Scan {
    source: Box<dyn BatchSource>,
    projection: Vec<usize>,
    filters: Vec<Expr>,
    schema: SchemaRef,
    /// Bytes of the most recently produced batch; `0` once the source is
    /// exhausted. See the module docs' Memory section.
    bytes_held: usize,
    session: SessionRef,
}

impl Scan {
    /// `projection` is a list of column indices into `source.schema()` — an
    /// empty list means zero columns, not all columns (see the module docs).
    /// `filters` are ANDed together and evaluated against the *unprojected*
    /// batch, so a filter may reference a column absent from `projection`.
    pub fn new(
        source: Box<dyn BatchSource>,
        projection: Vec<usize>,
        filters: Vec<Expr>,
    ) -> Result<Self, ExecError> {
        let source_schema = source.schema();
        let mut fields = Vec::with_capacity(projection.len());
        for &idx in &projection {
            let field = source_schema.fields().get(idx).ok_or_else(|| {
                ExecError::Internal(format!(
                    "scan projection index {idx} out of range for a {}-column schema — a \
                     planner bug, not user error",
                    source_schema.fields().len()
                ))
            })?;
            fields.push(field.clone());
        }
        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            source,
            projection,
            filters,
            schema,
            bytes_held: 0,
            session: default_session(),
        })
    }

    /// Evaluate this scan's pushed-down filters in `session`. See
    /// [`SessionRef`]. A pushed filter is an ordinary predicate — `WHERE
    /// date_trunc('day', ts) = DATE '2024-01-01'` reaches the scan whole — so
    /// it needs the zone for exactly the reason `Filter` does.
    pub fn in_session(mut self, session: SessionRef) -> Self {
        self.session = session;
        self
    }
}

impl Operator for Scan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.source.next_batch()? else {
            self.bytes_held = 0;
            return Ok(None);
        };

        let filtered = match combined_predicate(&self.filters, &batch, &self.session)? {
            Some(predicate) => filter_record_batch(&batch, &predicate)
                .map_err(|e| ExecError::Internal(e.to_string()))?,
            None => batch,
        };

        let projected = filtered
            .project(&self.projection)
            .map_err(|e| ExecError::Internal(e.to_string()))?;

        self.bytes_held = record_batch_bytes(&projected);
        Ok(Some(projected))
    }

    fn memory_used(&self) -> usize {
        self.bytes_held
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use basin_pgtype::{Oid, PgType};
    use basin_plan::{ColumnRef, Datum, OpId};

    fn schema_3col() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ]))
    }

    fn batch_3col(
        schema: &SchemaRef,
        a: Vec<Option<i32>>,
        b: Vec<Option<i32>>,
        c: Vec<Option<&str>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(a)),
                Arc::new(Int32Array::from(b)),
                Arc::new(StringArray::from(c)),
            ],
        )
        .unwrap()
    }

    fn col(index: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.to_string(),
        })
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    fn gt_expr(col_idx: u16, col_name: &str, v: i32) -> Expr {
        Expr::Binary {
            op: OpId(Oid(521)), // int4 >
            lhs: Box::new(col(col_idx, col_name)),
            rhs: Box::new(lit_i32(v)),
        }
    }

    // ── Projection ───────────────────────────────────────────────────────

    #[test]
    fn projection_selects_a_subset_of_columns() {
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(1), Some(2)],
            vec![Some(10), Some(20)],
            vec![Some("x"), Some("y")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![0, 2], vec![]).unwrap();

        assert_eq!(scan.schema().fields().len(), 2);
        assert_eq!(scan.schema().field(0).name(), "a");
        assert_eq!(scan.schema().field(1).name(), "c");

        let out = scan.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 2);
        let a = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let c = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), 1);
        assert_eq!(c.value(0), "x");
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn projection_reorders_columns() {
        let schema = schema_3col();
        let batch = batch_3col(&schema, vec![Some(1)], vec![Some(10)], vec![Some("x")]);
        let source = VecBatchSource::new(schema, vec![batch]);
        // Reversed and reordered: c, a, b.
        let mut scan = Scan::new(Box::new(source), vec![2, 0, 1], vec![]).unwrap();

        assert_eq!(scan.schema().field(0).name(), "c");
        assert_eq!(scan.schema().field(1).name(), "a");
        assert_eq!(scan.schema().field(2).name(), "b");

        let out = scan.next_batch().unwrap().unwrap();
        let c = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let a = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let b = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c.value(0), "x");
        assert_eq!(a.value(0), 1);
        assert_eq!(b.value(0), 10);
    }

    // Item: empty projection means zero columns, not all columns — and must
    // still report the correct row count. This is exactly what COUNT(*)
    // relies on: a scan under a count(*)-only aggregate has nothing to
    // project, but the aggregate still needs to see every row.
    #[test]
    fn empty_projection_yields_zero_columns_but_preserves_row_count() {
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(1), Some(2), Some(3)],
            vec![Some(10), Some(20), Some(30)],
            vec![Some("x"), Some("y"), Some("z")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![], vec![]).unwrap();

        assert_eq!(
            scan.schema().fields().len(),
            0,
            "empty projection is zero columns"
        );

        let out = scan.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_columns(),
            0,
            "the batch itself must carry zero columns"
        );
        assert_eq!(
            out.num_rows(),
            3,
            "COUNT(*) needs the row count preserved even with no columns read"
        );
    }

    #[test]
    fn out_of_range_projection_index_is_rejected_at_construction() {
        let schema = schema_3col();
        let source = VecBatchSource::new(schema, vec![]);
        match Scan::new(Box::new(source), vec![5], vec![]) {
            Err(ExecError::Internal(_)) => {}
            other => panic!(
                "expected ExecError::Internal, got a different result: {}",
                other.is_ok()
            ),
        }
    }

    // ── Filters ──────────────────────────────────────────────────────────

    #[test]
    fn filter_reduces_rows() {
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(1), Some(5), Some(10)],
            vec![Some(1), Some(2), Some(3)],
            vec![Some("x"), Some("y"), Some("z")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan =
            Scan::new(Box::new(source), vec![0, 1, 2], vec![gt_expr(0, "a", 3)]).unwrap();

        let out = scan.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2, "only a=5 and a=10 pass a > 3");
        let a = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.values(), &[5, 10]);
    }

    #[test]
    fn filter_and_projection_compose_filter_sees_unprojected_columns() {
        // WHERE b > 15, SELECT a — b is not in the projection but must still
        // be usable by the filter, since filtering happens before projecting.
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(1), Some(2), Some(3)],
            vec![Some(10), Some(20), Some(30)],
            vec![Some("x"), Some("y"), Some("z")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![gt_expr(1, "b", 15)]).unwrap();

        assert_eq!(scan.schema().fields().len(), 1);
        let out = scan.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 2);
        let a = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.values(), &[2, 3], "b=20 and b=30 pass b > 15");
    }

    #[test]
    fn filter_matching_everything_keeps_every_row() {
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(10), Some(20)],
            vec![Some(1), Some(2)],
            vec![Some("x"), Some("y")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![gt_expr(0, "a", 0)]).unwrap();

        let out = scan.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    // A filter that rejects every row in a batch must still yield a (zero-row)
    // batch rather than being silently skipped — see the module docs' note on
    // why skipping would break the "bounded work per call" cancellation
    // contract.
    #[test]
    fn filter_matching_nothing_yields_a_zero_row_batch_not_none() {
        let schema = schema_3col();
        let batch = batch_3col(
            &schema,
            vec![Some(1), Some(2)],
            vec![Some(1), Some(2)],
            vec![Some("x"), Some("y")],
        );
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![gt_expr(0, "a", 100)]).unwrap();

        let out = scan.next_batch().unwrap();
        assert!(
            out.is_some(),
            "the source was not exhausted, so this must be Some"
        );
        assert_eq!(out.unwrap().num_rows(), 0);
        assert!(
            scan.next_batch().unwrap().is_none(),
            "the source has exactly one batch, so the next call must be EOF"
        );
    }

    // The classic bug: a NULL predicate result must exclude the row, the same
    // as FALSE — SQL's WHERE keeps only definite TRUE, never "TRUE or
    // unknown". An implementation that treats a NULL mask bit as "keep" (or
    // panics trying to read it as a bool) gets this wrong.
    #[test]
    fn null_predicate_excludes_the_row() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let source = VecBatchSource::new(schema, vec![batch]);
        // a > NULL is NULL for every row where a is non-null too, since the
        // comparison's right side is NULL — but here we want a case where the
        // predicate ITSELF evaluates to NULL only for the NULL-valued row (a
        // IS NULL means `a > 1` is NULL there), while other rows get a
        // definite TRUE/FALSE.
        let filters = vec![gt_expr(0, "a", 1)];
        let mut scan = Scan::new(Box::new(source), vec![0], filters).unwrap();

        let out = scan.next_batch().unwrap().unwrap();
        // Row 0: 1 > 1 = FALSE (excluded). Row 1: NULL > 1 = NULL (excluded).
        // Row 2: 3 > 1 = TRUE (kept).
        assert_eq!(out.num_rows(), 1, "only the definite TRUE row survives");
        let a = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 3);
    }

    // ── Multi-batch sources, empty sources, cancellation ────────────────

    #[test]
    fn multi_batch_source_yields_one_batch_per_call() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
            .unwrap();
        let b2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3]))])
            .unwrap();
        let b3 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![4, 5]))])
            .unwrap();
        let source = VecBatchSource::new(schema, vec![b1, b2, b3]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![]).unwrap();

        let out1 = scan.next_batch().unwrap().unwrap();
        assert_eq!(
            out1.num_rows(),
            2,
            "one next_batch call returns control after one batch"
        );
        let out2 = scan.next_batch().unwrap().unwrap();
        assert_eq!(out2.num_rows(), 1);
        let out3 = scan.next_batch().unwrap().unwrap();
        assert_eq!(out3.num_rows(), 2);
        assert!(scan.next_batch().unwrap().is_none());
    }

    #[test]
    fn empty_source_yields_no_batches_immediately() {
        let schema = schema_3col();
        let source = VecBatchSource::new(schema, vec![]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![]).unwrap();
        assert!(scan.next_batch().unwrap().is_none());
    }

    // ── Memory accounting ───────────────────────────────────────────────

    #[test]
    fn memory_used_reports_the_current_batch_and_resets_at_eof() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let source = VecBatchSource::new(schema, vec![batch]);
        let mut scan = Scan::new(Box::new(source), vec![0], vec![]).unwrap();

        assert_eq!(scan.memory_used(), 0, "nothing pulled yet");
        scan.next_batch().unwrap();
        assert!(
            scan.memory_used() > 0,
            "must honestly report the batch just produced"
        );
        scan.next_batch().unwrap(); // EOF
        assert_eq!(
            scan.memory_used(),
            0,
            "nothing held once the source is exhausted"
        );
    }
}
