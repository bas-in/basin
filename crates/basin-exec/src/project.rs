//! `Filter` and `Project` — the two operators that connect [`crate::eval`]
//! (scalar expression evaluation over a single `RecordBatch`) to the
//! [`Operator`] layer. Every other physical node in this crate
//! (`aggregate.rs`, `sort.rs`) was written against pre-resolved column
//! indices because `eval.rs` did not exist yet; these two are the first to
//! actually call [`crate::eval::eval`], so the seam between the two files is
//! the point of this module, not just the two operators themselves.
//!
//! # Where Postgres's `WHERE` disagrees with a naive filter
//!
//! `WHERE`'s truth table is two-valued in effect even though the predicate is
//! three-valued: a row survives only when the predicate is exactly TRUE.
//! FALSE excludes it, and so does NULL — `WHERE NULL` keeps nothing, the same
//! as `WHERE FALSE`. [`Filter`] gets this for free from
//! `arrow::compute::filter_record_batch`: its predicate is masked through
//! `prep_null_mask_filter` first (see `arrow-select`'s own doc example on
//! that function), which turns every null predicate entry into `false`
//! before any row is selected. That is exactly the row-survives rule above,
//! not a coincidence this module relies on without checking — see
//! `where_null_excludes_the_row_like_where_false_not_like_a_kept_row` below.
//!
//! # Schema inference for Project — the awkward part of the eval.rs seam
//!
//! [`crate::eval::eval`] takes an `Expr` and a `RecordBatch` and returns an
//! `ArrayRef`; it has no separate "what type would this expression produce
//! against this input schema" entry point. `Project` needs exactly that at
//! construction time, before any real batch has arrived, so
//! `Operator::schema` has something to report. The workaround here is to
//! build a zero-row probe batch from the child's schema
//! (`RecordBatch::new_empty`) and run every projected expression through
//! `eval` once against it; the returned array's `data_type()` is the
//! project's output type for that column. This works — arrow's kernels are
//! well-defined on zero-length arrays, so nothing here special-cases
//! emptiness — but it is a workaround, not a first-class facility: a real
//! type-inference pass over `Expr` (independent of evaluating anything) is
//! the more direct fix, and would also let `Project` report accurate
//! nullability instead of the conservative "every output column is
//! nullable" this file falls back to. See the module's tests for hard
//! evidence: [`project_over_empty_input_yields_empty_output_with_correct_schema`]
//! exercises the exact path this workaround has to get right.

use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use basin_plan::Expr;

use crate::eval;
use crate::operator::{ExecError, Operator};

/// `WHERE predicate` (and `HAVING`, which is the same operator applied after
/// a `GROUP BY`). Evaluates `predicate` once per input batch via
/// [`crate::eval::eval`], then keeps only the rows where it is exactly TRUE.
///
/// # Cancellation
///
/// Unlike [`crate::sort::Sort`] or [`crate::aggregate::HashAggregate`],
/// `Filter` never buffers more than the batch it is currently processing —
/// one call to `next_batch` does exactly one call to the child's
/// `next_batch`, so control returns to the caller between every batch and
/// `statement_timeout` can be checked as often as the child allows.
pub struct Filter {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl Filter {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for Filter {
    /// A filter narrows rows, never columns — the output schema is exactly
    /// the input's.
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };

        let predicate = eval::eval(&self.predicate, &batch)?;
        let predicate = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "WHERE predicate must evaluate to boolean, got {:?} — a planner bug, not user \
                 error",
                    predicate.data_type()
                ))
            })?;

        // `filter_record_batch` treats a null predicate entry as false (see
        // this module's doc comment), which is exactly SQL's "NULL excludes
        // the row" rule for WHERE — no extra null-handling needed here. It
        // also always returns a batch, even an empty one, rather than `None`
        // — see `a_predicate_that_matches_nothing_yields_an_empty_batch_not_end_of_stream`.
        let filtered = filter_record_batch(&batch, predicate)
            .map_err(|e| ExecError::Internal(e.to_string()))?;
        Ok(Some(filtered))
    }

    fn memory_used(&self) -> usize {
        // Filter holds nothing between calls: the batch it is processing is
        // owned by the caller once `next_batch` returns, and nothing is
        // retained after that.
        self.input.memory_used()
    }
}

/// The target list — `SELECT expr AS alias, …`. Evaluates each expression
/// once per input batch via [`crate::eval::eval`] and assembles the results
/// into a new `RecordBatch` under the given output names.
///
/// # Column names
///
/// `exprs`' second element of each pair is the output name, independent of
/// whatever name (if any) the expression's own `Expr::Column` carries —
/// `SELECT x AS y` must produce a column literally named `y` in
/// `RowDescription`, since that is what clients read columns back by. See
/// `project_preserves_the_given_output_names_not_the_source_column_names`.
pub struct Project {
    input: Box<dyn Operator>,
    exprs: Vec<(Expr, String)>,
    schema: SchemaRef,
}

impl Project {
    /// `exprs` is `(expression, output name)` pairs, in target-list order.
    ///
    /// Fails if any expression cannot be evaluated against `input`'s schema
    /// at all (e.g. an out-of-range column reference) — see the module doc's
    /// note on how this determines the output schema via a zero-row probe
    /// batch. Every projected column is reported nullable, since `eval.rs`
    /// exposes no separate way to ask whether a given expression can ever
    /// produce NULL against a given input; see the module doc.
    pub fn new(input: Box<dyn Operator>, exprs: Vec<(Expr, String)>) -> Result<Self, ExecError> {
        let input_schema = input.schema();
        let probe = RecordBatch::new_empty(input_schema);

        let mut fields = Vec::with_capacity(exprs.len());
        for (expr, name) in &exprs {
            let array = eval::eval(expr, &probe)?;
            fields.push(Field::new(name, array.data_type().clone(), true));
        }

        Ok(Self {
            input,
            exprs,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

impl Operator for Project {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };

        let mut arrays = Vec::with_capacity(self.exprs.len());
        for (expr, _name) in &self.exprs {
            arrays.push(eval::eval(expr, &batch)?);
        }

        let out = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| ExecError::Internal(e.to_string()))?;
        Ok(Some(out))
    }

    fn memory_used(&self) -> usize {
        // Same reasoning as Filter::memory_used: nothing is retained between
        // calls.
        self.input.memory_used()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::DataType;
    use basin_pgtype::Oid;
    use basin_plan::{ColumnRef, Datum, OpId};
    use std::collections::VecDeque;

    /// A child operator that replays a fixed list of batches, one per
    /// `next_batch` call — the same test double `sort.rs`/`aggregate.rs` use,
    /// so batch boundaries in a test are exactly the boundaries the operator
    /// under test sees.
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

    fn schema_1i32(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]))
    }

    fn batch_i32(schema: &SchemaRef, values: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn col(index: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.to_string(),
        })
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), basin_pgtype::PgType::INT4)
    }

    fn op(oid_val: u32) -> OpId {
        OpId(Oid(oid_val))
    }

    // ── Filter item 1: WHERE excludes NULL rows, not just FALSE ones ────
    //
    // Wrong answer this prevents: keeping a row whose predicate evaluated to
    // NULL, which would happen if the operator checked "not equal to FALSE"
    // instead of "equal to TRUE".
    #[test]
    fn where_null_excludes_the_row_like_where_false_not_like_a_kept_row() {
        let schema = schema_1i32("x");
        // Predicate is `x > 0`: row 0 -> TRUE (kept), row 1 -> FALSE
        // (dropped), row 2 -> NULL, since x is NULL there (must also be
        // dropped, not kept).
        let batch = batch_i32(&schema, vec![Some(5), Some(-1), None]);
        let input = Feed::boxed(schema, vec![batch]);
        let predicate = Expr::Binary {
            op: op(521), // int4 >
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(0)),
        };
        let mut filter = Filter::new(input, predicate);

        let out = filter.next_batch().unwrap().unwrap();
        let x = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            x.iter().collect::<Vec<_>>(),
            vec![Some(5)],
            "only the row with a definite TRUE predicate must survive; the NULL-predicate row \
             must be excluded exactly like the FALSE one, not kept"
        );
    }

    // ── Filter item 2: a predicate matching nothing yields an EMPTY batch,
    // never None ────────────────────────────────────────────────────────
    //
    // Wrong answer this prevents: `next_batch` returning `None` when every
    // row is filtered out, which the caller would read as "no more data"
    // and would silently truncate the rest of the query (later batches from
    // the same child would never be pulled).
    #[test]
    fn a_predicate_that_matches_nothing_yields_an_empty_batch_not_end_of_stream() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(-1), Some(-2)]);
        let b2 = batch_i32(&schema, vec![Some(7)]);
        let input = Feed::boxed(schema, vec![b1, b2]);
        let predicate = Expr::Binary {
            op: op(521), // int4 >
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(0)),
        };
        let mut filter = Filter::new(input, predicate);

        let first = filter.next_batch().unwrap();
        assert!(
            first.is_some(),
            "an all-FALSE batch must come back as Some(empty), not None"
        );
        let first = first.unwrap();
        assert_eq!(
            first.num_rows(),
            0,
            "no rows matched, so the batch is empty"
        );
        assert_eq!(
            first.schema().field(0).name(),
            "x",
            "the empty batch must still carry the correct schema"
        );

        // The child still has a second, matching batch — Filter must not
        // have stopped pulling it just because the first result was empty.
        let second = filter.next_batch().unwrap().unwrap();
        assert_eq!(
            second
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        assert!(filter.next_batch().unwrap().is_none());
    }

    // Filter over a genuinely empty child (no batches at all) must behave
    // like every other operator: None, not an error and not a phantom batch.
    #[test]
    fn filter_over_an_empty_child_yields_no_batches() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(schema, vec![]);
        let predicate = Expr::Literal(Datum::Bool(true), basin_pgtype::PgType::BOOL);
        let mut filter = Filter::new(input, predicate);
        assert!(filter.next_batch().unwrap().is_none());
    }

    // Filter must evaluate the predicate independently per batch and keep
    // pulling across many of them — not just the first.
    #[test]
    fn filter_applies_the_predicate_across_multiple_batches() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(1), Some(2), Some(3)]);
        let b2 = batch_i32(&schema, vec![Some(4), Some(5)]);
        let b3 = batch_i32(&schema, vec![Some(6)]);
        let input = Feed::boxed(schema, vec![b1, b2, b3]);
        let predicate = Expr::Binary {
            op: op(521), // int4 >
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(3)),
        };
        let mut filter = Filter::new(input, predicate);

        let mut seen = Vec::new();
        while let Some(batch) = filter.next_batch().unwrap() {
            let x = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            seen.extend(x.iter().flatten());
        }
        assert_eq!(seen, vec![4, 5, 6]);
    }

    // ── Filter item 5: an evaluation error propagates, it never becomes a
    // silent NULL/false row ──────────────────────────────────────────────
    #[test]
    fn filter_propagates_an_evaluation_error_instead_of_treating_it_as_no_match() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(i32::MAX)]);
        let input = Feed::boxed(schema, vec![batch]);
        // (x + 1) > 0 — the addition overflows i32::MAX, which must raise,
        // not silently wrap into a negative number that then reads as FALSE.
        // 551 is int4 `+`, the same oid eval.rs's own
        // `integer_addition_overflow_errors_instead_of_wrapping` test uses.
        let predicate = Expr::Binary {
            op: op(551),
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(1)),
        };
        let mut filter = Filter::new(input, predicate);
        let err = filter.next_batch().unwrap_err();
        assert!(
            matches!(err, ExecError::Overflow(_)),
            "an overflow while evaluating the predicate must propagate as ExecError::Overflow, \
             not be swallowed into an excluded row: got {err:?}"
        );
    }

    // ── Project item 3: output column names are the given aliases, not the
    // source expression's own column name ───────────────────────────────
    #[test]
    fn project_preserves_the_given_output_names_not_the_source_column_names() {
        let schema = schema_1i32("source_col");
        let batch = batch_i32(&schema, vec![Some(1), Some(2)]);
        let input = Feed::boxed(schema, vec![batch]);
        let mut project =
            Project::new(input, vec![(col(0, "source_col"), "renamed".to_string())]).unwrap();

        assert_eq!(
            project.schema().field(0).name(),
            "renamed",
            "Project's advertised schema must use the given alias"
        );
        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(
            out.schema().field(0).name(),
            "renamed",
            "each output RecordBatch must carry the given alias too, since RowDescription and \
             clients read columns by name, not by the source expression's own name"
        );
    }

    // ── Project item 4: zero input rows -> zero output rows, correct schema
    // and types ──────────────────────────────────────────────────────────
    #[test]
    fn project_over_empty_input_yields_empty_output_with_correct_schema() {
        let schema = schema_1i32("x");
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        let input = Feed::boxed(Arc::clone(&schema), vec![empty_batch]);
        let exprs = vec![
            (col(0, "x"), "x_out".to_string()),
            (
                Expr::Cast {
                    arg: Box::new(col(0, "x")),
                    to: basin_pgtype::PgType::INT8,
                    kind: basin_pgtype::cast::CastKind::Implicit,
                },
                "x_as_bigint".to_string(),
            ),
        ];
        let mut project = Project::new(input, exprs).unwrap();

        assert_eq!(project.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(project.schema().field(1).data_type(), &DataType::Int64);

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_rows(),
            0,
            "zero input rows must yield zero output rows"
        );
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
        assert!(project.next_batch().unwrap().is_none());
    }

    // Project over a child with no batches at all (not even an empty one)
    // must still behave like every other operator: None.
    #[test]
    fn project_over_a_child_with_no_batches_yields_no_batches() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(Arc::clone(&schema), vec![]);
        let mut project = Project::new(input, vec![(col(0, "x"), "x".to_string())]).unwrap();
        assert!(project.next_batch().unwrap().is_none());
    }

    // Project must apply its expressions to every batch pulled from the
    // child, computing real values (not just carrying schema), across
    // multiple batches.
    #[test]
    fn project_computes_expressions_across_multiple_batches() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(1), Some(2)]);
        let b2 = batch_i32(&schema, vec![Some(3)]);
        let input = Feed::boxed(schema, vec![b1, b2]);
        let doubled = Expr::Binary {
            op: op(514), // int4 * (see eval.rs's own overflow test for this oid)
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(2)),
        };
        let mut project = Project::new(input, vec![(doubled, "doubled".to_string())]).unwrap();

        let mut seen = Vec::new();
        while let Some(batch) = project.next_batch().unwrap() {
            let x = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            seen.extend(x.iter().flatten());
        }
        assert_eq!(seen, vec![2, 4, 6]);
    }

    // Project can widen a column's type via CAST, string literals, and
    // multiple heterogeneous output types in one target list — general
    // coverage beyond the single-column cases above.
    #[test]
    fn project_builds_a_heterogeneous_target_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(StringArray::from(vec![Some("hi")])),
            ],
        )
        .unwrap();
        let input = Feed::boxed(schema, vec![batch]);
        let exprs = vec![
            (col(0, "n"), "n_out".to_string()),
            (col(1, "s"), "s_out".to_string()),
            (
                Expr::Cast {
                    arg: Box::new(col(0, "n")),
                    to: basin_pgtype::PgType::FLOAT8,
                    kind: basin_pgtype::cast::CastKind::Implicit,
                },
                "n_as_float".to_string(),
            ),
        ];
        let mut project = Project::new(input, exprs).unwrap();
        let out = project.next_batch().unwrap().unwrap();

        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "hi"
        );
        assert_eq!(
            out.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            10.0
        );
    }

    // ── Project item 5: an evaluation error propagates, never a silent NULL
    // ────────────────────────────────────────────────────────────────────
    #[test]
    fn project_propagates_an_evaluation_error_instead_of_producing_null() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(10)]);
        let input = Feed::boxed(schema, vec![batch]);
        // x / 0 must error, not become a NULL column value.
        let div = Expr::Binary {
            op: op(528), // int4 / (see eval.rs's own division-by-zero test)
            lhs: Box::new(col(0, "x")),
            rhs: Box::new(lit_i32(0)),
        };
        let mut project = Project::new(input, vec![(div, "bad".to_string())]).unwrap();
        let err = project.next_batch().unwrap_err();
        assert_eq!(
            err,
            ExecError::DivisionByZero,
            "division by zero while projecting must raise, not silently produce a NULL output \
             value"
        );
    }

    // Filter's own schema is exactly the input's — narrowing rows must never
    // change column shape.
    #[test]
    fn filter_schema_is_identical_to_the_inputs() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(Arc::clone(&schema), vec![]);
        let predicate = Expr::Literal(Datum::Bool(true), basin_pgtype::PgType::BOOL);
        let filter = Filter::new(input, predicate);
        assert_eq!(filter.schema(), schema);
    }

    // A boolean-typed predicate constructed from a non-boolean expression
    // (a planner bug, but one that must fail loudly rather than panic) is
    // rejected with TypeMismatch rather than a downcast panic.
    #[test]
    fn a_non_boolean_predicate_is_a_type_mismatch_not_a_panic() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(1)]);
        let input = Feed::boxed(schema, vec![batch]);
        // x itself (an Int32), used directly as a WHERE predicate.
        let predicate = col(0, "x");
        let mut filter = Filter::new(input, predicate);
        let err = filter.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // Filter and Project report no held memory of their own: they retain
    // nothing across `next_batch` calls, unlike Sort/HashAggregate/HashJoin.
    #[test]
    fn filter_and_project_report_no_retained_memory() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(1), Some(2), Some(3)]);
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let predicate = Expr::Literal(Datum::Bool(true), basin_pgtype::PgType::BOOL);
        let mut filter = Filter::new(input, predicate);
        filter.next_batch().unwrap();
        assert_eq!(filter.memory_used(), 0);

        let batch2 = batch_i32(&schema, vec![Some(1)]);
        let input2 = Feed::boxed(schema, vec![batch2]);
        let mut project = Project::new(input2, vec![(col(0, "x"), "x".to_string())]).unwrap();
        project.next_batch().unwrap();
        assert_eq!(project.memory_used(), 0);
    }
}
