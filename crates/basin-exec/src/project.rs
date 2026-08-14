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
//! the more direct fix. See the module's tests for hard evidence:
//! [`project_over_empty_input_yields_empty_output_with_correct_schema`]
//! exercises the exact path this workaround has to get right.
//!
//! # Nullability inference — [`expr_is_nullable`]
//!
//! The probe answers *what type*; it cannot answer *can this be NULL*, since
//! a zero-row array has no nulls to observe. `Project` used to give up and
//! report every output column nullable, which was the single largest schema
//! divergence in the recorded golden answers: **270 of the 505 recorded
//! result columns are `notnull`**, and nearly every `SELECT` ends in a
//! `Project`, so the flattening reached almost all of them.
//!
//! The information is available, just not from `basin_plan::Schema` — that is
//! `Vec<(String, PgType)>` and carries no nullability at all (see its own
//! `KNOWN LIMITATION` note). It comes from two places instead:
//!
//! 1. **The input batch's Arrow schema**, which the layers beneath already
//!    compute correctly: `join.rs`'s `join_output_schema` widens the
//!    null-extended side of `Left`/`Right`/`Full` joins, and `aggregate.rs`
//!    marks `count`/`count(*)` non-nullable and every other aggregate
//!    nullable. Both were confirmed against a live PostgreSQL 18.2: over zero
//!    rows `count(*)` and `count(x)` are `0`, while `sum`/`avg`/`min`/`max`/
//!    `bool_and`/`string_agg`/`array_agg` are all NULL; and the non-preserved
//!    side of a `LEFT JOIN` yields NULL even for a `NOT NULL` column.
//! 2. **The shape of the expression itself**, per [`expr_is_nullable`].
//!
//! The rule everywhere is *conservative*: anything not proven total is
//! reported nullable, which is exactly the old behaviour, so no shape can
//! regress. Over-claiming is not silent either — `RecordBatch::try_new`
//! rejects a non-nullable field whose array holds nulls, so a wrong
//! inference surfaces as an error from `next_batch`, not as a wrong answer.

use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use basin_plan::{Datum, Expr, OpId};

use crate::eval;
use crate::operator::{batch_with_row_count, default_session, ExecError, Operator, SessionRef};

/// Operator names (`pg_operator.oprname`, Postgres's own internal spelling)
/// that **cannot** return NULL unless one of their operands is NULL. Every
/// one was measured against a live PostgreSQL 18.2 rather than assumed — the
/// probe being `SELECT (<expr with non-NULL operands>) IS NULL`, which came
/// back `false` for each name below.
///
/// This is an allowlist rather than a denylist on purpose. The four JSON path
/// operators Basin knows — `->`, `->>`, `#>`, `#>>` — are the counterexample
/// that makes "most operators propagate" wrong as a blanket rule: they are
/// *strict* (NULL in, NULL out) yet still return NULL from wholly non-NULL
/// inputs when the key is absent, e.g. `'{"a":1}'::jsonb -> 'zz'` is NULL.
/// They are simply absent here, and so infer as nullable.
///
/// Note that `/` and `%` are total in the sense that matters: Postgres
/// *raises* on division by zero rather than returning NULL (measured:
/// `SELECT 1/0` is `ERROR: division by zero`), and `eval.rs` matches that
/// with [`ExecError::DivisionByZero`].
///
/// These are all 23 of the 27 distinct names in
/// `basin_pgtype::operator::OPERATORS`; the missing four are the JSON path
/// operators above.
const TOTAL_OPERATORS: &[&str] = &[
    // Comparison — always TRUE or FALSE for non-NULL operands.
    "=", "<>", "<", "<=", ">", ">=", //
    // Arithmetic and concatenation — raise on overflow/zero rather than
    // yielding NULL.
    "+", "-", "*", "/", "%", "||", //
    // Pattern matching — `~~` is LIKE in Postgres's internal spelling.
    "~", "~*", "!~", "!~*", "~~", //
    // Containment / overlap / key existence — total booleans.
    "@>", "<@", "&&", "?", "?&", "?|",
];

/// Whether the operator `op` denotes is known to be total: never NULL for
/// non-NULL operands. Unknown oids — including the private `AND`/`OR`/`NOT`
/// sentinels `eval.rs` invents, which are not `pg_operator` rows at all —
/// answer `false`, i.e. "assume it can be NULL".
fn operator_is_total(op: OpId) -> bool {
    basin_pgtype::operator::OPERATORS
        .iter()
        .find(|sig| sig.oid == op.0)
        .is_some_and(|sig| TOTAL_OPERATORS.contains(&sig.name))
}

/// Can `expr` produce SQL NULL when evaluated against a batch whose schema is
/// `input`?
///
/// **Conservative by construction**: every answer is either "proven it cannot"
/// or `true`. Any `Expr` variant, operator or function not enumerated below
/// falls through to `true`, which is precisely the behaviour this file had
/// before the inference existed, so adding a case can only ever tighten the
/// schema and never loosen it.
///
/// Each rule was measured against a live PostgreSQL 18.2 (the probe being
/// `SELECT (<expr>) IS NULL` over non-NULL inputs, or over zero rows for the
/// aggregate cases):
///
/// | Shape | Measured | Rule |
/// |---|---|---|
/// | `Column` | — | exactly its input field's nullability |
/// | `NULL::int` | NULL | literal `Datum::Null` is nullable, any other literal is not |
/// | `1::text` / `NULL::int::text` | not NULL / NULL | a cast propagates its argument |
/// | `1 + 2`, `'a' \|\| 'b'`, `'a' LIKE 'b'` | not NULL | see [`TOTAL_OPERATORS`] |
/// | `'{"a":1}'::jsonb -> 'zz'` | **NULL** | why [`TOTAL_OPERATORS`] is an allowlist |
/// | `COALESCE(NULL::int, 7)` | not NULL | non-nullable if *any* argument is |
/// | `COALESCE(NULL::int, NULL::int)` | NULL | all-nullable arguments stay nullable |
/// | `CASE WHEN false THEN 1 END` | **NULL** | a missing `ELSE` is an implicit `ELSE NULL` |
/// | `CASE WHEN false THEN 1 ELSE 2 END` | not NULL | every result arm, `ELSE` included, must be non-nullable |
/// | `NULL::int IS NULL` | not NULL | `IS NULL`/`IS NOT NULL` is total |
/// | `NULL::bool IS TRUE` | not NULL | every `BoolTest` is total |
/// | `NULL IS NOT DISTINCT FROM NULL` | not NULL | null-safe equality is total |
/// | `1 IN (2, NULL)` | **NULL** | so every list element counts, not just the argument |
/// | `1 IN (2, 3)` | not NULL | |
/// | `ARRAY[NULL::int]` | not NULL | the array is not NULL even when its elements are |
/// | `(ARRAY[1,2])[9]` | **NULL** | so `Subscript` stays nullable |
/// | `NULLIF(1,1)` | **NULL** | so `ScalarFn` stays nullable |
fn expr_is_nullable(expr: &Expr, input: &Schema) -> bool {
    // Read as "can be NULL". Helper for the common "nullable if any child is".
    let any = |es: &[Expr]| es.iter().any(|e| expr_is_nullable(e, input));

    match expr {
        // The whole point of the exercise: a column is nullable exactly when
        // its source field is. An out-of-range index is a planner bug that
        // `eval_column` will report properly; answer conservatively here.
        Expr::Column(c) => input
            .fields()
            .get(c.index as usize)
            .is_none_or(|f| f.is_nullable()),

        Expr::Literal(Datum::Null, _) => true,
        Expr::Literal(..) => false,

        // A parameter's value is not known until Bind; it may well be NULL.
        Expr::Parameter { .. } => true,

        Expr::Cast { arg, .. } => expr_is_nullable(arg, input),

        Expr::Unary { op, arg } => !operator_is_total(*op) || expr_is_nullable(arg, input),
        Expr::Binary { op, lhs, rhs } => {
            !operator_is_total(*op) || expr_is_nullable(lhs, input) || expr_is_nullable(rhs, input)
        }

        // COALESCE returns its first non-NULL argument, so one provably
        // non-nullable argument anywhere makes the whole expression
        // non-nullable — the arguments before it cannot all be NULL and reach
        // past it. An empty list (not something lowering produces) is NULL.
        Expr::Coalesce(args) => args.iter().all(|a| expr_is_nullable(a, input)),

        // Only the *result* arms decide: a NULL in a WHEN condition makes the
        // branch not taken, it does not make the result NULL. A missing ELSE
        // is an implicit `ELSE NULL`, which is why `else_: None` is nullable
        // regardless of how total the THEN arms are.
        Expr::Case {
            operand: _,
            whens,
            else_,
        } => {
            else_.as_ref().is_none_or(|e| expr_is_nullable(e, input))
                || whens.iter().any(|(_, then)| expr_is_nullable(then, input))
        }

        // The three-valued-logic tests are total by definition: they exist
        // precisely to turn an unknown into a definite boolean.
        Expr::IsNull { .. } | Expr::BoolTest { .. } | Expr::DistinctFrom { .. } => false,

        // `1 IN (2, NULL)` is NULL, so a nullable list element is as
        // infectious as a nullable argument.
        Expr::InList { arg, list, .. } => expr_is_nullable(arg, input) || any(list),
        Expr::Between {
            arg,
            low,
            high,
            symmetric: _,
            negated: _,
        } => {
            expr_is_nullable(arg, input)
                || expr_is_nullable(low, input)
                || expr_is_nullable(high, input)
        }
        Expr::Like {
            arg,
            pattern,
            escape,
            ..
        } => {
            expr_is_nullable(arg, input)
                || expr_is_nullable(pattern, input)
                || escape.as_ref().is_some_and(|e| expr_is_nullable(e, input))
        }

        // `ARRAY[…]` constructs an array value; that value is never NULL,
        // however many of its elements are (measured: `ARRAY[NULL::int] IS
        // NULL` is false).
        Expr::ArrayLit(_) => false,

        // Everything else is left nullable, which is where this file started:
        //
        // * `ScalarFn` — `NULLIF(1,1)` is NULL from non-NULL inputs, so
        //   function totality needs its own measured allowlist; not built yet.
        // * `Subscript` — `(ARRAY[1,2])[9]` is NULL (measured).
        // * `RowLit`, `FieldSelect` — composite handling is not settled.
        // * `Aggregate`, `Window`, `SetReturning`, `Subquery` — `eval.rs`
        //   refuses all four outright; they never reach a `Project`'s target
        //   list, they are computed by the operator below it and referenced
        //   here as `Expr::Column`, which is the case that gets `count(*)`'s
        //   non-nullability right.
        _ => true,
    }
}

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
    session: SessionRef,
}

impl Filter {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self {
            input,
            predicate,
            session: default_session(),
        }
    }

    /// Evaluate this filter's predicate in `session` — the session's
    /// `TimeZone` and clock, rather than [`SessionRef`]'s UTC-and-no-clock
    /// default. See [`SessionRef`] for why this is a builder rather than a
    /// constructor argument.
    pub fn in_session(mut self, session: SessionRef) -> Self {
        self.session = session;
        self
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

        let predicate = eval::eval_with(&self.predicate, &batch, &self.session)?;
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
    session: SessionRef,
}

impl Project {
    /// `exprs` is `(expression, output name)` pairs, in target-list order.
    ///
    /// Fails if any expression cannot be evaluated against `input`'s schema
    /// at all (e.g. an out-of-range column reference) — see the module doc's
    /// note on how this determines the output *type* via a zero-row probe
    /// batch. Each column's *nullability* comes from [`expr_is_nullable`]
    /// against the input's Arrow schema, not from the probe, which has no
    /// nulls to observe.
    pub fn new(input: Box<dyn Operator>, exprs: Vec<(Expr, String)>) -> Result<Self, ExecError> {
        let input_schema = input.schema();
        let probe = RecordBatch::new_empty(Arc::clone(&input_schema));

        let mut fields = Vec::with_capacity(exprs.len());
        for (expr, name) in &exprs {
            let array = eval::eval(expr, &probe)?;
            fields.push(Field::new(
                name,
                array.data_type().clone(),
                expr_is_nullable(expr, &input_schema),
            ));
        }

        Ok(Self {
            input,
            exprs,
            schema: Arc::new(Schema::new(fields)),
            session: default_session(),
        })
    }

    /// Evaluate this target list in `session`. See [`SessionRef`].
    ///
    /// The output *schema* is fixed at construction, deliberately without the
    /// session: it comes from evaluating each expression against a zero-row
    /// probe, and no `TimeZone` changes an expression's type — `date_trunc`
    /// returns a timestamptz in every zone. Only the values move.
    pub fn in_session(mut self, session: SessionRef) -> Self {
        self.session = session;
        self
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
            arrays.push(eval::eval_with(expr, &batch, &self.session)?);
        }

        // A projection to ZERO columns is a real plan, and its ROW COUNT is
        // the whole payload — see [`batch_with_row_count`], which is where
        // the reasoning lives so that every operator with a prunable schema
        // shares one answer.
        let out = batch_with_row_count(Arc::clone(&self.schema), arrays, batch.num_rows())
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

    // ── Nullability inference ───────────────────────────────────────────
    //
    // Every rule below was measured against a live PostgreSQL 18.2; the
    // probes are recorded in `expr_is_nullable`'s own doc table. What is
    // being pinned here is that `Project` reports each one, since a `Project`
    // that flattens everything to nullable is the single largest schema
    // divergence in the recorded golden answers (270 of 505 recorded result
    // columns are `notnull`).

    /// A two-column input where the first field is NOT NULL and the second is
    /// nullable — enough to tell "inherited from the source" apart from any
    /// blanket answer.
    fn mixed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("nn", DataType::Int32, false),
            Field::new("n", DataType::Int32, true),
        ]))
    }

    fn mixed_batch() -> RecordBatch {
        RecordBatch::try_new(
            mixed_schema(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(Int32Array::from(vec![Some(1), None])),
            ],
        )
        .unwrap()
    }

    /// The nullability `Project` advertises for a single expression over
    /// [`mixed_schema`].
    fn projected_nullability(expr: Expr) -> bool {
        let input = Feed::boxed(mixed_schema(), vec![]);
        Project::new(input, vec![(expr, "c".to_string())])
            .unwrap()
            .schema()
            .field(0)
            .is_nullable()
    }

    // ── The headline case: a column carries its source's nullability ────
    //
    // Wrong answer this prevents: reporting `SELECT id FROM t` nullable when
    // `id` is `NOT NULL`, which is what this file did for every projected
    // column regardless of source.
    #[test]
    fn a_projected_column_inherits_its_source_fields_nullability() {
        assert!(
            !projected_nullability(col(0, "nn")),
            "projecting a NOT NULL source column must stay NOT NULL — this is the case that \
             reaches nearly every SELECT, since almost all of them end in a Project"
        );
        assert!(
            projected_nullability(col(1, "n")),
            "projecting a nullable source column must stay nullable"
        );
    }

    // Literals: `SELECT 1` cannot be NULL, `SELECT NULL::int` always is.
    // Measured: `(1::text) IS NULL` -> false, `(NULL::int) IS NULL` -> true.
    #[test]
    fn a_literal_is_non_nullable_unless_it_is_the_null_literal() {
        assert!(!projected_nullability(lit_i32(7)));
        assert!(!projected_nullability(Expr::Literal(
            Datum::Bool(true),
            basin_pgtype::PgType::BOOL
        )));
        assert!(
            projected_nullability(Expr::Literal(Datum::Null, basin_pgtype::PgType::INT4)),
            "a NULL literal is nullable however it is typed"
        );
    }

    // A cast propagates its argument's nullability in both directions.
    // Measured: `(1::text) IS NULL` -> false, `(NULL::int::text) IS NULL` ->
    // true.
    #[test]
    fn a_cast_propagates_its_arguments_nullability() {
        let cast = |arg: Expr| Expr::Cast {
            arg: Box::new(arg),
            to: basin_pgtype::PgType::INT8,
            kind: basin_pgtype::cast::CastKind::Implicit,
        };
        assert!(!projected_nullability(cast(col(0, "nn"))));
        assert!(projected_nullability(cast(col(1, "n"))));
    }

    // Arithmetic and comparison over non-NULL operands cannot be NULL —
    // Postgres *raises* on overflow and division by zero rather than
    // returning NULL (measured: `SELECT 1/0` is `ERROR: division by zero`),
    // and `eval.rs` matches that. One nullable operand is enough to infect
    // the result.
    #[test]
    fn a_total_operator_is_non_nullable_exactly_when_both_operands_are() {
        let bin = |o: u32, l: Expr, r: Expr| Expr::Binary {
            op: op(o),
            lhs: Box::new(l),
            rhs: Box::new(r),
        };
        // 551 = int4 `+`, 96 = int4 `=`, 528 = int4 `/`.
        for oid_val in [551u32, 96, 528] {
            assert!(
                !projected_nullability(bin(oid_val, col(0, "nn"), lit_i32(2))),
                "operator {oid_val} over two non-nullable operands must be non-nullable"
            );
            assert!(
                projected_nullability(bin(oid_val, col(0, "nn"), col(1, "n"))),
                "operator {oid_val} must report nullable once either operand is"
            );
        }
    }

    // The reason `TOTAL_OPERATORS` is an allowlist and not "everything except
    // a few". The JSON path operators are strict, yet still return NULL from
    // wholly non-NULL inputs when the key is absent — measured live:
    // `'{"a":1}'::jsonb -> 'zz'` is NULL, and likewise for `->>`, `#>`,
    // `#>>`. Asserted on the classifier directly, since `eval.rs` does not
    // implement these operators and could not be probed through `Project`.
    #[test]
    fn json_path_operators_are_not_treated_as_total() {
        for (oid_val, name) in [(3211u32, "->"), (3477, "->>"), (3213, "#>"), (3206, "#>>")] {
            assert!(
                !operator_is_total(op(oid_val)),
                "{name} ({oid_val}) returns NULL for an absent key even from non-NULL operands, \
                 so it must never be classified total"
            );
        }
        // The counterweight: the ordinary families really are total, or the
        // allowlist would be worthless.
        for (oid_val, name) in [(96u32, "="), (551, "+"), (528, "/")] {
            assert!(operator_is_total(op(oid_val)), "{name} must be total");
        }
        // A sentinel that is not a `pg_operator` row at all — `eval.rs`'s
        // private `AND` — must fall through to "assume nullable".
        assert!(
            !operator_is_total(op(u32::MAX)),
            "the private AND/OR/NOT sentinels are not operator-table rows; classifying one total \
             would be a guess"
        );
    }

    // COALESCE returns its first non-NULL argument, so one provably
    // non-nullable argument anywhere makes the whole thing non-nullable.
    // Measured: `COALESCE(NULL::int, 7) IS NULL` -> false,
    // `COALESCE(NULL::int, NULL::int) IS NULL` -> true.
    #[test]
    fn coalesce_is_non_nullable_when_any_argument_is() {
        assert!(
            !projected_nullability(Expr::Coalesce(vec![col(1, "n"), lit_i32(0)])),
            "a nullable first argument backed by a non-nullable fallback cannot be NULL"
        );
        assert!(
            !projected_nullability(Expr::Coalesce(vec![col(0, "nn"), col(1, "n")])),
            "the non-nullable argument need not be last"
        );
        assert!(
            projected_nullability(Expr::Coalesce(vec![col(1, "n"), col(1, "n")])),
            "all-nullable arguments stay nullable"
        );
    }

    // CASE is decided by its result arms only. A missing ELSE is an implicit
    // `ELSE NULL` — measured: `CASE WHEN false THEN 1 END IS NULL` -> true,
    // while `CASE WHEN false THEN 1 ELSE 2 END IS NULL` -> false. A NULL in a
    // WHEN *condition* only means the branch is not taken, so a nullable
    // condition over non-nullable results is still non-nullable.
    #[test]
    fn case_is_non_nullable_only_when_every_result_arm_including_else_is() {
        let case = |whens: Vec<(Expr, Expr)>, else_: Option<Expr>| Expr::Case {
            operand: None,
            whens,
            else_: else_.map(Box::new),
        };
        let cond = || Expr::Binary {
            op: op(521), // int4 >
            lhs: Box::new(col(1, "n")),
            rhs: Box::new(lit_i32(0)),
        };

        assert!(
            !projected_nullability(case(vec![(cond(), lit_i32(1))], Some(lit_i32(2)))),
            "non-nullable THEN and ELSE make the CASE non-nullable, even though the WHEN \
             condition is nullable — a NULL condition skips the branch, it does not produce a \
             NULL result"
        );
        assert!(
            projected_nullability(case(vec![(cond(), lit_i32(1))], None)),
            "a missing ELSE is an implicit ELSE NULL, so the CASE is nullable however total its \
             THEN arms are"
        );
        assert!(
            projected_nullability(case(vec![(cond(), col(1, "n"))], Some(lit_i32(2)))),
            "one nullable THEN arm is enough"
        );
    }

    // The three-valued-logic tests exist to turn an unknown into a definite
    // boolean, so they are total over any argument, nullable or not.
    // Measured: `(NULL::int IS NULL) IS NULL` -> false,
    // `(NULL::bool IS TRUE) IS NULL` -> false,
    // `(NULL::int IS NOT DISTINCT FROM NULL::int) IS NULL` -> false.
    #[test]
    fn the_null_tests_are_non_nullable_over_a_nullable_argument() {
        assert!(!projected_nullability(Expr::IsNull {
            arg: Box::new(col(1, "n")),
            negated: false,
        }));
        assert!(!projected_nullability(Expr::DistinctFrom {
            lhs: Box::new(col(1, "n")),
            rhs: Box::new(col(1, "n")),
            negated: false,
        }));
    }

    // Anything not proven total stays nullable, which is exactly where this
    // file started — so a shape the inference has no rule for can never
    // regress. `NULLIF(1,1)` is NULL from non-NULL inputs (measured), which
    // is why `ScalarFn` has no rule.
    #[test]
    fn an_unrecognized_expression_shape_falls_back_to_nullable() {
        let schema = mixed_schema();
        assert!(
            expr_is_nullable(
                &Expr::ScalarFn {
                    func: basin_plan::FuncId(basin_pgtype::Oid(1)),
                    args: vec![col(0, "nn")],
                },
                &schema
            ),
            "an unmodelled ScalarFn must be reported nullable: NULLIF(1,1) is NULL from wholly \
             non-NULL arguments, so function totality needs its own measured allowlist"
        );
        assert!(
            expr_is_nullable(
                &Expr::Subscript {
                    arg: Box::new(col(0, "nn")),
                    indices: vec![],
                },
                &schema
            ),
            "(ARRAY[1,2])[9] is NULL, so a subscript must stay nullable"
        );
        assert!(
            expr_is_nullable(
                &Expr::Parameter {
                    index: 1,
                    ty: basin_pgtype::PgType::INT4
                },
                &schema
            ),
            "a bind parameter's value is unknown until Bind and may be NULL"
        );
    }

    // The inference is checked by arrow itself at run time:
    // `RecordBatch::try_new` rejects a non-nullable field whose array holds
    // nulls. This runs a batch through a Project whose output really is
    // non-nullable and confirms the declared schema and the produced batch
    // agree — an over-claim here would surface as an error from
    // `next_batch`, never as a silently wrong RowDescription.
    #[test]
    fn a_non_nullable_projection_produces_batches_that_satisfy_it() {
        let input = Feed::boxed(mixed_schema(), vec![mixed_batch()]);
        let exprs = vec![
            (col(0, "nn"), "nn_out".to_string()),
            (
                Expr::Coalesce(vec![col(1, "n"), lit_i32(0)]),
                "filled".to_string(),
            ),
            (col(1, "n"), "n_out".to_string()),
        ];
        let mut project = Project::new(input, exprs).unwrap();
        assert!(!project.schema().field(0).is_nullable());
        assert!(!project.schema().field(1).is_nullable());
        assert!(project.schema().field(2).is_nullable());

        let out = project.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        for i in [0usize, 1] {
            assert_eq!(
                out.column(i).null_count(),
                0,
                "column {i} is declared non-nullable, so the data must contain no nulls — \
                 RecordBatch::try_new would already have refused otherwise"
            );
        }
        assert_eq!(out.column(2).null_count(), 1);
    }

    // A Project over a join's null-extended side must go back to nullable:
    // `join.rs` widens those fields in its output schema (confirmed live —
    // the non-preserved side of a LEFT JOIN yields NULL even for a NOT NULL
    // column), and this file must read that rather than the base table's.
    #[test]
    fn a_column_widened_by_an_outer_join_is_reported_nullable_again() {
        // What `join_output_schema` hands a Project above a LEFT JOIN: the
        // preserved side keeps its NOT NULL, the null-extended side does not.
        let joined = Arc::new(Schema::new(vec![
            Field::new("left_nn", DataType::Int32, false),
            Field::new("right_nn_widened", DataType::Int32, true),
        ]));
        let input = Feed::boxed(joined, vec![]);
        let project = Project::new(
            input,
            vec![
                (col(0, "left_nn"), "l".to_string()),
                (col(1, "right_nn_widened"), "r".to_string()),
            ],
        )
        .unwrap();
        assert!(
            !project.schema().field(0).is_nullable(),
            "the preserved side of an outer join keeps its NOT NULL"
        );
        assert!(
            project.schema().field(1).is_nullable(),
            "the null-extended side was widened by join.rs and must stay widened here"
        );
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

    /// A projection to ZERO columns is a real plan, not a degenerate one:
    /// `SELECT count(*) FROM t CROSS JOIN d` needs no input column, so column
    /// pruning leaves a `Project` with no expressions under the aggregate.
    /// Its ROW COUNT is the entire payload — it is exactly what `count(*)`
    /// counts — and `RecordBatch::try_new` cannot carry one without an array
    /// to infer it from: it fails with "must either specify a row count or at
    /// least one column". That error is precisely what the fallback probe
    /// measured on that query, and it is why the row count has to be passed
    /// explicitly.
    #[test]
    fn a_zero_column_projection_still_carries_its_row_count() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(1), Some(2), Some(3), Some(4)]);
        let input = Feed::boxed(schema, vec![batch]);
        let mut project = Project::new(input, vec![]).unwrap();
        let out = project
            .next_batch()
            .expect("a zero-column projection must build, not error")
            .expect("one batch in, one batch out");
        assert_eq!(out.num_columns(), 0);
        assert_eq!(
            out.num_rows(),
            4,
            "the row count is the whole point — count(*) reads it"
        );
    }
}
