//! `CorrelatedScalar` — a correlated scalar subquery, evaluated once per
//! input row.
//!
//! # Why this operator exists
//!
//! `SELECT id, (SELECT count(*) FROM t x WHERE x.id = t.id) FROM t` has a
//! scalar subquery whose answer depends on the row it is being evaluated
//! for. `basin_plan::opt::decorrelate` cannot help here: all four of its
//! transforms are scoped to a subquery sitting as one conjunct of a
//! `LogicalPlan::Filter` predicate, and it says so — "a subquery anywhere
//! else (a `Project` target list, nested inside `OR`, inside `CASE`, ...) is
//! left untouched." So a correlated scalar subquery in a target list arrives
//! at the physical layer still correlated, and something has to evaluate it
//! per row. That is this operator, and it is the same bargain
//! [`crate::lateral::LateralJoin`] makes: the general, always-correct
//! fallback, not the fast path.
//!
//! Before it existed, [`crate::build::materialize_scalar_subquery`] folded
//! every scalar subquery — correlated or not — into a single value computed
//! once for the whole statement, on the strength of a doc comment asserting
//! that decorrelation guaranteed no correlated one could reach it. It did
//! not. The query above answered `3, 3, 3` where PostgreSQL answers
//! `1, 1, 1`: right row count, right shape, wrong values.
//!
//! # Shape
//!
//! Output is the input's columns, unchanged and in order, plus **one
//! appended column per subquery** carrying that subquery's value for each
//! row. The expression that contained the subquery is rewritten by
//! [`crate::build`] to read the appended column instead, which is why this
//! is a plain unary operator rather than something that has to understand
//! expressions. `Project` (or `Filter` plus a trimming `Project`) sits
//! above and decides what survives, exactly as it would for any other
//! column.
//!
//! Like `LateralJoin`, the per-row side is a factory rather than a fixed
//! `Box<dyn Operator>`: the subplan's predicates mention the current outer
//! row's values, so it must be rebuilt against each row (`build.rs` binds
//! `ColumnRef { relation: 1 }` — `opt::decorrelate`'s `OUTER_REF` — to
//! literals from that row and builds a fresh operator). See
//! `lateral.rs`'s module docs for why a closure, and `build.rs`'s
//! `SnapshotResolver` for how it satisfies the `'static` bound.
//!
//! # The two PostgreSQL rules for a scalar subquery, both enforced here
//!
//! - **Zero rows is `NULL`.** Not an error, and not a dropped outer row.
//! - **More than one row is SQLSTATE 21000 `cardinality_violation`**
//!   ([`ExecError::CardinalityViolation`]), not a silently-picked first row.
//!   PostgreSQL 18.2's own wording, checked against a live server:
//!   `more than one row returned by a subquery used as an expression`.
//!
//! Both are the same rules `build.rs`'s uncorrelated one-shot path applies;
//! the only difference here is that the subquery runs once per row instead
//! of once per statement, so the cardinality check is per row too.
//!
//! # Quantified subqueries share this operator
//!
//! `x IN (SELECT …)`, `x NOT IN (…)`, `x op ANY (…)` and `x op ALL (…)` have
//! the same per-row problem when the subquery is correlated, but the opposite
//! cardinality contract: **any** number of rows is legal, and the answer is a
//! three-valued boolean rather than a value. [`CorrelatedKind::Quantified`]
//! covers them. The rows are handed, as arrays, to a
//! [`QuantifiedDecider`] closure that `build.rs` supplies; it returns the
//! `Expr` deciding that one outer row, which this operator evaluates against
//! a one-row slice of the input.
//!
//! Routing it through an `Expr` rather than a comparison kernel here is the
//! point, not an accident: it makes the correlated path reuse — bit for bit —
//! the same `Expr` shape (`InList`, or a Kleene `AND`/`OR` fold) that
//! `build.rs` builds for the *uncorrelated* case, so there is exactly ONE
//! implementation of SQL's three-valued `IN`/`ANY`/`ALL` semantics in the
//! engine and no way for the two paths to disagree about, say, whether
//! `x NOT IN (…, NULL)` is `NULL` or `true`.
//!
//! # Memory
//!
//! One input batch at a time, plus the per-row values accumulated for that
//! batch's appended columns. Each per-row subquery operator is fully drained
//! and dropped before the next row starts, so nothing accumulates across
//! rows beyond the single scalar each contributes.

use std::sync::Arc;

use arrow_array::{new_null_array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use basin_plan::Expr;

use crate::lateral::InnerFactory;
use crate::operator::{ExecError, Operator};

/// PostgreSQL's own message for SQLSTATE 21000 in this position, verbatim
/// from a live 18.2 server so the two engines' errors are comparable rather
/// than merely both being errors.
pub(crate) const CARDINALITY_MESSAGE: &str =
    "more than one row returned by a subquery used as an expression";

/// Turns one correlated subquery's result column — for ONE outer row,
/// possibly split across several batches, and empty when the subquery matched
/// nothing — into the boolean expression that decides that row.
///
/// Returned rather than applied so that the *expression* is the interface:
/// see the module docs' "Quantified subqueries share this operator" for why
/// that is what keeps three-valued logic implemented exactly once.
pub type QuantifiedDecider = Box<dyn Fn(&[ArrayRef]) -> Result<Expr, ExecError>>;

/// What a correlated subquery's per-row rows are allowed to be, and what they
/// mean.
pub enum CorrelatedKind {
    /// A scalar subquery: zero rows is `NULL`, one row is that value, and
    /// more than one row is SQLSTATE 21000. The appended column carries the
    /// subquery's own value.
    Scalar,
    /// A quantified subquery (`IN`/`NOT IN`/`ANY`/`ALL`): **any** row count
    /// is legal, and the appended column carries a three-valued BOOLEAN, not
    /// the subquery's own values.
    Quantified(QuantifiedDecider),
}

/// One correlated subquery: how to build it for a given row, what it means
/// per row ([`CorrelatedKind`]), and the Arrow type of the column it
/// contributes — needed up front, because a row whose subquery returns
/// nothing still contributes a typed value and there is no batch to take the
/// type from.
pub struct CorrelatedSubquery {
    pub factory: InnerFactory,
    /// The type of the APPENDED column, which for
    /// [`CorrelatedKind::Quantified`] is `Boolean` regardless of what the
    /// subquery's own column is.
    pub data_type: DataType,
    /// Output column name. Cosmetic — the expression above reads it by
    /// position — but a named column keeps `EXPLAIN`-style dumps readable.
    pub name: String,
    pub kind: CorrelatedKind,
}

/// Appends one column per correlated scalar subquery to its input, each
/// evaluated once per row. See the module docs.
pub struct CorrelatedScalar {
    input: Box<dyn Operator>,
    subqueries: Vec<CorrelatedSubquery>,
    schema: SchemaRef,
    current: Option<RecordBatch>,
    session: crate::operator::SessionRef,
}

impl CorrelatedScalar {
    /// The output schema of `input` with one nullable field appended per
    /// subquery — nullable unconditionally, since a subquery matching no
    /// rows is `NULL` for that row no matter what its own column's
    /// nullability says.
    pub fn output_schema(input: &SchemaRef, subqueries: &[CorrelatedSubquery]) -> SchemaRef {
        let mut fields: Vec<Field> = input.fields().iter().map(|f| f.as_ref().clone()).collect();
        for s in subqueries {
            fields.push(Field::new(&s.name, s.data_type.clone(), true));
        }
        Arc::new(Schema::new(fields))
    }

    pub fn new(input: Box<dyn Operator>, subqueries: Vec<CorrelatedSubquery>) -> Self {
        let schema = Self::output_schema(&input.schema(), &subqueries);
        Self {
            input,
            subqueries,
            schema,
            current: None,
            session: crate::operator::default_session(),
        }
    }

    /// Evaluate the quantified decision expression in `session`. See
    /// [`crate::operator::SessionRef`].
    ///
    /// The subquery *bodies* get their session separately: each is an operator
    /// tree the `factory` builds, and `build.rs` threads the same session into
    /// that build, so the inner plan is in-session too rather than only the
    /// decider on top of it.
    pub fn in_session(mut self, session: crate::operator::SessionRef) -> Self {
        self.session = session;
        self
    }

    /// Evaluate one subquery for one row of `batch`, returning the one-row
    /// array it contributes: its value for a [`CorrelatedKind::Scalar`] (a
    /// typed NULL when it produced no rows), or the three-valued boolean for
    /// a [`CorrelatedKind::Quantified`].
    fn eval_one(
        sub: &mut CorrelatedSubquery,
        batch: &RecordBatch,
        row: usize,
        session: &crate::operator::SessionRef,
    ) -> Result<ArrayRef, ExecError> {
        let mut op = (sub.factory)(batch, row)?;
        let schema = op.schema();
        if schema.fields().len() != 1 {
            return Err(ExecError::Internal(format!(
                "a subquery used as an expression must return exactly one column, got {} — a \
                 planner bug",
                schema.fields().len()
            )));
        }
        match &sub.kind {
            CorrelatedKind::Scalar => {
                let mut value: Option<ArrayRef> = None;
                while let Some(b) = op.next_batch()? {
                    for r in 0..b.num_rows() {
                        if value.is_some() {
                            return Err(ExecError::CardinalityViolation(
                                CARDINALITY_MESSAGE.to_string(),
                            ));
                        }
                        value = Some(b.column(0).slice(r, 1));
                    }
                }
                Ok(value.unwrap_or_else(|| new_null_array(&sub.data_type, 1)))
            }
            // No cardinality check: `x IN (SELECT …)` over a hundred rows is
            // ordinary SQL, not the 21000 a scalar subquery would raise.
            // Empty is legal too and is NOT the same as NULL — `x NOT IN
            // (empty)` is TRUE even when `x` itself is NULL — which is why
            // the empty case goes to the decider like any other rather than
            // short-circuiting to a null here.
            CorrelatedKind::Quantified(decide) => {
                let mut chunks: Vec<ArrayRef> = Vec::new();
                while let Some(b) = op.next_batch()? {
                    if b.num_rows() > 0 {
                        chunks.push(Arc::clone(b.column(0)));
                    }
                }
                let expr = decide(&chunks)?;
                // The decision expression is written against the OUTER row's
                // schema (the operand is an expression at that level), so it
                // is evaluated against this row alone. `batch` is the input
                // batch, before any of this operator's own columns are
                // appended, so the column positions it names are the ones it
                // was built with.
                let decided = crate::eval::eval_with(&expr, &batch.slice(row, 1), session)?;
                if decided.data_type() != &DataType::Boolean {
                    return Err(ExecError::Internal(format!(
                        "a quantified subquery must decide to a boolean, got {:?}",
                        decided.data_type()
                    )));
                }
                if decided.len() != 1 {
                    return Err(ExecError::Internal(format!(
                        "a quantified subquery must decide exactly one row, got {}",
                        decided.len()
                    )));
                }
                Ok(decided)
            }
        }
    }
}

impl Operator for CorrelatedScalar {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            self.current = None;
            return Ok(None);
        };
        let rows = batch.num_rows();
        self.current = Some(batch.clone());

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        // Cloned out of `self` before the loop below borrows `self.subqueries`
        // mutably. An `Rc` clone is a refcount bump, not a session copy.
        let session = crate::operator::SessionRef::clone(&self.session);
        for sub in &mut self.subqueries {
            // An empty input batch still has to produce a column of the
            // right type and length (zero), and `concat` refuses an empty
            // list of arrays — so that case is built directly rather than
            // concatenated.
            if rows == 0 {
                columns.push(new_null_array(&sub.data_type, 0));
                continue;
            }
            let mut per_row: Vec<ArrayRef> = Vec::with_capacity(rows);
            for row in 0..rows {
                per_row.push(Self::eval_one(sub, &batch, row, &session)?);
            }
            let refs: Vec<&dyn arrow_array::Array> = per_row.iter().map(|a| a.as_ref()).collect();
            let joined =
                arrow::compute::concat(&refs).map_err(|e| ExecError::Internal(e.to_string()))?;
            columns.push(joined);
        }

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map(Some)
            .map_err(|e| ExecError::Internal(e.to_string()))
    }

    fn memory_used(&self) -> usize {
        self.current
            .as_ref()
            .map(|b| b.get_array_memory_size())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, Int64Array};
    use arrow_schema::DataType;

    /// An operator that hands back a fixed list of batches.
    struct Feed {
        schema: SchemaRef,
        batches: std::collections::VecDeque<RecordBatch>,
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

    fn int_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    fn feed(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
        Box::new(Feed {
            schema,
            batches: batches.into_iter().collect(),
        })
    }

    /// One row per outer row, whose VALUE depends on the outer row — the
    /// whole point of the operator. The factory reads the outer row's
    /// column 0 and answers `10 * that`, so a per-statement evaluation
    /// (the bug this operator exists to fix) could not produce this column.
    #[test]
    fn each_row_gets_its_own_subquery_value() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let inner_for_factory = Arc::clone(&inner_schema);
        let factory: InnerFactory = Box::new(move |b: &RecordBatch, i: usize| {
            let v = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i) as i64;
            let batch = RecordBatch::try_new(
                Arc::clone(&inner_for_factory),
                vec![Arc::new(Int64Array::from(vec![v * 10])) as ArrayRef],
            )
            .unwrap();
            Ok(feed(Arc::clone(&inner_for_factory), vec![batch]))
        });

        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![1, 2, 3])],
            ),
            vec![CorrelatedSubquery {
                factory,
                data_type: DataType::Int64,
                name: "sub".into(),
                kind: CorrelatedKind::Scalar,
            }],
        );

        let batch = op.next_batch().unwrap().expect("one batch");
        assert_eq!(batch.num_columns(), 2, "input column plus the subquery's");
        let got: Vec<i64> = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(got, vec![10, 20, 30]);
        assert!(op.next_batch().unwrap().is_none());
    }

    /// Zero rows out of the subquery is NULL for that row — not an error,
    /// and not a dropped row.
    #[test]
    fn a_subquery_matching_nothing_yields_null_not_a_missing_row() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let inner_for_factory = Arc::clone(&inner_schema);
        let factory: InnerFactory = Box::new(move |b: &RecordBatch, i: usize| {
            let v = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i);
            // Only the middle row matches anything.
            let batches = if v == 2 {
                vec![RecordBatch::try_new(
                    Arc::clone(&inner_for_factory),
                    vec![Arc::new(Int64Array::from(vec![99])) as ArrayRef],
                )
                .unwrap()]
            } else {
                vec![]
            };
            Ok(feed(Arc::clone(&inner_for_factory), batches))
        });

        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![1, 2, 3])],
            ),
            vec![CorrelatedSubquery {
                factory,
                data_type: DataType::Int64,
                name: "sub".into(),
                kind: CorrelatedKind::Scalar,
            }],
        );

        let batch = op.next_batch().unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 3, "every input row survives");
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(col.is_null(0));
        assert_eq!(col.value(1), 99);
        assert!(col.is_null(2));
    }

    /// Two rows out of the subquery is PostgreSQL's cardinality violation,
    /// raised per row rather than resolved by picking one.
    #[test]
    fn more_than_one_row_is_a_cardinality_violation() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let inner_for_factory = Arc::clone(&inner_schema);
        let factory: InnerFactory = Box::new(move |_b: &RecordBatch, _i: usize| {
            let batch = RecordBatch::try_new(
                Arc::clone(&inner_for_factory),
                vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            )
            .unwrap();
            Ok(feed(Arc::clone(&inner_for_factory), vec![batch]))
        });

        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![1])],
            ),
            vec![CorrelatedSubquery {
                factory,
                data_type: DataType::Int64,
                name: "sub".into(),
                kind: CorrelatedKind::Scalar,
            }],
        );

        match op.next_batch() {
            Err(ExecError::CardinalityViolation(m)) => assert_eq!(m, CARDINALITY_MESSAGE),
            other => panic!("expected a cardinality violation, got {other:?}"),
        }
    }

    /// A row split across two batches of the subquery's output is still two
    /// rows — the check cannot be per-batch.
    #[test]
    fn two_rows_spread_over_two_batches_still_violate() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let inner_for_factory = Arc::clone(&inner_schema);
        let factory: InnerFactory = Box::new(move |_b: &RecordBatch, _i: usize| {
            let one = RecordBatch::try_new(
                Arc::clone(&inner_for_factory),
                vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
            )
            .unwrap();
            Ok(feed(Arc::clone(&inner_for_factory), vec![one.clone(), one]))
        });

        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![1])],
            ),
            vec![CorrelatedSubquery {
                factory,
                data_type: DataType::Int64,
                name: "sub".into(),
                kind: CorrelatedKind::Scalar,
            }],
        );
        assert!(matches!(
            op.next_batch(),
            Err(ExecError::CardinalityViolation(_))
        ));
    }

    /// An empty input batch produces an empty output batch of the right
    /// width, rather than tripping `concat`'s "no arrays" error.
    #[test]
    fn an_empty_input_batch_produces_an_empty_output_batch() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let inner_for_factory = Arc::clone(&inner_schema);
        let factory: InnerFactory = Box::new(move |_b: &RecordBatch, _i: usize| {
            Ok(feed(Arc::clone(&inner_for_factory), vec![]))
        });
        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![])],
            ),
            vec![CorrelatedSubquery {
                factory,
                data_type: DataType::Int64,
                name: "sub".into(),
                kind: CorrelatedKind::Scalar,
            }],
        );
        let batch = op.next_batch().unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    /// Two subqueries append two columns, in order, each with its own
    /// per-row value.
    #[test]
    fn two_subqueries_append_two_columns_in_order() {
        let outer_schema = int_schema(&["id"]);
        let inner_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)]));
        let make = |mult: i64| -> CorrelatedSubquery {
            let s = Arc::clone(&inner_schema);
            CorrelatedSubquery {
                factory: Box::new(move |b: &RecordBatch, i: usize| {
                    let v = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(i) as i64;
                    let batch = RecordBatch::try_new(
                        Arc::clone(&s),
                        vec![Arc::new(Int64Array::from(vec![v * mult])) as ArrayRef],
                    )
                    .unwrap();
                    Ok(feed(Arc::clone(&s), vec![batch]))
                }),
                data_type: DataType::Int64,
                name: format!("sub{mult}"),
                kind: CorrelatedKind::Scalar,
            }
        };
        let mut op = CorrelatedScalar::new(
            feed(
                Arc::clone(&outer_schema),
                vec![int_batch(&outer_schema, vec![1, 2])],
            ),
            vec![make(10), make(100)],
        );
        let batch = op.next_batch().unwrap().expect("one batch");
        assert_eq!(batch.num_columns(), 3);
        let c1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((c1.value(0), c1.value(1)), (10, 20));
        assert_eq!((c2.value(0), c2.value(1)), (100, 200));
    }
}
